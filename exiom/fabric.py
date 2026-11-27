#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric: tagged request/response remote memory access between symmetric nodes, over several hops.

Every node is a Requester (sends read/write requests to other nodes, several can be in flight),
a Responder (executes the requests received on its memory window and answers them) and a switch
(forwards the packets of other nodes between its ports, following a static route table). The
Fabric only needs a packet stream per port (a SerWB port today, see
``FabricNode.connect_serwb``), not SerWB itself.

Links carry ``link_width`` bits per cycle: 32 (one word, SerWB), 64 or 128 (a whole packet).

Packet (32-bit words, 2 to 4 words; the number of words follows from the kind of packet):

- Word 0 (route)       : [7:0] destination node, [15:8] source node, [18:16] hop limit,
                         [23:19] sequence number on the link, [24] response, [25] flow control,
                         [26] routes, [31:28] magic. Only word used to forward a packet.
- Word 1 (transaction) : [7:0] tag, [11:8] byte enables (request) or status (response),
                         [12] write, [13] copy, [31:16] bits 47 to 32 of the word address (request).
- Word 2               : bits 31 to 0 of the word address in the destination window (request),
                         read data or words copied (response).
- Word 3               : write data (write request).

Word addresses are 48-bit (FABRIC_ADDRESS_BITS): windows of up to 2**48 words of 4 bytes.

Burst (bit [14] of word 1): 1 to FABRIC_BURST_WORDS consecutive words in one packet.
- Request: [11:8] of word 1 is the number of words - 1 (all the bytes are written); a write
  request carries its words from word 3 (3 + n words), a read request has 3 words.
- Response: [19:16] of word 1 is the number of words done (read or written before the first
  error); a read response carries them from word 2 (2 + n words), a write response has 2 words.

Copy request (6 words): write and copy bits; words 1 and 2: word address to copy from; word 3:
bits 31 to 0 of the word address to copy to; word 4: [7:0] node to copy to, [31:16] bits 47 to 32
of the word address to copy to; word 5: words to copy - 1. Answered like a read (words copied in
word 2). The nodes on the way forward it like any request.

Tag: slot of the request in the Requester ([slot_bits-1:0]) and generation of this slot (upper
bits, incremented each time the slot is reused): a response arriving after its request timed out
does not match the request that reuses the slot and is dropped.

Forwarding, flow control and deadlock freedom:

- Routes: each node keeps, per destination, the cost of each of its ports (links to the
  destination through this port). A packet leaves by the port with the lowest cost among those
  that are up, are not the port it came from, and can reach the destination within the hop limit.
- The costs follow the state of the fabric: each node regularly sends to each neighbor its
  distance to every destination without going through this neighbor (route packets), and the cost
  of a port is the distance received on it plus one. When a link or a node goes down, the nodes
  around it see their link down, their distances change, and the change spreads from neighbor to
  neighbor: packets then take the shortest path that is left, and requests for a node that cannot
  be reached any more are answered at once with FABRIC_NODE.
- The hop limit is ``max_hops`` at the source and decremented by each forwarding node.
- Each port stores the packets it receives in one queue per class. The class of a packet is
  (request or response, links already crossed). A packet waiting in a queue of class "h links"
  only ever waits for a queue of class "h + 1 links" of the next node, requests wait for responses
  and responses are always accepted by their Requester: no cycle of packets waiting for each other
  can form, whatever the topology and the routes.
- Credits: a node only sends a packet when the queue of its class has room on the other side of
  the link. Each port regularly sends a flow control packet with, per class, the number of packets
  received (R) and the number of packets it accepts in total (L = R + free places); the sender
  compares L with the number of packets it sent (S). Packets are never dropped for lack of room.
- Retransmission: each port numbers the packets it sends and keeps them until its neighbor has
  acknowledged them (flow control packets carry the next sequence number expected). A port only
  accepts the packet it expects: after a packet lost on the link (CRC error), the following ones
  are discarded, the neighbor is asked to send again from the missing one (at once when a later
  packet shows the gap, after ``retry_timeout`` cycles without acknowledgment otherwise), and
  packets are delivered once and in order. Flow control and route packets are not numbered: they
  carry a state, the next one replaces a lost one.

Semantics:

- Every request gets exactly one result: the response of the destination node (FABRIC_OK or the
  reason it was refused/failed), FABRIC_NODE when a node on the way cannot reach the destination,
  FABRIC_NO_RESPONSE after ``request_timeout`` cycles, or FABRIC_LINK when the node has no link up.
  A failed request only fails itself.
- Writes are acknowledged by the node that executed them (no posted writes).
- Requests from one node to another are executed in the order they were accepted, as long as
  the links of their route do not change state. Results are returned as they complete, identified
  by their tag.
- FABRIC_NO_RESPONSE and FABRIC_LINK mean "not confirmed": the request may have been executed.
- Copy (``FabricCopy``): the node that receives a copy request reads the words in its own window
  and writes them in the window of the node to copy to, with its own requests: the data goes
  straight from the source node to the destination node, not through the node that asked. One copy
  at a time per source node: a copy request received during another one is refused at once with
  FABRIC_BUSY (it never waits in the Responder, so two nodes copying to each other cannot wait for
  each other). The response gives the first error and the number of words written; statuses 8 and
  9 in a copy response are those of the requests of the source node (not confirmed).
- Packets lost on a link are sent again by the link. Packets in a node or on a link that goes
  down are lost, and while this change spreads in the fabric (a few route packet periods),
  packets can be sent towards the link that is down: their requests end with FABRIC_NODE or
  FABRIC_NO_RESPONSE. Requests are not sent again end to end.
"""

from functools import reduce
from operator  import and_, or_

from migen import *
from migen.genlib.coding import PriorityEncoder

from litex.gen import *
from litex.gen.genlib.misc import WaitTimer

from litex.soc.interconnect import stream
from litex.soc.interconnect import wishbone

from liteiclink.serwb.packet import packet_description

# Constants ----------------------------------------------------------------------------------------

FABRIC_MAGIC = 0xe
FABRIC_PORT  = 2 # Default SerWB port (0: SerWB MMAP/Etherbone, 1: SerIO).

# Status (4-bit). 0-7: from a node of the fabric, 8-15: local to the Requester.
FABRIC_OK          = 0
FABRIC_RANGE       = 1 # Outside the window of the destination node.
FABRIC_RO          = 2 # Write to a read-only window.
FABRIC_BUS         = 3 # Memory bus error.
FABRIC_TIMEOUT     = 4 # Memory did not answer.
FABRIC_NODE        = 5 # Destination node not reachable (unknown, or no route within the hop limit).
FABRIC_BUSY        = 6 # Copy refused: the node is already copying.
FABRIC_NO_RESPONSE = 8 # No response within the Requester timeout.
FABRIC_LINK        = 9 # No link up on this node.

FABRIC_ADDRESS_BITS = 48   # Word address in the window of a node.
FABRIC_COPY_WORDS   = 65536 # Words per copy request, at most (256 KiB).

def request_layout():
    # Copy (``copy``): ``words`` words from word ``adr`` of node ``dst`` to word ``copy_adr`` of
    # node ``copy_dst``. The result gives the number of words written in ``dat_r``.
    # Burst (``burst``): ``count`` words (1 to FABRIC_BURST_WORDS) from word ``adr``, written from
    # ``data`` (first word in the low bits). The result gives the words done in ``count`` and the
    # words read in ``data`` (the words after ``count`` are not defined).
    return [("we", 1), ("dst", 8), ("adr", FABRIC_ADDRESS_BITS), ("dat_w", 32), ("sel", 4),
        ("copy", 1), ("copy_dst", 8), ("copy_adr", FABRIC_ADDRESS_BITS), ("words", 17),
        ("burst", 1), ("count", 4), ("data", 32*FABRIC_BURST_WORDS)]

def copy_layout():
    # Copy accepted by a node: ``words`` words from its word ``adr`` to word ``dst_adr`` of node
    # ``dst``, asked by node ``src`` with tag ``tag``.
    return [("src", 8), ("tag", 8), ("adr", FABRIC_ADDRESS_BITS), ("dst", 8),
        ("dst_adr", FABRIC_ADDRESS_BITS), ("words", 17)]

def memory_cmd_layout():
    # Memory port (exiom/memory.py): one word access.
    return [("adr", FABRIC_ADDRESS_BITS), ("we", 1), ("dat_w", 32), ("sel", 4), ("id", 4)]

def memory_rsp_layout():
    # Memory port: response of an access, in the order of the accesses (``id`` of the access).
    return [("dat_r", 32), ("err", 1), ("id", 4)]

def result_layout():
    return [("tag", 8), ("we", 1), ("status", 4), ("dat_r", 32),
        ("count", 4), ("data", 32*FABRIC_BURST_WORDS)]

FABRIC_BURST_WORDS  = 8                      # Words of a burst, at most (up to 15).
FABRIC_PACKET_WORDS = 3 + FABRIC_BURST_WORDS  # Words of the longest packet (burst write request).
assert FABRIC_PACKET_WORDS >= 6               # Copy request.

def fabric_packet_layout():
    # Whole packet: words 0 and 1, the words after them (word 2 in the low bits) and its number
    # of words.
    return [("w0", 32), ("w1", 32), ("body", 32*(FABRIC_PACKET_WORDS - 2)),
        ("len", bits_for(FABRIC_PACKET_WORDS))]

def packet_words(packet):
    """Words of a packet (Record with ``fabric_packet_layout``)."""
    return [packet.w0, packet.w1] + [packet.body[32*i:32*i + 32] for i in range(FABRIC_PACKET_WORDS - 2)]

def packet_bits(packet):
    """All the words of a packet, word 0 in the low bits."""
    return Cat(packet.w0, packet.w1, packet.body)

def route_word(dst, src, ttl, rsp):
    return Cat(dst, src, ttl[:3] if isinstance(ttl, Signal) else Constant(ttl, 3), Constant(0, 5), rsp,
        Constant(0, 3), Constant(FABRIC_MAGIC, 4))

FABRIC_WINDOW = 8 # Packets sent and not yet acknowledged, per port.

def fabric_packet_len(w0, w1, fc_len):
    """Number of words of a packet, from its first two words (the link does not carry it). It can
    be more than FABRIC_PACKET_WORDS for words that are not those of a packet."""
    burst = w1[14]
    return Mux(route_fc(w0), fc_len,
        Mux(route_rt(w0), 4,
        Mux(route_rsp(w0),
            # Write response, read response (burst: words read).
            Mux(w1[12], 2, Mux(burst, 2 + w1[16:20], 3)),
            # Copy request, write request (burst: words to write), read request.
            Mux(w1[12], Mux(w1[13], 6, Mux(burst, 4 + w1[8:12], 4)), 3))))

def route_dst(w0):   return w0[0:8]
def route_src(w0):   return w0[8:16]
def route_ttl(w0):   return w0[16:19]
def route_seq(w0):   return w0[19:24]
def route_rsp(w0):   return w0[24]
def route_fc(w0):    return w0[25]
def route_rt(w0):    return w0[26]
def route_magic(w0): return w0[28:32]

# Routes -------------------------------------------------------------------------------------------

ROUTE_NONE    = 7  # Cost of a port that does not lead to the destination (distance: not reachable).
ROUTE_ENTRIES = 20 # Destinations per route packet.

class FabricRoutes(LiteXModule):
    """Routes of a node.

    ``routes``: {destination: [cost of each port]} with all the links up (``Topology.costs()``; a
    single port number per destination is also accepted): values at reset, then updated from the
    route packets of the neighbors. ``routes`` None: point to point, every destination through
    port 0, no route packets.

    ``up[p]``: state of the link of port p. ``rx[p]``/``tx[p]``: route packets from/to port p.

    Route packet: word 1 = first destination, words 2 and 3 = 10 distances each (3-bit, 7: not
    reachable): distance of the sender to these destinations without going through the receiver.
    """
    def __init__(self, node_id, routes, ports, up, entries=ROUTE_ENTRIES, period=256):
        assert 1 <= entries <= ROUTE_ENTRIES
        self.ports  = ports
        self.static = routes is None
        self.rx     = [stream.Endpoint(fabric_packet_layout()) for _ in range(ports)]
        self.tx     = [stream.Endpoint(fabric_packet_layout()) for _ in range(ports)]
        self.up     = up

        # Status.
        self.changes = changes = Signal(32) # Distances changed (sent to a neighbor).

        # # #

        for p in range(ports):
            self.comb += self.rx[p].ready.eq(1)
        if self.static:
            return

        # Table.
        # ------
        table = {}
        for dst, value in routes.items():
            assert 0 <= dst < 256 and dst != node_id
            if isinstance(value, int):
                value = [1 if p == value else None for p in range(ports)]
            assert len(value) == ports
            table[dst] = [ROUTE_NONE if c is None else c for c in value]
            assert all(1 <= c <= ROUTE_NONE for c in table[dst])
        self.size = size = max(list(table.keys()) + [node_id]) + 1
        self.bits = bits = max(1, bits_for(size - 1))
        none      = [ROUTE_NONE]*ports
        def reset_distance(d, without=None):
            if d == node_id:
                return 0
            return min([ROUTE_NONE] + [c for p, c in enumerate(table.get(d, none)) if p != without])
        # Cost of each port, distance of the node, distance sent to each neighbor.
        self.costs = costs = [[Signal(3, reset=table.get(d, none)[p], name=f"cost_p{p}_d{d}")
            for d in range(size)] for p in range(ports)]
        self.dist  = dist  = [Signal(3, reset=reset_distance(d), name=f"dist_d{d}") for d in range(size)]
        self.out   = out   = [[Signal(3, reset=reset_distance(d, q), name=f"out_p{q}_d{d}")
            for d in range(size)] for q in range(ports)]

        # Distances: one (neighbor, destination) per cycle.
        # -------------------------------------------------
        # Distance to d without going through port q (q == ports: through any port).
        q     = Signal(max=ports + 1)
        d     = Signal(max=max(2, size))
        best  = Constant(ROUTE_NONE, 3)
        for p in range(ports):
            cost = Signal(3, name=f"sweep_cost{p}")
            n    = Signal(3, name=f"sweep_best{p}")
            self.comb += [
                cost.eq(Mux(up[p] & (q != p), Array(costs[p])[d], ROUTE_NONE)),
                n.eq(Mux(cost < best, cost, best)),
            ]
            best = n
        value = Signal(3)
        dirty = [Signal(name=f"dirty{p}") for p in range(ports)]
        self.comb += value.eq(Mux(d == node_id, 0, best))
        self.sync += [
            If(d == (size - 1),
                d.eq(0),
                If(q == ports, q.eq(0)).Else(q.eq(q + 1)),
            ).Else(
                d.eq(d + 1)
            ),
            If(q == ports, Array(dist)[d].eq(value)),
        ]
        for p in range(ports):
            self.comb += dirty[p].eq((q == p) & (Array(out[p])[d] != value))
            self.sync += If(q == p, Array(out[p])[d].eq(value))
        self.sync += If(Cat(*dirty) != 0, If(changes != (2**32 - 1), changes.eq(changes + 1)))

        # Route packets to the neighbors: regularly, and when a distance changed or the link came up.
        # -------------------------------------------------------------------------------------------
        chunks = (size + entries - 1)//entries
        for p in range(ports):
            tx    = self.tx[p]
            need  = Signal(name=f"rt_need{p}")
            redo  = Signal(name=f"rt_redo{p}") # Changed while its packets were being sent.
            chunk = Signal(max=max(2, chunks), name=f"rt_chunk{p}")
            up_d  = Signal(name=f"rt_up_d{p}")
            timer = WaitTimer(period)
            self.add_module(f"rt_timer{p}", timer)
            base  = Signal(8, name=f"rt_base{p}")
            last  = Signal(name=f"rt_last{p}")
            distances = []
            for i in range(entries):
                if chunks == 1:
                    distances.append(out[p][i] if i < size else Constant(ROUTE_NONE, 3))
                else:
                    index = Signal(max=chunks*entries, name=f"rt_index{p}_{i}")
                    self.comb += index.eq(base + i)
                    distances.append(Array(out[p] + [Constant(ROUTE_NONE, 3)]*(chunks*entries - size))[index])
            distances += [Constant(ROUTE_NONE, 3)]*(ROUTE_ENTRIES - entries)
            self.comb += [
                base.eq(chunk*entries),
                last.eq(chunk == (chunks - 1)),
                timer.wait.eq(~(tx.valid & tx.ready)),
                tx.valid.eq(need & up[p]),
                tx.w0.eq((FABRIC_MAGIC << 28) | (1 << 26)),
                tx.w1.eq(base),
                packet_words(tx)[2].eq(Cat(*distances[:10])),
                packet_words(tx)[3].eq(Cat(*distances[10:])),
                tx.len.eq(4),
            ]
            self.sync += [
                up_d.eq(up[p]),
                If(tx.valid & tx.ready,
                    If(last,
                        chunk.eq(0),
                        need.eq(redo),
                        redo.eq(0),
                    ).Else(
                        chunk.eq(chunk + 1)
                    )
                ),
                If(dirty[p] | timer.done | (up[p] & ~up_d),
                    need.eq(1),
                    # First packet already gone: sent again with the new distances.
                    If((chunk != 0) | (tx.valid & tx.ready & ~last), redo.eq(1)),
                ),
                If(~up[p],
                    chunk.eq(0),
                    redo.eq(0),
                )
            ]

        # Route packets from the neighbors: cost of the port = distance received + 1.
        # ----------------------------------------------------------------------------
        for p in range(ports):
            rx       = self.rx[p]
            received = Cat(packet_words(rx)[2][:30], packet_words(rx)[3][:30])
            first    = Signal(8, name=f"rt_first{p}")
            self.comb += first.eq(rx.w1[:8])
            for dst in range(size):
                if dst == node_id:
                    continue
                if chunks == 1:
                    match    = (first == 0)
                    distance = received[3*dst:3*dst + 3]
                else:
                    offset   = Signal(8, name=f"rt_offset{p}_{dst}")
                    self.comb += offset.eq(dst - first)
                    match    = (dst >= first) & (offset < entries)
                    distance = Array([received[3*i:3*i + 3] for i in range(entries)])[offset[:max(1, bits_for(entries - 1))]]
                self.sync += [
                    If(rx.valid & (rx.len == 4) & match,
                        costs[p][dst].eq(Mux(distance >= (ROUTE_NONE - 1), ROUTE_NONE, distance + 1))
                    ),
                    # Link down: nothing through this port until its neighbor sends its distances.
                    If(~up[p], costs[p][dst].eq(ROUTE_NONE)),
                ]

    def reachable(self, dst, known):
        """Statements telling if ``dst`` can be reached now."""
        if self.static:
            return [known.eq(1)]
        return [known.eq((dst < self.size) & (Array(self.dist)[dst[:self.bits]] != ROUTE_NONE))]

    def select(self, dst, exclude, exclude_port, budget, valid, port):
        """Statements selecting the port of a packet for ``dst``: lowest cost among the ports that
        are up, are not the port it came from (``exclude_port`` when ``exclude``) and whose cost
        fits in the links the packet may still cross (``budget``)."""
        statements = []
        best_cost, best_port, best_valid = Constant(ROUTE_NONE, 3), Constant(0), Constant(0, 1)
        for p in range(self.ports):
            cost   = Signal(3, name=f"route_cost{p}")
            ok     = Signal(name=f"route_ok{p}")
            better = Signal(name=f"route_better{p}")
            n_cost, n_port, n_valid = Signal(3), Signal(max=max(2, self.ports)), Signal()
            if self.static:
                statements.append(cost.eq(1 if p == 0 else ROUTE_NONE))
            else:
                statements.append(cost.eq(Mux(dst < self.size, Array(self.costs[p])[dst[:self.bits]], ROUTE_NONE)))
            statements += [
                ok.eq(self.up[p] & ~(exclude & (exclude_port == p)) & (cost != ROUTE_NONE) & (cost <= budget)),
                better.eq(ok & (~best_valid | (cost < best_cost))),
                n_cost.eq(Mux(better, cost, best_cost)),
                n_port.eq(Mux(better, p, best_port)),
                n_valid.eq(best_valid | ok),
            ]
            best_cost, best_port, best_valid = n_cost, n_port, n_valid
        statements += [valid.eq(best_valid), port.eq(best_port)]
        return statements

# Fabric Requester ---------------------------------------------------------------------------------

class FabricRequester(LiteXModule):
    """Sends requests and returns their results, with up to ``requests`` requests in flight.

    ``sink``: requests (``tag`` gives the tag of the request being accepted). ``source``: results.
    ``copy_sink``/``copy_source``: same, for the requests of the copies run by this node
    (``FabricCopy``); they share the slots with the requests of ``sink``, served in turn.
    ``tx``/``rx``: request packets out, response packets in (whole packets). ``copy_timeout``:
    timeout of copy requests (default: 32 x ``timeout``), to which 8 cycles per word to copy are
    added.
    """
    def __init__(self, node_id, ports=1, max_hops=1, requests=8, timeout=4096, copy_timeout=None):
        assert requests >= 1 and requests <= 64
        copy_timeout = 32*timeout if copy_timeout is None else copy_timeout
        self.sink        = user_sink   = stream.Endpoint(request_layout())
        self.source      = source      = stream.Endpoint(result_layout())
        self.copy_sink   = copy_sink   = stream.Endpoint(request_layout())
        self.copy_source = copy_source = stream.Endpoint(result_layout())
        self.tag     = tag     = Signal(8)
        self.tx      = tx      = stream.Endpoint(fabric_packet_layout())
        self.rx      = rx      = stream.Endpoint(fabric_packet_layout())
        self.port_up = port_up = Signal(ports, reset=2**ports - 1)
        self.dst     = dst     = Signal(8)        # Destination of the request presented.
        self.known   = known   = Signal(reset=1)  # This destination can be reached (from the routes).

        # Status.
        self.pending = pending = Signal(max=requests + 1) # Requests in flight.
        self.stale   = stale   = Signal(32)               # Responses dropped (no matching request).

        # # #

        slot_bits = max(1, bits_for(requests - 1))
        gen_bits  = 8 - slot_bits

        # Request served: ``sink`` and ``copy_sink`` in turn.
        # ----------------------------------------------------
        sink       = stream.Endpoint(request_layout())
        from_copy  = Signal() # Request served is from ``copy_sink``.
        last_copy  = Signal()
        self.comb += [
            from_copy.eq(copy_sink.valid & (~user_sink.valid | ~last_copy)),
            If(from_copy,
                *copy_sink.connect(sink)
            ).Else(
                *user_sink.connect(sink)
            ),
        ]
        self.sync += If(sink.valid & sink.ready, last_copy.eq(from_copy))

        # Destination of the request.
        # ---------------------------
        local  = Signal()
        any_up = Signal()
        send   = Signal() # Request sent (not ended at once).
        bad    = Signal()
        self.comb += [
            dst.eq(sink.dst),
            local.eq(dst == node_id),
            any_up.eq(port_up != 0),
            # Burst of no word, or of more words than a packet carries: not sent.
            bad.eq(sink.burst & ~sink.copy & ((sink.count == 0) | (sink.count > FABRIC_BURST_WORDS))),
            send.eq((local | (known & any_up)) & ~bad),
        ]

        # Slots.
        # ------
        FREE, PENDING, DONE = range(3)
        states   = [Signal(2,         name=f"slot{i}_state")  for i in range(requests)]
        gens     = [Signal(gen_bits,  name=f"slot{i}_gen")    for i in range(requests)]
        wes      = [Signal(1,         name=f"slot{i}_we")     for i in range(requests)]
        statuses = [Signal(4,         name=f"slot{i}_status") for i in range(requests)]
        dat_rs   = [Signal(32*FABRIC_BURST_WORDS, name=f"slot{i}_dat_r") for i in range(requests)]
        locals_  = [Signal(1,         name=f"slot{i}_local")  for i in range(requests)]
        owners   = [Signal(1,         name=f"slot{i}_owner")  for i in range(requests)] # 1: copy_sink.
        copies   = [Signal(1,         name=f"slot{i}_copy")   for i in range(requests)] # Copy request.
        limit_max = max(timeout, copy_timeout + 8*FABRIC_COPY_WORDS)
        timers   = [Signal(max=limit_max + 1, name=f"slot{i}_timer") for i in range(requests)]
        limits   = [Signal(max=limit_max + 1, name=f"slot{i}_limit") for i in range(requests)]
        counts   = [Signal(4,         name=f"slot{i}_count")  for i in range(requests)]

        # Allocation (first free slot).
        self.free_encoder = free_encoder = PriorityEncoder(requests)
        self.comb += free_encoder.i.eq(Cat(*[s == FREE for s in states]))
        alloc      = Signal()
        alloc_slot = Signal(slot_bits)
        self.comb += alloc_slot.eq(free_encoder.o)

        # Completion by a response (always accepted).
        rsp_slot  = Signal(slot_bits)
        rsp_gen   = Signal(gen_bits)
        rsp_match = Signal()
        self.comb += [
            rx.ready.eq(1),
            rsp_slot.eq(rx.w1[0:slot_bits]),
            rsp_gen.eq(rx.w1[slot_bits:8]),
            rsp_match.eq(
                (Array(states)[rsp_slot] == PENDING) & (Array(gens)[rsp_slot] == rsp_gen) & (rsp_slot < requests)
            ),
        ]

        # Results (first done slot of each owner).
        releases = []
        for owner, endpoint in enumerate([source, copy_source]):
            done_encoder = PriorityEncoder(requests)
            self.add_module(f"done_encoder{owner}", done_encoder)
            release      = Signal(name=f"release{owner}")
            release_slot = Signal(slot_bits, name=f"release{owner}_slot")
            self.comb += [
                done_encoder.i.eq(Cat(*[(s == DONE) & (o == owner) for s, o in zip(states, owners)])),
                release_slot.eq(done_encoder.o),
                endpoint.valid.eq(~done_encoder.n),
                endpoint.tag.eq(Cat(release_slot, Array(gens)[release_slot])),
                endpoint.we.eq(Array(wes)[release_slot]),
                endpoint.status.eq(Array(statuses)[release_slot]),
                endpoint.dat_r.eq(Array(dat_rs)[release_slot][:32]),
                endpoint.data.eq(Array(dat_rs)[release_slot]),
                endpoint.count.eq(Array(counts)[release_slot]),
                release.eq(endpoint.valid & endpoint.ready),
            ]
            releases.append((release, release_slot))

        for i in range(requests):
            slot_up = Signal(name=f"slot{i}_up")
            self.comb += slot_up.eq(locals_[i] | any_up)
            self.sync += [
                If(alloc & (alloc_slot == i),
                    wes[i].eq(sink.we & ~sink.copy),
                    timers[i].eq(0),
                    dat_rs[i].eq(0),
                    locals_[i].eq(local),
                    owners[i].eq(from_copy),
                    copies[i].eq(sink.copy),
                    counts[i].eq(0),
                    limits[i].eq(Mux(sink.copy, copy_timeout + (sink.words << 3), timeout)),
                    If(send,
                        states[i].eq(PENDING),
                    ).Else(
                        # Not sent: burst not valid, no link up, or destination not reachable.
                        states[i].eq(DONE),
                        statuses[i].eq(Mux(bad, FABRIC_RANGE, Mux(any_up, FABRIC_NODE, FABRIC_LINK))),
                    )
                ).Elif(states[i] == PENDING,
                    If(~slot_up,
                        # No link left: the response cannot come back.
                        states[i].eq(DONE),
                        statuses[i].eq(FABRIC_LINK),
                    ).Elif(rx.valid & rsp_match & (rsp_slot == i),
                        states[i].eq(DONE),
                        statuses[i].eq(rx.w1[8:12]),
                        dat_rs[i].eq(Mux(rx.len >= 3, Cat(*packet_words(rx)[2:2 + FABRIC_BURST_WORDS]), 0)),
                        counts[i].eq(Mux(rx.w1[14], rx.w1[16:20], 0)),
                    ).Elif(timers[i] == limits[i],
                        states[i].eq(DONE),
                        statuses[i].eq(FABRIC_NO_RESPONSE),
                    ).Else(
                        timers[i].eq(timers[i] + 1)
                    )
                ).Elif((states[i] == DONE) & reduce(or_, [r & (slot == i) for r, slot in releases]),
                    states[i].eq(FREE),
                    gens[i].eq(gens[i] + 1),
                )
            ]
        self.comb += pending.eq(sum(s == PENDING for s in states))
        self.sync += If(rx.valid & ~rsp_match & (stale != (2**32 - 1)), stale.eq(stale + 1))

        # Request packet.
        # ---------------
        # One packet waiting to be taken by the switch.
        tx_valid = Signal()
        self.comb += [
            tag.eq(Cat(alloc_slot, Array(gens)[alloc_slot])),
            tx.valid.eq(tx_valid),
            # The next request is taken when the packet of the previous one leaves.
            sink.ready.eq((~tx_valid | tx.ready) & ~free_encoder.n),
            alloc.eq(sink.valid & sink.ready),
        ]
        self.sync += [
            If(tx.valid & tx.ready,
                tx_valid.eq(0)
            ),
            If(alloc,
                tx_valid.eq(send),
                tx.w0.eq(route_word(sink.dst, Constant(node_id, 8), max_hops, 0)),
                tx.w1.eq(Cat(tag, sink.sel, sink.we | sink.copy, sink.copy, Constant(0, 2), sink.adr[32:48])),
                packet_words(tx)[2].eq(sink.adr[:32]),
                packet_words(tx)[3].eq(sink.dat_w),
                *[packet_words(tx)[w].eq(0) for w in range(4, FABRIC_PACKET_WORDS)],
                tx.len.eq(Mux(sink.we, 4, 3)),
                If(sink.burst & ~sink.copy,
                    tx.w1[8:12].eq(sink.count - 1),
                    tx.w1[14].eq(1),
                    *[packet_words(tx)[3 + w].eq(sink.data[32*w:32*w + 32]) for w in range(FABRIC_BURST_WORDS)],
                    tx.len.eq(Mux(sink.we, 3 + sink.count, 3)),
                ),
                If(sink.copy,
                    packet_words(tx)[3].eq(sink.copy_adr[:32]),
                    packet_words(tx)[4].eq(Cat(sink.copy_dst, Constant(0, 8), sink.copy_adr[32:48])),
                    packet_words(tx)[5].eq(sink.words - 1),
                    tx.len.eq(6),
                ),
            )
        ]

# Fabric Responder ---------------------------------------------------------------------------------

class FabricResponder(LiteXModule):
    """Executes the requests received on the memory window of the node and answers them.

    ``rx``: request packets in (``buffer_depth`` packets, with backpressure), ``tx``: response
    packets out. ``mem``: Wishbone Master to the memory (word addresses relative to the window).
    Requests for another node (given to this node because they cannot go further) are answered
    with FABRIC_NODE. ``copy``: copy requests for this node, checked (source words in the window),
    for ``FabricCopy`` which answers those it takes; when it is not ready, they are answered with
    FABRIC_BUSY.
    """
    def __init__(self, node_id, size, max_hops=1, read_only=False, timeout=1024, buffer_depth=4):
        assert size > 0 and (size % 4) == 0 and buffer_depth >= 2
        self.size = size
        self.rx   = rx  = stream.Endpoint(fabric_packet_layout())
        self.tx   = tx  = stream.Endpoint(fabric_packet_layout())
        # Window of more than 4 GiB: wider address bus.
        self.mem  = mem = wishbone.Interface(data_width=32, adr_width=max(30, bits_for(size//4 - 1)))
        self.copy = copy = stream.Endpoint(copy_layout())

        # Status.
        self.requests  = requests  = Signal(32) # Requests answered.
        self.malformed = malformed = Signal(32) # Requests dropped (wrong number of words).

        # # #

        words = size//4

        # Request Buffer.
        # ---------------
        self.fifo = fifo = stream.SyncFIFO(fabric_packet_layout(), buffer_depth, buffered=True)
        self.comb += rx.connect(fifo.sink)
        sink = fifo.source

        # Request at the head of the buffer.
        # ----------------------------------
        we       = Signal()
        is_copy  = Signal()
        is_burst = Signal()
        count    = Signal(5) # Words of the request.
        local    = Signal()
        adr      = Signal(FABRIC_ADDRESS_BITS)
        refused = Signal(4) # Status when the request is not executed (FABRIC_OK: executed).
        self.comb += [
            we.eq(sink.w1[12]),
            is_copy.eq(sink.w1[12] & sink.w1[13]),
            is_burst.eq(sink.w1[14] & ~is_copy),
            count.eq(Mux(is_burst, sink.w1[8:12] + 1, 1)),
            local.eq(route_dst(sink.w0) == node_id),
            copy.src.eq(route_src(sink.w0)),
            copy.tag.eq(sink.w1[0:8]),
            adr.eq(Cat(packet_words(sink)[2], sink.w1[16:32])),
            copy.adr.eq(adr),
            copy.dst.eq(packet_words(sink)[4][0:8]),
            copy.dst_adr.eq(Cat(packet_words(sink)[3], packet_words(sink)[4][16:32])),
            copy.words.eq(packet_words(sink)[5][:16] + 1),
            refused.eq(FABRIC_OK),
            If(~local,
                refused.eq(FABRIC_NODE)
            ).Elif(adr >= words,
                refused.eq(FABRIC_RANGE)
            ).Elif(is_copy,
                # Words read by a copy: all in the window, FABRIC_COPY_WORDS at most.
                If(((adr + copy.words) > words) | (packet_words(sink)[5] >= FABRIC_COPY_WORDS), refused.eq(FABRIC_RANGE))
            ).Elif((count > FABRIC_BURST_WORDS) | ((adr + count) > words),
                refused.eq(FABRIC_RANGE)
            ).Elif(we & int(read_only),
                refused.eq(FABRIC_RO)
            ),
        ]

        # Response: one packet waiting to be taken by the switch.
        # --------------------------------------------------------
        tx_valid = Signal()
        free     = Signal() # A response can be stored at the end of this cycle.
        respond  = Signal()
        status   = Signal(4)
        dat_r    = Signal(32)
        idx      = Signal(4) # Word of the request being accessed.
        done     = Signal(4) # Words done.
        rdata    = [Signal(32, name=f"rdata{i}") for i in range(FABRIC_BURST_WORDS)]
        self.comb += [
            tx.valid.eq(tx_valid),
            free.eq(~tx_valid | tx.ready),
            done.eq(idx + (status == FABRIC_OK)),
        ]
        self.sync += [
            If(tx.valid & tx.ready,
                tx_valid.eq(0)
            ),
            If(respond,
                tx_valid.eq(1),
                tx.w0.eq(route_word(route_src(sink.w0), Constant(node_id, 8), max_hops, 1)),
                tx.w1.eq(Cat(sink.w1[0:8], status, we, Constant(0, 1), is_burst, Constant(0, 1),
                    Mux(is_burst, done, 0))),
                # Words read: those of the accesses before, and the one of this cycle.
                *[packet_words(tx)[2 + w].eq(Mux(idx == w, dat_r, rdata[w])) for w in range(FABRIC_BURST_WORDS)],
                tx.len.eq(Mux(we, 2, Mux(is_burst, 2 + done, 3))),
                requests.eq(requests + 1),
            )
        ]

        # Execution: the memory is accessed from the request itself, which leaves the buffer with
        # its response (a request every two cycles with a memory answering in one). The words of
        # a burst are accessed one after the other, until the first error.
        # ---------------------------------------------------------------------------------
        self.timer = timer = WaitTimer(timeout) # Watchdog.
        access  = Signal() # Memory access of the request.
        started = Signal() # Access started: goes to its end.
        well    = Signal() # Well formed.
        last    = Signal() # Last word of the request.
        self.comb += [
            well.eq(sink.len == Mux(we, Mux(is_copy, 6, Mux(is_burst, 3 + count, 4)), 3)),
            last.eq(idx == (count - 1)),
            access.eq(sink.valid & well & (refused == FABRIC_OK) & ~is_copy & (started | free)),
            mem.cyc.eq(access),
            mem.stb.eq(access),
            mem.adr.eq(adr + idx),
            mem.we.eq(we),
            mem.sel.eq(Mux(is_burst, 0b1111, sink.w1[8:12])),
            mem.dat_w.eq(Array(packet_words(sink)[3:3 + FABRIC_BURST_WORDS])[idx]),
            timer.wait.eq(access & ~respond & ~mem.ack), # Per word.
            If(sink.valid,
                If(~well,
                    sink.ready.eq(1)
                ).Elif(refused != FABRIC_OK,
                    status.eq(refused),
                    respond.eq(free),
                ).Elif(is_copy,
                    # Taken by the copy engine (which answers it), or refused: never waits here.
                    copy.valid.eq(1),
                    If(copy.ready,
                        sink.ready.eq(1)
                    ).Else(
                        status.eq(FABRIC_BUSY),
                        respond.eq(free),
                    )
                ).Elif(access & mem.ack,
                    status.eq(FABRIC_OK),
                    dat_r.eq(mem.dat_r),
                    respond.eq(last),
                ).Elif(access & mem.err,
                    status.eq(FABRIC_BUS),
                    respond.eq(1),
                ).Elif(access & timer.done,
                    status.eq(FABRIC_TIMEOUT),
                    respond.eq(1),
                ),
                If(respond, sink.ready.eq(1)),
            ),
        ]
        self.sync += [
            If(access & mem.ack & ~last,
                idx.eq(idx + 1),
                *[If(idx == w, rdata[w].eq(mem.dat_r)) for w in range(FABRIC_BURST_WORDS)],
            ),
            If(respond | ~access, idx.eq(0)),
            If(access, started.eq(1)),
            If(respond | ~access, started.eq(0)),
            If(sink.valid & sink.ready & ~well, malformed.eq(malformed + 1)),
        ]

# Fabric Pipelined Responder -----------------------------------------------------------------------

class FabricPipelinedResponder(LiteXModule):
    """Executes the requests received on a memory port (``cmd``/``rsp``, see exiom/memory.py):
    the accesses are sent one per cycle without waiting for their answers, the words of a burst
    and the requests one after the other.

    Same packets, checks and statuses as ``FabricResponder``, with these differences:
    - all the words of a burst are accessed: after an error in a burst, the words after it may
      have been written (the response still gives the words done before the first error);
    - ``timeout`` is the time without any answer from the memory to the oldest request; a request
      that times out reports no word done (which ones were is not known), and its answers are
      dropped when they come later. A request whose answers are missing when the memory answers
      a newer one ends at once with FABRIC_TIMEOUT.
    ``pending``: requests sent to the memory and not yet answered, at most.
    """
    def __init__(self, node_id, size, max_hops=1, read_only=False, timeout=1024, buffer_depth=4, pending=4):
        assert size % 4 == 0 and size >= 4
        self.rx   = rx  = stream.Endpoint(fabric_packet_layout())
        self.tx   = tx  = stream.Endpoint(fabric_packet_layout())
        self.cmd  = cmd = stream.Endpoint(memory_cmd_layout())
        self.rsp  = rsp = stream.Endpoint(memory_rsp_layout())
        self.copy = copy = stream.Endpoint(copy_layout())

        # Status.
        self.requests  = requests  = Signal(32) # Requests answered.
        self.malformed = malformed = Signal(32) # Requests dropped (wrong number of words).
        self.late      = late      = Signal(32) # Answers of the memory dropped (request timed out).

        # # #

        words = size//4
        burst = FABRIC_BURST_WORDS

        # Request Buffer.
        # ---------------
        self.fifo = fifo = stream.SyncFIFO(fabric_packet_layout(), buffer_depth, buffered=True)
        self.comb += rx.connect(fifo.sink)
        sink = fifo.source

        # Request at the head of the buffer.
        # ----------------------------------
        we       = Signal()
        is_copy  = Signal()
        is_burst = Signal()
        count    = Signal(5) # Words of the request.
        local    = Signal()
        adr      = Signal(FABRIC_ADDRESS_BITS)
        refused  = Signal(4) # Status when the request is not executed (FABRIC_OK: executed).
        well     = Signal()  # Well formed.
        self.comb += [
            we.eq(sink.w1[12]),
            is_copy.eq(sink.w1[12] & sink.w1[13]),
            is_burst.eq(sink.w1[14] & ~is_copy),
            count.eq(Mux(is_burst, sink.w1[8:12] + 1, 1)),
            local.eq(route_dst(sink.w0) == node_id),
            adr.eq(Cat(packet_words(sink)[2], sink.w1[16:32])),
            copy.src.eq(route_src(sink.w0)),
            copy.tag.eq(sink.w1[0:8]),
            copy.adr.eq(adr),
            copy.dst.eq(packet_words(sink)[4][0:8]),
            copy.dst_adr.eq(Cat(packet_words(sink)[3], packet_words(sink)[4][16:32])),
            copy.words.eq(packet_words(sink)[5][:16] + 1),
            well.eq(sink.len == Mux(we, Mux(is_copy, 6, Mux(is_burst, 3 + count, 4)), 3)),
            refused.eq(FABRIC_OK),
            If(~local,
                refused.eq(FABRIC_NODE)
            ).Elif(adr >= words,
                refused.eq(FABRIC_RANGE)
            ).Elif(is_copy,
                If(((adr + copy.words) > words) | (packet_words(sink)[5] >= FABRIC_COPY_WORDS), refused.eq(FABRIC_RANGE))
            ).Elif((count > burst) | ((adr + count) > words),
                refused.eq(FABRIC_RANGE)
            ).Elif(we & int(read_only),
                refused.eq(FABRIC_RO)
            ),
        ]

        # Requests sent to the memory (or refused), in order, until they are answered.
        # ------------------------------------------------------------------------------
        self.descs = descs = stream.SyncFIFO([("src", 8), ("tag", 8), ("we", 1), ("burst", 1),
            ("count", 5), ("status", 4), ("id", 4)], pending, buffered=True)
        push = descs.sink
        head = descs.source

        # Accesses: one word per cycle.
        # -----------------------------
        started = Signal()  # Request of the head of the buffer given to the memory.
        idx     = Signal(4) # Word to access.
        rid     = Signal(4) # Number of the request (id of its accesses).
        abort   = Signal()  # The request being sent timed out.
        pushed  = Signal()
        issued  = Signal()
        leave   = Signal()  # The request leaves the buffer.
        self.comb += [
            push.src.eq(route_src(sink.w0)),
            push.tag.eq(sink.w1[0:8]),
            push.we.eq(we),
            push.burst.eq(is_burst),
            push.id.eq(rid),
            pushed.eq(push.valid & push.ready),
            cmd.adr.eq(adr + idx),
            cmd.we.eq(we),
            cmd.sel.eq(Mux(is_burst, 0b1111, sink.w1[8:12])),
            cmd.dat_w.eq(Array(packet_words(sink)[3:3 + burst])[idx]),
            cmd.id.eq(rid),
            issued.eq(cmd.valid & cmd.ready),
            If(sink.valid,
                If(~well,
                    leave.eq(1)
                ).Elif(refused != FABRIC_OK,
                    push.valid.eq(1),
                    push.status.eq(refused),
                    leave.eq(push.ready),
                ).Elif(is_copy,
                    # Taken by the copy engine (which answers it), or refused: never waits here.
                    copy.valid.eq(1),
                    If(copy.ready,
                        leave.eq(1)
                    ).Else(
                        push.valid.eq(1),
                        push.status.eq(FABRIC_BUSY),
                        leave.eq(push.ready),
                    )
                ).Else(
                    # The request is announced with its first access.
                    push.valid.eq(~started),
                    push.count.eq(count),
                    cmd.valid.eq(started | push.ready),
                    leave.eq(abort | (issued & (idx == (count - 1)))),
                )
            ),
            sink.ready.eq(leave),
        ]
        self.sync += [
            If(pushed & (push.count != 0), started.eq(1)),
            If(issued, idx.eq(idx + 1)),
            If(leave,
                started.eq(0),
                idx.eq(0),
                If(started | (pushed & (push.count != 0)), rid.eq(rid + 1)),
            ),
            If(sink.valid & sink.ready & ~well, malformed.eq(malformed + 1)),
        ]

        # Answers: the words of the oldest request, then its response.
        # --------------------------------------------------------------
        tx_valid = Signal()
        free     = Signal() # A response can be stored at the end of this cycle.
        k        = Signal(4) # Answers of the request received.
        good     = Signal(4) # Words done before the first error.
        failed   = Signal()  # An access of the request ended with an error.
        rdata    = [Signal(32, name=f"rdata{i}") for i in range(burst)]
        waiting  = Signal()  # Oldest request waiting for answers.
        mine     = Signal()  # Answer of the oldest request.
        ahead    = Signal(4)
        older    = Signal()
        newer    = Signal()
        take     = Signal()
        last     = Signal()
        respond  = Signal()
        status   = Signal(4)
        done     = Signal(4)
        self.timer = timer = WaitTimer(timeout) # Watchdog.
        self.comb += [
            tx.valid.eq(tx_valid),
            free.eq(~tx_valid | tx.ready),
            waiting.eq(head.valid & (head.count != 0)),
            ahead.eq(rsp.id - head.id),
            mine.eq(waiting & rsp.valid & (ahead == 0)),
            last.eq(k == (head.count - 1)),
            # The memory answers in order. An answer of an older request is of a request that
            # timed out: dropped. An answer of a newer request means that the missing answers of
            # the oldest request will not come: it ends now. (The answers of the next requests
            # wait for a refused request to be answered first.)
            older.eq(waiting & rsp.valid & ahead[3]),
            newer.eq(waiting & rsp.valid & (ahead != 0) & ~ahead[3]),
            rsp.ready.eq((mine & (~last | free)) | older),
            take.eq(mine & rsp.ready),
            timer.wait.eq(waiting & ~take & ~respond),
            status.eq(head.status),
            done.eq(good),
            If(head.valid & (head.count == 0),
                respond.eq(free)
            ).Elif(take & last,
                respond.eq(1),
                If(failed | rsp.err,
                    status.eq(FABRIC_BUS)
                ).Else(
                    done.eq(good + 1)
                )
            ).Elif(waiting & (timer.done | newer) & free,
                # An answer is missing: which words were done is not known.
                respond.eq(1),
                done.eq(0),
                status.eq(Mux(failed, FABRIC_BUS, FABRIC_TIMEOUT)),
                # The request may still be giving its accesses to the memory.
                abort.eq(started & (rid == head.id)),
            ),
            head.ready.eq(respond),
        ]
        self.sync += [
            If(tx.valid & tx.ready,
                tx_valid.eq(0)
            ),
            If(take,
                k.eq(k + 1),
                *[If(k == w, rdata[w].eq(rsp.dat_r)) for w in range(burst)],
                If(rsp.err,
                    failed.eq(1)
                ).Elif(~failed,
                    good.eq(good + 1)
                ),
            ),
            If(rsp.valid & rsp.ready & ~take, late.eq(late + 1)),
            If(respond,
                k.eq(0),
                good.eq(0),
                failed.eq(0),
                tx_valid.eq(1),
                tx.w0.eq(route_word(head.src, Constant(node_id, 8), max_hops, 1)),
                tx.w1.eq(Cat(head.tag, status, head.we, Constant(0, 1), head.burst, Constant(0, 1),
                    Mux(head.burst, done, 0))),
                *[packet_words(tx)[2 + w].eq(Mux(k == w, Mux(take, rsp.dat_r, 0), rdata[w])) for w in range(burst)],
                tx.len.eq(Mux(head.we, 2, Mux(head.burst, 2 + done, 3))),
                requests.eq(requests + 1),
            )
        ]

# Fabric Copy --------------------------------------------------------------------------------------

class FabricCopy(LiteXModule):
    """Runs the copies asked to this node: reads the words in the window of the node and writes
    them in the window of the destination node, by bursts, ``window`` bursts at a time.

    ``cmd``: copy to run (from the Responder; one at a time). ``sink``/``source``/``tag``: requests
    and results (``copy_sink``/``copy_source``/``tag`` of the Requester): the words are read with
    burst requests to this node itself and written with burst requests to the destination node.
    ``tx``: response to the node that asked the copy (status: first error, word 2: words written).

    After an error no more word is read; the words already read are not written. The bursts are
    not written in order: when the source and the destination overlap, the result is not defined.
    """
    def __init__(self, node_id, max_hops=1, window=4):
        assert window >= 1
        self.cmd    = cmd    = stream.Endpoint(copy_layout())
        self.sink   = sink   = stream.Endpoint(request_layout())
        self.source = source = stream.Endpoint(result_layout())
        self.tag    = tag    = Signal(8)
        self.tx     = tx     = stream.Endpoint(fabric_packet_layout())

        # Status.
        self.copies = copies = Signal(32) # Copies run.
        self.words  = words  = Signal(32) # Words written.
        self.errors = errors = Signal(32) # Copies ended by an error.

        # # #

        burst = FABRIC_BURST_WORDS

        # Copy running.
        # -------------
        busy    = Signal()
        src     = Signal(8)
        src_tag = Signal(8)
        dst     = Signal(8)
        rd_adr  = Signal(FABRIC_ADDRESS_BITS)
        wr_adr  = Signal(FABRIC_ADDRESS_BITS)
        left    = Signal(17) # Words still to read.
        count   = Signal(17) # Words written.
        status  = Signal(4)  # First error.
        failed  = Signal()
        size    = Signal(4)  # Words of the next burst.
        self.comb += [
            failed.eq(status != FABRIC_OK),
            size.eq(Mux(left < burst, left, burst)),
        ]

        # Bursts in flight: read asked (READ), read (WAIT), write asked (WRITE).
        # ------------------------------------------------------------------------
        IDLE, READ, WAIT, WRITE = range(4)
        states = [Signal(2,  name=f"burst{i}_state") for i in range(window)]
        tags   = [Signal(8,  name=f"burst{i}_tag")   for i in range(window)]
        adrs   = [Signal(FABRIC_ADDRESS_BITS, name=f"burst{i}_adr") for i in range(window)] # Where to write it.
        sizes  = [Signal(4,  name=f"burst{i}_size")  for i in range(window)]
        dats   = [Signal(32*burst, name=f"burst{i}_dat") for i in range(window)]

        self.free_encoder = free_encoder = PriorityEncoder(window)
        self.wait_encoder = wait_encoder = PriorityEncoder(window)
        idle = Signal()
        self.comb += [
            free_encoder.i.eq(Cat(*[s == IDLE for s in states])),
            wait_encoder.i.eq(Cat(*[s == WAIT for s in states])),
            idle.eq(reduce(and_, [s == IDLE for s in states])),
        ]

        # Requests: the bursts read first (they free the source), then the next read.
        do_write = Signal()
        do_read  = Signal()
        self.comb += [
            do_write.eq(busy & ~failed & ~wait_encoder.n),
            do_read.eq(busy & ~failed & ~do_write & (left != 0) & ~free_encoder.n),
            sink.valid.eq(do_write | do_read),
            sink.sel.eq(0b1111),
            sink.burst.eq(1),
            If(do_write,
                sink.we.eq(1),
                sink.dst.eq(dst),
                sink.adr.eq(Array(adrs)[wait_encoder.o]),
                sink.count.eq(Array(sizes)[wait_encoder.o]),
                sink.data.eq(Array(dats)[wait_encoder.o]),
            ).Else(
                sink.dst.eq(node_id),
                sink.adr.eq(rd_adr),
                sink.count.eq(size),
            ),
            source.ready.eq(1),
        ]
        sent = Signal()
        self.comb += sent.eq(sink.valid & sink.ready)
        for i in range(window):
            match = Signal(name=f"burst{i}_match")
            self.comb += match.eq(source.valid & (source.tag == tags[i]))
            self.sync += [
                If(sent & do_read & (free_encoder.o == i),
                    states[i].eq(READ),
                    tags[i].eq(tag),
                    adrs[i].eq(wr_adr),
                    sizes[i].eq(size),
                ).Elif(sent & do_write & (wait_encoder.o == i),
                    states[i].eq(WRITE),
                    tags[i].eq(tag),
                ).Elif((states[i] == READ) & match,
                    dats[i].eq(source.data),
                    states[i].eq(Mux(source.status == FABRIC_OK, WAIT, IDLE)),
                ).Elif((states[i] == WRITE) & match,
                    states[i].eq(IDLE),
                ).Elif((states[i] == WAIT) & failed,
                    states[i].eq(IDLE),
                )
            ]
        results = Signal() # Result of a request of the copy.
        written = Signal() # Result of a write of the copy.
        self.comb += [
            results.eq(source.valid & reduce(or_, [
                ((s == READ) | (s == WRITE)) & (source.tag == t) for s, t in zip(states, tags)])),
            written.eq(source.valid & reduce(or_, [
                (s == WRITE) & (source.tag == t) for s, t in zip(states, tags)])),
        ]

        # Response: one packet waiting to be taken by the switch.
        # --------------------------------------------------------
        self.comb += cmd.ready.eq(~busy)
        self.sync += [
            If(tx.valid & tx.ready,
                tx.valid.eq(0),
                busy.eq(0),
            ),
            If(cmd.valid & cmd.ready,
                busy.eq(1),
                src.eq(cmd.src),
                src_tag.eq(cmd.tag),
                dst.eq(cmd.dst),
                rd_adr.eq(cmd.adr),
                wr_adr.eq(cmd.dst_adr),
                left.eq(cmd.words),
                count.eq(0),
                status.eq(FABRIC_OK),
                copies.eq(copies + 1),
            ),
            If(sent & do_read,
                rd_adr.eq(rd_adr + size),
                wr_adr.eq(wr_adr + size),
                left.eq(left - size),
            ),
            If(results & (source.status != FABRIC_OK) & ~failed,
                status.eq(source.status),
            ),
            # Words written by a burst: all of them, or those before its first error.
            If(written,
                count.eq(count + source.count),
                words.eq(words + source.count),
            ),
            # End: nothing left to read (or an error) and no burst in flight.
            If(busy & ~tx.valid & idle & ((left == 0) | failed) & ~(cmd.valid & cmd.ready),
                tx.valid.eq(1),
                tx.w0.eq(route_word(src, Constant(node_id, 8), max_hops, 1)),
                tx.w1.eq(Cat(src_tag, status, Constant(0, 1))),
                packet_words(tx)[2].eq(count),
                tx.len.eq(3),
                If(failed, errors.eq(errors + 1)),
            ),
        ]

# Fabric Port --------------------------------------------------------------------------------------

class FabricPort(LiteXModule):
    """Link to a neighbor: packet stream <-> whole packets, receive queues per class, credits,
    retransmission.

    ``sink``/``source``: packet stream from/to the neighbor. ``queues[c]``: packets received in
    class c (c = response*max_hops + links crossed - 1). ``tx``: packet to send in class
    ``tx_class`` (accepted when the port has room; the switch checks ``credit[tx_class]`` first).
    ``ctl``/``rt``: route packets to send (between two packets, without credit) and received.
    ``retry_timeout``: cycles without acknowledgment before the packets not acknowledged are sent
    again (default: ``fc_period``). ``link_width``: bits of the packet stream per cycle (32, 64 or
    128: 1, 2 or 4 words; words in the order of the packet, first word in the low bits).
    """
    def __init__(self, max_hops=1, queue_depth=4, fc_period=256, retry_timeout=None, link_width=32,
        link_ready=None):
        assert 1 <= max_hops <= 6 and 1 <= queue_depth <= 8 and fc_period >= 16
        assert link_width in [32, 64, 128]
        retry_timeout = fc_period if retry_timeout is None else retry_timeout
        self.link_width = link_width
        self.classes  = classes = 2*max_hops
        self.sink     = sink    = stream.Endpoint(packet_description(link_width))
        self.source   = source  = stream.Endpoint(packet_description(link_width))
        self.queues   = queues  = []
        self.tx       = tx      = stream.Endpoint(fabric_packet_layout())
        self.tx_class = Signal(max=classes)
        self.ctl      = ctl     = stream.Endpoint(fabric_packet_layout())
        self.rt       = rt      = stream.Endpoint(fabric_packet_layout())
        self.credit   = credit  = Signal(classes)
        self.up       = up      = Signal(reset=1)

        # Status.
        self.rx_invalid = rx_invalid = Signal(32) # Packets dropped (not a fabric packet, malformed).
        self.rx_drops   = rx_drops   = Signal(32) # Packets dropped (queue full: credits not respected).
        self.tx_drops   = tx_drops   = Signal(32) # Packets dropped (link down).
        self.retransmits = retransmits = Signal(32) # Times packets were sent again.
        self.rx_discards = rx_discards = Signal(32) # Packets discarded (not the one expected).
        self.tx_packets  = tx_packets  = Signal(32) # Packets accepted to be sent (sent once each).

        # # #

        if link_ready is not None:
            self.comb += up.eq(link_ready)

        # RX: Packet stream to whole packets.
        # -----------------------------------
        fc_len    = 1 + (classes + 3)//4
        pw        = FABRIC_PACKET_WORDS
        len_bits  = bits_for(pw)
        wpb       = link_width//32          # Words per cycle.
        max_beats = (pw + wpb - 1)//wpb     # Cycles of the longest packet.
        def beats_of(length):
            return ((length + (wpb - 1)) >> log2_int(wpb))[:len_bits]

        rx       = stream.Endpoint(fabric_packet_layout()) # Whole packet, one cycle.
        rx_beats = [Signal(link_width, name=f"rx_beat{i}") for i in range(max_beats)]
        rx_beat  = Signal(max=max_beats + 2) # Cycles of the current packet already received (saturating).
        rx_bad   = Signal()  # Packet cut, or not the number of words of its kind.
        rx_len   = Signal(6)
        rx_bits  = Signal(32*pw)
        self.comb += sink.ready.eq(1) # The link has no backpressure.
        # The words of cycle i of the packet: from the link in this cycle, kept afterwards.
        self.comb += [
            rx_bits.eq(Cat(*[Mux(rx_beat == i, sink.data, rx_beats[i]) for i in range(max_beats)])),
            rx.w0.eq(rx_bits[0:32]),
            rx.w1.eq(rx_bits[32:64]),
            rx.body.eq(rx_bits[64:]),
        ]
        self.sync += If(sink.valid, *[If(rx_beat == i, rx_beats[i].eq(sink.data)) for i in range(max_beats)])
        self.comb += [
            rx_len.eq(fabric_packet_len(rx.w0, rx.w1, fc_len)),
            rx.len.eq(rx_len),
            rx_bad.eq(sink.error | (rx_len > pw) | ((rx_beat + 1) != beats_of(rx.len))),
            rx.valid.eq(sink.valid & sink.last & ~rx_bad & up),
        ]
        self.sync += [
            If(sink.valid,
                If(sink.last,
                    rx_beat.eq(0)
                ).Elif(rx_beat != (max_beats + 1),
                    rx_beat.eq(rx_beat + 1)
                )
            ),
            # Link down: a packet cut by the link loss is not continued by the next one.
            If(~up, rx_beat.eq(0)),
        ]

        # RX: Class of the packet.
        # ------------------------
        rx_fc    = Signal()
        rx_rt    = Signal()
        rx_data  = Signal()
        rx_ok    = Signal()
        rx_class = Signal(max=classes)
        rx_ttl   = Signal(3)
        self.comb += [
            rx_ttl.eq(route_ttl(rx.w0)),
            rx_ok.eq(rx.valid & (route_magic(rx.w0) == FABRIC_MAGIC)),
            rx_fc.eq(rx_ok & route_fc(rx.w0)),
            rx_rt.eq(rx_ok & ~route_fc(rx.w0) & route_rt(rx.w0)),
            rx.connect(rt, omit={"valid", "ready"}),
            rt.valid.eq(rx_rt),
            # Hop limit of a received packet: max_hops (first link) to 1 (last link allowed).
            rx_data.eq(rx_ok & ~route_fc(rx.w0) & ~route_rt(rx.w0) & (rx_ttl >= 1) & (rx_ttl <= max_hops)),
            rx_class.eq(Mux(route_rsp(rx.w0), max_hops, 0) + (max_hops - rx_ttl)),
        ]
        self.sync += [
            If((sink.valid & sink.last & up & rx_bad) | (rx.valid & ~rx_fc & ~rx_rt & ~rx_data),
                If(rx_invalid != (2**32 - 1), rx_invalid.eq(rx_invalid + 1))
            )
        ]

        # RX: Sequence. Only the packet expected is accepted.
        # ---------------------------------------------------
        self.rx_expected = rx_expected = Signal(5) # Sequence number of the next packet to receive.
        rx_ahead    = Signal(5)
        rx_in_seq   = Signal()
        rx_future   = Signal() # A packet before this one was lost.
        rx_past     = Signal() # Already received (sent again).
        rx_accepted = Signal()
        self.comb += [
            rx_ahead.eq(route_seq(rx.w0) - rx_expected),
            rx_in_seq.eq(rx_data & (rx_ahead == 0)),
            rx_future.eq(rx_data & (rx_ahead != 0) & ~rx_ahead[4]),
            rx_past.eq(rx_data & rx_ahead[4]),
        ]

        # RX: Queues and receiver side of the credits (R: packets received, L: R + free places).
        # ---------------------------------------------------------------------------------------
        # Counters are 4-bit (modulo 16, queue_depth <= 8): a flow control word carries 4 classes.
        rx_r  = [Signal(4, name=f"rx_r{c}") for c in range(classes)]
        rx_l  = [Signal(4, name=f"rx_l{c}") for c in range(classes)]
        dirty = Signal() # A place was freed since the last flow control packet.
        freed = Signal()
        stores  = []
        refused = []
        for c in range(classes):
            fifo = ResetInserter()(stream.SyncFIFO(fabric_packet_layout(), queue_depth, buffered=True))
            self.add_module(f"queue{c}", fifo)
            queues.append(fifo.source)
            store = Signal(name=f"rx_store{c}")
            stores.append(store & fifo.sink.ready)
            refused.append(store & ~fifo.sink.ready)
            self.comb += [
                fifo.reset.eq(~up),
                store.eq(rx_in_seq & (rx_class == c)),
                rx.connect(fifo.sink, omit={"valid", "ready"}),
                fifo.sink.valid.eq(store),
                rx_l[c].eq(rx_r[c] + (queue_depth - fifo.level)),
                If(fifo.source.valid & fifo.source.ready, freed.eq(1)),
            ]
            self.sync += [
                If(store & fifo.sink.ready, rx_r[c].eq(rx_r[c] + 1)),
                If(~up, rx_r[c].eq(0)),
            ]
        # A packet that does not fit (credits not respected) is not accepted: it is sent again.
        self.comb += rx_accepted.eq(Cat(*stores) != 0)
        self.sync += [
            If(Cat(*refused) != 0, If(rx_drops != (2**32 - 1), rx_drops.eq(rx_drops + 1))),
            If(rx_future | rx_past, If(rx_discards != (2**32 - 1), rx_discards.eq(rx_discards + 1))),
        ]

        # RX: Acknowledgments, sent in the flow control packets.
        # -------------------------------------------------------
        fc_sent    = Signal()
        ack_due    = Signal()                        # Packets received since the last one.
        ack_count  = Signal(max=FABRIC_WINDOW + 1)
        nack       = Signal()                        # Gap seen: ask to send again from rx_expected.
        nack_done  = Signal()                        # Asked for this gap.
        self.sync += [
            If(fc_sent,
                ack_due.eq(0),
                ack_count.eq(0),
                nack.eq(0),
            ),
            If(rx_accepted,
                rx_expected.eq(rx_expected + 1),
                nack_done.eq(0),
                ack_due.eq(1),
                If(ack_count != FABRIC_WINDOW, ack_count.eq(ack_count + 1)),
            ),
            If(rx_past,
                ack_due.eq(1) # Its acknowledgment was lost.
            ),
            If(rx_future & ~nack_done,
                nack.eq(1),
                nack_done.eq(1),
            ),
            If(~up,
                rx_expected.eq(0),
                ack_due.eq(0),
                ack_count.eq(0),
                nack.eq(0),
                nack_done.eq(0),
            )
        ]

        # TX: Sender side of the credits (S: packets sent, L: last value from the neighbor).
        # -----------------------------------------------------------------------------------
        tx_s     = [Signal(4, name=f"tx_s{c}") for c in range(classes)]
        tx_l     = [Signal(4, reset=queue_depth, name=f"tx_l{c}") for c in range(classes)]
        fc_ok    = Signal() # Flow control packet received.
        sent     = Signal()
        self.comb += [
            sent.eq(tx.valid & tx.ready & up),
            fc_ok.eq(rx_fc),
        ]
        self.sync += If(sent, tx_packets.eq(tx_packets + 1))
        for c in range(classes):
            room = Signal(4, name=f"tx_room{c}")
            # Flow control word: classes 4n to 4n+3, {L, R} each.
            fc_word = [rx.w1, packet_words(rx)[2], packet_words(rx)[3]][c//4]
            fc_l    = fc_word[8*(c % 4) + 4:8*(c % 4) + 8]
            self.comb += [
                room.eq(tx_l[c] - tx_s[c]),
                credit[c].eq((room != 0) & (room <= queue_depth)),
            ]
            # Packets are sent again until they are received: a packet sent is never lost while
            # the link is up, and the packets sent (S) need no correction.
            self.sync += [
                If(sent & (self.tx_class == c), tx_s[c].eq(tx_s[c] + 1)),
                If(fc_ok, tx_l[c].eq(fc_l)),
                If(~up,
                    tx_s[c].eq(0),
                    tx_l[c].eq(queue_depth),
                )
            ]

        # TX: Packets sent, kept until acknowledged.
        # -------------------------------------------
        # Sequence numbers: tx_acked (oldest not acknowledged) <= tx_send (next to send) <= tx_tail
        # (next to number), modulo 32.
        window   = FABRIC_WINDOW
        slot     = bits_for(window - 1)
        self.tx_acked = tx_acked = Signal(5)
        self.tx_send  = tx_send  = Signal(5)
        self.tx_tail  = tx_tail  = Signal(5)
        tx_cur   = Signal(5) # Packet being sent.
        kept     = Signal(5)
        buf      = [Signal(32*pw, name=f"tx_buf{i}") for i in range(window)]
        buf_len  = [Signal(len_bits, name=f"tx_buf{i}_len") for i in range(window)]
        self.comb += [
            kept.eq(tx_tail - tx_acked),
            # Link down: packets are dropped instead of stalling the node.
            tx.ready.eq((kept < window) | ~up),
        ]
        for i in range(window):
            self.sync += If(sent & (tx_tail[:slot] == i),
                buf[i].eq(Cat(tx.w0[:19], tx_tail, tx.w0[24:], tx.w1, tx.body)),
                buf_len[i].eq(tx.len),
            )
        self.sync += If(tx.valid & tx.ready & ~up, If(tx_drops != (2**32 - 1), tx_drops.eq(tx_drops + 1)))

        # Acknowledgments from the neighbor.
        ack       = Signal(5)
        ack_delta = Signal(5)
        ack_ok    = Signal()
        progress  = Signal()
        acked_n   = Signal(5)
        self.comb += [
            ack.eq(rx.w0[0:5]),
            ack_delta.eq(ack - tx_acked),
            ack_ok.eq(fc_ok & (ack_delta <= kept)),
            progress.eq(ack_ok & (ack_delta != 0)),
            acked_n.eq(Mux(progress, ack, tx_acked)),
        ]

        # Sending again: asked by the neighbor, or nothing acknowledged for too long.
        self.retry_timer = retry_timer = WaitTimer(retry_timeout)
        rewind       = Signal() # To do when no packet is being sent.
        rewind_apply = Signal()
        packet_done  = Signal()
        send_n       = Signal(5)
        behind       = Signal(5)
        self.comb += [
            retry_timer.wait.eq((tx_acked != tx_tail) & ~progress & ~rewind_apply),
            behind.eq(send_n - acked_n),
        ]
        self.sync += [
            tx_acked.eq(acked_n),
            # A packet acknowledged while it was being sent again is not sent further.
            tx_send.eq(Mux(behind[4], acked_n, send_n)),
            If(sent, tx_tail.eq(tx_tail + 1)),
            If((ack_ok & rx.w0[5]) | retry_timer.done,
                rewind.eq(1)
            ),
            If(rewind_apply,
                rewind.eq((ack_ok & rx.w0[5])),
                If(retransmits != (2**32 - 1), retransmits.eq(retransmits + 1)),
            ),
            If(~up,
                tx_acked.eq(0),
                tx_send.eq(0),
                tx_tail.eq(0),
                rewind.eq(0),
            )
        ]

        # TX: Packets, flow control packets and route packets to the packet stream.
        # --------------------------------------------------------------------------
        available = Signal() # A packet to send.
        self.comb += available.eq(tx_send != tx_tail)

        fc_words = [Signal(32, name=f"fc_w{i}") for i in range(4)]
        self.comb += fc_words[0].eq((FABRIC_MAGIC << 28) | (1 << 25) | (nack << 5) | rx_expected)
        for c in range(classes):
            self.comb += [
                fc_words[1 + c//4][8*(c % 4) + 0:8*(c % 4) + 4].eq(rx_r[c]),
                fc_words[1 + c//4][8*(c % 4) + 4:8*(c % 4) + 8].eq(rx_l[c]),
            ]
        self.fc_timer    = fc_timer    = WaitTimer(fc_period)    # Periodic flow control packet.
        self.dirty_timer = dirty_timer = WaitTimer(fc_period//8) # Something to tell: sooner.
        fc_request = Signal()
        # Places freed in a class for which the neighbor was told it has none left: it is waiting.
        rx_l_sent  = [Signal(4, reset=queue_depth, name=f"rx_l_sent{c}") for c in range(classes)]
        starving   = Signal()
        self.comb += starving.eq(Cat(*[(rx_l_sent[c] == rx_r[c]) & (rx_l[c] != rx_l_sent[c]) for c in range(classes)]) != 0)
        self.sync += [
            If(fc_sent, *[rx_l_sent[c].eq(rx_l[c]) for c in range(classes)]),
            If(~up,     *[rx_l_sent[c].eq(queue_depth) for c in range(classes)]),
        ]
        self.comb += [
            fc_timer.wait.eq(~fc_sent),
            dirty_timer.wait.eq((dirty | ack_due) & ~fc_sent),
            # At once: neighbor waiting for credits, packets to send again, half of the packets the
            # neighbor may keep received. Otherwise when the link is free, or after a while.
            fc_request.eq(fc_timer.done | starving | nack | (ack_count >= window//2) |
                ((dirty | ack_due) & (~available | dirty_timer.done))),
        ]
        self.sync += [
            If(fc_sent,
                dirty.eq(freed) # Places freed in this cycle are not in the values sent.
            ).Elif(freed,
                dirty.eq(1)
            ),
            If(~up, dirty.eq(0)),
        ]

        index      = Signal(max=max(2, max_beats)) # Cycle of the packet being sent.
        cur_slot   = Signal(slot)
        cur_len    = Signal(len_bits)
        send_slot  = Signal(slot)
        send_len   = Signal(len_bits)
        sending    = Signal(5) # Sequence number of the packet whose last cycle is sent.
        fc_latched = Signal(32*4)
        self.sync += If(fc_sent, fc_latched.eq(Cat(*fc_words)))
        self.comb += [
            send_slot.eq(tx_send[:slot]),
            send_len.eq(Array(buf_len)[send_slot]),
            cur_slot.eq(tx_cur[:slot]),
            cur_len.eq(Array(buf_len)[cur_slot]),
            send_n.eq(Mux(rewind_apply, acked_n, Mux(packet_done, sending + 1, tx_send))),
        ]

        def beat(bits, n):
            """Words of the cycle n of a packet (bits: all its words)."""
            beats = [bits[b*link_width:(b + 1)*link_width] for b in range(max_beats) if b*link_width < len(bits)]
            beats += [Constant(0, link_width)]*(max_beats - len(beats))
            return beats[n] if isinstance(n, int) else Array(beats)[n]

        def send(words, length, done):
            """Statements sending the cycle `index` of a packet."""
            return [
                source.valid.eq(1),
                source.length.eq(Cat(Constant(0, 2), length)),
                source.data.eq(beat(words, index)),
                source.last.eq(index == (beats_of(length) - 1)),
                If(source.ready,
                    NextValue(index, index + 1),
                    If(source.last, *done)
                )
            ]

        send_words = Signal(32*pw)
        cur_words  = Signal(32*pw)
        self.comb += [
            send_words.eq(Array(buf)[send_slot]),
            cur_words.eq(Array(buf)[cur_slot]),
        ]
        self.tx_fsm = tx_fsm = ResetInserter()(FSM(reset_state="IDLE"))
        self.comb += tx_fsm.reset.eq(~up)
        ctl_words = Signal(32*pw)
        ctl_len   = Signal(len_bits)
        tx_fsm.act("IDLE",
            NextValue(index, 0),
            sending.eq(tx_send),
            If(fc_request,
                fc_sent.eq(1),
                NextState("FLOW-CONTROL")
            ).Elif(rewind,
                rewind_apply.eq(1)
            ).Elif(ctl.valid,
                ctl.ready.eq(1),
                NextValue(ctl_words, packet_bits(ctl)),
                NextValue(ctl_len,      ctl.len),
                NextState("CONTROL")
            ).Elif(available,
                # First cycle of the packet: once presented, the packet is sent before anything
                # else. A packet of one cycle is followed at once by the next one.
                source.valid.eq(1),
                source.length.eq(Cat(Constant(0, 2), send_len)),
                source.data.eq(beat(send_words, 0)),
                source.last.eq(beats_of(send_len) == 1),
                NextValue(tx_cur, tx_send),
                If(source.ready & source.last,
                    packet_done.eq(1)
                ).Else(
                    If(source.ready, NextValue(index, 1)),
                    NextState("PACKET")
                )
            )
        )
        tx_fsm.act("CONTROL",
            *send(ctl_words, ctl_len, [NextState("IDLE")])
        )
        tx_fsm.act("FLOW-CONTROL",
            *send(fc_latched, Constant(fc_len, len_bits), [NextState("IDLE")])
        )
        tx_fsm.act("PACKET",
            sending.eq(tx_cur),
            *send(cur_words, cur_len, [packet_done.eq(1), NextState("IDLE")])
        )

# Fabric Node --------------------------------------------------------------------------------------

class FabricNode(LiteXModule):
    """Node of the fabric: Requester, Responder and switch between ``ports`` links.

    ``ports[p].sink``/``ports[p].source``: packets from/to the neighbor of port p.
    ``requester.sink``/``requester.source``: requests and results. ``responder.mem``: memory window
    of this node (Wishbone), or with ``pipelined`` ``responder.cmd``/``responder.rsp`` (memory port, see
    exiom/memory.py): the Responder then sends an access per cycle. ``routes``: {destination node: [cost of each port]} (see ``Topology.costs()``;
    None: point to point, everything to port 0). ``max_hops``: links a packet may cross (same on
    all the nodes): longest shortest path, plus the detours allowed around links that are down.
    """
    def __init__(self, node_id, size, ports=1, routes=None, max_hops=1, requests=8, read_only=False,
        pipelined       = False,
        request_timeout = 4096,
        copy_timeout    = None,
        copy_window     = 4,
        memory_timeout  = 1024,
        buffer_depth    = 4,
        queue_depth     = 4,
        fc_period       = 256,
        route_period    = None,
        route_entries   = ROUTE_ENTRIES,
        retry_timeout   = None,
        link_width      = 32,
        link_ready      = None,
    ):
        assert 0 <= node_id < 256 and ports >= 1
        if link_ready is None:
            link_ready = [None]*ports
        if not isinstance(link_ready, (list, tuple)):
            link_ready = [link_ready]
        assert len(link_ready) == ports
        self.node_id  = node_id
        self.max_hops = max_hops

        # Status.
        self.forwarded   = forwarded   = Signal(32) # Packets of other nodes forwarded.
        self.unreachable = unreachable = Signal(32) # Requests answered with FABRIC_NODE here.
        self.rsp_drops   = rsp_drops   = Signal(32) # Responses dropped (cannot go further).

        # # #

        port_bits = max(1, bits_for(ports - 1))
        classes   = 2*max_hops

        self.requester = requester = FabricRequester(node_id,
            ports    = ports,
            max_hops = max_hops,
            requests     = requests,
            timeout      = request_timeout,
            copy_timeout = copy_timeout,
        )
        # Memory of the node: Wishbone bus (``responder.mem``), or memory port accessed without
        # waiting for the answers (``pipelined``: ``responder.cmd``/``responder.rsp``).
        self.responder = responder = (FabricPipelinedResponder if pipelined else FabricResponder)(node_id,
            size         = size,
            max_hops     = max_hops,
            read_only    = read_only,
            timeout      = memory_timeout,
            buffer_depth = buffer_depth,
        )
        # Copies asked to this node: taken from the Responder, run with requests of the Requester.
        self.copy = copy = FabricCopy(node_id, max_hops=max_hops, window=min(copy_window, requests))
        self.comb += [
            responder.copy.connect(copy.cmd),
            copy.sink.connect(requester.copy_sink),
            copy.tag.eq(requester.tag),
            requester.copy_source.connect(copy.source),
        ]
        self.ports = fports = []
        for p in range(ports):
            port = FabricPort(max_hops=max_hops, queue_depth=queue_depth, fc_period=fc_period,
                retry_timeout=retry_timeout, link_width=link_width, link_ready=link_ready[p])
            self.add_module(f"port{p}", port)
            fports.append(port)
            self.comb += requester.port_up[p].eq(port.up)

        # Routes.
        # -------
        self.routes = table = FabricRoutes(node_id, routes, ports,
            up      = [port.up for port in fports],
            entries = route_entries,
            period  = fc_period if route_period is None else route_period,
        )
        for p, port in enumerate(fports):
            self.comb += [
                port.rt.connect(table.rx[p]),
                table.tx[p].connect(port.ctl),
            ]
        self.comb += table.reachable(requester.dst, requester.known)

        # Switch.
        # -------
        # Inputs: packets injected by this node (Responder, Requester, responses of the copies) and
        # the queues of the ports.
        # Outputs: the ports, the Requester (responses) and the Responder (requests).
        #
        # Each group of inputs (the Responder, the Requester, each port) proposes one packet per
        # cycle: the next of its inputs that has a packet, in turn. Each output takes one of the
        # packets proposed to it, in turn. So every port can receive a packet and send a packet in
        # the same cycle, a packet that has to wait never blocks the packets of the other inputs,
        # and the packets of an input keep their order.
        def in_turn(requests, name):
            """Index of the next request set from a turn that follows the last one served, and
            statements moving the turn."""
            count = len(requests)
            sel   = Signal(max=max(2, count), name=f"{name}_sel")
            some  = Signal(name=f"{name}_some")
            if count == 1:
                self.comb += [sel.eq(0), some.eq(requests[0])]
                return sel, some, lambda served: []
            turn  = Signal(max=count, name=f"{name}_turn")
            vec   = Signal(count,     name=f"{name}_requests")
            ahead = PriorityEncoder(count)
            every = PriorityEncoder(count)
            self.submodules += ahead, every
            self.comb += [
                vec.eq(Cat(*requests)),
                ahead.i.eq(vec & ~((1 << turn) - 1)),
                every.i.eq(vec),
                sel.eq(Mux(ahead.n, every.o, ahead.o)),
                some.eq(vec != 0),
            ]
            return sel, some, lambda served: [If(served, If(sel == (count - 1), turn.eq(0)).Else(turn.eq(sel + 1)))]

        groups = [] # Inputs: (endpoint, port it came from (None: injected), links crossed).
        groups.append([(responder.tx, None, 0)])
        groups.append([(requester.tx, None, 0)])
        groups.append([(copy.tx,      None, 0)])
        for p, port in enumerate(fports):
            groups.append([(queue, p, (c % max_hops) + 1) for c, queue in enumerate(port.queues)])

        any_up = Signal()
        self.comb += any_up.eq(Cat(*[port.up for port in fports]) != 0)

        # Packet proposed by each group, and the output it asks for.
        proposals = [] # Per group: (packet, word 0 to send, class to send in).
        to_ports      = [[] for _ in fports] # Per port: request of each group.
        to_requesters = []
        to_responders = []
        granted       = []
        frees         = []
        for g, inputs in enumerate(groups):
            sel, some, move = in_turn([endpoint.valid for endpoint, _, _ in inputs], f"group{g}")
            # The turn moves on at each cycle with a packet: a packet that cannot leave lets the
            # next input of the group propose its own.
            self.sync += move(some)

            cur          = stream.Endpoint(fabric_packet_layout())
            cur_injected = Signal(name=f"group{g}_injected")
            cur_port     = Signal(port_bits, name=f"group{g}_port")
            cur_crossed  = Signal(max=max_hops + 1, name=f"group{g}_crossed")
            cases = {}
            for i, (endpoint, from_port, crossed) in enumerate(inputs):
                cases[i] = [
                    endpoint.connect(cur),
                    cur_injected.eq(int(from_port is None)),
                    cur_port.eq(0 if from_port is None else from_port),
                    cur_crossed.eq(crossed),
                ]
            self.comb += Case(sel, cases)

            # Route.
            dst         = Signal(8, name=f"group{g}_dst")
            rsp         = Signal(name=f"group{g}_rsp")
            local       = Signal(name=f"group{g}_local")
            route_valid = Signal(name=f"group{g}_route_valid")
            route_port  = Signal(port_bits, name=f"group{g}_route_port")
            budget      = Signal(3, name=f"group{g}_budget")
            forward     = Signal(name=f"group{g}_forward")
            out_class   = Signal(max=classes, name=f"group{g}_class")
            out_ready   = Signal(name=f"group{g}_out_ready")
            out_credit  = Signal(name=f"group{g}_out_credit")
            out_w0      = Signal(32, name=f"group{g}_w0")
            self.comb += [
                dst.eq(route_dst(cur.w0)),
                rsp.eq(route_rsp(cur.w0)),
                local.eq(dst == node_id),
                budget.eq(max_hops - cur_crossed),
                *table.select(dst,
                    exclude      = ~cur_injected,
                    exclude_port = cur_port,
                    budget       = budget,
                    valid        = route_valid,
                    port         = route_port,
                ),
                out_class.eq(Mux(rsp, max_hops, 0) + cur_crossed),
                out_ready.eq(Array([port.tx.ready for port in fports])[route_port]),
                out_credit.eq((Array([port.credit for port in fports])[route_port] >> out_class)[0]),
                # A packet goes on when a port can take it towards its destination.
                forward.eq(~local & route_valid),
                out_w0.eq(cur.w0),
                # Hop limit: decremented when forwarded.
                If(~cur_injected, out_w0[16:19].eq(route_ttl(cur.w0) - 1)),
            ]

            # Output asked.
            to_port      = Signal(name=f"group{g}_to_port")
            to_requester = Signal(name=f"group{g}_to_requester")
            to_responder = Signal(name=f"group{g}_to_responder")
            to_drop      = Signal(name=f"group{g}_to_drop")
            to_discard   = Signal(name=f"group{g}_to_discard")
            grant        = Signal(name=f"group{g}_grant")
            self.comb += [
                If(cur.valid,
                    If(forward,
                        to_port.eq(out_ready & out_credit)
                    ).Elif(rsp,
                        # Response for this node, or that cannot go further (dropped).
                        to_requester.eq(local),
                        to_drop.eq(~local),
                    ).Elif(cur_injected & ~local & ~any_up,
                        # Request of this node whose last link went down while it waited: its
                        # Requester already ended it with FABRIC_LINK.
                        to_discard.eq(1)
                    ).Else(
                        # Request for this node, or request of another node that cannot go further
                        # (answered with FABRIC_NODE).
                        to_responder.eq(responder.rx.ready)
                    )
                ),
                cur.ready.eq(grant | to_drop | to_discard),
            ]
            for p in range(ports):
                request = Signal(name=f"group{g}_to_port{p}")
                self.comb += request.eq(to_port & (route_port == p))
                to_ports[p].append(request)
            to_requesters.append(to_requester)
            to_responders.append(to_responder)
            proposals.append((cur, out_w0, out_class))
            granted.append(grant)
            frees.append((to_drop, cur_injected, local))

        # Packet taken by each output.
        grants = [[] for _ in groups] # Per group: grant of each output.
        def output(requests, name):
            sel, some, move = in_turn(requests, name)
            self.sync += move(some)
            for g in range(len(groups)):
                grants[g].append(some & (sel == g))
            return sel, some

        for p, port in enumerate(fports):
            sel, some = output(to_ports[p], f"port{p}_tx")
            self.comb += [
                port.tx.valid.eq(some),
                Case(sel, {g: [
                    cur.connect(port.tx, omit={"valid", "ready", "w0"}),
                    port.tx.w0.eq(out_w0),
                    port.tx_class.eq(out_class),
                ] for g, (cur, out_w0, out_class) in enumerate(proposals)}),
            ]
        sel, some = output(to_requesters, "requester_rx")
        self.comb += [
            requester.rx.valid.eq(some),
            Case(sel, {g: cur.connect(requester.rx, omit={"valid", "ready"}) for g, (cur, _, _) in enumerate(proposals)}),
        ]
        sel, some = output(to_responders, "responder_rx")
        self.comb += [
            responder.rx.valid.eq(some),
            Case(sel, {g: cur.connect(responder.rx, omit={"valid", "ready"}) for g, (cur, _, _) in enumerate(proposals)}),
        ]
        for g in range(len(groups)):
            self.comb += granted[g].eq(Cat(*grants[g]) != 0)

        # Counters (several packets can move in a cycle).
        def count(bits):
            return sum(Cat(bit, Constant(0, 3)) for bit in bits)
        port_grants      = [Cat(*grants[g][:ports]) != 0 for g in range(len(groups))]
        responder_grants = [grants[g][ports + 1] for g in range(len(groups))]
        self.sync += [
            forwarded.eq(forwarded + count([port_grants[g] & ~frees[g][1] for g in range(len(groups))])),
            unreachable.eq(unreachable + count([responder_grants[g] & ~frees[g][2] for g in range(len(groups))])),
            rsp_drops.eq(rsp_drops + count([frees[g][0] for g in range(len(groups))])),
        ]

    # Point to point compatibility (port 0).
    @property
    def sink(self):
        return self.ports[0].sink

    @property
    def source(self):
        return self.ports[0].source

    def connect_serwb(self, core, port=0, serwb_port=FABRIC_PORT):
        """Use a port of a SERWBCore as link of port ``port`` (before the core is finalized)."""
        assert self.ports[port].link_width == 32, "SerWB links carry 32 bits per cycle."
        core.add_downstream_endpoint(port=serwb_port, endpoint=self.ports[port].source)
        core.add_upstream_endpoint(  port=serwb_port, endpoint=self.ports[port].sink)

# Fabric Wishbone ----------------------------------------------------------------------------------

class FabricWishbone(LiteXModule):
    """Wishbone Slave giving access to the window of one remote node (one access at a time).

    Uses the Requester of ``node`` (not shared with other request sources). ``origin``: byte
    address of the window on the bus. ``status``: status of the last access (FABRIC_*).
    """
    def __init__(self, node, dst, size, origin=0):
        assert size > 0 and (size % 4) == 0 and (origin % 4) == 0
        self.bus    = bus    = wishbone.Interface(data_width=32, adr_width=30)
        self.status = status = Signal(4)
        self.errors = errors = Signal(32)

        # # #

        requester = node.requester
        tag       = Signal(8)
        offset    = Signal(30)
        self.comb += [
            offset.eq(bus.adr - (origin//4)),
            requester.sink.we.eq(bus.we),
            requester.sink.dst.eq(dst),
            requester.sink.adr.eq(offset),
            requester.sink.dat_w.eq(bus.dat_w),
            requester.sink.sel.eq(bus.sel),
            bus.dat_r.eq(requester.source.dat_r),
        ]

        def error(code):
            return [
                bus.err.eq(1),
                NextValue(status, code),
                If(errors != (2**32 - 1), NextValue(errors, errors + 1)),
                NextState("IDLE"),
            ]

        self.fsm = fsm = FSM(reset_state="IDLE")
        fsm.act("IDLE",
            If(bus.cyc & bus.stb,
                If(offset >= (size//4),
                    *error(FABRIC_RANGE)
                ).Else(
                    requester.sink.valid.eq(1),
                    If(requester.sink.ready,
                        NextValue(tag, requester.tag),
                        NextState("RESULT")
                    )
                )
            )
        )
        fsm.act("RESULT",
            # Results of other requests (none expected) are dropped.
            requester.source.ready.eq(1),
            If(requester.source.valid & (requester.source.tag == tag),
                If(requester.source.status == FABRIC_OK,
                    bus.ack.eq(1),
                    NextValue(status, FABRIC_OK),
                    NextState("IDLE")
                ).Else(
                    *error(requester.source.status)
                )
            )
        )
