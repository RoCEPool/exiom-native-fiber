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
                         [12] write, [13] copy, [21:14] node to copy to, [31:22] words to copy - 1.
- Word 2               : word address in the destination window (request), read data or words
                         copied (response).
- Word 3               : write data (write request) or word address to copy to (copy request).

Copy request: a write request with the copy bit, answered like a read (words copied in word 2).
The nodes on the way forward it like any request.

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

FABRIC_COPY_WORDS = 1024 # Words per copy request, at most.

def request_layout():
    # Copy (``copy``): ``words`` words from word ``adr`` of node ``dst`` to word ``dat_w`` of node
    # ``copy_dst``. The result gives the number of words written in ``dat_r``.
    return [("we", 1), ("dst", 8), ("adr", 32), ("dat_w", 32), ("sel", 4),
        ("copy", 1), ("copy_dst", 8), ("words", 11)]

def copy_layout():
    # Copy accepted by a node: ``words`` words from its word ``adr`` to word ``dst_adr`` of node
    # ``dst``, asked by node ``src`` with tag ``tag``.
    return [("src", 8), ("tag", 8), ("adr", 32), ("dst", 8), ("dst_adr", 32), ("words", 11)]

def result_layout():
    return [("tag", 8), ("we", 1), ("status", 4), ("dat_r", 32)]

def fabric_packet_layout():
    # Whole packet: words 0 to 3 and number of words.
    return [("w0", 32), ("w1", 32), ("w2", 32), ("w3", 32), ("len", 3)]

def route_word(dst, src, ttl, rsp):
    return Cat(dst, src, ttl[:3] if isinstance(ttl, Signal) else Constant(ttl, 3), Constant(0, 5), rsp,
        Constant(0, 3), Constant(FABRIC_MAGIC, 4))

FABRIC_WINDOW = 8 # Packets sent and not yet acknowledged, per port.

def fabric_packet_len(w0, w1, fc_len):
    """Number of words of a packet, from its first two words (the link does not carry it)."""
    return Mux(route_fc(w0), fc_len,
        Mux(route_rt(w0), 4,
        Mux(route_rsp(w0),
            Mux(w1[12], 2, 3),    # Write response, read response.
            Mux(w1[12], 4, 3))))  # Write request, read request.

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
                tx.w2.eq(Cat(*distances[:10])),
                tx.w3.eq(Cat(*distances[10:])),
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
            received = Cat(rx.w2[:30], rx.w3[:30])
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
    timeout of copy requests (default: 32 x ``timeout``).
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
        self.comb += [
            dst.eq(sink.dst),
            local.eq(dst == node_id),
            any_up.eq(port_up != 0),
            send.eq(local | (known & any_up)),
        ]

        # Slots.
        # ------
        FREE, PENDING, DONE = range(3)
        states   = [Signal(2,         name=f"slot{i}_state")  for i in range(requests)]
        gens     = [Signal(gen_bits,  name=f"slot{i}_gen")    for i in range(requests)]
        wes      = [Signal(1,         name=f"slot{i}_we")     for i in range(requests)]
        statuses = [Signal(4,         name=f"slot{i}_status") for i in range(requests)]
        dat_rs   = [Signal(32,        name=f"slot{i}_dat_r")  for i in range(requests)]
        locals_  = [Signal(1,         name=f"slot{i}_local")  for i in range(requests)]
        owners   = [Signal(1,         name=f"slot{i}_owner")  for i in range(requests)] # 1: copy_sink.
        copies   = [Signal(1,         name=f"slot{i}_copy")   for i in range(requests)] # Copy request.
        timers   = [Signal(max=max(timeout, copy_timeout) + 1, name=f"slot{i}_timer") for i in range(requests)]

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
                endpoint.dat_r.eq(Array(dat_rs)[release_slot]),
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
                    If(send,
                        states[i].eq(PENDING),
                    ).Else(
                        # Not sent: no link up, or destination not reachable.
                        states[i].eq(DONE),
                        statuses[i].eq(Mux(any_up, FABRIC_NODE, FABRIC_LINK)),
                    )
                ).Elif(states[i] == PENDING,
                    If(~slot_up,
                        # No link left: the response cannot come back.
                        states[i].eq(DONE),
                        statuses[i].eq(FABRIC_LINK),
                    ).Elif(rx.valid & rsp_match & (rsp_slot == i),
                        states[i].eq(DONE),
                        statuses[i].eq(rx.w1[8:12]),
                        dat_rs[i].eq(Mux(rx.len == 3, rx.w2, 0)),
                    ).Elif(timers[i] == Mux(copies[i], copy_timeout, timeout),
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
                tx.w1.eq(Cat(tag, sink.sel, sink.we | sink.copy)),
                If(sink.copy, tx.w1[13:].eq(Cat(Constant(1, 1), sink.copy_dst, (sink.words - 1)[:10]))),
                tx.w2.eq(sink.adr),
                tx.w3.eq(sink.dat_w),
                tx.len.eq(Mux(sink.we | sink.copy, 4, 3)),
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
        self.mem  = mem = wishbone.Interface(data_width=32, adr_width=30)
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
        we      = Signal()
        is_copy = Signal()
        local   = Signal()
        refused = Signal(4) # Status when the request is not executed (FABRIC_OK: executed).
        self.comb += [
            we.eq(sink.w1[12]),
            is_copy.eq(sink.w1[12] & sink.w1[13]),
            local.eq(route_dst(sink.w0) == node_id),
            copy.src.eq(route_src(sink.w0)),
            copy.tag.eq(sink.w1[0:8]),
            copy.adr.eq(sink.w2),
            copy.dst.eq(sink.w1[14:22]),
            copy.dst_adr.eq(sink.w3),
            copy.words.eq(sink.w1[22:32] + 1),
            refused.eq(FABRIC_OK),
            If(~local,
                refused.eq(FABRIC_NODE)
            ).Elif(sink.w2 >= words,
                refused.eq(FABRIC_RANGE)
            ).Elif(is_copy,
                # Words read by a copy: all in the window.
                If((sink.w2 + copy.words) > words, refused.eq(FABRIC_RANGE))
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
        self.comb += [
            tx.valid.eq(tx_valid),
            free.eq(~tx_valid | tx.ready),
        ]
        self.sync += [
            If(tx.valid & tx.ready,
                tx_valid.eq(0)
            ),
            If(respond,
                tx_valid.eq(1),
                tx.w0.eq(route_word(route_src(sink.w0), Constant(node_id, 8), max_hops, 1)),
                tx.w1.eq(Cat(sink.w1[0:8], status, we)),
                tx.w2.eq(dat_r),
                tx.len.eq(Mux(we, 2, 3)),
                requests.eq(requests + 1),
            )
        ]

        # Execution: the memory is accessed from the request itself, which leaves the buffer with
        # its response (a request every two cycles with a memory answering in one).
        # ---------------------------------------------------------------------------------
        self.timer = timer = WaitTimer(timeout) # Watchdog.
        access  = Signal() # Memory access of the request.
        started = Signal() # Access started: goes to its end.
        well    = Signal() # Well formed.
        self.comb += [
            well.eq(sink.len == Mux(we, 4, 3)),
            access.eq(sink.valid & well & (refused == FABRIC_OK) & ~is_copy & (started | free)),
            mem.cyc.eq(access),
            mem.stb.eq(access),
            mem.adr.eq(sink.w2),
            mem.we.eq(we),
            mem.sel.eq(sink.w1[8:12]),
            mem.dat_w.eq(sink.w3),
            timer.wait.eq(access & ~respond), # Per request.
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
                    respond.eq(1),
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
            If(access, started.eq(1)),
            If(respond | ~access, started.eq(0)),
            If(sink.valid & sink.ready & ~well, malformed.eq(malformed + 1)),
        ]

# Fabric Copy --------------------------------------------------------------------------------------

class FabricCopy(LiteXModule):
    """Runs the copies asked to this node: reads the words in the window of the node and writes
    them in the window of the destination node, ``window`` words at a time.

    ``cmd``: copy to run (from the Responder; one at a time). ``sink``/``source``/``tag``: requests
    and results (``copy_sink``/``copy_source``/``tag`` of the Requester): the words are read with
    requests to this node itself and written with requests to the destination node. ``tx``:
    response to the node that asked the copy (status: first error, word 2: words written).

    After an error no more word is read; the words already read are not written. The words are
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

        # Copy running.
        # -------------
        busy    = Signal()
        src     = Signal(8)
        src_tag = Signal(8)
        dst     = Signal(8)
        rd_adr  = Signal(32)
        wr_adr  = Signal(32)
        left    = Signal(11) # Words still to read.
        count   = Signal(11) # Words written.
        status  = Signal(4)  # First error.
        failed  = Signal()
        self.comb += failed.eq(status != FABRIC_OK)

        # Words in flight: read asked (READ), read (WAIT), write asked (WRITE).
        # -----------------------------------------------------------------------
        IDLE, READ, WAIT, WRITE = range(4)
        states = [Signal(2,  name=f"word{i}_state") for i in range(window)]
        tags   = [Signal(8,  name=f"word{i}_tag")   for i in range(window)]
        adrs   = [Signal(32, name=f"word{i}_adr")   for i in range(window)] # Where to write it.
        dats   = [Signal(32, name=f"word{i}_dat")   for i in range(window)]

        self.free_encoder = free_encoder = PriorityEncoder(window)
        self.wait_encoder = wait_encoder = PriorityEncoder(window)
        idle = Signal()
        self.comb += [
            free_encoder.i.eq(Cat(*[s == IDLE for s in states])),
            wait_encoder.i.eq(Cat(*[s == WAIT for s in states])),
            idle.eq(reduce(and_, [s == IDLE for s in states])),
        ]

        # Requests: the words read first (they free the source), then the next read.
        do_write = Signal()
        do_read  = Signal()
        self.comb += [
            do_write.eq(busy & ~failed & ~wait_encoder.n),
            do_read.eq(busy & ~failed & ~do_write & (left != 0) & ~free_encoder.n),
            sink.valid.eq(do_write | do_read),
            sink.sel.eq(0b1111),
            If(do_write,
                sink.we.eq(1),
                sink.dst.eq(dst),
                sink.adr.eq(Array(adrs)[wait_encoder.o]),
                sink.dat_w.eq(Array(dats)[wait_encoder.o]),
            ).Else(
                sink.dst.eq(node_id),
                sink.adr.eq(rd_adr),
            ),
            source.ready.eq(1),
        ]
        sent = Signal()
        self.comb += sent.eq(sink.valid & sink.ready)
        written = Signal()
        for i in range(window):
            match = Signal(name=f"word{i}_match")
            self.comb += match.eq(source.valid & (source.tag == tags[i]))
            self.sync += [
                If(sent & do_read & (free_encoder.o == i),
                    states[i].eq(READ),
                    tags[i].eq(tag),
                    adrs[i].eq(wr_adr),
                ).Elif(sent & do_write & (wait_encoder.o == i),
                    states[i].eq(WRITE),
                    tags[i].eq(tag),
                ).Elif((states[i] == READ) & match,
                    dats[i].eq(source.dat_r),
                    states[i].eq(Mux(source.status == FABRIC_OK, WAIT, IDLE)),
                ).Elif((states[i] == WRITE) & match,
                    states[i].eq(IDLE),
                ).Elif((states[i] == WAIT) & failed,
                    states[i].eq(IDLE),
                )
            ]
        results = Signal() # Result of a request of the copy.
        self.comb += [
            results.eq(source.valid & reduce(or_, [
                ((s == READ) | (s == WRITE)) & (source.tag == t) for s, t in zip(states, tags)])),
            written.eq(source.valid & (source.status == FABRIC_OK) & reduce(or_, [
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
                rd_adr.eq(rd_adr + 1),
                wr_adr.eq(wr_adr + 1),
                left.eq(left - 1),
            ),
            If(results & (source.status != FABRIC_OK) & ~failed,
                status.eq(source.status),
            ),
            If(written,
                count.eq(count + 1),
                words.eq(words + 1),
            ),
            # End: nothing left to read (or an error) and no word in flight.
            If(busy & ~tx.valid & idle & ((left == 0) | failed) & ~(cmd.valid & cmd.ready),
                tx.valid.eq(1),
                tx.w0.eq(route_word(src, Constant(node_id, 8), max_hops, 1)),
                tx.w1.eq(Cat(src_tag, status, Constant(0, 1))),
                tx.w2.eq(count),
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

        # # #

        if link_ready is not None:
            self.comb += up.eq(link_ready)

        # RX: Packet stream to whole packets.
        # -----------------------------------
        fc_len    = 1 + (classes + 3)//4
        wpb       = link_width//32          # Words per cycle.
        max_beats = (4 + wpb - 1)//wpb      # Cycles of the longest packet.
        def beats_of(length):
            return {1: length, 2: (length + 1)[1:3], 4: 1}[wpb]

        rx       = stream.Endpoint(fabric_packet_layout()) # Whole packet, one cycle.
        rx_words = [Signal(32, name=f"rx_w{i}") for i in range(4)]
        rx_beat  = Signal(3) # Cycles of the current packet already received (saturating).
        rx_bad   = Signal()  # Packet cut, or not the number of words of its kind.
        rx_w     = [rx.w0, rx.w1, rx.w2, rx.w3]
        self.comb += sink.ready.eq(1) # The link has no backpressure.
        for i in range(4):
            # Word i comes in cycle i//wpb of the packet.
            self.comb += rx_w[i].eq(Mux(rx_beat == (i//wpb), sink.data[32*(i % wpb):32*(i % wpb) + 32], rx_words[i]))
            self.sync += If(sink.valid & (rx_beat == (i//wpb)), rx_words[i].eq(sink.data[32*(i % wpb):32*(i % wpb) + 32]))
        self.comb += [
            rx.len.eq(fabric_packet_len(rx.w0, rx.w1, fc_len)),
            rx_bad.eq(sink.error | ((rx_beat + 1) != beats_of(rx.len))),
            rx.valid.eq(sink.valid & sink.last & ~rx_bad & up),
        ]
        self.sync += [
            If(sink.valid,
                If(sink.last,
                    rx_beat.eq(0)
                ).Elif(rx_beat != 7,
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
        for c in range(classes):
            room = Signal(4, name=f"tx_room{c}")
            # Flow control word: classes 4n to 4n+3, {L, R} each.
            fc_word = [rx.w1, rx.w2, rx.w3][c//4]
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
        buf      = [[Signal(32, name=f"tx_buf{i}_w{w}") for i in range(window)] for w in range(4)]
        buf_len  = [Signal(3, name=f"tx_buf{i}_len") for i in range(window)]
        self.comb += [
            kept.eq(tx_tail - tx_acked),
            # Link down: packets are dropped instead of stalling the node.
            tx.ready.eq((kept < window) | ~up),
        ]
        for i in range(window):
            self.sync += If(sent & (tx_tail[:slot] == i),
                buf[0][i].eq(Cat(tx.w0[:19], tx_tail, tx.w0[24:])),
                buf[1][i].eq(tx.w1),
                buf[2][i].eq(tx.w2),
                buf[3][i].eq(tx.w3),
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

        index      = Signal(2) # Cycle of the packet being sent.
        cur_slot   = Signal(slot)
        cur_len    = Signal(3)
        send_slot  = Signal(slot)
        send_len   = Signal(3)
        sending    = Signal(5) # Sequence number of the packet whose last cycle is sent.
        fc_latched = [Signal(32, name=f"fc_l{i}") for i in range(4)]
        self.sync += If(fc_sent, *[fc_latched[i].eq(fc_words[i]) for i in range(4)])
        self.comb += [
            send_slot.eq(tx_send[:slot]),
            send_len.eq(Array(buf_len)[send_slot]),
            cur_slot.eq(tx_cur[:slot]),
            cur_len.eq(Array(buf_len)[cur_slot]),
            send_n.eq(Mux(rewind_apply, acked_n, Mux(packet_done, sending + 1, tx_send))),
        ]

        def beat(words, n):
            """Words of the cycle n of a packet (words: 4 Signals, or 4 functions of nothing)."""
            beats = []
            for b in range(max_beats):
                beats.append(Cat(*[words[b*wpb + j] if (b*wpb + j) < 4 else Constant(0, 32) for j in range(wpb)]))
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

        send_words = [Array(buf[w])[send_slot] for w in range(4)]
        cur_words  = [Array(buf[w])[cur_slot]  for w in range(4)]
        self.tx_fsm = tx_fsm = ResetInserter()(FSM(reset_state="IDLE"))
        self.comb += tx_fsm.reset.eq(~up)
        ctl_words = [Signal(32, name=f"ctl_w{i}") for i in range(4)]
        ctl_len   = Signal(3)
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
                NextValue(ctl_words[0], ctl.w0),
                NextValue(ctl_words[1], ctl.w1),
                NextValue(ctl_words[2], ctl.w2),
                NextValue(ctl_words[3], ctl.w3),
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
            *send(fc_latched, Constant(fc_len, 3), [NextState("IDLE")])
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
    of this node. ``routes``: {destination node: [cost of each port]} (see ``Topology.costs()``;
    None: point to point, everything to port 0). ``max_hops``: links a packet may cross (same on
    all the nodes): longest shortest path, plus the detours allowed around links that are down.
    """
    def __init__(self, node_id, size, ports=1, routes=None, max_hops=1, requests=8, read_only=False,
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
        self.responder = responder = FabricResponder(node_id,
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
