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
Fabric only needs a packet stream per port (``packet_description(32)``: a SerWB port today, see
``FabricNode.connect_serwb``), not SerWB itself.

Packet (32-bit words, 2 to 4 words):

- Word 0 (route)       : [7:0] destination node, [15:8] source node, [23:16] hop limit,
                         [24] response, [25] flow control, [31:28] magic. Only word used to forward
                         a packet.
- Word 1 (transaction) : [7:0] tag, [11:8] byte enables (request) or status (response),
                         [12] write.
- Word 2               : word address in the destination window (request) or read data (response).
- Word 3               : write data (write request).

Tag: slot of the request in the Requester ([slot_bits-1:0]) and generation of this slot (upper
bits, incremented each time the slot is reused): a response arriving after its request timed out
does not match the request that reuses the slot and is dropped.

Forwarding, flow control and deadlock freedom:

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
- Packets lost on a link (CRC error) are counted as sent and never received: when a class has
  not sent anything for ``4*fc_period`` cycles, S is set back to the last R received.

Semantics:

- Every request gets exactly one result: the response of the destination node (FABRIC_OK or the
  reason it was refused/failed), FABRIC_NODE when a node on the way cannot reach the destination,
  FABRIC_NO_RESPONSE after ``request_timeout`` cycles, or FABRIC_LINK when the first link of its
  route is or goes down. A failed request only fails itself.
- Writes are acknowledged by the node that executed them (no posted writes).
- Requests from one node to another are executed in the order they were accepted. Results are
  returned as they complete, identified by their tag.
- FABRIC_NO_RESPONSE and FABRIC_LINK mean "not confirmed": the request may have been executed.
- No retransmission. Routes are static (no rerouting around a link or node that is down).
"""

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
FABRIC_NODE        = 5 # Destination node not reachable (no route, link down or too many hops).
FABRIC_NO_RESPONSE = 8 # No response within the Requester timeout.
FABRIC_LINK        = 9 # First link of the route down.

def request_layout():
    return [("we", 1), ("dst", 8), ("adr", 32), ("dat_w", 32), ("sel", 4)]

def result_layout():
    return [("tag", 8), ("we", 1), ("status", 4), ("dat_r", 32)]

def fabric_packet_layout():
    # Whole packet: words 0 to 3 and number of words.
    return [("w0", 32), ("w1", 32), ("w2", 32), ("w3", 32), ("len", 3)]

def route_word(dst, src, ttl, rsp):
    return Cat(dst, src, ttl, rsp, Constant(0, 3), Constant(FABRIC_MAGIC, 4))

def route_dst(w0):   return w0[0:8]
def route_src(w0):   return w0[8:16]
def route_ttl(w0):   return w0[16:24]
def route_rsp(w0):   return w0[24]
def route_fc(w0):    return w0[25]
def route_magic(w0): return w0[28:32]

# Routes -------------------------------------------------------------------------------------------

class _Routes:
    """Static route table of a node: destination -> port (None: all destinations to port 0)."""
    def __init__(self, node_id, routes, ports):
        self.default = routes is None
        if routes is None:
            routes = {}
        for dst, port in routes.items():
            assert 0 <= dst < 256 and 0 <= port < ports and dst != node_id
        size = max(list(routes.keys()) + [0]) + 1
        self.size  = size
        self.bits  = max(1, bits_for(size - 1))
        self.valid = Array([Constant(int(dst in routes), 1) for dst in range(size)])
        self.port  = Array([Constant(routes.get(dst, 0), max(1, bits_for(ports - 1))) for dst in range(size)])

    def lookup(self, dst, valid, port):
        """Statements giving the route of ``dst``."""
        if self.default:
            return [valid.eq(1), port.eq(0)]
        return [
            valid.eq((dst < self.size) & self.valid[dst[:self.bits]]),
            port.eq(self.port[dst[:self.bits]]),
        ]

# Fabric Requester ---------------------------------------------------------------------------------

class FabricRequester(LiteXModule):
    """Sends requests and returns their results, with up to ``requests`` requests in flight.

    ``sink``: requests (``tag`` gives the tag of the request being accepted). ``source``: results.
    ``tx``/``rx``: request packets out, response packets in (whole packets).
    """
    def __init__(self, node_id, ports=1, routes=None, max_hops=1, requests=8, timeout=4096):
        assert requests >= 1 and requests <= 64
        self.sink    = sink    = stream.Endpoint(request_layout())
        self.source  = source  = stream.Endpoint(result_layout())
        self.tag     = tag     = Signal(8)
        self.tx      = tx      = stream.Endpoint(fabric_packet_layout())
        self.rx      = rx      = stream.Endpoint(fabric_packet_layout())
        self.port_up = port_up = Signal(ports, reset=2**ports - 1)

        # Status.
        self.pending = pending = Signal(max=requests + 1) # Requests in flight.
        self.stale   = stale   = Signal(32)               # Responses dropped (no matching request).

        # # #

        slot_bits = max(1, bits_for(requests - 1))
        gen_bits  = 8 - slot_bits
        port_bits = max(1, bits_for(ports - 1))
        routes    = _Routes(node_id, routes, ports)
        ports_up  = Array([port_up[n] for n in range(ports)])

        # Route of the request.
        # ---------------------
        dst         = Signal(8)
        local       = Signal()
        route_valid = Signal()
        route_port  = Signal(port_bits)
        send        = Signal() # Request sent (not ended at once).
        self.comb += [
            dst.eq(sink.dst),
            local.eq(dst == node_id),
            *routes.lookup(dst, route_valid, route_port),
            send.eq(local | (route_valid & ports_up[route_port])),
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
        sports   = [Signal(port_bits, name=f"slot{i}_port")   for i in range(requests)]
        timers   = [Signal(max=timeout + 1, name=f"slot{i}_timer") for i in range(requests)]

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

        # Results (first done slot).
        self.done_encoder = done_encoder = PriorityEncoder(requests)
        self.comb += done_encoder.i.eq(Cat(*[s == DONE for s in states]))
        release      = Signal()
        release_slot = Signal(slot_bits)
        self.comb += [
            release_slot.eq(done_encoder.o),
            source.valid.eq(~done_encoder.n),
            source.tag.eq(Cat(release_slot, Array(gens)[release_slot])),
            source.we.eq(Array(wes)[release_slot]),
            source.status.eq(Array(statuses)[release_slot]),
            source.dat_r.eq(Array(dat_rs)[release_slot]),
            release.eq(source.valid & source.ready),
        ]

        for i in range(requests):
            slot_up = Signal(name=f"slot{i}_up")
            self.comb += slot_up.eq(locals_[i] | ports_up[sports[i]])
            self.sync += [
                If(alloc & (alloc_slot == i),
                    wes[i].eq(sink.we),
                    timers[i].eq(0),
                    dat_rs[i].eq(0),
                    locals_[i].eq(local),
                    sports[i].eq(route_port),
                    If(send,
                        states[i].eq(PENDING),
                    ).Else(
                        # Not sent: no route to the destination, or first link down.
                        states[i].eq(DONE),
                        statuses[i].eq(Mux(route_valid, FABRIC_LINK, FABRIC_NODE)),
                    )
                ).Elif(states[i] == PENDING,
                    If(~slot_up,
                        states[i].eq(DONE),
                        statuses[i].eq(FABRIC_LINK),
                    ).Elif(rx.valid & rsp_match & (rsp_slot == i),
                        states[i].eq(DONE),
                        statuses[i].eq(rx.w1[8:12]),
                        dat_rs[i].eq(Mux(rx.len == 3, rx.w2, 0)),
                    ).Elif(timers[i] == timeout,
                        states[i].eq(DONE),
                        statuses[i].eq(FABRIC_NO_RESPONSE),
                    ).Else(
                        timers[i].eq(timers[i] + 1)
                    )
                ).Elif((states[i] == DONE) & release & (release_slot == i),
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
            sink.ready.eq(~tx_valid & ~free_encoder.n),
            alloc.eq(sink.valid & sink.ready),
        ]
        self.sync += [
            If(tx.valid & tx.ready,
                tx_valid.eq(0)
            ),
            If(alloc,
                tx_valid.eq(send),
                tx.w0.eq(route_word(sink.dst, Constant(node_id, 8), Constant(max_hops, 8), 0)),
                tx.w1.eq(Cat(tag, sink.sel, sink.we)),
                tx.w2.eq(sink.adr),
                tx.w3.eq(sink.dat_w),
                tx.len.eq(Mux(sink.we, 4, 3)),
            )
        ]

# Fabric Responder ---------------------------------------------------------------------------------

class FabricResponder(LiteXModule):
    """Executes the requests received on the memory window of the node and answers them.

    ``rx``: request packets in (``buffer_depth`` packets, with backpressure), ``tx``: response
    packets out. ``mem``: Wishbone Master to the memory (word addresses relative to the window).
    Requests for another node (given to this node because they cannot go further) are answered
    with FABRIC_NODE.
    """
    def __init__(self, node_id, size, max_hops=1, read_only=False, timeout=1024, buffer_depth=4):
        assert size > 0 and (size % 4) == 0 and buffer_depth >= 2
        self.size = size
        self.rx   = rx  = stream.Endpoint(fabric_packet_layout())
        self.tx   = tx  = stream.Endpoint(fabric_packet_layout())
        self.mem  = mem = wishbone.Interface(data_width=32, adr_width=30)

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

        # Request.
        # --------
        src    = Signal(8)
        local  = Signal()
        tag    = Signal(8)
        sel    = Signal(4)
        we     = Signal()
        adr    = Signal(32)
        dat_w  = Signal(32)
        dat_r  = Signal(32)
        status = Signal(4)

        # Watchdog.
        # ---------
        self.timer = timer = WaitTimer(timeout)

        # FSM.
        # ----
        self.comb += [
            mem.adr.eq(adr),
            mem.we.eq(we),
            mem.sel.eq(sel),
            mem.dat_w.eq(dat_w),
            tx.w0.eq(route_word(src, Constant(node_id, 8), Constant(max_hops, 8), 1)),
            tx.w1.eq(Cat(tag, status, we)),
            tx.w2.eq(dat_r),
            tx.len.eq(Mux(we, 2, 3)),
        ]
        self.fsm = fsm = FSM(reset_state="REQUEST")
        fsm.act("REQUEST",
            sink.ready.eq(1),
            If(sink.valid,
                NextValue(src,   route_src(sink.w0)),
                NextValue(local, route_dst(sink.w0) == node_id),
                NextValue(tag,   sink.w1[0:8]),
                NextValue(sel,   sink.w1[8:12]),
                NextValue(we,    sink.w1[12]),
                NextValue(adr,   sink.w2),
                NextValue(dat_w, sink.w3),
                If(sink.len == Mux(sink.w1[12], 4, 3),
                    NextState("CHECK")
                ).Else(
                    NextValue(malformed, malformed + 1)
                )
            )
        )
        fsm.act("CHECK",
            NextValue(dat_r, 0),
            NextState("RESPONSE"),
            If(~local,
                NextValue(status, FABRIC_NODE)
            ).Elif(adr >= words,
                NextValue(status, FABRIC_RANGE)
            ).Elif(we & int(read_only),
                NextValue(status, FABRIC_RO)
            ).Else(
                NextState("MEMORY")
            )
        )
        fsm.act("MEMORY",
            mem.cyc.eq(1),
            mem.stb.eq(1),
            timer.wait.eq(1),
            If(mem.ack,
                NextValue(dat_r,  mem.dat_r),
                NextValue(status, FABRIC_OK),
                NextState("RESPONSE")
            ).Elif(mem.err,
                NextValue(status, FABRIC_BUS),
                NextState("RESPONSE")
            ).Elif(timer.done,
                NextValue(status, FABRIC_TIMEOUT),
                NextState("RESPONSE")
            )
        )
        fsm.act("RESPONSE",
            tx.valid.eq(1),
            If(tx.ready,
                NextValue(requests, requests + 1),
                NextState("REQUEST")
            )
        )

# Fabric Port --------------------------------------------------------------------------------------

class FabricPort(LiteXModule):
    """Link to a neighbor: packet stream <-> whole packets, receive queues per class, credits.

    ``sink``/``source``: packet stream from/to the neighbor. ``queues[c]``: packets received in
    class c (c = response*max_hops + links crossed - 1). ``tx``: packet to send in class
    ``tx_class`` (accepted when the port is free; the switch checks ``credit[tx_class]`` first).
    """
    def __init__(self, max_hops=1, queue_depth=2, fc_period=256, link_ready=None):
        assert 1 <= max_hops <= 3 and 1 <= queue_depth <= 64 and fc_period >= 16
        self.classes  = classes = 2*max_hops
        self.sink     = sink    = stream.Endpoint(packet_description(32))
        self.source   = source  = stream.Endpoint(packet_description(32))
        self.queues   = queues  = []
        self.tx       = tx      = stream.Endpoint(fabric_packet_layout())
        self.tx_class = Signal(max=classes)
        self.credit   = credit  = Signal(classes)
        self.up       = up      = Signal(reset=1)

        # Status.
        self.rx_invalid = rx_invalid = Signal(32) # Packets dropped (not a fabric packet, malformed).
        self.rx_drops   = rx_drops   = Signal(32) # Packets dropped (queue full: credits not respected).
        self.tx_drops   = tx_drops   = Signal(32) # Packets dropped (link down).
        self.resyncs    = resyncs    = Signal(32) # Credits recovered after packets lost on the link.

        # # #

        if link_ready is not None:
            self.comb += up.eq(link_ready)

        # RX: Packet stream to whole packets.
        # -----------------------------------
        rx       = stream.Endpoint(fabric_packet_layout()) # Whole packet, one cycle.
        rx_words = [Signal(32, name=f"rx_w{i}") for i in range(3)]
        rx_count = Signal(3) # Words of the current packet already received (saturating).
        rx_bad   = Signal()  # Packet cut or too long.
        self.comb += [
            sink.ready.eq(1), # The link has no backpressure.
            rx.w0.eq(Mux(rx_count == 0, sink.data, rx_words[0])),
            rx.w1.eq(Mux(rx_count == 1, sink.data, rx_words[1])),
            rx.w2.eq(Mux(rx_count == 2, sink.data, rx_words[2])),
            rx.w3.eq(sink.data),
            rx.len.eq(rx_count + 1),
            rx_bad.eq(sink.error | (rx_count >= 4)),
            rx.valid.eq(sink.valid & sink.last & ~rx_bad & up),
        ]
        self.sync += [
            If(sink.valid,
                Case(rx_count, {i: rx_words[i].eq(sink.data) for i in range(3)}),
                If(sink.last,
                    rx_count.eq(0)
                ).Elif(rx_count != 7,
                    rx_count.eq(rx_count + 1)
                )
            ),
            # Link down: a packet cut by the link loss is not continued by the next one.
            If(~up, rx_count.eq(0)),
        ]

        # RX: Class of the packet.
        # ------------------------
        rx_fc    = Signal()
        rx_data  = Signal()
        rx_ok    = Signal()
        rx_class = Signal(max=classes)
        rx_ttl   = Signal(8)
        self.comb += [
            rx_ttl.eq(route_ttl(rx.w0)),
            rx_ok.eq(rx.valid & (route_magic(rx.w0) == FABRIC_MAGIC)),
            rx_fc.eq(rx_ok & route_fc(rx.w0)),
            # Hop limit of a received packet: max_hops (first link) to 1 (last link allowed).
            rx_data.eq(rx_ok & ~route_fc(rx.w0) & (rx.len >= 2) & (rx_ttl >= 1) & (rx_ttl <= max_hops)),
            rx_class.eq(Mux(route_rsp(rx.w0), max_hops, 0) + (max_hops - rx_ttl)),
        ]
        self.sync += [
            If((sink.valid & sink.last & up & rx_bad) | (rx.valid & ~rx_fc & ~rx_data),
                If(rx_invalid != (2**32 - 1), rx_invalid.eq(rx_invalid + 1))
            )
        ]

        # RX: Queues and receiver side of the credits (R: packets received, L: R + free places).
        # ---------------------------------------------------------------------------------------
        rx_r  = [Signal(8, name=f"rx_r{c}") for c in range(classes)]
        rx_l  = [Signal(8, name=f"rx_l{c}") for c in range(classes)]
        dirty = Signal() # A place was freed since the last flow control packet.
        freed = Signal()
        stores = []
        for c in range(classes):
            fifo = ResetInserter()(stream.SyncFIFO(fabric_packet_layout(), queue_depth, buffered=True))
            self.add_module(f"queue{c}", fifo)
            queues.append(fifo.source)
            store = Signal(name=f"rx_store{c}")
            stores.append(store & ~fifo.sink.ready)
            self.comb += [
                fifo.reset.eq(~up),
                store.eq(rx_data & (rx_class == c)),
                rx.connect(fifo.sink, omit={"valid", "ready"}),
                fifo.sink.valid.eq(store),
                rx_l[c].eq(rx_r[c] + (queue_depth - fifo.level)),
                If(fifo.source.valid & fifo.source.ready, freed.eq(1)),
            ]
            self.sync += [
                If(store, rx_r[c].eq(rx_r[c] + 1)),
                If(~up,   rx_r[c].eq(0)),
            ]
        self.sync += If(Cat(*stores) != 0, If(rx_drops != (2**32 - 1), rx_drops.eq(rx_drops + 1)))

        # TX: Sender side of the credits (S: packets sent, R/L: last values from the neighbor).
        # --------------------------------------------------------------------------------------
        tx_s     = [Signal(8, name=f"tx_s{c}") for c in range(classes)]
        tx_r     = [Signal(8, name=f"tx_r{c}") for c in range(classes)]
        tx_l     = [Signal(8, reset=queue_depth, name=f"tx_l{c}") for c in range(classes)]
        tx_idle  = [Signal(max=4*fc_period + 1, name=f"tx_idle{c}") for c in range(classes)]
        fc_len   = 1 + (classes + 1)//2
        sent     = Signal()
        resynced = []
        self.comb += sent.eq(tx.valid & tx.ready & up)
        for c in range(classes):
            room   = Signal(8, name=f"tx_room{c}")
            resync = Signal(name=f"tx_resync{c}")
            resynced.append(resync)
            # Flow control word: classes 2n and 2n+1, {L, R} each.
            fc_word = [rx.w1, rx.w2, rx.w3][c//2]
            fc_r    = fc_word[16*(c % 2) + 0:16*(c % 2) + 8]
            fc_l    = fc_word[16*(c % 2) + 8:16*(c % 2) + 16]
            self.comb += [
                room.eq(tx_l[c] - tx_s[c]),
                credit[c].eq((room != 0) & (room <= queue_depth)),
                # Nothing sent for a long time: packets sent and not received are lost.
                resync.eq((tx_idle[c] == 4*fc_period) & (tx_s[c] != tx_r[c]) & ~(sent & (self.tx_class == c))),
            ]
            self.sync += [
                If(sent & (self.tx_class == c),
                    tx_s[c].eq(tx_s[c] + 1),
                    tx_idle[c].eq(0),
                ).Elif(tx_idle[c] != 4*fc_period,
                    tx_idle[c].eq(tx_idle[c] + 1)
                ).Elif(resync,
                    tx_s[c].eq(tx_r[c]),
                ),
                If(rx_fc & (rx.len == fc_len),
                    tx_r[c].eq(fc_r),
                    tx_l[c].eq(fc_l),
                ),
                If(~up,
                    tx_s[c].eq(0),
                    tx_r[c].eq(0),
                    tx_l[c].eq(queue_depth),
                    tx_idle[c].eq(0),
                )
            ]
        self.sync += If((Cat(*resynced) != 0) & up, If(resyncs != (2**32 - 1), resyncs.eq(resyncs + 1)))

        # TX: Whole packets and flow control packets to the packet stream.
        # -----------------------------------------------------------------
        hold       = stream.Endpoint(fabric_packet_layout()) # Packet being sent.
        hold_valid = Signal()
        # Link down: packets are dropped instead of stalling the node.
        self.comb += tx.ready.eq(~hold_valid | ~up)

        fc_words = [Signal(32, name=f"fc_w{i}") for i in range(4)]
        self.comb += fc_words[0].eq((FABRIC_MAGIC << 28) | (1 << 25))
        for c in range(classes):
            self.comb += [
                fc_words[1 + c//2][16*(c % 2) + 0:16*(c % 2) + 8].eq(rx_r[c]),
                fc_words[1 + c//2][16*(c % 2) + 8:16*(c % 2) + 16].eq(rx_l[c]),
            ]
        self.fc_timer    = fc_timer    = WaitTimer(fc_period)    # Periodic flow control packet.
        self.dirty_timer = dirty_timer = WaitTimer(fc_period//8) # Places freed: sooner.
        fc_request = Signal()
        fc_sent    = Signal()
        self.comb += [
            fc_timer.wait.eq(~fc_sent),
            dirty_timer.wait.eq(dirty & ~fc_sent),
            fc_request.eq(fc_timer.done | (dirty & (~hold_valid | dirty_timer.done))),
        ]
        self.sync += [
            If(fc_sent,
                dirty.eq(freed) # Places freed in this cycle are not in the values sent.
            ).Elif(freed,
                dirty.eq(1)
            ),
            If(~up, dirty.eq(0)),
        ]

        index      = Signal(2)
        words      = Array([hold.w0, hold.w1, hold.w2, hold.w3])
        fc_latched = [Signal(32, name=f"fc_l{i}") for i in range(4)]
        self.sync += If(fc_sent, *[fc_latched[i].eq(fc_words[i]) for i in range(4)])
        self.tx_fsm = tx_fsm = ResetInserter()(FSM(reset_state="IDLE"))
        self.comb += tx_fsm.reset.eq(~up)
        tx_fsm.act("IDLE",
            NextValue(index, 0),
            If(fc_request,
                fc_sent.eq(1),
                NextState("FLOW-CONTROL")
            ).Elif(hold_valid,
                NextState("PACKET")
            )
        )
        tx_fsm.act("FLOW-CONTROL",
            source.valid.eq(1),
            source.length.eq(4*fc_len),
            source.data.eq(Array(fc_latched)[index]),
            source.last.eq(index == (fc_len - 1)),
            If(source.ready,
                NextValue(index, index + 1),
                If(source.last, NextState("IDLE"))
            )
        )
        packet_done = Signal()
        tx_fsm.act("PACKET",
            source.valid.eq(1),
            source.length.eq(Cat(Constant(0, 2), hold.len)),
            source.data.eq(words[index]),
            source.last.eq(index == (hold.len - 1)),
            If(source.ready,
                NextValue(index, index + 1),
                If(source.last,
                    packet_done.eq(1),
                    NextState("IDLE")
                )
            )
        )
        self.sync += [
            If(packet_done,
                hold_valid.eq(0)
            ),
            If(tx.valid & tx.ready,
                If(up,
                    hold_valid.eq(1),
                    hold.w0.eq(tx.w0),
                    hold.w1.eq(tx.w1),
                    hold.w2.eq(tx.w2),
                    hold.w3.eq(tx.w3),
                    hold.len.eq(tx.len),
                ).Elif(tx_drops != (2**32 - 1),
                    tx_drops.eq(tx_drops + 1)
                )
            ),
            If(~up, hold_valid.eq(0)),
        ]

# Fabric Node --------------------------------------------------------------------------------------

class FabricNode(LiteXModule):
    """Node of the fabric: Requester, Responder and switch between ``ports`` links.

    ``ports[p].sink``/``ports[p].source``: packets from/to the neighbor of port p.
    ``requester.sink``/``requester.source``: requests and results. ``responder.mem``: memory window
    of this node. ``routes``: {destination node: port} (None: point to point, everything to port 0).
    ``max_hops``: longest route of the fabric in links (same on all the nodes).
    """
    def __init__(self, node_id, size, ports=1, routes=None, max_hops=1, requests=8, read_only=False,
        request_timeout = 4096,
        memory_timeout  = 1024,
        buffer_depth    = 4,
        queue_depth     = 2,
        fc_period       = 256,
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
            routes   = routes,
            max_hops = max_hops,
            requests = requests,
            timeout  = request_timeout,
        )
        self.responder = responder = FabricResponder(node_id,
            size         = size,
            max_hops     = max_hops,
            read_only    = read_only,
            timeout      = memory_timeout,
            buffer_depth = buffer_depth,
        )
        self.ports = fports = []
        for p in range(ports):
            port = FabricPort(max_hops=max_hops, queue_depth=queue_depth, fc_period=fc_period, link_ready=link_ready[p])
            self.add_module(f"port{p}", port)
            fports.append(port)
            self.comb += requester.port_up[p].eq(port.up)

        # Switch.
        # -------
        # Inputs: packets injected by this node (Responder, Requester) and the queues of the ports.
        # One input is looked at per cycle; its packet moves when its output can take it, so a
        # packet that has to wait never blocks the packets of the other inputs.
        inputs = [] # (endpoint, injected, links crossed).
        inputs.append((responder.tx, True, 0))
        inputs.append((requester.tx, True, 0))
        for port in fports:
            for c, queue in enumerate(port.queues):
                inputs.append((queue, False, (c % max_hops) + 1))
        sel = Signal(max=len(inputs))
        self.sync += If(sel == (len(inputs) - 1), sel.eq(0)).Else(sel.eq(sel + 1))

        # Selected input.
        cur          = stream.Endpoint(fabric_packet_layout())
        cur_injected = Signal()
        cur_crossed  = Signal(max=max_hops + 1)
        cases = {}
        for i, (endpoint, injected, crossed) in enumerate(inputs):
            cases[i] = [
                endpoint.connect(cur),
                cur_injected.eq(int(injected)),
                cur_crossed.eq(crossed),
            ]
        self.comb += Case(sel, cases)

        # Route.
        table       = _Routes(node_id, routes, ports)
        dst         = Signal(8)
        rsp         = Signal()
        local       = Signal()
        route_valid = Signal()
        route_port  = Signal(port_bits)
        forward     = Signal()
        out_class   = Signal(max=classes)
        out_ready   = Signal()
        out_credit  = Signal()
        self.comb += [
            dst.eq(route_dst(cur.w0)),
            rsp.eq(route_rsp(cur.w0)),
            local.eq(dst == node_id),
            *table.lookup(dst, route_valid, route_port),
            out_class.eq(Mux(rsp, max_hops, 0) + cur_crossed),
            out_ready.eq(Array([port.tx.ready for port in fports])[route_port]),
            out_credit.eq((Array([port.credit for port in fports])[route_port] >> out_class)[0]),
            # A packet goes on when it has a route, its port is up and it may cross one more link.
            forward.eq(~local & route_valid & Array([port.up for port in fports])[route_port] &
                (cur_crossed < max_hops)),
        ]

        # Destination of the selected packet.
        to_requester = Signal()
        to_responder = Signal()
        to_port      = Signal()
        to_drop      = Signal()
        to_discard   = Signal()
        self.comb += [
            If(cur.valid,
                If(forward,
                    to_port.eq(out_ready & out_credit)
                ).Elif(rsp,
                    # Response for this node, or that cannot go further (dropped).
                    to_requester.eq(local),
                    to_drop.eq(~local),
                ).Elif(cur_injected & ~local,
                    # Request of this node whose link went down while it waited: its Requester
                    # already ended it with FABRIC_LINK.
                    to_discard.eq(1)
                ).Else(
                    # Request for this node, or request of another node that cannot go further
                    # (answered with FABRIC_NODE).
                    to_responder.eq(responder.rx.ready)
                )
            ),
            cur.ready.eq(to_port | to_requester | to_responder | to_drop | to_discard),
            cur.connect(requester.rx, omit={"valid", "ready"}),
            cur.connect(responder.rx, omit={"valid", "ready"}),
            requester.rx.valid.eq(to_requester),
            responder.rx.valid.eq(to_responder),
        ]
        out_w0 = Signal(32)
        self.comb += [
            out_w0.eq(cur.w0),
            # Hop limit: decremented when forwarded.
            If(~cur_injected, out_w0[16:24].eq(route_ttl(cur.w0) - 1)),
        ]
        for p, port in enumerate(fports):
            self.comb += [
                cur.connect(port.tx, omit={"valid", "ready", "w0"}),
                port.tx.w0.eq(out_w0),
                port.tx.valid.eq(to_port & (route_port == p)),
                port.tx_class.eq(out_class),
            ]
        self.sync += [
            If(to_port & ~cur_injected, forwarded.eq(forwarded + 1)),
            If(to_responder & ~local,   unreachable.eq(unreachable + 1)),
            If(to_drop,                 rsp_drops.eq(rsp_drops + 1)),
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
