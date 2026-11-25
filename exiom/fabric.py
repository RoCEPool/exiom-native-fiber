#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric: tagged request/response remote memory access between symmetric nodes.

Every node is both a Requester (sends read/write requests to other nodes, several can be in
flight) and a Responder (executes the requests received on its memory window and answers them).
The Fabric only needs a packet stream to its neighbor (``packet_description(32)``: SerWB port
today, see ``FabricNode.connect_serwb``), not SerWB itself.

Packet (32-bit words), sized for the multi-node fabric from the start:

- Word 0 (route)       : [7:0] destination node, [15:8] source node, [23:16] hop limit,
                         [24] response, [31:28] magic. Only word used to forward a packet.
- Word 1 (transaction) : [7:0] tag, [11:8] byte enables (request) or status (response),
                         [12] write.
- Word 2               : word address in the destination window (request) or read data (response).
- Word 3               : write data (write request).

Read request: 3 words, write request: 4, read response: 3, write response: 2.

Tag: slot of the request in the Requester ([slot_bits-1:0]) and generation of this slot (upper
bits, incremented each time the slot is reused): a response arriving after its request timed out
does not match the request that reuses the slot and is dropped.

Semantics:

- Every request gets exactly one result: the response of the destination node (status FABRIC_OK or
  the reason it was refused/failed), FABRIC_NO_RESPONSE after ``timeout`` cycles, or FABRIC_LINK
  when the link is or goes down. A failed request only fails itself.
- Writes are acknowledged by the node that executed them (no posted writes).
- Requests to the same node are executed in the order they were accepted. Results are returned
  as they complete, identified by their tag.
- FABRIC_NO_RESPONSE and FABRIC_LINK mean "not confirmed": the request may have been executed.
- No retransmission, no flow control between nodes yet: the Responder request buffer must hold
  the requests its neighbor can have in flight (``requests`` of the neighbor).
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
FABRIC_TTL   = 16

# Status (4-bit). 0-7: from the destination node, 8-15: local to the Requester.
FABRIC_OK          = 0
FABRIC_RANGE       = 1 # Outside the window of the destination node.
FABRIC_RO          = 2 # Write to a read-only window.
FABRIC_BUS         = 3 # Memory bus error.
FABRIC_TIMEOUT     = 4 # Memory did not answer.
FABRIC_NODE        = 5 # Destination node not reachable from the node that received the request.
FABRIC_NO_RESPONSE = 8 # No response within the Requester timeout.
FABRIC_LINK        = 9 # Link down.

def request_layout():
    return [("we", 1), ("dst", 8), ("adr", 32), ("dat_w", 32), ("sel", 4)]

def result_layout():
    return [("tag", 8), ("we", 1), ("status", 4), ("dat_r", 32)]

def route_word(dst, src, ttl, rsp):
    return Cat(dst, src, ttl, rsp, Constant(0, 3), Constant(FABRIC_MAGIC, 4))

# Fabric Requester ---------------------------------------------------------------------------------

class FabricRequester(LiteXModule):
    """Sends requests and returns their results, with up to ``requests`` requests in flight.

    ``sink``: requests (``tag`` gives the tag of the request being accepted). ``source``: results.
    ``tx``/``rx``: request packets out, response packets in.
    """
    def __init__(self, node_id, requests=8, timeout=4096, link_ready=None):
        assert requests >= 1 and requests <= 64
        self.sink   = sink   = stream.Endpoint(request_layout())
        self.source = source = stream.Endpoint(result_layout())
        self.tag    = tag    = Signal(8)
        self.tx     = tx     = stream.Endpoint(packet_description(32))
        self.rx     = rx     = stream.Endpoint(packet_description(32))

        # Status.
        self.pending = pending = Signal(max=requests + 1) # Requests in flight.
        self.stale   = stale   = Signal(32)               # Responses dropped (no matching request).

        # # #

        slot_bits = max(1, bits_for(requests - 1))
        gen_bits  = 8 - slot_bits

        link_up = Signal(reset=1)
        if link_ready is not None:
            self.comb += link_up.eq(link_ready)

        # Slots.
        # ------
        FREE, PENDING, DONE = range(3)
        states   = [Signal(2,        name=f"slot{i}_state")  for i in range(requests)]
        gens     = [Signal(gen_bits, name=f"slot{i}_gen")    for i in range(requests)]
        wes      = [Signal(1,        name=f"slot{i}_we")     for i in range(requests)]
        statuses = [Signal(4,        name=f"slot{i}_status") for i in range(requests)]
        dat_rs   = [Signal(32,       name=f"slot{i}_dat_r")  for i in range(requests)]
        timers   = [Signal(max=timeout + 1, name=f"slot{i}_timer") for i in range(requests)]

        # Allocation (first free slot).
        self.free_encoder = free_encoder = PriorityEncoder(requests)
        self.comb += free_encoder.i.eq(Cat(*[s == FREE for s in states]))
        alloc      = Signal()
        alloc_slot = Signal(slot_bits)
        self.comb += alloc_slot.eq(free_encoder.o)

        # Completion by a response.
        rsp_done   = Signal()
        rsp_last   = Signal()
        rsp_slot   = Signal(slot_bits)
        rsp_gen    = Signal(gen_bits)
        rsp_status = Signal(4)
        rsp_dat_r  = Signal(32)
        rsp_match  = Signal()
        self.comb += rsp_match.eq(
            (Array(states)[rsp_slot] == PENDING) & (Array(gens)[rsp_slot] == rsp_gen) & (rsp_slot < requests)
        )

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
            self.sync += [
                If(alloc & (alloc_slot == i),
                    wes[i].eq(sink.we),
                    timers[i].eq(0),
                    dat_rs[i].eq(0),
                    If(link_up,
                        states[i].eq(PENDING),
                    ).Else(
                        # Not sent.
                        states[i].eq(DONE),
                        statuses[i].eq(FABRIC_LINK),
                    )
                ).Elif(states[i] == PENDING,
                    If(~link_up,
                        states[i].eq(DONE),
                        statuses[i].eq(FABRIC_LINK),
                    ).Elif(rsp_done & rsp_match & (rsp_slot == i),
                        states[i].eq(DONE),
                        statuses[i].eq(rsp_status),
                        dat_rs[i].eq(rsp_dat_r),
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
        self.sync += If(rsp_done & ~rsp_match & (stale != (2**32 - 1)), stale.eq(stale + 1))

        # Requests TX.
        # ------------
        request = stream.Endpoint(request_layout())
        req_tag = Signal(8)
        self.comb += tag.eq(Cat(alloc_slot, Array(gens)[alloc_slot]))
        self.comb += tx.length.eq(Mux(request.we, 4*4, 3*4))

        self.tx_fsm = tx_fsm = FSM(reset_state="IDLE")
        tx_fsm.act("IDLE",
            If(sink.valid & ~free_encoder.n,
                sink.ready.eq(1),
                alloc.eq(1),
                NextValue(request.we,    sink.we),
                NextValue(request.dst,   sink.dst),
                NextValue(request.adr,   sink.adr),
                NextValue(request.dat_w, sink.dat_w),
                NextValue(request.sel,   sink.sel),
                NextValue(req_tag,       tag),
                If(link_up,
                    NextState("ROUTE")
                )
            )
        )
        tx_fsm.act("ROUTE",
            tx.valid.eq(1),
            tx.data.eq(route_word(request.dst, Constant(node_id, 8), Constant(FABRIC_TTL, 8), 0)),
            If(tx.ready, NextState("TRANSACTION"))
        )
        tx_fsm.act("TRANSACTION",
            tx.valid.eq(1),
            tx.data.eq(Cat(req_tag, request.sel, request.we)),
            If(tx.ready, NextState("ADDRESS"))
        )
        tx_fsm.act("ADDRESS",
            tx.valid.eq(1),
            tx.last.eq(~request.we),
            tx.data.eq(request.adr),
            If(tx.ready,
                If(request.we,
                    NextState("DATA")
                ).Else(
                    NextState("IDLE")
                )
            )
        )
        tx_fsm.act("DATA",
            tx.valid.eq(1),
            tx.last.eq(1),
            tx.data.eq(request.dat_w),
            If(tx.ready, NextState("IDLE"))
        )

        # Responses RX.
        # -------------
        # Responses are always accepted (the link has no backpressure). Malformed or cut responses
        # are ignored: their request ends with FABRIC_NO_RESPONSE.
        self.comb += rx.ready.eq(1)
        # Link down: a response cut by the link loss is not continued by the next packet.
        self.rx_fsm = rx_fsm = ResetInserter()(FSM(reset_state="ROUTE"))
        self.comb += rx_fsm.reset.eq(~link_up)
        rx_fsm.act("ROUTE",
            If(rx.valid & ~rx.last,
                If(rx.data[0:8] == node_id,
                    NextState("TRANSACTION")
                ).Else(
                    NextState("DRAIN") # Not for this node.
                )
            )
        )
        rx_fsm.act("TRANSACTION",
            If(rx.valid,
                NextValue(rsp_slot,   rx.data[0:slot_bits]),
                NextValue(rsp_gen,    rx.data[slot_bits:8]),
                NextValue(rsp_status, rx.data[8:12]),
                NextValue(rsp_dat_r,  0),
                If(rx.error,
                    NextState("ROUTE")
                ).Elif(rx.last,
                    rsp_last.eq(1), # Write response.
                    NextState("ROUTE")
                ).Else(
                    NextState("DATA")
                )
            )
        )
        rx_fsm.act("DATA",
            If(rx.valid,
                NextValue(rsp_dat_r, rx.data),
                If(rx.error,
                    NextState("ROUTE")
                ).Elif(rx.last,
                    rsp_last.eq(1), # Read response.
                    NextState("ROUTE")
                ).Else(
                    NextState("DRAIN")
                )
            )
        )
        rx_fsm.act("DRAIN",
            If(rx.valid & rx.last,
                NextState("ROUTE")
            )
        )
        # Response fields are registered: completion one cycle after the last word (the fields of the
        # next response are not registered before its second word).
        self.sync += rsp_done.eq(rsp_last)

# Fabric Responder ---------------------------------------------------------------------------------

class FabricResponder(LiteXModule):
    """Executes the requests received on the memory window of the node and answers them.

    ``rx``: request packets in (buffered: ``buffer_depth`` words), ``tx``: response packets out.
    ``mem``: Wishbone Master to the memory (word addresses relative to the window).
    """
    def __init__(self, node_id, size, read_only=False, timeout=1024, buffer_depth=64, link_ready=None):
        assert size > 0 and (size % 4) == 0 and buffer_depth >= 8
        self.size = size
        self.rx   = rx  = stream.Endpoint(packet_description(32))
        self.tx   = tx  = stream.Endpoint(packet_description(32))
        self.mem  = mem = wishbone.Interface(data_width=32, adr_width=30)

        # Status.
        self.rx_drops = rx_drops = Signal(32) # Requests dropped (request buffer full).
        self.requests = requests = Signal(32) # Requests answered.

        # # #

        words = size//4

        # Request Buffer.
        # ---------------
        # The link has no backpressure: packets are stored whole or dropped whole.
        self.fifo = fifo = stream.SyncFIFO([("data", 32), ("error", 1)], buffer_depth, buffered=True)
        link_up = Signal(reset=1)
        if link_ready is not None:
            self.comb += link_up.eq(link_ready)
        first   = Signal(reset=1) # Next word is the first word of a packet.
        count   = Signal(3)       # Words of the current packet already received.
        index   = Signal(3)
        room    = Signal()
        storing = Signal()
        keep    = Signal()
        self.comb += [
            rx.ready.eq(1),
            index.eq(Mux(first, 0, count)),
            # A packet is only started with room for a full request (4 words). Longer packets are
            # cut with an error word (discarded by the FSM).
            room.eq(fifo.level <= (buffer_depth - 4)),
            keep.eq(Mux(first, room, storing) & (index < 4)),
            fifo.sink.valid.eq(rx.valid & keep),
            fifo.sink.data.eq(rx.data),
            fifo.sink.last.eq(rx.last | (index == 3)),
            fifo.sink.error.eq(rx.error | ((index == 3) & ~rx.last)),
            # Link down inside a stored packet: ended with an error word, the next packet is a new
            # one.
            If(~link_up & ~first & storing & (count < 4),
                fifo.sink.valid.eq(1),
                fifo.sink.last.eq(1),
                fifo.sink.error.eq(1),
            )
        ]
        self.sync += [
            If(rx.valid,
                first.eq(rx.last),
                storing.eq(keep),
                If(index != 7, count.eq(index + 1)),
                If(first & ~room & (rx_drops != (2**32 - 1)),
                    rx_drops.eq(rx_drops + 1)
                )
            ),
            If(~link_up,
                first.eq(1),
                storing.eq(0),
            )
        ]
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
            tx.length.eq(Mux(we, 2*4, 3*4)),
        ]
        self.fsm = fsm = FSM(reset_state="ROUTE")
        fsm.act("ROUTE",
            sink.ready.eq(1),
            If(sink.valid & ~sink.last,
                NextValue(src,   sink.data[8:16]),
                NextValue(local, sink.data[0:8] == node_id),
                NextState("TRANSACTION")
            )
        )
        fsm.act("TRANSACTION",
            sink.ready.eq(1),
            If(sink.valid,
                NextValue(tag, sink.data[0:8]),
                NextValue(sel, sink.data[8:12]),
                NextValue(we,  sink.data[12]),
                If(sink.last,
                    NextState("ROUTE") # Malformed or cut request.
                ).Else(
                    NextState("ADDRESS")
                )
            )
        )
        fsm.act("ADDRESS",
            sink.ready.eq(1),
            If(sink.valid,
                NextValue(adr, sink.data),
                If(sink.error,
                    NextState("ROUTE")
                ).Elif(we,
                    If(sink.last,
                        NextState("ROUTE") # Malformed request.
                    ).Else(
                        NextState("DATA")
                    )
                ).Else(
                    If(sink.last,
                        NextState("CHECK")
                    ).Else(
                        NextState("DRAIN")
                    )
                )
            )
        )
        fsm.act("DATA",
            sink.ready.eq(1),
            If(sink.valid,
                NextValue(dat_w, sink.data),
                If(sink.error,
                    NextState("ROUTE")
                ).Elif(sink.last,
                    NextState("CHECK")
                ).Else(
                    NextState("DRAIN")
                )
            )
        )
        fsm.act("DRAIN",
            sink.ready.eq(1),
            If(sink.valid & sink.last,
                NextState("ROUTE")
            )
        )
        fsm.act("CHECK",
            NextValue(dat_r, 0),
            NextState("RESPONSE-ROUTE"),
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
                NextState("RESPONSE-ROUTE")
            ).Elif(mem.err,
                NextValue(status, FABRIC_BUS),
                NextState("RESPONSE-ROUTE")
            ).Elif(timer.done,
                NextValue(status, FABRIC_TIMEOUT),
                NextState("RESPONSE-ROUTE")
            )
        )
        fsm.act("RESPONSE-ROUTE",
            tx.valid.eq(1),
            tx.data.eq(route_word(src, Constant(node_id, 8), Constant(FABRIC_TTL, 8), 1)),
            If(tx.ready, NextState("RESPONSE-TRANSACTION"))
        )
        fsm.act("RESPONSE-TRANSACTION",
            tx.valid.eq(1),
            tx.last.eq(we),
            tx.data.eq(Cat(tag, status, we)),
            If(tx.ready,
                If(we,
                    NextValue(requests, requests + 1),
                    NextState("ROUTE")
                ).Else(
                    NextState("RESPONSE-DATA")
                )
            )
        )
        fsm.act("RESPONSE-DATA",
            tx.valid.eq(1),
            tx.last.eq(1),
            tx.data.eq(dat_r),
            If(tx.ready,
                NextValue(requests, requests + 1),
                NextState("ROUTE")
            )
        )

# Fabric Node --------------------------------------------------------------------------------------

class FabricNode(LiteXModule):
    """Node of the fabric: Requester and Responder sharing one packet stream to the neighbor.

    ``sink``/``source``: packets from/to the neighbor. ``requester.sink``/``requester.source``:
    requests and results. ``responder.mem``: memory window of this node.
    """
    def __init__(self, node_id, size, requests=8, read_only=False,
        request_timeout = 4096,
        memory_timeout  = 1024,
        buffer_depth    = 64,
        link_ready      = None,
    ):
        assert 0 <= node_id < 256
        self.node_id = node_id
        self.sink    = sink   = stream.Endpoint(packet_description(32))
        self.source  = source = stream.Endpoint(packet_description(32))

        # Status.
        self.rx_invalid = rx_invalid = Signal(32) # Packets dropped (not a fabric packet).

        # # #

        self.requester = requester = FabricRequester(node_id,
            requests   = requests,
            timeout    = request_timeout,
            link_ready = link_ready,
        )
        self.responder = responder = FabricResponder(node_id,
            size         = size,
            read_only    = read_only,
            timeout      = memory_timeout,
            buffer_depth = buffer_depth,
            link_ready   = link_ready,
        )

        # RX: Requests to the Responder, responses to the Requester (route word of each packet).
        # --------------------------------------------------------------------------------------
        first   = Signal(reset=1)
        valid   = Signal()
        route   = Signal(2)
        REQUEST, RESPONSE, DROP = range(3)
        self.comb += [
            sink.ready.eq(1),
            valid.eq(sink.data[28:32] == FABRIC_MAGIC),
        ]
        current = Signal(2)
        self.comb += [
            If(first,
                If(~valid,
                    current.eq(DROP)
                ).Elif(sink.data[24],
                    current.eq(RESPONSE)
                ).Else(
                    current.eq(REQUEST)
                )
            ).Else(
                current.eq(route)
            ),
            sink.connect(requester.rx, keep={"data", "last", "error"}),
            sink.connect(responder.rx, keep={"data", "last", "error"}),
            requester.rx.valid.eq(sink.valid & (current == RESPONSE)),
            responder.rx.valid.eq(sink.valid & (current == REQUEST)),
        ]
        self.sync += [
            If(sink.valid,
                first.eq(sink.last),
                route.eq(current),
                If(first & ~valid & (rx_invalid != (2**32 - 1)),
                    rx_invalid.eq(rx_invalid + 1)
                )
            )
        ]
        if link_ready is not None:
            # Link down: a packet cut by the link loss is not continued by the next one.
            self.sync += If(~link_ready, first.eq(1))

        # TX: Responses first (they free the requests of the neighbor), packets not interleaved.
        # --------------------------------------------------------------------------------------
        grant = Signal() # 0: Responder, 1: Requester.
        busy  = Signal() # Inside a packet.
        mux   = stream.Endpoint(packet_description(32))
        self.comb += [
            If(grant,
                requester.tx.connect(mux, keep={"valid", "ready", "last", "data", "length"})
            ).Else(
                responder.tx.connect(mux, keep={"valid", "ready", "last", "data", "length"})
            )
        ]
        self.sync += [
            If(mux.valid & mux.ready,
                busy.eq(~mux.last)
            ),
            # Grant only changes between packets, when the granted side has nothing to send.
            If(~busy & ~mux.valid,
                grant.eq(~responder.tx.valid & requester.tx.valid)
            )
        ]

        # Link down: packets are discarded instead of stalling the node (the remaining words of a
        # packet cut by the link loss too); sending resumes on a packet boundary.
        discard = Signal()
        if link_ready is not None:
            self.sync += [
                If(~link_ready,
                    discard.eq(1)
                ).Elif(~busy & ~mux.valid,
                    discard.eq(0)
                )
            ]
            self.comb += [
                mux.connect(source, keep={"valid", "ready", "last", "data", "length"}),
                If(discard | ~link_ready,
                    source.valid.eq(0),
                    mux.ready.eq(1),
                )
            ]
        else:
            self.comb += mux.connect(source, keep={"valid", "ready", "last", "data", "length"})

    def connect_serwb(self, core, port=FABRIC_PORT):
        """Use a port of a SERWBCore as link to the neighbor (before the core is finalized)."""
        core.add_downstream_endpoint(port=port, endpoint=self.source)
        core.add_upstream_endpoint(  port=port, endpoint=self.sink)

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
