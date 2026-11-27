#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric (exiom/fabric.py): two symmetric nodes, tagged requests with several requests in flight.

- "direct": the two nodes connected by their packet streams (no link model).
- "serwb" : through SERWBCore and the word-level PHY model, with packet drops and link down.
"""

import unittest

from migen import *

from litex.gen import *
from litex.gen.sim import *

from litex.soc.interconnect import wishbone

from litex.soc.interconnect import stream

from liteiclink.serwb.core   import SERWBCore
from liteiclink.serwb.packet import packet_description

from exiom.fabric  import *
from exiom.network import FabricLossyLink
from exiom.memory  import FabricMemory

from test.test_serwb_link import LoopbackPHY, bus_access

# After the other imports (run_simulation is also exported by litex.gen).
from test.fabric_sim import run_simulation # Same simulator, starts faster on large designs.

# Memory -------------------------------------------------------------------------------------------

class Memory(LiteXModule):
    """SRAM with ack latency (`latency`, or `slow_latency` at word `slow_adr`), a word never
    answered (`hang_adr`) and a word answered with an error (`err_adr`)."""
    def __init__(self, bus, latency=0, slow_adr=None, slow_latency=0, hang_adr=None, err_adr=None):
        self.sram = sram = wishbone.SRAM(1024)
        self.mem  = sram.mem
        hang  = Signal()
        err   = Signal()
        delay = Signal(16)
        count = Signal(16)
        self.comb += delay.eq(latency)
        if slow_adr is not None:
            self.comb += If(bus.adr == slow_adr, delay.eq(slow_latency))
        if hang_adr is not None:
            self.comb += hang.eq(bus.adr == hang_adr)
        if err_adr is not None:
            self.comb += err.eq(bus.adr == err_adr)
        self.comb += [
            bus.connect(sram.bus, omit={"stb", "cyc", "ack", "err"}),
            sram.bus.cyc.eq(bus.cyc & ~hang & ~err),
            sram.bus.stb.eq(bus.stb & ~hang & ~err & (count == delay)),
            bus.ack.eq(sram.bus.ack),
            bus.err.eq(bus.cyc & bus.stb & err),
        ]
        self.sync += [
            If(~(bus.cyc & bus.stb) | bus.ack,
                count.eq(0)
            ).Elif(count != delay,
                count.eq(count + 1)
            )
        ]

# Packet Filter ------------------------------------------------------------------------------------

class PacketFilter(LiteXModule):
    """Drops `count` packets of a packet stream from the one of index `drop`: data packets (packets
    sent again included), or flow control packets (`flow_control`)."""
    def __init__(self, sink, source):
        self.drop         = Signal(16, reset=2**16 - 1)
        self.count        = Signal(16, reset=1)
        self.flow_control = Signal()
        self.dropped      = Signal(16)
        first    = Signal(reset=1)
        index    = Signal(16)
        counted  = Signal()
        dropping = Signal()
        drop_now = Signal()
        self.comb += [
            counted.eq(Mux(self.flow_control, sink.data[25], ~sink.data[25] & ~sink.data[26])),
            drop_now.eq(Mux(first, counted & (index >= self.drop) & (index < (self.drop + self.count)), dropping)),
            sink.connect(source),
            If(drop_now,
                source.valid.eq(0),
                sink.ready.eq(1),
            )
        ]
        self.sync += [
            If(sink.valid & sink.ready,
                first.eq(sink.last),
                dropping.eq(drop_now & ~sink.last),
                If(first & counted,
                    index.eq(index + 1),
                    If(drop_now, self.dropped.eq(self.dropped + 1)),
                )
            )
        ]

# Memory of a node ---------------------------------------------------------------------------------

class Defaults:
    pipelined = False # Nodes with a memory port (FabricPipelinedResponder) instead of Wishbone.

defaults = Defaults()

def node_memory(node, pipelined, latency=0, slow_adr=None, slow_latency=0, hang_adr=None, err_adr=None):
    """Memory of 256 words for a node, with a ``mem`` attribute (the words)."""
    if pipelined:
        assert slow_adr is None
        return FabricMemory(node.responder.cmd, node.responder.rsp, 256, latency=1 + latency,
            err_adr=err_adr, hang_adr=hang_adr)
    return Memory(node.responder.mem, latency=latency, slow_adr=slow_adr, slow_latency=slow_latency,
        hang_adr=hang_adr, err_adr=err_adr)

# DUT ----------------------------------------------------------------------------------------------

class DUT(LiteXModule):
    """Nodes A (id 1) and B (id 2), each with its memory."""
    def __init__(self, link="direct", size=128, requests=8, request_timeout=4096, buffer_depth=4,
        queue_depth=2, fc_period=64, read_only=False, memory_timeout=128, loss=0, link_width=32,
        mem_kwargs={}, pipelined=None):
        assert (link == "direct") or (link_width == 32)
        self.link_up = Signal(reset=1)
        pipelined = defaults.pipelined if pipelined is None else pipelined

        # Nodes.
        node_kwargs = dict(pipelined=pipelined, size=size, requests=requests, request_timeout=request_timeout,
            memory_timeout=memory_timeout, buffer_depth=buffer_depth, queue_depth=queue_depth,
            fc_period=fc_period, read_only=read_only, link_width=link_width, link_ready=self.link_up)
        self.a = FabricNode(node_id=1, **node_kwargs)
        self.b = FabricNode(node_id=2, **node_kwargs)
        self.a_mem = node_memory(self.a, pipelined, **mem_kwargs)
        self.b_mem = node_memory(self.b, pipelined, **mem_kwargs)

        # Packet drops (data packet of index `x2y.drop` of the direction).
        a_source = stream.Endpoint(packet_description(link_width))
        b_source = stream.Endpoint(packet_description(link_width))
        self.a2b = PacketFilter(self.a.source, a_source)
        self.b2a = PacketFilter(self.b.source, b_source)
        if loss:
            # Packets lost at random in both directions (`loss` out of 256).
            a_lossy, b_lossy = a_source, b_source
            a_source = stream.Endpoint(packet_description(link_width))
            b_source = stream.Endpoint(packet_description(link_width))
            self.a_lossy = FabricLossyLink(a_lossy, a_source, loss=loss, seed=1)
            self.b_lossy = FabricLossyLink(b_lossy, b_source, loss=loss, seed=2)

        # Link.
        if link == "direct":
            self.comb += [
                a_source.connect(self.b.sink),
                b_source.connect(self.a.sink),
            ]
        else:
            self.a_phy = LoopbackPHY()
            self.b_phy = LoopbackPHY()
            self.comb += [
                self.a_phy.tx.connect(self.b_phy.rx),
                self.b_phy.tx.connect(self.a_phy.rx),
                self.a_phy.init.ready.eq(self.link_up),
                self.b_phy.init.ready.eq(self.link_up),
            ]
            self.a_core = SERWBCore(self.a_phy, clk_freq=int(1e6), mode="slave")
            self.b_core = SERWBCore(self.b_phy, clk_freq=int(1e6), mode="master")
            self.a_core.add_downstream_endpoint(port=FABRIC_PORT, endpoint=a_source)
            self.b_core.add_downstream_endpoint(port=FABRIC_PORT, endpoint=b_source)
            self.a_core.add_upstream_endpoint(port=FABRIC_PORT, endpoint=self.a.sink)
            self.b_core.add_upstream_endpoint(port=FABRIC_PORT, endpoint=self.b.sink)

# Helpers ------------------------------------------------------------------------------------------

def pattern(adr, salt=0):
    return (0xa5000000 | (salt << 20) | (adr << 8) | (~adr & 0xff)) & 0xffffffff

def request(node, adr, we=0, dat_w=0, dst=None, sel=0b1111, timeout=20000):
    """Sends one request, returns its tag."""
    sink = node.requester.sink
    yield sink.we.eq(we)
    yield sink.dst.eq(dst)
    yield sink.adr.eq(adr)
    yield sink.dat_w.eq(dat_w)
    yield sink.sel.eq(sel)
    yield sink.valid.eq(1)
    yield
    for _ in range(timeout):
        if (yield sink.ready):
            break
        yield
    else:
        raise TimeoutError("Request not accepted.")
    tag = (yield node.requester.tag)
    yield sink.valid.eq(0)
    yield
    return tag

def result(node, timeout=20000):
    """Waits for one result, returns (tag, we, status, dat_r)."""
    source = node.requester.source
    yield source.ready.eq(1)
    yield
    for _ in range(timeout):
        if (yield source.valid):
            break
        yield
    else:
        raise TimeoutError("No result.")
    r = ((yield source.tag), (yield source.we), (yield source.status), (yield source.dat_r))
    yield source.ready.eq(0)
    yield
    return r

def results(node, n):
    """Waits for n results, returns {tag: (we, status, dat_r)}."""
    r = {}
    for _ in range(n):
        tag, we, status, dat_r = yield from result(node)
        assert tag not in r, "Tag returned twice."
        r[tag] = (we, status, dat_r)
    return r

def write(node, dst, adr, dat_w, sel=0b1111):
    tag = yield from request(node, adr, we=1, dat_w=dat_w, dst=dst, sel=sel)
    rtag, we, status, _ = yield from result(node)
    assert (rtag, we) == (tag, 1)
    return status

def read(node, dst, adr):
    tag = yield from request(node, adr, dst=dst)
    rtag, we, status, dat_r = yield from result(node)
    assert (rtag, we) == (tag, 0)
    return dat_r, status

def wait(n):
    for _ in range(n):
        yield

A, B = 1, 2

# Test Fabric --------------------------------------------------------------------------------------

class TestFabric(unittest.TestCase):
    def links(self):
        return ["direct", "serwb"]

    def test_link_widths(self):
        # 64 and 128 bits per cycle (a whole packet): same behavior, losses included.
        for link_width in [64, 128]:
            for loss in [0, 32]:
                with self.subTest(link_width=link_width, loss=loss):
                    dut = DUT(link_width=link_width, loss=loss, request_timeout=30000,
                        mem_kwargs=dict(latency=2, hang_adr=20))
                    def generator():
                        tags = {}
                        for adr in range(8):
                            tags[(yield from request(dut.a, adr, we=1, dat_w=pattern(adr), dst=B))] = adr
                        r = yield from results(dut.a, 8)
                        self.assertTrue(all(v == (1, FABRIC_OK, 0) for v in r.values()))
                        tags = {}
                        for adr in [5, 2, 7, 0, 3, 6, 1, 4]:
                            tags[(yield from request(dut.a, adr, dst=B))] = adr
                        r = yield from results(dut.a, 8)
                        for tag, adr in tags.items():
                            self.assertEqual(r[tag], (0, FABRIC_OK, pattern(adr)))
                        for adr, we, status in [(40, 1, FABRIC_RANGE), (40, 0, FABRIC_RANGE), (20, 1, FABRIC_TIMEOUT)]:
                            tag = yield from request(dut.a, adr, we=we, dat_w=1, dst=B)
                            self.assertEqual((yield from result(dut.a))[2], status)
                        # Both directions, byte enables.
                        self.assertEqual((yield from write(dut.b, A, 3, 0xffffffff, sel=0b0110)), FABRIC_OK)
                        self.assertEqual((yield from read(dut.b, A, 3)), (0x00ffff00, FABRIC_OK))
                        self.assertEqual((yield dut.b.responder.requests), 8 + 8 + 3)
                        self.assertEqual((yield dut.a.requester.stale), 0)
                        for node in [dut.a, dut.b]:
                            self.assertEqual((yield node.ports[0].rx_invalid), 0)
                    run_simulation(dut, generator())

    def test_read_write(self):
        for link in self.links():
            with self.subTest(link=link):
                dut = DUT(link=link)
                def generator():
                    for adr in range(8):
                        self.assertEqual((yield from write(dut.a, B, adr, pattern(adr))), FABRIC_OK)
                    for adr in range(8):
                        self.assertEqual((yield from read(dut.a, B, adr)), (pattern(adr), FABRIC_OK))
                        self.assertEqual((yield dut.b_mem.mem[adr]), pattern(adr))
                    # Byte enables.
                    self.assertEqual((yield from write(dut.a, B, 3, 0xffffffff, sel=0b0110)), FABRIC_OK)
                    self.assertEqual((yield from read(dut.a, B, 3)), ((pattern(3) | 0x00ffff00), FABRIC_OK))
                    # Nothing written on the requesting node.
                    self.assertEqual((yield dut.a_mem.mem[3]), 0)
                    self.assertEqual((yield dut.b.responder.requests), 8 + 8 + 2)
                run_simulation(dut, generator())

    def test_concurrent(self):
        # Requests in flight together: every result goes to its own request.
        for link in self.links():
            with self.subTest(link=link):
                # Memory slow enough to have the 8 requests sent before the first response.
                dut = DUT(link=link, memory_timeout=1024, request_timeout=8192, mem_kwargs=dict(latency=400))
                max_pending = [0]
                def generator():
                    # 8 writes sent without waiting for their results.
                    tags = {}
                    for adr in range(8):
                        tags[(yield from request(dut.a, adr, we=1, dat_w=pattern(adr), dst=B))] = adr
                    self.assertEqual(len(tags), 8)
                    r = yield from results(dut.a, 8)
                    self.assertEqual(set(r.keys()), set(tags.keys()))
                    for tag, (we, status, _) in r.items():
                        self.assertEqual((we, status), (1, FABRIC_OK))
                    # 8 reads in flight, in another order.
                    tags = {}
                    for adr in [5, 2, 7, 0, 3, 6, 1, 4]:
                        tags[(yield from request(dut.a, adr, dst=B))] = adr
                    r = yield from results(dut.a, 8)
                    for tag, adr in tags.items():
                        self.assertEqual(r[tag], (0, FABRIC_OK, pattern(adr)))
                    self.assertEqual((yield dut.a.requester.pending), 0)
                    self.assertEqual((yield dut.a.requester.stale), 0)
                    self.assertEqual((yield dut.b.ports[0].rx_drops), 0)
                def monitor():
                    while True:
                        max_pending[0] = max(max_pending[0], (yield dut.a.requester.pending))
                        yield
                run_simulation(dut, [generator(), passive(monitor)()])
                self.assertEqual(max_pending[0], 8)

    def test_queueing(self):
        # More requests than slots: the Requester applies backpressure, nothing lost or duplicated.
        dut  = DUT(link="serwb", requests=4, mem_kwargs=dict(latency=16))
        n    = 24
        sent = {}
        got  = {}
        def sender():
            for i in range(n):
                tag = yield from request(dut.a, i, we=1, dat_w=pattern(i, 1), dst=B)
                sent.setdefault(tag, []).append(i)
        def receiver():
            source = dut.a.requester.source
            yield source.ready.eq(1)
            count = 0
            while count < n:
                yield
                if (yield source.valid):
                    got.setdefault((yield source.tag), []).append((yield source.status))
                    count += 1
            yield source.ready.eq(0)
            yield
            for i in range(n):
                self.assertEqual((yield dut.b_mem.mem[i]), pattern(i, 1))
            self.assertEqual((yield dut.b.responder.requests), n)
        run_simulation(dut, [sender(), receiver()])
        self.assertEqual(sum(len(v) for v in sent.values()), n)
        self.assertEqual({tag: len(v) for tag, v in sent.items()}, {tag: len(v) for tag, v in got.items()})
        self.assertTrue(all(s == FABRIC_OK for v in got.values() for s in v))

    def test_bidirectional(self):
        # Both nodes request each other at the same time (requests and responses share the link).
        for link in self.links():
            with self.subTest(link=link):
                dut = DUT(link=link, mem_kwargs=dict(latency=8))
                def side(node, dst, salt):
                    def generator():
                        tags = {}
                        for adr in range(8):
                            tags[(yield from request(node, adr, we=1, dat_w=pattern(adr, salt), dst=dst))] = adr
                        r = yield from results(node, 8)
                        self.assertTrue(all(status == FABRIC_OK for _, status, _ in r.values()))
                        tags = {}
                        for adr in range(8):
                            tags[(yield from request(node, adr, dst=dst))] = adr
                        r = yield from results(node, 8)
                        for tag, adr in tags.items():
                            self.assertEqual(r[tag], (0, FABRIC_OK, pattern(adr, salt)))
                    return generator()
                run_simulation(dut, [side(dut.a, B, 1), side(dut.b, A, 2)])

    def test_errors_isolated(self):
        # A failed request only fails itself, with its own status.
        for link in self.links():
            with self.subTest(link=link):
                dut = DUT(link=link, mem_kwargs=dict(hang_adr=20, err_adr=21))
                def generator():
                    expected = {}
                    for adr, we, status in [
                        (1,  1, FABRIC_OK),
                        (40, 1, FABRIC_RANGE),
                        (2,  1, FABRIC_OK),
                        (20, 1, FABRIC_TIMEOUT),
                        (21, 0, FABRIC_BUS),
                        (40, 0, FABRIC_RANGE),
                        (3,  1, FABRIC_OK),
                        (9,  1, FABRIC_NODE), # Sent to a node that does not exist.
                    ]:
                        dst = 9 if status == FABRIC_NODE else B
                        tag = yield from request(dut.a, adr, we=we, dat_w=pattern(adr), dst=dst)
                        expected[tag] = (we, status)
                    r = yield from results(dut.a, len(expected))
                    self.assertEqual({tag: v[:2] for tag, v in r.items()}, expected)
                    for adr in [1, 2, 3]:
                        self.assertEqual((yield from read(dut.a, B, adr)), (pattern(adr), FABRIC_OK))
                    for adr in [9, 20, 21, 40]:
                        self.assertEqual((yield dut.b_mem.mem[adr]), 0)
                run_simulation(dut, generator())

    def test_read_only(self):
        dut = DUT(read_only=True)
        def generator():
            yield dut.b_mem.mem[2].eq(0x1234)
            yield
            self.assertEqual((yield from write(dut.a, B, 2, 0x5678)), FABRIC_RO)
            self.assertEqual((yield from read(dut.a, B, 2)), (0x1234, FABRIC_OK))
        run_simulation(dut, generator())

    def lost_packet_test(self, direction, link="serwb", count=1, flow_control=False):
        # Packets lost on the link while 6 writes are in flight: they are sent again, every write
        # is executed once and acknowledged.
        dut = DUT(link=link, request_timeout=4000, mem_kwargs=dict(latency=8))
        def generator():
            lossy = {"request": dut.a2b, "response": dut.b2a}[direction]
            yield lossy.flow_control.eq(int(flow_control))
            yield lossy.count.eq(count)
            yield lossy.drop.eq(2)
            tags = []
            for adr in range(6):
                tags.append((yield from request(dut.a, adr, we=1, dat_w=pattern(adr), dst=B)))
            r = yield from results(dut.a, 6)
            for adr, tag in enumerate(tags):
                self.assertEqual(r[tag][1], FABRIC_OK)
            for adr in range(6):
                self.assertEqual((yield dut.b_mem.mem[adr]), pattern(adr))
            for adr in range(6, 12):
                self.assertEqual((yield from write(dut.a, B, adr, pattern(adr))), FABRIC_OK)
                self.assertEqual((yield from read(dut.a, B, adr)), (pattern(adr), FABRIC_OK))
            yield from wait(600)
            # Lost packets really dropped, nothing executed twice, nothing left over.
            self.assertEqual((yield lossy.dropped), count)
            self.assertEqual((yield dut.b.responder.requests), 6 + 12)
            self.assertEqual((yield dut.a.requester.stale), 0)
            retransmits = 0
            for node in [dut.a, dut.b]:
                retransmits += (yield node.ports[0].retransmits)
                self.assertEqual((yield node.ports[0].rx_drops), 0)
            if not flow_control:
                self.assertGreater(retransmits, 0)
        run_simulation(dut, generator())

    def test_lost_request(self):
        for link in self.links():
            with self.subTest(link=link):
                self.lost_packet_test("request", link=link)

    def test_lost_response(self):
        for link in self.links():
            with self.subTest(link=link):
                self.lost_packet_test("response", link=link)

    def test_lost_several(self):
        # Three packets in a row, so also the packets sent again.
        self.lost_packet_test("request",  link="direct", count=3)
        self.lost_packet_test("response", link="direct", count=3)

    def test_lost_flow_control(self):
        # Acknowledgments and credits lost: sent again with the next flow control packets, packets
        # sent again meanwhile are recognized and discarded.
        self.lost_packet_test("response", link="direct", count=4, flow_control=True)
        self.lost_packet_test("request",  link="direct", count=4, flow_control=True)

    def test_lossy_link(self):
        # One packet out of 8 lost at random in both directions (data, flow control): everything
        # is executed once and in order.
        for link in self.links():
            with self.subTest(link=link):
                dut = DUT(link=link, loss=32, request_timeout=30000, mem_kwargs=dict(latency=4))
                n = 120
                def sender():
                    for i in range(n):
                        # Same word written again and again: the last write must be the last executed.
                        yield from request(dut.a, i % 8, we=1, dat_w=pattern(i % 8, i // 8), dst=B)
                def receiver():
                    statuses = []
                    for _ in range(n // 8):
                        r = yield from results(dut.a, 8)
                        statuses += [status for _, status, _ in r.values()]
                    self.assertEqual(statuses, [FABRIC_OK]*n)
                    for adr in range(8):
                        self.assertEqual((yield dut.b_mem.mem[adr]), pattern(adr, n // 8 - 1))
                    self.assertEqual((yield dut.b.responder.requests), n)
                    self.assertGreater((yield dut.a_lossy.dropped), 5)
                    self.assertGreater((yield dut.b_lossy.dropped), 5)
                    self.assertEqual((yield dut.a.requester.stale), 0)
                run_simulation(dut, [sender(), receiver()])

    def test_late_response(self):
        # Response arriving after the timeout of its request, when its slot is used by another
        # request: dropped (tag generation), not given to the new request.
        dut = DUT(requests=1, request_timeout=80, mem_kwargs=dict(slow_adr=5, slow_latency=100))
        def generator():
            yield dut.b_mem.mem[5].eq(0x5555)
            yield dut.b_mem.mem[6].eq(0x6666)
            yield
            first = yield from request(dut.a, 5, dst=B)
            tag, _, status, _ = yield from result(dut.a)
            self.assertEqual((tag, status), (first, FABRIC_NO_RESPONSE))
            # Same slot, new generation, sent before the late response comes back.
            second = yield from request(dut.a, 6, dst=B)
            self.assertNotEqual(second, first)
            self.assertEqual(second & 0x1, first & 0x1)
            tag, _, status, dat_r = yield from result(dut.a)
            self.assertEqual((tag, status, dat_r), (second, FABRIC_OK, 0x6666))
            self.assertEqual((yield dut.a.requester.stale), 1)
        run_simulation(dut, generator())

    def test_link_down(self):
        dut = DUT(link="serwb", mem_kwargs=dict(latency=64))
        def generator():
            self.assertEqual((yield from write(dut.a, B, 0, pattern(0))), FABRIC_OK)
            # Requests in flight when the link goes down.
            tags = []
            for adr in range(1, 5):
                tags.append((yield from request(dut.a, adr, we=1, dat_w=pattern(adr), dst=B)))
            yield dut.link_up.eq(0)
            r = yield from results(dut.a, 4)
            self.assertEqual(set(r.keys()), set(tags))
            self.assertTrue(all(status == FABRIC_LINK for _, status, _ in r.values()))
            # Requests while the link is down.
            self.assertEqual((yield from write(dut.a, B, 8, pattern(8))), FABRIC_LINK)
            self.assertEqual((yield from read(dut.a, B, 0))[1], FABRIC_LINK)
            yield from wait(600)
            yield dut.link_up.eq(1)
            yield from wait(16)
            # Link back: requests served, nothing left from before.
            self.assertEqual((yield from write(dut.a, B, 9, pattern(9))), FABRIC_OK)
            self.assertEqual((yield from read(dut.a, B, 9)), (pattern(9), FABRIC_OK))
            self.assertEqual((yield from read(dut.a, B, 0)), (pattern(0), FABRIC_OK))
            self.assertEqual((yield dut.b_mem.mem[8]), 0)
            self.assertEqual((yield dut.a.requester.pending), 0)
        run_simulation(dut, generator())

    def test_link_down_sweep(self):
        # Link lost at any point of a request/response exchange (packets cut anywhere): the
        # requests in flight end with FABRIC_LINK or their real result, and the nodes are back in
        # sync when the link returns.
        for delay in range(0, 160, 5):
            with self.subTest(delay=delay):
                dut = DUT(link="serwb", mem_kwargs=dict(latency=4))
                def generator():
                    tags = []
                    for adr in range(2):
                        tags.append((yield from request(dut.a, adr, we=1, dat_w=pattern(adr), dst=B)))
                    yield from wait(delay)
                    yield dut.link_up.eq(0)
                    r = yield from results(dut.a, 2)
                    for tag in tags:
                        self.assertIn(r[tag][1], [FABRIC_OK, FABRIC_LINK])
                    yield from wait(300)
                    yield dut.link_up.eq(1)
                    yield from wait(16)
                    for adr in range(4, 8):
                        self.assertEqual((yield from write(dut.a, B, adr, pattern(adr))), FABRIC_OK)
                        self.assertEqual((yield from read(dut.a, B, adr)), (pattern(adr), FABRIC_OK))
                    self.assertEqual((yield from write(dut.b, A, 1, pattern(1))), FABRIC_OK)
                    # Requests before the loss: executed whole or not at all.
                    for adr in range(2):
                        self.assertIn((yield dut.b_mem.mem[adr]), [0, pattern(adr)])
                run_simulation(dut, generator())

    def test_backpressure(self):
        # Smallest buffers and a slow memory: requests wait in the nodes (credits), none is dropped
        # and none is lost.
        for link in self.links():
            with self.subTest(link=link):
                dut = DUT(link=link, buffer_depth=2, queue_depth=1, request_timeout=30000, memory_timeout=1024,
                    mem_kwargs=dict(latency=100))
                n = 24
                def sender():
                    for i in range(n):
                        yield from request(dut.a, i, we=1, dat_w=pattern(i, 3), dst=B)
                def receiver():
                    r = yield from results(dut.a, 8)
                    statuses = [status for _, status, _ in r.values()]
                    for _ in range(2):
                        r = yield from results(dut.a, 8)
                        statuses += [status for _, status, _ in r.values()]
                    self.assertEqual(statuses, [FABRIC_OK]*n)
                    for i in range(n):
                        self.assertEqual((yield dut.b_mem.mem[i]), pattern(i, 3))
                    for node in [dut.a, dut.b]:
                        self.assertEqual((yield node.ports[0].rx_drops), 0)
                run_simulation(dut, [sender(), receiver()])

    def test_single_place_queues(self):
        # A single place per class and a packet lost: sent again when the neighbor asks or after
        # the timeout, credits are not lost.
        dut = DUT(queue_depth=1, request_timeout=4000)
        def generator():
            yield dut.a2b.drop.eq(1)
            for adr in range(6):
                self.assertEqual((yield from write(dut.a, B, adr, pattern(adr))), FABRIC_OK)
            self.assertEqual((yield dut.a.ports[0].retransmits), 1)
            self.assertEqual((yield dut.b.ports[0].rx_drops), 0)
            self.assertEqual((yield dut.b.responder.requests), 6)
        run_simulation(dut, generator())

    def test_invalid_packets(self):
        # Packets that are not fabric packets, or malformed, are dropped without side effect.
        dut = DUTInject()
        def send(words):
            for i, word in enumerate(words):
                yield dut.b.sink.valid.eq(1)
                yield dut.b.sink.data.eq(word)
                yield dut.b.sink.last.eq(i == (len(words) - 1))
                yield
            yield dut.b.sink.valid.eq(0)
            yield
        def generator():
            route = (0xe << 28) | (1 << 16) | (A << 8) | B
            def seq(n):
                return n << 19 # Sequence number on the link.
            yield from send([0x12345678, 0x1, 0x2])                   # Invalid magic.
            yield from send([route])                                  # Route word only.
            yield from send([route & ~(0xff << 16), 0xf14, 4])        # Hop limit 0.
            yield from send([route | (2 << 16), 0xf14, 4])            # Hop limit above max_hops.
            yield from send([route, (1 << 12) | (0xf << 8) | 0x12, 4, 0xdead, 0xbeef, 0xcafe]) # Too long.
            yield from send([route | seq(0), (1 << 12) | (0xf << 8) | 0x11]) # Write without address.
            yield from send([route | seq(0), (0xf << 8) | 0x16, 4, 0x1234])  # Read with data.
            yield from wait(64)
            # Not accepted by the port (so sent again on a real link), nothing reaches the Responder.
            self.assertEqual((yield dut.b.ports[0].rx_invalid), 7)
            self.assertEqual((yield dut.b.responder.malformed), 0)
            self.assertEqual((yield dut.b.responder.requests), 0)
            self.assertEqual((yield dut.b_mem.mem[4]), 0)
            # Valid write then read still served.
            # Not the sequence number expected, and a write whose data would be seen.
            yield from send([route | seq(3), (1 << 12) | (0xf << 8) | 0x15, 5, 0x5555])
            # Valid write then read still served.
            yield from send([route | seq(0), (1 << 12) | (0xf << 8) | 0x13, 4, 0x4444])
            yield from send([route | seq(1), (0xf << 8) | 0x14, 4])
            yield from send([route | seq(0), (1 << 12) | (0xf << 8) | 0x13, 4, 0x9999]) # Sent again: discarded.
            yield from wait(64)
            self.assertEqual((yield dut.b_mem.mem[4]), 0x4444)
            self.assertEqual((yield dut.b_mem.mem[5]), 0)
            self.assertEqual((yield dut.b.responder.requests), 2)
            self.assertEqual((yield dut.b.ports[0].rx_discards), 2)
            # Responses numbered 0 and 1 (and sent again later: nothing acknowledges them here).
            self.assertEqual(dut.responses[:2], [
                [(0xe << 28) | (1 << 24) | seq(0) | (1 << 16) | (B << 8) | A, (1 << 12) | 0x13],
                [(0xe << 28) | (1 << 24) | seq(1) | (1 << 16) | (B << 8) | A, 0x14, 0x4444],
            ])
        run_simulation(dut, [generator(), passive(dut.monitor)()])

    def test_wishbone(self):
        for link in self.links():
            with self.subTest(link=link):
                dut = DUT(link=link, mem_kwargs=dict(hang_adr=20))
                dut.wb = FabricWishbone(dut.a, dst=B, size=192, origin=0x30000000)
                base = 0x30000000//4
                def access(adr, **kwargs):
                    return bus_access(dut.wb.bus, base + adr, timeout=20000, **kwargs)
                def generator():
                    for adr in range(4):
                        self.assertEqual((yield from access(adr, we=1, dat_w=pattern(adr)))[1], 0)
                    for adr in range(4):
                        self.assertEqual((yield from access(adr)), (pattern(adr), 0))
                    for adr, status in [(40, FABRIC_RANGE), (48, FABRIC_RANGE), (20, FABRIC_TIMEOUT)]:
                        self.assertEqual((yield from access(adr, we=1, dat_w=1))[1], 1)
                        self.assertEqual((yield dut.wb.status), status)
                    self.assertEqual((yield from access(1)), (pattern(1), 0))
                    self.assertEqual((yield dut.wb.status), FABRIC_OK)
                    self.assertEqual((yield dut.wb.errors), 3)
                run_simulation(dut, generator())

class DUTInject(LiteXModule):
    """Node B alone, packets injected on its input, responses recorded."""
    def __init__(self):
        self.b     = FabricNode(node_id=B, size=128)
        self.b_mem = Memory(self.b.responder.mem)
        self.responses = []
        self.comb += self.b.source.ready.eq(1)

    def monitor(self):
        packet = []
        while True:
            if (yield self.b.source.valid):
                packet.append((yield self.b.source.data))
                if (yield self.b.source.last):
                    if not (packet[0] & (3 << 25)): # Flow control and route packets not recorded.
                        self.responses.append(packet)
                    packet = []
            yield

if __name__ == "__main__":
    unittest.main()
