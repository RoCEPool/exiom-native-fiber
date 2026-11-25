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

from liteiclink.serwb.core   import SERWBCore
from liteiclink.serwb.packet import PREAMBLE

from exiom.fabric import *

from test.test_serwb_link import LoopbackPHY, bus_access

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

# DUT ----------------------------------------------------------------------------------------------

class DUT(LiteXModule):
    """Nodes A (id 1) and B (id 2), each with its memory."""
    def __init__(self, link="direct", size=128, requests=8, request_timeout=4096, buffer_depth=64,
        read_only=False, memory_timeout=128, mem_kwargs={}):
        self.link_up = Signal(reset=1)

        # Nodes.
        node_kwargs = dict(size=size, requests=requests, request_timeout=request_timeout,
            memory_timeout=memory_timeout, buffer_depth=buffer_depth, read_only=read_only, link_ready=self.link_up)
        self.a = FabricNode(node_id=1, **node_kwargs)
        self.b = FabricNode(node_id=2, **node_kwargs)
        self.a_mem = Memory(self.a.responder.mem, **mem_kwargs)
        self.b_mem = Memory(self.b.responder.mem, **mem_kwargs)

        # Link.
        if link == "direct":
            self.comb += [
                self.a.source.connect(self.b.sink),
                self.b.source.connect(self.a.sink),
            ]
        else:
            # Packet of index `drop_x2y` of the direction is dropped (preamble replaced by an idle
            # word).
            self.drop_a2b = Signal(16, reset=2**16 - 1)
            self.drop_b2a = Signal(16, reset=2**16 - 1)
            self.a_phy = LoopbackPHY()
            self.b_phy = LoopbackPHY()
            for tx_phy, rx_phy, drop in [(self.a_phy, self.b_phy, self.drop_a2b), (self.b_phy, self.a_phy, self.drop_b2a)]:
                preamble = Signal()
                index    = Signal(16)
                self.comb += [
                    preamble.eq(tx_phy.tx.valid & (tx_phy.tx.data == PREAMBLE)),
                    tx_phy.tx.connect(rx_phy.rx),
                    If(preamble & (index == drop), rx_phy.rx.data.eq(0)),
                    tx_phy.init.ready.eq(self.link_up),
                ]
                self.sync += If(preamble, index.eq(index + 1))
            self.a_core = SERWBCore(self.a_phy, clk_freq=int(1e6), mode="slave")
            self.b_core = SERWBCore(self.b_phy, clk_freq=int(1e6), mode="master")
            self.a.connect_serwb(self.a_core)
            self.b.connect_serwb(self.b_core)

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
                    self.assertEqual((yield dut.b.responder.rx_drops), 0)
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

    def lost_packet_test(self, direction):
        # Third of 6 writes in flight lost (request or response): only this one fails.
        dut = DUT(link="serwb", request_timeout=2000, mem_kwargs=dict(latency=8))
        def generator():
            yield {"request": dut.drop_a2b, "response": dut.drop_b2a}[direction].eq(2)
            tags = []
            for adr in range(6):
                tags.append((yield from request(dut.a, adr, we=1, dat_w=pattern(adr), dst=B)))
            r = yield from results(dut.a, 6)
            for adr, tag in enumerate(tags):
                self.assertEqual(r[tag][1], FABRIC_NO_RESPONSE if adr == 2 else FABRIC_OK)
            # Lost request: not executed. Lost response: executed, but not confirmed.
            self.assertEqual((yield dut.b_mem.mem[2]), 0 if direction == "request" else pattern(2))
            for adr in [0, 1, 3, 4, 5]:
                self.assertEqual((yield dut.b_mem.mem[adr]), pattern(adr))
            # Following requests are fine.
            self.assertEqual((yield from write(dut.a, B, 6, pattern(6))), FABRIC_OK)
            self.assertEqual((yield from read(dut.a, B, 6)), (pattern(6), FABRIC_OK))
            self.assertEqual((yield dut.a.requester.stale), 0)
        run_simulation(dut, generator())

    def test_lost_request(self):
        self.lost_packet_test("request")

    def test_lost_response(self):
        self.lost_packet_test("response")

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
        for delay in range(0, 160, 3):
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

    def test_request_buffer_overflow(self):
        # Request buffer too small for the requests in flight (no flow control between nodes yet):
        # requests are dropped whole and counted, their Requester gets FABRIC_NO_RESPONSE, others
        # are served and nothing stalls.
        dut = DUT(buffer_depth=8, request_timeout=3000, mem_kwargs=dict(latency=100))
        def generator():
            tags = {}
            for adr in range(8):
                tags[(yield from request(dut.a, adr, we=1, dat_w=pattern(adr), dst=B))] = adr
            r       = yield from results(dut.a, 8)
            drops   = (yield dut.b.responder.rx_drops)
            ok      = [tags[tag] for tag, (_, status, _) in r.items() if status == FABRIC_OK]
            dropped = [tags[tag] for tag, (_, status, _) in r.items() if status == FABRIC_NO_RESPONSE]
            self.assertEqual(len(ok) + len(dropped), 8)
            self.assertGreater(drops, 0)
            self.assertEqual(len(dropped), drops)
            for adr in range(8):
                self.assertEqual((yield dut.b_mem.mem[adr]), pattern(adr) if adr in ok else 0)
            self.assertEqual((yield from write(dut.a, B, 9, pattern(9))), FABRIC_OK)
        run_simulation(dut, generator())

    def test_invalid_packets(self):
        # Packets that are not fabric packets, or malformed, are dropped without side effect.
        def send(words):
            for i, word in enumerate(words):
                yield dut.b.sink.valid.eq(1)
                yield dut.b.sink.data.eq(word)
                yield dut.b.sink.last.eq(i == (len(words) - 1))
                yield
            yield dut.b.sink.valid.eq(0)
            yield
        dut = DUTInject()
        def generator():
            route = (0xe << 28) | (16 << 16) | (A << 8) | B
            yield from send([0x12345678, 0x1, 0x2])                   # Invalid magic.
            yield from send([route])                                  # Route word only.
            yield from send([route, (1 << 12) | (0xf << 8) | 0x11])   # Write without address.
            yield from send([route, (1 << 12) | (0xf << 8) | 0x12, 4, 0xdead, 0xbeef, 0xcafe]) # Too long.
            yield from wait(64)
            self.assertEqual((yield dut.b.rx_invalid), 1)
            self.assertEqual((yield dut.b.responder.requests), 0)
            self.assertEqual((yield dut.b_mem.mem[4]), 0)
            # Valid write then read still served.
            yield from send([route, (1 << 12) | (0xf << 8) | 0x13, 4, 0x4444])
            yield from send([route, (0xf << 8) | 0x14, 4])
            yield from wait(64)
            self.assertEqual((yield dut.b_mem.mem[4]), 0x4444)
            self.assertEqual((yield dut.b.responder.requests), 2)
            self.assertEqual(dut.responses, [
                [(0xe << 28) | (1 << 24) | (16 << 16) | (B << 8) | A, (1 << 12) | 0x13],
                [(0xe << 28) | (1 << 24) | (16 << 16) | (B << 8) | A, 0x14, 0x4444],
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
                    self.responses.append(packet)
                    packet = []
            yield

if __name__ == "__main__":
    unittest.main()
