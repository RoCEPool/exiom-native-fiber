#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric nodes with a memory port (FabricPipelinedResponder, exiom/memory.py): the Responder sends an
access per cycle instead of one every two cycles.

The tests of the bursts, of the copies and of the 4-node networks are run again with these nodes
(``defaults.pipelined``); the tests below are those of what differs: errors and timeouts in the
middle of the accesses in flight, a memory that stalls, a Wishbone slave behind the port, and the
rate.
"""

import unittest

from migen import *

from litex.gen import *
from litex.gen.sim import *

from litex.soc.interconnect import wishbone

from exiom.fabric   import *
from exiom.topology import *
from exiom.network  import *
from exiom.memory   import *

import test.test_fabric         as test_fabric
import test.test_fabric_burst   as test_fabric_burst
import test.test_fabric_copy    as test_fabric_copy
import test.test_fabric_network as test_fabric_network

from test.test_fabric       import DUT as DUT2, A, B, defaults, request, result, write, read, wait
from test.test_fabric_burst import BURST, burst_request, burst_result, burst_write, burst_read, values
from test.fabric_sim        import run_simulation

# Existing tests, with memory ports ----------------------------------------------------------------

class Pipelined:
    def setUp(self):
        defaults.pipelined = True

    def tearDown(self):
        defaults.pipelined = False

class TestFabricBurstPipelined(Pipelined, test_fabric_burst.TestFabricBurst):
    # Errors in the middle of a burst: below (the words after the error are accessed too).
    test_burst_errors = None

class TestFabricCopyPipelined(Pipelined, test_fabric_copy.TestFabricCopy):
    test_copy_memory_errors = None

class TestFabricPoolPipelined(Pipelined, test_fabric_copy.TestFabricPool):
    pass

class TestFabricNetworkPipelined(Pipelined, test_fabric_network.TestFabricNetwork):
    pass

# The 4-node networks of every push only.
for _name in dir(TestFabricNetworkPipelined):
    if _name.startswith("test_") and _name not in ["test_4_nodes", "test_4_nodes_wide",
        "test_4_nodes_backpressure", "test_4_nodes_lossy"]:
        setattr(TestFabricNetworkPipelined, _name, None)

# Test Fabric Pipelined ----------------------------------------------------------------------------

class TestFabricPipelined(unittest.TestCase):
    def test_requests(self):
        # Single words, every status, to the other node and to the node itself.
        dut = DUT2(pipelined=True, mem_kwargs=dict(err_adr=20, hang_adr=26), memory_timeout=64)
        def generator():
            for dst in [B, A]:
                self.assertEqual((yield from write(dut.a, dst, 3, 0x12345678)), FABRIC_OK)
                self.assertEqual((yield from write(dut.a, dst, 3, 0xab000000, sel=0b1000)), FABRIC_OK)
                self.assertEqual((yield from read(dut.a, dst, 3)), (0xab345678, FABRIC_OK))
                self.assertEqual((yield from read(dut.a, dst, 32))[1], FABRIC_RANGE)
                self.assertEqual((yield from read(dut.a, dst, 20)), (0, FABRIC_BUS))
                self.assertEqual((yield from write(dut.a, dst, 20, 1)), FABRIC_BUS)
                self.assertEqual((yield from read(dut.a, dst, 26)), (0, FABRIC_TIMEOUT))
                # Still working after the timeout.
                self.assertEqual((yield from read(dut.a, dst, 3)), (0xab345678, FABRIC_OK))
            self.assertEqual((yield from read(dut.a, 9, 0))[1], FABRIC_NODE)
        run_simulation(dut, generator())

    def test_burst_errors(self):
        dut = DUT2(pipelined=True, mem_kwargs=dict(err_adr=20, hang_adr=26), memory_timeout=64)
        def generator():
            node = dut.a
            self.assertEqual((yield from burst_write(node, B, 28, values(1, 5))), (FABRIC_RANGE, 0))
            self.assertEqual((yield from burst_read(node, B, 32, 1)), (FABRIC_RANGE, []))
            self.assertEqual((yield from burst_write(node, B, 28, values(1, 4))), (FABRIC_OK, 4))
            # Error in the middle: the words before are reported done; the words after are
            # accessed too (the accesses do not wait for the answers).
            self.assertEqual((yield from burst_write(node, B, 17, values(0x700, 6))), (FABRIC_BUS, 3))
            for i, word in zip([17, 18, 19, 20, 21, 22], values(0x700, 3) + [0] + values(0x700, 6)[4:]):
                self.assertEqual((yield dut.b_mem.mem[i]), word)
            self.assertEqual((yield from burst_read(node, B, 17, 6)), (FABRIC_BUS, values(0x700, 3)))
            # A word never answered: no word reported done; the answers that come later for this
            # request are dropped, the next requests are not disturbed.
            self.assertEqual((yield from burst_read(node, B, 24, 4)), (FABRIC_TIMEOUT, []))
            self.assertEqual((yield from burst_write(node, B, 24, values(0x900, 4))), (FABRIC_TIMEOUT, 0))
            self.assertEqual((yield from burst_read(node, B, 28, 4)), (FABRIC_OK, values(1, 4)))
            self.assertEqual((yield from burst_read(node, B, 17, 3)), (FABRIC_OK, values(0x700, 3)))
            self.assertEqual((yield dut.b.responder.late), 0)
        run_simulation(dut, generator())

    def test_in_flight_with_errors(self):
        # 8 requests in flight, some of them failing: each result is the one of its request.
        dut = DUT2(pipelined=True, mem_kwargs=dict(err_adr=5, hang_adr=6), memory_timeout=64, size=256)
        def generator():
            node = dut.a
            for adr in range(16):
                if adr not in [5, 6]:
                    self.assertEqual((yield from write(node, B, adr, 0x1000 + adr)), FABRIC_OK)
            for round_ in range(3):
                tags = {}
                for adr in [0, 5, 1, 6, 2, 7, 3, 4]:
                    tags[(yield from request(node, adr, dst=B))] = adr
                for _ in range(8):
                    tag, we, status, dat_r = yield from result(node)
                    adr = tags.pop(tag)
                    expected = {5: (FABRIC_BUS, 0), 6: (FABRIC_TIMEOUT, 0)}.get(adr, (FABRIC_OK, 0x1000 + adr))
                    self.assertEqual((status, dat_r), expected, adr)
        run_simulation(dut, generator())

    def test_memory_stalls(self):
        # Memory that stops taking accesses from time to time, and that answers late.
        dut = DUT2(pipelined=True, mem_kwargs=dict(latency=3), size=256, link_width=128)
        def stall():
            for i in range(600):
                yield dut.b_mem.stall.eq((i % 7) < 3)
                yield
            yield dut.b_mem.stall.eq(0)
        def generator():
            for i in range(6):
                words = values(0x40000000 + (i << 16), BURST)
                self.assertEqual((yield from burst_write(dut.a, B, 8*i, words)), (FABRIC_OK, BURST))
                self.assertEqual((yield from burst_read(dut.a, B, 8*i, BURST)), (FABRIC_OK, words))
        run_simulation(dut, [generator(), stall()])

    def test_memory_stuck(self):
        # Memory that takes no access at all for longer than the watchdog: the request ends with
        # a timeout, and the node works again afterwards.
        dut = DUT2(pipelined=True, memory_timeout=64, size=256)
        def generator():
            self.assertEqual((yield from burst_write(dut.a, B, 0, values(7, 4))), (FABRIC_OK, 4))
            yield dut.b_mem.stall.eq(1)
            tag = yield from burst_request(dut.a, B, 0, 4)
            rtag, we, status, count, words = yield from burst_result(dut.a)
            self.assertEqual((rtag, status, count), (tag, FABRIC_TIMEOUT, 0))
            yield dut.b_mem.stall.eq(0)
            self.assertEqual((yield from burst_read(dut.a, B, 0, 4)), (FABRIC_OK, values(7, 4)))
        run_simulation(dut, generator())

    def test_wishbone_memory(self):
        # A Wishbone slave behind the memory port.
        class WishboneDUT(LiteXModule):
            def __init__(self):
                self.net   = FabricNetwork(two_tier(2), size=256, pipelined=True)
                self.nodes = self.net.nodes
                self.srams = []
                for n, node in enumerate(self.nodes):
                    adapter = FabricMemoryWishbone(node.responder.cmd, node.responder.rsp)
                    sram    = wishbone.SRAM(256)
                    self.add_module(f"adapter{n}", adapter)
                    self.add_module(f"sram{n}", sram)
                    self.comb += adapter.bus.connect(sram.bus)
                    self.srams.append(sram)
        dut = WishboneDUT()
        def generator():
            node  = dut.nodes[0]
            words = values(0x51000000, BURST)
            self.assertEqual((yield from burst_write(node, 1, 4, words)), (FABRIC_OK, BURST))
            for i in range(BURST):
                self.assertEqual((yield dut.srams[1].mem[4 + i]), words[i])
            self.assertEqual((yield from burst_read(node, 1, 4, BURST)), (FABRIC_OK, words))
            self.assertEqual((yield from write(node, 1, 0, 0xcafe)), FABRIC_OK)
            self.assertEqual((yield from read(node, 1, 0)), (0xcafe, FABRIC_OK))
        run_simulation(dut, generator())

    def test_rate(self):
        # Words per cycle served by one Responder: bursts of 8 words from 8 slots, 128-bit link.
        rates = {}
        for pipelined in [False, True]:
            dut = DUT2(pipelined=pipelined, size=256, link_width=128, queue_depth=4, buffer_depth=4)
            def generator():
                node, sink, source = dut.a, dut.a.requester.sink, dut.a.requester.source
                yield source.ready.eq(1)
                yield sink.burst.eq(1)
                yield sink.count.eq(BURST)
                yield sink.dst.eq(B)
                yield sink.valid.eq(1)
                done = 0
                for cycle in range(600):
                    if (yield source.valid):
                        self.assertEqual((yield source.status), FABRIC_OK)
                        if cycle >= 100:
                            done += (yield source.count)
                    yield
                rates[pipelined] = done/500
            run_simulation(dut, generator())
        self.assertGreater(rates[False], 0.40)             # One word every two cycles.
        self.assertGreater(rates[True], 1.6*rates[False])
        self.rates = rates
        print(f"Words per cycle: {rates}")

if __name__ == "__main__":
    unittest.main()
