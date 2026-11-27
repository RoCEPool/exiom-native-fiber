#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric: windows of 4 TiB per node (word addresses of more than 32 bits).

The memory of a node is a model: a word reads as a pattern of its address until it is written
(the last words written are kept), so that any address of the window can be used.
"""

import unittest

from migen import *

from litex.gen import *
from litex.gen.sim import *

from litex.soc.interconnect import wishbone

from exiom.fabric   import *
from exiom.topology import *
from exiom.network  import *
from exiom.pool     import *

from test.test_fabric      import request, result, write, read, wait
from test.test_fabric_copy import copy, pool_op
from test.fabric_sim       import run_simulation

SIZE        = 4*2**40    # Bytes per node: 4 TiB.
WORDS       = SIZE//4    # 2**40 words.
WINDOW_BITS = 40

# Memory model -------------------------------------------------------------------------------------

def pattern(node, adr):
    """Word read at an address never written."""
    return ((adr & 0xffffffff) ^ ((adr >> 32) * 0x01010101) ^ (node << 28) ^ 0x5a5a5a5a) & 0xffffffff

class SparseMemory(LiteXModule):
    """Wishbone memory of any size: pattern of the address, and the last ``entries`` words
    written."""
    def __init__(self, bus, node, entries=64):
        self.adrs   = adrs   = [Signal(len(bus.adr), name=f"entry{i}_adr") for i in range(entries)]
        self.dats   = dats   = [Signal(32, name=f"entry{i}_dat") for i in range(entries)]
        self.valids = valids = [Signal(name=f"entry{i}_valid") for i in range(entries)]
        self.writes = writes = Signal(32)
        next_ = Signal(max=entries)
        hit   = Signal()
        dat   = Signal(32)
        self.comb += dat.eq(bus.adr[:32] ^ (bus.adr[32:]*0x01010101)[:32] ^ (node << 28) ^ 0x5a5a5a5a)
        for i in range(entries):
            self.comb += If(valids[i] & (adrs[i] == bus.adr), hit.eq(1), dat.eq(dats[i]))
        self.sync += [
            bus.ack.eq(bus.cyc & bus.stb & ~bus.ack),
            If(bus.cyc & bus.stb & ~bus.ack,
                bus.dat_r.eq(dat),
                If(bus.we,
                    writes.eq(writes + 1),
                    # Word already kept: replaced. Otherwise the oldest entry is taken.
                    *[If(valids[i] & (adrs[i] == bus.adr), dats[i].eq(bus.dat_w)) for i in range(entries)],
                    If(~hit,
                        *[If(next_ == i, adrs[i].eq(bus.adr), dats[i].eq(bus.dat_w), valids[i].eq(1))
                            for i in range(entries)],
                        next_.eq(Mux(next_ == entries - 1, 0, next_ + 1)),
                    )
                )
            )
        ]

class DUT(LiteXModule):
    def __init__(self, nodes=2, pool=True, **kwargs):
        self.net   = net = FabricNetwork(two_tier(nodes), size=SIZE, **kwargs)
        self.nodes = net.nodes
        self.mems  = []
        for n, node in enumerate(net.nodes):
            mem = SparseMemory(node.responder.mem, n)
            self.add_module(f"mem{n}", mem)
            self.mems.append(mem)
        if pool:
            self.pool = FabricPool(net.nodes[0], window_bits=WINDOW_BITS, chunk=8, busy_wait=16)

def g(node, word):
    return pool_address(node, word, WINDOW_BITS)

# Test Fabric Address ------------------------------------------------------------------------------

class TestFabricAddress(unittest.TestCase):
    def test_sizes(self):
        # 128 nodes of 4 TiB: 512 TiB, 49 bits of byte address; the last word of the last node.
        self.assertEqual(128*SIZE, 512*2**40)
        self.assertEqual(pool_split(g(127, WORDS - 1), WINDOW_BITS), (127, WORDS - 1))
        self.assertEqual(g(127, WORDS - 1).bit_length(), 47)
        self.assertEqual(g(255, WORDS - 1).bit_length(), WINDOW_BITS + 8)

    def test_read_write(self):
        # Words all over a window of 4 TiB, in the node itself and in another node.
        dut = DUT(pool=False)
        def generator():
            node = dut.nodes[0]
            self.assertEqual(len(dut.nodes[1].responder.mem.adr), 40)
            for dst in [1, 0]:
                for i, adr in enumerate([0, 2**32 - 1, 2**32, 2**32 + 5, 2**39 + 2**33 + 7, WORDS - 1]):
                    self.assertEqual((yield from read(node, dst, adr)), (pattern(dst, adr), FABRIC_OK), hex(adr))
                    value = 0xc0de0000 + 16*dst + i
                    self.assertEqual((yield from write(node, dst, adr, value)), FABRIC_OK)
                    self.assertEqual((yield from read(node, dst, adr)), (value, FABRIC_OK), hex(adr))
                # Words that differ by their high bits only are different words.
                self.assertEqual((yield from read(node, dst, 5)), (pattern(dst, 5), FABRIC_OK))
                # After the end of the window.
                for adr in [WORDS, WORDS + 5, 2**47]:
                    self.assertEqual((yield from read(node, dst, adr))[1], FABRIC_RANGE, hex(adr))
                    self.assertEqual((yield from write(node, dst, adr, 0)), FABRIC_RANGE, hex(adr))
            self.assertEqual((yield dut.mems[1].writes), 6)
        run_simulation(dut, generator())

    def test_copy(self):
        # Copies between addresses over 4 GiB of words, in both nodes.
        dut = DUT(pool=False)
        def generator():
            node = dut.nodes[0]
            src_adr, dst_adr = 2**39 + 100, 2**38 + 2**32 + 200
            self.assertEqual((yield from copy(node, 1, src_adr, 0, dst_adr, 16)), (FABRIC_OK, 16))
            for i in range(16):
                self.assertEqual((yield from read(node, 0, dst_adr + i)), (pattern(1, src_adr + i), FABRIC_OK))
            # The words of the low addresses were not touched.
            self.assertEqual((yield from read(node, 0, 200)), (pattern(0, 200), FABRIC_OK))
            # Up to the last word of the window; one word further is refused, nothing copied.
            self.assertEqual((yield from copy(node, 1, WORDS - 8, 0, 0, 8)), (FABRIC_OK, 8))
            self.assertEqual((yield from copy(node, 1, WORDS - 8, 0, 0, 9)), (FABRIC_RANGE, 0))
            # Destination past the end of its window: the words before the end are written.
            status, count = yield from copy(node, 0, 2**33, 1, WORDS - 4, 8)
            self.assertEqual(status, FABRIC_RANGE)
            self.assertLessEqual(count, 4)
        run_simulation(dut, generator())

    def test_pool(self):
        # Global addresses with 4 TiB per node.
        dut = DUT()
        def generator():
            pool = dut.pool
            high = 2**39 + 2**35 + 3
            self.assertEqual((yield from pool_op(pool, POOL_READ, g(1, high)))[:2], (FABRIC_OK, pattern(1, high)))
            self.assertEqual((yield from pool_op(pool, POOL_WRITE, g(1, high), dat_w=0x12345678))[0], FABRIC_OK)
            self.assertEqual((yield from pool_op(pool, POOL_READ, g(1, high)))[:2], (FABRIC_OK, 0x12345678))
            self.assertEqual((yield from pool_op(pool, POOL_READ, g(2, 0)))[0], FABRIC_NODE)
            # Copy of 20 words over the end of node 0 (12 words) and the start of node 1 (8 words),
            # to the high addresses of node 1.
            src, dst = g(0, WORDS - 12), g(1, high + 64)
            self.assertEqual((yield from pool_op(pool, POOL_COPY, src, dst_adr=dst, words=20)), (FABRIC_OK, 0, 20))
            for i in range(20):
                sn, sw = pool_split(src + i, WINDOW_BITS)
                self.assertEqual((yield from pool_op(pool, POOL_READ, dst + i))[:2], (FABRIC_OK, pattern(sn, sw)), i)
        run_simulation(dut, generator())

if __name__ == "__main__":
    unittest.main()
