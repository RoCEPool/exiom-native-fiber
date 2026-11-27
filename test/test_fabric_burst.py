#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric bursts: 1 to FABRIC_BURST_WORDS words read or written by one request.
"""

import random
import unittest

from migen import *

from litex.gen import *
from litex.gen.sim import *

from exiom.fabric   import *
from exiom.topology import *
from exiom.network  import *

from test.test_fabric         import DUT as DUT2, A, B, request, write, read, wait
from test.test_fabric_network import DUT as NetDUT, run, parallel
from test.fabric_sim          import run_simulation

BURST = FABRIC_BURST_WORDS

# Helpers ------------------------------------------------------------------------------------------

def pack(words):
    return sum(word << (32*i) for i, word in enumerate(words))

def unpack(value, count):
    return [(value >> (32*i)) & 0xffffffff for i in range(count)]

def burst_request(node, dst, adr, count, words=None):
    """Sends one burst request (write when ``words`` is given), returns its tag."""
    sink = node.requester.sink
    yield sink.burst.eq(1)
    yield sink.count.eq(count)
    yield sink.data.eq(pack(words or []))
    tag = yield from request(node, adr, we=int(words is not None), dst=dst)
    yield sink.burst.eq(0)
    yield sink.count.eq(0)
    yield sink.data.eq(0)
    return tag

def burst_result(node, timeout=20000):
    """Waits for one result, returns (tag, we, status, words done, words read)."""
    source = node.requester.source
    yield source.ready.eq(1)
    yield
    for _ in range(timeout):
        if (yield source.valid):
            break
        yield
    else:
        raise TimeoutError("No result.")
    count = (yield source.count)
    r = ((yield source.tag), (yield source.we), (yield source.status), count, unpack((yield source.data), count))
    yield source.ready.eq(0)
    yield
    return r

def burst_write(node, dst, adr, words):
    tag = yield from burst_request(node, dst, adr, len(words), words)
    rtag, we, status, count, _ = yield from burst_result(node)
    assert (rtag, we) == (tag, 1)
    return status, count

def burst_write_count(node, dst, adr, count, words):
    """Burst write with a number of words that is not the length of ``words``."""
    sink = node.requester.sink
    yield sink.burst.eq(1)
    yield sink.count.eq(count)
    yield sink.data.eq(pack(words))
    tag = yield from request(node, adr, we=1, dst=dst)
    yield sink.burst.eq(0)
    yield sink.count.eq(0)
    yield sink.data.eq(0)
    rtag, we, status, count, _ = yield from burst_result(node)
    assert (rtag, we) == (tag, 1)
    return status, count

def burst_read(node, dst, adr, count):
    tag = yield from burst_request(node, dst, adr, count)
    rtag, we, status, count, words = yield from burst_result(node)
    assert (rtag, we) == (tag, 0)
    return status, words

def values(base, count):
    return [(base + 0x01010101*i) & 0xffffffff for i in range(count)]

# Test Fabric Burst --------------------------------------------------------------------------------

class TestFabricBurst(unittest.TestCase):
    def test_sizes(self):
        # Longest packet: burst write request; a copy request fits.
        self.assertEqual(FABRIC_PACKET_WORDS, 3 + BURST)
        self.assertGreaterEqual(FABRIC_PACKET_WORDS, 6)
        self.assertLessEqual(BURST, 15)

    def test_burst(self):
        # Every size, on every link width, to the other node and to the node itself.
        for link_width in [32, 64, 128]:
            with self.subTest(link_width=link_width):
                dut = DUT2(link_width=link_width, size=256)
                def generator():
                    for dst, mem in [(B, dut.b_mem), (A, dut.a_mem)]:
                        adr = 0
                        for count in range(1, BURST + 1):
                            words = values(0x10000000*count + dst, count)
                            self.assertEqual((yield from burst_write(dut.a, dst, adr, words)), (FABRIC_OK, count))
                            for i in range(count):
                                self.assertEqual((yield mem.mem[adr + i]), words[i])
                            self.assertEqual((yield mem.mem[adr + count]), 0) # Not one word more.
                            self.assertEqual((yield from burst_read(dut.a, dst, adr, count)), (FABRIC_OK, words))
                            adr += count
                        # Requests of one word on the same words (words 1 and 2: burst of 2 words).
                        self.assertEqual((yield from read(dut.a, dst, 2)), (values(0x20000000 + dst, 2)[1], FABRIC_OK))
                        self.assertEqual((yield from write(dut.a, dst, 2, 0x55aa55aa)), FABRIC_OK)
                        self.assertEqual((yield from burst_read(dut.a, dst, 1, 2)), (FABRIC_OK, [0x20000000 + dst, 0x55aa55aa]))
                    self.assertEqual((yield dut.a.ports[0].rx_invalid), 0)
                    self.assertEqual((yield dut.b.ports[0].rx_invalid), 0)
                    self.assertEqual((yield dut.b.responder.malformed), 0)
                run_simulation(dut, generator())

    def test_burst_serwb(self):
        # Through SerWB (word-level PHY model).
        dut = DUT2(link="serwb")
        def generator():
            yield from wait(64)
            words = values(0xabc00000, BURST)
            self.assertEqual((yield from burst_write(dut.a, B, 8, words)), (FABRIC_OK, BURST))
            self.assertEqual((yield from burst_read(dut.a, B, 8, BURST)), (FABRIC_OK, words))
            self.assertEqual((yield from burst_read(dut.b, A, 0, 3)), (FABRIC_OK, [0, 0, 0]))
        run_simulation(dut, generator())

    def test_burst_errors(self):
        # Window of 32 words; word 20 answers with an error, word 26 never answers.
        dut = DUT2(mem_kwargs=dict(err_adr=20, hang_adr=26), memory_timeout=64)
        def generator():
            node = dut.a
            # Out of the window: nothing done.
            self.assertEqual((yield from burst_write(node, B, 28, values(1, 5))), (FABRIC_RANGE, 0))
            self.assertEqual((yield from burst_read(node, B, 32, 1)), (FABRIC_RANGE, []))
            for i in range(28, 32):
                self.assertEqual((yield dut.b_mem.mem[i]), 0)
            # Up to the last word.
            self.assertEqual((yield from burst_write(node, B, 28, values(1, 4))), (FABRIC_OK, 4))
            # Error in the middle: the words before are done, the words after are not.
            self.assertEqual((yield from burst_write(node, B, 17, values(0x700, 6))), (FABRIC_BUS, 3))
            for i, word in zip([17, 18, 19, 21, 22], values(0x700, 3) + [0, 0]):
                self.assertEqual((yield dut.b_mem.mem[i]), word)
            self.assertEqual((yield from burst_read(node, B, 17, 6)), (FABRIC_BUS, values(0x700, 3)))
            self.assertEqual((yield from burst_read(node, B, 24, 4)), (FABRIC_TIMEOUT, [0, 0]))
            # Sizes that are not those of a burst: not sent.
            for count in [0, BURST + 1, 15]:
                self.assertEqual((yield from burst_read(node, B, 0, count))[0], FABRIC_RANGE)
                self.assertEqual((yield from burst_write_count(node, B, 0, count, values(3, BURST)))[0], FABRIC_RANGE)
            self.assertEqual((yield dut.b_mem.mem[0]), 0)
            # Node that does not exist.
            self.assertEqual((yield from burst_read(node, 9, 0, 4))[0], FABRIC_NODE)
            # Still working.
            self.assertEqual((yield from burst_read(node, B, 28, 4)), (FABRIC_OK, values(1, 4)))
        run_simulation(dut, generator())

    def test_burst_read_only(self):
        dut = DUT2(read_only=True)
        def generator():
            self.assertEqual((yield from burst_write(dut.a, B, 0, values(1, 4))), (FABRIC_RO, 0))
            self.assertEqual((yield from burst_read(dut.a, B, 0, 4)), (FABRIC_OK, [0]*4))
        run_simulation(dut, generator())

    def test_burst_in_flight(self):
        # 8 bursts in flight in each direction, packets lost on the links: each node writes its
        # own words in the other node, then reads them; everything is done once.
        for loss in [0, 32]:
            with self.subTest(loss=loss):
                dut = DUT2(loss=loss, link_width=128)
                def node_generator(node, dst, base, seed):
                    prng  = random.Random(seed)
                    model = {}
                    for round_ in range(3):
                        tags = {}
                        for i in range(8):
                            # 8 areas of BURST//2 words, each with one burst at a time.
                            count = prng.randint(1, BURST//2)
                            adr   = base + i*(BURST//2)
                            if round_ % 2 == 0:
                                words = [prng.getrandbits(32) for _ in range(count)]
                                tag = yield from burst_request(node, dst, adr, count, words)
                                tags[tag] = (adr, words, 1)
                            else:
                                tag = yield from burst_request(node, dst, adr, count)
                                tags[tag] = (adr, [model.get(adr + j, 0) for j in range(count)], 0)
                        for _ in range(8):
                            tag, we, status, count, words = yield from burst_result(node)
                            adr, expected, rwe = tags.pop(tag)
                            self.assertEqual((we, status, count), (rwe, FABRIC_OK, len(expected)))
                            if we:
                                for j, word in enumerate(expected):
                                    model[adr + j] = word
                            else:
                                self.assertEqual(words, expected)
                def generator():
                    yield from parallel(dut, [
                        node_generator(dut.a, B, 0, 1),
                        node_generator(dut.b, A, 0, 2),
                    ])
                    if loss:
                        self.assertGreater((yield dut.a.ports[0].retransmits) + (yield dut.b.ports[0].retransmits), 0)
                run_simulation(dut, generator())

    def test_burst_forwarded(self):
        # 8 nodes in two groups: bursts forwarded over 3 links, and all sizes at once.
        dut = NetDUT(two_tier(8), link_width=64)
        def generator():
            hops = dut.topology.distances()
            src, dst = max(((s, d) for s in range(8) for d in range(8)), key=lambda p: hops[p[0]][p[1]])
            self.assertEqual(hops[src][dst], 3)
            node = dut.nodes[src]
            for count in [1, 5, BURST]:
                words = values(0x33000000 + count, count)
                self.assertEqual((yield from burst_write(node, dst, 32, words)), (FABRIC_OK, count))
                self.assertEqual((yield from burst_read(node, dst, 32, count)), (FABRIC_OK, words))
            forwarded = 0
            for n in dut.nodes:
                forwarded += (yield n.forwarded)
            self.assertGreaterEqual(forwarded, 2*2*6) # Requests and responses, 2 nodes on the way.
            yield from dut.check_no_drop(self)
        run(dut, [generator()])

if __name__ == "__main__":
    unittest.main()
