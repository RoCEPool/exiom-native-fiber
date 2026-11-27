#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric copies (FabricCopy in exiom/fabric.py) and global addresses (exiom/pool.py).

Word-level links, 2 and 4 nodes. Windows of 64 words; the copies use words 32 to 63 (the traffic
of the other scenarios uses the words under 32).
"""

import random
import unittest

from migen import *

from litex.gen import *
from litex.gen.sim import *

from exiom.fabric   import *
from exiom.topology import *
from exiom.network  import *
from exiom.pool     import *

from test.test_fabric         import request, result, write, read, wait
from test.test_fabric_network import DUT, run, parallel, random_traffic

WORDS = 64 # Words per window.

# Helpers ------------------------------------------------------------------------------------------

def data(node, word, salt=0):
    return (0xc0000000 | (salt << 20) | (node << 8) | word) & 0xffffffff

def fill(dut, node, first, words, salt=0):
    """Writes the pattern in the memory of a node (not through the fabric)."""
    for word in range(first, first + words):
        yield dut.mems[node].mem[word].eq(data(node, word, salt))

def check(test, dut, src, adr, dst, dst_adr, words, salt=0, msg=None):
    """Words of the destination: those of the source."""
    for i in range(words):
        test.assertEqual((yield dut.mems[dst].mem[dst_adr + i]), data(src, adr + i, salt), msg)

def copy_request(node, src, adr, dst, dst_adr, words):
    """Sends one copy request, returns its tag."""
    sink = node.requester.sink
    yield sink.copy.eq(1)
    yield sink.copy_dst.eq(dst)
    yield sink.words.eq(words)
    yield sink.copy_adr.eq(dst_adr)
    tag = yield from request(node, adr, dst=src)
    yield sink.copy.eq(0)
    yield sink.copy_dst.eq(0)
    yield sink.words.eq(0)
    yield sink.copy_adr.eq(0)
    return tag

def copy(node, src, adr, dst, dst_adr, words):
    """Copy, returns (status, words written)."""
    tag = yield from copy_request(node, src, adr, dst, dst_adr, words)
    rtag, we, status, count = yield from result(node, timeout=200000)
    assert (rtag, we) == (tag, 0)
    return status, count

def values(signals):
    """Values of a list of Signals."""
    r = []
    for signal in signals:
        r.append((yield signal))
    return r

def idle(test, dut):
    """Nothing left running, nothing dropped."""
    yield from wait(64)
    for node in dut.nodes:
        test.assertEqual((yield node.requester.pending), 0)
        test.assertEqual((yield node.copy.busy), 0)
    yield from dut.check_no_drop(test)

# Pool DUT -----------------------------------------------------------------------------------------

WINDOW_BITS = 6 # 64 words per node: the windows follow each other in the global addresses.

class PoolDUT(DUT):
    def __init__(self, topology, pools=(0,), chunk=8, **kwargs):
        DUT.__init__(self, topology, **kwargs)
        self.pools = {}
        for n in pools:
            pool = FabricPool(self.nodes[n], window_bits=WINDOW_BITS, chunk=chunk, busy_wait=16)
            self.add_module(f"pool{n}", pool)
            self.pools[n] = pool

def pool_op(pool, op, adr, dat_w=0, dst_adr=0, words=0, timeout=400000):
    """Operation with global addresses, returns (status, dat_r, words copied)."""
    yield pool.sink.op.eq(op)
    yield pool.sink.adr.eq(adr)
    yield pool.sink.dat_w.eq(dat_w)
    yield pool.sink.sel.eq(0b1111)
    yield pool.sink.dst_adr.eq(dst_adr)
    yield pool.sink.words.eq(words)
    yield pool.sink.valid.eq(1)
    yield pool.source.ready.eq(1)
    yield
    for _ in range(timeout):
        if (yield pool.source.valid):
            break
        yield
    else:
        raise TimeoutError("No result.")
    r = ((yield pool.source.status), (yield pool.source.dat_r), (yield pool.source.words))
    yield pool.sink.valid.eq(0)
    yield pool.source.ready.eq(0)
    yield
    return r

def g(node, word):
    return pool_address(node, word, WINDOW_BITS)

# Test Fabric Copy ---------------------------------------------------------------------------------

class TestFabricCopy(unittest.TestCase):
    def test_copy(self):
        # Asked by a node that is neither the source nor the destination, by the source, by the
        # destination, and inside one node.
        dut = DUT(two_tier(4))
        def generator():
            cases = [
                # asks, src, adr, dst, dst_adr, words
                (0, 1, 32, 2, 40, 16),
                (1, 1, 32, 3, 32, 32), # The whole end of the window.
                (2, 3, 48, 2, 33,  1),
                (0, 0, 32, 0, 48, 16), # Inside the node that asks.
                (3, 2, 40, 2, 56,  8), # Inside another node.
            ]
            for salt, (asks, src, adr, dst, dst_adr, words) in enumerate(cases):
                yield from fill(dut, src, adr, words, salt)
                requests = yield from values([node.responder.requests for node in dut.nodes])
                status, count = yield from copy(dut.nodes[asks], src, adr, dst, dst_adr, words)
                self.assertEqual((status, count), (FABRIC_OK, words), cases[salt])
                yield from check(self, dut, src, adr, dst, dst_adr, words, salt, cases[salt])
                # The words went from the source to the destination: the node that asked only
                # sent the copy request (nothing executed by its Responder, unless it takes part).
                if asks not in [src, dst]:
                    self.assertEqual((yield dut.nodes[asks].responder.requests), requests[asks])
                    self.assertEqual((yield dut.nodes[asks].forwarded), 0)
            self.assertEqual((yield from values([node.copy.copies for node in dut.nodes])), [1, 2, 1, 1])
            self.assertEqual(sum((yield from values([node.copy.words for node in dut.nodes]))), 16 + 32 + 1 + 16 + 8)
            yield from idle(self, dut)
        run(dut, [generator()])

    def test_copy_two_nodes(self):
        # Point to point link widths.
        for link_width in [32, 128]:
            with self.subTest(link_width=link_width):
                dut = DUT(two_tier(2), link_width=link_width)
                def generator():
                    yield from fill(dut, 0, 32, 32)
                    yield from fill(dut, 1, 32, 16, salt=1)
                    self.assertEqual((yield from copy(dut.nodes[0], 0, 32, 1, 0, 32)), (FABRIC_OK, 32))
                    self.assertEqual((yield from copy(dut.nodes[0], 1, 32, 0, 0, 16)), (FABRIC_OK, 16))
                    yield from check(self, dut, 0, 32, 1, 0, 32)
                    yield from check(self, dut, 1, 32, 0, 0, 16, salt=1)
                    yield from idle(self, dut)
                run(dut, [generator()])

    def test_copy_errors(self):
        dut = DUT(two_tier(4))
        def generator():
            node = dut.nodes[0]
            yield from fill(dut, 1, 32, 32)
            # Source out of the window (start, end): nothing copied.
            self.assertEqual((yield from copy(node, 1, 64, 2, 32, 1)),  (FABRIC_RANGE, 0))
            self.assertEqual((yield from copy(node, 1, 60, 2, 32, 5)),  (FABRIC_RANGE, 0))
            self.assertEqual((yield node.requester.source.valid), 0)
            # Destination out of the window: the words before the end are written.
            status, count = yield from copy(node, 1, 32, 2, 60, 8)
            self.assertEqual(status, FABRIC_RANGE)
            self.assertLessEqual(count, 4)
            # Destination node that does not exist, source node that does not exist.
            self.assertEqual((yield from copy(node, 1, 32, 9, 32, 4)),  (FABRIC_NODE, 0))
            self.assertEqual((yield from copy(node, 9, 32, 2, 32, 4)),  (FABRIC_NODE, 0))
            # Still working.
            self.assertEqual((yield from copy(node, 1, 32, 3, 32, 8)),  (FABRIC_OK, 8))
            yield from check(self, dut, 1, 32, 3, 32, 8)
            self.assertEqual((yield dut.nodes[1].copy.errors), 2)
            yield from idle(self, dut)
        run(dut, [generator()])

    def test_copy_memory_errors(self):
        # Word of the source never answered, word of the destination answered with an error.
        dut = DUT(two_tier(4), mem_kwargs=dict(hang_adr=40, err_adr=50), memory_timeout=64)
        def generator():
            node = dut.nodes[0]
            yield from fill(dut, 1, 32, 32)
            status, count = yield from copy(node, 1, 36, 2, 0, 8)   # Reads word 40.
            self.assertEqual(status, FABRIC_TIMEOUT)
            self.assertLessEqual(count, 4)
            status, count = yield from copy(node, 1, 32, 2, 48, 4)  # Writes word 50.
            self.assertEqual((status, count), (FABRIC_BUS, 2))      # Words before the error.
            self.assertEqual((yield from copy(node, 1, 32, 2, 0, 8)), (FABRIC_OK, 8))
            yield from idle(self, dut)
        run(dut, [generator()])

    def test_copy_busy(self):
        # Second copy asked to a node that is copying and runs one copy at a time: refused at
        # once, the first one goes on.
        dut = DUT(two_tier(4), copy_lanes=1)
        def generator():
            yield from fill(dut, 1, 32, 32)
            first = yield from copy_request(dut.nodes[0], 1, 32, 2, 32, 32)
            yield from wait(16)
            status, count = yield from copy(dut.nodes[3], 1, 32, 3, 32, 4)
            self.assertEqual((status, count), (FABRIC_BUSY, 0))
            tag, we, status, count = yield from result(dut.nodes[0])
            self.assertEqual((tag, status, count), (first, FABRIC_OK, 32))
            yield from check(self, dut, 1, 32, 2, 32, 32)
            # Free again.
            self.assertEqual((yield from copy(dut.nodes[3], 1, 32, 3, 32, 4)), (FABRIC_OK, 4))
            yield from idle(self, dut)
        run(dut, [generator()])

    def test_copy_lanes(self):
        # Two copies at a time per node (default): two nodes copy from the same node at the same
        # time, a third one is refused.
        dut = DUT(two_tier(4))
        def generator():
            yield from fill(dut, 1, 32, 32)
            first  = yield from copy_request(dut.nodes[0], 1, 32, 2, 32, 32)
            second = yield from copy_request(dut.nodes[3], 1, 32, 3, 32, 32)
            yield from wait(20)
            self.assertEqual((yield dut.nodes[1].copy.busy), 2)
            self.assertEqual((yield from copy(dut.nodes[2], 1, 32, 0, 32, 4)), (FABRIC_BUSY, 0))
            tag, we, status, count = yield from result(dut.nodes[0])
            self.assertEqual((tag, status, count), (first, FABRIC_OK, 32))
            tag, we, status, count = yield from result(dut.nodes[3])
            self.assertEqual((tag, status, count), (second, FABRIC_OK, 32))
            yield from check(self, dut, 1, 32, 2, 32, 32)
            yield from check(self, dut, 1, 32, 3, 32, 32)
            self.assertEqual((yield dut.nodes[1].copy.copies), 2)
            self.assertEqual((yield dut.nodes[1].copy.words), 64)
            # One of them ends with an error: the other one is not disturbed.
            first  = yield from copy_request(dut.nodes[0], 1, 32, 2, 60, 32) # Past the end.
            second = yield from copy_request(dut.nodes[3], 1, 40, 3, 0, 16)
            tag, we, status, count = yield from result(dut.nodes[0])
            self.assertEqual((tag, status), (first, FABRIC_RANGE))
            tag, we, status, count = yield from result(dut.nodes[3])
            self.assertEqual((tag, status, count), (second, FABRIC_OK, 16))
            yield from check(self, dut, 1, 40, 3, 0, 16)
            self.assertEqual((yield dut.nodes[1].copy.errors), 1)
            yield from idle(self, dut)
        run(dut, [generator()])

    def test_copy_crossed(self):
        # Every node copies to the next one and receives from the previous one at the same time
        # (the Responders never wait for a copy), with the other traffic going on.
        dut = DUT(two_tier(4))
        def generator():
            for round_ in range(2):
                for n in range(4):
                    yield from fill(dut, n, 32, 16, salt=round_)
                def node_generator(n):
                    status, count = yield from copy(dut.nodes[n], n, 32, (n + 1) % 4, 48, 16)
                    self.assertEqual((status, count), (FABRIC_OK, 16))
                generators = [node_generator(n) for n in range(4)]
                yield from parallel(dut, generators)
                for n in range(4):
                    yield from check(self, dut, n, 32, (n + 1) % 4, 48, 16, salt=round_)
            yield from idle(self, dut)
        run(dut, [generator()])

    def test_copy_with_traffic(self):
        # Copies asked by node 0 between the other nodes while every node (the node of the copy
        # too) sends its own requests: all the slots are shared.
        dut = DUT(two_tier(4))
        def copies():
            for i in range(6):
                src, dst = 1 + (i % 3), 1 + ((i + 1) % 3)
                yield from fill(dut, src, 32, 16, salt=i)
                status, count = yield from copy(dut.nodes[0], src, 32, dst, 48, 16)
                self.assertEqual((status, count), (FABRIC_OK, 16), i)
                yield from check(self, dut, src, 32, dst, 48, 16, salt=i)
        def traffic():
            # The requests of node 0 (results) belong to the copies.
            yield from random_traffic_from(self, dut, [1, 2, 3], requests=40)
        def generator():
            yield from parallel(dut, [copies(), traffic()])
            yield from idle(self, dut)
        run(dut, [generator()])

    def test_copy_lossy(self):
        # Packets lost on every link: sent again by the links.
        dut = DUT(two_tier(4), loss=26)
        def generator():
            yield from fill(dut, 1, 32, 32)
            self.assertEqual((yield from copy(dut.nodes[0], 1, 32, 2, 32, 32)), (FABRIC_OK, 32))
            yield from check(self, dut, 1, 32, 2, 32, 32)
            self.assertGreater(sum((yield from values([link.dropped for link in dut.net.lossy]))), 0)
            yield from idle(self, dut)
        run(dut, [generator()])

    def test_copy_node_down(self):
        # Destination node cut off during the copy: the copy ends with an error, the source node
        # copies again afterwards.
        dut = DUT(two_tier(4), request_timeout=512)
        def generator():
            yield from fill(dut, 1, 32, 32)
            tag = yield from copy_request(dut.nodes[0], 1, 32, 2, 32, 32)
            yield from wait(40)
            for up in dut.net.node_links(2):
                yield up.eq(0)
            rtag, we, status, count = yield from result(dut.nodes[0], timeout=200000)
            self.assertEqual(rtag, tag)
            self.assertIn(status, [FABRIC_NODE, FABRIC_NO_RESPONSE])
            self.assertLess(count, 32)
            self.assertEqual((yield from copy(dut.nodes[0], 1, 32, 3, 32, 32)), (FABRIC_OK, 32))
            yield from check(self, dut, 1, 32, 3, 32, 32)
            # Source node cut off during the copy: no response.
            tag = yield from copy_request(dut.nodes[0], 1, 32, 3, 0, 32)
            yield from wait(40)
            for up in dut.net.node_links(1):
                yield up.eq(0)
            rtag, we, status, count = yield from result(dut.nodes[0], timeout=200000)
            self.assertEqual((rtag, status), (tag, FABRIC_NO_RESPONSE))
        run(dut, [generator()])

def random_traffic_from(test, dut, sources, requests=24, seed=1):
    """Random writes and reads from the nodes ``sources`` to random nodes: each node owns its
    words in the other nodes, and checks what it reads against what it wrote."""
    n = len(dut.nodes)
    def node_generator(src):
        prng  = random.Random(seed*1000 + src)
        node  = dut.nodes[src]
        model = {}
        for _ in range(requests):
            dst  = prng.randrange(n)
            word = 16 + 2*src + prng.randrange(2)
            if (dst, word) in model and prng.random() < 0.5:
                dat_r, status = yield from read(node, dst, word)
                test.assertEqual((dat_r, status), (model[(dst, word)], FABRIC_OK))
            else:
                value = prng.getrandbits(32)
                test.assertEqual((yield from write(node, dst, word, value)), FABRIC_OK)
                model[(dst, word)] = value
    yield from parallel(dut, [node_generator(src) for src in sources])

# Test Fabric Pool ---------------------------------------------------------------------------------

class TestFabricPool(unittest.TestCase):
    def test_addresses(self):
        self.assertEqual(pool_address(3, 5, 6), 3*64 + 5)
        self.assertEqual(pool_split(3*64 + 5, 6), (3, 5))

    def test_read_write(self):
        dut = PoolDUT(two_tier(4))
        def generator():
            pool = dut.pools[0]
            for node in range(4):
                adr = g(node, 32 + node)
                self.assertEqual((yield from pool_op(pool, POOL_WRITE, adr, dat_w=data(node, 1)))[0], FABRIC_OK)
                self.assertEqual((yield dut.mems[node].mem[32 + node]), data(node, 1))
                self.assertEqual((yield from pool_op(pool, POOL_READ, adr))[:2], (FABRIC_OK, data(node, 1)))
            # Node that is not in the fabric.
            self.assertEqual((yield from pool_op(pool, POOL_READ, g(4, 0)))[0], FABRIC_NODE)
            yield from idle(self, dut)
        run(dut, [generator()])

    def test_copy(self):
        # Copies longer than a copy request, and going from a node to the next one.
        dut = PoolDUT(two_tier(4), chunk=8)
        def generator():
            pool = dut.pools[0]
            for n in range(4):
                yield from fill(dut, n, 0, WORDS)
            def expect(adr, dst_adr, words):
                for i in range(words):
                    sn, sw = pool_split(adr + i, WINDOW_BITS)
                    dn, dw = pool_split(dst_adr + i, WINDOW_BITS)
                    self.assertEqual((yield dut.mems[dn].mem[dw]), data(sn, sw), (adr, dst_adr, i))
            # 20 words inside node 1 to node 2: 3 copy requests.
            self.assertEqual((yield from pool_op(pool, POOL_COPY, g(1, 32), dst_adr=g(2, 40), words=20)),
                (FABRIC_OK, 0, 20))
            yield from expect(g(1, 32), g(2, 40), 20)
            self.assertEqual((yield pool.chunks), 3)
            # 24 words from the end of node 0 and the start of node 1, to the end of node 2 and
            # the start of node 3: cut at the end of each window.
            for n in range(4):
                yield from fill(dut, n, 0, WORDS)
            self.assertEqual((yield from pool_op(pool, POOL_COPY, g(0, 52), dst_adr=g(2, 60), words=24)),
                (FABRIC_OK, 0, 24))
            yield from expect(g(0, 52), g(2, 60), 24)
            # Nothing to copy.
            self.assertEqual((yield from pool_op(pool, POOL_COPY, g(1, 0), dst_adr=g(2, 0), words=0)),
                (FABRIC_OK, 0, 0))
            # Past the last node: the words before are copied.
            for n in range(4):
                yield from fill(dut, n, 0, WORDS)
            status, _, words = yield from pool_op(pool, POOL_COPY, g(1, 0), dst_adr=g(3, 56), words=16)
            self.assertEqual((status, words), (FABRIC_NODE, 8))
            yield from expect(g(1, 0), g(3, 56), 8)
            yield from idle(self, dut)
        run(dut, [generator()])

    def test_copy_busy(self):
        # Two nodes asking copies to the same source node, which runs one copy at a time: each
        # one is asked again until served.
        dut = PoolDUT(two_tier(4), pools=(0, 3), chunk=16, copy_lanes=1)
        def generator():
            for n in range(4):
                yield from fill(dut, n, 0, WORDS)
            def copier(n, dst_adr):
                r = yield from pool_op(dut.pools[n], POOL_COPY, g(1, 0), dst_adr=dst_adr, words=32)
                self.assertEqual(r, (FABRIC_OK, 0, 32))
            yield from parallel(dut, [copier(0, g(2, 32)), copier(3, g(3, 32))])
            for i in range(32):
                self.assertEqual((yield dut.mems[2].mem[32 + i]), data(1, i))
                self.assertEqual((yield dut.mems[3].mem[32 + i]), data(1, i))
            self.assertGreater((yield dut.pools[0].retries) + (yield dut.pools[3].retries), 0)
            yield from idle(self, dut)
        run(dut, [generator()])

if __name__ == "__main__":
    unittest.main()
