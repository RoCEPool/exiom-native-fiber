#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric networks (exiom/network.py): several nodes, forwarding over several links, credits.

Word-level links. 4 nodes (one group, all connected) and 8 nodes (two groups, up to 3 links
between two nodes); larger fabrics are simulated with Verilator (test/test_fabric_sim.py), the
Migen simulator being slow to start on them.
"""

import random
import unittest

from migen import *

from litex.gen import *
from litex.gen.sim import *

from exiom.fabric   import *
from exiom.topology import *
from exiom.network  import *

from test.test_fabric import Memory, request, result, results, write, read, wait

# DUT ----------------------------------------------------------------------------------------------

class DUT(LiteXModule):
    def __init__(self, topology, mem_kwargs={}, **kwargs):
        self.topology = topology
        self.net  = FabricNetwork(topology, size=256, fc_period=64, **kwargs)
        self.mems = []
        for n, node in enumerate(self.net.nodes):
            mem = Memory(node.responder.mem, **mem_kwargs)
            self.add_module(f"mem{n}", mem)
            self.mems.append(mem)
        self.nodes = self.net.nodes

    def check_no_drop(self, test):
        """No packet dropped for lack of room, no credit lost, no response without request."""
        for node in self.nodes:
            test.assertEqual((yield node.requester.stale), 0)
            test.assertEqual((yield node.rsp_drops), 0)
            for port in node.ports:
                test.assertEqual((yield port.rx_drops), 0)
                test.assertEqual((yield port.resyncs), 0)

def value(src, dst, salt=0):
    return (0xa5000000 | (salt << 20) | (src << 8) | dst) & 0xffffffff

def run(dut, generators):
    """Runs the generators one after the other in one simulation (the simulator is slow to start)."""
    def sequence():
        for generator in generators:
            yield from generator
    run_simulation(dut, sequence())

def parallel(dut, generators):
    """Generator running `generators` together (one clock cycle for all of them at a time)."""
    running = [g for g in generators if g is not None]
    while running:
        still = []
        for g in running:
            # Signal reads and writes of this generator, until it waits for the next cycle.
            try:
                command = next(g)
                while command is not None:
                    command = g.send((yield command))
                still.append(g)
            except StopIteration:
                pass
        running = still
        if running:
            yield

# Scenarios ----------------------------------------------------------------------------------------

def all_to_all(test, dut, salt=0, check_hops=True):
    """Every node writes one word in every other node (several requests in flight), then reads it."""
    n = len(dut.nodes)
    def node_generator(src):
        node = dut.nodes[src]
        for we in [1, 0]:
            tags = {}
            dsts = [d for d in range(n) if d != src]
            while dsts or tags:
                while dsts and len(tags) < 8:
                    dst = dsts.pop(0)
                    tag = yield from request(node, src, we=we, dat_w=value(src, dst, salt), dst=dst)
                    tags[tag] = dst
                tag, rwe, status, dat_r = yield from result(node)
                dst = tags.pop(tag)
                test.assertEqual((rwe, status), (we, FABRIC_OK), f"{src} -> {dst}")
                if not we:
                    test.assertEqual(dat_r, value(src, dst, salt), f"{src} -> {dst}")
    yield from parallel(dut, [node_generator(src) for src in range(n)])
    for dst in range(n):
        for src in range(n):
            if src != dst:
                test.assertEqual((yield dut.mems[dst].mem[src]), value(src, dst, salt))
    yield from dut.check_no_drop(test)
    if check_hops and dut.net.max_hops > 1:
        forwarded = 0
        for node in dut.nodes:
            forwarded += (yield node.forwarded)
        test.assertGreater(forwarded, 0)

def random_traffic(test, dut, requests=24, seed=1):
    """Random writes and reads from every node to random nodes (itself included): each node owns
    its words in the other nodes, and checks what it reads against what it wrote."""
    n = len(dut.nodes)
    def node_generator(src):
        prng  = random.Random(seed*1000 + src)
        node  = dut.nodes[src]
        model = {} # (dst, word) -> value written and confirmed.
        tags  = {}
        todo  = requests
        while todo or tags:
            while todo and len(tags) < 8:
                dst  = prng.randrange(n)
                word = 16 + 2*src + prng.randrange(2)
                busy = any((d, w) == (dst, word) for d, w, _, _ in tags.values())
                if busy:
                    break # One request at a time on a word (results order is not the request order).
                if (dst, word) in model and prng.random() < 0.5:
                    tag = yield from request(node, word, dst=dst)
                    tags[tag] = (dst, word, 0, model[(dst, word)])
                else:
                    data = prng.getrandbits(32)
                    tag  = yield from request(node, word, we=1, dat_w=data, dst=dst)
                    tags[tag] = (dst, word, 1, data)
                todo -= 1
            tag, we, status, dat_r = yield from result(node)
            dst, word, rwe, data = tags.pop(tag)
            test.assertEqual((we, status), (rwe, FABRIC_OK), f"{src} -> {dst}")
            if we:
                model[(dst, word)] = data
            else:
                test.assertEqual(dat_r, data, f"{src} -> {dst} word {word}")
    yield from parallel(dut, [node_generator(src) for src in range(n)])
    yield from dut.check_no_drop(test)

def hotspot(test, dut, dst=0, writes=6):
    """All the nodes write the same node together: every write is acknowledged, executed once, and
    the writes of one node are executed in order."""
    n = len(dut.nodes)
    def node_generator(src):
        if src == dst:
            return
        node = dut.nodes[src]
        tags = []
        for i in range(writes):
            # Own word (last write must win) and shared word (one of the writes wins).
            tags.append((yield from request(node, 40 + src, we=1, dat_w=value(src, dst, i), dst=dst)))
            tags.append((yield from request(node, 63, we=1, dat_w=value(src, dst, 0xf), dst=dst)))
            if len(tags) == 8 or i == (writes - 1):
                r = yield from results(node, len(tags))
                test.assertTrue(all(status == FABRIC_OK for _, status, _ in r.values()))
                tags = []
    before = (yield dut.nodes[dst].responder.requests)
    yield from parallel(dut, [node_generator(src) for src in range(n)])
    for src in range(n):
        if src != dst:
            test.assertEqual((yield dut.mems[dst].mem[40 + src]), value(src, dst, writes - 1))
    test.assertIn((yield dut.mems[dst].mem[63]), [value(src, dst, 0xf) for src in range(n) if src != dst])
    test.assertEqual((yield dut.nodes[dst].responder.requests) - before, 2*writes*(n - 1))
    yield from dut.check_no_drop(test)

def node_down(test, dut, down):
    """Node cut from the fabric: requests to it or through it end with an error, the others are
    served; then the node comes back."""
    n = len(dut.nodes)
    topology, routes = dut.topology, dut.net.routes
    def node_generator(src, expect_ok):
        def generator():
            node = dut.nodes[src]
            for dst in range(n):
                if dst == src:
                    continue
                expected = FABRIC_OK if expect_ok else expected_status(topology, routes, src, dst, down)
                status   = yield from write(node, dst, 8, value(src, dst, 7))
                test.assertEqual(status, expected, f"{src} -> {dst}")
        return generator()
    for up in node_links_of(dut, down):
        yield up.eq(0)
    yield from wait(8)
    yield from parallel(dut, [node_generator(src, False) for src in range(n)])
    statuses = [expected_status(topology, routes, s, d, down) for s in range(n) for d in range(n) if s != d]
    test.assertIn(FABRIC_OK,   statuses)
    test.assertIn(FABRIC_LINK, statuses)
    for up in node_links_of(dut, down):
        yield up.eq(1)
    yield from wait(8)
    yield from parallel(dut, [node_generator(src, True) for src in range(n)])

def node_links_of(dut, node):
    return dut.net.node_links(node)

# Test Fabric Network ------------------------------------------------------------------------------

class TestFabricNetwork(unittest.TestCase):
    def test_4_nodes(self):
        dut = DUT(two_tier(4))
        self.assertEqual(dut.net.max_hops, 1)
        run(dut, [
            all_to_all(self, dut),
            random_traffic(self, dut),
            hotspot(self, dut),
            node_down(self, dut, down=2),
            all_to_all(self, dut, salt=1),
        ])

    def test_4_nodes_backpressure(self):
        # Smallest buffers, slow memories: no packet dropped, nothing stalls.
        dut = DUT(two_tier(4), buffer_depth=2, queue_depth=1, request_timeout=60000, mem_kwargs=dict(latency=40))
        run(dut, [
            all_to_all(self, dut),
            random_traffic(self, dut, requests=32),
            hotspot(self, dut),
        ])

    def test_4_nodes_hypercube(self):
        # Other topology (2 links between opposite nodes): same nodes, other routes.
        dut = DUT(hypercube(4))
        self.assertEqual(dut.net.max_hops, 2)
        run(dut, [
            all_to_all(self, dut),
            random_traffic(self, dut),
            node_down(self, dut, down=1),
        ])

    def test_8_nodes(self):
        # Two groups of 4, one cable between them (fabric cabled for 16 nodes): up to 3 links.
        dut = DUT(two_tier(8), buffer_depth=2, queue_depth=1, request_timeout=60000, mem_kwargs=dict(latency=20))
        self.assertEqual(dut.net.max_hops, 3)
        run(dut, [
            all_to_all(self, dut),
            random_traffic(self, dut, requests=16),
            hotspot(self, dut, writes=3),
            node_down(self, dut, down=6), # Node 6 has the cable to the other group.
            all_to_all(self, dut, salt=1),
        ])

if __name__ == "__main__":
    unittest.main()
