#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric networks (exiom/network.py): several nodes, forwarding over several links, credits.

Word-level links. 4 nodes (one group, all connected), 8 nodes (two groups) and 16 nodes (four
groups, up to 3 links between two nodes, 5 around a node that is down). The longest ones are
nightly tests (test/nightly.py).
"""

import random
import unittest

from migen import *

from litex.gen import *
from litex.gen.sim import *

from exiom.fabric   import *
from exiom.topology import *
from exiom.network  import *

from test.test_fabric import Memory, node_memory, defaults, request, result, results, write, read, wait
from test.nightly     import nightly

# After the other imports (run_simulation is also exported by litex.gen).
from test.fabric_sim import run_simulation # Same simulator, starts faster on large designs.

# DUT ----------------------------------------------------------------------------------------------

class DUT(LiteXModule):
    def __init__(self, topology, mem_kwargs={}, size=256, pipelined=None, **kwargs):
        self.topology = topology
        pipelined = defaults.pipelined if pipelined is None else pipelined
        self.net  = FabricNetwork(topology, size=size, fc_period=64, pipelined=pipelined, **kwargs)
        self.mems = []
        for n, node in enumerate(self.net.nodes):
            mem = node_memory(node, pipelined, **mem_kwargs)
            self.add_module(f"mem{n}", mem)
            self.mems.append(mem)
        self.nodes = self.net.nodes

    def check_no_drop(self, test):
        """No packet refused for lack of room, no response without request."""
        for node in self.nodes:
            test.assertEqual((yield node.requester.stale), 0)
            test.assertEqual((yield node.rsp_drops), 0)
            for port in node.ports:
                test.assertEqual((yield port.rx_drops), 0)

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

def flood(test, dut, sources, destination, requests, floor):
    """Writes sent as fast as the nodes accept them: `floor` requests per 1000 cycles at least."""
    cycles = [0]
    def node_generator(src):
        node   = dut.nodes[src]
        prng   = random.Random(src)
        sink   = node.requester.sink
        source = node.requester.source
        sent, received = 0, 0
        yield source.ready.eq(1)
        yield sink.we.eq(1)
        yield sink.sel.eq(0b1111)
        while received < requests:
            yield sink.valid.eq(int(sent < requests))
            if sent < requests:
                yield sink.dst.eq(destination(src, prng))
                yield sink.adr.eq(sent % 32)
                yield sink.dat_w.eq(sent)
            yield
            if (sent < requests) and (yield sink.ready):
                sent += 1
            if (yield source.valid):
                test.assertEqual((yield source.status), FABRIC_OK)
                received += 1
        yield sink.valid.eq(0)
        yield source.ready.eq(0)
    running = [node_generator(src) for src in sources]
    # One generator per source and the cycle counter, together.
    def together():
        generators = [g for g in running]
        while generators:
            still = []
            for g in generators:
                try:
                    command = next(g)
                    while command is not None:
                        command = g.send((yield command))
                    still.append(g)
                except StopIteration:
                    pass
            generators = still
            if generators:
                cycles[0] += 1
                yield
    yield from together()
    rate = 1000*requests*len(sources)/cycles[0]
    test.assertGreater(rate, floor, f"{rate:.0f} requests per 1000 cycles")
    yield from dut.check_no_drop(test)

SETTLE = 800 # Cycles for the routes to follow a change (route packets every 64 cycles here).

def degraded(test, dut, down, check=None):
    """Nodes or links (`down`, see Topology.distances) cut from the fabric: once the routes have
    followed, requests are served by the paths that are left, and those that cannot be are
    answered with an error; then everything comes back."""
    n        = len(dut.nodes)
    topology = dut.topology
    def links(d):
        return [dut.net.link_up[d]] if isinstance(d, tuple) else dut.net.node_links(d)
    def node_generator(src, down, word):
        def generator():
            node = dut.nodes[src]
            tags = {}
            dsts = [d for d in range(n) if d != src]
            while dsts or tags:
                while dsts and len(tags) < 8:
                    dst = dsts.pop(0)
                    tag = yield from request(node, word, we=1, dat_w=value(src, dst, 7), dst=dst)
                    tags[tag] = dst
                tag, _, status, _ = yield from result(node)
                dst = tags.pop(tag)
                test.assertEqual(status, expected_status(topology, dut.net.max_hops, src, dst, down), f"{src} -> {dst}")
        return generator()
    statuses = [expected_status(topology, dut.net.max_hops, s, d, down) for s in range(n) for d in range(n) if s != d]
    if check is not None:
        check(statuses)
    forwarded = 0
    for node in dut.nodes:
        forwarded -= (yield node.forwarded)
    for d in down:
        for up in links(d):
            yield up.eq(0)
    yield from wait(SETTLE)
    yield from parallel(dut, [node_generator(src, down, 8) for src in range(n)])
    for node in dut.nodes:
        forwarded += (yield node.forwarded)
    for d in down:
        for up in links(d):
            yield up.eq(1)
    yield from wait(SETTLE)
    yield from parallel(dut, [node_generator(src, (), 9) for src in range(n)])
    return forwarded

def node_down(test, dut, down, unreachable=False):
    def check(statuses):
        test.assertIn(FABRIC_LINK, statuses)           # From the node.
        test.assertIn(FABRIC_NODE, statuses)           # To the node.
        # Other pairs: all served, unless the fabric has no other path.
        others = len(dut.nodes) - 1
        test.assertEqual(statuses.count(FABRIC_OK) == others*(others - 1), not unreachable)
    yield from degraded(test, dut, [down], check)

def link_down(test, dut, link):
    def check(statuses):
        test.assertEqual(set(statuses), {FABRIC_OK})   # Everything served around the link.
    forwarded = yield from degraded(test, dut, [link], check)
    test.assertGreater(forwarded, 0)

# Test Fabric Network ------------------------------------------------------------------------------

class TestFabricNetwork(unittest.TestCase):
    def test_4_nodes(self):
        dut = DUT(two_tier(4))
        self.assertEqual((dut.net.diameter, dut.net.max_hops), (1, 3))
        run(dut, [
            all_to_all(self, dut, check_hops=False),
            random_traffic(self, dut),
            hotspot(self, dut),
            node_down(self, dut, down=2),
            link_down(self, dut, link=(0, 0)), # Nodes 0 and 1 then talk through another node.
            all_to_all(self, dut, salt=1, check_hops=False),
        ])

    def test_4_nodes_wide(self):
        # 128 bits per cycle on the links (a whole packet).
        dut = DUT(two_tier(4), link_width=128)
        run(dut, [
            all_to_all(self, dut, check_hops=False),
            random_traffic(self, dut),
            hotspot(self, dut),
            node_down(self, dut, down=2),
        ])

    @nightly
    def test_wide_links(self):
        # 128 and 64 bits per cycle at scale: 8 nodes with losses, 16 nodes.
        dut = DUT(two_tier(8), link_width=128, loss=26, buffer_depth=2, queue_depth=1, request_timeout=60000)
        self.lossy(dut, [
            all_to_all(self, dut),
            random_traffic(self, dut, requests=16),
            hotspot(self, dut, writes=3),
        ])
        dut = DUT(two_tier(16), link_width=64)
        run(dut, [
            all_to_all(self, dut),
            random_traffic(self, dut, requests=8),
        ])

    # Requests per 1000 cycles (floors about 25 % under the rates measured), with links of 32 bits
    # per cycle and of 128 bits per cycle (a whole packet): [2 nodes one way, 2 nodes both ways,
    # 4 nodes 3 to one, 4 nodes all to all].
    throughput_floors = {32: [110, 130, 320, 665], 128: [285, 460, 365, 1125]}

    def test_throughput(self):
        # Two nodes.
        for link_width, floors in self.throughput_floors.items():
            with self.subTest(link_width=link_width):
                dut = DUT(two_tier(2), link_width=link_width)
                run(dut, [
                    flood(self, dut, [0],    lambda src, prng: 1,       200, floor=floors[0]),
                    flood(self, dut, [0, 1], lambda src, prng: 1 - src, 200, floor=floors[1]),
                ])

    @nightly
    def test_throughput_4_nodes(self):
        any_other = lambda src, prng: (src + 1 + prng.randrange(3)) % 4
        for link_width, floors in self.throughput_floors.items():
            with self.subTest(link_width=link_width):
                dut = DUT(two_tier(4), link_width=link_width)
                run(dut, [
                    flood(self, dut, [1, 2, 3],    lambda src, prng: 0, 100, floor=floors[2]), # One destination.
                    flood(self, dut, [0, 1, 2, 3], any_other,           100, floor=floors[3]),
                ])

    def test_4_nodes_backpressure(self):
        # Smallest buffers, slow memories: no packet dropped, nothing stalls.
        dut = DUT(two_tier(4), buffer_depth=2, queue_depth=1, request_timeout=60000, mem_kwargs=dict(latency=40))
        run(dut, [
            all_to_all(self, dut, check_hops=False),
            random_traffic(self, dut, requests=32),
            hotspot(self, dut),
        ])

    def lossy(self, dut, generators):
        """Runs on links that lose packets: nothing may be lost end to end."""
        def check():
            dropped, retransmits = 0, 0
            for link in dut.net.lossy:
                dropped += (yield link.dropped)
            for node in dut.nodes:
                for port in node.ports:
                    retransmits += (yield port.retransmits)
            self.assertGreater(dropped, 20)
            self.assertGreater(retransmits, 0)
        run(dut, generators + [check()])

    def test_4_nodes_lossy(self):
        # One packet out of 10 lost on every link, in both directions.
        dut = DUT(two_tier(4), loss=26, request_timeout=60000)
        self.lossy(dut, [
            all_to_all(self, dut, check_hops=False),
            random_traffic(self, dut, requests=32),
            hotspot(self, dut),
        ])

    @nightly
    def test_8_nodes_lossy(self):
        # Packets forwarded over up to 3 lossy links, smallest buffers.
        dut = DUT(two_tier(8), loss=26, buffer_depth=2, queue_depth=1, request_timeout=60000)
        self.lossy(dut, [
            all_to_all(self, dut),
            random_traffic(self, dut, requests=16),
            hotspot(self, dut, writes=3),
        ])

    def test_4_nodes_hypercube(self):
        # Other topology (2 links between opposite nodes): same nodes, other routes.
        dut = DUT(hypercube(4))
        self.assertEqual((dut.net.diameter, dut.net.max_hops), (2, 4))
        run(dut, [
            all_to_all(self, dut),
            random_traffic(self, dut),
            node_down(self, dut, down=1),
            link_down(self, dut, link=(0, 0)),
        ])

    def test_8_nodes_forwarding(self):
        # Two groups of 4: requests and responses forwarded over up to 3 links.
        dut = DUT(two_tier(8))
        self.assertEqual((dut.net.diameter, dut.net.max_hops), (3, 5))
        run(dut, [all_to_all(self, dut)])

    @nightly
    def test_8_nodes(self):
        # Two groups of 4 of a fabric cabled for 16 nodes: one cable between the groups, up to 3
        # links. Without the node that has this cable, the other group cannot be reached.
        dut = DUT(two_tier(8), buffer_depth=2, queue_depth=1, request_timeout=60000, mem_kwargs=dict(latency=20))
        self.assertEqual((dut.net.diameter, dut.net.max_hops), (3, 5))
        run(dut, [
            all_to_all(self, dut),
            random_traffic(self, dut, requests=16),
            hotspot(self, dut, writes=3),
            node_down(self, dut, down=6, unreachable=True),
            node_down(self, dut, down=5),
            all_to_all(self, dut, salt=1),
        ])

    @nightly
    def test_8_nodes_two_groups(self):
        # Fabric cabled for 8 nodes: 4 cables between the two groups. Any node or cable can go,
        # the others are still served. Route packets in several parts (4 destinations each).
        dut = DUT(two_tier(8, design_groups=2), route_entries=4)
        self.assertEqual((dut.net.diameter, dut.net.max_hops), (2, 4))
        run(dut, [
            all_to_all(self, dut),
            node_down(self, dut, down=6),
            link_down(self, dut, link=(0, 3)), # Cable between the groups.
            link_down(self, dut, link=(4, 0)), # Cable inside a group.
            all_to_all(self, dut, salt=1),
        ])

    @nightly
    def test_16_nodes(self):
        # Four groups of 4, one cable between each pair of groups, 4 ports per node.
        dut = DUT(two_tier(16))
        self.assertEqual((dut.net.diameter, dut.net.max_hops), (3, 5))
        run(dut, [
            all_to_all(self, dut),
            node_down(self, dut, down=6),      # Has the cable between groups 0 and 1: through another group.
            link_down(self, dut, link=(0, 3)), # This cable alone.
            random_traffic(self, dut, requests=8),
        ])

if __name__ == "__main__":
    unittest.main()
