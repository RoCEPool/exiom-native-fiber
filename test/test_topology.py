#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

import unittest

from exiom.topology import *

class TestTopology(unittest.TestCase):
    def test_two_tier_sizes(self):
        # nodes: (design groups, inter ports, ports used per node at most, max hops).
        for nodes, (groups, inter, ports, hops) in {
            2  : (4, 1, 1, 1),
            4  : (4, 1, 3, 1),
            8  : (4, 1, 4, 3),
            16 : (4, 1, 4, 3),
            32 : (8, 2, 5, 3),
        }.items():
            with self.subTest(nodes=nodes):
                t = two_tier(nodes, design_groups=groups, inter_ports=inter)
                self.assertEqual(max(t.ports_used(n) for n in range(nodes)), ports)
                self.assertTrue(all(h is not None for h in t.hops().values()))
                self.assertEqual(t.max_hops(), hops)

    def test_two_tier_growth_only_adds_cables(self):
        # 2 -> 4 -> 8 -> 16 nodes: the cables of the smaller fabric are all kept.
        previous = set()
        for nodes in [2, 4, 8, 16]:
            links = set(two_tier(nodes).links)
            self.assertTrue(previous <= links)
            self.assertGreater(len(links), len(previous))
            previous = links
        self.assertEqual(len(previous), 4*6 + 6) # 4 groups fully connected + 1 link per group pair.

    def test_two_tier_groups_fully_connected(self):
        for groups, inter in [(2, 1), (3, 1), (4, 1), (5, 1), (8, 2), (9, 2)]:
            with self.subTest(groups=groups):
                t = two_tier(4*groups, design_groups=groups, inter_ports=inter)
                pairs = {}
                for (a, _), (b, _) in t.links:
                    if a//4 != b//4:
                        pair = (min(a//4, b//4), max(a//4, b//4))
                        pairs[pair] = pairs.get(pair, 0) + 1
                self.assertEqual(len(pairs), groups*(groups - 1)//2)
                # 2 groups: the 4 inter-group links are parallel (member to member), 2 links at most.
                self.assertEqual(t.max_hops(), 2 if groups == 2 else 3)

    def test_routes(self):
        t = two_tier(16)
        routes = t.routes()
        for (src, dst), hops in t.hops(routes).items():
            self.assertLessEqual(hops, 1 if src//4 == dst//4 else 3)
        # Node 5 down: not reachable, the others still are (within the group and through the other
        # inter-group links).
        routes = t.routes(down=[5])
        hops   = t.hops(routes)
        for (src, dst), h in hops.items():
            if 5 in (src, dst):
                self.assertIsNone(h)
            else:
                self.assertIsNotNone(h)

    def test_hypercube(self):
        for nodes, hops in [(2, 1), (4, 2), (8, 3), (16, 4)]:
            t = hypercube(nodes)
            self.assertEqual(t.max_hops(), hops)
            self.assertEqual(t.ports_used(0), hops)

if __name__ == "__main__":
    unittest.main()
