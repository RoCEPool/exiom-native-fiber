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

    def test_one_node_down(self):
        # Paths left when any one node is down, and their length (links).
        from exiom.network import expected_status
        from exiom.fabric  import FABRIC_OK
        for name, t, longest, all_served in [
            ("4 nodes",              two_tier(4),                   1, True),
            ("8 nodes, 16 design",   two_tier(8),                   3, False), # One cable between the groups.
            ("8 nodes, 8 design",    two_tier(8, design_groups=2),  2, True),
            ("16 nodes",             two_tier(16),                  5, True),
            ("32 nodes",             two_tier(32, design_groups=8, inter_ports=2), 4, True),
        ]:
            with self.subTest(fabric=name):
                served, worst = True, 0
                for down in range(t.nodes):
                    distances = t.distances(down=[down])
                    for src in range(t.nodes):
                        for dst in range(t.nodes):
                            if down in (src, dst) or src == dst:
                                continue
                            d = distances[src].get(dst)
                            served &= d is not None
                            worst   = max(worst, d or 0)
                            if t.nodes <= 8:
                                # Same answer from the model used by the network tests.
                                status = expected_status(t, 6, src, dst, down=[down])
                                self.assertEqual(status == FABRIC_OK, d is not None)
                self.assertEqual((served, worst), (all_served, longest))

    def test_chassis_of_8(self):
        # Groups of 8 nodes (one chassis each, 7 ports inside the chassis):
        # nodes: (design groups, inter ports, cables, max hops, busiest link (pairs), max hops with
        # one node down).
        for nodes, (groups, inter, cables, hops, busiest, hops_down) in {
            16  : (2,  1,  64, 2,  8, 2),
            32  : (4,  1, 126, 3, 38, 3),
            64  : (8,  1, 252, 3, None, None),
            128 : (16, 2, 568, 3, 72, 4),
        }.items():
            with self.subTest(nodes=nodes):
                t = two_tier(nodes, group_size=8, design_groups=groups, inter_ports=inter)
                self.assertEqual(t.ports, 7 + inter)
                self.assertEqual(len(t.links), cables)
                distances = t.distances()
                self.assertTrue(all(len(distances[n]) == nodes for n in range(nodes))) # All reachable.
                self.assertEqual(max(max(d.values()) for d in distances.values()), hops)
                self.assertLessEqual(hops + 2, 6) # Hop limit of the packets, with detours.
                # Inside a chassis: always one link.
                self.assertTrue(all(distances[n][m] == 1 for n in range(8) for m in range(8) if n != m))
                if busiest is not None:
                    loads = t.loads()
                    self.assertEqual(sum(loads.values()), sum(sum(d.values()) for d in distances.values()))
                    self.assertEqual(max(loads.values()), busiest)
                    # The busiest links are between the chassis.
                    self.assertGreaterEqual(max(loads, key=loads.get)[1], 7)
                if hops_down is not None:
                    worst = 0
                    for down in range(0, nodes, max(1, nodes//32)):
                        d = t.distances(down=[down])
                        for src in range(nodes):
                            if src != down:
                                self.assertEqual(len(d[src]), nodes - 1, (down, src)) # Nobody else lost.
                                worst = max(worst, max(d[src].values()))
                    self.assertEqual(worst, hops_down)
                    self.assertLessEqual(worst, 6)

    def test_chassis_of_8_growth(self):
        # Cabled for 16 chassis: 2 -> 4 -> 8 -> 16 chassis only adds cables, and every size works.
        previous = set()
        for nodes in [16, 32, 64, 128]:
            t = two_tier(nodes, group_size=8, design_groups=16, inter_ports=2)
            links = set(t.links)
            self.assertTrue(previous <= links)
            previous = links
            distances = t.distances()
            self.assertTrue(all(len(distances[n]) == nodes for n in range(nodes)), nodes)
            self.assertLessEqual(max(max(d.values()) for d in distances.values()), 4)

    def test_too_few_inter_ports(self):
        # 16 chassis of 8 with one inter-chassis port per node: 8 cables for 15 other chassis.
        with self.assertRaises(ValueError):
            two_tier(128, group_size=8, design_groups=16, inter_ports=1)
        two_tier(128, group_size=8, design_groups=16, inter_ports=2)

    def test_costs(self):
        # Cost of a port: links to the destination through it, without coming back.
        t = two_tier(4)
        self.assertEqual(t.costs()[0][1], [1, 2, 2, None]) # Direct, or through nodes 2 or 3.
        t = two_tier(8)
        costs = t.costs()
        self.assertEqual(costs[0][6], [None, None, None, 1]) # Node 0 has the only cable to node 6.
        self.assertEqual(costs[1][6], [3, 3, 2, None])       # Node 1: through node 0, directly or not.

    def test_hypercube(self):
        for nodes, hops in [(2, 1), (4, 2), (8, 3), (16, 4)]:
            t = hypercube(nodes)
            self.assertEqual(t.max_hops(), hops)
            self.assertEqual(t.ports_used(0), hops)

if __name__ == "__main__":
    unittest.main()
