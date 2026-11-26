#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric end-to-end bench (bench/fabric/sim.py: two symmetric nodes, bit-level generic PHYs with
link initialization, CRC and auto-retrain, requests in flight) with the Migen simulator: same
bench as the Verilator simulation of test/test_fabric_sim.py, with access to the internal state.
"""

import os
import unittest
import importlib.util

from migen import *

from litex.gen.sim import *

from exiom.fabric import *

# After the other imports (run_simulation is also exported by litex.gen).
from test.fabric_sim import run_simulation # Same simulator, starts faster on large designs.

SIM = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bench", "fabric", "sim.py")

def load_sim():
    spec   = importlib.util.spec_from_file_location("fabric_sim", SIM)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

class TestFabricBench(unittest.TestCase):
    def run_bench(self, steps_a=None, **kwargs):
        sim = load_sim()
        if steps_a is not None:
            sim.scenario_steps_a = lambda scenarios: steps_a
        dut     = sim.FabricTestBench(timeout=400000, **kwargs)
        results = {}
        def generator():
            link_losses = 0
            link_ready  = 0
            max_pending = 0
            while not ((yield dut.done) or (yield dut.timeout)):
                ready = (yield dut.link_ready)
                link_losses += int(link_ready and not ready)
                link_ready   = ready
                max_pending  = max(max_pending, (yield dut.nodes["a"].requester.pending))
                yield
            results["done"]        = (yield dut.done)
            results["errors"]      = (yield dut.errors)
            results["failed"]      = ((yield dut.failed_node), (yield dut.failed_step))
            results["link_losses"] = link_losses
            results["max_pending"] = max_pending
            results["results"]     = {"a": (yield dut.a_seq.results), "b": (yield dut.b_seq.results)}
            results["stale"]       = (yield dut.nodes["a"].requester.stale) + (yield dut.nodes["b"].requester.stale)
            results["rx_drops"]    = (yield dut.nodes["a"].ports[0].rx_drops) + (yield dut.nodes["b"].ports[0].rx_drops)
            for name in ["a", "b"]:
                results[name] = []
                for i in range(64):
                    results[name].append((yield dut.srams[name].mem[i]))
        run_simulation(dut, generator())
        return sim, dut, results

    def check_pass(self, **kwargs):
        sim, dut, results = self.run_bench(**kwargs)
        self.assertEqual(results["done"], 1)
        node, step = results["failed"]
        self.assertEqual(results["errors"], 0, "first failed step: node {} {}".format(node, dut.steps[node][step]))
        # Every request got exactly one result.
        for name, node_id in [("a", sim.NODE_A), ("b", sim.NODE_B)]:
            requests = sum(1 for s in dut.steps[node_id] if s[0] in [sim.WRITE, sim.READ])
            self.assertEqual(results["results"][name], requests)
        return sim, dut, results

    def test_all(self):
        sim, dut, results = self.check_pass()
        # Requests really in flight together, link really lost and retrained.
        self.assertGreater(results["max_pending"], 1)
        self.assertEqual(results["link_losses"], 1)
        self.assertEqual(results["rx_drops"], 0)
        # Node B memory: written by node A (refused/failed/lost writes never reached it).
        mem = results["b"]
        for i in list(range(28)) + [29]:
            self.assertEqual(mem[i], sim.pattern(i, 1))
        self.assertEqual(mem[sim.HANG_ADR], 0)
        self.assertEqual(mem[32:64], [0]*32)
        # Node A memory: written by node B.
        mem = results["a"]
        for i in range(12):
            self.assertEqual(mem[i], sim.pattern(i, 2))
        self.assertEqual(mem[12:64], [0]*52)

    def test_slow_memory(self):
        sim, dut, results = self.check_pass(sram_latency=64, scenarios=["normal", "errors"])
        self.assertGreater(results["max_pending"], 4)

    def test_flow_control(self):
        self.check_pass(with_flow_control=True, sram_latency=16)

    def test_scenarios_alone(self):
        sim = load_sim()
        for scenario in sim.SCENARIOS:
            with self.subTest(scenario=scenario):
                self.check_pass(scenarios=[scenario])

    def test_checker(self):
        # The sequencer must report wrong data and wrong statuses, on the right request.
        sim = load_sim()
        def steps(*requests):
            s = sim.Steps(dst=sim.NODE_B)
            s.write(1, 0x1111)
            for kind, word, data, status in requests:
                s.append((kind, sim.NODE_B, word, data, status))
            s.step(sim.SYNC)
            return s
        cases = {
            "data"   : steps((sim.READ,  1,  0x2222, FABRIC_OK)),
            "status" : steps((sim.WRITE, 40, 0x1111, FABRIC_OK)),
            "error"  : steps((sim.READ,  1,  0x1111, FABRIC_RANGE)),
        }
        for name, steps_a in cases.items():
            with self.subTest(case=name):
                _, _, results = self.run_bench(steps_a=steps_a, scenarios=[])
                self.assertEqual(results["done"], 1)
                self.assertEqual(results["errors"], 1)
                self.assertEqual(results["failed"], (sim.NODE_A, 1))

class TestFabricNetworkBench(unittest.TestCase):
    """Network bench (word-level links, one sequencer per node). 4 nodes here; 8 and 16 nodes with
    Verilator (test/test_fabric_sim.py)."""
    def run_bench(self, **kwargs):
        sim     = load_sim()
        dut     = sim.FabricNetworkBench(timeout=400000, **kwargs)
        results = {}
        def generator():
            was_down = 0
            while not ((yield dut.done) or (yield dut.timeout)):
                was_down |= (yield dut.node_down)
                yield
            results["done"]      = (yield dut.done)
            results["errors"]    = (yield dut.errors)
            results["failed"]    = ((yield dut.failed_node), (yield dut.failed_step))
            results["was_down"]  = was_down
            results["results"]   = []
            results["forwarded"] = 0
            results["drops"]     = 0
            for n, node in enumerate(dut.net.nodes):
                results["results"].append((yield dut.seqs[n].results))
                results["forwarded"] += (yield node.forwarded)
                results["drops"]     += (yield node.requester.stale)
                for port in node.ports:
                    results["drops"] += (yield port.rx_drops)
        run_simulation(dut, generator())
        self.assertEqual(results["done"], 1)
        node, step = results["failed"]
        self.assertEqual(results["errors"], 0, "first failed step: node {} {}".format(node, dut.steps[node][step]))
        # Every request got exactly one result, nothing dropped.
        for n in range(len(dut.net.nodes)):
            requests = sum(1 for s in dut.steps[n] if s[0] in [sim.WRITE, sim.READ])
            self.assertEqual(results["results"][n], requests)
        self.assertEqual(results["drops"], 0)
        return sim, dut, results

    def test_4_nodes(self):
        sim, dut, results = self.run_bench(nodes=4)
        self.assertEqual(results["was_down"], 1)
        # Statuses expected: served, from the node cut (no link), to it or unknown (no route).
        statuses = set(s[4] for n in range(4) for s in dut.steps[n] if s[0] == sim.WRITE)
        self.assertTrue({FABRIC_OK, FABRIC_LINK, FABRIC_RANGE, FABRIC_NODE} <= statuses)

    def test_4_nodes_small_buffers(self):
        self.run_bench(nodes=4, sram_latency=20, queue_depth=1, buffer_depth=2)

    def test_4_nodes_lossy(self):
        # One packet out of 10 lost on every link: sent again, every request served once.
        sim, dut, results = self.run_bench(nodes=4, loss=26, scenarios=["all", "hotspot", "errors"])

    def test_4_nodes_wide(self):
        # 128 bits per cycle on the links, with and without losses.
        self.run_bench(nodes=4, link_width=128)
        self.run_bench(nodes=4, link_width=64, loss=26, scenarios=["all", "hotspot", "errors"])

    def test_4_nodes_hypercube(self):
        # Requests forwarded by a node (2 links), and going around the node cut.
        sim, dut, results = self.run_bench(nodes=4, topology="hypercube")
        self.assertGreater(results["forwarded"], 0)
        statuses = [s[4] for n in range(4) for s in dut.steps[n] if s[0] == sim.WRITE and s[2] == 48]
        # With the node cut: 3 requests from it, 3 to it, and the 6 others served.
        self.assertEqual(sorted(statuses), sorted([FABRIC_LINK]*3 + [FABRIC_NODE]*3 + [FABRIC_OK]*6))

if __name__ == "__main__":
    unittest.main()
