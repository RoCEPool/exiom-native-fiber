#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Remote Memory Access end-to-end bench (bench/rma/sim.py: two nodes, bit-level generic PHYs with
link initialization, CRC and auto-retrain) with the Migen simulator: same bench as the Verilator
simulation of test/test_rma_sim.py, with access to the internal state.
"""

import os
import unittest
import importlib.util

from migen import *

from litex.gen.sim import *

from exiom.rma import *

SIM = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bench", "rma", "sim.py")

def load_sim():
    spec   = importlib.util.spec_from_file_location("rma_sim", SIM)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

class TestRMABench(unittest.TestCase):
    def run_bench(self, steps=None, **kwargs):
        sim = load_sim()
        if steps is not None:
            sim.scenario_steps = lambda scenarios: steps
        dut     = sim.RMATestBench(**kwargs)
        results = {}
        def generator():
            link_losses = 0
            link_ready  = 0
            while not ((yield dut.done) or (yield dut.timeout)):
                ready = (yield dut.link_ready)
                link_losses += int(link_ready and not ready)
                link_ready   = ready
                yield
            results["done"]        = (yield dut.done)
            results["errors"]      = (yield dut.errors)
            results["failed_step"] = (yield dut.failed_step)
            results["link_losses"] = link_losses
            results["wr_count"]    = (yield dut.target.wr_count)
            results["mem"]         = []
            for i in range(64):
                results["mem"].append((yield dut.sram.mem[i]))
        run_simulation(dut, generator())
        return sim, dut, results

    def check_pass(self, **kwargs):
        sim, dut, results = self.run_bench(**kwargs)
        self.assertEqual(results["done"], 1)
        self.assertEqual(results["errors"], 0, "first failed step: {}".format(dut.steps[results["failed_step"]]))
        return sim, dut, results

    def test_all(self):
        sim, dut, results = self.check_pass()
        mem = results["mem"]
        # Link really lost and retrained during the link scenario.
        self.assertEqual(results["link_losses"], 1)
        # Refused/failed/lost writes never reached the memory.
        self.assertEqual(mem[sim.HANG_ADR], 0)
        self.assertEqual(mem[21], 0)
        self.assertEqual(mem[32:64], [0]*32) # Outside the window.
        # Written data.
        for i in [0, 1, 2, 4, 5, 6, 7, 16, 20, 22, 31]:
            self.assertEqual(mem[i], sim.pattern(i))
        self.assertEqual(mem[3], 0x12345678)

    def test_slow_memory(self):
        self.check_pass(sram_latency=64)

    def test_flow_control(self):
        self.check_pass(with_flow_control=True, sram_latency=16)

    def test_scenarios_alone(self):
        sim = load_sim()
        for scenario in sim.SCENARIOS:
            with self.subTest(scenario=scenario):
                self.check_pass(scenarios=[scenario])

    def test_checker(self):
        # The sequencer must report wrong data, missing errors and wrong error codes.
        sim = load_sim()
        cases = {
            "data"  : [(sim.WRITE, 1, 0x1111, 0, None), (sim.READ, 1, 0x2222, 0, None)],
            "error" : [(sim.WRITE, 40, 0x1111, 0, None)],
            "code"  : [(sim.WRITE, 40, 0x1111, 1, RMA_ERROR_LOST)],
        }
        for name, steps in cases.items():
            with self.subTest(case=name):
                _, _, results = self.run_bench(steps=steps)
                self.assertEqual(results["done"], 1)
                self.assertEqual(results["errors"], 1)
                self.assertEqual(results["failed_step"], len(steps) - 1)

if __name__ == "__main__":
    unittest.main()
