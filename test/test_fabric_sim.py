#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric end-to-end Verilator simulations (two symmetric nodes, one SerWB link, requests in flight;
normal accesses in both directions, errors in flight with valid requests, link cut and recovery).

Uses the self-checking FabricTestSoC of bench/fabric/sim.py. Skipped when Verilator is not installed.
"""

import os
import sys
import shutil
import unittest
import tempfile
import subprocess

SIM = os.path.join(os.path.dirname(__file__), "..", "bench", "fabric", "sim.py")

CONFIGS = {
    "default"      : [],
    "slow"         : ["--sram-latency", "64"],
    "flow_control" : ["--with-flow-control", "--sram-latency", "16"],
}

@unittest.skipUnless(shutil.which("verilator"), "Verilator not installed.")
class TestFabricSim(unittest.TestCase):
    def run_sim(self, args):
        with tempfile.TemporaryDirectory() as tmp:
            p = subprocess.run([sys.executable, os.path.abspath(SIM), "--output-dir", tmp, *args],
                cwd            = tmp,
                stdout         = subprocess.PIPE,
                stderr         = subprocess.STDOUT,
                text           = True,
                timeout        = 600,
            )
        results = [l for l in p.stdout.splitlines() if "FABRIC TEST" in l]
        return results[0] if results else p.stdout[-2000:]

    def test_sim(self):
        for name, args in CONFIGS.items():
            with self.subTest(config=name):
                result = self.run_sim(args)
                self.assertIn("FABRIC TEST: PASS", result, f"{name}: {result}")

if __name__ == "__main__":
    unittest.main()
