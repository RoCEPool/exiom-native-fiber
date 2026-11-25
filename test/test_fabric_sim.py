#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric end-to-end Verilator simulations:

- Two symmetric nodes, one bit-level SerWB link: requests in flight in both directions, errors in
  flight with valid requests, link cut and recovery.
- Networks of 4 and 8 nodes (word-level links): all nodes to all nodes over up to 3 links,
  hotspot, errors, one node cut from the fabric and back.

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
    # Two nodes, bit-level SerWB link.
    "default"              : [],
    "slow"                 : ["--sram-latency", "64"],
    "flow_control"         : ["--with-flow-control", "--sram-latency", "16"],
    # Networks, word-level links (two-tier topology cabled for 16 nodes).
    "4_nodes"              : ["--nodes", "4"],
    "4_nodes_small_buffers": ["--nodes", "4", "--queue-depth", "1", "--sram-latency", "20"],
    "8_nodes"              : ["--nodes", "8", "--sram-latency", "8"],
    "8_nodes_hypercube"    : ["--nodes", "8", "--topology", "hypercube"],
    # 16 nodes: with the Migen simulator (test_fabric_network.py); their Verilator build needs more
    # memory than the CI runner has (./bench/fabric/sim.py --nodes 16 on a larger machine).
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
                timeout        = 3000,
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
