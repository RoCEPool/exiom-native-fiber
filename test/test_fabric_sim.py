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
- Networks of 4 nodes (word-level links, 32 and 128 bits per cycle, with and without packets
  lost): all nodes to all nodes, hotspot, errors, one node cut from the fabric and back.

Uses the self-checking FabricTestSoC of bench/fabric/sim.py. Skipped when Verilator is not installed.
"""

import os
import sys
import shutil
import unittest
import tempfile
import subprocess

from test.nightly import nightly

SIM = os.path.join(os.path.dirname(__file__), "..", "bench", "fabric", "sim.py")

CONFIGS = {
    # Two nodes, bit-level SerWB link.
    "default"              : [],
    "slow"                 : ["--sram-latency", "64"],
    "flow_control"         : ["--with-flow-control", "--sram-latency", "16"],
    # Networks, word-level links (two-tier topology cabled for 16 nodes).
    "4_nodes"              : ["--nodes", "4"],
    "4_nodes_wide_lossy"   : ["--nodes", "4", "--link-width", "128", "--loss", "26", "--scenarios", "all,hotspot,errors"],
}

NIGHTLY_CONFIGS = {
    "4_nodes_small_buffers": ["--nodes", "4", "--queue-depth", "1", "--sram-latency", "20"],
    "4_nodes_hypercube"    : ["--nodes", "4", "--topology", "hypercube"],
    "4_nodes_lossy"        : ["--nodes", "4", "--loss", "26", "--scenarios", "all,hotspot,errors"],
    "4_nodes_wide"         : ["--nodes", "4", "--link-width", "128"],
    # 8 and 16 nodes: with the Migen simulator (test_fabric_network.py, test_fabric_bench.py);
    # their Verilator build needs more memory than the CI runner has (./bench/fabric/sim.py
    # --nodes 8 on a larger machine).
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

    def run_configs(self, configs):
        for name, args in configs.items():
            with self.subTest(config=name):
                result = self.run_sim(args)
                self.assertIn("FABRIC TEST: PASS", result, f"{name}: {result}")

    def test_sim(self):
        self.run_configs(CONFIGS)

    @nightly
    def test_sim_nightly(self):
        self.run_configs(NIGHTLY_CONFIGS)

if __name__ == "__main__":
    unittest.main()
