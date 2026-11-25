#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric end-to-end simulation: two symmetric nodes, one SerWB link, tagged requests in flight.

    Sequencer A -> FabricNode A -> SERWBCore -> SERWBPHY <-wires-> SERWBPHY -> SERWBCore -> FabricNode B <- Sequencer B
                   + SRAM A                                                              + SRAM B

Bit-level generic PHYs (link initialization, 8b10b, CRC, auto-retrain). Hardware sequencers send
groups of requests without waiting for their results (up to 8 in flight) and check every result
against its request:

- normal  : writes and reads in flight together, more requests than slots, both nodes requesting
            each other at the same time.
- errors  : out of window, memory not answering and unknown node, in flight with valid requests.
- link    : wires cut with requests in flight, link retrained, requests served again.

Displays "FABRIC TEST: PASS" or "FABRIC TEST: FAIL" and ends the simulation (see
test/test_fabric_sim.py).
"""

import os
import sys
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from migen import *

from litex.gen import *

from litex.build.generic_platform import *
from litex.build.sim              import SimPlatform
from litex.build.sim.config       import SimConfig

from litex.soc.interconnect import wishbone

from litex.soc.integration.soc     import *
from litex.soc.integration.builder import *

from liteiclink.serwb.genphy import SERWBPHY
from liteiclink.serwb.core   import SERWBCore

from exiom.fabric import *

# Scenarios ----------------------------------------------------------------------------------------

WRITE, READ, SYNC, CUT, WAIT_LINK, WAIT_PEER = range(6)

NODE_A   = 1
NODE_B   = 2
SIZE     = 128 # Window of each node (32 words).
HANG_ADR = 30  # Memory word that never answers.
REQUESTS = 8   # Requests in flight per node.

def pattern(i, salt=0):
    return (0xa5000000 | (salt << 20) | (i << 8) | ((~i) & 0xff)) & 0xffffffff

class Steps(list):
    """Steps: (kind, destination node, word, data, expected status)."""
    def __init__(self, dst, salt=0):
        list.__init__(self)
        self.dst  = dst
        self.salt = salt

    def write(self, word, data=None, status=FABRIC_OK, dst=None):
        data = pattern(word, self.salt) if data is None else data
        self.append((WRITE, self.dst if dst is None else dst, word, data, status))

    def read(self, word, data=None, status=FABRIC_OK, dst=None):
        data = pattern(word, self.salt) if data is None else data
        self.append((READ, self.dst if dst is None else dst, word, data, status))

    def step(self, kind, data=0):
        self.append((kind, 0, 0, data, 0))

def scenario_steps_a(scenarios):
    steps = Steps(dst=NODE_B, salt=1)
    if "normal" in scenarios:
        for i in range(8):                       # 8 writes in flight.
            steps.write(i)
        steps.step(SYNC)
        for i in [5, 2, 7, 0, 3, 6, 1, 4]:       # 8 reads in flight.
            steps.read(i)
        for i in range(8, 24):                   # More requests than slots, no pause.
            steps.write(i)
        steps.step(SYNC)
        for i in range(8, 24):
            steps.read(i)
        steps.step(SYNC)
    if "errors" in scenarios:
        steps.write(24)
        steps.write(40, 0xdead0001, status=FABRIC_RANGE)
        steps.read( 40, status=FABRIC_RANGE)
        steps.write(HANG_ADR, 0xdead0002, status=FABRIC_TIMEOUT)
        steps.write(25)
        steps.read( HANG_ADR, status=FABRIC_TIMEOUT)
        steps.write(2, 0xdead0003, status=FABRIC_NODE, dst=9)
        steps.write(26)
        steps.step(SYNC)
        for i in [24, 25, 26]:
            steps.read(i)
        steps.step(SYNC)
    if "link" in scenarios:
        steps.step(WAIT_PEER)                    # Node B requests done.
        steps.write(27)
        steps.step(SYNC)
        steps.step(CUT, 1)
        for i in range(4):                       # In flight during the link loss.
            steps.write(28, 0xdead0004 + i, status=FABRIC_LINK)
        steps.step(SYNC)
        steps.step(CUT, 0)
        steps.step(WAIT_LINK)
        steps.write(29)
        steps.read( 29)
        steps.read( 27)                          # Written before the cut.
        steps.step(SYNC)
    return steps

def scenario_steps_b(scenarios):
    # Node B requests node A while node A requests node B.
    steps = Steps(dst=NODE_A, salt=2)
    if "normal" in scenarios:
        for i in range(12):
            steps.write(i)
        steps.step(SYNC)
        for i in range(12):
            steps.read(i)
    steps.step(SYNC)
    return steps

SCENARIOS = ["normal", "errors", "link"]

# Sequencer ----------------------------------------------------------------------------------------

class Sequencer(LiteXModule):
    """Sends the requests of `steps` on the Requester of `node` and checks their results."""
    def __init__(self, node, steps, link_ready):
        self.steps       = steps
        self.done        = Signal()
        self.errors      = Signal(32)
        self.failed_step = Signal(16) # First failed step.
        self.results     = Signal(32) # Results checked.
        self.link_cut    = Signal()
        self.peer_done   = Signal()

        # # #

        assert len(steps) > 0
        kinds    = Array([Constant(s[0], 3)  for s in steps])
        dsts     = Array([Constant(s[1], 8)  for s in steps])
        words    = Array([Constant(s[2], 32) for s in steps])
        datas    = Array([Constant(s[3], 32) for s in steps])
        statuses = Array([Constant(s[4], 4)  for s in steps])

        requester   = node.requester
        sink        = requester.sink
        source      = requester.source
        slot_bits   = max(1, bits_for(REQUESTS - 1))
        step        = Signal(max=len(steps) + 1)
        outstanding = Signal(max=REQUESTS + 2)
        failed      = Signal()

        # Step of the request of each slot.
        slot_steps = Array([Signal(16, name=f"slot{i}_step") for i in range(REQUESTS)])

        # Requests.
        # ---------
        sent = Signal()
        self.comb += [
            sink.we.eq(kinds[step] == WRITE),
            sink.dst.eq(dsts[step]),
            sink.adr.eq(words[step]),
            sink.dat_w.eq(datas[step]),
            sink.sel.eq(0b1111),
            sent.eq(sink.valid & sink.ready),
        ]

        def next_step():
            return [
                NextValue(step, step + 1),
                If(step == (len(steps) - 1),
                    NextState("DONE")
                )
            ]

        self.fsm = fsm = FSM(reset_state="WAIT-LINK")
        fsm.act("WAIT-LINK",
            If(link_ready,
                NextState("STEP")
            )
        )
        fsm.act("STEP",
            Case(kinds[step], {
                WRITE     : [sink.valid.eq(1), If(sink.ready, *next_step())],
                READ      : [sink.valid.eq(1), If(sink.ready, *next_step())],
                SYNC      : If(outstanding == 0, *next_step()),
                CUT       : [NextValue(self.link_cut, datas[step][0])] + next_step(),
                WAIT_LINK : If(link_ready, *next_step()),
                WAIT_PEER : If(self.peer_done, *next_step()),
            })
        )
        fsm.act("DONE",
            self.done.eq(outstanding == 0)
        )
        self.sync += If(sent, slot_steps[requester.tag[:slot_bits]].eq(step))

        # Results.
        # --------
        result_step = Signal(16)
        received    = Signal()
        mismatch    = Signal()
        self.comb += [
            source.ready.eq(1),
            received.eq(source.valid & source.ready),
            result_step.eq(slot_steps[source.tag[:slot_bits]]),
            mismatch.eq(
                (source.status != statuses[result_step]) |
                (source.we     != (kinds[result_step] == WRITE)) |
                (~source.we & (source.status == FABRIC_OK) & (source.dat_r != datas[result_step]))
            ),
        ]
        self.sync += [
            outstanding.eq(outstanding + sent - received),
            If(received,
                self.results.eq(self.results + 1),
                If(mismatch,
                    self.errors.eq(self.errors + 1),
                    If(~failed,
                        failed.eq(1),
                        self.failed_step.eq(result_step),
                    )
                )
            )
        ]

# Fabric Test Bench --------------------------------------------------------------------------------

class FabricTestBench(LiteXModule):
    """Two nodes, the link and the self-checking sequencers (simulated here with Verilator and in
    test/test_fabric_bench.py with the Migen simulator)."""
    def __init__(self, clk_freq=int(1e6), scenarios=SCENARIOS, sram_latency=0, with_flow_control=False,
        timeout=2000000):
        self.done        = Signal()
        self.errors      = Signal(32)
        self.failed_node = Signal(8)
        self.failed_step = Signal(16)
        self.cycles      = Signal(32)
        self.link_ready  = Signal()
        self.link_cut    = Signal()
        self.timeout     = Signal()

        # # #

        # Link ---------------------------------------------------------------------------------
        a_pads = Record([("tx", 1), ("rx", 1)])
        b_pads = Record([("tx", 1), ("rx", 1)])
        self.comb += [
            b_pads.rx.eq(a_pads.tx & ~self.link_cut),
            a_pads.rx.eq(b_pads.tx & ~self.link_cut),
        ]
        phy_kwargs  = dict(device="SIM", init_timeout=128, with_flow_control=with_flow_control)
        core_kwargs = dict(clk_freq=clk_freq, with_crc=True)
        self.a_phy  = SERWBPHY(pads=a_pads, mode="master", with_auto_retrain=True, **phy_kwargs)
        self.b_phy  = SERWBPHY(pads=b_pads, mode="slave", **phy_kwargs)
        self.a_core = SERWBCore(phy=self.a_phy, mode="slave",  **core_kwargs)
        self.b_core = SERWBCore(phy=self.b_phy, mode="master", **core_kwargs)
        self.comb += self.link_ready.eq(self.a_phy.init.ready & self.b_phy.init.ready)

        # Nodes --------------------------------------------------------------------------------
        self.nodes = nodes = {}
        self.srams = srams = {}
        for name, node_id, phy, core in [("a", NODE_A, self.a_phy, self.a_core), ("b", NODE_B, self.b_phy, self.b_core)]:
            node = FabricNode(node_id,
                size            = SIZE,
                requests        = REQUESTS,
                request_timeout = 16384,
                memory_timeout  = 256,
                link_ready      = phy.init.ready,
            )
            node.connect_serwb(core)
            self.add_module(f"{name}_node", node)
            nodes[name] = node

            # Memory: SRAM with optional latency and a word that never answers.
            sram  = wishbone.SRAM(256)
            mem   = node.responder.mem
            hang  = Signal()
            count = Signal(max=sram_latency + 2)
            self.comb += [
                hang.eq(mem.adr == HANG_ADR),
                mem.connect(sram.bus, omit={"stb", "cyc", "ack"}),
                sram.bus.cyc.eq(mem.cyc & ~hang),
                sram.bus.stb.eq(mem.stb & ~hang & (count == sram_latency)),
                mem.ack.eq(sram.bus.ack),
            ]
            self.sync += [
                If(~(mem.cyc & mem.stb) | mem.ack,
                    count.eq(0)
                ).Elif(count != sram_latency,
                    count.eq(count + 1)
                )
            ]
            self.add_module(f"{name}_sram", sram)
            srams[name] = sram

        # Sequencers ---------------------------------------------------------------------------
        self.a_seq = a_seq = Sequencer(nodes["a"], scenario_steps_a(scenarios), self.link_ready)
        self.b_seq = b_seq = Sequencer(nodes["b"], scenario_steps_b(scenarios), self.link_ready)
        self.steps = {NODE_A: a_seq.steps, NODE_B: b_seq.steps}
        self.comb += [
            a_seq.peer_done.eq(b_seq.done),
            b_seq.peer_done.eq(a_seq.done),
            self.link_cut.eq(a_seq.link_cut),
            self.done.eq(a_seq.done & b_seq.done),
            self.errors.eq(a_seq.errors + b_seq.errors),
            If(a_seq.errors != 0,
                self.failed_node.eq(NODE_A),
                self.failed_step.eq(a_seq.failed_step),
            ).Else(
                self.failed_node.eq(NODE_B),
                self.failed_step.eq(b_seq.failed_step),
            ),
        ]

        # Timeout (request never answered or link never ready).
        self.sync += self.cycles.eq(self.cycles + 1)
        self.comb += self.timeout.eq(self.cycles == timeout)

# IOs ----------------------------------------------------------------------------------------------

_io = [
    ("sys_clk", 0, Pins(1)),
    ("sys_rst", 0, Pins(1)),
]

# Platform -----------------------------------------------------------------------------------------

class Platform(SimPlatform):
    def __init__(self):
        SimPlatform.__init__(self, "SIM", _io)

# FabricTestSoC ------------------------------------------------------------------------------------

class FabricTestSoC(SoCMini):
    def __init__(self, **kwargs):
        platform     = Platform()
        sys_clk_freq = int(1e6)

        # SoCMini ----------------------------------------------------------------------------------
        SoCMini.__init__(self, platform, clk_freq=sys_clk_freq)

        # CRG --------------------------------------------------------------------------------------
        self.crg = CRG(platform.request("sys_clk"))

        # Bench ------------------------------------------------------------------------------------
        self.bench = bench = FabricTestBench(clk_freq=sys_clk_freq, **kwargs)
        requests = sum(1 for steps in bench.steps.values() for s in steps if s[0] in [WRITE, READ])
        self.sync += [
            If(bench.done,
                If(bench.errors == 0,
                    Display("FABRIC TEST: PASS ({} requests, %d cycles)".format(requests), bench.cycles)
                ).Else(
                    Display("FABRIC TEST: FAIL (%d errors, first at node %d step %d)",
                        bench.errors, bench.failed_node, bench.failed_step)
                ),
                Finish()
            ),
            If(bench.timeout,
                Display("FABRIC TEST: FAIL (timeout, link ready: %d)", bench.link_ready),
                Finish()
            )
        ]

# Build --------------------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fabric LiteX/Verilator simulation.")
    parser.add_argument("--scenarios",         default=",".join(SCENARIOS), help="Scenarios to run (comma separated).")
    parser.add_argument("--sram-latency",      default=0, type=int,         help="SRAM ack latency (cycles).")
    parser.add_argument("--with-flow-control", action="store_true",         help="Enable PHY flow control.")
    parser.add_argument("--output-dir",        default=None,                help="Build directory.")
    parser.add_argument("--trace",             action="store_true",         help="Enable tracing.")
    parser.add_argument("--trace-fst",         action="store_true",         help="Use .fst format for tracing.")
    args = parser.parse_args()

    scenarios = args.scenarios.split(",")
    for scenario in scenarios:
        if scenario not in SCENARIOS:
            raise ValueError(f"Unknown scenario {scenario} (available: {SCENARIOS}).")

    sim_config = SimConfig()
    sim_config.add_clocker("sys_clk", freq_hz=1e6)

    soc = FabricTestSoC(
        scenarios         = scenarios,
        sram_latency      = args.sram_latency,
        with_flow_control = args.with_flow_control,
    )
    builder = Builder(soc, output_dir=args.output_dir, csr_csv=None)
    builder.build(sim_config=sim_config,
        trace     = args.trace,
        trace_fst = int(args.trace_fst),
    )

if __name__ == "__main__":
    main()
