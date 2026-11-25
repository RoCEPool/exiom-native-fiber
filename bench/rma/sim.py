#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Remote Memory Access end-to-end simulation: two nodes, one SerWB link, one remote SRAM.

    Sequencer -> RMAInitiator -> SERWBCore -> SERWBPHY <-wires-> SERWBPHY -> SERWBCore -> RMATarget -> SRAM
    \\____________________ local node ____________________/      \\____________ remote node ____________/

Bit-level generic PHYs (link initialization, 8b10b, CRC, auto-retrain). A hardware sequencer runs
the scenarios and checks every result:

- normal    : writes, read back, consecutive accesses.
- range     : accesses outside the window (refused by the Target, and by the Initiator).
- timeout   : remote memory not answering (Target watchdog), then accesses still served.
- link      : wires cut during a write, link retrained, accesses served again.

Displays "RMA TEST: PASS" or "RMA TEST: FAIL" and ends the simulation (see test/test_rma_sim.py).
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

from exiom.rma import *

# Scenarios ----------------------------------------------------------------------------------------

WRITE, READ, CUT, WAIT_LINK = range(4)

ORIGIN         = 0x30000000 # Window on the local bus.
TARGET_SIZE    = 128        # Remote window (32 words).
INITIATOR_SIZE = 192        # Local view, larger than the remote window (remote range check).
HANG_ADR       = 30         # Remote memory word that never answers.

def pattern(i):
    return (0xa5000000 | (i << 16) | ((~i) & 0xffff)) & 0xffffffff

def scenario_steps(scenarios):
    """Steps: (kind, word, data, expected error, expected error code (None: not checked))."""
    steps = []
    def write(word, data, err=0, code=None):
        steps.append((WRITE, word, data, err, code))
    def read(word, data=0, err=0, code=None):
        steps.append((READ, word, data, err, code))
    if "normal" in scenarios:
        for i in range(8):
            write(i, pattern(i))
        for i in range(8):
            read(i, pattern(i))
        write(3, 0x12345678)
        read(3, 0x12345678)
    if "range" in scenarios:
        read( 40, err=1, code=RMA_ERROR_READ)   # Refused by the Target.
        write(40, 0xdead0001, err=1, code=RMA_ERROR_RANGE)
        read( 60, err=1, code=RMA_ERROR_RANGE)  # Refused by the Initiator.
        write(60, 0xdead0002, err=1, code=RMA_ERROR_RANGE)
        write(31, pattern(31))                  # Last word of the window.
        read( 31, pattern(31))
    if "timeout" in scenarios:
        write(HANG_ADR, 0xdead0003, err=1, code=RMA_ERROR_TIMEOUT)
        read( HANG_ADR, err=1, code=RMA_ERROR_READ)
        write(16, pattern(16))                  # Link not stalled.
        read( 16, pattern(16))
    if "link" in scenarios:
        write(20, pattern(20))
        steps.append((CUT, 0, 1, 0, None))
        write(21, 0xdead0004, err=1, code=RMA_ERROR_LINK)
        steps.append((CUT, 0, 0, 0, None))
        steps.append((WAIT_LINK, 0, 0, 0, None))
        write(22, pattern(22))
        read( 22, pattern(22))
        read( 20, pattern(20))                  # Written before the cut.
    return steps

SCENARIOS = ["normal", "range", "timeout", "link"]

# RMA Test Bench -----------------------------------------------------------------------------------

class RMATestBench(LiteXModule):
    """Two nodes, the link and the self-checking sequencer (simulated here with Verilator and in
    test/test_rma_bench.py with the Migen simulator)."""
    def __init__(self, clk_freq=int(1e6), scenarios=SCENARIOS, sram_latency=0, with_flow_control=False,
        timeout=2000000):
        self.done        = Signal()
        self.errors      = Signal(32)
        self.failed_step = Signal(16) # First failed step.
        self.cycles      = Signal(32)
        self.link_ready  = Signal()
        self.link_cut    = Signal()

        # # #

        # Link ---------------------------------------------------------------------------------
        local_pads  = Record([("tx", 1), ("rx", 1)])
        remote_pads = Record([("tx", 1), ("rx", 1)])
        self.comb += [
            remote_pads.rx.eq(local_pads.tx  & ~self.link_cut),
            local_pads.rx.eq( remote_pads.tx & ~self.link_cut),
        ]
        phy_kwargs  = dict(device="SIM", init_timeout=128, with_flow_control=with_flow_control)
        # Read responses have to wait for the slowest remote access (Target watchdog).
        core_kwargs = dict(clk_freq=clk_freq, with_crc=True, response_timeout=4000/clk_freq)

        # Local Node ---------------------------------------------------------------------------
        self.local_phy  = SERWBPHY(pads=local_pads, mode="master", with_auto_retrain=True, **phy_kwargs)
        self.local_core = SERWBCore(phy=self.local_phy, mode="slave", **core_kwargs)
        self.initiator  = RMAInitiator(size=INITIATOR_SIZE, origin=ORIGIN, link_ready=self.local_phy.init.ready)
        self.comb += self.initiator.link.connect(self.local_core.bus)

        # Remote Node --------------------------------------------------------------------------
        self.remote_phy  = SERWBPHY(pads=remote_pads, mode="slave", **phy_kwargs)
        self.remote_core = SERWBCore(phy=self.remote_phy, mode="master", **core_kwargs)
        self.target      = RMATarget(size=TARGET_SIZE, timeout=256)
        self.comb += self.remote_core.bus.connect(self.target.bus)

        # Remote memory: SRAM with optional latency and a word that never answers.
        self.sram = wishbone.SRAM(256)
        mem   = self.target.mem
        hang  = Signal()
        count = Signal(max=sram_latency + 2)
        self.comb += [
            hang.eq(mem.adr == HANG_ADR),
            mem.connect(self.sram.bus, omit={"stb", "cyc", "ack"}),
            self.sram.bus.cyc.eq(mem.cyc & ~hang),
            self.sram.bus.stb.eq(mem.stb & ~hang & (count == sram_latency)),
            mem.ack.eq(self.sram.bus.ack),
        ]
        self.sync += [
            If(~(mem.cyc & mem.stb) | mem.ack,
                count.eq(0)
            ).Elif(count != sram_latency,
                count.eq(count + 1)
            )
        ]

        # Sequencer ----------------------------------------------------------------------------
        steps = scenario_steps(scenarios)
        assert len(steps) > 0
        self.steps = steps
        kinds = Array([Constant(s[0], 2)  for s in steps])
        words = Array([Constant(s[1], 30) for s in steps])
        datas = Array([Constant(s[2], 32) for s in steps])
        errs  = Array([Constant(s[3], 1)  for s in steps])
        codes = Array([Constant(s[4] or 0, 3) for s in steps])
        check = Array([Constant(int(s[4] is not None), 1) for s in steps])

        bus    = self.initiator.bus
        step   = Signal(max=len(steps) + 1)
        failed = Signal()
        self.sync += self.cycles.eq(self.cycles + 1)
        self.comb += self.link_ready.eq(self.local_phy.init.ready & self.remote_phy.init.ready)

        def fail():
            return [
                NextValue(self.errors, self.errors + 1),
                If(~failed,
                    NextValue(failed, 1),
                    NextValue(self.failed_step, step),
                )
            ]

        def next_step():
            return [
                NextValue(step, step + 1),
                If(step == (len(steps) - 1),
                    NextState("DONE")
                ).Else(
                    NextState("STEP")
                )
            ]

        self.fsm = fsm = FSM(reset_state="WAIT-LINK")
        fsm.act("WAIT-LINK",
            If(self.link_ready,
                NextState("STEP")
            )
        )
        fsm.act("STEP",
            Case(kinds[step], {
                WRITE     : NextState("ACCESS"),
                READ      : NextState("ACCESS"),
                CUT       : [NextValue(self.link_cut, datas[step][0])] + next_step(),
                WAIT_LINK : If(self.link_ready, *next_step()),
            })
        )
        fsm.act("ACCESS",
            bus.cyc.eq(1),
            bus.stb.eq(1),
            bus.we.eq(kinds[step] == WRITE),
            bus.adr.eq(ORIGIN//4 + words[step]),
            bus.sel.eq(0b1111),
            bus.dat_w.eq(datas[step]),
            If(bus.ack | bus.err,
                If(bus.err != errs[step],
                    *fail()
                ).Elif((kinds[step] == READ) & bus.ack & (bus.dat_r != datas[step]),
                    *fail()
                ),
                NextState("CHECK")
            )
        )
        # Error code is registered: checked one cycle after the access.
        fsm.act("CHECK",
            If(check[step] & (self.initiator.error_code != codes[step]),
                *fail()
            ),
            *next_step()
        )
        fsm.act("DONE",
            self.done.eq(1)
        )

        # Timeout (stalled access or link never ready).
        self.timeout = Signal()
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

# RMATestSoC ---------------------------------------------------------------------------------------

class RMATestSoC(SoCMini):
    def __init__(self, **kwargs):
        platform     = Platform()
        sys_clk_freq = int(1e6)

        # SoCMini ----------------------------------------------------------------------------------
        SoCMini.__init__(self, platform, clk_freq=sys_clk_freq)

        # CRG --------------------------------------------------------------------------------------
        self.crg = CRG(platform.request("sys_clk"))

        # Bench ------------------------------------------------------------------------------------
        self.bench = bench = RMATestBench(clk_freq=sys_clk_freq, **kwargs)
        self.sync += [
            If(bench.done,
                If(bench.errors == 0,
                    Display("RMA TEST: PASS ({} steps, %d cycles)".format(len(bench.steps)), bench.cycles)
                ).Else(
                    Display("RMA TEST: FAIL (%d errors, first at step %d)", bench.errors, bench.failed_step)
                ),
                Finish()
            ),
            If(bench.timeout,
                Display("RMA TEST: FAIL (timeout, link ready: %d)", bench.link_ready),
                Finish()
            )
        ]

# Build --------------------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Remote Memory Access LiteX/Verilator simulation.")
    parser.add_argument("--scenarios",         default=",".join(SCENARIOS), help="Scenarios to run (comma separated).")
    parser.add_argument("--sram-latency",      default=0, type=int,         help="Remote SRAM ack latency (cycles).")
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

    soc = RMATestSoC(
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
