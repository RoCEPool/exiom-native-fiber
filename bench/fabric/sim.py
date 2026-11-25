#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric end-to-end simulations.

Two nodes (default), one SerWB link:

    Sequencer A -> FabricNode A -> SERWBCore -> SERWBPHY <-wires-> SERWBPHY -> SERWBCore -> FabricNode B <- Sequencer B
                   + SRAM A                                                              + SRAM B

Bit-level generic PHYs (link initialization, 8b10b, CRC, auto-retrain). Hardware sequencers send
groups of requests without waiting for their results (up to 8 in flight) and check every result
against its request:

- normal  : writes and reads in flight together, more requests than slots, both nodes requesting
            each other at the same time.
- errors  : out of window, memory not answering and unknown node, in flight with valid requests.
- link    : wires cut with requests in flight, link retrained, requests served again.

Network (--nodes 4, 8, 16...): the nodes of a topology (two-tier by default) with word-level links,
one hardware sequencer per node:

- all     : every node writes one word in every other node, then reads it (requests in flight,
            forwarded over up to 3 links).
- hotspot : all the nodes write node 0 together.
- errors  : out of window and unknown node.
- down    : one node is cut from the fabric: once the routes have followed, requests to it are
            answered with an error and the others are served by the paths that are left; then the
            node comes back.

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

from exiom.fabric   import *
from exiom.topology import two_tier, hypercube
from exiom.network  import FabricNetwork, expected_status

# Scenarios ----------------------------------------------------------------------------------------

WRITE, READ, SYNC, CUT, WAIT_LINK, WAIT_PEER, BARRIER, DELAY = range(8)

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

# Network Scenarios --------------------------------------------------------------------------------

NETWORK_SIZE      = 512 # Window of each node (128 words).
NETWORK_SCENARIOS = ["all", "hotspot", "errors", "down"]

def value(src, dst, salt=0):
    return (0xa5000000 | (salt << 20) | (src << 8) | dst) & 0xffffffff

SETTLE = 4096 # Cycles for the routes to follow a change.

def network_steps(topology, max_hops, src, scenarios, down):
    """Steps of the sequencer of node `src`."""
    steps  = Steps(dst=None)
    others = [d for d in range(topology.nodes) if d != src]
    if "all" in scenarios:
        for dst in others:
            steps.write(src, value(src, dst), dst=dst)
        steps.step(SYNC)
        for dst in others:
            steps.read(src, value(src, dst), dst=dst)
        steps.step(SYNC)
    if "hotspot" in scenarios and src != 0:
        for i in range(4):                       # In flight, executed in order.
            steps.write(32 + src, value(src, 0, 1 + i), dst=0)
        steps.step(SYNC)
        steps.read(32 + src, value(src, 0, 4), dst=0)
        steps.step(SYNC)
    if "errors" in scenarios:
        steps.write(200, 0xdead0001, status=FABRIC_RANGE, dst=others[0])
        steps.write(1,   0xdead0002, status=FABRIC_NODE,  dst=200)
        steps.read( src, value(src, others[-1]) if "all" in scenarios else 0, dst=others[-1])
        steps.step(SYNC)
    if "down" in scenarios:
        steps.step(BARRIER)                      # Node cut when all the nodes are here.
        steps.step(DELAY, SETTLE)
        for dst in others:
            steps.write(48, value(src, dst, 5), status=expected_status(topology, max_hops, src, dst, [down]), dst=dst)
        steps.step(SYNC)
        steps.step(BARRIER)                      # Node back.
        steps.step(DELAY, SETTLE)
        for dst in others:
            steps.write(64 + src, value(src, dst, 6), dst=dst)
        steps.step(SYNC)
        for dst in others:
            steps.read(64 + src, value(src, dst, 6), dst=dst)
        steps.step(SYNC)
    return steps

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
        self.at_barrier  = Signal()
        self.go          = Signal()

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

        delay = Signal(32)
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
                BARRIER   : [self.at_barrier.eq(outstanding == 0), If(self.go, *next_step())],
                DELAY     : [NextValue(delay, delay + 1), If(delay == datas[step], NextValue(delay, 0), *next_step())],
            })
        )
        fsm.act("DONE",
            self.done.eq(outstanding == 0)
        )
        self.sync += If(sent, slot_steps[requester.tag[:slot_bits]].eq(step))
        self.comb += If(fsm.ongoing("DONE") & (outstanding == 0), self.at_barrier.eq(1))

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
                request_timeout = 32768,
                memory_timeout  = 256,
                # Flow control packets take 5 words on the link (200 cycles with the bit-level PHY).
                fc_period       = 2048,
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

# Fabric Network Bench -----------------------------------------------------------------------------

class FabricNetworkBench(LiteXModule):
    """The nodes of a topology (word-level links) and one self-checking sequencer per node."""
    def __init__(self, nodes=4, topology="two_tier", scenarios=NETWORK_SCENARIOS, down=None, sram_latency=0,
        queue_depth=2, buffer_depth=4, timeout=4000000):
        self.done        = Signal()
        self.errors      = Signal(32)
        self.failed_node = Signal(8)
        self.failed_step = Signal(16)
        self.cycles      = Signal(32)
        self.link_ready  = Signal(reset=1)
        self.timeout     = Signal()
        self.node_down   = Signal()

        # # #

        self.topology = topology = {"two_tier": two_tier, "hypercube": hypercube}[topology](nodes)
        if "down" in scenarios:
            # Default: the node with the most links (in a two-tier fabric, one with a cable to
            # another group).
            if down is None:
                down = max(range(nodes), key=lambda n: (topology.ports_used(n), n))
        else:
            down = None
        self.down = down

        # Network ------------------------------------------------------------------------------
        self.net = net = FabricNetwork(topology,
            size            = NETWORK_SIZE,
            requests        = REQUESTS,
            request_timeout = 16384,
            memory_timeout  = 256,
            queue_depth     = queue_depth,
            buffer_depth    = buffer_depth,
        )
        if down is not None:
            for up in net.node_links(down):
                self.comb += up.eq(~self.node_down)

        # Memories and sequencers -----------------------------------------------------------------
        self.srams = srams = []
        self.seqs  = seqs  = []
        self.steps = {}
        for n, node in enumerate(net.nodes):
            sram  = wishbone.SRAM(NETWORK_SIZE)
            mem   = node.responder.mem
            count = Signal(max=sram_latency + 2)
            self.comb += [
                mem.connect(sram.bus, omit={"stb", "ack"}),
                sram.bus.stb.eq(mem.stb & (count == sram_latency)),
                mem.ack.eq(sram.bus.ack),
            ]
            self.sync += [
                If(~(mem.cyc & mem.stb) | mem.ack,
                    count.eq(0)
                ).Elif(count != sram_latency,
                    count.eq(count + 1)
                )
            ]
            self.add_module(f"sram{n}", sram)
            srams.append(sram)

            seq = Sequencer(node, network_steps(topology, net.max_hops, n, scenarios, down), self.link_ready)
            self.add_module(f"seq{n}", seq)
            seqs.append(seq)
            self.steps[n] = seq.steps

        # Barriers: all the sequencers go on together; the node is cut after the first one and back
        # after the second one.
        go       = Signal()
        barriers = Signal(2)
        self.sync += [
            go.eq(0),
            If(Cat(*[seq.at_barrier for seq in seqs]) == (2**nodes - 1),
                If(~go & ~self.done,
                    go.eq(1),
                    barriers.eq(barriers + 1),
                )
            )
        ]
        self.comb += [seq.go.eq(go) for seq in seqs]
        self.comb += self.node_down.eq(barriers == 1)

        # Results --------------------------------------------------------------------------------
        self.comb += [
            self.done.eq(Cat(*[seq.done for seq in seqs]) == (2**nodes - 1)),
            self.errors.eq(sum(seq.errors for seq in seqs)),
        ]
        for n, seq in reversed(list(enumerate(seqs))):
            self.comb += If(seq.errors != 0,
                self.failed_node.eq(n),
                self.failed_step.eq(seq.failed_step),
            )
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
    def __init__(self, nodes=2, **kwargs):
        platform     = Platform()
        sys_clk_freq = int(1e6)

        # SoCMini ----------------------------------------------------------------------------------
        SoCMini.__init__(self, platform, clk_freq=sys_clk_freq)

        # CRG --------------------------------------------------------------------------------------
        self.crg = CRG(platform.request("sys_clk"))

        # Bench ------------------------------------------------------------------------------------
        if nodes == 2:
            self.bench = bench = FabricTestBench(clk_freq=sys_clk_freq, **kwargs)
        else:
            self.bench = bench = FabricNetworkBench(nodes=nodes, **kwargs)
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
    parser.add_argument("--nodes",             default=2, type=int,         help="Nodes (2: SerWB link, more: network with word-level links).")
    parser.add_argument("--topology",          default="two_tier",          help="Network: topology (two_tier or hypercube).")
    parser.add_argument("--down",              default=None, type=int,      help="Network: node cut by the down scenario.")
    parser.add_argument("--queue-depth",       default=2, type=int,         help="Network: packets per class in the ports.")
    parser.add_argument("--scenarios",         default=None,                help="Scenarios to run (comma separated).")
    parser.add_argument("--sram-latency",      default=0, type=int,         help="SRAM ack latency (cycles).")
    parser.add_argument("--with-flow-control", action="store_true",         help="Two nodes: enable PHY flow control.")
    parser.add_argument("--output-dir",        default=None,                help="Build directory.")
    parser.add_argument("--trace",             action="store_true",         help="Enable tracing.")
    parser.add_argument("--trace-fst",         action="store_true",         help="Use .fst format for tracing.")
    args = parser.parse_args()

    available = SCENARIOS if args.nodes == 2 else NETWORK_SCENARIOS
    scenarios = available if args.scenarios is None else args.scenarios.split(",")
    for scenario in scenarios:
        if scenario not in available:
            raise ValueError(f"Unknown scenario {scenario} (available: {available}).")

    sim_config = SimConfig()
    sim_config.add_clocker("sys_clk", freq_hz=1e6)

    if args.nodes == 2:
        soc = FabricTestSoC(
            scenarios         = scenarios,
            sram_latency      = args.sram_latency,
            with_flow_control = args.with_flow_control,
        )
    else:
        soc = FabricTestSoC(
            nodes        = args.nodes,
            topology     = args.topology,
            scenarios    = scenarios,
            down         = args.down,
            sram_latency = args.sram_latency,
            queue_depth  = args.queue_depth,
        )
    builder = Builder(soc, output_dir=args.output_dir, csr_csv=None)
    builder.build(sim_config=sim_config,
        trace     = args.trace,
        trace_fst = int(args.trace_fst),
    )

if __name__ == "__main__":
    main()
