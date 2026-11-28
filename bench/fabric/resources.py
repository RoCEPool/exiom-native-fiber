#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric node resource usage estimation (Yosys synth_xilinx, UltraScale+ cells).

One node of a fabric of chassis of 8 nodes (7 ports inside the chassis and the inter-chassis
ports), with the route table of the whole fabric: switch, ports (queues, retransmission), routes,
Requester, Responder with the memory port, copies. The links (transceivers, link layer), the
memory and its controller, and the user logic are not in the node: their signals are the pins of
the design synthesized.

What it gives: cells after synthesis (LUT, FF, LUT RAM, block RAM, carry) and the longest
combinational path in cells. What it does not give: timing (no placement, no routing) - the
longest path only tells where to look. Compare with the device: for example a VU13P has 1728000
LUTs and 3456000 flip-flops.

    ./bench/fabric/resources.py --ports 9 --markdown build/fabric_resources.md
"""

import os
import re
import json
import time
import argparse
import tempfile
import subprocess

from migen import *

from litex.gen import *

from litex.build.xilinx import XilinxPlatform

from exiom.fabric   import *
from exiom.topology import *

# Device (for the comparison only) -----------------------------------------------------------------

DEVICE     = "xcvu13p-fhgb2104-2-e"
DEVICE_LUT = 1728000
DEVICE_FF  = 3456000

# Top ----------------------------------------------------------------------------------------------

class FabricNodeTop(LiteXModule):
    """Node 0 of a fabric of ``nodes`` nodes in chassis of 8, with ``ports`` ports."""
    def __init__(self, ports, nodes=128, link_width=128, size=4*2**40, pipelined=True, **kwargs):
        inter    = ports - 7
        chassis  = nodes//8
        assert inter >= 1 and 8*inter >= chassis - 1
        topology = two_tier(nodes, group_size=8, design_groups=chassis, inter_ports=inter)
        max_hops = min(topology.max_hops(topology.routes()) + 2, 6)
        link_ready = [Signal(name=f"link{p}_ready") for p in range(ports)]
        self.node = node = FabricNode(0,
            size       = size,
            ports      = ports,
            routes     = topology.costs()[0],
            max_hops   = max_hops,
            pipelined  = pipelined,
            link_width = link_width,
            link_ready = link_ready,
            **kwargs,
        )
        self.cd_sys = ClockDomain("sys")
        self.ios    = {self.cd_sys.clk, self.cd_sys.rst} | set(link_ready)
        endpoints   = [node.requester.sink, node.requester.source]
        endpoints  += [node.responder.cmd, node.responder.rsp] if pipelined else []
        for port in node.ports:
            endpoints += [port.sink, port.source]
        for endpoint in endpoints:
            self.ios |= set(endpoint.flatten())
        self.ios.add(node.requester.tag)
        if not pipelined:
            self.ios |= set(node.responder.mem.flatten())

# Synthesis ----------------------------------------------------------------------------------------

CELLS = {
    "LUT"    : r"LUT[1-6]",
    "FF"     : r"FD[A-Z]{2}",
    "LUTRAM" : r"RAM\d+[XM]\w*",
    "BRAM"   : r"RAMB\w+",
    "SRL"    : r"SRL\w+",
    "CARRY"  : r"CARRY\w+",
    "MUXF"   : r"MUXF\w+",
}

def parse_stat(stat):
    """Cell counts from the Yosys stat report, with "<cell> <count>" lines (older Yosys) or
    "<count> <cell>" lines (newer Yosys)."""
    results = {k: 0 for k in CELLS}
    cells   = re.findall(r"^\s+(\S+)\s+(\d+)\s*$", stat, re.M)
    cells  += [(cell, count) for count, cell in re.findall(r"^\s+(\d+)\s+(\S+)\s*$", stat, re.M)]
    for cell, count in cells:
        for k, pattern in CELLS.items():
            if re.fullmatch(pattern, cell):
                results[k] += int(count)
    return results

def parse_ltp(ltp):
    """Cells of the longest combinational path (Yosys ltp)."""
    m = re.search(r"Longest topological path in \S+ \(length=(\d+)\)", ltp)
    return int(m.group(1)) if m else None

def write_verilog(top, path):
    platform = XilinxPlatform(DEVICE, io=[])
    platform.get_verilog(top, ios=top.ios, name="top").write(path)

def synthesize(top, families=("xcup", "xcu", None)):
    with tempfile.TemporaryDirectory() as tmp:
        v = os.path.join(tmp, "top.v")
        write_verilog(top, v)
        error = None
        for family in families:
            # Older Yosys do not know every family: next one.
            option = f"-family {family} " if family else ""
            script = (f"read_verilog {v}; synth_xilinx {option}-top top -flatten; "
                f"tee -o {tmp}/stat.txt stat; tee -o {tmp}/ltp.txt ltp -noff")
            run = subprocess.run(["yosys", "-q", "-p", script], capture_output=True, text=True)
            if run.returncode == 0:
                results = parse_stat(open(os.path.join(tmp, "stat.txt")).read())
                results["PATH"]   = parse_ltp(open(os.path.join(tmp, "ltp.txt")).read())
                results["FAMILY"] = family or "default"
                return results
            error = run.stderr[-2000:]
        raise RuntimeError(f"Yosys failed: {error}")

# Main ---------------------------------------------------------------------------------------------

COLUMNS = ["LUT", "FF", "LUTRAM", "BRAM", "CARRY", "MUXF", "PATH"]

def main():
    parser = argparse.ArgumentParser(description="Fabric node resource usage estimation (Yosys).")
    parser.add_argument("--ports",       default="9",       help="Ports of the node (7 inside the chassis + inter-chassis), comma separated.")
    parser.add_argument("--nodes",       default=128,       type=int, help="Nodes of the fabric (chassis of 8).")
    parser.add_argument("--link-width",  default="128",     help="Bits per cycle of the links (32, 64, 128), comma separated.")
    parser.add_argument("--requests",    default=8,         type=int, help="Request slots of the Requester.")
    parser.add_argument("--queue-depth", default=4,         type=int, help="Packets per receive queue.")
    parser.add_argument("--copy-lanes",  default=2,         type=int, help="Copies at a time.")
    parser.add_argument("--wishbone",    action="store_true", help="Wishbone memory instead of the memory port.")
    parser.add_argument("--verilog",     default=None,      help="Only write the Verilog of the first configuration to this file.")
    parser.add_argument("--json",        default=None,      help="Save results to JSON file.")
    parser.add_argument("--markdown",    default=None,      help="Append the results as a Markdown table to this file.")
    args = parser.parse_args()

    results = {}
    rows    = []
    print(f"{'Configuration':34s}" + "".join(f"{k:>10s}" for k in COLUMNS) + f"{'LUT %':>8s}{'FF %':>8s}{'Time':>8s}")
    for ports in [int(p) for p in args.ports.split(",")]:
        for link_width in [int(w) for w in args.link_width.split(",")]:
            name = f"{args.nodes} nodes, {ports} ports, {link_width}-bit links"
            top  = FabricNodeTop(ports, nodes=args.nodes, link_width=link_width, pipelined=not args.wishbone,
                requests=args.requests, queue_depth=args.queue_depth, copy_lanes=args.copy_lanes)
            if args.verilog:
                write_verilog(top, args.verilog)
                return
            start = time.time()
            r = results[name] = synthesize(top)
            r["LUT%"]  = round(100*(r["LUT"] + r["LUTRAM"] + r["SRL"])/DEVICE_LUT, 1)
            r["FF%"]   = round(100*r["FF"]/DEVICE_FF, 1)
            r["TIME"]  = round(time.time() - start)
            print(f"{name:34s}" + "".join(f"{str(r[k]):>10s}" for k in COLUMNS) + f"{r['LUT%']:>8}{r['FF%']:>8}{r['TIME']:>7}s", flush=True)
            rows.append([name] + [r[k] for k in COLUMNS] + [r["LUT%"], r["FF%"], r["FAMILY"]])

    if args.json:
        json.dump(results, open(args.json, "w"), indent=2)
    if args.markdown:
        os.makedirs(os.path.dirname(args.markdown) or ".", exist_ok=True)
        new = not os.path.exists(args.markdown)
        with open(args.markdown, "a") as f:
            if new:
                header = ["Node"] + ["LUT", "FF", "LUT RAM", "Block RAM", "Carry", "MUXF", "Longest path (cells)",
                    "LUT % of VU13P", "FF % of VU13P", "Yosys family"]
                f.write("| " + " | ".join(header) + " |\n|" + "---|"*len(header) + "\n")
            for row in rows:
                f.write("| " + " | ".join(str(c) for c in row) + " |\n")

if __name__ == "__main__":
    main()
