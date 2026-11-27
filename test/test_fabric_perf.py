#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric performance scenarios: throughput and queueing per node.

Two chassis of 8 nodes (16 nodes, 8 ports per node, 128-bit links), word-level links, Migen
simulator. The numbers are requests and words per 1000 cycles: they compare designs, they are not
GB/s (no clock frequency, no line rate, no PHY).

- requests: every node keeps 8 requests in flight (8 request sources behind one node, like 8
  GPUs), to the other nodes at random, single words or bursts of 8 words.
- hotspot: the same, all to one node.
- copies: every node asks copies between two other nodes (data from the source node straight to
  the destination node), sources spread or all the same.

Each scenario runs with the two memories of a node: Wishbone (one word every two cycles) and
memory port (FabricPipelinedResponder: an access per cycle).

The report (tables per node and per link) is printed and written to the file named by
EXIOM_PERF_REPORT (default: build/fabric_perf.md), which the nightly workflow publishes.
The scenarios at 16 nodes are nightly tests; a small one (4 nodes) runs on every push.
"""

import os
import random
import unittest

from migen import *

from litex.gen import *
from litex.gen.sim import *

from exiom.fabric   import *
from exiom.topology import *
from exiom.network  import *
from exiom.pool     import *

from test.nightly             import nightly
from test.test_fabric_network import DUT, parallel
from test.test_fabric_copy    import fill
from test.fabric_sim          import run_simulation

BURST  = FABRIC_BURST_WORDS
REPORT = os.environ.get("EXIOM_PERF_REPORT", os.path.join("build", "fabric_perf.md"))

WORDS       = 256 # Words per window: [0, 128) read and written by the requests and copied from,
                  # [128, 256) copied to.
WINDOW_BITS = 8

# Memory of the nodes.
MODES = {
    False : "Wishbone memory",
    True  : "memory port",
}

# Report -------------------------------------------------------------------------------------------

_report = []

def report(text=""):
    print(text)
    _report.append(text)

def table(header, rows):
    report("| " + " | ".join(header) + " |")
    report("|" + "---|"*len(header))
    for row in rows:
        report("| " + " | ".join(str(c) for c in row) + " |")
    report()

def write_report():
    os.makedirs(os.path.dirname(REPORT) or ".", exist_ok=True)
    with open(REPORT, "a") as f:
        f.write("\n".join(_report) + "\n")
    del _report[:]

# Drivers ------------------------------------------------------------------------------------------

class Stats:
    def __init__(self, nodes):
        self.done      = [0]*nodes # Requests completed, by source.
        self.words     = [0]*nodes # Words read or written, by source.
        self.latency   = [0]*nodes # Sum of the cycles between a request accepted and its result.
        self.worst     = [0]*nodes
        self.errors    = []
        self.fifo_sum  = [0]*nodes # Requests waiting in the Responder (sampled).
        self.fifo_max  = [0]*nodes
        self.samples   = 0
        self.cycle     = 0

def requests_driver(dut, stats, src, cycles, in_flight, destination, burst, seed):
    """Keeps ``in_flight`` requests in flight from node ``src`` for ``cycles`` cycles, then waits
    for the results."""
    prng   = random.Random(seed)
    node   = dut.nodes[src]
    sink   = node.requester.sink
    source = node.requester.source
    tags   = {} # tag -> (cycle accepted, words).
    valid  = 0
    count  = 0
    yield source.ready.eq(1)
    for cycle in range(cycles + 4096):
        if (yield source.valid):
            tag = (yield source.tag)
            start, words = tags.pop(tag)
            status = (yield source.status)
            if status != FABRIC_OK:
                stats.errors.append((src, status))
            latency = cycle - start
            stats.done[src]    += 1
            stats.words[src]   += words
            stats.latency[src] += latency
            stats.worst[src]    = max(stats.worst[src], latency)
        if valid and (yield sink.ready):
            tags[(yield node.requester.tag)] = (cycle, count)
            valid = 0
        if cycle >= cycles:
            if not tags and not valid:
                break
            if not valid:
                yield sink.valid.eq(0)
        elif not valid and len(tags) < in_flight:
            # Next request: its own words in the destination (words 8*src to 8*src + 7).
            count = BURST if burst else 1
            yield sink.dst.eq(destination(src, prng))
            yield sink.adr.eq(8*src + (0 if burst else prng.randrange(8)))
            yield sink.we.eq(prng.randrange(2))
            yield sink.sel.eq(0b1111)
            yield sink.dat_w.eq(prng.getrandbits(32))
            yield sink.burst.eq(burst)
            yield sink.count.eq(count)
            yield sink.data.eq(prng.getrandbits(32*BURST))
            yield sink.valid.eq(1)
            valid = 1
        elif not valid:
            yield sink.valid.eq(0)
        yield
    else:
        raise TimeoutError(f"Node {src}: results missing.")
    yield sink.valid.eq(0)

def sampler(dut, stats, running, period=8):
    """Requests waiting in the Responder of every node, every ``period`` cycles."""
    while running[0]:
        for n, node in enumerate(dut.nodes):
            level = (yield node.responder.fifo.level)
            stats.fifo_sum[n] += level
            stats.fifo_max[n]  = max(stats.fifo_max[n], level)
        stats.samples += 1
        for _ in range(period):
            stats.cycle += 1
            yield

def counters(dut):
    """Counters of the nodes and of the ports."""
    r = {"served": [], "forwarded": [], "tx": {}, "retransmits": 0, "copies": [], "copied": []}
    for n, node in enumerate(dut.nodes):
        r["served"].append((yield node.responder.requests))
        r["forwarded"].append((yield node.forwarded))
        r["copies"].append((yield node.copy.copies))
        r["copied"].append((yield node.copy.words))
        for p, port in enumerate(node.ports):
            r["tx"][(n, p)] = (yield port.tx_packets)
            r["retransmits"] += (yield port.retransmits)
    return r

def measure(test, dut, drivers, stats):
    """Runs the drivers with the sampler; returns the counters over the run."""
    running = [True]
    def main():
        yield from parallel(dut, drivers)
        running[0] = False
    def generator():
        before = yield from counters(dut)
        yield from parallel(dut, [main(), sampler(dut, stats, running)])
        after = yield from counters(dut)
        yield from dut.check_no_drop(test)
        delta = {}
        for key in before:
            if isinstance(before[key], list):
                delta[key] = [a - b for a, b in zip(after[key], before[key])]
            elif isinstance(before[key], dict):
                delta[key] = {k: after[key][k] - before[key][k] for k in before[key]}
            else:
                delta[key] = after[key] - before[key]
        return delta
    return generator()

def per_1000(value, cycles):
    return round(1000*value/cycles)

def links_table(dut, tx, cycles, group_size):
    """Packets per 1000 cycles on the links, inside and between the chassis."""
    intra = [v for (n, p), v in tx.items() if p <  group_size - 1 and (n, p) in dut.topology.peers]
    inter = [v for (n, p), v in tx.items() if p >= group_size - 1 and (n, p) in dut.topology.peers]
    rows  = []
    for name, values in [("inside a chassis", intra), ("between chassis", inter)]:
        if values:
            rows.append([name, len(values), per_1000(sum(values)/len(values), cycles),
                per_1000(max(values), cycles), per_1000(min(values), cycles)])
    table(["Links (per direction)", "Count", "Packets/1000 cycles: average", "Busiest", "Least used"], rows)
    return (max(inter) if inter else 0), (max(intra) if intra else 0)

def requests_scenario(test, name, topology, group_size, cycles, destination, burst, in_flight=8, **kwargs):
    dut   = DUT(topology, link_width=128, size=4*WORDS, **kwargs)
    nodes = len(dut.nodes)
    stats = Stats(nodes)
    out   = {}
    def generator():
        drivers = [requests_driver(dut, stats, src, cycles, in_flight, destination, burst, seed=src + 1)
            for src in range(nodes)]
        out["delta"] = yield from measure(test, dut, drivers, stats)
    run_simulation(dut, generator())
    delta, total = out["delta"], stats.cycle
    test.assertEqual(stats.errors, [])
    report(f"### {name}")
    report()
    report(f"{nodes} nodes, {in_flight} requests in flight per node, {'bursts of %d words' % BURST if burst else 'single words'}, "
        f"{cycles} cycles of requests ({total} cycles with the last results).")
    report()
    table(["Node", "Requests/1000 cycles (source)", "Words/1000 cycles (source)", "Requests/1000 cycles (destination)",
        "Latency: average", "Latency: worst", "Responder queue: average", "Responder queue: most", "Packets forwarded"],
        [[n, per_1000(stats.done[n], total), per_1000(stats.words[n], total), per_1000(delta["served"][n], total),
            round(stats.latency[n]/max(1, stats.done[n]), 1), stats.worst[n],
            round(stats.fifo_sum[n]/max(1, stats.samples), 2), stats.fifo_max[n], delta["forwarded"][n]]
            for n in range(nodes)])
    busiest_inter, busiest_intra = links_table(dut, delta["tx"], total, group_size)
    result = {
        "requests"    : per_1000(sum(stats.done), total),
        "words"       : per_1000(sum(stats.words), total),
        "latency"     : round(sum(stats.latency)/max(1, sum(stats.done)), 1),
        "served_max"  : per_1000(max(delta["served"]), total),
        "queue_max"   : max(stats.fifo_max),
        "retransmits" : delta["retransmits"],
        "inter"       : per_1000(busiest_inter, total),
        "intra"       : per_1000(busiest_intra, total),
    }
    report(f"Fabric: {result['requests']} requests and {result['words']} words per 1000 cycles, average latency "
        f"{result['latency']} cycles, {result['retransmits']} retransmissions.")
    report()
    write_report()
    return result

def copies_scenario(test, name, topology, group_size, copies, words, pattern, chunk=64, **kwargs):
    """Every node asks ``copies`` copies of ``words`` words, one after the other: ``pattern(node,
    i)`` gives (source node, destination node)."""
    dut   = DUT(topology, link_width=128, size=4*WORDS, **kwargs)
    nodes = len(dut.nodes)
    pools = []
    for n in range(nodes):
        pool = FabricPool(dut.nodes[n], window_bits=WINDOW_BITS, chunk=chunk, busy_wait=16)
        dut.add_module(f"pool{n}", pool)
        pools.append(pool)
    stats  = Stats(nodes)
    out    = {}
    copied = [0]*nodes
    spent  = [0]*nodes
    def driver(n):
        pool = pools[n]
        yield pool.source.ready.eq(1)
        for i in range(copies):
            src, dst = pattern(n, i)
            # Each node copies to its own words of the destination.
            yield pool.sink.op.eq(POOL_COPY)
            yield pool.sink.adr.eq(pool_address(src, 0, WINDOW_BITS))
            yield pool.sink.dst_adr.eq(pool_address(dst, 128 + (8*n) % (128 - words + 1), WINDOW_BITS))
            yield pool.sink.words.eq(words)
            yield pool.sink.valid.eq(1)
            yield
            cycles = 1
            while not (yield pool.source.valid):
                yield
                cycles += 1
                if cycles > 400000:
                    raise TimeoutError(f"Node {n}: copy not ended.")
            if (yield pool.source.status) != FABRIC_OK:
                stats.errors.append((n, (yield pool.source.status)))
            copied[n] += (yield pool.source.words)
            spent[n]  += cycles
            yield pool.sink.valid.eq(0)
            yield
    def generator():
        for n in range(nodes):
            yield from fill(dut, n, 0, 128)
        out["delta"]   = yield from measure(test, dut, [driver(n) for n in range(nodes)], stats)
        out["retries"] = []
        for pool in pools:
            out["retries"].append((yield pool.retries))
    run_simulation(dut, generator())
    delta, total = out["delta"], stats.cycle
    test.assertEqual(stats.errors, [])
    test.assertEqual(sum(copied), nodes*copies*words)
    report(f"### {name}")
    report()
    report(f"{nodes} nodes, each asks {copies} copies of {words} words ({total} cycles).")
    report()
    table(["Node", "Words/1000 cycles (asked by)", "Cycles per word (asked by)", "Busy retries (asked by)",
        "Copies run (source)", "Words/1000 cycles (source)", "Requests/1000 cycles (Responder)", "Responder queue: most",
        "Packets forwarded"],
        [[n, per_1000(copied[n], total), round(spent[n]/max(1, copied[n]), 2), out["retries"][n],
            delta["copies"][n], per_1000(delta["copied"][n], total), per_1000(delta["served"][n], total),
            stats.fifo_max[n], delta["forwarded"][n]] for n in range(nodes)])
    busiest_inter, busiest_intra = links_table(dut, delta["tx"], total, group_size)
    result = {
        "words"       : per_1000(sum(copied), total),
        "retries"     : sum(out["retries"]),
        "per_word"    : round(sum(spent)/sum(copied), 2),
        "source_max"  : per_1000(max(delta["copied"]), total),
        "retransmits" : delta["retransmits"],
        "inter"       : per_1000(busiest_inter, total),
    }
    report(f"Fabric: {result['words']} words copied per 1000 cycles, {result['per_word']} cycles per word seen by "
        f"the nodes that ask, {result['retries']} busy retries.")
    report()
    write_report()
    return result

# Destinations and patterns ------------------------------------------------------------------------

def uniform(nodes):
    return lambda src, prng: (src + 1 + prng.randrange(nodes - 1)) % nodes

def same_chassis(group_size):
    return lambda src, prng: src - src % group_size + (src % group_size + 1 + prng.randrange(group_size - 1)) % group_size

def hotspot(target):
    return lambda src, prng: target if src != target else (target + 1)

# Test Fabric Perf ---------------------------------------------------------------------------------

class TestFabricPerf(unittest.TestCase):
    def tearDown(self):
        write_report()

    def test_small(self):
        # 4 nodes: the scenarios and their report on every push.
        t = two_tier(4)
        for pipelined, floors in [(False, (500, 800, 300)), (True, (700, 1500, 500))]:
            mode = MODES[pipelined]
            r = requests_scenario(self, f"4 nodes, requests to all, {mode}", t, 4, 300, uniform(4), burst=0, pipelined=pipelined)
            self.assertGreater(r["requests"], floors[0])
            r = requests_scenario(self, f"4 nodes, bursts to all, {mode}", t, 4, 300, uniform(4), burst=1, pipelined=pipelined)
            self.assertGreater(r["words"], floors[1])
            r = copies_scenario(self, f"4 nodes, copies, {mode}", t, 4, 2, 32, lambda n, i: ((n + 1) % 4, (n + 2) % 4),
                pipelined=pipelined)
            self.assertGreater(r["words"], floors[2])

    chassis = dict(group_size=8, design_groups=2, inter_ports=1)

    # Floors: about 25 % under the rates measured (requests or words per 1000 cycles).
    requests_floors = {
        False : [4100, 4580, 4580, 490],
        True  : [4330, 10200, 9900, 1090],
    }
    copies_floors = {
        False : [2700, 2660, 315],
        True  : [4900, 4800, 550],
    }

    def requests_16_nodes(self, pipelined):
        # Two chassis of 8 nodes, 8 requests in flight per node.
        report(f"## 16 nodes (2 chassis of 8), 8 requests in flight per node, {MODES[pipelined]}")
        report()
        t = two_tier(16, **self.chassis)
        results = {}
        for name, destination, burst in [
            ("Single words, to all the nodes",        uniform(16),     0),
            ("Bursts of 8 words, to all the nodes",   uniform(16),     1),
            ("Bursts of 8 words, inside the chassis", same_chassis(8), 1),
            ("Bursts of 8 words, all to node 0",      hotspot(0),      1),
        ]:
            results[name] = requests_scenario(self, name, t, 8, 600, destination, burst, pipelined=pipelined)
        table(["Scenario", "Requests/1000 cycles", "Words/1000 cycles", "Average latency", "Busiest Responder (requests/1000 cycles)",
            "Responder queue: most", "Busiest cable between chassis (packets/1000 cycles)", "Busiest link inside a chassis"],
            [[name, r["requests"], r["words"], r["latency"], r["served_max"], r["queue_max"], r["inter"], r["intra"]]
                for name, r in results.items()])
        write_report()
        r, floors = list(results.values()), self.requests_floors[pipelined]
        self.assertGreater(r[0]["requests"], floors[0])
        self.assertGreater(r[1]["words"],    floors[1])
        self.assertGreater(r[2]["words"],    floors[2])
        self.assertGreater(r[3]["words"],    floors[3])

    def copies_16_nodes(self, pipelined):
        # Two chassis of 8 nodes: copies between other nodes.
        report(f"## 16 nodes (2 chassis of 8), copies, {MODES[pipelined]}")
        report()
        t = two_tier(16, **self.chassis)
        results = {}
        for name, pattern in [
            # Sources all different.
            ("Next node to the one after, inside the chassis", lambda n, i: (n - n % 8 + (n + 1) % 8, n - n % 8 + (n + 2) % 8)),
            ("Next node to the same node of the other chassis", lambda n, i: (n - n % 8 + (n + 1) % 8, (n + 8) % 16)),
            # One source for all.
            ("All from node 0", lambda n, i: (0, n if n else 1)),
        ]:
            results[name] = copies_scenario(self, name, t, 8, 2, 64, pattern, pipelined=pipelined)
        table(["Scenario", "Words copied/1000 cycles", "Cycles per word (asked by)", "Busy retries",
            "Busiest source (words/1000 cycles)", "Busiest cable between chassis (packets/1000 cycles)"],
            [[name, r["words"], r["per_word"], r["retries"], r["source_max"], r["inter"]] for name, r in results.items()])
        write_report()
        r, floors = list(results.values()), self.copies_floors[pipelined]
        self.assertGreater(r[0]["words"], floors[0])
        self.assertGreater(r[1]["words"], floors[1])
        self.assertGreater(r[2]["words"], floors[2])
        self.assertEqual(r[0]["retries"] + r[1]["retries"], 0) # Sources all different: nobody busy.

    @nightly
    def test_16_nodes_requests(self):
        self.requests_16_nodes(pipelined=False)

    @nightly
    def test_16_nodes_requests_pipelined(self):
        self.requests_16_nodes(pipelined=True)

    @nightly
    def test_16_nodes_copies(self):
        self.copies_16_nodes(pipelined=False)

    @nightly
    def test_16_nodes_copies_pipelined(self):
        self.copies_16_nodes(pipelined=True)

if __name__ == "__main__":
    unittest.main()
