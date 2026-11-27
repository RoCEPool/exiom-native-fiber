#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Block cache (exiom/block_cache.py): remote memory -> on-board slot -> accelerator stream.

- A FETCH reads a block of a remote node's window into a slot (reads in flight, results in any
  order), a PUSH sends the slot to the accelerator word for word, with addresses and ``last``.
- Reads in flight: the same FETCH with 8 reads in flight against 1.
- Errors: a block larger than a slot (nothing moved), reads outside the remote window, an absent
  node: the command completes with the first error.
- Two links away with packets lost on the links: the block arrives whole (retransmission).
- Backpressure from the accelerator: no word lost or repeated.
"""

import random
import unittest

from migen import *

from litex.gen import *

from exiom.fabric      import *
from exiom.topology    import *
from exiom.network     import *
from exiom.block_cache import *

from test.test_fabric import Memory

# After the other imports (run_simulation is also exported by litex.gen).
from test.fabric_sim import run_simulation

WINDOW = 1024 # Bytes of each node's window (256 words).

# DUT ----------------------------------------------------------------------------------------------

class DUT(LiteXModule):
    def __init__(self, topology, slots=4, slot_words=64, inflight=8, **kwargs):
        self.net   = FabricNetwork(topology, size=WINDOW, fc_period=64, **kwargs)
        self.nodes = self.net.nodes
        self.mems  = []
        for n, node in enumerate(self.nodes):
            mem = Memory(node.responder.mem)
            self.add_module(f"mem{n}", mem)
            self.mems.append(mem)
        self.cache = FabricBlockCache(self.nodes[0], slots=slots, slot_words=slot_words, inflight=inflight)

def word(node, i):
    return (0xb1000000 | (node << 16) | i) & 0xffffffff

def fill(dut):
    for n, mem in enumerate(dut.mems):
        for i in range(WINDOW//4):
            yield mem.mem[i].eq(word(n, i))

def command(dut, op, slot, words, dst=0, adr=0, timeout=50000):
    """Sends a command and waits for its completion: (status, words, cycles)."""
    cmd, done = dut.cache.cmd, dut.cache.done
    yield cmd.op.eq(op)
    yield cmd.slot.eq(slot)
    yield cmd.dst.eq(dst)
    yield cmd.adr.eq(adr)
    yield cmd.words.eq(words)
    yield cmd.valid.eq(1)
    yield
    cycles = 1
    while not (yield cmd.ready):
        yield
        cycles += 1
    yield cmd.valid.eq(0)
    yield done.ready.eq(1)
    for _ in range(timeout):
        yield
        cycles += 1
        if (yield done.valid):
            r = ((yield done.status), (yield done.words), cycles)
            yield
            yield done.ready.eq(0)
            return r
    raise TimeoutError("No completion.")

class Accelerator:
    """Takes the words of ``out`` (randomly not ready when ``stall``), keeps (adr, data, last)."""
    def __init__(self, dut, stall=0.0, seed=1):
        self.dut   = dut
        self.stall = stall
        self.rnd   = random.Random(seed)
        self.words = []
        self.stop  = False

    def generator(self):
        out = self.dut.cache.out
        while not self.stop:
            # A word moves at the clock edge where valid and ready are both high: both read in
            # the same cycle, before that edge.
            if (yield out.valid) and (yield out.ready):
                self.words.append(((yield out.adr), (yield out.data), (yield out.last)))
            yield out.ready.eq(int(self.rnd.random() >= self.stall))
            yield

def run(dut, main, acc=None):
    gens = [main()]
    if acc is not None:
        def stopper(g=gens[0]):
            yield from g
            acc.stop = True
        gens = [stopper(), acc.generator()]
    run_simulation(dut, gens)

# Tests --------------------------------------------------------------------------------------------

class TestFabricBlockCache(unittest.TestCase):
    def two_nodes(self, **kwargs):
        return DUT(hypercube(2), **kwargs)

    def test_fetch_then_push_moves_the_block_word_for_word(self):
        dut = self.two_nodes()
        acc = Accelerator(dut)
        res = {}
        def main():
            yield from fill(dut)
            res["fetch"] = yield from command(dut, BLOCK_FETCH, slot=2, words=64, dst=1, adr=16)
            res["push"]  = yield from command(dut, BLOCK_PUSH,  slot=2, words=64, adr=0x1000)
            res["fetched"] = (yield dut.cache.fetched)
            res["pushed"]  = (yield dut.cache.pushed)
        run(dut, main, acc)
        self.assertEqual(res["fetch"][:2], (FABRIC_OK, 64))
        self.assertEqual(res["push"][:2],  (FABRIC_OK, 64))
        self.assertEqual(acc.words, [(0x1000 + i, word(1, 16 + i), int(i == 63)) for i in range(64)])
        self.assertEqual((res["fetched"], res["pushed"]), (64, 64))

    def test_reads_in_flight(self):
        cycles = {}
        pending = {}
        for inflight in (1, 8):
            dut = self.two_nodes(inflight=inflight)
            def main(dut=dut, inflight=inflight):
                yield from fill(dut)
                status, words, c = yield from command(dut, BLOCK_FETCH, slot=0, words=64, dst=1)
                self.assertEqual((status, words), (FABRIC_OK, 64))
                cycles[inflight]  = c
                pending[inflight] = (yield dut.cache.max_pending)
            run(dut, main)
        self.assertEqual(pending[1], 1)
        self.assertGreaterEqual(pending[8], 4)
        self.assertLess(cycles[8]*3, cycles[1], cycles)

    def test_errors_complete_the_command_with_the_first_error(self):
        dut = self.two_nodes()
        res = {}
        def main():
            yield from fill(dut)
            res["too_big"]    = yield from command(dut, BLOCK_FETCH, slot=0, words=65, dst=1)
            res["bad_slot"]   = yield from command(dut, BLOCK_PUSH,  slot=4, words=8)
            res["huge"]       = yield from command(dut, BLOCK_FETCH, slot=0, words=64 + 128, dst=1)
            res["fetched0"]   = (yield dut.cache.fetched)
            # Last 8 words of the window and 8 beyond it.
            res["past_end"]   = yield from command(dut, BLOCK_FETCH, slot=1, words=16, dst=1, adr=WINDOW//4 - 8)
            res["fetched1"]   = (yield dut.cache.fetched)
            res["no_node"]    = yield from command(dut, BLOCK_FETCH, slot=1, words=4, dst=7)
            res["errors"]     = (yield dut.cache.errors)
            res["ok_after"]   = yield from command(dut, BLOCK_FETCH, slot=3, words=4, dst=1)
        run(dut, main)
        self.assertEqual(res["too_big"][:2],  (FABRIC_RANGE, 0))
        self.assertEqual(res["bad_slot"][:2], (FABRIC_RANGE, 0), "slot 4 of 4: refused, not slot 0")
        self.assertEqual(res["huge"][:2], (FABRIC_RANGE, 0), "192 words: refused, not 64")
        self.assertEqual(res["fetched0"], 0, "nothing moved")
        self.assertEqual(res["past_end"][0], FABRIC_RANGE)
        self.assertEqual(res["fetched1"], 8, "the 8 words inside the window")
        self.assertEqual(res["no_node"][0], FABRIC_NODE)
        self.assertEqual(res["errors"], 5)
        self.assertEqual(res["ok_after"][:2], (FABRIC_OK, 4))

    def test_two_links_away_with_lost_packets(self):
        dut = DUT(hypercube(4), loss=32, slot_words=32)
        acc = Accelerator(dut)
        res = {}
        def main():
            yield from fill(dut)
            res["fetch"] = yield from command(dut, BLOCK_FETCH, slot=1, words=32, dst=3, adr=100)
            res["push"]  = yield from command(dut, BLOCK_PUSH,  slot=1, words=32, adr=0)
            dropped = 0
            for link in dut.net.lossy:
                dropped += (yield link.dropped)
            res["dropped"] = dropped
            forwarded = 0
            for n in dut.nodes:
                forwarded += (yield n.forwarded)
            res["forwarded"] = forwarded
        run(dut, main, acc)
        self.assertEqual(res["fetch"][:2], (FABRIC_OK, 32))
        self.assertEqual([d for _, d, _ in acc.words], [word(3, 100 + i) for i in range(32)])
        self.assertGreater(res["dropped"], 0, "packets were lost and sent again")
        self.assertGreater(res["forwarded"], 0, "two links: forwarded by a node on the way")

    def test_backpressure_from_the_accelerator(self):
        dut = self.two_nodes()
        acc = Accelerator(dut, stall=0.6, seed=3)
        res = {}
        def main():
            yield from fill(dut)
            yield from command(dut, BLOCK_FETCH, slot=0, words=48, dst=1, adr=5)
            res["push"] = yield from command(dut, BLOCK_PUSH, slot=0, words=48, adr=7)
        run(dut, main, acc)
        self.assertEqual(res["push"][:2], (FABRIC_OK, 48))
        self.assertEqual(acc.words, [(7 + i, word(1, 5 + i), int(i == 47)) for i in range(48)])

if __name__ == "__main__":
    unittest.main()
