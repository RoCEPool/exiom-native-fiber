#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Memory tiers (exiom/tier.py): the host software of a memory server with the RoCE target.

A region larger than the frames of the fabric: the pages come from NVMe (model) on a miss, other
pages are evicted (and written back when dirty), the requester sends again the packets dropped on
a miss. Then a power loss: a fresh target, the regions back from the journal, the data back from
NVMe up to the last flush of each page.
"""

import unittest

from migen import *

from litex.gen import *

from exiom.pool import *
from exiom.roce import *
from exiom.tier import *

from test.test_fabric_network import parallel
from test.test_fabric_roce    import (RoCEDUT, Message, Requester, exchange, pattern, VA, RKEY, PAGE,
    PMTU, WINDOW_BITS, HOST_QPN, HOST_MAC, HOST_IP, SPORT, QPN)
from test.fabric_sim          import run_simulation

FRAMES = [pool_address(1, p*PAGE//4, WINDOW_BITS) for p in range(4)] # The memory of node 1.
PSN    = 0x200000

def server_for(dut, journal, nvme):
    driver = SimDriver(dut.roce, {n: mem.mem for n, mem in enumerate(dut.mems)}, WINDOW_BITS)
    return MemoryServer(driver, FRAMES, PAGE, 16, journal, nvme, qpn_base=QPN)

def end_psn(requester):
    """PSN after the last packet of a requester."""
    _, psn, (op, _, _), n = requester.packets[-1]
    return (psn + (n if op == ROCE_READ_REQUEST else 1)) & 0xffffff

def with_host(server, generator):
    """Runs ``generator`` (the requester) with the host serving the misses."""
    state = dict(done=False)
    def requester():
        yield from generator
        state["done"] = True
    def host():
        while not state["done"]:
            if not (yield from server.service()):
                for _ in range(8):
                    yield
    yield from parallel(None, [requester(), host()])

def run_messages(test, dut, server, messages, psn, **kwargs):
    requester = Requester(messages, psn=psn, **kwargs)
    yield from with_host(server, exchange(test, dut, requester, max_cycles=800000))
    return requester

class TestFabricTier(unittest.TestCase):
    def test_misses(self):
        # 8 pages on 4 frames: misses, evictions, write backs; the data is right.
        dut = RoCEDUT()
        nvme, journal = NVMeStore(PAGE), Journal()
        server = server_for(dut, journal, nvme)
        data   = pattern(8*PAGE, salt=9)
        small  = pattern(64, salt=4)
        messages = [
            Message("write", VA, data),
            Message("read",  VA, length=8*PAGE),
            # A READ dropped on a miss runs again when it comes again, maybe after the next
            # messages: a write to the bytes it reads waits for it (fence), as with any RC READ.
            Message("write", VA + 5*PAGE + 32, small, fence=True),
            Message("read",  VA + 4*PAGE, length=2*PAGE),
            Message("read",  VA, length=PAGE),
        ]
        expected = bytearray(data)
        expected[5*PAGE + 32:5*PAGE + 96] = small
        def generator():
            yield from server.region(0, RKEY, VA, 8*PAGE, access=3)
            qpn = yield from server.connect(0, HOST_QPN, HOST_MAC, HOST_IP, SPORT, PSN)
            self.assertEqual(qpn, QPN)
            server.pin(0) # Page 0: never evicted once brought in.
            requester = yield from run_messages(self, dut, server, messages, PSN, timeout=3000)
            for m in messages:
                self.assertEqual(m.result, ROCE_SYN_ACK)
            self.assertEqual(bytes(messages[1].got), data)
            self.assertEqual(bytes(messages[3].got), bytes(expected[4*PAGE:6*PAGE]))
            self.assertEqual(bytes(messages[4].got), bytes(expected[:PAGE]))
            self.assertGreaterEqual((yield dut.roce.misses), 8)
            self.assertGreaterEqual(server.loads, 8)
            self.assertGreaterEqual(server.evictions, 4)
            self.assertGreaterEqual(server.writebacks, 4)
            self.assertIn(0, server.resident)
            # Everything to NVMe: the pages there are the region.
            yield from server.flush()
            for entry in range(8):
                page = nvme.pages.get(entry, bytes(PAGE))
                if entry in server.resident:
                    page = yield from server.driver.read_frame(server.resident[entry], PAGE)
                self.assertEqual(page, bytes(expected[entry*PAGE:(entry + 1)*PAGE]), entry)
        run_simulation(dut, generator())

    def test_power_loss(self):
        # Writes, flushes, a write not flushed, power loss: after recovery the flushed data is
        # read back (from NVMe, on misses), the write not flushed is lost.
        nvme, journal = NVMeStore(PAGE), Journal()
        p1, p2, p3 = pattern(4*PAGE, salt=1), pattern(2*PAGE, salt=2), pattern(PAGE, salt=3)
        psn = [PSN]
        dut = RoCEDUT()
        def before():
            server = server_for(dut, journal, nvme)
            yield from server.region(0, RKEY, VA, 4*PAGE, access=3)
            yield from server.connect(0, HOST_QPN, HOST_MAC, HOST_IP, SPORT, psn[0])
            for writes, flush in [([(0, p1)], True), ([(0, p2)], True), ([(2*PAGE, p3)], False)]:
                messages = [Message("write", VA + offset, d) for offset, d in writes]
                requester = yield from run_messages(self, dut, server, messages, psn[0], timeout=3000)
                psn[0] = end_psn(requester)
                for m in messages:
                    self.assertEqual(m.result, ROCE_SYN_ACK)
                if flush:
                    yield from server.flush()
            self.assertEqual(server.evictions, 0)
            self.assertEqual(nvme.pages[2], p1[2*PAGE:3*PAGE]) # The write of p3 is in memory only.
        run_simulation(dut, before())

        # Power loss: fresh target and memories; the journal and NVMe are what is left.
        dut = RoCEDUT()
        expected = bytearray(p1)
        expected[:2*PAGE] = p2
        def after():
            server = yield from MemoryServer.recover(server_for(dut, journal, nvme).driver, FRAMES,
                PAGE, 16, journal, nvme, qpn_base=QPN)
            yield from server.connect(0, HOST_QPN, HOST_MAC, HOST_IP, SPORT, psn[0])
            messages = [Message("read", VA, length=4*PAGE)]
            yield from run_messages(self, dut, server, messages, psn[0], timeout=3000)
            self.assertEqual(messages[0].result, ROCE_SYN_ACK)
            self.assertEqual(bytes(messages[0].got), bytes(expected))
            self.assertEqual(server.loads, 4)
        run_simulation(dut, after())

if __name__ == "__main__":
    unittest.main()
