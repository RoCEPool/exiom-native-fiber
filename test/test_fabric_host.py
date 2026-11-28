#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Host channel (exiom/host.py): the host drives the RoCE target and moves pages only through the
registers and the DMA of the card (``CardDriver``), the RoCE target and the DMA sharing the pool
of the node. The memory tier scenarios run again this way.
"""

import unittest

from migen import *

from litex.gen import *

from exiom.fabric   import *
from exiom.topology import *
from exiom.pool     import *
from exiom.roce     import *
from exiom.host     import *
from exiom.tier     import *

from test.test_fabric_network import DUT, parallel
from test.test_fabric_roce    import (Message, Requester, exchange, pattern, MAC, IP, VA, RKEY, PAGE,
    PMTU, WINDOW_BITS, HOST_QPN, HOST_MAC, HOST_IP, SPORT, QPN)
from test.test_fabric_tier    import FRAMES, PSN, end_psn, with_host, run_messages
from test.fabric_sim          import run_simulation

BUFFER = 0x100 # Bounce buffer of the DMA, in host words.

class HostDUT(DUT):
    def __init__(self, latency=8):
        DUT.__init__(self, two_tier(2), size=4*2**WINDOW_BITS)
        self.pool    = FabricPool(self.nodes[0], window_bits=WINDOW_BITS, chunk=16, busy_wait=16)
        self.arbiter = PoolArbiter(self.pool, ports=2)
        self.roce    = FabricRoCE(self.arbiter.ports[0], mac=MAC, ip=IP, qpn_base=QPN, qps=2, mrs=4,
            pt_entries=16, page_bits=8, pmtu=PMTU, cnp_interval=2000)
        self.card    = FabricHost(self.roce, self.arbiter.ports[1])
        self.hostmem = HostMemory(1024, latency=latency)
        self.comb += [
            self.card.host.cmd.connect(self.hostmem.cmd),
            self.hostmem.rsp.connect(self.card.host.rsp),
        ]

class SimCPU:
    """The CPU of the host: its memory, directly."""
    def __init__(self, mem):
        self.mem = mem
    def read(self, word):
        return (yield self.mem[word])
    def write(self, word, value):
        yield self.mem[word].eq(value)
        yield

def card_driver(dut):
    return CardDriver(dut.card.bus, SimCPU(dut.hostmem.mem), BUFFER)

def card_server(dut, journal, nvme):
    return MemoryServer(card_driver(dut), FRAMES, PAGE, 16, journal, nvme, qpn_base=QPN)

class TestFabricHost(unittest.TestCase):
    def test_registers_and_dma(self):
        # Identification, DMA both ways, DMA to a node that does not exist (error, words before it
        # done).
        dut = HostDUT()
        def generator():
            driver = card_driver(dut)
            self.assertEqual((yield from dut.card.bus.read(0)), HOST_ID)
            data = pattern(PAGE, salt=7)
            yield from driver.write_frame(FRAMES[2], data)
            for i in range(PAGE//4):
                self.assertEqual((yield dut.mems[1].mem[2*PAGE//4 + i]), int.from_bytes(data[4*i:4*i + 4], "little"))
            yield dut.mems[1].mem[3*PAGE//4 + 5].eq(0x12345678)
            self.assertEqual((yield from driver.read_frame(FRAMES[3], PAGE))[20:24], (0x12345678).to_bytes(4, "little"))
            # 4 words, the last two past the window of node 1 (node 2 does not exist).
            end = pool_address(1, 2**WINDOW_BITS - 2, WINDOW_BITS)
            status, done = yield from driver.dma(BUFFER, end, 4, DMA_TO_HOST)
            self.assertEqual((status, done), (FABRIC_NODE, 2))
            # Counters through the registers.
            self.assertEqual((yield from driver.counter("rx_packets")), 0)
        run_simulation(dut, generator())

    def test_misses_through_the_card(self):
        # 8 pages on 4 frames, every page moved by the DMA while the RoCE target runs.
        dut = HostDUT()
        nvme, journal = NVMeStore(PAGE), Journal()
        server = card_server(dut, journal, nvme)
        data = pattern(8*PAGE, salt=11)
        messages = [Message("write", VA, data), Message("read", VA, length=8*PAGE)]
        def generator():
            yield from server.region(0, RKEY, VA, 8*PAGE, access=3)
            yield from server.connect(0, HOST_QPN, HOST_MAC, HOST_IP, SPORT, PSN)
            yield from run_messages(self, dut, server, messages, PSN, timeout=6000)
            for m in messages:
                self.assertEqual(m.result, ROCE_SYN_ACK)
            self.assertEqual(bytes(messages[1].got), data)
            self.assertGreaterEqual(server.evictions, 4)
            self.assertGreaterEqual(server.writebacks, 4)
            self.assertEqual((yield from server.driver.counter("writes")), 1)
            yield from server.flush()
            for entry in range(8):
                page = nvme.pages.get(entry)
                if entry in server.resident:
                    page = yield from server.driver.read_frame(server.resident[entry], PAGE)
                self.assertEqual(page, data[entry*PAGE:(entry + 1)*PAGE], entry)
        run_simulation(dut, generator())

    def test_power_loss_through_the_card(self):
        nvme, journal = NVMeStore(PAGE), Journal()
        p1, p2 = pattern(4*PAGE, salt=1), pattern(PAGE, salt=2)
        psn = [PSN]
        dut = HostDUT()
        def before():
            server = card_server(dut, journal, nvme)
            yield from server.region(0, RKEY, VA, 4*PAGE, access=3)
            yield from server.connect(0, HOST_QPN, HOST_MAC, HOST_IP, SPORT, psn[0])
            for offset, d, flush in [(0, p1, True), (PAGE, p2, False)]:
                requester = yield from run_messages(self, dut, server, [Message("write", VA + offset, d)],
                    psn[0], timeout=6000)
                psn[0] = end_psn(requester)
                if flush:
                    yield from server.flush()
        run_simulation(dut, before())
        dut = HostDUT()
        def after():
            server = yield from MemoryServer.recover(card_driver(dut), FRAMES, PAGE, 16, journal, nvme,
                qpn_base=QPN)
            yield from server.connect(0, HOST_QPN, HOST_MAC, HOST_IP, SPORT, psn[0])
            messages = [Message("read", VA, length=4*PAGE)]
            yield from run_messages(self, dut, server, messages, psn[0], timeout=6000)
            self.assertEqual(bytes(messages[0].got), p1) # p2 was not flushed.
        run_simulation(dut, after())

if __name__ == "__main__":
    unittest.main()
