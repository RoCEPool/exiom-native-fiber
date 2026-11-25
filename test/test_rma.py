#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Remote Memory Access layer (exiom/rma.py) over a SerWB link, word-level PHY model:

bus -> RMAInitiator -> SERWBCore("slave") -> PHY <-> PHY -> SERWBCore("master") -> RMATarget -> SRAM
"""

import unittest

from migen import *

from litex.gen import *
from litex.gen.sim import *

from litex.soc.interconnect import wishbone

from liteiclink.serwb.core   import SERWBCore
from liteiclink.serwb.packet import PREAMBLE

from exiom.rma import *

from test.test_serwb_link import LoopbackPHY, SlowAck, bus_access, sel_mask

# Faulty Wishbone Slave ----------------------------------------------------------------------------

class Faulty(LiteXModule):
    """Forwards accesses to `slave`, except at word `hang_adr` (never answered) and `err_adr`
    (Wishbone error)."""
    def __init__(self, master, slave, hang_adr, err_adr):
        hang = Signal()
        err  = Signal()
        self.comb += [
            hang.eq(master.adr == hang_adr) if hang_adr is not None else hang.eq(0),
            err.eq(master.adr == err_adr)   if err_adr  is not None else err.eq(0),
            master.connect(slave, omit={"stb", "cyc", "ack", "err"}),
            slave.cyc.eq(master.cyc & ~hang & ~err),
            slave.stb.eq(master.stb & ~hang & ~err),
            master.ack.eq(slave.ack),
            master.err.eq(master.cyc & master.stb & err),
        ]

# DUT ----------------------------------------------------------------------------------------------

class DUT(LiteXModule):
    def __init__(self, size=128, target_size=None, origin=0, read_only=False, sram_latency=0,
        hang_adr=None, err_adr=None, target_timeout=128, with_initiator=True):
        target_size = size if target_size is None else target_size

        # Link state and packets drop (preamble of the next packet replaced by an idle word).
        self.link_up     = Signal(reset=1)
        self.drop_m2s    = Signal() # Drop next Initiator -> Target packet.
        self.dropped_m2s = Signal()

        # PHYs.
        self.master_phy = LoopbackPHY()
        self.slave_phy  = LoopbackPHY()
        m2s_preamble = Signal()
        self.comb += [
            m2s_preamble.eq(self.master_phy.tx.valid & (self.master_phy.tx.data == PREAMBLE)),
            self.master_phy.tx.connect(self.slave_phy.rx),
            If(self.drop_m2s & m2s_preamble, self.slave_phy.rx.data.eq(0)),
            self.slave_phy.tx.connect(self.master_phy.rx),
            self.master_phy.init.ready.eq(self.link_up),
            self.slave_phy.init.ready.eq(self.link_up),
        ]
        self.sync += If(self.drop_m2s & m2s_preamble, self.dropped_m2s.eq(1))

        # Cores.
        self.master_core = SERWBCore(self.master_phy, clk_freq=int(1e6), mode="slave")
        self.slave_core  = SERWBCore(self.slave_phy,  clk_freq=int(1e6), mode="master")

        # RMA.
        self.target = ResetInserter()(RMATarget(size=target_size, read_only=read_only, timeout=target_timeout))
        self.comb += self.slave_core.bus.connect(self.target.bus)
        if with_initiator:
            self.initiator = RMAInitiator(size=size, origin=origin, link_ready=self.master_phy.init.ready)
            self.comb += self.initiator.link.connect(self.master_core.bus)
            self.bus = self.initiator.bus
        else:
            # Raw SerWB bus to the Target.
            self.bus = self.master_core.bus

        # Memory (larger than the window, to check that nothing is accessed outside of it).
        mem_bus = self.target.mem
        if (hang_adr is not None) or (err_adr is not None):
            _mem_bus    = wishbone.Interface()
            self.faulty = Faulty(mem_bus, _mem_bus, hang_adr=hang_adr, err_adr=err_adr)
            mem_bus     = _mem_bus
        if sram_latency:
            _mem_bus  = wishbone.Interface()
            self.slow = SlowAck(mem_bus, _mem_bus, latency=sram_latency)
            mem_bus   = _mem_bus
        self.sram = wishbone.SRAM(1024, bus=mem_bus, read_only=False)

# Helpers ------------------------------------------------------------------------------------------

def write(dut, adr, dat_w, sel=0b1111):
    _, err = yield from bus_access(dut.bus, adr, we=1, dat_w=dat_w, sel=sel, timeout=20000)
    return err

def read(dut, adr):
    return (yield from bus_access(dut.bus, adr, timeout=20000))

def sram_word(dut, adr):
    return (yield dut.sram.mem[adr])

def drop_next_m2s(dut):
    yield dut.dropped_m2s.eq(0)
    yield dut.drop_m2s.eq(1)
    yield

def error_code(dut):
    return (yield dut.initiator.error_code)

# Test RMA -----------------------------------------------------------------------------------------

class TestRMA(unittest.TestCase):
    def test_read_write(self):
        # Normal accesses: single, consecutive and byte enables.
        dut = DUT()
        def generator():
            ref = {}
            for adr in [0, 5, 31]:
                ref[adr] = 0x1000_0000 + adr
                self.assertEqual((yield from write(dut, adr, ref[adr])), 0)
            for adr in range(8, 16):
                ref[adr] = 0xcafe_0000 + adr
                self.assertEqual((yield from write(dut, adr, ref[adr])), 0)
            for sel in [0b0001, 0b0110, 0b1000]:
                self.assertEqual((yield from write(dut, 5, 0xffff_ffff, sel=sel)), 0)
                ref[5] = (ref[5] & ~sel_mask(sel)) | sel_mask(sel)
            for adr, value in ref.items():
                self.assertEqual((yield from read(dut, adr)), (value, 0))
                self.assertEqual((yield from sram_word(dut, adr)), value)
            self.assertEqual((yield dut.initiator.errors), 0)
            self.assertEqual((yield dut.target.wr_count), 3 + 8 + 3)
        run_simulation(dut, generator())

    def test_origin(self):
        # Window mapped at 0x30000000 on the SoC bus: offsets are sent on the link.
        dut = DUT(origin=0x30000000)
        base = 0x30000000//4
        def generator():
            self.assertEqual((yield from write(dut, base + 3, 0x1234_5678)), 0)
            self.assertEqual((yield from read(dut, base + 3)), (0x1234_5678, 0))
            self.assertEqual((yield from sram_word(dut, 3)), 0x1234_5678)
            # Below/above the window.
            self.assertEqual((yield from write(dut, base - 1, 1)), 1)
            self.assertEqual((yield from write(dut, base + 32, 1)), 1)
            self.assertEqual((yield from read(dut, 3))[1], 1)
        run_simulation(dut, generator())

    def test_out_of_window_local(self):
        # Accesses outside the window end with an error on the Initiator, without link traffic.
        dut = DUT(size=128)
        def generator():
            self.assertEqual((yield from write(dut, 31, 0x1111)), 0)
            self.assertEqual((yield from write(dut, 32, 0x2222)), 1)
            self.assertEqual((yield from error_code(dut)), RMA_ERROR_RANGE)
            self.assertEqual((yield from read(dut, 32))[1], 1)
            self.assertEqual((yield from read(dut, 0x1fffffff))[1], 1)
            self.assertEqual((yield from sram_word(dut, 32)), 0)
            self.assertEqual((yield dut.target.wr_count), 1)
            self.assertEqual((yield dut.initiator.errors), 3)
            # Window still usable.
            self.assertEqual((yield from read(dut, 31)), (0x1111, 0))
        run_simulation(dut, generator())

    def test_out_of_window_remote(self):
        # Initiator configured larger than the Target window: the Target is the authority, refuses
        # the accesses and reports them.
        dut = DUT(size=256, target_size=64)
        def generator():
            self.assertEqual((yield from write(dut, 15, 0x1111)), 0)
            self.assertEqual((yield from write(dut, 16, 0x2222)), 1)
            self.assertEqual((yield from error_code(dut)), RMA_ERROR_RANGE)
            self.assertEqual((yield from sram_word(dut, 16)), 0)
            self.assertEqual((yield from read(dut, 16))[1], 1)
            self.assertEqual((yield from error_code(dut)), RMA_ERROR_READ)
            # Refused write does not break the following ones.
            self.assertEqual((yield from write(dut, 14, 0x3333)), 0)
            self.assertEqual((yield from read(dut, 14)), (0x3333, 0))
            self.assertEqual((yield from read(dut, 15)), (0x1111, 0))
        run_simulation(dut, generator())

    def test_read_only(self):
        dut = DUT(read_only=True)
        def generator():
            yield dut.sram.mem[2].eq(0xabcd_ef01)
            yield
            self.assertEqual((yield from read(dut, 2)), (0xabcd_ef01, 0))
            self.assertEqual((yield from write(dut, 2, 0x5555)), 1)
            self.assertEqual((yield from error_code(dut)), RMA_ERROR_RO)
            self.assertEqual((yield from read(dut, 2)), (0xabcd_ef01, 0))
        run_simulation(dut, generator())

    def test_slow_memory(self):
        # Slow memory (below the Target timeout): accesses succeed.
        dut = DUT(sram_latency=64)
        def generator():
            for adr in range(4):
                self.assertEqual((yield from write(dut, adr, 0xa000 + adr)), 0)
            for adr in range(4):
                self.assertEqual((yield from read(dut, adr)), (0xa000 + adr, 0))
        run_simulation(dut, generator())

    def test_memory_timeout(self):
        # Memory not answering: error from the Target watchdog, link not stalled.
        dut = DUT(hang_adr=7)
        def generator():
            self.assertEqual((yield from write(dut, 6, 0x6666)), 0)
            self.assertEqual((yield from write(dut, 7, 0x7777)), 1)
            self.assertEqual((yield from error_code(dut)), RMA_ERROR_TIMEOUT)
            self.assertEqual((yield from read(dut, 7))[1], 1)
            self.assertEqual((yield from error_code(dut)), RMA_ERROR_READ)
            self.assertEqual((yield from write(dut, 8, 0x8888)), 0)
            self.assertEqual((yield from read(dut, 6)), (0x6666, 0))
            self.assertEqual((yield from read(dut, 8)), (0x8888, 0))
        run_simulation(dut, generator())

    def test_memory_error(self):
        dut = DUT(err_adr=9)
        def generator():
            self.assertEqual((yield from write(dut, 9, 0x9999)), 1)
            self.assertEqual((yield from error_code(dut)), RMA_ERROR_BUS)
            self.assertEqual((yield from read(dut, 9))[1], 1)
            self.assertEqual((yield from write(dut, 10, 0xaaaa)), 0)
            self.assertEqual((yield from read(dut, 10)), (0xaaaa, 0))
        run_simulation(dut, generator())

    def test_lost_write(self):
        # Write request lost on the link: SerWB alone would ack it, RMA reports it.
        dut = DUT()
        def generator():
            self.assertEqual((yield from write(dut, 1, 0x1111)), 0)
            yield from drop_next_m2s(dut)
            err = yield from write(dut, 2, 0x2222)
            self.assertEqual(err, 1)
            self.assertEqual((yield from error_code(dut)), RMA_ERROR_LOST)
            self.assertEqual((yield from sram_word(dut, 2)), 0)
            # Next accesses are fine (write count taken from the Target).
            self.assertEqual((yield from write(dut, 3, 0x3333)), 0)
            self.assertEqual((yield from read(dut, 3)), (0x3333, 0))
            self.assertEqual((yield from read(dut, 1)), (0x1111, 0))
        def stopper():
            # Stop dropping once the write request is gone (the confirmation must go through).
            while not (yield dut.dropped_m2s):
                yield
            yield dut.drop_m2s.eq(0)
        run_simulation(dut, [generator(), stopper()])

    def test_lost_confirmation(self):
        # Write executed but its confirmation lost: error (not confirmed), then recovery.
        dut = DUT()
        def generator():
            self.assertEqual((yield from write(dut, 1, 0x1111)), 0)
            self.assertEqual((yield dut.initiator.synced), 1)
            err = yield from write(dut, 2, 0x2222)
            self.assertEqual(err, 1)
            self.assertEqual((yield from error_code(dut)), RMA_ERROR_LINK)
            self.assertEqual((yield dut.initiator.synced), 0)
            self.assertEqual((yield from sram_word(dut, 2)), 0x2222) # Applied, but not confirmed.
            self.assertEqual((yield from write(dut, 3, 0x3333)), 0)
            self.assertEqual((yield from read(dut, 3)), (0x3333, 0))
        def dropper():
            # Second write: let the write request go (2nd packet), drop the status read (3rd one;
            # 1st write: SYNC read, write, CONFIRM read = packets 0..2, 2nd write: packets 3, 4).
            packets = 0
            last    = 0
            while True:
                preamble = (yield dut.master_phy.tx.valid) and ((yield dut.master_phy.tx.data) == PREAMBLE)
                if preamble and not last:
                    packets += 1
                last = preamble
                if packets == 4:
                    yield dut.drop_m2s.eq(1)
                if (yield dut.dropped_m2s):
                    yield dut.drop_m2s.eq(0)
                    return
                yield
        run_simulation(dut, [generator(), dropper()])

    def test_link_down(self):
        # Link down: errors instead of stalling, then recovery (with a Target reset meanwhile,
        # that must not produce a false error on the first write).
        dut = DUT()
        def generator():
            for adr in range(3):
                self.assertEqual((yield from write(dut, adr, 0x100 + adr)), 0)
            yield dut.link_up.eq(0)
            yield
            self.assertEqual((yield from write(dut, 4, 0x4444)), 1)
            self.assertEqual((yield from error_code(dut)), RMA_ERROR_LINK)
            self.assertEqual((yield from read(dut, 0))[1], 1)
            yield dut.target.reset.eq(1)
            yield
            yield dut.target.reset.eq(0)
            yield dut.link_up.eq(1)
            for _ in range(16):
                yield
            self.assertEqual((yield dut.target.wr_count), 0)
            self.assertEqual((yield from write(dut, 5, 0x5555)), 0)
            self.assertEqual((yield from read(dut, 5)), (0x5555, 0))
            self.assertEqual((yield from read(dut, 2)), (0x102, 0))
            self.assertEqual((yield from sram_word(dut, 4)), 0)
        run_simulation(dut, generator())

    def test_link_down_during_write(self):
        dut = DUT(sram_latency=64)
        def generator():
            self.assertEqual((yield from write(dut, 0, 0x1111)), 0)
            yield dut.bus.adr.eq(1)
            yield dut.bus.we.eq(1)
            yield dut.bus.dat_w.eq(0x2222)
            yield dut.bus.sel.eq(0b1111)
            yield dut.bus.cyc.eq(1)
            yield dut.bus.stb.eq(1)
            for _ in range(20):
                yield
            yield dut.link_up.eq(0)
            for _ in range(2000):
                if (yield dut.bus.err) or (yield dut.bus.ack):
                    break
                yield
            self.assertEqual((yield dut.bus.err), 1)
            yield dut.bus.cyc.eq(0)
            yield dut.bus.stb.eq(0)
            for _ in range(200):
                yield
            yield dut.link_up.eq(1)
            for _ in range(16):
                yield
            self.assertEqual((yield from write(dut, 1, 0x3333)), 0)
            self.assertEqual((yield from read(dut, 1)), (0x3333, 0))
            self.assertEqual((yield from read(dut, 0)), (0x1111, 0))
        run_simulation(dut, generator())

    def test_control_registers(self):
        # Target registers and write accounting, through the raw SerWB bus (no Initiator).
        dut = DUT(size=128, with_initiator=False)
        def status():
            dat_r, err = yield from bus_access(dut.bus, RMA_CTRL_BASE + RMA_REG_STATUS)
            self.assertEqual(err, 0)
            return dat_r >> RMA_STATUS_COUNT, dat_r & 0x1f
        def generator():
            self.assertEqual((yield from bus_access(dut.bus, RMA_CTRL_BASE + RMA_REG_ID)),   (RMA_ID, 0))
            self.assertEqual((yield from bus_access(dut.bus, RMA_CTRL_BASE + RMA_REG_SIZE)), (32, 0))
            self.assertEqual((yield from status()), (0, 0))
            yield from bus_access(dut.bus, 4, we=1, dat_w=0x1234)
            self.assertEqual((yield from status()), (1, 0))
            yield from bus_access(dut.bus, 32, we=1, dat_w=0x1234)
            self.assertEqual((yield from status()), (2, (1 << RMA_STATUS_ERR) | (1 << RMA_STATUS_RANGE)))
            # Registers are read-only and not part of the window: writes ignored, not counted.
            yield from bus_access(dut.bus, RMA_CTRL_BASE + RMA_REG_SIZE, we=1, dat_w=0xffff)
            self.assertEqual((yield from bus_access(dut.bus, RMA_CTRL_BASE + RMA_REG_SIZE)), (32, 0))
            self.assertEqual((yield from status()), (2, (1 << RMA_STATUS_ERR) | (1 << RMA_STATUS_RANGE)))
            yield from bus_access(dut.bus, 5, we=1, dat_w=0x5678)
            self.assertEqual((yield from status()), (3, 0))
        run_simulation(dut, generator())

if __name__ == "__main__":
    unittest.main()
