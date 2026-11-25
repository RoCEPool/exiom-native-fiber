#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Remote Memory Access (RMA) over a SerWB link.

Thin layer on top of the unmodified SerWB native MMAP protocol (liteiclink/serwb/mmap.py):

    SoC bus -> RMAInitiator -> SERWBCore("slave") -> link -> SERWBCore("master") -> RMATarget -> memory

It adds what the SerWB protocol does not provide for remote memory:

- Fixed window: the Target only exposes ``size`` bytes of its memory; accesses outside the window
  return an error and never reach the memory.
- Acknowledged writes: SerWB writes are posted (acked locally, lost silently on a link error and
  remote errors are not reported). The Target counts the writes it executes and records their
  result; the Initiator reads this status back after each write and only acks the access when
  the remote side confirms it. A lost, refused or failed write ends with a bus error.
- Target watchdog: a memory that does not answer within ``timeout`` cycles returns an error
  instead of stalling the link.

Remote address map (word addresses on the SerWB bus):

- 0x00000000 .. size/4 - 1 : Memory window.
- RMA_CTRL_BASE + 0        : ID (RMA_ID, read-only).
- RMA_CTRL_BASE + 1        : STATUS (read-only):
                             [0] last write failed, [1] out of window, [2] read-only window,
                             [3] memory bus error, [4] memory timeout, [31:8] write count.
- RMA_CTRL_BASE + 2        : Window size in words (read-only).

Semantics:

- One access at a time (no pipelining), 32-bit data, byte enables supported.
- Read  : ack with data, or error (out of window, memory error/timeout, link down, no response).
- Write : ack once the Target confirmed it was executed. An error means "not confirmed": it was
  refused (see ``error_code``), failed on the memory, or its confirmation was lost; in this last
  case it may still have been applied.
- After an error, or when the link goes down, the Initiator reads the remote write count again
  before the next write (so a remote reset does not produce false errors).
"""

from migen import *

from litex.gen import *
from litex.gen.genlib.misc import WaitTimer

from litex.soc.interconnect import wishbone

# Constants ----------------------------------------------------------------------------------------

RMA_ID        = 0x524d4131 # "RMA1".
RMA_CTRL_BASE = 1 << 29    # Control registers (word address on the 30-bit SerWB bus).

RMA_REG_ID     = 0
RMA_REG_STATUS = 1
RMA_REG_SIZE   = 2

# STATUS flags.
RMA_STATUS_ERR     = 0
RMA_STATUS_RANGE   = 1
RMA_STATUS_RO      = 2
RMA_STATUS_BUS     = 3
RMA_STATUS_TIMEOUT = 4
RMA_STATUS_COUNT   = 8 # [31:8].
RMA_COUNT_BITS     = 24

# Initiator error codes (``error_code``, cause of the last error).
RMA_ERROR_NONE       = 0
RMA_ERROR_RANGE      = 1 # Out of window (local check or reported by the Target).
RMA_ERROR_RO         = 2 # Write to a read-only window.
RMA_ERROR_BUS        = 3 # Remote memory bus error.
RMA_ERROR_TIMEOUT    = 4 # Remote memory timeout.
RMA_ERROR_LOST       = 5 # Write not executed by the Target (lost on the link).
RMA_ERROR_LINK       = 6 # Link down or no response from the remote side.
RMA_ERROR_READ       = 7 # Read error reported by the remote side or no response.

# RMA Target ---------------------------------------------------------------------------------------

class RMATarget(LiteXModule):
    """Remote side: exposes a fixed memory window to the SerWB Wishbone Master.

    ``bus``: connect to SERWBCore(mode="master").bus. ``mem``: Wishbone Master to the memory
    (word addresses relative to the window).
    """
    def __init__(self, size, read_only=False, timeout=1024):
        assert size > 0 and (size % 4) == 0
        assert (size//4) <= RMA_CTRL_BASE
        self.size = size
        self.bus  = bus = wishbone.Interface(data_width=32, adr_width=30)
        self.mem  = mem = wishbone.Interface(data_width=32, adr_width=30)

        # Status.
        self.wr_count = wr_count = Signal(RMA_COUNT_BITS) # Writes handled (executed or refused).
        self.wr_flags = wr_flags = Signal(5)              # Result of the last write.

        # # #

        words  = size//4
        access = Signal()
        ctrl   = Signal()
        inside = Signal()
        self.comb += [
            access.eq(bus.cyc & bus.stb),
            ctrl.eq(bus.adr[29]),
            inside.eq(~ctrl & (bus.adr < words)),
        ]

        # Control Registers.
        # ------------------
        status = Signal(32)
        self.comb += [
            status[:5].eq(wr_flags),
            status[RMA_STATUS_COUNT:].eq(wr_count),
        ]
        reg = Signal(32)
        self.comb += Case(bus.adr[:2], {
            RMA_REG_ID     : reg.eq(RMA_ID),
            RMA_REG_STATUS : reg.eq(status),
            RMA_REG_SIZE   : reg.eq(words),
            "default"      : reg.eq(0),
        })

        # Response.
        # ---------
        err   = Signal()
        dat_r = Signal(32)

        def write_done(flags):
            # Writes to the window (inside or not) are counted, with their result.
            return If(bus.we,
                NextValue(wr_count, wr_count + 1),
                NextValue(wr_flags, flags),
            )

        def flags(*bits):
            return sum(1 << b for b in bits)

        # Watchdog.
        # ---------
        self.timer = timer = WaitTimer(timeout)

        # FSM.
        # ----
        self.fsm = fsm = FSM(reset_state="IDLE")
        fsm.act("IDLE",
            If(access,
                If(ctrl,
                    # Control registers: read-only, not part of the window (writes not counted).
                    NextValue(dat_r, reg),
                    NextValue(err,   bus.we),
                    NextState("RESPONSE")
                ).Elif(~inside,
                    NextValue(err, 1),
                    write_done(flags(RMA_STATUS_ERR, RMA_STATUS_RANGE)),
                    NextState("RESPONSE")
                ).Elif(bus.we & int(read_only),
                    NextValue(err, 1),
                    write_done(flags(RMA_STATUS_ERR, RMA_STATUS_RO)),
                    NextState("RESPONSE")
                ).Else(
                    NextState("MEMORY")
                )
            )
        )
        self.comb += [
            mem.adr.eq(bus.adr),
            mem.we.eq(bus.we),
            mem.sel.eq(bus.sel),
            mem.dat_w.eq(bus.dat_w),
        ]
        fsm.act("MEMORY",
            mem.cyc.eq(1),
            mem.stb.eq(1),
            timer.wait.eq(1),
            If(~access,
                # Access released by the SerWB side (request cut): not executed, not counted.
                NextState("IDLE")
            ).Elif(mem.ack,
                NextValue(dat_r, mem.dat_r),
                NextValue(err,   0),
                write_done(0),
                NextState("RESPONSE")
            ).Elif(mem.err,
                NextValue(err, 1),
                write_done(flags(RMA_STATUS_ERR, RMA_STATUS_BUS)),
                NextState("RESPONSE")
            ).Elif(timer.done,
                NextValue(err, 1),
                write_done(flags(RMA_STATUS_ERR, RMA_STATUS_TIMEOUT)),
                NextState("RESPONSE")
            )
        )
        fsm.act("RESPONSE",
            bus.dat_r.eq(dat_r),
            bus.ack.eq(access & ~err),
            bus.err.eq(access &  err),
            NextState("IDLE")
        )

# RMA Initiator ------------------------------------------------------------------------------------

class RMAInitiator(LiteXModule):
    """Local side: Wishbone Slave giving access to the remote window, with acknowledged writes.

    ``bus``: SoC side (``origin``: byte address of the window on this bus, as given to
    SoCRegion). ``link``: connect to SERWBCore(mode="slave").bus. ``link_ready``: link state
    (phy.init.ready), used to read the remote write count again after a link loss.
    """
    def __init__(self, size, origin=0, link_ready=None):
        assert size > 0 and (size % 4) == 0 and (origin % 4) == 0
        self.size = size
        self.bus  = bus  = wishbone.Interface(data_width=32, adr_width=30)
        self.link = link = wishbone.Interface(data_width=32, adr_width=30)

        # Status.
        self.error_code = error_code = Signal(3)  # Cause of the last error (RMA_ERROR_*).
        self.errors     = errors     = Signal(32) # Accesses ended with an error.
        self.synced     = synced     = Signal()   # Remote write count known (no error since read).

        # # #

        words  = size//4
        access = Signal()
        offset = Signal(30)
        inside = Signal()
        self.comb += [
            access.eq(bus.cyc & bus.stb),
            offset.eq(bus.adr - (origin//4)),
            inside.eq(offset < words),
        ]

        # Expected remote write count.
        count      = Signal(RMA_COUNT_BITS)
        link_count = Signal(RMA_COUNT_BITS)
        link_flags = Signal(5)
        self.comb += [
            link_count.eq(link.dat_r[RMA_STATUS_COUNT:]),
            link_flags.eq(link.dat_r[:5]),
        ]

        # Link loss: remote write count has to be read again.
        desync = Signal()
        resync = Signal()
        if link_ready is not None:
            self.comb += desync.eq(~link_ready)

        def error(code):
            return [
                bus.err.eq(1),
                NextValue(error_code, code),
                If(errors != (2**32 - 1), NextValue(errors, errors + 1)),
                NextState("IDLE"),
            ]

        def status_read():
            return [
                link.cyc.eq(1),
                link.stb.eq(1),
                link.sel.eq(0b1111),
                link.adr.eq(RMA_CTRL_BASE + RMA_REG_STATUS),
            ]

        # FSM.
        # ----
        self.fsm = fsm = FSM(reset_state="IDLE")
        fsm.act("IDLE",
            If(access,
                If(~inside,
                    *error(RMA_ERROR_RANGE)
                ).Elif(~bus.we,
                    NextState("READ")
                ).Elif(synced & ~desync & ~resync,
                    NextState("WRITE")
                ).Else(
                    NextState("SYNC")
                )
            )
        )
        fsm.act("READ",
            link.cyc.eq(1),
            link.stb.eq(1),
            link.sel.eq(bus.sel),
            link.adr.eq(offset),
            bus.dat_r.eq(link.dat_r),
            If(~access,
                NextState("IDLE") # Access released by the SoC.
            ).Elif(link.ack,
                bus.ack.eq(1),
                NextState("IDLE")
            ).Elif(link.err,
                *error(RMA_ERROR_READ)
            )
        )
        # Read the remote write count (first write, after an error or a link loss).
        fsm.act("SYNC",
            *status_read(),
            If(link.ack,
                NextValue(count,  link_count),
                NextValue(synced, 1),
                NextState("WRITE")
            ).Elif(link.err,
                *error(RMA_ERROR_LINK)
            )
        )
        # Posted SerWB write.
        fsm.act("WRITE",
            link.cyc.eq(1),
            link.stb.eq(1),
            link.we.eq(1),
            link.sel.eq(bus.sel),
            link.adr.eq(offset),
            link.dat_w.eq(bus.dat_w),
            If(link.ack,
                NextState("CONFIRM")
            ).Elif(link.err,
                NextValue(synced, 0),
                *error(RMA_ERROR_LINK)
            )
        )
        # Confirmation: the Target must have handled exactly this write, without error.
        fsm.act("CONFIRM",
            *status_read(),
            If(link.ack,
                NextValue(count, link_count),
                If(link_count != (count + 1)[:RMA_COUNT_BITS],
                    *error(RMA_ERROR_LOST)
                ).Elif(link_flags[RMA_STATUS_RANGE],
                    *error(RMA_ERROR_RANGE)
                ).Elif(link_flags[RMA_STATUS_RO],
                    *error(RMA_ERROR_RO)
                ).Elif(link_flags[RMA_STATUS_BUS],
                    *error(RMA_ERROR_BUS)
                ).Elif(link_flags[RMA_STATUS_TIMEOUT],
                    *error(RMA_ERROR_TIMEOUT)
                ).Elif(link_flags[RMA_STATUS_ERR],
                    *error(RMA_ERROR_BUS)
                ).Else(
                    bus.ack.eq(1),
                    NextState("IDLE")
                )
            ).Elif(link.err,
                NextValue(synced, 0),
                *error(RMA_ERROR_LINK)
            )
        )
        self.sync += [
            If(desync,
                resync.eq(1)
            ).Elif(fsm.ongoing("SYNC") & link.ack,
                resync.eq(0)
            )
        ]
