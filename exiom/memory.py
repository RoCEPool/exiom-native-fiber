#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Memory port of a node: commands in, responses out, in the order of the commands.

``cmd``: one word access (``adr``: word address, ``we``, ``dat_w``, ``sel``, ``id``). ``rsp``: its
response (``dat_r``, ``err``, ``id`` copied from the command), for the writes too. A memory accepts
a command per cycle when it can and answers some cycles later: the accesses follow each other
without waiting for the answers, as with the native port of a memory controller. A command that is
never answered is the business of the user of the port (the Responder has a watchdog).

- ``FabricMemory``: block RAM behind this port (simulation model, small on-board memories).
- ``FabricMemoryWishbone``: a Wishbone slave behind this port, one access at a time.
"""

from migen import *

from litex.gen import *

from litex.soc.interconnect import stream, wishbone

from exiom.fabric import memory_cmd_layout, memory_rsp_layout

# Fabric Memory ------------------------------------------------------------------------------------

class FabricMemory(LiteXModule):
    """Block RAM of ``words`` words: a command per cycle, answered ``latency`` cycles later
    (``latency`` >= 1).

    For the tests: ``err_adr`` (word answered with an error, not accessed), ``hang_adr`` (word
    never answered), ``stall`` (Signal: no command accepted while it is 1).
    """
    def __init__(self, cmd, rsp, words, latency=1, err_adr=None, hang_adr=None, init=None):
        assert latency >= 1
        self.mem   = mem  = Memory(32, words, init=init)
        self.port  = port = mem.get_port(write_capable=True, we_granularity=8)
        self.specials += mem, port
        self.stall = stall = Signal()

        # # #

        # Responses wait in a FIFO with room for all the commands accepted.
        self.fifo = fifo = stream.SyncFIFO(memory_rsp_layout(), latency + 2, buffered=False)
        flight = Signal(max=latency + 4) # Commands accepted and not yet answered to the user.
        accept = Signal()
        err    = Signal()
        hang   = Signal()
        if err_adr is not None:
            self.comb += err.eq(cmd.adr == err_adr)
        if hang_adr is not None:
            self.comb += hang.eq(cmd.adr == hang_adr)
        self.comb += [
            cmd.ready.eq((flight < (latency + 2)) & ~stall),
            accept.eq(cmd.valid & cmd.ready),
            port.adr.eq(cmd.adr),
            port.dat_w.eq(cmd.dat_w),
            If(accept & cmd.we & ~err & ~hang, port.we.eq(cmd.sel)),
            fifo.source.connect(rsp),
        ]
        self.sync += flight.eq(flight + (accept & ~hang) - (rsp.valid & rsp.ready))

        # Command accepted in cycle n: memory output in cycle n + 1, response ``latency`` cycles
        # after the command.
        valid = accept & ~hang
        info  = Cat(cmd.id, err)
        for i in range(latency):
            valid_d = Signal(name=f"valid{i}")
            info_d  = Signal(5, name=f"info{i}")
            self.sync += [valid_d.eq(valid), info_d.eq(info)]
            valid, info = valid_d, info_d
        data = port.dat_r
        for i in range(latency - 1):
            data_d = Signal(32, name=f"data{i}")
            self.sync += data_d.eq(data)
            data = data_d
        self.comb += [
            fifo.sink.valid.eq(valid),
            fifo.sink.dat_r.eq(Mux(info[4], 0, data)),
            fifo.sink.err.eq(info[4]),
            fifo.sink.id.eq(info[:4]),
        ]

# Fabric Memory Wishbone ---------------------------------------------------------------------------

class FabricMemoryWishbone(LiteXModule):
    """Wishbone master behind a memory port: one access at a time (two cycles per word with a
    slave answering in one)."""
    def __init__(self, cmd, rsp, adr_width=30):
        self.bus = bus = wishbone.Interface(data_width=32, adr_width=adr_width)

        # # #

        done = Signal() # Response of the access waiting to be taken.
        self.comb += [
            bus.cyc.eq(cmd.valid & ~done),
            bus.stb.eq(cmd.valid & ~done),
            bus.adr.eq(cmd.adr),
            bus.we.eq(cmd.we),
            bus.sel.eq(cmd.sel),
            bus.dat_w.eq(cmd.dat_w),
            cmd.ready.eq(~done & (bus.ack | bus.err)),
            rsp.valid.eq(done),
        ]
        self.sync += [
            If(cmd.valid & cmd.ready,
                done.eq(1),
                rsp.dat_r.eq(bus.dat_r),
                rsp.err.eq(bus.err),
                rsp.id.eq(cmd.id),
            ),
            If(rsp.valid & rsp.ready, done.eq(0)),
        ]
