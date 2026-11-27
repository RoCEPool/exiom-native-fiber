#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Block cache: remote memory -> on-board memory -> accelerator, the data path of a near tier.

A host keeps the blocks it will need soon (for example the experts of a mixture-of-experts model)
in memory next to the accelerator, filled from the remote memory of other fabric nodes. What to
keep, where and what to evict is decided by the host (control plane); this module only moves the
words (data path):

    remote node window --(Fabric reads, several in flight)--> slot of the on-board memory
    slot of the on-board memory --(stream of address/data words)--> accelerator (e.g. PCIe P2P)

Commands (``cmd``), executed one at a time, in order:

- FETCH (op 0): read ``words`` words of the window of node ``dst`` from word address ``adr`` into
  slot ``slot``. Up to ``inflight`` reads are in flight (limited by the Requester of the node);
  results come back in any order (by tag) and are written at their place in the slot.
- PUSH  (op 1): send the first ``words`` words of slot ``slot`` on ``out``, to accelerator word
  addresses ``adr``, ``adr + 1``, ... (``last`` on the last word). ``out`` may apply backpressure.

Completions (``done``): op, slot, status and the number of words moved. Status is FABRIC_OK, the
first error of the reads of a FETCH (FABRIC_RANGE, FABRIC_NODE, FABRIC_NO_RESPONSE...: every read
of the FETCH still completes, the slot then holds a partial block the host must not use), or
FABRIC_RANGE for a command whose block does not fit in a slot (nothing is moved).

The module uses the Requester of ``node`` alone (not shared with other request sources).
The on-board memory is modeled by a Migen Memory of ``slots * slot_words`` words (on a board:
external DDR/HBM behind a LiteDRAM port).
"""

from migen import *

from litex.gen import *

from litex.soc.interconnect import stream

from exiom.fabric import FABRIC_OK, FABRIC_RANGE

# Constants ----------------------------------------------------------------------------------------

BLOCK_FETCH = 0
BLOCK_PUSH  = 1

# Layouts ------------------------------------------------------------------------------------------

def block_cmd_layout(slot_bits, words_bits):
    return [("op", 2), ("slot", slot_bits), ("dst", 8), ("adr", 32), ("words", words_bits)]

def block_done_layout(slot_bits, words_bits):
    return [("op", 2), ("slot", slot_bits), ("status", 4), ("words", words_bits)]

def block_out_layout():
    return [("adr", 32), ("data", 32)]

# Block Cache --------------------------------------------------------------------------------------

class FabricBlockCache(LiteXModule):
    def __init__(self, node, slots, slot_words, inflight=8):
        assert slots >= 1 and slot_words >= 1 and inflight >= 1
        slot_bits  = max(1, bits_for(slots - 1))
        words_bits = bits_for(slot_words)
        self.slots      = slots
        self.slot_words = slot_words

        self.cmd  = cmd  = stream.Endpoint(block_cmd_layout(slot_bits, words_bits))
        self.done = done = stream.Endpoint(block_done_layout(slot_bits, words_bits))
        self.out  = out  = stream.Endpoint(block_out_layout())

        # Status.
        self.fetched     = fetched     = Signal(32) # Words written into slots by FETCHes.
        self.pushed      = pushed      = Signal(32) # Words sent on out by PUSHes.
        self.errors      = errors      = Signal(32) # Commands completed with an error.
        self.max_pending = max_pending = Signal(8)  # Most reads in flight at once.

        # # #

        requester = node.requester

        # On-board memory.
        # ----------------
        self.mem = mem = Memory(32, slots*slot_words)
        wport = mem.get_port(write_capable=True)
        rport = mem.get_port(async_read=True)
        self.specials += mem, wport, rport

        # Tag -> word of the block (results come back in any order).
        # ------------------------------------------------------------
        self.index = index = Memory(words_bits, 256)
        iw = index.get_port(write_capable=True)
        ir = index.get_port(async_read=True)
        self.specials += index, iw, ir

        # Command being executed.
        # -----------------------
        op     = Signal(2)
        slot   = Signal(slot_bits)
        dst    = Signal(8)
        adr    = Signal(32)
        words  = Signal(words_bits)
        status = Signal(4)
        base   = Signal(max=slots*slot_words + 1)
        self.comb += base.eq(slot*slot_words)

        issued   = Signal(words_bits) # FETCH: reads accepted by the Requester / PUSH: words sent.
        received = Signal(words_bits) # FETCH: results received.
        pending  = Signal(words_bits)
        self.comb += pending.eq(issued - received)

        # FETCH: reads out.
        # -----------------
        issuing = Signal()
        self.comb += [
            requester.sink.we.eq(0),
            requester.sink.dst.eq(dst),
            requester.sink.adr.eq(adr + issued),
            requester.sink.dat_w.eq(0),
            requester.sink.sel.eq(0b1111),
            requester.sink.valid.eq(issuing & (issued < words) & (pending < inflight)),
            iw.adr.eq(requester.tag),
            iw.dat_w.eq(issued),
            iw.we.eq(requester.sink.valid & requester.sink.ready),
        ]

        # FETCH: results in (always accepted; a result of no FETCH is dropped).
        # -----------------------------------------------------------------------
        receiving = Signal()
        result    = requester.source
        self.comb += [
            result.ready.eq(1),
            ir.adr.eq(result.tag),
            wport.adr.eq(base + ir.dat_r),
            wport.dat_w.eq(result.dat_r),
            wport.we.eq(receiving & result.valid & (result.status == FABRIC_OK)),
        ]

        # PUSH: words out.
        # ----------------
        pushing = Signal()
        self.comb += [
            rport.adr.eq(base + issued),
            out.adr.eq(adr + issued),
            out.data.eq(rport.dat_r),
            out.last.eq(issued == (words - 1)),
            out.valid.eq(pushing),
        ]

        # Control.
        # --------
        self.fsm = fsm = FSM(reset_state="IDLE")
        fsm.act("IDLE",
            cmd.ready.eq(1),
            If(cmd.valid,
                NextValue(op,       cmd.op),
                NextValue(slot,     cmd.slot),
                NextValue(dst,      cmd.dst),
                NextValue(adr,      cmd.adr),
                NextValue(words,    cmd.words),
                NextValue(status,   FABRIC_OK),
                NextValue(issued,   0),
                NextValue(received, 0),
                If((cmd.slot >= slots) | (cmd.words > slot_words),
                    NextValue(status, FABRIC_RANGE),
                    NextValue(words,  0),
                    NextState("DONE")
                ).Elif(cmd.words == 0,
                    NextState("DONE")
                ).Elif(cmd.op == BLOCK_FETCH,
                    NextState("FETCH")
                ).Else(
                    NextState("PUSH")
                )
            )
        )
        fsm.act("FETCH",
            issuing.eq(1),
            receiving.eq(1),
            If(requester.sink.valid & requester.sink.ready,
                NextValue(issued, issued + 1)
            ),
            If(result.valid,
                NextValue(received, received + 1),
                If(result.status == FABRIC_OK,
                    NextValue(fetched, fetched + 1)
                ).Elif(status == FABRIC_OK,
                    NextValue(status, result.status)
                ),
                If((received + 1) == words,
                    NextState("DONE")
                )
            )
        )
        fsm.act("PUSH",
            pushing.eq(1),
            If(out.ready,
                NextValue(issued, issued + 1),
                NextValue(pushed, pushed + 1),
                If(out.last,
                    NextState("DONE")
                )
            )
        )
        fsm.act("DONE",
            done.valid.eq(1),
            done.op.eq(op),
            done.slot.eq(slot),
            done.status.eq(status),
            done.words.eq(words),
            If(done.ready,
                If(status != FABRIC_OK,
                    NextValue(errors, errors + 1)
                ),
                NextState("IDLE")
            )
        )
        self.sync += If(pending > max_pending, max_pending.eq(pending))
