#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Pool: the memory windows of the nodes of a fabric seen as one address space.

Global address (words): [window_bits-1:0] word in the window of a node, [window_bits+7:
window_bits] node. The map is static: every node owns the same number of addresses
(``2**window_bits`` words, at least the size of its window) and the node of an address is
computed, not looked up: no table to keep consistent between the nodes. An address of a node that
is not in the fabric ends with FABRIC_NODE, an address after the end of the window of its node
with FABRIC_RANGE.

Operations (one at a time per ``FabricPool``):

- POOL_READ, POOL_WRITE: one word.
- POOL_COPY: ``words`` words from ``adr`` to ``dst_adr``, wherever they are. The copy is cut in
  copy requests (``FabricCopy``) of at most ``chunk`` words, each inside the window of one source
  node and of one destination node, sent to the source node, which writes straight to the
  destination node: the data does not go through the node that asks. A copy can start in a node
  and go on in the next ones. A source node already copying (FABRIC_BUSY) is asked again every
  ``busy_wait`` cycles, ``busy_retries`` times at most.

Result: status (FABRIC_*) and read data, or, for a copy, the first error and the words copied
before it.
"""

from migen import *

from litex.gen import *
from litex.gen.genlib.misc import WaitTimer

from litex.soc.interconnect import stream

from exiom.fabric import *

# Constants ----------------------------------------------------------------------------------------

POOL_READ  = 0
POOL_WRITE = 1
POOL_COPY  = 2

def pool_address(node, word, window_bits):
    """Global address of word ``word`` of node ``node``."""
    assert 0 <= node < 256 and 0 <= word < 2**window_bits
    return (node << window_bits) | word

def pool_split(address, window_bits):
    """(node, word) of a global address."""
    return address >> window_bits, address & (2**window_bits - 1)

def pool_request_layout(window_bits):
    return [("op", 2), ("adr", window_bits + 8), ("dat_w", 32), ("sel", 4),
        ("dst_adr", window_bits + 8), ("words", 32)]

def pool_result_layout():
    return [("status", 4), ("dat_r", 32), ("words", 32)]

def smallest(target, values):
    """Statements setting ``target`` to the smallest of ``values``."""
    statements = []
    best = values[0]
    for i, value in enumerate(values[1:]):
        new = Signal(max(len(best), len(value)), name=f"smallest{i}")
        statements.append(new.eq(Mux(value < best, value, best)))
        best = new
    return statements + [target.eq(best)]

# Fabric Pool --------------------------------------------------------------------------------------

class FabricPool(LiteXModule):
    """Reads, writes and copies with global addresses, through the Requester of ``node`` (not
    shared with other request sources).

    ``sink``: operation (taken when its result is given). ``source``: result.
    """
    def __init__(self, node, window_bits, chunk=256, busy_wait=64, busy_retries=1024):
        assert 1 <= window_bits <= 32 and 1 <= chunk <= FABRIC_COPY_WORDS
        self.window_bits = window_bits
        self.sink   = sink   = stream.Endpoint(pool_request_layout(window_bits))
        self.source = source = stream.Endpoint(pool_result_layout())

        # Status.
        self.chunks  = chunks  = Signal(32) # Copy requests sent.
        self.retries = retries = Signal(32) # Copy requests sent again (source node busy).

        # # #

        requester = node.requester
        abits     = window_bits + 8
        window    = 2**window_bits

        adr     = Signal(abits)
        dst_adr = Signal(abits)
        left    = Signal(32)
        copied  = Signal(32)
        tag     = Signal(8)
        status  = Signal(4)
        dat_r   = Signal(32)
        tries   = Signal(max=busy_retries + 1)

        # Next copy request: up to the end of the window of the source and of the destination.
        src_room = Signal(window_bits + 1)
        dst_room = Signal(window_bits + 1)
        size     = Signal(11)
        self.comb += [
            src_room.eq(window - adr[:window_bits]),
            dst_room.eq(window - dst_adr[:window_bits]),
            *smallest(size, [Constant(chunk, 11), left, src_room, dst_room]),
        ]

        self.timer = timer = WaitTimer(busy_wait)

        result = Signal()
        self.comb += [
            source.status.eq(status),
            source.dat_r.eq(dat_r),
            source.words.eq(copied),
            # Results of other requests (none expected) are dropped.
            result.eq(requester.source.valid & (requester.source.tag == tag)),
        ]

        self.fsm = fsm = FSM(reset_state="IDLE")
        fsm.act("IDLE",
            If(sink.valid,
                NextValue(adr,     sink.adr),
                NextValue(dst_adr, sink.dst_adr),
                NextValue(left,    sink.words),
                NextValue(copied,  0),
                NextValue(dat_r,   0),
                NextValue(status,  FABRIC_OK),
                NextValue(tries,   0),
                If(sink.op == POOL_COPY,
                    If(sink.words == 0, NextState("RESULT")).Else(NextState("COPY"))
                ).Else(
                    NextState("ACCESS")
                )
            )
        )
        fsm.act("ACCESS",
            requester.sink.valid.eq(1),
            requester.sink.we.eq(sink.op == POOL_WRITE),
            requester.sink.dst.eq(adr[window_bits:]),
            requester.sink.adr.eq(adr[:window_bits]),
            requester.sink.dat_w.eq(sink.dat_w),
            requester.sink.sel.eq(sink.sel),
            If(requester.sink.ready,
                NextValue(tag, requester.tag),
                NextState("ACCESS-RESULT")
            )
        )
        fsm.act("ACCESS-RESULT",
            requester.source.ready.eq(1),
            If(result,
                NextValue(status, requester.source.status),
                NextValue(dat_r,  requester.source.dat_r),
                NextState("RESULT")
            )
        )
        fsm.act("COPY",
            requester.sink.valid.eq(1),
            requester.sink.copy.eq(1),
            requester.sink.dst.eq(adr[window_bits:]),
            requester.sink.adr.eq(adr[:window_bits]),
            requester.sink.copy_dst.eq(dst_adr[window_bits:]),
            requester.sink.dat_w.eq(dst_adr[:window_bits]),
            requester.sink.words.eq(size),
            If(requester.sink.ready,
                NextValue(tag, requester.tag),
                NextValue(chunks, chunks + 1),
                NextState("COPY-RESULT")
            )
        )
        fsm.act("COPY-RESULT",
            requester.source.ready.eq(1),
            If(result,
                If((requester.source.status == FABRIC_BUSY) & (tries != busy_retries),
                    NextValue(tries,   tries + 1),
                    NextValue(retries, retries + 1),
                    NextState("COPY-WAIT")
                ).Else(
                    # Words written by this copy request (all of them without error).
                    NextValue(copied,  copied  + requester.source.dat_r),
                    NextValue(adr,     adr     + requester.source.dat_r),
                    NextValue(dst_adr, dst_adr + requester.source.dat_r),
                    NextValue(left,    left    - requester.source.dat_r),
                    NextValue(tries,   0),
                    NextValue(status,  requester.source.status),
                    If((requester.source.status != FABRIC_OK) | (left == requester.source.dat_r),
                        NextState("RESULT")
                    ).Else(
                        NextState("COPY")
                    )
                )
            )
        )
        fsm.act("COPY-WAIT",
            timer.wait.eq(1),
            If(timer.done, NextState("COPY"))
        )
        fsm.act("RESULT",
            source.valid.eq(1),
            If(source.ready,
                sink.ready.eq(1),
                NextState("IDLE")
            )
        )
