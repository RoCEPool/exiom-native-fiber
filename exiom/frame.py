#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Control frames: a host asks reads, writes and copies in the pool with Ethernet frames.

For a node placed between the network card of a server and the fabric: the host sends a command
frame through its card to the MAC address of the node, the node runs it with ``FabricPool`` (global
addresses; the data of a copy goes from the source node straight to the destination node, not
through the host) and sends a reply frame back. Only these frames are handled: the others are
ignored (they are not forwarded through the fabric).

Byte streams with ``last`` (the frames without preamble and FCS, as on the user side of a MAC).

Command frame (bytes, big-endian fields):

    0   destination MAC (6): MAC of the node
    6   source MAC (6)
    12  EtherType (2): 0x88B5 (local experimental)
    14  operation (1): 0 read, 1 write, 2 copy
    15  reserved (1)
    16  request id (2): copied to the reply
    18  address (8): global word address (source of a copy)
    26  write data (4), or destination global word address of a copy (8)
    34  words to copy (4)
    38  padding, ignored

Reply frame (60 bytes):

    0   destination MAC (6): source of the command
    6   source MAC (6): MAC of the node
    12  EtherType (2)
    14  operation (1): the one of the command, with bit 7 set
    15  status (1): FABRIC_* (0: done)
    16  request id (2)
    18  read data (4), or words copied (4)
    22  zeros

One command at a time: a command received while another one runs is dropped (``busy_drops``); the
host sends it again when the reply does not come. Frames too short for a command, or with an
operation that does not exist, are dropped (``malformed``).
"""

from migen import *

from litex.gen import *

from litex.soc.interconnect import stream

from exiom.fabric import *
from exiom.pool   import *

# Constants ----------------------------------------------------------------------------------------

FRAME_ETHERTYPE = 0x88B5
FRAME_COMMAND   = 38 # Bytes of a command frame, at least.
FRAME_REPLY     = 60 # Bytes of a reply frame.

def frame_layout():
    return [("data", 8)]

def command_frame(mac, src_mac, op, request_id, adr, dat_w=0, dst_adr=0, words=0, ethertype=FRAME_ETHERTYPE, pad=22):
    """Bytes of a command frame."""
    frame  = mac.to_bytes(6, "big") + src_mac.to_bytes(6, "big") + ethertype.to_bytes(2, "big")
    frame += bytes([op, 0]) + request_id.to_bytes(2, "big") + adr.to_bytes(8, "big")
    frame += dst_adr.to_bytes(8, "big") if op == POOL_COPY else dat_w.to_bytes(4, "big") + bytes(4)
    frame += words.to_bytes(4, "big") + bytes(pad)
    return list(frame)

def parse_reply(frame):
    """Fields of a reply frame (bytes)."""
    frame = bytes(frame)
    return {
        "dst"       : int.from_bytes(frame[0:6], "big"),
        "src"       : int.from_bytes(frame[6:12], "big"),
        "ethertype" : int.from_bytes(frame[12:14], "big"),
        "op"        : frame[14],
        "status"    : frame[15],
        "id"        : int.from_bytes(frame[16:18], "big"),
        "value"     : int.from_bytes(frame[18:22], "big"),
        "length"    : len(frame),
    }

# Fabric Frame -------------------------------------------------------------------------------------

class FabricFrame(LiteXModule):
    """Runs the commands received in control frames with ``pool`` (FabricPool, used by this module
    only) and answers them.

    ``sink``: frames from the network card (always accepted). ``source``: reply frames.
    """
    def __init__(self, pool, mac, ethertype=FRAME_ETHERTYPE):
        self.sink   = sink   = stream.Endpoint(frame_layout())
        self.source = source = stream.Endpoint(frame_layout())

        # Status.
        self.commands   = commands   = Signal(32) # Commands run.
        self.ignored    = ignored    = Signal(32) # Frames that are not for this module.
        self.malformed  = malformed  = Signal(32) # Commands too short, or with an unknown operation.
        self.busy_drops = busy_drops = Signal(32) # Commands received while another one was running.

        # # #

        abits = len(pool.sink.adr)

        # Frames in: the first bytes of the frame, first byte in the high bits.
        # -----------------------------------------------------------------------
        head    = Signal(8*FRAME_COMMAND)
        count   = Signal(max=FRAME_COMMAND + 1) # Bytes of the frame received (saturating).
        shifted = Signal(8*FRAME_COMMAND)
        full    = Signal()
        self.comb += [
            sink.ready.eq(1),
            shifted.eq(Mux(count == FRAME_COMMAND, head, Cat(sink.data, head[:-8]))),
            full.eq((count == FRAME_COMMAND) | (count == FRAME_COMMAND - 1)),
        ]
        self.sync += If(sink.valid,
            head.eq(shifted),
            If(sink.last,
                count.eq(0)
            ).Elif(count != FRAME_COMMAND,
                count.eq(count + 1)
            )
        )
        def field(first, length, bits=shifted):
            """Bytes ``first`` to ``first + length - 1`` of the frame."""
            high = 8*(FRAME_COMMAND - first)
            return bits[high - 8*length:high]

        # Command being run.
        # ------------------
        running = Signal()
        cmd     = Signal(8*FRAME_COMMAND)
        end     = Signal() # Last byte of a frame.
        mine    = Signal() # Frame for this module.
        valid   = Signal() # With a command.
        op      = Signal(8)
        self.comb += [
            end.eq(sink.valid & sink.last),
            mine.eq(full & (field(0, 6) == mac) & (field(12, 2) == ethertype)),
            op.eq(field(14, 1)),
            valid.eq(mine & (op <= POOL_COPY)),
        ]
        # A frame shorter than a command is still in the low bytes of ``shifted``: only the
        # frames with all their bytes are looked at (``full``); the short ones are told apart by
        # the EtherType seen at its place when it came.
        typed   = Signal() # Frame with our MAC and EtherType (seen at byte 13).
        at_type = Signal()
        first = Signal(8*14) # First 14 bytes of the frame, when the last one of them comes.
        self.comb += [
            first.eq(Cat(sink.data, head[:8*13])),
            at_type.eq(sink.valid & (count == 13) & (first[-48:] == mac) & (first[:16] == ethertype)),
        ]
        self.sync += [
            If(at_type, typed.eq(1)),
            If(end, typed.eq(0)),
        ]
        start = Signal()
        self.comb += start.eq(end & valid & ~running)
        self.sync += [
            If(end,
                If(valid,
                    If(running,
                        busy_drops.eq(busy_drops + 1)
                    ).Else(
                        running.eq(1),
                        cmd.eq(shifted),
                        commands.eq(commands + 1),
                    )
                ).Elif(typed | (at_type & sink.last),
                    malformed.eq(malformed + 1)
                ).Else(
                    ignored.eq(ignored + 1)
                )
            )
        ]

        # Pool.
        # -----
        c_op    = field(14, 1, cmd)
        c_adr   = field(18, 8, cmd)
        c_data  = field(26, 4, cmd)
        c_dst   = field(26, 8, cmd)
        c_words = field(34, 4, cmd)
        outside = Signal() # Address that is not in the pool.
        status  = Signal(4)
        value   = Signal(32)
        self.comb += [
            outside.eq((c_adr[abits:] != 0) | ((c_op == POOL_COPY) & (c_dst[abits:] != 0))),
            pool.sink.op.eq(c_op),
            pool.sink.adr.eq(c_adr),
            pool.sink.dat_w.eq(c_data),
            pool.sink.sel.eq(0b1111),
            pool.sink.dst_adr.eq(c_dst),
            pool.sink.words.eq(c_words),
        ]

        # Reply.
        # ------
        reply = Signal(8*FRAME_REPLY)
        index = Signal(max=FRAME_REPLY)
        self.fsm = fsm = FSM(reset_state="IDLE")
        fsm.act("IDLE",
            If(running,
                If(outside,
                    NextValue(status, FABRIC_NODE),
                    NextValue(value,  0),
                    NextState("REPLY")
                ).Else(
                    NextState("RUN")
                )
            )
        )
        fsm.act("RUN",
            pool.sink.valid.eq(1),
            pool.source.ready.eq(1),
            If(pool.source.valid,
                NextValue(status, pool.source.status),
                NextValue(value,  Mux(c_op == POOL_COPY, pool.source.words, pool.source.dat_r)),
                NextState("REPLY")
            )
        )
        fsm.act("REPLY",
            # Destination: source of the command. First byte in the high bits.
            NextValue(reply, Cat(Constant(0, 8*(FRAME_REPLY - 22)), value, field(16, 2, cmd),
                status, Constant(0, 4), c_op[:7], Constant(1, 1), Constant(ethertype, 16),
                Constant(mac, 48), field(6, 6, cmd))),
            NextValue(index, 0),
            NextState("SEND")
        )
        fsm.act("SEND",
            source.valid.eq(1),
            source.data.eq(reply[-8:]),
            source.last.eq(index == FRAME_REPLY - 1),
            If(source.ready,
                NextValue(reply, Cat(Constant(0, 8), reply[:-8])),
                NextValue(index, index + 1),
                If(source.last,
                    NextValue(running, 0),
                    NextState("IDLE")
                )
            )
        )
