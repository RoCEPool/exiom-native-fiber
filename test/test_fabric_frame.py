#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Control frames (exiom/frame.py): commands from a host in Ethernet frames, run in the pool.

4 nodes; the frames are given to node 0 as bytes (no MAC, no PHY).
"""

import unittest

from migen import *

from litex.gen import *
from litex.gen.sim import *

from exiom.fabric   import *
from exiom.topology import *
from exiom.network  import *
from exiom.pool     import *
from exiom.frame    import *

from test.test_fabric         import wait
from test.test_fabric_network import DUT, run, parallel
from test.test_fabric_copy    import fill, data, idle, WINDOW_BITS, WORDS
from test.fabric_sim          import run_simulation

MAC  = 0x02_00_00_e7_10_00 # Node.
HOST = 0x02_00_00_00_00_aa

def g(node, word):
    return pool_address(node, word, WINDOW_BITS)

class FrameDUT(DUT):
    def __init__(self, topology, **kwargs):
        DUT.__init__(self, topology, **kwargs)
        self.pool  = FabricPool(self.nodes[0], window_bits=WINDOW_BITS, chunk=16, busy_wait=16)
        self.frame = FabricFrame(self.pool, mac=MAC)

def send(dut, frame, gap=0):
    """Gives a frame to the node, one byte per cycle."""
    sink = dut.frame.sink
    for i, byte in enumerate(frame):
        yield sink.data.eq(byte)
        yield sink.last.eq(i == len(frame) - 1)
        yield sink.valid.eq(1)
        yield
    yield sink.valid.eq(0)
    yield sink.last.eq(0)
    for _ in range(gap):
        yield

def receive(dut, timeout=20000):
    """Waits for a frame from the node, returns its bytes."""
    source = dut.frame.source
    frame  = []
    yield source.ready.eq(1)
    yield
    for _ in range(timeout):
        if (yield source.valid):
            frame.append((yield source.data))
            if (yield source.last):
                break
        yield
    else:
        raise TimeoutError("No reply.")
    yield source.ready.eq(0)
    yield
    return frame

def command(dut, *args, **kwargs):
    """Sends a command, returns the fields of its reply."""
    yield from send(dut, command_frame(MAC, HOST, *args, **kwargs))
    return parse_reply((yield from receive(dut)))

def no_reply(test, dut, cycles=200):
    for _ in range(cycles):
        test.assertEqual((yield dut.frame.source.valid), 0)
        yield

# Test Fabric Frame --------------------------------------------------------------------------------

class TestFabricFrame(unittest.TestCase):
    def test_frames(self):
        # Layout of the frames.
        frame = command_frame(MAC, HOST, POOL_COPY, 0x1234, 0x0102030405060708, dst_adr=0x1112131415161718, words=0x21222324)
        self.assertEqual(len(frame), 60)
        self.assertEqual(frame[0:14], [0x02, 0, 0, 0xe7, 0x10, 0, 0x02, 0, 0, 0, 0, 0xaa, 0x88, 0xb5])
        self.assertEqual(frame[14:18], [2, 0, 0x12, 0x34])
        self.assertEqual(frame[18:26], [1, 2, 3, 4, 5, 6, 7, 8])
        self.assertEqual(frame[26:38], [0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x21, 0x22, 0x23, 0x24])

    def test_commands(self):
        # Copy, write and read asked by the host; the data of the copy does not go through
        # node 0.
        dut = FrameDUT(two_tier(4))
        def generator():
            yield from fill(dut, 1, 0, 40)
            forwarded = (yield dut.nodes[0].responder.requests)
            reply = yield from command(dut, POOL_COPY, 0x0101, g(1, 0), dst_adr=g(2, 20), words=40)
            self.assertEqual(reply, dict(dst=HOST, src=MAC, ethertype=FRAME_ETHERTYPE, op=0x80 | POOL_COPY,
                status=FABRIC_OK, id=0x0101, value=40, length=60))
            for i in range(40):
                self.assertEqual((yield dut.mems[2].mem[20 + i]), data(1, i))
            self.assertEqual((yield dut.nodes[0].responder.requests), forwarded)
            self.assertEqual((yield dut.nodes[0].forwarded), 0)
            # Write then read, two frames in a row.
            reply = yield from command(dut, POOL_WRITE, 2, g(3, 5), dat_w=0xdeadbeef)
            self.assertEqual((reply["op"], reply["status"], reply["id"]), (0x80 | POOL_WRITE, FABRIC_OK, 2))
            self.assertEqual((yield dut.mems[3].mem[5]), 0xdeadbeef)
            reply = yield from command(dut, POOL_READ, 3, g(3, 5))
            self.assertEqual((reply["op"], reply["status"], reply["id"], reply["value"]),
                (0x80 | POOL_READ, FABRIC_OK, 3, 0xdeadbeef))
            # Frame of exactly the bytes of a command, and a longer one.
            reply = yield from command(dut, POOL_READ, 4, g(3, 5), pad=0)
            self.assertEqual((reply["status"], reply["value"]), (FABRIC_OK, 0xdeadbeef))
            reply = yield from command(dut, POOL_READ, 5, g(3, 5), pad=1000)
            self.assertEqual((reply["status"], reply["value"]), (FABRIC_OK, 0xdeadbeef))
            self.assertEqual((yield dut.frame.commands), 5)
            self.assertEqual((yield dut.frame.malformed) + (yield dut.frame.ignored) + (yield dut.frame.busy_drops), 0)
            yield from idle(self, dut)
        run(dut, [generator()])

    def test_errors(self):
        # Errors of the pool in the reply.
        dut = FrameDUT(two_tier(4))
        def generator():
            yield from fill(dut, 1, 0, WORDS)
            # Node that does not exist, address over the pool, word after the end of the window.
            reply = yield from command(dut, POOL_READ, 1, g(9, 0))
            self.assertEqual((reply["status"], reply["id"]), (FABRIC_NODE, 1))
            reply = yield from command(dut, POOL_READ, 2, 1 << 40)
            self.assertEqual(reply["status"], FABRIC_NODE)
            reply = yield from command(dut, POOL_COPY, 3, g(1, 0), dst_adr=1 << 63, words=4)
            self.assertEqual((reply["status"], reply["value"]), (FABRIC_NODE, 0))
            # Copy running past the last node: words copied before the error.
            reply = yield from command(dut, POOL_COPY, 4, g(1, 0), dst_adr=g(3, WORDS - 8), words=16)
            self.assertEqual((reply["status"], reply["value"]), (FABRIC_NODE, 8))
            self.assertEqual((yield dut.mems[3].mem[WORDS - 1]), data(1, 7))
            yield from idle(self, dut)
        run(dut, [generator()])

    def test_other_frames(self):
        # Frames that are not commands: no reply, nothing done.
        dut = FrameDUT(two_tier(4))
        def generator():
            good = command_frame(MAC, HOST, POOL_WRITE, 1, g(1, 0), dat_w=0x55)
            # Other EtherType (IPv4), other destination, broadcast.
            yield from send(dut, command_frame(MAC, HOST, POOL_WRITE, 1, g(1, 0), dat_w=0x55, ethertype=0x0800))
            yield from send(dut, command_frame(MAC + 1, HOST, POOL_WRITE, 1, g(1, 0), dat_w=0x55))
            yield from send(dut, command_frame(0xffffffffffff, HOST, POOL_WRITE, 1, g(1, 0), dat_w=0x55))
            yield from send(dut, [0x45]*7)
            yield from wait(4)
            self.assertEqual((yield dut.frame.ignored), 4)
            # Ours, but cut, or with an operation that does not exist.
            yield from send(dut, good[:37])
            yield from send(dut, good[:14])
            yield from send(dut, good[:20])
            yield from send(dut, command_frame(MAC, HOST, 3, 1, g(1, 0)))
            yield from wait(4)
            self.assertEqual((yield dut.frame.malformed), 4)
            yield from no_reply(self, dut)
            self.assertEqual((yield dut.frame.commands), 0)
            self.assertEqual((yield dut.mems[1].mem[0]), 0)
            # A command after them, without any gap.
            yield from send(dut, good)
            reply = parse_reply((yield from receive(dut)))
            self.assertEqual((reply["status"], reply["id"]), (FABRIC_OK, 1))
            self.assertEqual((yield dut.mems[1].mem[0]), 0x55)
        run(dut, [generator()])

    def test_busy(self):
        # Command received while another one runs: dropped and counted; the first one is answered,
        # and the host can send the second one again.
        dut = FrameDUT(two_tier(4))
        def generator():
            yield from fill(dut, 1, 0, WORDS)
            yield from send(dut, command_frame(MAC, HOST, POOL_COPY, 1, g(1, 0), dst_adr=g(2, 0), words=WORDS))
            yield from send(dut, command_frame(MAC, HOST, POOL_WRITE, 2, g(3, 9), dat_w=0x77))
            reply = parse_reply((yield from receive(dut)))
            self.assertEqual((reply["id"], reply["status"], reply["value"]), (1, FABRIC_OK, WORDS))
            self.assertEqual((yield dut.frame.busy_drops), 1)
            self.assertEqual((yield dut.mems[3].mem[9]), 0)
            yield from no_reply(self, dut, 100)
            reply = yield from command(dut, POOL_WRITE, 2, g(3, 9), dat_w=0x77)
            self.assertEqual((reply["id"], reply["status"]), (2, FABRIC_OK))
            self.assertEqual((yield dut.mems[3].mem[9]), 0x77)
        run(dut, [generator()])

    def test_slow_reader(self):
        # Reply taken slowly by the network card.
        dut = FrameDUT(two_tier(2))
        def generator():
            yield from send(dut, command_frame(MAC, HOST, POOL_WRITE, 7, g(1, 3), dat_w=0x99))
            source = dut.frame.source
            frame  = []
            for cycle in range(2000):
                # Ready one cycle out of three.
                ready = int(cycle % 3 == 0)
                yield source.ready.eq(ready)
                yield
                if ready and (yield source.valid):
                    frame.append((yield source.data))
                    if (yield source.last):
                        break
            reply = parse_reply(frame)
            self.assertEqual((reply["length"], reply["id"], reply["status"], reply["dst"]), (60, 7, FABRIC_OK, HOST))
        run(dut, [generator()])

if __name__ == "__main__":
    unittest.main()
