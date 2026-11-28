#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
RoCE target (exiom/roce.py): RDMA READ and WRITE of a RoCEv2 requester, run in the pool.

The requester is a software model of the RC requester of a network card (go-back-N: it goes back
to the first packet not acknowledged on a PSN sequence error NAK, on a response after a missing
one, or after a timeout), with packets lost or damaged on purpose in both directions. 2 nodes; the
frames are given to node 0 as bytes (no MAC, no PHY). The memory region is spread over the
memory of node 1 by its page table, the pages in another order than in the region.
"""

import random
import unittest
import zlib

from migen import *

from litex.gen import *

from exiom.fabric   import *
from exiom.topology import *
from exiom.pool     import *
from exiom.roce     import *

from test.test_fabric_network import DUT, run, parallel
from test.fabric_sim          import run_simulation

MAC      = 0x02_00_00_e7_10_00 # Node 0 (target).
IP       = 0x0a_00_00_01
HOST_MAC = 0x02_00_00_00_00_aa
HOST_IP  = 0x0a_00_00_02
QPN      = 0x100               # QP 0 of the target.
HOST_QPN = 0x11
SPORT    = 0xc011
RKEY     = 0x00c0ffee
RO_RKEY  = 0x00000bad          # Read-only region.
VA       = 0x7f00_1234_0000
PMTU     = 256
PAGE     = 256                 # Bytes.
WINDOW_BITS = 8                # 256 words (1 KiB) per node.
PAGES    = [3, 0, 2, 1]        # Page i of the region: page PAGES[i] of node 1.
REGION   = PAGE*len(PAGES)

# Packets (software model) -------------------------------------------------------------------------

def ip_checksum(header):
    total = sum(int.from_bytes(header[i:i + 2], "big") for i in range(0, len(header), 2))
    while total >> 16:
        total = (total & 0xffff) + (total >> 16)
    return (~total) & 0xffff

def roce_packet(opcode, psn, payload=b"", reth=None, aeth=None, ackreq=0, dqpn=QPN, ecn=0, pad=0,
    dst_mac=MAC, src_mac=HOST_MAC, dst_ip=IP, src_ip=HOST_IP, sport=SPORT, dport=ROCE_PORT):
    """Bytes of a RoCEv2 frame, with its ICRC."""
    bth  = bytes([opcode, pad << 4]) + (0xffff).to_bytes(2, "big") + bytes([0])
    bth += dqpn.to_bytes(3, "big") + bytes([ackreq << 7]) + (psn & 0xffffff).to_bytes(3, "big")
    ext  = b""
    if reth is not None:
        va, rkey, dlen = reth
        ext += va.to_bytes(8, "big") + rkey.to_bytes(4, "big") + dlen.to_bytes(4, "big")
    if aeth is not None:
        syn, msn = aeth
        ext += bytes([syn]) + msn.to_bytes(3, "big")
    udp_len = 8 + len(bth) + len(ext) + len(payload) + 4
    ip_hdr  = bytes([0x45, ecn]) + (20 + udp_len).to_bytes(2, "big") + bytes(2) + (0x4000).to_bytes(2, "big")
    ip_hdr += bytes([64, 17, 0, 0]) + src_ip.to_bytes(4, "big") + dst_ip.to_bytes(4, "big")
    ip_hdr  = ip_hdr[:10] + ip_checksum(ip_hdr).to_bytes(2, "big") + ip_hdr[12:]
    udp     = sport.to_bytes(2, "big") + dport.to_bytes(2, "big") + udp_len.to_bytes(2, "big") + bytes(2)
    frame   = list(dst_mac.to_bytes(6, "big") + src_mac.to_bytes(6, "big") + (0x0800).to_bytes(2, "big"))
    frame  += list(ip_hdr + udp + bth + ext + payload)
    return frame + roce_icrc(frame)

def parse_packet(frame):
    """Fields of a frame from the target; checks its IP checksum, lengths and ICRC."""
    frame = bytes(frame)
    assert int.from_bytes(frame[12:14], "big") == 0x0800
    assert ip_checksum(frame[14:34]) == 0, "IP checksum"
    assert int.from_bytes(frame[16:18], "big") + 14 == len(frame), "IP length"
    assert int.from_bytes(frame[38:40], "big") + 34 == len(frame), "UDP length"
    assert list(frame[-4:]) == roce_icrc(list(frame[:-4])), "ICRC"
    op  = frame[42]
    pkt = dict(
        dst_mac = int.from_bytes(frame[0:6], "big"),
        src_mac = int.from_bytes(frame[6:12], "big"),
        src_ip  = int.from_bytes(frame[26:30], "big"),
        dst_ip  = int.from_bytes(frame[30:34], "big"),
        sport   = int.from_bytes(frame[34:36], "big"),
        dport   = int.from_bytes(frame[36:38], "big"),
        op      = op,
        dqpn    = int.from_bytes(frame[47:50], "big"),
        psn     = int.from_bytes(frame[51:54], "big"),
    )
    body = frame[54:-4]
    if op in (ROCE_ACK, ROCE_READ_RESP_FIRST, ROCE_READ_RESP_LAST, ROCE_READ_RESP_ONLY):
        pkt["syn"] = body[0]
        pkt["msn"] = int.from_bytes(body[1:4], "big")
        body = body[4:]
    pkt["payload"] = body
    return pkt

# Requester (software model) -----------------------------------------------------------------------

class Message:
    def __init__(self, kind, va, data=b"", length=0, rkey=RKEY, opcode=None):
        self.kind   = kind   # "write", "read" or "raw" (one packet, given opcode, answered by ACK/NAK).
        self.va     = va
        self.data   = data
        self.length = length if kind == "read" else len(data)
        self.rkey   = rkey
        self.opcode = opcode
        self.result = None   # Syndrome of the completion.
        self.got    = bytearray()

class Requester:
    """RC requester of a network card: sends the packets of ``messages``, at most ``window`` not
    acknowledged, and completes them in order."""
    def __init__(self, messages, psn=0x123456, window=4, timeout=6000, drop_tx=(), drop_rx=(),
        corrupt_tx=(), ecn_tx=(), pmtu=PMTU):
        self.messages = messages
        self.packets  = [] # (message, psn, frame, responses expected)
        p = psn
        for m in messages:
            first = p
            if m.kind == "write":
                n = max(1, -(-m.length // pmtu))
                for i in range(n):
                    chunk = m.data[i*pmtu:(i + 1)*pmtu]
                    if n == 1:
                        op, reth = ROCE_WRITE_ONLY, (m.va, m.rkey, m.length)
                    elif i == 0:
                        op, reth = ROCE_WRITE_FIRST, (m.va, m.rkey, m.length)
                    else:
                        op, reth = (ROCE_WRITE_LAST if i == n - 1 else ROCE_WRITE_MIDDLE), None
                    self.packets.append([m, p, (op, chunk, reth), 1 if i == n - 1 else 0])
                    p = (p + 1) & 0xffffff
            elif m.kind == "read":
                n = max(1, -(-m.length // pmtu))
                self.packets.append([m, p, (ROCE_READ_REQUEST, b"", (m.va, m.rkey, m.length)), n])
                p = (p + n) & 0xffffff
            else:
                self.packets.append([m, p, (m.opcode, m.data, (m.va, m.rkey, m.length)), 1])
                p = (p + 1) & 0xffffff
        self.window     = window
        self.timeout    = timeout
        self.drop_tx    = set(drop_tx)
        self.drop_rx    = set(drop_rx)
        self.corrupt_tx = set(corrupt_tx)
        self.ecn_tx     = set(ecn_tx)
        self.una  = 0 # First packet not completed.
        self.next = 0 # Next packet to send.
        self.sent = 0
        self.received   = 0
        self.retransmits = 0
        self.cnps  = []
        self.idle  = 0
        self.got_responses = 0 # Responses of the read at ``una`` received.
        self.back_pending  = False # Gone back, no progress since: later gaps are the same loss.

    def done(self):
        return self.una == len(self.packets)

    def frame(self, index):
        m, psn, (op, payload, reth), _ = self.packets[index]
        ackreq = 1 if op in (ROCE_WRITE_LAST, ROCE_WRITE_ONLY) else 0
        frame = roce_packet(op, psn, payload, reth=reth, ackreq=ackreq,
            ecn=3 if self.sent in self.ecn_tx else 2)
        if self.sent in self.corrupt_tx:
            frame[60] ^= 0x01
        dropped = self.sent in self.drop_tx
        self.sent += 1
        return None if dropped else frame

    def next_frame(self):
        """Next frame to send (None: nothing to send, or the frame is lost)."""
        if self.next < len(self.packets) and self.next - self.una < self.window:
            frame = self.frame(self.next)
            self.next += 1
            return frame, True
        return None, False

    def go_back(self):
        self.retransmits += 1
        self.next = self.una
        self.got_responses = 0
        self.back_pending = True

    def complete(self, result):
        m = self.packets[self.una][0]
        if self.packets[self.una][3] or m.kind != "write":
            m.result = result
        self.una += 1
        self.got_responses = 0
        self.idle = 0
        self.back_pending = False

    def receive(self, frame):
        index = self.received
        self.received += 1
        if index in self.drop_rx:
            return
        pkt = parse_packet(frame)
        assert (pkt["dst_mac"], pkt["src_mac"], pkt["src_ip"], pkt["dst_ip"]) == (HOST_MAC, MAC, IP, HOST_IP)
        assert (pkt["sport"], pkt["dport"]) == (SPORT, ROCE_PORT)
        if pkt["op"] == ROCE_CNP:
            self.cnps.append(pkt)
            return
        assert pkt["dqpn"] == HOST_QPN
        psn = pkt["psn"]
        def before(a, b): # a < b in PSN order.
            return 0 < ((b - a) & 0xffffff) < 0x800000
        if self.done():
            return
        m, first, (op, _, _), n = self.packets[self.una]
        if pkt["op"] == ROCE_ACK and pkt["syn"] == ROCE_NAK_SEQUENCE:
            # Packets before ``psn`` done (writes), go back to ``psn``.
            while not self.done() and before(self.packets[self.una][1], psn) and self.packets[self.una][0].kind == "write":
                self.complete(ROCE_SYN_ACK)
            self.go_back()
        elif pkt["op"] == ROCE_ACK and pkt["syn"] != ROCE_SYN_ACK:
            # Error of the packet ``psn``: its message ends with this error. A real requester
            # moves its queue pair to the error state; the target skips the packet, and the tests
            # go on with the next messages.
            while not self.done() and before(self.packets[self.una][1], psn) and self.packets[self.una][0].kind == "write":
                self.complete(ROCE_SYN_ACK)
            if not self.done():
                m = self.packets[self.una][0]
                m.result = pkt["syn"]
                while not self.done() and self.packets[self.una][0] is m:
                    self.una += 1
                self.go_back()
        elif pkt["op"] == ROCE_ACK:
            # Coalesced ACK: the writes up to ``psn``; a read before it not answered is missing.
            while not self.done() and not before(psn, self.packets[self.una][1]):
                if self.packets[self.una][0].kind == "read":
                    if not self.back_pending:
                        self.go_back()
                    return
                self.complete(ROCE_SYN_ACK)
        else:
            if m.kind != "read" or op != ROCE_READ_REQUEST:
                if not self.back_pending:
                    self.go_back() # Response of a read while a write is expected: an ACK is missing.
                return
            expected = (first + self.got_responses) & 0xffffff
            if psn == expected:
                if self.got_responses == 0:
                    m.got = bytearray()
                m.got += pkt["payload"]
                self.got_responses += 1
                self.idle = 0
                self.back_pending = False
                if self.got_responses == n:
                    self.complete(pkt["syn"])
            elif before(expected, psn) and not self.back_pending:
                self.go_back() # Response after a missing one.

    def tick(self):
        self.idle += 1
        if self.idle > self.timeout and not self.done():
            self.idle = 0
            self.go_back()

# DUT ----------------------------------------------------------------------------------------------

class RoCEDUT(DUT):
    def __init__(self, topology=None, **kwargs):
        DUT.__init__(self, topology or two_tier(2), size=4*2**WINDOW_BITS)
        self.pool = FabricPool(self.nodes[0], window_bits=WINDOW_BITS, chunk=16, busy_wait=16)
        self.roce = FabricRoCE(self.pool, mac=MAC, ip=IP, qpn_base=QPN, qps=2, mrs=4, pt_entries=16,
            page_bits=8, pmtu=PMTU, cnp_interval=2000, **kwargs)

def configure(dut, pages=PAGES, extra=()):
    """Queue pair 0 for the host, region RKEY (read and write) on the pages of node 1, region
    RO_RKEY (read only) on the same pages."""
    roce = dut.roce
    yield roce.qp_cfg.index.eq(0)
    yield roce.qp_cfg.valid.eq(1)
    yield roce.qp_cfg.dqpn.eq(HOST_QPN)
    yield roce.qp_cfg.mac.eq(HOST_MAC)
    yield roce.qp_cfg.ip.eq(HOST_IP)
    yield roce.qp_cfg.sport.eq(SPORT)
    yield roce.qp_cfg.epsn.eq(0x123456)
    yield roce.qp_cfg.we.eq(1)
    yield
    yield roce.qp_cfg.we.eq(0)
    for i, page in enumerate(pages):
        yield roce.pt_cfg.adr.eq(i)
        yield roce.pt_cfg.dat.eq(page)
        yield roce.pt_cfg.we.eq(1)
        yield
    for i, node in extra:
        yield roce.pt_cfg.adr.eq(i)
        yield roce.pt_cfg.dat.eq(node)
        yield roce.pt_cfg.we.eq(1)
        yield
    yield roce.pt_cfg.we.eq(0)
    for index, (rkey, access, length) in enumerate([(RKEY, 3, REGION), (RO_RKEY, 1, REGION)]):
        yield roce.mr_cfg.index.eq(index)
        yield roce.mr_cfg.valid.eq(1)
        yield roce.mr_cfg.rkey.eq(rkey)
        yield roce.mr_cfg.va.eq(VA)
        yield roce.mr_cfg.length.eq(length)
        yield roce.mr_cfg.access.eq(access)
        yield roce.mr_cfg.base.eq(0)
        yield roce.mr_cfg.we.eq(1)
        yield
    yield roce.mr_cfg.we.eq(0)
    yield

def set_pages(pages, node=1):
    """Page table entries: global word address of the pages."""
    return [pool_address(node, p*PAGE//4, WINDOW_BITS) for p in pages]

def region_word(dut, offset):
    """Word at byte ``offset`` of the region, from the memory of node 1."""
    page, rest = divmod(offset, PAGE)
    return (yield dut.mems[1].mem[PAGES[page]*PAGE//4 + rest//4])

def region_bytes(dut, offset, length):
    data = bytearray()
    for o in range(offset, offset + length, 4):
        data += (yield from region_word(dut, o)).to_bytes(4, "little")
    return bytes(data)

def exchange(test, dut, requester, max_cycles=400000, ready=None, gap=0):
    """Runs the requester against the target until all its messages are completed. ``gap``:
    cycles at least from the start of a frame to the start of the next one (rate of the
    requester); 0: back to back."""
    sink, source = dut.roce.sink, dut.roce.source
    out   = []    # Bytes of the frame being sent.
    frame = []    # Bytes of the frame being received.
    ready_now = 0
    since = gap
    rng = random.Random(7)
    for cycle in range(max_cycles):
        # Frame from the target: byte taken at this cycle when valid and ready.
        if ready_now and (yield source.valid):
            frame.append((yield source.data))
            if (yield source.last):
                requester.receive(frame)
                frame = []
        ready_now = 1 if ready is None else int(rng.random() < ready)
        yield source.ready.eq(ready_now)
        # Frames to the target.
        since += 1
        while not out and since >= gap:
            f, more = requester.next_frame()
            if not more:
                break
            since = 0 # A lost frame takes its time too.
            if f is not None:
                out = list(f)
        if out:
            yield sink.data.eq(out.pop(0))
            yield sink.valid.eq(1)
            yield sink.last.eq(len(out) == 0)
        else:
            yield sink.valid.eq(0)
            yield sink.last.eq(0)
        requester.tick()
        if requester.done() and not out and not frame:
            break
        yield
    else:
        raise TimeoutError(f"Requester not done: {requester.una}/{len(requester.packets)} completed.")
    yield sink.valid.eq(0)
    yield sink.last.eq(0)
    yield source.ready.eq(1)
    for _ in range(50):
        yield

def pattern(length, salt=0):
    return bytes((i*7 + salt*13 + (i >> 8)) & 0xff for i in range(length))

# Test RoCE ----------------------------------------------------------------------------------------

class TestFabricRoCE(unittest.TestCase):
    def test_icrc_model(self):
        # Register of the model: the one of zlib (CRC-32), not inverted.
        for data in [b"", b"a", bytes(range(256)), pattern(1000)]:
            crc = 0xffffffff
            for byte in data:
                crc = crc32_update(crc, byte)
            self.assertEqual(crc ^ 0xffffffff, zlib.crc32(data))
        # ICRC: masked fields do not change it, the others do.
        frame = roce_packet(ROCE_WRITE_ONLY, 5, bytes(8), reth=(VA, RKEY, 8))[:-4]
        icrc  = roce_icrc(frame)
        for offset in ROCE_ICRC_MASKED + tuple(range(14)):
            other = list(frame)
            other[offset] ^= 0x5a
            self.assertEqual(roce_icrc(other), icrc)
        for offset in (14, 16, 23, 30, 36, 42, 47, 53, 54, 70):
            other = list(frame)
            other[offset] ^= 0x5a
            self.assertNotEqual(roce_icrc(other), icrc)

    def test_write_read(self):
        # Writes and reads of one and several packets, through the page table of the region.
        dut = RoCEDUT()
        data  = pattern(REGION)
        small = pattern(40, salt=3)
        messages = [
            Message("write", VA, data),
            Message("read",  VA, length=REGION),
            Message("write", VA + PAGE - 16, small),      # Over two pages.
            Message("read",  VA + PAGE - 16, length=40),
            Message("read",  VA, length=0),
            Message("read",  VA + 2*PMTU + 4, length=PMTU), # Two responses.
        ]
        def generator():
            yield from configure(dut, pages=set_pages(PAGES))
            # Paced so that the target takes every packet (two waiting at most; a read of 1 KiB takes about 7000 cycles).
            requester = Requester(messages, timeout=40000)
            yield from exchange(self, dut, requester, gap=5000)
            for m in messages:
                self.assertEqual(m.result, ROCE_SYN_ACK)
            self.assertEqual(bytes(messages[1].got), data)
            self.assertEqual(bytes(messages[3].got), small)
            self.assertEqual(bytes(messages[4].got), b"")
            expected = bytearray(data)
            expected[PAGE - 16:PAGE + 24] = small
            self.assertEqual(bytes(messages[5].got), bytes(expected[2*PMTU + 4:3*PMTU + 4]))
            self.assertEqual((yield from region_bytes(dut, 0, REGION)), bytes(expected))
            # Page 0 of the region is page 3 of node 1, the others in the memory too.
            self.assertEqual((yield dut.mems[1].mem[3*PAGE//4]), int.from_bytes(data[0:4], "little"))
            self.assertEqual((yield dut.roce.writes), 2)
            self.assertEqual((yield dut.roce.reads), 4)
            self.assertEqual((yield dut.roce.acks), 2)
            for counter in ["naks", "duplicates", "icrc_errors", "busy_drops", "qp_drops", "ignored", "cnps"]:
                self.assertEqual((yield getattr(dut.roce, counter)), 0, counter)
            self.assertEqual(requester.retransmits, 0)
        run(dut, [generator()])

    def test_loss(self):
        # Packets lost both ways: the requester goes back and the data is right in the end.
        dut = RoCEDUT()
        data = pattern(REGION, salt=1)
        messages = [
            Message("write", VA, data),
            Message("read",  VA, length=REGION),
            Message("write", VA + 64, pattern(64, salt=2)),
            Message("read",  VA + 64, length=64),
        ]
        def generator():
            yield from configure(dut, pages=set_pages(PAGES))
            # Lost: write packet 1 (the next ones get a PSN sequence error NAK), the second read
            # response, the ACK of the second write (sent again after a timeout: duplicate).
            requester = Requester(messages, drop_tx=[1], drop_rx=[2, 6], timeout=4000)
            yield from exchange(self, dut, requester)
            for m in messages:
                self.assertEqual(m.result, ROCE_SYN_ACK)
            self.assertEqual(bytes(messages[1].got), data)
            self.assertEqual(bytes(messages[3].got), pattern(64, salt=2))
            expected = bytearray(data)
            expected[64:128] = pattern(64, salt=2)
            self.assertEqual((yield from region_bytes(dut, 0, REGION)), bytes(expected))
            self.assertEqual((yield dut.roce.writes), 2)
            self.assertGreaterEqual(requester.retransmits, 3)
            self.assertGreaterEqual((yield dut.roce.naks), 1)
            self.assertGreaterEqual((yield dut.roce.duplicates), 1)
        run(dut, [generator()])

    def test_back_to_back(self):
        # Many messages in flight without gaps: frames dropped while two wait (busy) are sent
        # again by the requester.
        dut = RoCEDUT()
        rng = random.Random(3)
        expected = bytearray(REGION)
        messages = []
        for i in range(12):
            offset = 4*rng.randrange(0, (REGION - 96)//4)
            if rng.random() < 0.5:
                d = pattern(4*rng.randrange(1, 24), salt=i)
                expected[offset:offset + len(d)] = d
                messages.append(Message("write", VA + offset, d))
            else:
                messages.append(Message("read", VA + offset, length=4*rng.randrange(1, 24)))
        snapshots = []
        state = bytearray(REGION)
        for m in messages:
            o = m.va - VA
            if m.kind == "write":
                state[o:o + m.length] = m.data
            else:
                snapshots.append(bytes(state[o:o + m.length]))
        def generator():
            yield from configure(dut, pages=set_pages(PAGES))
            requester = Requester(messages, window=8, timeout=3000)
            yield from exchange(self, dut, requester, max_cycles=600000)
            reads = [m for m in messages if m.kind == "read"]
            for m, snapshot in zip(reads, snapshots):
                self.assertEqual(bytes(m.got), snapshot)
            for m in messages:
                self.assertEqual(m.result, ROCE_SYN_ACK)
            self.assertEqual((yield from region_bytes(dut, 0, REGION)), bytes(expected))
            self.assertGreater((yield dut.roce.busy_drops), 0)
        run(dut, [generator()])

    def test_errors(self):
        # NAKs: R_Key, range, access, alignment, opcode, fabric error; bad ICRC and unknown queue
        # pair dropped.
        dut = RoCEDUT()
        def generator():
            # Page 4 of the region (after its end) in a node that does not exist.
            yield from configure(dut, pages=set_pages(PAGES), extra=[(4, pool_address(9, 0, WINDOW_BITS))])
            messages = [
                Message("write", VA, pattern(8), rkey=0x1),                   # Unknown R_Key.
                Message("read",  VA + REGION - 4, length=8),                  # Past the end.
                Message("read",  VA - 4, length=4),                           # Before the start.
                Message("write", VA, pattern(8), rkey=RO_RKEY),               # Read-only region.
                Message("read",  VA + 2, length=4),                           # Not aligned.
                Message("raw",   VA, b"\0"*4, opcode=0x04),                   # SEND ONLY.
                Message("read",  VA, length=8, rkey=RO_RKEY),                 # Fine.
            ]
            requester = Requester(messages, window=1)
            yield from exchange(self, dut, requester)
            self.assertEqual([m.result for m in messages], [ROCE_NAK_ACCESS, ROCE_NAK_ACCESS,
                ROCE_NAK_ACCESS, ROCE_NAK_ACCESS, ROCE_NAK_INVALID, ROCE_NAK_INVALID, ROCE_SYN_ACK])
            self.assertEqual((yield dut.mems[1].mem[3*PAGE//4]), 0)
            self.assertEqual((yield dut.roce.naks), 6)
            # Region made longer over the page of node 9: fabric error.
            yield dut.roce.mr_cfg.index.eq(0)
            yield dut.roce.mr_cfg.valid.eq(1)
            yield dut.roce.mr_cfg.rkey.eq(RKEY)
            yield dut.roce.mr_cfg.va.eq(VA)
            yield dut.roce.mr_cfg.length.eq(REGION + PAGE)
            yield dut.roce.mr_cfg.access.eq(3)
            yield dut.roce.mr_cfg.we.eq(1)
            yield
            yield dut.roce.mr_cfg.we.eq(0)
            psn = 0x123456 + 7
            messages = [Message("read", VA + REGION - 8, length=16), Message("write", VA + REGION, pattern(4))]
            requester = Requester(messages, psn=psn, window=1)
            yield from exchange(self, dut, requester)
            self.assertEqual([m.result for m in messages], [ROCE_NAK_OPERATION, ROCE_NAK_OPERATION])
            # Bad ICRC, unknown queue pair, other UDP port, other IP: no answer.
            sink = dut.roce.sink
            frames = [
                roce_packet(ROCE_READ_REQUEST, 0x12345f, reth=(VA, RKEY, 4)),
                roce_packet(ROCE_READ_REQUEST, 0x12345f, reth=(VA, RKEY, 4), dqpn=QPN + 1),
                roce_packet(ROCE_READ_REQUEST, 0x12345f, reth=(VA, RKEY, 4), dqpn=QPN + 9),
                roce_packet(ROCE_READ_REQUEST, 0x12345f, reth=(VA, RKEY, 4), dport=4792),
                roce_packet(ROCE_READ_REQUEST, 0x12345f, reth=(VA, RKEY, 4), dst_ip=IP + 1),
            ]
            frames[0][58] ^= 0x10
            for frame in frames:
                for i, byte in enumerate(frame):
                    yield sink.data.eq(byte)
                    yield sink.valid.eq(1)
                    yield sink.last.eq(i == len(frame) - 1)
                    yield
                yield sink.valid.eq(0)
                yield sink.last.eq(0)
                for _ in range(20):
                    yield
            for _ in range(500):
                self.assertEqual((yield dut.roce.source.valid), 0)
                yield
            self.assertEqual((yield dut.roce.icrc_errors), 1)
            self.assertEqual((yield dut.roce.qp_drops), 2)
            self.assertEqual((yield dut.roce.ignored), 2)
        run(dut, [generator()])

    def test_cnp(self):
        # Packets marked CE: one CNP to the requester per interval.
        dut = RoCEDUT()
        def generator():
            yield from configure(dut, pages=set_pages(PAGES))
            messages = [Message("write", VA + 4*i, pattern(4, salt=i)) for i in range(4)]
            requester = Requester(messages, window=1, ecn_tx=[0, 1, 2, 3])
            yield from exchange(self, dut, requester)
            self.assertGreaterEqual(len(requester.cnps), 1)
            self.assertLess(len(requester.cnps), 4)
            cnp = requester.cnps[0]
            self.assertEqual((cnp["dqpn"], cnp["psn"], cnp["payload"]), (HOST_QPN, 0, bytes(16)))
            self.assertEqual((yield dut.roce.cnps), len(requester.cnps))
            for m in messages:
                self.assertEqual(m.result, ROCE_SYN_ACK)
        run(dut, [generator()])

    def test_slow_network_card(self):
        # Frames from the target taken one byte in three on average.
        dut = RoCEDUT()
        data = pattern(2*PMTU + 8, salt=5)
        messages = [Message("write", VA + 8, data), Message("read", VA + 8, length=len(data))]
        def generator():
            yield from configure(dut, pages=set_pages(PAGES))
            requester = Requester(messages, timeout=40000)
            yield from exchange(self, dut, requester, ready=0.3, gap=5000)
            self.assertEqual(bytes(messages[1].got), data)
            self.assertEqual(requester.retransmits, 0)
        run(dut, [generator()])

if __name__ == "__main__":
    unittest.main()
