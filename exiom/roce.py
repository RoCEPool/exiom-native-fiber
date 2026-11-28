#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
RoCE target: RDMA READ and WRITE from commodity RoCEv2 network cards, run in the pool.

A memory server of the centralized design faces the GPU servers through their own RDMA network
cards: those cards are the requesters, this module is the responder of their Reliable Connected
(RC) queue pairs. A READ or WRITE names a memory region (R_Key) and a virtual address in it; the
page table of the region gives the global address of each page in the pool (``FabricPool``), so
the pages of a region can be spread over the nodes of the fabric.

Byte streams with ``last`` (frames without preamble and FCS, as on the user side of a MAC), like
``FabricFrame``.

Wire format (IBA Annex A17, RoCEv2 over IPv4):

    0   Ethernet (14): destination MAC, source MAC, EtherType 0x0800
    14  IPv4 (20, no options), protocol 17
    34  UDP (8): destination port 4791
    42  BTH (12): opcode, flags (pad count in [5:4]), P_Key, reserved, destination QP, AckReq, PSN
    54  RETH (16, WRITE FIRST/ONLY and READ REQUEST): virtual address, R_Key, DMA length
        AETH (4, ACK, NAK and READ RESPONSE FIRST/LAST/ONLY): syndrome, MSN
    ..  payload
    -4  ICRC: CRC-32 of the IP header to the end of the payload, with the variant fields masked
        (IP TOS, TTL and checksum, UDP checksum, BTH reserved byte) and 8 bytes 0xff in front,
        least significant byte first.

Handled (first version):

- Opcodes: RDMA WRITE FIRST/MIDDLE/LAST/ONLY, RDMA READ REQUEST; answered with ACK, NAK and READ
  RESPONSE FIRST/MIDDLE/LAST/ONLY. Other opcodes are answered with a NAK "invalid request".
- PSN checks: a packet after a missing one gets one NAK "PSN sequence error" (the requester goes
  back to the missing packet, go-back-N); a duplicate WRITE is acknowledged again without being
  written; a duplicate READ REQUEST is run again (the requester asks it again when a response is
  lost).
- ICRC checked on receive (bad packets dropped and counted), computed on send. IPv4 header
  checksum computed on send; the UDP checksum is 0 (allowed by IPv4).
- ECN: a packet received with CE marks makes a CNP to its requester (congestion notification
  point of DCQCN), at most one every ``cnp_interval`` cycles.
- Memory regions: R_Key, base virtual address, length, access (bit 0 read, bit 1 write), first
  entry in the page table, protection domain. Pages of ``2**page_bits`` bytes. A queue pair only
  reaches the regions of its protection domain (tenant): the others are unknown R_Keys to it.

Limits of this first version (the throughput is not the goal yet, the data path is one byte per
cycle and the pool does one word at a time):

- Addresses and lengths multiple of 4 bytes (words of the fabric; byte ``a`` of the region is
  lane ``a % 4`` of its word, little-endian). Others are answered with a NAK "invalid request".
- No atomics, no SEND, no immediate data, no connection manager (the host programs the queue
  pairs), no requester side.
- Two frames are buffered: a frame coming while two are waiting is dropped (``busy_drops``) and the
  requester sends it again (PSN sequence error or timeout).

Configuration (one write per cycle, ``we`` for one cycle):

- ``qp_cfg``: queue pair ``index`` (QPN ``qpn_base + index``): valid, destination QPN, MAC, IP and
  UDP source port of the requester, expected PSN, protection domain. Clears its MSN and its message state.
- ``mr_cfg``: memory region ``index``: valid, R_Key, virtual address, length (bytes), access, first
  page table entry, protection domain.
- ``pt_cfg``: page table entry ``adr``: global word address of the page.
"""

from migen import *

from litex.gen import *

from litex.soc.interconnect import stream

from exiom.fabric import *
from exiom.pool   import *

# Constants ----------------------------------------------------------------------------------------

ROCE_PORT = 4791

# BTH opcodes (Reliable Connected).
ROCE_WRITE_FIRST     = 0x06
ROCE_WRITE_MIDDLE    = 0x07
ROCE_WRITE_LAST      = 0x08
ROCE_WRITE_ONLY      = 0x0a
ROCE_READ_REQUEST    = 0x0c
ROCE_READ_RESP_FIRST = 0x0d
ROCE_READ_RESP_MID   = 0x0e
ROCE_READ_RESP_LAST  = 0x0f
ROCE_READ_RESP_ONLY  = 0x10
ROCE_ACK             = 0x11
ROCE_CNP             = 0x81

# AETH syndromes.
ROCE_SYN_ACK       = 0x1f # ACK, credits not given.
ROCE_NAK_SEQUENCE  = 0x60 # PSN sequence error.
ROCE_NAK_INVALID   = 0x61 # Invalid request.
ROCE_NAK_ACCESS    = 0x62 # Remote access error.
ROCE_NAK_OPERATION = 0x63 # Remote operational error.

# Header bytes.
ROCE_HEADER      = 54 # Ethernet, IPv4, UDP, BTH.
ROCE_HEADER_RETH = 70
ROCE_HEADER_AETH = 58

# Bytes of the IP header to the BTH masked in the ICRC (frame offsets).
ROCE_ICRC_MASKED = (15, 22, 24, 25, 40, 41, 46)

# CRC-32 -------------------------------------------------------------------------------------------

CRC32_POLY = 0xedb88320 # Reflected.

def crc32_update(crc, byte):
    """CRC-32 register (reflected, not inverted) after one byte."""
    crc ^= byte
    for _ in range(8):
        crc = (crc >> 1) ^ (CRC32_POLY if crc & 1 else 0)
    return crc

ROCE_ICRC_SEED = 0xffffffff
for _ in range(8):
    ROCE_ICRC_SEED = crc32_update(ROCE_ICRC_SEED, 0xff) # The 8 bytes 0xff in front.

def crc32_next(crc, byte):
    """Expression of the CRC-32 register after ``byte`` (8 bits), XOR network."""
    # Each bit of the register is a XOR of bits of the old register (0-31) and of the byte (32-39).
    bits = [1 << i for i in range(32)]
    for i in range(8):
        bits[i] ^= 1 << (32 + i)
    for _ in range(8):
        lsb  = bits[0]
        bits = bits[1:] + [0]
        for j in range(32):
            if (CRC32_POLY >> j) & 1:
                bits[j] ^= lsb
    inputs = [crc[i] for i in range(32)] + [byte[i] for i in range(8)]
    out = []
    for mask in bits:
        terms = [inputs[i] for i in range(40) if (mask >> i) & 1]
        expr  = terms[0]
        for term in terms[1:]:
            expr = expr ^ term
        out.append(expr)
    return Cat(*out)

def roce_icrc(frame):
    """ICRC (4 bytes, as sent) of a frame without its ICRC (software model)."""
    crc = ROCE_ICRC_SEED
    for i, byte in enumerate(frame[14:], start=14):
        crc = crc32_update(crc, 0xff if i in ROCE_ICRC_MASKED else byte)
    return list((crc ^ 0xffffffff).to_bytes(4, "little"))

# Layouts ------------------------------------------------------------------------------------------

def roce_frame_layout():
    return [("data", 8)]

def qp_cfg_layout():
    return [("we", 1), ("index", 8), ("valid", 1), ("dqpn", 24), ("mac", 48), ("ip", 32),
        ("sport", 16), ("epsn", 24), ("pd", 8)]

def mr_cfg_layout(abits):
    return [("we", 1), ("index", 8), ("valid", 1), ("rkey", 32), ("va", 64), ("length", 64),
        ("access", 2), ("base", 16), ("pd", 8)]

def pt_cfg_layout(abits):
    return [("we", 1), ("adr", 16), ("dat", abits), ("resident", 1)]

def dirty_cfg_layout():
    return [("we", 1), ("adr", 16)]

def miss_layout():
    return [("entry", 16)]

def packet_layout():
    """Header fields of a received packet, with its buffer slot and length."""
    return [("slot", 1), ("length", 16), ("opcode", 8), ("pad", 2), ("ackreq", 1), ("dqpn", 24),
        ("psn", 24), ("va", 64), ("rkey", 32), ("dlen", 32), ("ce", 1)]

# RoCE Target --------------------------------------------------------------------------------------

class FabricRoCE(LiteXModule):
    """RC responder of RoCEv2 READ and WRITE, run with ``pool`` (FabricPool, used by this module
    only).

    ``sink``: frames from the network (always accepted). ``source``: frames to the network.
    """
    def __init__(self, pool, mac, ip, qpn_base=0x100, qps=4, mrs=4, pt_entries=256, page_bits=12,
        pmtu=1024, cnp_interval=1024):
        assert pmtu in (256, 512, 1024, 2048, 4096)
        assert 2 <= page_bits <= 32 and qps >= 1 and mrs >= 1
        self.pmtu = pmtu
        self.sink   = sink   = stream.Endpoint(roce_frame_layout())
        self.source = source = stream.Endpoint(roce_frame_layout())

        abits = len(pool.sink.adr)
        self.qp_cfg = qp_cfg = Record(qp_cfg_layout())
        self.mr_cfg = mr_cfg = Record(mr_cfg_layout(abits))
        self.pt_cfg = pt_cfg = Record(pt_cfg_layout(abits))
        self.dirty_cfg = dirty_cfg = Record(dirty_cfg_layout())

        # Status.
        self.rx_packets  = rx_packets  = Signal(32) # Packets for this target with a good ICRC.
        self.ignored     = ignored     = Signal(32) # Frames that are not RoCEv2 for this target.
        self.icrc_errors = icrc_errors = Signal(32) # RoCEv2 for this target, bad ICRC: dropped.
        self.busy_drops  = busy_drops  = Signal(32) # Dropped: two frames already waiting.
        self.qp_drops    = qp_drops    = Signal(32) # Dropped: queue pair that does not exist.
        self.duplicates  = duplicates  = Signal(32) # Duplicate packets (PSN already received).
        self.writes      = writes      = Signal(32) # WRITE messages done.
        self.reads       = reads       = Signal(32) # READ requests answered (duplicates too).
        self.acks        = acks        = Signal(32) # ACKs sent.
        self.naks        = naks        = Signal(32) # NAKs sent.
        self.cnps        = cnps        = Signal(32) # CNPs sent.
        self.misses      = misses      = Signal(32) # Packets dropped on a page not resident.
        self.packets     = packets     = Signal(32) # Packets done (answered or dropped).
        self.idle        = Signal()                 # No packet being processed.

        # # #

        buf_bytes  = 1 << (pmtu + ROCE_HEADER_RETH + 4 - 1).bit_length() # Frame buffer slot.
        buf_bits   = log2_int(buf_bytes)
        pmtu_bits  = log2_int(pmtu)
        pt_bits    = max(1, (pt_entries - 1).bit_length())
        qp_bits    = max(1, (qps - 1).bit_length())
        mr_bits    = max(1, (mrs - 1).bit_length())

        # Tables.
        # -------
        def table(n, bits, name):
            return Array(Signal(bits, name=f"{name}{i}") for i in range(n))
        qp_valid = table(qps, 1,  "qp_valid")
        qp_dqpn  = table(qps, 24, "qp_dqpn")
        qp_mac   = table(qps, 48, "qp_mac")
        qp_ip    = table(qps, 32, "qp_ip")
        qp_sport = table(qps, 16, "qp_sport")
        qp_epsn  = table(qps, 24, "qp_epsn")  # Expected PSN.
        qp_pd    = table(qps, 8,  "qp_pd")    # Protection domain (tenant).
        qp_msn   = table(qps, 24, "qp_msn")   # Messages done.
        qp_nak   = table(qps, 1,  "qp_nak")   # PSN sequence error NAK sent, not yet resolved.
        qp_wact  = table(qps, 1,  "qp_wact")  # WRITE message in progress (FIRST received).
        qp_wva   = table(qps, 64, "qp_wva")   # Its next virtual address.
        qp_wleft = table(qps, 32, "qp_wleft") # Its bytes still to come.
        qp_wmr   = table(qps, mr_bits, "qp_wmr")

        mr_valid  = table(mrs, 1,  "mr_valid")
        mr_rkey   = table(mrs, 32, "mr_rkey")
        mr_va     = table(mrs, 64, "mr_va")
        mr_length = table(mrs, 64, "mr_length")
        mr_access = table(mrs, 2,  "mr_access")
        mr_base   = table(mrs, 16, "mr_base")
        mr_pd     = table(mrs, 8,  "mr_pd")

        self.sync += [
            If(qp_cfg.we,
                qp_valid[qp_cfg.index].eq(qp_cfg.valid),
                qp_dqpn[qp_cfg.index].eq(qp_cfg.dqpn),
                qp_mac[qp_cfg.index].eq(qp_cfg.mac),
                qp_ip[qp_cfg.index].eq(qp_cfg.ip),
                qp_sport[qp_cfg.index].eq(qp_cfg.sport),
                qp_epsn[qp_cfg.index].eq(qp_cfg.epsn),
                qp_pd[qp_cfg.index].eq(qp_cfg.pd),
                qp_msn[qp_cfg.index].eq(0),
                qp_nak[qp_cfg.index].eq(0),
                qp_wact[qp_cfg.index].eq(0),
            ),
            If(mr_cfg.we,
                mr_valid[mr_cfg.index].eq(mr_cfg.valid),
                mr_rkey[mr_cfg.index].eq(mr_cfg.rkey),
                mr_va[mr_cfg.index].eq(mr_cfg.va),
                mr_length[mr_cfg.index].eq(mr_cfg.length),
                mr_access[mr_cfg.index].eq(mr_cfg.access),
                mr_base[mr_cfg.index].eq(mr_cfg.base),
                mr_pd[mr_cfg.index].eq(mr_cfg.pd),
            ),
        ]

        pt      = Memory(abits + 1, pt_entries) # Resident bit, global word address of the page.
        pt_wr   = pt.get_port(write_capable=True)
        pt_rd   = pt.get_port()
        self.specials += pt, pt_wr, pt_rd
        self.comb += [
            pt_wr.we.eq(pt_cfg.we),
            pt_wr.adr.eq(pt_cfg.adr),
            pt_wr.dat_w.eq(Cat(pt_cfg.dat, pt_cfg.resident)),
        ]

        # Pages written since the host cleared their bit (a write clears after: it stays set).
        self.dirty = dirty = Array(Signal(name=f"dirty{i}") for i in range(pt_entries))
        self.sync += If(dirty_cfg.we, dirty[dirty_cfg.adr].eq(0))

        # Pages not resident met: entries for the host, which brings them in and makes them
        # resident; the requester sends the packet again (timeout or sequence NAK).
        self.miss_fifo = miss_fifo = stream.SyncFIFO(miss_layout(), 8)
        self.miss = miss_fifo.source

        # Receive: frame bytes into a buffer slot, header fields, ICRC.
        # --------------------------------------------------------------
        rxbuf    = Memory(8, 2*buf_bytes)
        rxbuf_wr = rxbuf.get_port(write_capable=True)
        rxbuf_rd = rxbuf.get_port()
        self.specials += rxbuf, rxbuf_wr, rxbuf_rd

        self.fifo = fifo = stream.SyncFIFO(packet_layout(), 2, buffered=False)

        count  = Signal(16)             # Byte index in the frame.
        hdr    = Signal(8*ROCE_HEADER_RETH) # Byte k in [8k:8k+8].
        delay  = Array(Signal(8, name=f"rx_delay{i}") for i in range(4)) # Last bytes, 0 newest.
        crc    = Signal(32, reset=ROCE_ICRC_SEED)
        wslot  = Signal()               # Slot of the next frame taken.
        taking = Signal()               # This frame is written in the buffer.
        take   = Signal()
        self.comb += take.eq(Mux(count == 0, fifo.sink.ready, taking))

        def be(first, length, bits=hdr):
            """Bytes ``first`` to ``first + length - 1`` (big-endian value)."""
            return Cat(*[bits[8*(first + length - 1 - i):8*(first + length - i)] for i in range(length)])

        # CRC of the byte received 4 bytes ago (the last 4 bytes of a frame are its ICRC).
        fed     = Signal(16)
        fed_byte = Signal(8)
        crc_in  = Signal(32)
        self.comb += [
            fed.eq(count - 4),
            fed_byte.eq(Mux(reduce(or_, [fed == m for m in ROCE_ICRC_MASKED]), 0xff, delay[3])),
            crc_in.eq(Mux((count >= 4 + 14), crc32_next(crc, fed_byte), crc)),
        ]
        icrc_ok = Signal()
        self.comb += icrc_ok.eq(Cat(delay[2], delay[1], delay[0], sink.data) == (crc_in ^ 0xffffffff))

        mine = Signal()
        self.comb += mine.eq(
            (be(0, 6) == mac) & (be(12, 2) == 0x0800) & (be(14, 1) == 0x45) & (be(23, 1) == 17) &
            (be(30, 4) == ip) & (be(36, 2) == ROCE_PORT) &
            (count + 1 == 14 + be(16, 2)) & (count + 1 >= ROCE_HEADER + 4))

        self.comb += [
            sink.ready.eq(1),
            rxbuf_wr.adr.eq(Cat(count[:buf_bits], wslot)),
            rxbuf_wr.dat_w.eq(sink.data),
            rxbuf_wr.we.eq(sink.valid & take & (count < buf_bytes)),
            fifo.sink.slot.eq(wslot),
            fifo.sink.length.eq(count + 1),
            fifo.sink.opcode.eq(be(42, 1)),
            fifo.sink.pad.eq(hdr[8*43 + 4:8*43 + 6]),
            fifo.sink.ackreq.eq(hdr[8*50 + 7]),
            fifo.sink.dqpn.eq(be(47, 3)),
            fifo.sink.psn.eq(be(51, 3)),
            fifo.sink.va.eq(be(54, 8)),
            fifo.sink.rkey.eq(be(62, 4)),
            fifo.sink.dlen.eq(be(66, 4)),
            fifo.sink.ce.eq(hdr[8*15:8*15 + 2] == 0b11),
        ]
        # Frames without RETH are shorter: their RETH fields (payload or older bytes) are not used.
        self.sync += If(sink.valid,
            Case(count, {k: hdr[8*k:8*k + 8].eq(sink.data) for k in range(ROCE_HEADER_RETH)}),
            delay[0].eq(sink.data), delay[1].eq(delay[0]), delay[2].eq(delay[1]), delay[3].eq(delay[2]),
            crc.eq(crc_in),
            If(count == 0, taking.eq(fifo.sink.ready)),
            If(count != 0xffff, count.eq(count + 1)),
            If(sink.last,
                count.eq(0),
                crc.eq(ROCE_ICRC_SEED),
                If(~mine | (count >= buf_bytes),
                    ignored.eq(ignored + 1)
                ).Elif(~icrc_ok,
                    icrc_errors.eq(icrc_errors + 1)
                ).Elif(~take,
                    busy_drops.eq(busy_drops + 1)
                ).Else(
                    rx_packets.eq(rx_packets + 1),
                    wslot.eq(~wslot)
                )
            )
        )
        # ``mine`` and the fields above use ``hdr``: the bytes before the last one (the last one is
        # an ICRC byte, never a header byte, for the frames that are accepted).
        self.comb += fifo.sink.valid.eq(sink.valid & sink.last & mine & (count < buf_bytes) & icrc_ok & take)

        # Transmit.
        # ---------
        txbuf    = Memory(32, pmtu//4)
        txbuf_wr = txbuf.get_port(write_capable=True)
        txbuf_rd = txbuf.get_port()
        self.specials += txbuf, txbuf_wr, txbuf_rd

        # Parameters, held by the processing FSM while ``tx_start``.
        s_op    = Signal(8)
        s_psn   = Signal(24)
        s_aeth  = Signal()
        s_syn   = Signal(8)
        s_msn   = Signal(24)
        s_words = Signal(max=pmtu//4 + 1)
        s_cnp   = Signal()
        s_qp    = Signal(qp_bits)
        tx_start = Signal()
        tx_done  = Signal()

        paylen = Signal(16)
        hlen   = Signal(8)
        iplen  = Signal(16)
        csum   = Signal(20)
        csum1  = Signal(17)
        csum2  = Signal(16)
        dip    = Signal(32)
        dmac   = Signal(48)
        dqpn   = Signal(24)
        sport  = Signal(16)
        udplen = Signal(16)
        bpsn   = Signal(24)
        self.comb += [
            dmac.eq(qp_mac[s_qp]),
            dqpn.eq(qp_dqpn[s_qp]),
            sport.eq(qp_sport[s_qp]),
            udplen.eq(iplen - 20),
            bpsn.eq(Mux(s_cnp, 0, s_psn)),
            paylen.eq(Mux(s_cnp, 16, s_words << 2)),
            hlen.eq(Mux(s_aeth, ROCE_HEADER_AETH, ROCE_HEADER)),
            iplen.eq(hlen - 14 + paylen + 4),
            dip.eq(qp_ip[s_qp]),
            csum.eq(0x4502 + iplen + 0x4000 + 0x4011 + (ip >> 16) + (ip & 0xffff) + dip[16:] + dip[:16]),
            csum1.eq(csum[:16] + csum[16:]),
            csum2.eq(~(csum1[:16] + csum1[16:])),
        ]
        hdr_bytes = []
        def put(value, length):
            for i in range(length):
                hdr_bytes.append(Constant((value >> 8*(length - 1 - i)) & 0xff, 8) if isinstance(value, int)
                    else value[8*(length - 1 - i):8*(length - i)])
        put(dmac, 6); put(mac, 6); put(0x0800, 2)
        put(0x45, 1); put(0x02, 1); put(iplen, 2); put(0, 2); put(0x4000, 2); put(64, 1); put(17, 1)
        put(csum2, 2); put(ip, 4); put(dip, 4)
        put(sport, 2); put(ROCE_PORT, 2); put(udplen, 2); put(0, 2)
        put(s_op, 1); put(0, 1); put(0xffff, 2); put(0, 1); put(dqpn, 3)
        put(0, 1); put(bpsn, 3)
        put(s_syn, 1); put(s_msn, 3)
        assert len(hdr_bytes) == ROCE_HEADER_AETH

        tx_hdr  = Signal(8*ROCE_HEADER_AETH)
        tidx    = Signal(16)
        lane    = Signal(2)
        widx    = Signal(max=pmtu//4 + 1)
        tcrc    = Signal(32)
        tbyte   = Signal(8)
        advance = Signal()
        icrc    = Signal(32)
        self.comb += [
            source.data.eq(tbyte),
            txbuf_rd.adr.eq(Mux(advance, widx + 1, widx)),
            icrc.eq(tcrc ^ 0xffffffff),
        ]
        tx_fed = Signal(8)
        self.comb += tx_fed.eq(Mux(reduce(or_, [tidx == m for m in ROCE_ICRC_MASKED]), 0xff, tbyte))

        self.txfsm = txfsm = FSM(reset_state="IDLE")
        tx_feed = Signal()
        self.sync += If(tx_feed & source.ready & (tidx >= 14), tcrc.eq(crc32_next(tcrc, tx_fed)))
        txfsm.act("IDLE",
            If(tx_start,
                NextValue(tx_hdr, Cat(*hdr_bytes)),
                NextValue(tidx, 0),
                NextValue(widx, 0),
                NextValue(lane, 0),
                NextValue(tcrc, ROCE_ICRC_SEED),
                NextState("HEADER")
            )
        )
        txfsm.act("HEADER",
            source.valid.eq(1),
            tbyte.eq(tx_hdr[:8]),
            tx_feed.eq(1),
            If(source.ready,
                NextValue(tx_hdr, tx_hdr[8:]),
                NextValue(tidx, tidx + 1),
                If(tidx == hlen - 1,
                    If(paylen == 0, NextState("ICRC")).Else(NextState("PAYLOAD"))
                )
            )
        )
        txfsm.act("PAYLOAD",
            source.valid.eq(1),
            tbyte.eq(Mux(s_cnp, 0, Array(txbuf_rd.dat_r[8*i:8*i + 8] for i in range(4))[lane])),
            tx_feed.eq(1),
            advance.eq(source.ready & (lane == 3)),
            If(source.ready,
                NextValue(tidx, tidx + 1),
                NextValue(lane, lane + 1),
                If(lane == 3, NextValue(widx, widx + 1)),
                If(tidx == hlen + paylen - 1, NextState("ICRC"))
            )
        )
        txfsm.act("ICRC",
            source.valid.eq(1),
            tbyte.eq(Array(icrc[8*i:8*i + 8] for i in range(4))[lane]),
            source.last.eq(lane == 3),
            If(source.ready,
                NextValue(lane, lane + 1),
                If(lane == 3,
                    tx_done.eq(1),
                    NextState("IDLE")
                )
            )
        )
        # ``lane`` is 0 when the ICRC starts: the payload is a whole number of words.

        # Processing: one packet at a time, from the buffer.
        # ---------------------------------------------------
        e = fifo.source
        cq  = Signal(qp_bits)   # Queue pair of the packet.
        q   = Signal(qp_bits)
        qok = Signal()
        self.comb += [
            q.eq(e.dqpn - qpn_base),
            qok.eq((e.dqpn >= qpn_base) & (e.dqpn < qpn_base + qps) & qp_valid[q]),
        ]

        epsn   = Signal(24)
        diff   = Signal(24)
        inorder = Signal()
        dup     = Signal()
        self.comb += [
            epsn.eq(qp_epsn[cq]),
            diff.eq(e.psn - epsn),
            inorder.eq(diff == 0),
            dup.eq(diff[23]),
        ]

        op = e.opcode
        w_first = Signal(); w_middle = Signal(); w_last = Signal(); w_only = Signal(); rd = Signal()
        is_write = Signal()
        has_reth = Signal()
        hlen_rx  = Signal(8)
        short    = Signal()
        pay      = Signal(16)
        self.comb += [
            w_first.eq(op == ROCE_WRITE_FIRST),
            w_middle.eq(op == ROCE_WRITE_MIDDLE),
            w_last.eq(op == ROCE_WRITE_LAST),
            w_only.eq(op == ROCE_WRITE_ONLY),
            rd.eq(op == ROCE_READ_REQUEST),
            is_write.eq(w_first | w_middle | w_last | w_only),
            has_reth.eq(w_first | w_only | rd),
            hlen_rx.eq(Mux(has_reth, ROCE_HEADER_RETH, ROCE_HEADER)),
            short.eq(e.length < hlen_rx + 4),
            pay.eq(e.length - hlen_rx - 4),
        ]

        # Memory region of the R_Key, with the range and access checks of the RETH.
        hit     = Signal(mr_bits)
        hits    = Signal(mrs)
        allowed = Signal(mrs) # Range of the RETH inside the region.
        for m in range(mrs):
            end = Signal(66, name=f"mr_end{m}")
            self.comb += [
                end.eq(e.va - mr_va[m] + e.dlen),
                hits[m].eq(mr_valid[m] & (mr_rkey[m] == e.rkey) & (mr_pd[m] == qp_pd[cq])),
                allowed[m].eq((e.va >= mr_va[m]) & (end <= mr_length[m])),
            ]
        self.comb += If(hits != 0, *[If(hits[m], hit.eq(m)) for m in reversed(range(mrs))])
        mr_ok_rd = Signal()
        mr_ok_wr = Signal()
        self.comb += [
            mr_ok_rd.eq((e.dlen == 0) | ((hits != 0) & Array(allowed[m] for m in range(mrs))[hit] & mr_access[hit][0])),
            mr_ok_wr.eq((e.dlen == 0) | ((hits != 0) & Array(allowed[m] for m in range(mrs))[hit] & mr_access[hit][1])),
        ]
        aligned = Signal()
        self.comb += aligned.eq((e.pad == 0) & (pay[:2] == 0) & ~short &
            (~has_reth | ((e.va[:2] == 0) & (e.dlen[:2] == 0)))) # No RETH: payload bytes there.

        # Address translation of ``cva`` in region ``cmr``: page table entry, then word address.
        cva  = Signal(64)
        cmr  = Signal(mr_bits)
        off  = Signal(64)
        gadr = Signal(abits)
        entry    = Signal(16)
        resident = Signal()
        self.comb += [
            off.eq(cva - mr_va[cmr]),
            entry.eq(mr_base[cmr] + off[page_bits:]),
            pt_rd.adr.eq(entry),
            gadr.eq(pt_rd.dat_r[:abits] + off[2:page_bits]),
            resident.eq(pt_rd.dat_r[abits]),
        ]

        # Buffer read of the payload word at byte ``boff`` of the slot.
        boff  = Signal(16)
        k     = Signal(3)
        word  = Signal(32)
        self.comb += rxbuf_rd.adr.eq(Cat((boff + k)[:buf_bits], e.slot))

        nwords = Signal(16) # Words left in the packet.
        wfirst = Signal()   # Values of the packet in progress.
        wlast  = Signal()
        wack   = Signal()
        status = Signal(4)

        rleft  = Signal(32) # READ: bytes left to send.
        rpsn   = Signal(24)
        rfirst = Signal()
        rwords = Signal(max=pmtu//4 + 1)
        rj     = Signal(max=pmtu//4 + 1)
        npkts  = Signal(24)
        self.comb += npkts.eq(Mux(e.dlen == 0, 1, (e.dlen + pmtu - 1)[pmtu_bits:]))

        cnp_timer = Signal(max=cnp_interval + 1)
        cnp_done  = Signal()
        self.sync += If(cnp_timer != 0, cnp_timer.eq(cnp_timer - 1))

        self.comb += [
            txbuf_wr.adr.eq(rj),
            txbuf_wr.dat_w.eq(pool.source.dat_r),
            pool.sink.sel.eq(0b1111),
            pool.sink.adr.eq(gadr),
            pool.sink.dat_w.eq(word),
        ]

        after = Signal(2) # State after SEND: 0 CHECK, 1 POP, 2 READ-NEXT.
        def send(opcode, psn, aeth, syn, msn, words=0, cnp=0, then=1):
            return [
                NextValue(s_op,    opcode),
                NextValue(s_psn,   psn),
                NextValue(s_aeth,  aeth),
                NextValue(s_syn,   syn),
                NextValue(s_msn,   msn),
                NextValue(s_words, words),
                NextValue(s_cnp,   cnp),
                NextValue(s_qp,    cq),
                NextValue(after,   then),
                NextState("SEND"),
            ]
        def nak(syndrome, psn):
            return [NextValue(naks, naks + 1)] + send(ROCE_ACK, psn, 1, syndrome, qp_msn[cq])
        def fatal(syndrome, psn, next_psn):
            # The requester ends the message in error (and its queue pair with it); the packet is
            # skipped, the message in progress forgotten.
            return [NextValue(qp_epsn[cq], next_psn), NextValue(qp_wact[cq], 0)] + nak(syndrome, psn)

        self.fsm = fsm = FSM(reset_state="IDLE")
        self.comb += self.idle.eq(fsm.ongoing("IDLE") & ~e.valid)
        fsm.act("IDLE",
            If(e.valid,
                NextValue(cq, q),
                If(~qok,
                    NextValue(qp_drops, qp_drops + 1),
                    NextState("POP")
                ).Else(
                    NextState("CHECK")
                )
            )
        )
        fsm.act("CHECK",
            If(e.ce & ~cnp_done & (cnp_timer == 0),
                NextValue(cnp_done, 1),
                NextValue(cnp_timer, cnp_interval),
                NextValue(cnps, cnps + 1),
                *send(ROCE_CNP, 0, 0, 0, 0, words=0, cnp=1, then=0)
            ).Elif(~inorder & ~dup,
                # Packet after a missing one: one NAK, until the missing one comes.
                If(~qp_nak[cq],
                    NextValue(qp_nak[cq], 1),
                    *nak(ROCE_NAK_SEQUENCE, epsn)
                ).Else(
                    NextState("POP")
                )
            ).Elif(dup,
                NextValue(duplicates, duplicates + 1),
                If(rd & aligned & mr_ok_rd & (pay == 0),
                    # Asked again: answered again, nothing else changes.
                    NextState("READ-START")
                ).Elif(is_write & (w_last | w_only | e.ackreq),
                    NextValue(acks, acks + 1),
                    *send(ROCE_ACK, epsn - 1, 1, ROCE_SYN_ACK, qp_msn[cq])
                ).Else(
                    NextState("POP")
                )
            ).Else(
                NextValue(qp_nak[cq], 0),
                If(rd,
                    If(~aligned | (pay != 0),
                        *fatal(ROCE_NAK_INVALID, e.psn, e.psn + npkts)
                    ).Elif(~mr_ok_rd,
                        *fatal(ROCE_NAK_ACCESS, e.psn, e.psn + npkts)
                    ).Else(
                        NextValue(qp_msn[cq], qp_msn[cq] + 1),
                        NextValue(qp_epsn[cq], e.psn + npkts),
                        NextState("READ-START")
                    )
                ).Elif(w_first | w_only,
                    If(~aligned | qp_wact[cq] |
                       (w_first & ((pay != pmtu) | (e.dlen <= pmtu))) |
                       (w_only & (pay != e.dlen)),
                        *fatal(ROCE_NAK_INVALID, e.psn, e.psn + 1)
                    ).Elif(~mr_ok_wr,
                        *fatal(ROCE_NAK_ACCESS, e.psn, e.psn + 1)
                    ).Else(
                        NextValue(cva, e.va),
                        NextValue(cmr, hit),
                        NextValue(qp_wleft[cq], e.dlen),
                        NextValue(boff, ROCE_HEADER_RETH),
                        NextValue(nwords, pay[2:]),
                        NextState("WRITE-WORD")
                    )
                ).Elif(w_middle | w_last,
                    If(~aligned | ~qp_wact[cq] |
                       (w_middle & ((pay != pmtu) | (qp_wleft[cq] <= pmtu))) |
                       (w_last & ((pay != qp_wleft[cq]) | (pay == 0))),
                        *fatal(ROCE_NAK_INVALID, e.psn, e.psn + 1)
                    ).Else(
                        NextValue(cva, qp_wva[cq]),
                        NextValue(cmr, qp_wmr[cq]),
                        NextValue(boff, ROCE_HEADER),
                        NextValue(nwords, pay[2:]),
                        NextState("WRITE-WORD")
                    )
                ).Else(
                    *fatal(ROCE_NAK_INVALID, e.psn, e.psn + 1)
                )
            )
        )

        # WRITE: payload words from the buffer to the pool.
        fsm.act("WRITE-WORD",
            NextValue(k, 0),
            If(nwords == 0, NextState("WRITE-DONE")).Else(NextState("WRITE-GATHER"))
        )
        fsm.act("WRITE-GATHER",
            # Buffer byte k presented, byte k-1 read; page table read during these cycles.
            NextValue(k, k + 1),
            If(k != 0,
                Case(k, {i: NextValue(word[8*(i - 1):8*i], rxbuf_rd.dat_r) for i in range(1, 5)})
            ),
            If(k == 4, NextState("WRITE-POOL"))
        )
        fsm.act("WRITE-POOL",
            If(~resident,
                NextState("MISS")
            ).Else(
                pool.sink.valid.eq(1),
                pool.source.ready.eq(1),
            ),
            pool.sink.op.eq(POOL_WRITE),
            If(pool.source.valid,
                If(pool.source.status != FABRIC_OK,
                    *fatal(ROCE_NAK_OPERATION, e.psn, e.psn + 1)
                ).Else(
                    NextValue(dirty[entry], 1),
                    NextValue(cva,    cva + 4),
                    NextValue(boff,   boff + 4),
                    NextValue(nwords, nwords - 1),
                    NextState("WRITE-WORD")
                )
            )
        )
        fsm.act("WRITE-DONE",
            NextValue(qp_epsn[cq], e.psn + 1),
            If(w_first | w_middle,
                NextValue(qp_wact[cq], 1),
                NextValue(qp_wva[cq], cva),
                NextValue(qp_wmr[cq], cmr),
                NextValue(qp_wleft[cq], Mux(w_first, e.dlen, qp_wleft[cq]) - pay)
            ).Else(
                NextValue(qp_wact[cq], 0),
                NextValue(qp_wleft[cq], 0),
                NextValue(qp_msn[cq], qp_msn[cq] + 1),
                NextValue(writes, writes + 1)
            ),
            If(w_last | w_only | e.ackreq,
                NextValue(acks, acks + 1),
                *send(ROCE_ACK, e.psn, 1, ROCE_SYN_ACK, qp_msn[cq] + (w_last | w_only))
            ).Else(
                NextState("POP")
            )
        )

        # READ: one response packet at a time, its words from the pool to the transmit buffer.
        fsm.act("READ-START",
            NextValue(cva,    e.va),
            NextValue(cmr,    hit),
            NextValue(rleft,  e.dlen),
            NextValue(rpsn,   e.psn),
            NextValue(rfirst, 1),
            NextValue(reads,  reads + 1),
            NextState("READ-FILL")
        )
        fsm.act("READ-FILL",
            NextValue(rwords, Mux(rleft > pmtu, pmtu, rleft)[2:]),
            NextValue(rj, 0),
            NextState("READ-PT")
        )
        fsm.act("READ-PT",
            # Page table read of ``cva``.
            If(rj == rwords, NextState("READ-SEND")).Else(NextState("READ-POOL"))
        )
        fsm.act("READ-POOL",
            If(~resident,
                NextState("MISS")
            ).Else(
                pool.sink.valid.eq(1),
                pool.source.ready.eq(1),
            ),
            pool.sink.op.eq(POOL_READ),
            If(pool.source.valid,
                If(pool.source.status != FABRIC_OK,
                    *nak(ROCE_NAK_OPERATION, rpsn)
                ).Else(
                    txbuf_wr.we.eq(1),
                    NextValue(rj,  rj + 1),
                    NextValue(cva, cva + 4),
                    NextState("READ-PT")
                )
            )
        )
        last_pkt = Signal()
        self.comb += last_pkt.eq(rleft <= pmtu)
        fsm.act("READ-SEND",
            *send(
                Mux(rfirst & last_pkt, ROCE_READ_RESP_ONLY,
                Mux(rfirst, ROCE_READ_RESP_FIRST,
                Mux(last_pkt, ROCE_READ_RESP_LAST, ROCE_READ_RESP_MID))),
                rpsn, rfirst | last_pkt, ROCE_SYN_ACK, qp_msn[cq], words=rwords, then=2)
        )
        fsm.act("READ-NEXT",
            NextValue(rleft,  rleft - (rwords << 2)),
            NextValue(rpsn,   rpsn + 1),
            NextValue(rfirst, 0),
            If(last_pkt, NextState("POP")).Else(NextState("READ-FILL"))
        )

        fsm.act("SEND",
            tx_start.eq(1),
            If(tx_done,
                If(after == 0,
                    NextState("CHECK")
                ).Elif(after == 2,
                    NextState("READ-NEXT")
                ).Else(
                    NextState("POP")
                )
            )
        )
        fsm.act("MISS",
            # The packet is dropped: no answer, the PSN expected does not move for a WRITE (for a
            # READ the requester sends it again as a duplicate, run again).
            miss_fifo.sink.valid.eq(1),
            miss_fifo.sink.entry.eq(entry),
            NextValue(misses, misses + 1),
            NextState("POP")
        )
        fsm.act("POP",
            e.ready.eq(1),
            NextValue(packets, packets + 1),
            NextValue(cnp_done, 0),
            NextState("IDLE")
        )
