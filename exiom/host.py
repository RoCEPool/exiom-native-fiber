#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Host channel of a memory server card: registers and DMA between the host memory and the fabric.

The host (CPU of the memory server) drives the RoCE target and moves pages between its memory and
the frames of the fabric through this channel only, as through the PCIe BAR and the DMA of the
card:

- ``bus``: Wishbone slave (32-bit words, word addresses) with the registers below. On hardware it
  sits behind the PCIe BAR (LitePCIe); in the tests the simulation drives it.
- ``host``: port to the host memory, the side of the DMA that is PCIe on hardware: commands of one
  word (``adr`` in words, ``we``, ``dat_w``), each answered in order (``dat_r``). ``HostMemory``
  is a model of it with a latency.
- DMA: ``words`` words between the host memory and global addresses of the pool, one transfer at
  a time, with its status (first error of the pool and words done).
- ``PoolArbiter``: the pool of the node is shared by the RoCE target and the DMA, one operation
  at a time, in turn.

Registers (word addresses; 64-bit values in two words, low first; a table entry is staged in its
registers and written by the write to its ``COMMIT`` register):

    0x00 ID (0x6578696f, "exio")
    0x10-0x18 queue pair: index, valid, dqpn, mac low, mac high, ip, sport, epsn, COMMIT;
              0x19 protection domain
    0x20-0x29 memory region: index, valid, rkey, va low, va high, length low, length high, access,
              base, COMMIT; 0x2a protection domain
    0x30-0x34 page table: entry, frame low, frame high, resident, COMMIT
    0x38 dirty index (write), 0x39 dirty bit of that entry (read), 0x3a clear it (write)
    0x40 miss (read, takes it): bit 31 valid, [15:0] page table entry
    0x48 idle, 0x49 packets done, 0x4a misses
    0x50 rx packets, 0x51 ignored, 0x52 ICRC errors, 0x53 busy drops, 0x54 QP drops,
    0x55 duplicates, 0x56 writes, 0x57 reads, 0x58 ACKs, 0x59 NAKs, 0x5a CNPs
    0x60-0x66 DMA: host word address low, high, global word address low, high, words,
              direction (0 host to fabric, 1 fabric to host), START
    0x67 DMA status (read): bit 0 busy, [7:4] status of the pool (FABRIC_*), 0x68 words done
"""

from migen import *

from litex.gen import *

from litex.soc.interconnect import stream, wishbone

from exiom.fabric import *
from exiom.pool   import *

# Registers ----------------------------------------------------------------------------------------

HOST_ID = 0x6578696f

REG_QP_INDEX, REG_QP_VALID, REG_QP_DQPN, REG_QP_MAC_LO, REG_QP_MAC_HI = 0x10, 0x11, 0x12, 0x13, 0x14
REG_QP_IP, REG_QP_SPORT, REG_QP_EPSN, REG_QP_COMMIT, REG_QP_PD = 0x15, 0x16, 0x17, 0x18, 0x19
REG_MR_INDEX, REG_MR_VALID, REG_MR_RKEY, REG_MR_VA_LO, REG_MR_VA_HI = 0x20, 0x21, 0x22, 0x23, 0x24
REG_MR_LEN_LO, REG_MR_LEN_HI, REG_MR_ACCESS, REG_MR_BASE, REG_MR_COMMIT = 0x25, 0x26, 0x27, 0x28, 0x29
REG_MR_PD = 0x2a
REG_PT_ENTRY, REG_PT_FRAME_LO, REG_PT_FRAME_HI, REG_PT_RESIDENT, REG_PT_COMMIT = 0x30, 0x31, 0x32, 0x33, 0x34
REG_DIRTY_INDEX, REG_DIRTY, REG_DIRTY_CLEAR = 0x38, 0x39, 0x3a
REG_MISS = 0x40
REG_IDLE, REG_PACKETS, REG_MISSES = 0x48, 0x49, 0x4a
REG_COUNTERS = 0x50
COUNTERS = ["rx_packets", "ignored", "icrc_errors", "busy_drops", "qp_drops", "duplicates",
    "writes", "reads", "acks", "naks", "cnps"]
REG_DMA_HOST_LO, REG_DMA_HOST_HI, REG_DMA_GADR_LO, REG_DMA_GADR_HI = 0x60, 0x61, 0x62, 0x63
REG_DMA_WORDS, REG_DMA_DIR, REG_DMA_START, REG_DMA_STATUS, REG_DMA_DONE = 0x64, 0x65, 0x66, 0x67, 0x68

DMA_TO_FABRIC = 0
DMA_TO_HOST   = 1

def host_cmd_layout():
    return [("adr", 40), ("we", 1), ("dat_w", 32)]

def host_rsp_layout():
    return [("dat_r", 32)]

# Pool Arbiter -------------------------------------------------------------------------------------

class PoolArbiter(LiteXModule):
    """``ports`` users of one ``FabricPool``: an operation at a time, the users in turn. Each port
    has the ``sink`` and ``source`` of a pool."""
    def __init__(self, pool, ports=2):
        abits = len(pool.sink.adr)
        window_bits = abits - 8
        self.ports = []
        for i in range(ports):
            port = Record([])
            port.sink   = stream.Endpoint(pool_request_layout(window_bits))
            port.source = stream.Endpoint(pool_result_layout())
            self.ports.append(port)

        # # #

        grant   = Signal(max=max(ports, 2))
        sinks   = Array(p.sink for p in self.ports)
        sources = Array(p.source for p in self.ports)
        valid   = Signal()
        self.comb += [
            valid.eq(sinks[grant].valid),
            pool.sink.valid.eq(valid),
            sinks[grant].ready.eq(pool.sink.ready),
            pool.sink.op.eq(sinks[grant].op),
            pool.sink.adr.eq(sinks[grant].adr),
            pool.sink.dat_w.eq(sinks[grant].dat_w),
            pool.sink.sel.eq(sinks[grant].sel),
            pool.sink.dst_adr.eq(sinks[grant].dst_adr),
            pool.sink.words.eq(sinks[grant].words),
            sources[grant].valid.eq(pool.source.valid),
            pool.source.ready.eq(sources[grant].ready),
        ]
        for p in self.ports:
            self.comb += [
                p.source.status.eq(pool.source.status),
                p.source.dat_r.eq(pool.source.dat_r),
                p.source.words.eq(pool.source.words),
            ]
        # A user keeps its operation valid until its result (the pool reads the operation while it
        # runs): the grant moves on when the user granted does not ask, or after its result.
        self.sync += If(~valid | (pool.source.valid & pool.source.ready),
            grant.eq(Mux(grant == ports - 1, 0, grant + 1))
        )

# Host Memory (model) ------------------------------------------------------------------------------

class HostMemory(LiteXModule):
    """Host memory of ``words`` words behind a DMA port, each command answered ``latency`` cycles
    later (PCIe round trip). ``mem``: the words (what the CPU reads and writes)."""
    def __init__(self, words, latency=8):
        self.cmd = cmd = stream.Endpoint(host_cmd_layout())
        self.rsp = rsp = stream.Endpoint(host_rsp_layout())
        self.mem = mem = Memory(32, words)
        port = mem.get_port(write_capable=True)
        self.specials += mem, port

        # # #

        wait  = Signal(max=latency + 2)
        data  = Signal(32)
        self.fsm = fsm = FSM(reset_state="IDLE")
        self.comb += [
            port.adr.eq(cmd.adr),
            port.dat_w.eq(cmd.dat_w),
            rsp.dat_r.eq(data),
        ]
        fsm.act("IDLE",
            If(cmd.valid,
                cmd.ready.eq(1),
                port.we.eq(cmd.we),
                NextValue(wait, latency),
                NextState("READ")
            )
        )
        fsm.act("READ",
            NextValue(data, port.dat_r),
            NextState("WAIT")
        )
        fsm.act("WAIT",
            If(wait == 0,
                rsp.valid.eq(1),
                If(rsp.ready, NextState("IDLE"))
            ).Else(
                NextValue(wait, wait - 1)
            )
        )

# Host Channel -------------------------------------------------------------------------------------

class FabricHost(LiteXModule):
    """Registers of ``roce`` (``FabricRoCE``) and DMA through ``pool_port`` (a pool, or a port of a
    ``PoolArbiter``) for the host."""
    def __init__(self, roce, pool_port):
        self.bus  = bus  = wishbone.Interface(data_width=32, adr_width=16)
        self.host = Record([])
        self.host.cmd = hcmd = stream.Endpoint(host_cmd_layout())
        self.host.rsp = hrsp = stream.Endpoint(host_rsp_layout())

        # # #

        abits = len(pool_port.sink.adr)

        def reg(width, name):
            return Signal(width, name=name)
        qp_index, qp_valid, qp_dqpn, qp_ip, qp_sport, qp_epsn = (reg(8, "qp_index"), reg(1, "qp_valid"),
            reg(24, "qp_dqpn"), reg(32, "qp_ip"), reg(16, "qp_sport"), reg(24, "qp_epsn"))
        qp_mac = reg(48, "qp_mac")
        qp_pd, mr_pd = reg(8, "qp_pd"), reg(8, "mr_pd")
        mr_index, mr_valid, mr_rkey, mr_va, mr_len, mr_access, mr_base = (reg(8, "mr_index"),
            reg(1, "mr_valid"), reg(32, "mr_rkey"), reg(64, "mr_va"), reg(64, "mr_len"),
            reg(2, "mr_access"), reg(16, "mr_base"))
        pt_entry, pt_frame, pt_resident = reg(16, "pt_entry"), reg(64, "pt_frame"), reg(1, "pt_resident")
        dirty_index = reg(16, "dirty_index")
        dma_host, dma_gadr, dma_words, dma_dir = reg(64, "dma_host"), reg(64, "dma_gadr"), reg(32, "dma_words"), reg(1, "dma_dir")
        dma_start  = Signal()
        dma_busy   = Signal()
        dma_status = Signal(4)
        dma_done   = Signal(32)

        # Bus: one access answered the next cycle; writes and reads with side effects happen then.
        access = Signal()
        wr     = Signal()
        rd     = Signal()
        adr    = bus.adr
        self.comb += [
            access.eq(bus.cyc & bus.stb & ~bus.ack),
            wr.eq(access & bus.we),
            rd.eq(access & ~bus.we),
        ]
        self.sync += bus.ack.eq(access)

        def lo(value): return value[:32]
        def hi(value): return value[32:]
        writes = {
            REG_QP_INDEX: qp_index, REG_QP_VALID: qp_valid, REG_QP_DQPN: qp_dqpn,
            REG_QP_MAC_LO: lo(qp_mac), REG_QP_MAC_HI: hi(qp_mac), REG_QP_IP: qp_ip,
            REG_QP_SPORT: qp_sport, REG_QP_EPSN: qp_epsn, REG_QP_PD: qp_pd, REG_MR_PD: mr_pd,
            REG_MR_INDEX: mr_index, REG_MR_VALID: mr_valid, REG_MR_RKEY: mr_rkey,
            REG_MR_VA_LO: lo(mr_va), REG_MR_VA_HI: hi(mr_va), REG_MR_LEN_LO: lo(mr_len),
            REG_MR_LEN_HI: hi(mr_len), REG_MR_ACCESS: mr_access, REG_MR_BASE: mr_base,
            REG_PT_ENTRY: pt_entry, REG_PT_FRAME_LO: lo(pt_frame), REG_PT_FRAME_HI: hi(pt_frame),
            REG_PT_RESIDENT: pt_resident, REG_DIRTY_INDEX: dirty_index,
            REG_DMA_HOST_LO: lo(dma_host), REG_DMA_HOST_HI: hi(dma_host),
            REG_DMA_GADR_LO: lo(dma_gadr), REG_DMA_GADR_HI: hi(dma_gadr),
            REG_DMA_WORDS: dma_words, REG_DMA_DIR: dma_dir,
        }
        self.sync += If(wr, Case(adr, {a: s.eq(bus.dat_w) for a, s in writes.items()}))

        # Commits: one-cycle write strobes of the tables of the target.
        self.comb += [
            roce.qp_cfg.we.eq(wr & (adr == REG_QP_COMMIT)),
            roce.qp_cfg.index.eq(qp_index), roce.qp_cfg.valid.eq(qp_valid),
            roce.qp_cfg.dqpn.eq(qp_dqpn), roce.qp_cfg.mac.eq(qp_mac), roce.qp_cfg.ip.eq(qp_ip),
            roce.qp_cfg.sport.eq(qp_sport), roce.qp_cfg.epsn.eq(qp_epsn), roce.qp_cfg.pd.eq(qp_pd),
            roce.mr_cfg.we.eq(wr & (adr == REG_MR_COMMIT)),
            roce.mr_cfg.index.eq(mr_index), roce.mr_cfg.valid.eq(mr_valid),
            roce.mr_cfg.rkey.eq(mr_rkey), roce.mr_cfg.va.eq(mr_va), roce.mr_cfg.length.eq(mr_len),
            roce.mr_cfg.access.eq(mr_access), roce.mr_cfg.base.eq(mr_base), roce.mr_cfg.pd.eq(mr_pd),
            roce.pt_cfg.we.eq(wr & (adr == REG_PT_COMMIT)),
            roce.pt_cfg.adr.eq(pt_entry), roce.pt_cfg.dat.eq(pt_frame),
            roce.pt_cfg.resident.eq(pt_resident),
            roce.dirty_cfg.we.eq(wr & (adr == REG_DIRTY_CLEAR)),
            roce.dirty_cfg.adr.eq(dirty_index),
            dma_start.eq(wr & (adr == REG_DMA_START)),
        ]

        # Reads (the miss is taken by its read).
        miss = roce.miss
        self.comb += miss.ready.eq(rd & (adr == REG_MISS))
        reads = {
            0x00: Constant(HOST_ID, 32),
            REG_DIRTY: roce.dirty[dirty_index[:max(1, (len(roce.dirty) - 1).bit_length())]],
            REG_MISS: Cat(miss.entry, Constant(0, 15), miss.valid),
            REG_IDLE: roce.idle, REG_PACKETS: roce.packets, REG_MISSES: roce.misses,
            REG_DMA_STATUS: Cat(dma_busy, Constant(0, 3), dma_status), REG_DMA_DONE: dma_done,
        }
        for i, name in enumerate(COUNTERS):
            reads[REG_COUNTERS + i] = getattr(roce, name)
        self.sync += If(rd, Case(adr, {**{a: bus.dat_r.eq(v) for a, v in reads.items()},
            "default": bus.dat_r.eq(0)}))

        # DMA: one word at a time, host memory <-> pool.
        # -------------------------------------------------
        hadr = Signal(64)
        gadr = Signal(64)
        left = Signal(32)
        data = Signal(32)
        pool = pool_port
        self.comb += [
            pool.sink.adr.eq(gadr),
            pool.sink.dat_w.eq(data),
            pool.sink.sel.eq(0b1111),
            hcmd.adr.eq(hadr),
        ]
        self.fsm = fsm = FSM(reset_state="IDLE")
        self.comb += dma_busy.eq(~fsm.ongoing("IDLE"))
        fsm.act("IDLE",
            If(dma_start,
                NextValue(hadr, dma_host),
                NextValue(gadr, dma_gadr),
                NextValue(left, dma_words),
                NextValue(dma_done, 0),
                NextValue(dma_status, FABRIC_OK),
                NextState("NEXT")
            )
        )
        fsm.act("NEXT",
            If(left == 0,
                NextState("IDLE")
            ).Elif(dma_dir == DMA_TO_FABRIC,
                NextState("HOST-READ")
            ).Else(
                NextState("POOL-READ")
            )
        )
        # Host to fabric.
        fsm.act("HOST-READ",
            hcmd.valid.eq(1),
            hcmd.we.eq(0),
            If(hcmd.ready, NextState("HOST-DATA"))
        )
        fsm.act("HOST-DATA",
            hrsp.ready.eq(1),
            If(hrsp.valid,
                NextValue(data, hrsp.dat_r),
                NextState("POOL-WRITE")
            )
        )
        fsm.act("POOL-WRITE",
            pool.sink.valid.eq(1),
            pool.sink.op.eq(POOL_WRITE),
            pool.source.ready.eq(1),
            If(pool.source.valid,
                If(pool.source.status != FABRIC_OK,
                    NextValue(dma_status, pool.source.status),
                    NextState("IDLE")
                ).Else(
                    NextState("ADVANCE")
                )
            )
        )
        # Fabric to host.
        fsm.act("POOL-READ",
            pool.sink.valid.eq(1),
            pool.sink.op.eq(POOL_READ),
            pool.source.ready.eq(1),
            If(pool.source.valid,
                If(pool.source.status != FABRIC_OK,
                    NextValue(dma_status, pool.source.status),
                    NextState("IDLE")
                ).Else(
                    NextValue(data, pool.source.dat_r),
                    NextState("HOST-WRITE")
                )
            )
        )
        fsm.act("HOST-WRITE",
            hcmd.valid.eq(1),
            hcmd.we.eq(1),
            hcmd.dat_w.eq(data),
            If(hcmd.ready, NextState("HOST-ACK"))
        )
        fsm.act("HOST-ACK",
            hrsp.ready.eq(1),
            If(hrsp.valid, NextState("ADVANCE"))
        )
        fsm.act("ADVANCE",
            NextValue(hadr, hadr + 1),
            NextValue(gadr, gadr + 1),
            NextValue(left, left - 1),
            NextValue(dma_done, dma_done + 1),
            NextState("NEXT")
        )
