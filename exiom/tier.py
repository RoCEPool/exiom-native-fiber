#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Memory tiers of a memory server: host software around the RoCE target (``FabricRoCE``).

The memory of the fabric (DRAM pages, called frames here) is a cache of the persistent layer (NVMe,
the RoCEPool storage layer in the full system): every page of a memory region has a place on
NVMe, and a frame while it is resident. The page table of the target says, for each page, whether
it is resident and in which frame; the host keeps the rest:

- Regions: ``region()`` gives a memory region (R_Key, virtual address, length, access) its page
  table entries and its NVMe pages, and writes them in the journal. Pages start not resident
  (zeros on NVMe until written).
- Misses: the target drops a packet that meets a page not resident and gives the page table entry
  to the host (``miss``); ``service()`` brings the page in (evicting another one when no frame is
  free) and makes it resident. The requester sends the packet again (timeout, or sequence NAK).
  A READ is then run again, maybe after the messages sent after it: as for any READ run again in
  RC, it can see their writes; a client that writes bytes it has just asked to read fences the
  write (IBV_SEND_FENCE).
- Eviction: the oldest page brought in that is not pinned. Its entry is made not resident first,
  then the host waits for the end of the packet being processed (no access with the old
  translation after it), then writes the page to NVMe if the target marked it dirty.
- ``flush()``: dirty pages to NVMe (the dirty bit is cleared before the copy: a write during the
  copy sets it again).
- Journal: region records, on persistent storage with NVMe. After a power loss, ``recover()``
  replays it on a fresh target: the regions are back, every page not resident, and the data comes
  back from NVMe on the first access. Writes after the last flush of their page are lost.
- Queue pairs: ``connect()`` writes the context of a queue pair (from the connection exchange
  with the client, out of band) in the target.

Everything that touches the target is a generator (simulation: Migen; on hardware the same calls
go through the driver of the card). The data of the frames is moved by the driver
(``read_frame``, ``write_frame``): PCIe DMA on hardware, the memories directly in the tests.
"""

from collections import OrderedDict

from exiom import host as h

# Persistent layer (model) -------------------------------------------------------------------------

class NVMeStore:
    """Pages of ``page_bytes`` bytes, zeros until written. Survives a power loss."""
    def __init__(self, page_bytes):
        self.page_bytes = page_bytes
        self.pages  = {}
        self.reads  = 0
        self.writes = 0

    def read(self, page):
        self.reads += 1
        return self.pages.get(page, bytes(self.page_bytes))

    def write(self, page, data):
        assert len(data) == self.page_bytes
        self.writes += 1
        self.pages[page] = bytes(data)

class Journal:
    """Append-only records. Survives a power loss."""
    def __init__(self):
        self.records = []

    def append(self, record):
        self.records.append(dict(record))

# Driver (simulation) ------------------------------------------------------------------------------

class SimDriver:
    """Access to a ``FabricRoCE`` in a Migen simulation. ``memories``: node -> Memory of the node
    (frames read and written directly, as a DMA would), ``window_bits`` of the pool."""
    def __init__(self, roce, memories, window_bits):
        self.roce        = roce
        self.memories    = memories
        self.window_bits = window_bits

    def _write(self, record, **fields):
        for name, value in fields.items():
            yield getattr(record, name).eq(value)
        yield record.we.eq(1)
        yield
        yield record.we.eq(0)

    def set_qp(self, index, valid, dqpn, mac, ip, sport, epsn):
        yield from self._write(self.roce.qp_cfg, index=index, valid=valid, dqpn=dqpn, mac=mac, ip=ip,
            sport=sport, epsn=epsn)

    def set_mr(self, index, valid, rkey, va, length, access, base):
        yield from self._write(self.roce.mr_cfg, index=index, valid=valid, rkey=rkey, va=va,
            length=length, access=access, base=base)

    def set_pt(self, entry, frame, resident):
        yield from self._write(self.roce.pt_cfg, adr=entry, dat=frame, resident=resident)

    def dirty(self, entry):
        return (yield self.roce.dirty[entry])

    def clear_dirty(self, entry):
        yield from self._write(self.roce.dirty_cfg, adr=entry)

    def misses(self):
        """Page table entries of the misses waiting."""
        entries = []
        miss = self.roce.miss
        yield miss.ready.eq(1)
        while True:
            yield
            if not (yield miss.valid):
                break
            entries.append((yield miss.entry))
        yield miss.ready.eq(0)
        yield
        return entries

    def quiesce(self):
        """Waits for the end of the packet being processed, if any."""
        done = yield self.roce.packets
        while not (yield self.roce.idle):
            if (yield self.roce.packets) != done:
                break
            yield

    def _word(self, gadr):
        node, word = gadr >> self.window_bits, gadr & (2**self.window_bits - 1)
        return self.memories[node], word

    def read_frame(self, frame, length):
        data = bytearray()
        for i in range(length//4):
            mem, word = self._word(frame + i)
            data += (yield mem[word]).to_bytes(4, "little")
        return bytes(data)

    def write_frame(self, frame, data):
        for i in range(len(data)//4):
            mem, word = self._word(frame + i)
            yield mem[word].eq(int.from_bytes(data[4*i:4*i + 4], "little"))
        yield

# Memory Server ------------------------------------------------------------------------------------

class MemoryServer:
    """Host software of a memory server.

    ``frames``: global word addresses of the frames (DRAM pages) given to the regions.
    ``page_bytes``: page size of the target (``2**page_bits``). ``pt_entries``: entries of the
    page table of the target.
    """
    def __init__(self, driver, frames, page_bytes, pt_entries, journal, nvme, qpn_base=0x100):
        assert nvme.page_bytes == page_bytes
        self.driver     = driver
        self.free       = list(frames)
        self.page_bytes = page_bytes
        self.pt_entries = pt_entries
        self.journal    = journal
        self.nvme       = nvme
        self.qpn_base   = qpn_base
        self.regions    = {}           # Index -> record.
        self.nvme_page  = {}           # Entry -> NVMe page.
        self.resident   = OrderedDict() # Entry -> frame, oldest brought in first.
        self.pinned     = set()
        self.next_entry = 0
        self.next_page  = 0
        self.loads      = 0
        self.evictions  = 0
        self.writebacks = 0

    # Regions.

    def region(self, index, rkey, va, length, access):
        """Memory region ``index``: its pages, not resident. Returns the record."""
        pages = -(-length // self.page_bytes)
        assert self.next_entry + pages <= self.pt_entries, "Page table full."
        record = dict(kind="region", index=index, rkey=rkey, va=va, length=length, access=access,
            base=self.next_entry, nvme=self.next_page, pages=pages)
        self.journal.append(record)
        yield from self._install(record)
        return record

    def _install(self, record):
        self.regions[record["index"]] = record
        for i in range(record["pages"]):
            entry = record["base"] + i
            self.nvme_page[entry] = record["nvme"] + i
            yield from self.driver.set_pt(entry, 0, 0)
        self.next_entry = max(self.next_entry, record["base"] + record["pages"])
        self.next_page  = max(self.next_page, record["nvme"] + record["pages"])
        yield from self.driver.set_mr(record["index"], 1, record["rkey"], record["va"],
            record["length"], record["access"], record["base"])

    def pin(self, entry, pinned=True):
        """A pinned page is never evicted (a KV block in use)."""
        (self.pinned.add if pinned else self.pinned.discard)(entry)

    # Queue pairs.

    def connect(self, index, dqpn, mac, ip, sport, epsn):
        """Context of queue pair ``index`` from the connection exchange; returns its QPN."""
        yield from self.driver.set_qp(index, 1, dqpn, mac, ip, sport, epsn)
        return self.qpn_base + index

    # Tiers.

    def service(self):
        """Brings in the pages of the misses waiting. Returns the number brought in."""
        loaded = 0
        for entry in (yield from self.driver.misses()):
            if entry in self.resident or entry not in self.nvme_page:
                continue # Already brought in (the packet was sent again before), or no region.
            yield from self.load(entry)
            loaded += 1
        return loaded

    def load(self, entry):
        if not self.free:
            yield from self.evict()
        frame = self.free.pop(0)
        yield from self.driver.write_frame(frame, self.nvme.read(self.nvme_page[entry]))
        yield from self.driver.set_pt(entry, frame, 1)
        self.resident[entry] = frame
        self.loads += 1

    def evict(self):
        victim = next((e for e in self.resident if e not in self.pinned), None)
        assert victim is not None, "Every resident page is pinned."
        frame = self.resident.pop(victim)
        yield from self.driver.set_pt(victim, frame, 0)
        yield from self.driver.quiesce()
        yield from self._write_back(victim, frame)
        self.free.append(frame)
        self.evictions += 1

    def _write_back(self, entry, frame):
        if (yield from self.driver.dirty(entry)):
            yield from self.driver.clear_dirty(entry)
            data = yield from self.driver.read_frame(frame, self.page_bytes)
            self.nvme.write(self.nvme_page[entry], data)
            self.writebacks += 1

    def flush(self):
        """Dirty resident pages to NVMe."""
        for entry, frame in list(self.resident.items()):
            yield from self._write_back(entry, frame)

    # Power loss.

    @classmethod
    def recover(cls, driver, frames, page_bytes, pt_entries, journal, nvme, **kwargs):
        """Memory server on a fresh target after a power loss: regions from the journal, pages
        not resident, data from NVMe on the first access."""
        server = cls(driver, frames, page_bytes, pt_entries, journal, nvme, **kwargs)
        for record in journal.records:
            if record["kind"] == "region":
                yield from server._install(record)
        return server

# Driver (card registers and DMA) ------------------------------------------------------------------

class CardDriver:
    """Access to the card through its registers and its DMA (``FabricHost``), as the driver on
    hardware: ``bus``: register reads and writes (``read(adr)``, ``write(adr, value)``, generators:
    PCIe BAR on hardware, the Wishbone of the simulation in the tests); ``cpu``: words of the host
    memory read and written by the CPU (``read(word)``, ``write(word, value)``); ``buffer``: word
    address in the host memory of the bounce buffer of the DMA (one page)."""
    def __init__(self, bus, cpu, buffer, timeout=1000000):
        self.bus     = bus
        self.cpu     = cpu
        self.buffer  = buffer
        self.timeout = timeout

    def _regs(self, values):
        for adr, value in values:
            yield from self.bus.write(adr, value & 0xffffffff)

    def set_qp(self, index, valid, dqpn, mac, ip, sport, epsn):
        yield from self._regs([(h.REG_QP_INDEX, index), (h.REG_QP_VALID, valid), (h.REG_QP_DQPN, dqpn),
            (h.REG_QP_MAC_LO, mac), (h.REG_QP_MAC_HI, mac >> 32), (h.REG_QP_IP, ip),
            (h.REG_QP_SPORT, sport), (h.REG_QP_EPSN, epsn), (h.REG_QP_COMMIT, 1)])

    def set_mr(self, index, valid, rkey, va, length, access, base):
        yield from self._regs([(h.REG_MR_INDEX, index), (h.REG_MR_VALID, valid), (h.REG_MR_RKEY, rkey),
            (h.REG_MR_VA_LO, va), (h.REG_MR_VA_HI, va >> 32), (h.REG_MR_LEN_LO, length),
            (h.REG_MR_LEN_HI, length >> 32), (h.REG_MR_ACCESS, access), (h.REG_MR_BASE, base),
            (h.REG_MR_COMMIT, 1)])

    def set_pt(self, entry, frame, resident):
        yield from self._regs([(h.REG_PT_ENTRY, entry), (h.REG_PT_FRAME_LO, frame),
            (h.REG_PT_FRAME_HI, frame >> 32), (h.REG_PT_RESIDENT, resident), (h.REG_PT_COMMIT, 1)])

    def dirty(self, entry):
        yield from self.bus.write(h.REG_DIRTY_INDEX, entry)
        return (yield from self.bus.read(h.REG_DIRTY))

    def clear_dirty(self, entry):
        yield from self._regs([(h.REG_DIRTY_INDEX, entry), (h.REG_DIRTY_CLEAR, 1)])

    def misses(self):
        entries = []
        while True:
            value = yield from self.bus.read(h.REG_MISS)
            if not value >> 31:
                return entries
            entries.append(value & 0xffff)

    def quiesce(self):
        done = yield from self.bus.read(h.REG_PACKETS)
        while not (yield from self.bus.read(h.REG_IDLE)):
            if (yield from self.bus.read(h.REG_PACKETS)) != done:
                break

    def counter(self, name):
        return (yield from self.bus.read(h.REG_COUNTERS + h.COUNTERS.index(name)))

    def dma(self, host_word, gadr, words, direction):
        """One DMA transfer; returns (status, words done)."""
        yield from self._regs([(h.REG_DMA_HOST_LO, host_word), (h.REG_DMA_HOST_HI, host_word >> 32),
            (h.REG_DMA_GADR_LO, gadr), (h.REG_DMA_GADR_HI, gadr >> 32), (h.REG_DMA_WORDS, words),
            (h.REG_DMA_DIR, direction), (h.REG_DMA_START, 1)])
        for _ in range(self.timeout):
            status = yield from self.bus.read(h.REG_DMA_STATUS)
            if not status & 1:
                return (status >> 4) & 0xf, (yield from self.bus.read(h.REG_DMA_DONE))
        raise TimeoutError("DMA not done.")

    def read_frame(self, frame, length):
        status, done = yield from self.dma(self.buffer, frame, length//4, h.DMA_TO_HOST)
        assert status == 0, f"DMA error {status} after {done} words."
        data = bytearray()
        for i in range(length//4):
            data += (yield from self.cpu.read(self.buffer + i)).to_bytes(4, "little")
        return bytes(data)

    def write_frame(self, frame, data):
        for i in range(len(data)//4):
            yield from self.cpu.write(self.buffer + i, int.from_bytes(data[4*i:4*i + 4], "little"))
        status, done = yield from self.dma(self.buffer, frame, len(data)//4, h.DMA_TO_FABRIC)
        assert status == 0, f"DMA error {status} after {done} words."
