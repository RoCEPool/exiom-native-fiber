#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Connection exchange and address allocation of a memory server, with a key-value index.

Host software, next to the memory tiers (``MemoryServer``). A client (the library of a GPU
server) talks to it over TCP, out of band, before and next to its RDMA traffic:

- ``connect``: the client gives its queue pair (QPN, first PSN, MAC, IP, UDP source port) and its
  tenant; the server takes a queue pair of the target for it, writes its context (with the
  protection domain of the tenant) and answers with the QPN of the target, its path MTU and the
  region of the tenant (R_Key, virtual address, length). ``disconnect`` gives the queue pair back.
  The client sets the path MTU of its queue pair to it, so that its network card cuts messages
  in the packets the target takes (WRITE FIRST/MIDDLE carry exactly one MTU).
- Addresses by key: a tenant has one region; its space is given out by key. ``alloc`` gives an
  address for a key (a new place, even when the key exists), the client writes the value there
  with RDMA WRITE, then ``commit`` makes it the value of the key. ``lookup`` gives the place of
  the committed value (R_Key, address, length, version): the client reads it with one RDMA READ,
  by address, and can keep the address for ``lease`` seconds. ``free`` removes a key.
- Space given back (a key freed, or replaced by a commit) is not given out again before the end of
  the leases of the addresses already given: a client reading with an address it kept never reads
  another value than the one it was given (or the same place not yet reused).
- Journal: tenants and key changes are written in the journal of the memory server. After a power
  loss, ``recover()`` rebuilds the tenants and the committed keys; values allocated and not
  committed are dropped (their space is free again). Values committed after the last flush of
  their pages are as lost as any write after it.

Messages: 4-byte big-endian length, then a JSON object (``op`` and its fields); answers have
``ok`` and, when not ok, ``error``.
"""

import json
import time
import socket
import struct
import threading

# Messages -----------------------------------------------------------------------------------------

def encode(message):
    data = json.dumps(message, sort_keys=True).encode()
    return struct.pack(">I", len(data)) + data

def decode(data):
    """(message, rest) of the bytes, or (None, data) when the message is not complete."""
    if len(data) < 4:
        return None, data
    length = struct.unpack(">I", data[:4])[0]
    if len(data) < 4 + length:
        return None, data
    return json.loads(data[4:4 + length].decode()), data[4 + length:]

# Allocator ----------------------------------------------------------------------------------------

class Allocator:
    """Ranges of ``[base, base + length)``, first fit, ``align`` bytes, free neighbors merged."""
    def __init__(self, base, length, align=64):
        assert align % 4 == 0 and base % align == 0
        self.align = align
        self.free  = [(base, length)] # Sorted, not touching.

    def size(self, length):
        return max(self.align, -(-length // self.align)*self.align)

    def alloc(self, length):
        size = self.size(length)
        for i, (start, room) in enumerate(self.free):
            if room >= size:
                if room == size:
                    del self.free[i]
                else:
                    self.free[i] = (start + size, room - size)
                return start
        return None

    def reserve(self, start, length):
        """Takes ``[start, start + size(length))`` (replay of the journal)."""
        size = self.size(length)
        for i, (s, room) in enumerate(self.free):
            if s <= start and start + size <= s + room:
                parts = [(s, start - s), (start + size, s + room - start - size)]
                self.free[i:i + 1] = [p for p in parts if p[1]]
                return
        raise ValueError("Range not free.")

    def release(self, start, length):
        size = self.size(length)
        for s, room in self.free:
            assert start + size <= s or s + room <= start, "Range freed twice."
        merged = []
        for s, room in sorted(self.free + [(start, size)]):
            if merged and merged[-1][0] + merged[-1][1] == s:
                merged[-1] = (merged[-1][0], merged[-1][1] + room)
            else:
                merged.append((s, room))
        self.free = merged

    def available(self):
        return sum(room for _, room in self.free)

# Connection Manager -------------------------------------------------------------------------------

class ConnectionManager:
    """Connections and keys of the tenants of ``server`` (``MemoryServer``).

    ``qps``: queue pairs of the target. ``mac``, ``ip``, ``pmtu``: of the target. ``clock``: seconds
    (``time.monotonic``). ``run``: runs a generator of the driver to its end (the driver on
    hardware, a stub in the tests); without it the changes of the target wait in ``actions``
    for ``apply()`` (a simulation runs them).
    """
    def __init__(self, server, qps, mac, ip, pmtu=1024, lease=1.0, align=64, clock=time.monotonic, run=None):
        self.server  = server
        self.mac     = mac
        self.ip      = ip
        self.pmtu    = pmtu
        self.lease   = lease
        self.align   = align
        self.clock   = clock
        self.run     = run
        self.free_qps = list(range(qps))
        self.qps     = {}   # Index -> (tenant, client QPN).
        self.tenants = {}   # Name -> dict(pd, region, allocator, keys, pending).
        self.quarantine = [] # (time, tenant, address, length): given back after the leases.
        self.actions = []
        self.version = 0
        self.lock    = threading.Lock()

    # Target.

    def _target(self, generator):
        if self.run is not None:
            self.run(generator)
        else:
            self.actions.append(generator)

    def apply(self):
        """Runs the changes of the target waiting (simulation)."""
        while self.actions:
            yield from self.actions.pop(0)

    # Tenants.

    def add_tenant(self, name, pd, region_index, rkey, va, length):
        """Tenant ``name``: protection domain ``pd``, one region (writes the journal)."""
        assert name not in self.tenants
        def generator():
            yield from self.server.region(region_index, rkey, va, length, access=3, pd=pd)
            self.server.journal.append(dict(kind="tenant", name=name, pd=pd, region=region_index))
        self._target(generator())
        self._tenant(name, pd, dict(index=region_index, rkey=rkey, va=va, length=length))

    def _tenant(self, name, pd, region):
        self.tenants[name] = dict(pd=pd, region=region,
            allocator=Allocator(region["va"], region["length"], self.align), keys={}, pending={})

    # Requests.

    def handle(self, message):
        with self.lock:
            self._release()
            try:
                op = message.get("op")
                handler = getattr(self, "_op_" + str(op), None)
                if handler is None:
                    raise ValueError(f"Unknown operation {op!r}.")
                reply = handler(message)
                reply["ok"] = True
                return reply
            except (ValueError, KeyError) as error:
                return dict(ok=False, error=str(error))

    def _tenant_of(self, message):
        name = message["tenant"]
        if name not in self.tenants:
            raise ValueError(f"Unknown tenant {name!r}.")
        return self.tenants[name]

    def _op_connect(self, m):
        tenant = self._tenant_of(m)
        if not self.free_qps:
            raise ValueError("No queue pair left.")
        index = self.free_qps.pop(0)
        self.qps[index] = (m["tenant"], m["qpn"])
        self._target(self.server.connect(index, m["qpn"], m["mac"], m["ip"], m["sport"], m["psn"],
            tenant["pd"]))
        region = tenant["region"]
        return dict(qpn=self.server.qpn_base + index, mac=self.mac, ip=self.ip, pmtu=self.pmtu,
            rkey=region["rkey"], va=region["va"], length=region["length"])

    def _op_disconnect(self, m):
        index = m["qpn"] - self.server.qpn_base
        if index not in self.qps:
            raise ValueError("Queue pair not connected.")
        del self.qps[index]
        self.free_qps.append(index)
        self._target(self.server.driver.set_qp(index, 0, 0, 0, 0, 0, 0, 0))
        return {}

    def _op_alloc(self, m):
        tenant = self._tenant_of(m)
        length = int(m["length"])
        if length <= 0 or length % 4:
            raise ValueError("Length must be a positive multiple of 4 bytes.")
        va = tenant["allocator"].alloc(length)
        if va is None:
            raise ValueError("No space left.")
        self.version += 1
        tenant["pending"][(m["key"], self.version)] = (va, length)
        self.server.journal.append(dict(kind="kv_alloc", tenant=m["tenant"], key=m["key"],
            version=self.version, va=va, length=length))
        return dict(key=m["key"], version=self.version, va=va, length=length,
            rkey=tenant["region"]["rkey"])

    def _op_commit(self, m):
        tenant = self._tenant_of(m)
        place = tenant["pending"].pop((m["key"], m["version"]), None)
        if place is None:
            raise ValueError("No such allocation.")
        old = tenant["keys"].get(m["key"])
        tenant["keys"][m["key"]] = (m["version"],) + place
        if old is not None:
            self._retire(m["tenant"], old[1], old[2])
        self.server.journal.append(dict(kind="kv_commit", tenant=m["tenant"], key=m["key"],
            version=m["version"]))
        return {}

    def _op_lookup(self, m):
        tenant = self._tenant_of(m)
        if m["key"] not in tenant["keys"]:
            raise ValueError("No such key.")
        version, va, length = tenant["keys"][m["key"]]
        return dict(key=m["key"], version=version, va=va, length=length,
            rkey=tenant["region"]["rkey"], lease=self.lease)

    def _op_free(self, m):
        tenant = self._tenant_of(m)
        if m["key"] not in tenant["keys"]:
            raise ValueError("No such key.")
        version, va, length = tenant["keys"].pop(m["key"])
        self._retire(m["tenant"], va, length)
        self.server.journal.append(dict(kind="kv_free", tenant=m["tenant"], key=m["key"]))
        return {}

    def _op_stats(self, m):
        tenant = self._tenant_of(m)
        return dict(keys=len(tenant["keys"]), pending=len(tenant["pending"]),
            available=tenant["allocator"].available(), quarantine=len(self.quarantine))

    def _retire(self, name, va, length):
        self.quarantine.append((self.clock() + self.lease, name, va, length))

    def _release(self):
        now  = self.clock()
        keep = []
        for until, name, va, length in self.quarantine:
            if until <= now:
                self.tenants[name]["allocator"].release(va, length)
            else:
                keep.append((until, name, va, length))
        self.quarantine = keep

    # Power loss.

    @classmethod
    def recover(cls, server, journal, qps, mac, ip, **kwargs):
        """Tenants and committed keys from the journal (after ``MemoryServer.recover``, which
        installed the regions)."""
        manager = cls(server, qps, mac, ip, **kwargs)
        pending = {}
        for record in journal.records:
            kind = record["kind"]
            if kind == "tenant":
                region = server.regions[record["region"]]
                manager._tenant(record["name"], record["pd"], region)
            elif kind == "kv_alloc":
                pending[(record["tenant"], record["key"], record["version"])] = (record["va"], record["length"])
                manager.version = max(manager.version, record["version"])
            elif kind == "kv_commit":
                tenant = manager.tenants[record["tenant"]]
                va, length = pending.pop((record["tenant"], record["key"], record["version"]))
                tenant["keys"][record["key"]] = (record["version"], va, length)
            elif kind == "kv_free":
                manager.tenants[record["tenant"]]["keys"].pop(record["key"])
        # Space in use: the committed values only (no address of before the power loss is
        # still held by a client).
        for tenant in manager.tenants.values():
            for version, va, length in tenant["keys"].values():
                tenant["allocator"].reserve(va, length)
        return manager

# TCP ----------------------------------------------------------------------------------------------

class Server:
    """``manager`` on a TCP socket, one thread per client."""
    def __init__(self, manager, host="127.0.0.1", port=0):
        self.manager = manager
        self.sock = socket.create_server((host, port))
        self.address = self.sock.getsockname()
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def _accept(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn):
        data = b""
        with conn:
            while True:
                chunk = conn.recv(65536)
                if not chunk:
                    return
                data += chunk
                while True:
                    message, data = decode(data)
                    if message is None:
                        break
                    conn.sendall(encode(self.manager.handle(message)))

    def close(self):
        self.sock.close()

class Client:
    """Client side of the exchange (the library of a GPU server)."""
    def __init__(self, address, timeout=5.0):
        self.sock = socket.create_connection(address, timeout=timeout)
        self.data = b""

    def request(self, **message):
        self.sock.sendall(encode(message))
        while True:
            reply, self.data = decode(self.data)
            if reply is not None:
                return reply
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("Server closed the connection.")
            self.data += chunk

    def close(self):
        self.sock.close()
