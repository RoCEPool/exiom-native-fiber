#!/usr/bin/env python3

#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Connection exchange and addresses by key (exiom/connect.py).

Without simulation: messages, allocator, keys (versions, leases, journal and recovery), the
exchange over a real TCP socket. With the RoCE target: two tenants connect, store and read values
by key (lookup, then one RDMA READ by address); a tenant cannot reach the region of the other.
"""

import os
import tempfile
import unittest

from migen import *

from litex.gen import *

from exiom.roce    import *
from exiom.tier    import *
from exiom.connect import *

from test.test_fabric_roce import (RoCEDUT, Message, Requester, exchange, pattern, MAC, IP, PAGE,
    WINDOW_BITS, HOST_MAC, HOST_IP, SPORT, QPN)
from test.test_fabric_tier import FRAMES, with_host
from test.fabric_sim       import run_simulation

VA_A, VA_B = 0x1000_0000, 0x2000_0000
RKEY_A, RKEY_B = 0xa0a0, 0xb0b0

class StubDriver:
    """Driver without a target: records the calls."""
    def __init__(self):
        self.calls = []
    def __getattr__(self, name):
        def call(*args):
            self.calls.append((name,) + args)
            return
            yield
        return call

def drain(generator):
    """Runs a generator of the stub driver, returns its value."""
    try:
        while True:
            next(generator)
    except StopIteration as stop:
        return stop.value

class Clock:
    def __init__(self):
        self.now = 0.0
    def __call__(self):
        return self.now

def stub_manager(journal=None, clock=None, lease=1.0):
    journal = journal or Journal()
    server  = MemoryServer(StubDriver(), FRAMES, PAGE, 16, journal, NVMeStore(PAGE), qpn_base=QPN)
    manager = ConnectionManager(server, qps=2, mac=MAC, ip=IP, lease=lease, clock=clock or Clock(), run=drain)
    manager.add_tenant("a", 1, 0, RKEY_A, VA_A, 4*PAGE)
    return manager

class TestConnect(unittest.TestCase):
    def test_messages(self):
        data = encode(dict(op="lookup", key="x")) + encode(dict(op="free", key="y"))
        message, rest = decode(data[:6])
        self.assertIsNone(message)
        message, rest = decode(data)
        self.assertEqual(message, dict(op="lookup", key="x"))
        message, rest = decode(rest)
        self.assertEqual((message, rest), (dict(op="free", key="y"), b""))

    def test_allocator(self):
        a = Allocator(0x1000, 1024, align=64)
        x = a.alloc(10)    # 64 bytes.
        y = a.alloc(200)   # 256 bytes.
        z = a.alloc(64)
        self.assertEqual((x, y, z), (0x1000, 0x1040, 0x1140))
        self.assertIsNone(a.alloc(1024))
        a.release(y, 200)
        self.assertEqual(a.alloc(100), 0x1040) # First fit, in the hole.
        a.release(x, 10)
        a.release(0x1040, 100)
        self.assertEqual(a.free, [(0x1000, 320), (0x1180, 640)])
        a.release(z, 64)
        self.assertEqual(a.free, [(0x1000, 1024)]) # Everything merged back.
        with self.assertRaises(AssertionError):
            a.release(0x1000, 64) # Twice.
        a.reserve(0x1100, 64)
        self.assertEqual(a.available(), 1024 - 64)
        with self.assertRaises(ValueError):
            a.reserve(0x1100, 64)

    def test_keys(self):
        clock = Clock()
        m = stub_manager(clock=clock, lease=2.0)
        r = m.handle(dict(op="alloc", tenant="a", key="k", length=100))
        self.assertTrue(r["ok"])
        self.assertEqual((r["va"], r["rkey"], r["length"]), (VA_A, RKEY_A, 100))
        # Not visible before its commit.
        self.assertFalse(m.handle(dict(op="lookup", tenant="a", key="k"))["ok"])
        self.assertTrue(m.handle(dict(op="commit", tenant="a", key="k", version=r["version"]))["ok"])
        l = m.handle(dict(op="lookup", tenant="a", key="k"))
        self.assertEqual((l["va"], l["version"], l["lease"]), (VA_A, r["version"], 2.0))
        # New value: a new place; the old one is not given again before the lease ends.
        r2 = m.handle(dict(op="alloc", tenant="a", key="k", length=100))
        self.assertNotEqual(r2["va"], VA_A)
        m.handle(dict(op="commit", tenant="a", key="k", version=r2["version"]))
        self.assertEqual(m.handle(dict(op="lookup", tenant="a", key="k"))["va"], r2["va"])
        r3 = m.handle(dict(op="alloc", tenant="a", key="other", length=64))
        self.assertNotEqual(r3["va"], VA_A) # Still in quarantine.
        clock.now = 2.5
        r4 = m.handle(dict(op="alloc", tenant="a", key="third", length=64))
        self.assertEqual(r4["va"], VA_A)    # Given back.
        # Errors.
        for message in [dict(op="nothing"), dict(op="lookup", tenant="z", key="k"),
                        dict(op="commit", tenant="a", key="k", version=999),
                        dict(op="alloc", tenant="a", key="k", length=6),
                        dict(op="alloc", tenant="a", key="big", length=8*PAGE),
                        dict(op="free", tenant="a", key="nope")]:
            reply = m.handle(message)
            self.assertFalse(reply["ok"], message)
            self.assertIn("error", reply)
        self.assertTrue(m.handle(dict(op="free", tenant="a", key="k"))["ok"])
        self.assertFalse(m.handle(dict(op="lookup", tenant="a", key="k"))["ok"])

    def test_recovery(self):
        # Journal in a file: committed keys come back, allocations not committed do not.
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "journal")
            m = stub_manager(journal=Journal(path))
            done = []
            for key in ["x", "y", "z"]:
                r = m.handle(dict(op="alloc", tenant="a", key=key, length=128))
                m.handle(dict(op="commit", tenant="a", key=key, version=r["version"]))
                done.append(r)
            m.handle(dict(op="free", tenant="a", key="y"))
            m.handle(dict(op="alloc", tenant="a", key="w", length=128)) # Not committed.
            lookups = {k: m.handle(dict(op="lookup", tenant="a", key=k)) for k in ["x", "z"]}

            journal = Journal.load(path)
            server  = drain(MemoryServer.recover(StubDriver(), FRAMES, PAGE, 16, journal, NVMeStore(PAGE), qpn_base=QPN))
            m2 = ConnectionManager.recover(server, journal, qps=2, mac=MAC, ip=IP, clock=Clock(), run=drain)
            for key, before in lookups.items():
                self.assertEqual(m2.handle(dict(op="lookup", tenant="a", key=key)), before)
            self.assertFalse(m2.handle(dict(op="lookup", tenant="a", key="y"))["ok"])
            self.assertFalse(m2.handle(dict(op="lookup", tenant="a", key="w"))["ok"])
            stats = m2.handle(dict(op="stats", tenant="a"))
            self.assertEqual((stats["keys"], stats["available"]), (2, 4*PAGE - 2*128))
            # New versions after the old ones.
            self.assertGreater(m2.handle(dict(op="alloc", tenant="a", key="v", length=4))["version"], done[-1]["version"])

    def test_tcp(self):
        # The exchange over a TCP socket: connect, store, read back the place, disconnect.
        m = stub_manager()
        server = Server(m)
        try:
            client = Client(server.address)
            r = client.request(op="connect", tenant="a", qpn=0x33, psn=0x4567, mac=HOST_MAC, ip=HOST_IP, sport=SPORT)
            self.assertTrue(r["ok"])
            self.assertEqual((r["qpn"], r["rkey"], r["va"], r["length"], r["mac"], r["ip"]),
                (QPN, RKEY_A, VA_A, 4*PAGE, MAC, IP))
            self.assertIn(("set_qp", 0, 1, 0x33, HOST_MAC, HOST_IP, SPORT, 0x4567, 1), m.server.driver.calls)
            a = client.request(op="alloc", tenant="a", key="blk", length=256)
            self.assertTrue(client.request(op="commit", tenant="a", key="blk", version=a["version"])["ok"])
            self.assertEqual(client.request(op="lookup", tenant="a", key="blk")["va"], a["va"])
            # Second client, second queue pair; a third one finds none left.
            other = Client(server.address)
            self.assertEqual(other.request(op="connect", tenant="a", qpn=0x34, psn=1, mac=1, ip=2, sport=3)["qpn"], QPN + 1)
            third = Client(server.address)
            self.assertFalse(third.request(op="connect", tenant="a", qpn=0x35, psn=1, mac=1, ip=2, sport=3)["ok"])
            self.assertTrue(other.request(op="disconnect", qpn=QPN + 1)["ok"])
            self.assertEqual(third.request(op="connect", tenant="a", qpn=0x35, psn=1, mac=1, ip=2, sport=3)["qpn"], QPN + 1)
            for c in (client, other, third):
                c.close()
        finally:
            server.close()

    def test_tenants_on_the_target(self):
        # Two tenants on the RoCE target: values stored and read by key (one RDMA READ by
        # address); a tenant cannot reach the region of the other, even with its R_Key.
        dut = RoCEDUT()
        journal, nvme = Journal(), NVMeStore(PAGE)
        driver  = SimDriver(dut.roce, {n: mem.mem for n, mem in enumerate(dut.mems)}, WINDOW_BITS)
        server  = MemoryServer(driver, FRAMES, PAGE, 16, journal, nvme, qpn_base=QPN)
        manager = ConnectionManager(server, qps=2, mac=MAC, ip=IP, clock=Clock())
        manager.add_tenant("a", 1, 0, RKEY_A, VA_A, 4*PAGE)
        manager.add_tenant("b", 2, 1, RKEY_B, VA_B, 4*PAGE)
        psn = {"a": 0x1000, "b": 0x8000}
        def run(tenant, qpn, host_qpn, messages):
            requester = Requester(messages, psn=psn[tenant], timeout=3000, dqpn=qpn, host_qpn=host_qpn)
            yield from with_host(server, exchange(self, dut, requester, max_cycles=400000))
            psn[tenant] = psn[tenant] + len(requester.packets) + sum(n - 1 for *_, n in requester.packets if n > 1)
            return requester
        def put(tenant, qpn, host_qpn, key, value):
            r = manager.handle(dict(op="alloc", tenant=tenant, key=key, length=len(value)))
            self.assertTrue(r["ok"], r)
            m = Message("write", r["va"], value, rkey=r["rkey"])
            yield from run(tenant, qpn, host_qpn, [m])
            self.assertEqual(m.result, ROCE_SYN_ACK)
            self.assertTrue(manager.handle(dict(op="commit", tenant=tenant, key=key, version=r["version"]))["ok"])
        def get(tenant, qpn, host_qpn, key):
            r = manager.handle(dict(op="lookup", tenant=tenant, key=key))
            self.assertTrue(r["ok"], r)
            m = Message("read", r["va"], length=r["length"], rkey=r["rkey"])
            yield from run(tenant, qpn, host_qpn, [m])
            self.assertEqual(m.result, ROCE_SYN_ACK)
            return bytes(m.got)
        def generator():
            yield from manager.apply()
            ca = manager.handle(dict(op="connect", tenant="a", qpn=0x21, psn=psn["a"], mac=HOST_MAC, ip=HOST_IP, sport=SPORT))
            cb = manager.handle(dict(op="connect", tenant="b", qpn=0x22, psn=psn["b"], mac=HOST_MAC, ip=HOST_IP, sport=SPORT))
            yield from manager.apply()
            self.assertEqual((ca["qpn"], cb["qpn"]), (QPN, QPN + 1))
            va, vb = pattern(300, salt=1), pattern(520, salt=2)
            yield from put("a", ca["qpn"], 0x21, "kv-0001", va)
            yield from put("b", cb["qpn"], 0x22, "kv-0001", vb) # Same key, other tenant.
            self.assertEqual((yield from get("a", ca["qpn"], 0x21, "kv-0001")), va)
            self.assertEqual((yield from get("b", cb["qpn"], 0x22, "kv-0001")), vb)
            # Tenant b with the R_Key and the address of tenant a: not its region.
            r = manager.handle(dict(op="lookup", tenant="a", key="kv-0001"))
            m = Message("read", r["va"], length=r["length"], rkey=r["rkey"])
            yield from run("b", cb["qpn"], 0x22, [m])
            self.assertEqual(m.result, ROCE_NAK_ACCESS)
            m = Message("write", r["va"], pattern(8), rkey=r["rkey"])
            yield from run("b", cb["qpn"], 0x22, [m])
            self.assertEqual(m.result, ROCE_NAK_ACCESS)
            self.assertEqual((yield from get("a", ca["qpn"], 0x21, "kv-0001")), va)
        run_simulation(dut, generator())

if __name__ == "__main__":
    unittest.main()
