# Fabric: tagged requests between symmetric nodes

Second project layer (`exiom/fabric.py`). It replaces the single-initiator scheme of
[RMA.md](RMA.md) for anything beyond one requester and one target, and is the base of the
multi-node fabric (2 -> 4 -> 8 -> 16 nodes).

```
requests -> +-----------+           +-----------+ <- requests
results  <- | Requester |  packets  | Requester | -> results
            |   Node A  | <-------> |   Node B  |
  memory <- | Responder |           | Responder | -> memory
            +-----------+           +-----------+
```

Why: the acknowledged writes of RMA compare a write count kept by the target, which only works
with one requester per target and one request at a time. Here every request carries its source
node and a tag, and every request (reads and writes) gets its own response.

## Packet

32-bit words. The format is sized for the multi-node fabric so that it does not change when
nodes are added.

| Word | Content |
|------|---------|
| 0 (route) | `[7:0]` destination node, `[15:8]` source node, `[23:16]` hop limit, `[24]` response, `[31:28]` magic |
| 1 (transaction) | `[7:0]` tag, `[11:8]` byte enables (request) or status (response), `[12]` write |
| 2 | Word address in the destination window (request), read data (response) |
| 3 | Write data (write request) |

- Word 0 is all a node needs to forward a packet; the transaction is opaque to forwarding.
- Up to 256 nodes. The hop limit is written but not used yet (no forwarding with two nodes).
- Tag: slot of the request in the Requester and generation of this slot. A response arriving
  after its request timed out does not match the request that reuses the slot and is dropped.

## Semantics

- Every request gets exactly one result, identified by its tag. A failed request only fails
  itself.
- Writes are acknowledged by the node that executed them.
- Requests to one node are executed in the order they were accepted; results are returned as
  they complete.
- Up to `requests` requests in flight per node (8 by default); the Requester applies
  backpressure beyond that.

| Status | From | Meaning |
|--------|------|---------|
| `FABRIC_OK` | Destination | Executed |
| `FABRIC_RANGE` | Destination | Outside its window, not executed |
| `FABRIC_RO` | Destination | Write to a read-only window, not executed |
| `FABRIC_BUS` | Destination | Memory bus error |
| `FABRIC_TIMEOUT` | Destination | Memory did not answer |
| `FABRIC_NODE` | Receiving node | Destination node not reachable from there |
| `FABRIC_NO_RESPONSE` | Requester | No response within the timeout: not confirmed, may have been executed |
| `FABRIC_LINK` | Requester | Link down: not confirmed, may have been executed |

## Interfaces

- `FabricNode(node_id, size, requests=8, ...)`: one node. `requester.sink` (requests: `we`,
  `dst`, `adr`, `dat_w`, `sel`; `requester.tag` gives the tag of the request being accepted),
  `requester.source` (results: `tag`, `we`, `status`, `dat_r`), `responder.mem` (memory window).
- `node.sink` / `node.source`: packet streams to the neighbor. The fabric does not depend on
  SerWB: `node.connect_serwb(core)` attaches them to a port of a `SERWBCore` (port 2, next to the
  SerWB MMAP port), and the tests also connect two nodes directly.
- `FabricWishbone(node, dst, size, origin)`: Wishbone slave on the window of one remote node, one
  access at a time.

## Limits

- No flow control between nodes: the Responder request buffer (`buffer_depth` words, 4 per
  request) must hold what its neighbor can have in flight. Requests that do not fit are dropped
  whole, counted (`rx_drops`) and end with `FABRIC_NO_RESPONSE` on their Requester
  (`test_request_buffer_overflow`). Credit-based flow control is the next step.
- No retransmission: errors are reported, the user decides.
- Two nodes: a request for another node is answered with `FABRIC_NODE`. Forwarding and routing
  come with the multi-node step.
- With a SerWB link, use CRC on the cores and `with_auto_retrain=True` on the Master PHY.

## Verification

```sh
python3 -m pytest -v test/test_fabric.py test/test_fabric_bench.py test/test_fabric_sim.py
```

- `test_fabric.py`: two nodes connected directly and through SerWB with a word-level PHY model:
  requests in flight, queueing beyond the slots, both directions at once, errors isolated to
  their request, lost request, lost response, late response, link down (and a sweep of the moment
  the link is lost), request buffer overflow, invalid packets, Wishbone access.
- `test_fabric_bench.py`: two nodes with the bit-level generic PHYs (CRC, auto-retrain), hardware
  sequencers on both nodes, Migen simulator.
- `test_fabric_sim.py`: the same bench with Verilator (runs in CI).

Scope: simulation only. No FPGA synthesis, timing closure or hardware test.
