# Fabric: remote memory access between symmetric nodes

Project layer above the imported LiteICLink code (`exiom/fabric.py`, `exiom/topology.py`,
`exiom/network.py`). It replaces the single-initiator scheme of [RMA.md](RMA.md) and is the base
of the multi-node fabric: 2 -> 4 -> 8 -> 16 nodes.

Every node is:

- a **Requester**: sends read/write requests to other nodes, several can be in flight;
- a **Responder**: executes the requests it receives on its memory window and answers them;
- a **switch**: forwards the packets of other nodes between its ports.

```
                 requests / results                      memory window
                        |                                      |
                  +-----------+      +----------+       +-----------+
                  | Requester | <--> |  switch  | <-->  | Responder |
                  +-----------+      +----------+       +-----------+
                                      |  |  |  |
                                     ports (links to the neighbors)
```

## Topology: two tiers

The way large SMP servers interconnect their processors (processors of a book all connected
together, books connected together):

- Tier 1: the 4 nodes of a group are all connected to each other (3 ports).
- Tier 2: groups are connected to each other by the remaining port(s) of their nodes.

| Nodes | Groups | Ports per node | Links between two nodes, at most |
|-------|--------|----------------|----------------------------------|
| 2 | 1 (half) | 1 | 1 |
| 4 | 1 | 3 | 1 |
| 8 | 2 | 4 | 3 |
| 16 | 4 | 4 | 3 |
| 32 | 8 | 5 | 3 |

Cabling only depends on the size the fabric is designed for (16 nodes by default), not on the
nodes present: growing 2 -> 4 -> 8 -> 16 only adds cables (`test_two_tier_growth_only_adds_cables`).
With 8 nodes of a fabric cabled for 16, the two groups are connected by one cable.

`exiom/topology.py` builds the topology (`two_tier()`, and `hypercube()` to check that nothing
depends on the topology) and the static route tables (shortest paths).

## Packet

32-bit words, 2 to 4 words.

| Word | Content |
|------|---------|
| 0 (route) | `[7:0]` destination node, `[15:8]` source node, `[23:16]` hop limit, `[24]` response, `[25]` flow control, `[31:28]` magic |
| 1 (transaction) | `[7:0]` tag, `[11:8]` byte enables (request) or status (response), `[12]` write |
| 2 | Word address in the destination window (request), read data (response) |
| 3 | Write data (write request) |

- Word 0 is all a node needs to forward a packet. Up to 256 nodes.
- Hop limit: number of links of the longest route at the source, decremented by each forwarding
  node.
- Tag: slot of the request in the Requester and generation of this slot. A response arriving
  after its request timed out does not match the request that reuses the slot and is dropped.

## Flow control: no packet dropped, no deadlock

- Each port stores the packets it receives in one queue per class. The class of a packet is
  (request or response, links already crossed).
- **Credits**: a node only sends a packet when the queue of its class has room on the other side
  of the link. Ports exchange flow control packets with, per class, the packets received (R) and
  the packets accepted in total (L = R + free places); the sender compares L with the packets it
  sent (S). A packet that has to wait stays where it is and does not block the other classes or
  the other ports.
- **No deadlock**: a packet waiting in a queue "h links crossed" only waits for a queue "h + 1
  links crossed" of the next node; requests wait for responses, and responses are always accepted
  by their Requester. No cycle of packets waiting for each other can form, whatever the topology
  and the routes.
- **Lost packets**: a packet lost on a link (CRC error) is counted as sent and never as received.
  When a class has not sent anything for `4*fc_period` cycles, S is set back to the last R
  received (`resyncs` counter).
- Link down: the queues and the credits of the port are reset, packets for this port are
  discarded instead of stalling the node.

Cost: `2 * max_hops` queues per port (6 with 3 links at most), `queue_depth` packets each.

## Semantics

- Every request gets exactly one result, identified by its tag. A failed request only fails
  itself.
- Writes are acknowledged by the node that executed them.
- Requests from one node to another are executed in the order they were accepted; results are
  returned as they complete.
- Up to `requests` requests in flight per node (8 by default); the Requester applies
  backpressure beyond that.

| Status | From | Meaning |
|--------|------|---------|
| `FABRIC_OK` | Destination | Executed |
| `FABRIC_RANGE` | Destination | Outside its window, not executed |
| `FABRIC_RO` | Destination | Write to a read-only window, not executed |
| `FABRIC_BUS` | Destination | Memory bus error |
| `FABRIC_TIMEOUT` | Destination | Memory did not answer |
| `FABRIC_NODE` | Requester or node on the way | No route to the destination, or the next link of the route is down: not executed |
| `FABRIC_LINK` | Requester | First link of the route down: not confirmed, may have been executed |
| `FABRIC_NO_RESPONSE` | Requester | No response within the timeout: not confirmed, may have been executed |

Node or link down: routes are static, there is no rerouting. Requests to the node, or whose
route crosses it, end with `FABRIC_LINK` (seen by the Requester) or `FABRIC_NODE` (seen by a node
on the way); a request whose response route crosses it ends with `FABRIC_NO_RESPONSE`; all the
other requests are served.

## Interfaces

- `FabricNode(node_id, size, ports, routes, max_hops, ...)`: one node. `requester.sink`
  (requests: `we`, `dst`, `adr`, `dat_w`, `sel`; `requester.tag` gives the tag of the request
  being accepted), `requester.source` (results: `tag`, `we`, `status`, `dat_r`), `responder.mem`
  (memory window).
- `node.ports[p].sink` / `.source`: packet streams of port p. The fabric does not depend on
  SerWB: `node.connect_serwb(core, port=p)` attaches a port to a `SERWBCore` (SerWB port 2, next
  to the SerWB MMAP port); the networks of the tests connect the streams directly.
- `FabricNetwork(topology, size, ...)`: the nodes of a topology and their links.
- `FabricWishbone(node, dst, size, origin)`: Wishbone slave on the window of one remote node, one
  access at a time.

## Limits

- No retransmission: errors are reported, the user decides.
- Static routes: no rerouting around a link or node that is down.
- At most 3 links between two nodes (6 classes per port, one flow control packet).
- With a SerWB link, use CRC on the cores and `with_auto_retrain=True` on the Master PHY.
- The switch looks at one input per cycle: sized for correctness, not for throughput.

## Verification

```sh
python3 -m pytest -v test/test_topology.py test/test_fabric.py test/test_fabric_network.py \
    test/test_fabric_bench.py test/test_fabric_sim.py
```

- `test_topology.py`: two-tier sizes (2 to 32 nodes), growth only adds cables, routes, node down.
- `test_fabric.py`: two nodes, connected directly and through SerWB (word-level PHY model):
  requests in flight, queueing, both directions, errors isolated to their request, lost request,
  lost response, late response, link down (with a sweep of the moment the link is lost),
  backpressure with the smallest buffers, credit recovery, invalid packets, Wishbone.
- `test_fabric_network.py`: 4 nodes (two-tier and hypercube) and 8 nodes: all nodes to all nodes,
  random traffic checked against a model, hotspot, node cut and back, smallest buffers.
- `test_fabric_bench.py`: the benches of `bench/fabric/sim.py` with the Migen simulator: two nodes
  with the bit-level generic PHYs, and 4-node networks with one hardware sequencer per node.
- `test_fabric_sim.py`: the same benches with Verilator (runs in CI): two nodes bit-level, and
  networks of 4, 8 and 16 nodes.

Scope: simulation only. No FPGA synthesis, timing closure or hardware test. Links between more
than two nodes are word-level (no PHY model).
