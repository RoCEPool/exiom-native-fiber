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
depends on the topology) and the route tables at reset.

## Routes: following the state of the fabric

- Each node keeps, per destination, the cost of each of its ports: the links to the destination
  through this port. A packet leaves by the port with the lowest cost among those that are up,
  are not the port it came from, and can reach the destination within the hop limit.
- Each node regularly sends to each neighbor its distance to every destination without going
  through this neighbor (route packets, also sent when a distance changes or a link comes up).
  The cost of a port is the distance received on it plus one; a link that goes down makes its
  port unusable until its neighbor sends its distances again.
- When a link or a node goes down, the change spreads from neighbor to neighbor and packets take
  the shortest path that is left. Requests for a node that cannot be reached any more are
  answered at once with `FABRIC_NODE`, without being sent.
- Hop limit: longest shortest path of the fabric plus 2 links of detour (6 at most).

Paths left with one node down (`test_one_node_down`):

| Fabric | Other nodes still reach each other | Longest path |
|--------|------------------------------------|--------------|
| 4 nodes | Yes | 1 link |
| 8 nodes, cabled for 16 | Not when it is one of the two nodes with the cable between the groups | 3 links |
| 8 nodes, cabled for 8 (4 cables between the groups) | Yes | 2 links |
| 16 nodes | Yes | 5 links |
| 32 nodes | Yes | 4 links |

## Packet

32-bit words, 2 to 4 words. The number of words follows from the kind of packet (write request:
4, read request: 3, read response: 3, write response: 2); a port does not accept a packet that
has another number of words.

Links carry `link_width` bits per cycle: 32 (one word per cycle, the SerWB link), 64 or 128 (a
whole packet per cycle).

| Word | Content |
|------|---------|
| 0 (route) | `[7:0]` destination node, `[15:8]` source node, `[18:16]` hop limit, `[23:19]` sequence number on the link, `[24]` response, `[25]` flow control, `[26]` routes, `[31:28]` magic |
| 1 (transaction) | `[7:0]` tag, `[11:8]` byte enables (request) or status (response), `[12]` write |
| 2 | Word address in the destination window (request), read data (response) |
| 3 | Write data (write request) |

- Word 0 is all a node needs to forward a packet. Up to 256 nodes.
- Hop limit: links the packet may cross, decremented by each forwarding node.
- Bit `[26]` of word 0 marks a route packet (word 1: first destination, words 2 and 3: 10
  distances of 3 bits each); like flow control packets, it only goes to the neighbor.
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
- A packet that does not fit in its queue is not accepted, and is sent again (see below).
- Link down: the queues and the credits of the port are reset, packets for this port are
  discarded instead of stalling the node.

Cost: `2 * max_hops` queues per port (10 with a hop limit of 5), `queue_depth` packets each.

## Retransmission: packets lost on a link are sent again

- Each port numbers the packets it sends (5-bit sequence number) and keeps up to 8 of them until
  its neighbor has acknowledged them. Flow control packets carry the acknowledgment: the next
  sequence number the neighbor expects.
- A port only accepts the packet it expects. After a lost packet, the following ones are
  discarded and the sender starts again from the missing one: at once when a later packet shows
  the gap (the flow control packet asks for it), or after `retry_timeout` cycles without
  acknowledgment (last packet lost, or acknowledgment lost).
- Result: packets are delivered once and in order on every link, so a request is executed once
  and requests from one node to another stay in order, on links that lose packets.
- Flow control and route packets are not numbered: they carry a state, and the next one replaces
  a lost one.
- Not covered: packets in a node or on a link that goes down are lost (their requests end with
  `FABRIC_NODE` or `FABRIC_NO_RESPONSE`), and requests are not sent again end to end. A link that
  stays up without passing any packet is retried forever.

Counters per port: `retransmits`, `rx_discards`.

## Throughput

- Switch: each group of inputs (the Responder, the Requester, each port) proposes one packet per
  cycle, the next of its inputs that has a packet, in turn; each output (each port, the Requester,
  the Responder) takes one of the packets proposed to it, in turn. Every port can receive a packet
  and send a packet in the same cycle. The packets of an input keep their order, and a packet
  that has to wait does not block the others.
- Ports send packets back to back; flow control packets are sent at once when the neighbor has no
  credit left.
- The Requester takes a request per cycle; the Responder executes a request from the head of its
  buffer (a request every two cycles with a memory answering in one).
- With links of 128 bits per cycle a packet takes one cycle on the link.

Requests per 1000 cycles, writes sent as fast as the nodes accept them, word-level links
(`test_throughput` keeps floors about 25 % below):

| Fabric | 32-bit links | 128-bit links |
|--------|--------------|---------------|
| 2 nodes, one way | 148 | 383 |
| 2 nodes, both ways | 177 | 614 |
| 4 nodes, 3 nodes to one | 426 | 490 |
| 4 nodes, all to all | 889 | 1504 |
| 8 nodes (one cable between the groups), all to all | 283 | 839 |
| 16 nodes, all to all | 1022 | 3033 |

Bounds now: a Responder executes a request every two cycles (500 requests per 1000 cycles per
destination node, reached with 3 nodes writing one); a Requester has 8 requests in flight by
default; with 8 nodes of a fabric cabled for 16, the single cable between the two groups.

## Semantics

- Every request gets exactly one result, identified by its tag. A failed request only fails
  itself.
- Writes are acknowledged by the node that executed them.
- Requests from one node to another are executed in the order they were accepted, as long as
  the links of their route do not change state; results are returned as they complete.
- Up to `requests` requests in flight per node (8 by default); the Requester applies
  backpressure beyond that.

| Status | From | Meaning |
|--------|------|---------|
| `FABRIC_OK` | Destination | Executed |
| `FABRIC_RANGE` | Destination | Outside its window, not executed |
| `FABRIC_RO` | Destination | Write to a read-only window, not executed |
| `FABRIC_BUS` | Destination | Memory bus error |
| `FABRIC_TIMEOUT` | Destination | Memory did not answer |
| `FABRIC_NODE` | Requester or node on the way | Destination not reachable (unknown, or no path within the hop limit): not executed |
| `FABRIC_LINK` | Requester | No link up on this node: not confirmed, may have been executed |
| `FABRIC_NO_RESPONSE` | Requester | No response within the timeout: not confirmed, may have been executed |

Node or link down, once the routes have followed (a few route packet periods): requests to a node
that cannot be reached are answered with `FABRIC_NODE`, all the others are served by the paths
that are left. Requests in flight through the link when it goes down, or sent before the routes
have followed, can end with `FABRIC_NODE` or `FABRIC_NO_RESPONSE`.

## Interfaces

- `FabricNode(node_id, size, ports, routes, max_hops, ...)`: one node (`routes`: costs at reset,
  from `Topology.costs()`). `requester.sink`
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

- Retransmission is per link: a request lost with a node or a link that goes down is reported,
  the user decides.
- A node only learns the state of the fabric from its neighbors: while a change spreads,
  requests can fail (see above).
- At most 6 links on a path (12 classes per port, one flow control packet). Distances are 3-bit.
- With a SerWB link, use CRC on the cores and `with_auto_retrain=True` on the Master PHY.
- A Responder executes one request at a time, in two cycles (see Throughput).

## Verification

```sh
python3 -m pytest -v test/test_topology.py test/test_fabric.py test/test_fabric_network.py \
    test/test_fabric_bench.py test/test_fabric_sim.py
```

- `test_topology.py`: two-tier sizes (2 to 32 nodes), growth only adds cables, costs, paths left
  with any one node down.
- `test_fabric.py`: two nodes, connected directly and through SerWB (word-level PHY model):
  requests in flight, queueing, both directions, errors isolated to their request,
  late response, link down (with a sweep of the moment the link is lost), backpressure with the
  smallest buffers, invalid packets, Wishbone. Packets lost: a request, a response, several in
  a row, flow control packets, and one packet out of 8 at random in both directions: everything
  executed once, in order, nothing reported lost.
- `test_fabric_network.py`: 4 nodes (two-tier and hypercube), 8 nodes (cabled for 16 and for 8)
  and 16 nodes: all nodes to all nodes, random traffic checked against a model, hotspot, node cut
  and back, cable cut and back (requests go around), smallest buffers, route packets in several
  parts, and 4 and 8 nodes with one packet out of 10 lost on every link.
- `test_fabric_bench.py`: the benches of `bench/fabric/sim.py` with the Migen simulator: two nodes
  with the bit-level generic PHYs, and 4-node networks with one hardware sequencer per node.
- `test_fabric_sim.py`: the same benches with Verilator (runs in CI): two nodes bit-level, and
  networks of 4 and 8 nodes (32-bit and 128-bit links, with and without losses). The Verilator build of 16 nodes needs more memory than the CI runner
  has since the routes were added; 16 nodes run with the Migen simulator.

`test/fabric_sim.py` runs the Migen simulator with a faster equivalent of its statement grouping
(the original takes minutes to start on 8 nodes and more).

Scope: simulation only. No FPGA synthesis, timing closure or hardware test. Links between more
than two nodes are word-level (no PHY model).
