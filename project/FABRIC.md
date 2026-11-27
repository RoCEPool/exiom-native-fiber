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

### Chassis of 8 nodes, up to 128 nodes

The same rule with `group_size=8`: one chassis of 8 nodes is one group (7 ports per node inside
the chassis), the chassis are cabled together with `inter_ports` more ports per node. Computed by
`exiom/topology.py` (`test_topology.py::test_chassis_of_8`), shortest paths, every node sending as
much to every other node:

| Nodes (chassis) | Ports per node | Cables | Links between two nodes: most / average | Most with one node down | Busiest link (pairs of nodes) |
|---|---|---|---|---|---|
| 16 (2)   | 8  | 64  | 2 / 1.47 | 2 | 8  |
| 32 (4)   | 8  | 126 | 3 / 2.14 | 3 | 38 |
| 32 (4)   | 10 | 160 | 2 / 1.68 | 2 | 8  |
| 128 (16) | 9  | 568 | 3 / 2.65 | 4 | 72 |
| 128 (16) | 11 | 688 | 3 / 2.42 | 3 | 64 |

- Two nodes of the same chassis are always one link apart. The busiest links are the cables
  between the chassis; a link inside a chassis carries at most 34 to 38 pairs at 128 nodes.
- A design of D chassis needs `8 x inter_ports >= D - 1` (a cable between any two chassis):
  `two_tier()` refuses fewer ports. 16 chassis need 2 inter-chassis ports per node.
- Node numbers are 8-bit (256 nodes), the hop limit is 6: 128 nodes fit, with one node down too.
- A fabric cabled for 16 chassis works with 2, 4 and 8 chassis installed; adding chassis only
  adds cables.
- Verified for 128 nodes: topology, routes, paths with a node down (model in Python). Not
  simulated at the packet level (see Verification for the sizes simulated).

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

32-bit words, 2 to 11 words. The number of words follows from the first two words of the packet
(write request: 4, read request: 3, copy request: 6, read response: 3, write response: 2; burst
write request: 3 + words, burst read response: 2 + words); a port does not accept a packet that
has another number of words.

Links carry `link_width` bits per cycle: 32 (one word per cycle, the SerWB link), 64 or 128 (a
whole packet of up to 4 words per cycle; a burst of 8 words takes 3 cycles).

| Word | Content |
|------|---------|
| 0 (route) | `[7:0]` destination node, `[15:8]` source node, `[18:16]` hop limit, `[23:19]` sequence number on the link, `[24]` response, `[25]` flow control, `[26]` routes, `[31:28]` magic |
| 1 (transaction) | `[7:0]` tag, `[11:8]` byte enables or words of the burst - 1 (request), status (response), `[12]` write, `[13]` copy, `[14]` burst, `[31:16]` bits 47 to 32 of the word address (request), `[19:16]` words done (burst response) |
| 2 | Bits 31 to 0 of the word address in the destination window (request), read data or words copied (response) |
| 3 | Write data (write request; a burst goes on in the next words), bits 31 to 0 of the word address to copy to (copy request) |
| 4 | Copy request: `[7:0]` node to copy to, `[31:16]` bits 47 to 32 of the word address to copy to |
| 5 | Copy request: words to copy - 1 |

- Burst: 1 to 8 consecutive words (`FABRIC_BURST_WORDS`) read or written by one request (`burst`,
  `count`, `data` of `requester.sink`; `count` and `data` of the result). The words are accessed
  one after the other until the first error; the response gives the number of words done. A write
  burst of 8 words carries 256 bits of data in 11 words (73 % of the packet, 25 % for a single
  write).
- Word addresses are 48-bit: a window can be up to 2^48 words of 4 bytes. A node of 4 TiB uses
  40 bits; 128 nodes of 4 TiB are 512 TiB.

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

## Copies and global addresses

**Copy request.** A request can ask a node to copy up to 65536 words (256 KiB) of its window to the window
of another node (`copy`, `copy_dst`, `words` of `requester.sink`; on the wire: a write request
with the copy bit, answered like a read with the number of words written). The node that receives
it (`FabricCopy`) reads the words in its own window and writes them to the destination node with
its own burst requests, `copy_window` bursts in flight (default 4): the data goes straight from the
source node to the destination node, whichever node asked. The other nodes forward a copy request
like any request; switches and ports are not modified.

- One copy at a time per source node. A copy request received during another one is refused at
  once with `FABRIC_BUSY`: it never waits in the Responder, so nodes copying to each other cannot
  wait for each other, and the Responder keeps serving the other requests during a copy.
- The requests of a copy share the slots of the Requester of the source node with the requests
  of its user, served in turn.
- Result: first error and words written. Source words outside the window: `FABRIC_RANGE`, nothing
  copied. Error on the way (destination window, node that cannot be reached, memory error): no
  more word is read, the words already written stay. Statuses 8 and 9 in a copy result are those
  of the requests of the source node ("not confirmed").
- Bursts are not written in order: source and destination must not overlap.
- Timeout of a copy request: `copy_timeout` (default 32 x `request_timeout`) plus 8 cycles per
  word to copy.

**Global addresses** (`exiom/pool.py`). The windows of the nodes seen as one address space: global
word address = node x 2^`window_bits` + word in the window (`window_bits` up to 48; 40 for nodes
of 4 TiB). The map is static and computed (every
node owns the same number of addresses): no table to keep consistent between the nodes. Which
address holds what (allocation, migration, reclaim) stays with the software above.

`FabricPool(node, window_bits, chunk)` takes one operation at a time with global addresses:
`POOL_READ`, `POOL_WRITE` (one word), and `POOL_COPY` (`words` words from `adr` to `dst_adr`,
wherever they are). A copy is cut in copy requests of at most `chunk` words, each inside the
window of one source node and one destination node, so a copy can start in a node and go on in
the next ones; a source node that is busy is asked again (`busy_wait`, `busy_retries`).

Cost (simulation, copy of 120 words, cycles per word from the copy request to its result):

| | 32-bit links | 128-bit links |
|---|---|---|
| One request per word (before bursts) | 8.6 | 3.7 to 6.0 |
| Bursts of 8 words | 2.6 | 2.4 |

What limits it now: the Responders access the memory one word every two cycles (the source node
to read, the destination node to write). The links are no longer the limit.

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
- `FabricPool(node, window_bits, chunk)`: reads, writes and copies with global addresses, one
  operation at a time.

## Limits

- Retransmission is per link: a request lost with a node or a link that goes down is reported,
  the user decides.
- A node only learns the state of the fabric from its neighbors: while a change spreads,
  requests can fail (see above).
- At most 6 links on a path (12 classes per port, one flow control packet). Distances are 3-bit.
- With a SerWB link, use CRC on the cores and `with_auto_retrain=True` on the Master PHY.
- A Responder executes one request at a time, in two cycles (see Throughput).
- Copies: one at a time per source node (see Copies and global addresses);
  no entry yet for a host to ask for a copy (the user of `FabricPool` is the test bench).

## Verification

```sh
python3 -m pytest -v test/test_topology.py test/test_fabric.py test/test_fabric_network.py \
    test/test_fabric_copy.py test/test_fabric_burst.py test/test_fabric_address.py \
    test/test_fabric_bench.py test/test_fabric_sim.py
```

- `test_topology.py`: two-tier sizes (2 to 32 nodes), growth only adds cables, costs, paths left
  with any one node down; chassis of 8 nodes up to 128 nodes (cables, longest paths, busiest
  link, a node down, growth from 2 to 16 chassis, too few inter-chassis ports refused).
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
- `test_fabric_copy.py`: 2 and 4 nodes. Copies asked by a third node, by the source, by the
  destination and inside one node (the node that asked executes and forwards nothing); errors
  (source and destination out of the window, nodes that do not exist, memory that does not answer
  or answers with an error); second copy refused while one runs; every node copying to the next
  one at the same time; copies with the other traffic; packets lost on every link; destination
  and source node cut off during a copy. Global addresses: reads and writes, copies longer than a
  copy request and going from a node to the next ones, copy running past the last node, two nodes
  asking the same source node.
- `test_fabric_burst.py`: bursts of every size on 32, 64 and 128-bit links and through SerWB, to
  the other node and to the node itself; errors (out of the window, read-only, memory error or no
  answer in the middle: words before done, words after not); sizes that are not valid; 8 bursts
  in flight in both directions with packets lost; bursts forwarded over 3 links.
- `test_fabric_address.py`: two nodes with windows of 4 TiB (memory model: pattern of the address
  and the last words written): reads, writes and copies at addresses over 32 bits, end of the
  window, global addresses of 128 nodes of 4 TiB, copy from the end of a node to the next one.
- `test_fabric_bench.py`: the benches of `bench/fabric/sim.py` with the Migen simulator: two nodes
  with the bit-level generic PHYs, and 4-node networks with one hardware sequencer per node.
- `test_fabric_sim.py`: the same benches with Verilator (runs in CI): two nodes bit-level, and
  networks of 4 nodes (32-bit and 128-bit links, with and without losses). The Verilator build
  of 8 nodes and more needs more memory than the CI runner has; they run with the Migen
  simulator.

`test/fabric_sim.py` runs the Migen simulator with a faster equivalent of its statement grouping
(the original takes minutes to start on 8 nodes and more), and makes Migen number the objects of
a design without searching them (same names; building 16 nodes of 8 ports: 51 s to 7 s).

Sizes simulated at the packet level: up to 16 nodes. With chassis of 8 nodes (8 ports per node),
16 nodes take about 50 ms per cycle in the Migen simulator; 32 nodes did not finish a few hundred
cycles in 9 minutes, and building 128 nodes runs out of memory. 128 nodes are verified by the
topology model only.

Every push and pull request runs the tests that cover each feature at a small scale (2 and 4
nodes, 8 nodes for forwarding over 3 links). The longest simulations (8 and 16 nodes with a node
or a cable cut, networks with losses, wide links at scale, throughput of 4 nodes, more Verilator
networks) are nightly tests (`test/nightly.py`): skipped by default, run every night and on
demand by `.github/workflows/nightly.yml`, or locally with `EXIOM_NIGHTLY=1 python3 -m pytest -v`.

Scope: simulation only. No FPGA synthesis, timing closure or hardware test. Links between more
than two nodes are word-level (no PHY model).
