#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric topologies and static routes (build time, plain Python).

Two-tier topology (the way large SMP servers interconnect their processors: processors of a book
all connected together, books connected together):

- Tier 1: the nodes of a group are all connected to each other (``group_size - 1`` ports).
- Tier 2: groups are connected to each other by the ``inter_ports`` remaining ports of their
  nodes.

Node id = group*group_size + member. Cabling only depends on the size the fabric is designed for
(``design_groups``), not on the nodes present: growing 2 -> 4 -> 8 -> 16 nodes only adds cables.

Ports of a node:

- 0 .. group_size-2 : port p of member m goes to member (m + p + 1) % group_size of its group.
- group_size-1 ..   : inter-group ports.

Any two nodes are at most 3 links apart (group, inter-group, group) when every group has a direct
link to every other group.
"""

from collections import deque

# Topology -----------------------------------------------------------------------------------------

class Topology:
    """``links``: list of ((node, port), (node, port)), between present nodes only."""
    def __init__(self, nodes, ports, links, name=""):
        self.nodes = nodes
        self.ports = ports
        self.links = links
        self.name  = name
        self.peers = {}
        for a, b in links:
            assert a not in self.peers and b not in self.peers, "Port used twice."
            assert a[0] < nodes and b[0] < nodes and a[1] < ports and b[1] < ports
            self.peers[a] = b
            self.peers[b] = a

    def neighbors(self, node):
        """[(port, neighbor)] of a node, by port."""
        return [(port, self.peers[(node, port)][0]) for port in range(self.ports) if (node, port) in self.peers]

    def ports_used(self, node):
        return len(self.neighbors(node))

    def routes(self, down=()):
        """Shortest path routes: {node: {destination: port}} (first port of the lowest numbered
        shortest path). ``down``: links ((node, port) of one of their ends) or nodes not usable."""
        down_ports = set()
        down_nodes = set()
        for d in down:
            if isinstance(d, tuple):
                down_ports |= {d, self.peers[d]}
            else:
                down_nodes.add(d)
        routes = {}
        for src in range(self.nodes):
            routes[src] = {}
            if src in down_nodes:
                continue
            # Breadth first search, remembering the first port taken.
            first = {src: None}
            queue = deque([src])
            while queue:
                node = queue.popleft()
                for port, neighbor in self.neighbors(node):
                    if ((node, port) in down_ports) or (neighbor in down_nodes) or (neighbor in first):
                        continue
                    first[neighbor] = port if node == src else first[node]
                    queue.append(neighbor)
            routes[src] = {dst: port for dst, port in first.items() if dst != src}
        return routes

    def hops(self, routes=None):
        """{(source, destination): links crossed} following the routes (None: not reachable)."""
        routes = self.routes() if routes is None else routes
        hops   = {}
        for src in range(self.nodes):
            for dst in range(self.nodes):
                if src == dst:
                    continue
                node, count = src, 0
                while node != dst and count <= self.nodes:
                    port = routes[node].get(dst)
                    if port is None:
                        count = None
                        break
                    node   = self.peers[(node, port)][0]
                    count += 1
                hops[(src, dst)] = count if (count is None or count <= self.nodes) else None
        return hops

    def loads(self, routes=None):
        """{(node, port): pairs (source, destination) whose route leaves ``node`` by ``port``}:
        load of each direction of each link when every node sends as much to every other node."""
        routes = self.routes() if routes is None else routes
        loads  = {port: 0 for port in self.peers}
        for src in range(self.nodes):
            for dst in routes[src]:
                node, count = src, 0
                while node != dst and count <= self.nodes:
                    port = routes[node].get(dst)
                    if port is None:
                        break
                    loads[(node, port)] += 1
                    node   = self.peers[(node, port)][0]
                    count += 1
        return loads

    def max_hops(self, routes=None):
        hops = [h for h in self.hops(routes).values() if h is not None]
        return max(hops) if hops else 1

    def distances(self, down=()):
        """{node: {destination: links of the shortest path}}. ``down``: nodes, or links ((node,
        port) of one of their ends), that are down."""
        down_ports = set()
        down_nodes = set()
        for d in down:
            if isinstance(d, tuple):
                down_ports |= {d, self.peers[d]}
            else:
                down_nodes.add(d)
        distances = {}
        for src in range(self.nodes):
            distances[src] = {src: 0}
            if src in down_nodes:
                continue
            queue = deque([src])
            while queue:
                node = queue.popleft()
                for port, neighbor in self.neighbors(node):
                    if ((node, port) in down_ports) or (neighbor in down_nodes) or (neighbor in distances[src]):
                        continue
                    distances[src][neighbor] = distances[src][node] + 1
                    queue.append(neighbor)
        return distances

    def costs(self):
        """Route tables: {node: {destination: [cost per port]}}, the cost of a port being the links
        to the destination through it, without coming back through the node, with all the links up
        (None: no cable or destination not reachable through it)."""
        costs = {}
        for node in range(self.nodes):
            distances   = self.distances(down=[node])
            costs[node] = {}
            for dst in range(self.nodes):
                if dst == node:
                    continue
                costs[node][dst] = [None]*self.ports
                for port, neighbor in self.neighbors(node):
                    if dst in distances[neighbor]:
                        costs[node][dst][port] = 1 + distances[neighbor][dst]
        return costs

# Two-Tier -----------------------------------------------------------------------------------------

def two_tier(nodes, group_size=4, design_groups=4, inter_ports=1):
    """Two-tier topology with ``nodes`` nodes present, cabled for ``design_groups`` groups.

    Inter-group ports: with D = design_groups, inter-group port index i of member m is the
    "link index" n = m + i*group_size of its group; link index n goes to group (g + k + 1) % D
    with k = n % (D - 1), where it is link index (n - k) + (D - 2 - k) of that group.
    """
    assert nodes >= 1 and group_size >= 1 and design_groups >= 1
    assert nodes <= group_size*design_groups, "More nodes than the fabric is designed for."
    if group_size*inter_ports < design_groups - 1:
        # Fewer cables leaving a group than other groups: some groups would have no cable between
        # them, and with the cabling rule below most of them would not be connected at all.
        raise ValueError(f"{design_groups} groups of {group_size} nodes need at least "
            f"{-(-(design_groups - 1)//group_size)} inter-group ports per node (inter_ports={inter_ports}).")
    links = []
    def present(node):
        return node < nodes
    groups = design_groups
    for node in range(group_size*groups):
        g, m = divmod(node, group_size)
        # Tier 1: intra-group.
        for p in range(group_size - 1):
            peer_m = (m + p + 1) % group_size
            peer   = g*group_size + peer_m
            peer_p = group_size - 2 - p
            if node < peer and present(node) and present(peer):
                links.append(((node, p), (peer, peer_p)))
        # Tier 2: inter-group.
        if groups < 2:
            continue
        for i in range(inter_ports):
            n   = m + i*group_size
            k   = n % (groups - 1)
            lap = n - k
            peer_g = (g + k + 1) % groups
            peer_n = lap + (groups - 2 - k)
            peer_i, peer_m = divmod(peer_n, group_size)
            if peer_i >= inter_ports:
                continue # Spare port.
            peer = peer_g*group_size + peer_m
            a = (node, group_size - 1 + i)
            b = (peer, group_size - 1 + peer_i)
            if a < b and present(node) and present(peer):
                links.append((a, b))
    return Topology(nodes, group_size - 1 + inter_ports, links,
        name=f"two_tier({nodes} nodes, groups of {group_size}, designed for {design_groups} groups)")

# Hypercube ----------------------------------------------------------------------------------------

def hypercube(nodes):
    """Hypercube (port d goes to the node whose id differs by bit d)."""
    assert nodes >= 1 and (nodes & (nodes - 1)) == 0
    ports = max(1, nodes.bit_length() - 1)
    links = []
    for node in range(nodes):
        for d in range(nodes.bit_length() - 1):
            peer = node ^ (1 << d)
            if node < peer:
                links.append(((node, d), (peer, d)))
    return Topology(nodes, ports, links, name=f"hypercube({nodes} nodes)")
