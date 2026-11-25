#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Fabric network: the nodes of a topology and their links (simulation and test benches).

Links are direct connections of the packet streams of the ports (word-level, no PHY model): the
bit-level SerWB link is exercised by the two-node benches.
"""

from migen import *

from litex.gen import *

from exiom.fabric import FabricNode, FABRIC_OK, FABRIC_NODE, FABRIC_NO_RESPONSE, FABRIC_LINK

# Expected Status ----------------------------------------------------------------------------------

def expected_status(topology, max_hops, src, dst, down=()):
    """Status of a request from `src` to `dst` once the routes have followed the nodes and links
    ``down`` (see ``Topology.distances``): served when a path of at most `max_hops` links is left."""
    down_nodes = [d for d in down if not isinstance(d, tuple)]
    if src == dst:
        return FABRIC_OK
    if src in down_nodes:
        return FABRIC_LINK # No link up.
    if dst >= topology.nodes:
        return FABRIC_NODE
    distance = topology.distances(down).get(src, {}).get(dst)
    if (distance is None) or (distance > max_hops):
        return FABRIC_NODE
    return FABRIC_OK

# Fabric Network -----------------------------------------------------------------------------------

class FabricNetwork(LiteXModule):
    """``nodes[n]``: FabricNode of id n. ``link_up[(node, port)]``: state of the link of this port
    (same Signal on both ends, 1 at reset). ``costs``: route tables (default: ``topology.costs()``).
    ``detour``: links a packet may cross beyond the longest shortest path, to go around links or
    nodes that are down (6 links at most in total)."""
    def __init__(self, topology, size, costs=None, detour=2, **node_kwargs):
        self.topology = topology
        self.costs    = costs    = topology.costs() if costs is None else costs
        self.diameter = diameter = topology.max_hops(topology.routes())
        self.max_hops = max_hops = min(diameter + detour, 6)
        self.nodes    = nodes    = []
        self.link_up  = link_up  = {}

        # # #

        for a, b in topology.links:
            up = Signal(reset=1, name=f"link_n{a[0]}p{a[1]}_n{b[0]}p{b[1]}_up")
            link_up[a] = up
            link_up[b] = up

        for n in range(topology.nodes):
            node = FabricNode(n,
                size       = size,
                ports      = topology.ports,
                routes     = costs[n],
                max_hops   = max_hops,
                # Ports without cable: link down.
                link_ready = [link_up.get((n, p), Constant(0)) for p in range(topology.ports)],
                **node_kwargs,
            )
            self.add_module(f"node{n}", node)
            nodes.append(node)

        for (na, pa), (nb, pb) in topology.links:
            self.comb += [
                nodes[na].ports[pa].source.connect(nodes[nb].ports[pb].sink),
                nodes[nb].ports[pb].source.connect(nodes[na].ports[pa].sink),
            ]

    def node_links(self, node):
        """Link state Signals of a node (to take a node down)."""
        return [up for (n, _), up in self.link_up.items() if n == node]
