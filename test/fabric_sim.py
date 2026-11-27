#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Migen simulations of large designs.

The simulator groups the combinatorial statements that share a target before compiling them. The
grouping function compares each statement with every group already made, and the simulator gives
a default statement to every combinatorial Signal: the time to start a simulation grows with the
square of the number of statements (minutes for a fabric of 8 nodes).

`run_simulation` here runs the same simulator with an equivalent grouping (union-find: same groups,
same statements order inside a group), in time proportional to the number of statements. The
simulator itself is not modified; if it stops using this function, simulations just start slower.
"""

from migen.fhdl import tracer as _tracer
from migen.fhdl.tools import list_targets, flat_iteration

import litex.gen.sim.core as _core

# Building the design. Migen numbers the objects of a class by searching each new one in the list
# of all the objects of its class, for every Signal created (most of the time to build a fabric of
# 16 nodes). Same numbers here, remembered instead of searched (the lists only grow). Import this
# module before building.
_indexes = {} # id(list) -> [list, {id(object): index}, objects indexed].

def index_id(l, obj):
    entry = _indexes.get(id(l))
    if entry is None or entry[0] is not l:
        entry = _indexes[id(l)] = [l, {}, 0]
    _, index, done = entry
    if done < len(l):
        for n in range(done, len(l)):
            index.setdefault(id(l[n]), n)
        entry[2] = len(l)
    try:
        return index[id(obj)]
    except KeyError:
        raise ValueError

if hasattr(_tracer, "index_id"):
    _tracer.index_id = index_id

def group_by_targets(sl):
    """Groups of statements sharing targets: [(targets, statements)], as migen.fhdl.tools."""
    parent = {}

    def find(x):
        root = x
        while parent[root] is not root:
            root = parent[root]
        while parent[x] is not root:
            parent[x], x = root, parent[x]
        return root

    statements = []
    for order, statement in enumerate(flat_iteration(sl)):
        targets = set(list_targets(statement))
        statements.append((order, statement, targets))
        first = None
        for target in targets:
            if target not in parent:
                parent[target] = target
            if first is None:
                first = find(target)
            else:
                root = find(target)
                if root is not first:
                    parent[root] = first

    groups = {} # Root (or statement order, without target) -> [targets, statements, last order].
    for order, statement, targets in statements:
        key = find(next(iter(targets))) if targets else ("statement", order)
        group = groups.setdefault(key, [set(), [], order])
        group[0] |= targets
        group[1].append(statement)
        group[2]  = order
    # Groups in the order of their last statement, as the original function.
    return [(targets, group) for targets, group, _ in sorted(groups.values(), key=lambda g: g[2])]

def run_simulation(*args, **kwargs):
    original = getattr(_core, "group_by_targets", None)
    if original is not None:
        _core.group_by_targets = group_by_targets
    try:
        return _core.run_simulation(*args, **kwargs)
    finally:
        if original is not None:
            _core.group_by_targets = original
