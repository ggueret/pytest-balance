"""CI-level test splitting using LPT and scope-aware grouping."""

from __future__ import annotations

from pytest_balance.algorithms.lpt import partition
from pytest_balance.algorithms.partitioner import Scope, group_by_scope
from pytest_balance.store.models import DurationEstimate
from pytest_balance.store.reader import default_estimate


def split_tests(
    tests: list[str],
    estimates: dict[str, DurationEstimate],
    node_index: int,
    node_total: int,
    scope: Scope,
) -> list[str]:
    if not tests:
        return []

    fallback = default_estimate(estimates)

    # Group tests by scope
    groups = group_by_scope(tests, scope)

    # Compute estimated duration for each group
    group_durations: dict[str, float] = {}
    for group in groups:
        group.estimated_duration = sum(
            estimates.get(tid, fallback).estimate for tid in group.test_ids
        )
        group_durations[group.scope_id] = group.estimated_duration

    # LPT partition groups across nodes
    buckets = partition(group_durations, node_total)

    # Select groups for this node
    selected_scopes = set(buckets[node_index])

    # Flatten test IDs preserving original order within groups
    selected_tests: list[str] = []
    for group in groups:
        if group.scope_id in selected_scopes:
            selected_tests.extend(group.test_ids)

    return selected_tests
