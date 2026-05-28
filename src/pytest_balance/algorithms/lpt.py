"""Longest Processing Time First (LPT) partitioning algorithm."""

from __future__ import annotations

import heapq

from pytest_balance.algorithms.partitioner import Scope, group_by_scope
from pytest_balance.store.models import DurationEstimate
from pytest_balance.store.reader import default_estimate


def partition(durations: dict[str, float], n: int) -> list[list[str]]:
    """Partition items into n buckets minimizing makespan using LPT.

    Args:
        durations: Mapping of item ID to estimated duration.
        n: Number of buckets.

    Returns:
        List of n lists, each containing item IDs assigned to that bucket.
    """
    if n <= 0:
        raise ValueError(f"n must be >= 1, got {n}")

    buckets: list[list[str]] = [[] for _ in range(n)]

    if not durations:
        return buckets

    # Sort by duration descending, break ties by name for determinism
    sorted_items = sorted(durations.items(), key=lambda x: (-x[1], x[0]))

    # Min-heap of (total_duration, bucket_index)
    heap: list[tuple[float, int]] = [(0.0, i) for i in range(n)]
    heapq.heapify(heap)

    for item_id, _ in sorted_items:
        total, idx = heapq.heappop(heap)
        buckets[idx].append(item_id)
        heapq.heappush(heap, (total + durations[item_id], idx))

    return buckets


def compute_order(
    collection: list[str],
    estimates: dict[str, DurationEstimate],
    scope: Scope,
) -> list[int]:
    """Return indices of `collection` ordered LPT scope-adjacent.

    Groups are built by scope, sorted by descending estimated duration with a
    lexicographic tie-break on scope_id, and tests of the same group are
    emitted consecutively. Pure function: deterministic, no side effects.
    """
    if not collection:
        return []

    fallback = default_estimate(estimates)
    groups = group_by_scope(collection, scope)
    for group in groups:
        group.estimated_duration = sum(
            estimates.get(tid, fallback).estimate for tid in group.test_ids
        )
    groups.sort(key=lambda g: (-g.estimated_duration, g.scope_id))

    test_id_to_index = {tid: idx for idx, tid in enumerate(collection)}
    return [test_id_to_index[tid] for group in groups for tid in group.test_ids]
