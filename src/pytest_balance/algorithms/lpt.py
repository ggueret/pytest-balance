"""Longest Processing Time First (LPT) partitioning algorithm."""

from __future__ import annotations

import heapq


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
