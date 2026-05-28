from __future__ import annotations

import random

from pytest_balance.algorithms.lpt import compute_order, partition
from pytest_balance.algorithms.partitioner import Scope
from pytest_balance.store.models import DurationEstimate


class TestLPTPartition:
    def test_perfect_balance(self):
        """4 items of equal duration on 2 workers -> 2 per worker."""
        durations = {"a": 10.0, "b": 10.0, "c": 10.0, "d": 10.0}
        buckets = partition(durations, n=2)
        assert len(buckets) == 2
        totals = [sum(durations[t] for t in b) for b in buckets]
        assert totals[0] == 20.0
        assert totals[1] == 20.0

    def test_single_dominant_test(self):
        """One slow test gets its own bucket."""
        durations = {"slow": 100.0} | {f"fast_{i}": 1.0 for i in range(10)}
        buckets = partition(durations, n=3)
        slow_bucket = [b for b in buckets if "slow" in b]
        assert len(slow_bucket) == 1
        assert slow_bucket[0] == ["slow"]

    def test_empty_input(self):
        buckets = partition({}, n=4)
        assert buckets == [[], [], [], []]

    def test_single_bucket(self):
        durations = {"a": 5.0, "b": 3.0, "c": 1.0}
        buckets = partition(durations, n=1)
        assert len(buckets) == 1
        assert set(buckets[0]) == {"a", "b", "c"}

    def test_more_workers_than_tests(self):
        durations = {"a": 5.0, "b": 3.0}
        buckets = partition(durations, n=4)
        non_empty = [b for b in buckets if b]
        assert len(non_empty) == 2

    def test_deterministic(self):
        """Same input always produces same output."""
        durations = {f"test_{i}": float(i) for i in range(20)}
        b1 = partition(durations, n=4)
        b2 = partition(durations, n=4)
        assert b1 == b2

    def test_all_tests_assigned(self):
        """No test is lost."""
        durations = {f"t_{i}": float(i % 5 + 1) for i in range(50)}
        buckets = partition(durations, n=6)
        all_assigned = [t for b in buckets for t in b]
        assert sorted(all_assigned) == sorted(durations.keys())


def _est(test_ids, durations):
    return {
        tid: DurationEstimate(tid, d, 1.0, 5) for tid, d in zip(test_ids, durations, strict=True)
    }


def test_compute_order_empty_collection():
    assert compute_order([], {}, Scope.MODULE) == []


def test_compute_order_is_permutation():
    collection = ["a.py::t1", "a.py::t2", "b.py::t1"]
    order = compute_order(collection, _est(collection, [1.0, 1.0, 1.0]), Scope.MODULE)
    assert sorted(order) == [0, 1, 2]


def test_compute_order_scope_adjacent_under_interleaved_collection():
    collection = ["a.py::t1", "b.py::t1", "a.py::t2", "b.py::t2"]
    order = compute_order(collection, _est(collection, [1.0, 1.0, 1.0, 1.0]), Scope.MODULE)
    scopes = [collection[i].split("::")[0] for i in order]
    runs = []
    for s in scopes:
        if not runs or runs[-1] != s:
            runs.append(s)
    assert len(runs) == len(set(runs)), f"scopes interleaved: {scopes}"


def test_compute_order_descending_duration_first():
    collection = ["heavy.py::t1", "light.py::t1"]
    order = compute_order(collection, _est(collection, [10.0, 1.0]), Scope.MODULE)
    assert collection[order[0]] == "heavy.py::t1"


def test_compute_order_lexicographic_tiebreak_on_scope_id():
    collection = ["z.py::t1", "a.py::t1"]
    order = compute_order(collection, _est(collection, [1.0, 1.0]), Scope.MODULE)
    assert collection[order[0]] == "a.py::t1"


def test_compute_order_invariant_to_collection_shuffle():
    base = ["m1.py::t1", "m1.py::t2", "m2.py::t1", "m3.py::t1"]
    shuffled = list(base)
    random.Random(42).shuffle(shuffled)
    estimates = _est(base, [1.0, 1.0, 2.0, 3.0])

    order_base = compute_order(base, estimates, Scope.MODULE)
    order_shuf = compute_order(shuffled, estimates, Scope.MODULE)

    scopes_base = [base[i].split("::")[0] for i in order_base]
    scopes_shuf = [shuffled[i].split("::")[0] for i in order_shuf]
    assert scopes_base == scopes_shuf


def test_compute_order_unknown_tests_use_fallback():
    # Known test has duration 5.0; unknown gets the fallback (median of knowns,
    # also 5.0 here). Equal durations -> lexicographic tie-break on scope_id
    # places "known.py" before "unknown.py". The assertion would fail if the
    # fallback raised, returned an unusable value, or wasn't applied at all.
    collection = ["known.py::t1", "unknown.py::t1"]
    estimates = {"known.py::t1": DurationEstimate("known.py::t1", 5.0, 1.0, 5)}
    order = compute_order(collection, estimates, Scope.MODULE)
    assert order[0] == 0
    assert sorted(order) == [0, 1]
