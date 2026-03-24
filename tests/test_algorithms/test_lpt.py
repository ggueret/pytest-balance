from __future__ import annotations

from pytest_balance.algorithms.lpt import partition


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
