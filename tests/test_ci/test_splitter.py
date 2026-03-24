from __future__ import annotations

from pytest_balance.algorithms.partitioner import Scope
from pytest_balance.ci.splitter import split_tests
from pytest_balance.store.models import DurationEstimate


def _est(test_id: str, estimate: float) -> DurationEstimate:
    return DurationEstimate(test_id, estimate, confidence=1.0, sample_count=10)


class TestSplitTests:
    def test_basic_split(self):
        tests = ["a::t1", "a::t2", "b::t1", "b::t2"]
        estimates = {t: _est(t, 1.0) for t in tests}
        result = split_tests(tests, estimates, node_index=0, node_total=2, scope=Scope.TEST)
        assert len(result) == 2

    def test_all_tests_distributed(self):
        tests = [f"mod{i}::t{j}" for i in range(4) for j in range(5)]
        estimates = {t: _est(t, 1.0) for t in tests}
        all_selected: list[str] = []
        for idx in range(4):
            selected = split_tests(tests, estimates, idx, 4, Scope.TEST)
            all_selected.extend(selected)
        assert sorted(all_selected) == sorted(tests)

    def test_scope_module(self):
        tests = [
            "tests/test_a.py::test_1",
            "tests/test_a.py::test_2",
            "tests/test_b.py::test_1",
        ]
        estimates = {t: _est(t, 1.0) for t in tests}
        g0 = split_tests(tests, estimates, 0, 2, Scope.MODULE)
        g1 = split_tests(tests, estimates, 1, 2, Scope.MODULE)
        # Module tests should not be split across groups
        if "tests/test_a.py::test_1" in g0:
            assert "tests/test_a.py::test_2" in g0
        else:
            assert "tests/test_a.py::test_1" in g1
            assert "tests/test_a.py::test_2" in g1

    def test_duration_aware_balance(self):
        tests = ["slow", "fast_1", "fast_2", "fast_3"]
        estimates = {
            "slow": _est("slow", 100.0),
            "fast_1": _est("fast_1", 1.0),
            "fast_2": _est("fast_2", 1.0),
            "fast_3": _est("fast_3", 1.0),
        }
        g0 = split_tests(tests, estimates, 0, 2, Scope.TEST)
        g1 = split_tests(tests, estimates, 1, 2, Scope.TEST)
        # Slow test should be alone in one group
        if "slow" in g0:
            assert len(g0) == 1
        else:
            assert len(g1) == 1

    def test_unknown_tests_get_default(self):
        tests = ["known", "unknown_1", "unknown_2"]
        estimates = {"known": _est("known", 5.0)}
        result = split_tests(tests, estimates, 0, 2, Scope.TEST)
        assert len(result) > 0  # Unknown tests are assigned, not dropped

    def test_empty_tests(self):
        assert split_tests([], {}, 0, 2, Scope.TEST) == []
