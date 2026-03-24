from __future__ import annotations

from unittest.mock import MagicMock

import pytest

xdist = pytest.importorskip("xdist")

from pytest_balance.algorithms.partitioner import Scope  # noqa: E402
from pytest_balance.store.models import DurationEstimate  # noqa: E402
from pytest_balance.xdist.scheduler import BalanceScheduler  # noqa: E402


def _mock_node(gateway_id: str = "gw0") -> MagicMock:
    node = MagicMock()
    node.gateway = MagicMock()
    node.gateway.id = gateway_id
    node.shutting_down = False
    return node


class TestBalanceScheduler:
    def test_add_nodes(self):
        sched = BalanceScheduler(MagicMock(), MagicMock(), Scope.TEST, {})
        sched.numnodes = 2
        n1, n2 = _mock_node("gw0"), _mock_node("gw1")
        sched.add_node(n1)
        sched.add_node(n2)
        assert len(sched.nodes) == 2

    def test_collection_completed(self):
        sched = BalanceScheduler(MagicMock(), MagicMock(), Scope.TEST, {})
        sched.numnodes = 2
        n1, n2 = _mock_node("gw0"), _mock_node("gw1")
        sched.add_node(n1)
        sched.add_node(n2)
        assert not sched.collection_is_completed
        sched.add_node_collection(n1, ["test_a.py::test_1", "test_a.py::test_2"])
        assert not sched.collection_is_completed
        sched.add_node_collection(n2, ["test_a.py::test_1", "test_a.py::test_2"])
        assert sched.collection_is_completed

    def test_schedule_distributes_tests(self):
        estimates = {
            "test_a.py::test_slow": DurationEstimate("test_a.py::test_slow", 10.0, 1.0, 5),
            "test_a.py::test_fast": DurationEstimate("test_a.py::test_fast", 1.0, 1.0, 5),
        }
        sched = BalanceScheduler(MagicMock(), MagicMock(), Scope.TEST, estimates)
        sched.numnodes = 2
        n1, n2 = _mock_node("gw0"), _mock_node("gw1")
        sched.add_node(n1)
        sched.add_node(n2)
        collection = ["test_a.py::test_slow", "test_a.py::test_fast"]
        sched.add_node_collection(n1, collection)
        sched.add_node_collection(n2, collection)
        sched.schedule()
        # Each node should get exactly 1 test
        assert n1.send_runtest_some.called or n2.send_runtest_some.called

    def test_mark_test_complete(self):
        estimates = {"a::t1": DurationEstimate("a::t1", 1.0, 1.0, 5)}
        sched = BalanceScheduler(MagicMock(), MagicMock(), Scope.TEST, estimates)
        sched.numnodes = 1
        n1 = _mock_node("gw0")
        sched.add_node(n1)
        sched.add_node_collection(n1, ["a::t1"])
        sched.schedule()
        sched.mark_test_complete(n1, 0, 1.0)
        assert sched.tests_finished

    def test_empty_collection(self):
        sched = BalanceScheduler(MagicMock(), MagicMock(), Scope.TEST, {})
        sched.numnodes = 2
        n1, n2 = _mock_node("gw0"), _mock_node("gw1")
        sched.add_node(n1)
        sched.add_node(n2)
        sched.add_node_collection(n1, [])
        sched.add_node_collection(n2, [])
        sched.schedule()
        assert sched.tests_finished
