from __future__ import annotations

import warnings
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


def _make_estimates(test_ids: list[str], durations: list[float]) -> dict[str, DurationEstimate]:
    """Build an estimates dict from parallel lists of test IDs and durations."""
    return {
        tid: DurationEstimate(tid, dur, 1.0, 5)
        for tid, dur in zip(test_ids, durations, strict=True)
    }


def _setup_scheduler(
    collection: list[str],
    estimates: dict[str, DurationEstimate],
    n_workers: int = 2,
    scope: Scope = Scope.TEST,
) -> tuple[BalanceScheduler, list[MagicMock]]:
    """Create a scheduler, register nodes, add collections, and schedule."""
    sched = BalanceScheduler(MagicMock(), MagicMock(), scope, estimates)
    sched.numnodes = n_workers
    nodes = [_mock_node(f"gw{i}") for i in range(n_workers)]
    for node in nodes:
        sched.add_node(node)
        sched.add_node_collection(node, collection)
    sched.schedule()
    return sched, nodes


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


class TestWorkStealing:
    """Tests for the work-stealing mechanism in _check_schedule."""

    def test_work_stealing_triggered(self):
        """When one worker finishes and another has >3 pending, steal is attempted."""
        # 6 tests: one slow (10s) and five fast (1s each).
        # LPT with 2 workers: gw0 gets the slow test, gw1 gets the 5 fast ones.
        collection = [
            "test_a.py::test_slow",
            "test_b.py::test_1",
            "test_b.py::test_2",
            "test_b.py::test_3",
            "test_b.py::test_4",
            "test_b.py::test_5",
        ]
        estimates = _make_estimates(collection, [10.0, 1.0, 1.0, 1.0, 1.0, 1.0])
        sched, nodes = _setup_scheduler(collection, estimates, n_workers=2)
        n_slow, n_fast = nodes[0], nodes[1]

        # Figure out which node got the slow test (1 item) vs the fast tests (5 items).
        if len(sched.node2pending[n_slow]) < len(sched.node2pending[n_fast]):
            idle_node, busy_node = n_slow, n_fast
        else:
            idle_node, busy_node = n_fast, n_slow

        # The idle node has only 1 test. Complete it so it becomes idle (<2 pending).
        assert len(sched.node2pending[idle_node]) == 1
        item = sched.node2pending[idle_node][0]
        sched.mark_test_complete(idle_node, item, 1.0)

        # The busy node should have had send_steal called on it.
        busy_node.send_steal.assert_called_once()
        assert sched.steal_in_flight is busy_node

    def test_work_stealing_scope_aware(self):
        """With scope=MODULE, stealing takes complete module groups, not individual tests."""
        # Two modules: mod_a (3 tests) and mod_b (3 tests).
        # With 2 workers, LPT should put both modules on one worker if durations
        # are equal (or split them). We force one worker to be busy with both.
        collection = [
            "mod_a.py::test_1",
            "mod_a.py::test_2",
            "mod_a.py::test_3",
            "mod_b.py::test_1",
            "mod_b.py::test_2",
            "mod_b.py::test_3",
        ]
        estimates = _make_estimates(collection, [1.0, 1.0, 1.0, 1.0, 1.0, 1.0])

        sched = BalanceScheduler(MagicMock(), MagicMock(), Scope.MODULE, estimates)
        sched.numnodes = 2
        n1, n2 = _mock_node("gw0"), _mock_node("gw1")
        sched.add_node(n1)
        sched.add_node(n2)
        sched.add_node_collection(n1, collection)
        sched.add_node_collection(n2, collection)
        sched.schedule()

        # With MODULE scope and equal durations, LPT puts one module per worker.
        # Manually set up the scenario: n1 has both modules (6 tests), n2 is idle.
        # Reset to force the scenario we need.
        sched.node2pending[n1] = [0, 1, 2, 3, 4, 5]
        sched.node2pending[n2] = []
        sched.pending = []
        sched.steal_in_flight = None

        # Trigger rebalance by calling _check_schedule.
        sched._check_schedule()

        # n1.send_steal should have been called with a complete module group.
        n1.send_steal.assert_called_once()
        stolen_indices = n1.send_steal.call_args[0][0]
        stolen_ids = [collection[i] for i in stolen_indices]

        # All stolen tests should belong to the same module.
        modules = {tid.split("::")[0] for tid in stolen_ids}
        assert len(modules) == 1, f"Stole from multiple modules: {modules}"

    def test_steal_in_flight_guard(self):
        """Only one steal at a time -- second idle worker does not trigger another steal."""
        collection = [f"test_{i}.py::test" for i in range(8)]
        estimates = _make_estimates(collection, [1.0] * 8)

        sched = BalanceScheduler(MagicMock(), MagicMock(), Scope.TEST, estimates)
        sched.numnodes = 3
        n1, n2, n3 = _mock_node("gw0"), _mock_node("gw1"), _mock_node("gw2")
        for n in [n1, n2, n3]:
            sched.add_node(n)
            sched.add_node_collection(n, collection)
        sched.schedule()

        # Put all pending on n1, leave n2 and n3 idle.
        sched.node2pending[n1] = list(range(8))
        sched.node2pending[n2] = []
        sched.node2pending[n3] = []
        sched.pending = []
        sched.steal_in_flight = None

        # First rebalance triggers steal from n1.
        sched._check_schedule()
        assert sched.steal_in_flight is n1
        n1.send_steal.assert_called_once()
        steal_call_count = n1.send_steal.call_count

        # Second rebalance (e.g., another node goes idle) should NOT trigger another steal.
        sched._check_schedule()
        assert n1.send_steal.call_count == steal_call_count, (
            "Second steal triggered while one in flight"
        )

    def test_remove_pending_tests_from_node(self):
        """Stolen tests are moved from node pending to global pending and redistributed."""
        collection = [f"test_{i}.py::test" for i in range(6)]
        estimates = _make_estimates(collection, [1.0] * 6)
        sched, nodes = _setup_scheduler(collection, estimates, n_workers=2)
        n1, n2 = nodes

        # Simulate: n1 has all 6 tests, n2 is idle, steal in flight from n1.
        sched.node2pending[n1] = list(range(6))
        sched.node2pending[n2] = []
        sched.pending = []
        sched.steal_in_flight = n1

        # The worker returns indices [3, 4, 5] as stolen.
        sched.remove_pending_tests_from_node(n1, [3, 4, 5])

        # Steal should be cleared.
        assert sched.steal_in_flight is None
        # n1 should no longer have indices 3, 4, 5.
        assert 3 not in sched.node2pending[n1]
        assert 4 not in sched.node2pending[n1]
        assert 5 not in sched.node2pending[n1]
        # The stolen tests should have been redistributed (sent to idle n2).
        assert n2.send_runtest_some.called


class TestNodeCrashRecovery:
    """Tests for remove_node crash recovery."""

    def test_remove_node_redistributes(self):
        """When a node crashes, its pending tests go to remaining workers."""
        collection = [f"test_{i}.py::test" for i in range(6)]
        estimates = _make_estimates(collection, [1.0] * 6)
        sched, nodes = _setup_scheduler(collection, estimates, n_workers=2)
        n1, n2 = nodes

        # Give n1 some pending work, n2 has 1 test (idle after completion).
        sched.node2pending[n1] = [0, 1, 2]
        sched.node2pending[n2] = [3, 4, 5]
        sched.pending = []

        # n1 crashes -- remove it.
        crashitem = sched.remove_node(n1)

        # Crash item should be the first pending test on the crashed node.
        assert crashitem == collection[0]
        # n1 should be gone from tracking.
        assert n1 not in sched.node2pending
        # The remaining tests (indices 1, 2) should end up in global pending
        # or dispatched to n2.
        remaining_on_n2 = sched.node2pending[n2]
        total_tracked = len(remaining_on_n2) + len(sched.pending)
        # Original 3 on n2 + 2 remaining from n1 crash = 5
        assert total_tracked == 5

    def test_remove_node_returns_crash_item(self):
        """remove_node() returns the first pending test as crash item."""
        collection = ["test_a.py::test_1", "test_a.py::test_2", "test_a.py::test_3"]
        estimates = _make_estimates(collection, [1.0, 1.0, 1.0])

        sched = BalanceScheduler(MagicMock(), MagicMock(), Scope.TEST, estimates)
        sched.numnodes = 2
        n1, n2 = _mock_node("gw0"), _mock_node("gw1")
        sched.add_node(n1)
        sched.add_node(n2)
        sched.add_node_collection(n1, collection)
        sched.add_node_collection(n2, collection)
        sched.schedule()

        # Give n1 known pending items.
        sched.node2pending[n1] = [0, 1, 2]
        sched.collection = collection

        crashitem = sched.remove_node(n1)
        assert crashitem == "test_a.py::test_1"

    def test_remove_node_no_pending_returns_none(self):
        """remove_node() returns None when the node had no pending tests."""
        collection = ["test_a.py::test_1"]
        estimates = _make_estimates(collection, [1.0])

        sched = BalanceScheduler(MagicMock(), MagicMock(), Scope.TEST, estimates)
        sched.numnodes = 1
        n1 = _mock_node("gw0")
        sched.add_node(n1)
        sched.add_node_collection(n1, collection)
        sched.schedule()

        # Complete the only test so the node has no pending items.
        sched.mark_test_complete(n1, 0, 1.0)
        crashitem = sched.remove_node(n1)
        assert crashitem is None

    def test_remove_node_clears_steal_in_flight(self):
        """If the crashed node was the steal target, steal_in_flight is cleared."""
        collection = [f"test_{i}.py::test" for i in range(6)]
        estimates = _make_estimates(collection, [1.0] * 6)
        sched, nodes = _setup_scheduler(collection, estimates, n_workers=2)
        n1, n2 = nodes

        sched.node2pending[n1] = [0, 1, 2, 3]
        sched.node2pending[n2] = [4, 5]
        sched.steal_in_flight = n1

        sched.remove_node(n1)
        assert sched.steal_in_flight is None


class TestCollectionMismatch:
    """Tests for mismatched collections across workers."""

    def test_mismatched_collections(self):
        """When workers have different collections, scheduling aborts gracefully."""
        sched = BalanceScheduler(MagicMock(), MagicMock(), Scope.TEST, {})
        sched.numnodes = 2
        n1, n2 = _mock_node("gw0"), _mock_node("gw1")
        sched.add_node(n1)
        sched.add_node(n2)

        sched.add_node_collection(n1, ["test_a.py::test_1", "test_a.py::test_2"])
        sched.add_node_collection(n2, ["test_a.py::test_1", "test_b.py::test_3"])
        sched.schedule()

        # Should have logged the diff and not assigned any tests.
        assert sched.collection is None
        # No tests should have been sent to any worker.
        n1.send_runtest_some.assert_not_called()
        n2.send_runtest_some.assert_not_called()


class TestHooksIntegration:
    """Tests for the make_balance_scheduler hook function."""

    def test_hooks_returns_none_without_balance(self):
        """make_balance_scheduler returns None when --balance is not active."""
        from pytest_balance.xdist.hooks import make_balance_scheduler

        config = MagicMock()
        config.getoption.side_effect = lambda key, default=None: {
            "balance": False,
        }.get(key, default)

        result = make_balance_scheduler(config, MagicMock())
        assert result is None

    def test_hooks_returns_none_for_dist_each(self):
        """Returns None and warns when --dist=each."""
        from pytest_balance.xdist.hooks import make_balance_scheduler

        config = MagicMock()
        config.getoption.side_effect = lambda key, default=None: {
            "balance": True,
            "dist": "each",
        }.get(key, default)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = make_balance_scheduler(config, MagicMock())

        assert result is None
        assert any("incompatible" in str(w.message) for w in caught)


class TestEdgeCases:
    """Edge case tests."""

    def test_mark_test_pending_reschedules(self):
        """mark_test_pending requeues a test to the global pending list."""
        collection = ["test_a.py::test_1", "test_a.py::test_2"]
        estimates = _make_estimates(collection, [1.0, 1.0])
        sched, nodes = _setup_scheduler(collection, estimates, n_workers=1)
        n1 = nodes[0]

        # Complete all tests first.
        for idx in list(sched.node2pending[n1]):
            sched.mark_test_complete(n1, idx, 1.0)
        assert sched.tests_finished

        # Re-add a test via mark_test_pending (used for crash item retry).
        sched.mark_test_pending("test_a.py::test_1")

        # The test should be in global pending or already dispatched to n1.
        idx_0 = collection.index("test_a.py::test_1")
        all_pending = sched.pending + sched.node2pending[n1]
        assert idx_0 in all_pending
        # has_pending reflects that work exists somewhere in the system.
        assert sched.has_pending

    def test_more_workers_than_groups(self):
        """Extra workers get shutdown immediately when there are fewer groups than workers."""
        collection = ["test_a.py::test_1"]
        estimates = _make_estimates(collection, [1.0])

        sched = BalanceScheduler(MagicMock(), MagicMock(), Scope.TEST, estimates)
        sched.numnodes = 3
        n1, n2, n3 = _mock_node("gw0"), _mock_node("gw1"), _mock_node("gw2")
        for n in [n1, n2, n3]:
            sched.add_node(n)
            sched.add_node_collection(n, collection)

        sched.schedule()

        # Only 1 test exists, so at most 1 worker should have received work.
        # The others should have been shut down.
        workers_with_tests = [n for n in [n1, n2, n3] if sched.node2pending[n]]
        workers_shutdown = [n for n in [n1, n2, n3] if n.shutdown.called]
        assert len(workers_with_tests) == 1
        assert len(workers_shutdown) == 2

    def test_tests_finished_false_while_steal_in_flight(self):
        """tests_finished is False while a steal is in flight, even if all node queues are low."""
        collection = [f"test_{i}.py::test" for i in range(4)]
        estimates = _make_estimates(collection, [1.0] * 4)
        sched, nodes = _setup_scheduler(collection, estimates, n_workers=2)
        n1, n2 = nodes

        # Drain both nodes to < MIN_PENDING but mark steal in flight.
        sched.node2pending[n1] = [0]
        sched.node2pending[n2] = []
        sched.pending = []
        sched.steal_in_flight = n1

        assert not sched.tests_finished

    def test_has_pending_with_global_pending(self):
        """has_pending is True when only global pending list has items."""
        sched = BalanceScheduler(MagicMock(), MagicMock(), Scope.TEST, {})
        sched.numnodes = 1
        n1 = _mock_node("gw0")
        sched.add_node(n1)
        sched.add_node_collection(n1, ["test_a.py::test_1"])
        sched.schedule()

        # Clear node pending but add to global pending.
        sched.node2pending[n1] = []
        sched.pending = [0]
        assert sched.has_pending

    def test_schedule_idempotent_after_initial(self):
        """Calling schedule() again after initial distribution acts like _check_schedule."""
        collection = ["test_a.py::test_1", "test_a.py::test_2"]
        estimates = _make_estimates(collection, [5.0, 1.0])
        sched, nodes = _setup_scheduler(collection, estimates, n_workers=2)
        n1, n2 = nodes

        # Record state after first schedule.
        pending_before_n1 = list(sched.node2pending[n1])
        pending_before_n2 = list(sched.node2pending[n2])

        # Call schedule again -- should not crash or re-distribute.
        sched.schedule()

        # State should remain consistent.
        assert sched.node2pending[n1] == pending_before_n1
        assert sched.node2pending[n2] == pending_before_n2
