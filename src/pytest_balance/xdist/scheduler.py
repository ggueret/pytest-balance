"""Duration-aware xdist scheduler with work-stealing."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from xdist.remote import Producer  # type: ignore[import-untyped]
from xdist.report import report_collection_diff  # type: ignore[import-untyped]

from pytest_balance.algorithms.lpt import partition
from pytest_balance.algorithms.partitioner import Scope, _extract_scope, group_by_scope
from pytest_balance.store.models import DurationEstimate
from pytest_balance.store.reader import default_estimate

if TYPE_CHECKING:
    import pytest
    from xdist.workermanage import WorkerController  # type: ignore[import-untyped]

# Workers need at least 2 tests queued -- the running one + the next.
MIN_PENDING = 2

# Minimum pending on a source node before we consider stealing from it.
MIN_STEAL_SOURCE = 3


class BalanceScheduler:
    """Scheduling implementation that uses duration estimates and LPT
    to distribute tests across xdist workers, with scope-aware
    work-stealing for load balancing at runtime.
    """

    def __init__(
        self,
        config: pytest.Config | Any,
        log: Producer | Any,
        scope: Scope,
        estimates: dict[str, DurationEstimate],
    ) -> None:
        self.config = config
        if hasattr(log, "balancesched"):
            self.log: Any = log.balancesched
        else:
            self.log = log
        self.scope = scope
        self.estimates = estimates

        self.numnodes: int = 0
        self.node2collection: dict[WorkerController, list[str]] = {}
        self.node2pending: dict[WorkerController, list[int]] = {}
        self.pending: list[int] = []
        self.collection: list[str] | None = None
        self.steal_in_flight: WorkerController | None = None

    # -- Protocol properties --------------------------------------------------

    @property
    def nodes(self) -> list[WorkerController]:
        """All registered worker nodes."""
        return list(self.node2pending.keys())

    @property
    def collection_is_completed(self) -> bool:
        """True when every expected node has reported its collection."""
        return len(self.node2collection) >= self.numnodes

    @property
    def tests_finished(self) -> bool:
        """True when no pending work remains anywhere."""
        if not self.collection_is_completed:
            return False
        if self.pending:
            return False
        if self.steal_in_flight is not None:
            return False
        return all(len(pending) < MIN_PENDING for pending in self.node2pending.values())

    @property
    def has_pending(self) -> bool:
        """True if any test is still pending (globally or on a node)."""
        if self.pending:
            return True
        return any(bool(p) for p in self.node2pending.values())

    # -- Protocol methods -----------------------------------------------------

    def add_node(self, node: WorkerController) -> None:
        """Register a new worker node."""
        assert node not in self.node2pending
        self.node2pending[node] = []

    def add_node_collection(
        self,
        node: WorkerController,
        collection: Sequence[str],
    ) -> None:
        """Store the test collection reported by *node*."""
        assert node in self.node2pending
        if self.collection_is_completed:
            # Late addition -- validate against the canonical collection.
            assert self.collection is not None
            if list(collection) != self.collection:
                other_node = next(iter(self.node2collection.keys()))
                msg = report_collection_diff(
                    self.collection,
                    list(collection),
                    other_node.gateway.id,
                    node.gateway.id,
                )
                if msg:
                    self.log(msg)
                return
        self.node2collection[node] = list(collection)

    def mark_test_complete(
        self,
        node: WorkerController,
        item_index: int,
        duration: float = 0,
    ) -> None:
        """Mark a test as completed and try to rebalance."""
        self.node2pending[node].remove(item_index)
        self._check_schedule()

    def mark_test_pending(self, item: str) -> None:
        """Re-add a test to the global pending list."""
        assert self.collection is not None
        self.pending.insert(0, self.collection.index(item))
        self._check_schedule()

    def remove_pending_tests_from_node(
        self,
        node: WorkerController,
        indices: Sequence[int],
    ) -> None:
        """Handle stolen tests returned by a worker.

        Called by ``DSession.worker_unscheduled`` after a steal completes.
        """
        assert node is self.steal_in_flight
        self.steal_in_flight = None

        indices_set = set(indices)
        self.node2pending[node] = [i for i in self.node2pending[node] if i not in indices_set]
        self.pending.extend(indices)
        self._check_schedule()

    def remove_node(self, node: WorkerController) -> str | None:
        """Remove a crashed or finished node.

        Returns the crash item (the test that was running) or None.
        """
        pending = self.node2pending.pop(node)

        crashitem: str | None = None
        if pending:
            assert self.collection is not None
            crashitem = self.collection[pending.pop(0)]

        # Return remaining work to the global pool.
        self.pending.extend(pending)

        if self.steal_in_flight is node:
            self.steal_in_flight = None

        self._check_schedule()
        return crashitem

    def schedule(self) -> None:
        """Perform the initial LPT-based distribution of tests.

        Called by DSession once ``collection_is_completed`` is True.
        Subsequent calls behave like ``_check_schedule()``.
        """
        assert self.collection_is_completed

        # If already initialised, just rebalance.
        if self.collection is not None:
            self._check_schedule()
            return

        # Validate that all workers collected the same tests.
        if not self._check_nodes_have_same_collection():
            self.log("**Different tests collected, aborting run**")
            return

        self.collection = list(next(iter(self.node2collection.values())))

        if not self.collection:
            return

        # -- LPT partitioning --
        fallback = default_estimate(self.estimates)
        groups = group_by_scope(self.collection, self.scope)

        group_durations: dict[str, float] = {}
        group_test_ids: dict[str, list[str]] = {}
        for group in groups:
            group.estimated_duration = sum(
                self.estimates.get(tid, fallback).estimate for tid in group.test_ids
            )
            group_durations[group.scope_id] = group.estimated_duration
            group_test_ids[group.scope_id] = group.test_ids

        active_nodes = [n for n in self.nodes if not n.shutting_down]
        n_workers = len(active_nodes)
        if n_workers == 0:
            return

        buckets = partition(group_durations, n_workers)

        # Build a fast test_id -> index lookup.
        test_id_to_index: dict[str, int] = {tid: idx for idx, tid in enumerate(self.collection)}

        for worker_idx, bucket in enumerate(buckets):
            node = active_nodes[worker_idx]
            indices: list[int] = []
            for scope_id in bucket:
                for tid in group_test_ids[scope_id]:
                    indices.append(test_id_to_index[tid])

            if indices:
                self.node2pending[node].extend(indices)
                node.send_runtest_some(indices)
            else:
                node.shutdown()

    # -- Internal helpers -----------------------------------------------------

    def _check_schedule(self) -> None:
        """Distribute global pending work and attempt work-stealing."""
        active_nodes = [n for n in self.nodes if not n.shutting_down]
        idle_nodes = [n for n in active_nodes if len(self.node2pending[n]) < MIN_PENDING]

        if not idle_nodes:
            return

        # Distribute any globally pending tests first.
        if self.pending:
            for i, node in enumerate(idle_nodes):
                remaining = len(idle_nodes) - i
                num_send = len(self.pending) // remaining
                self._send_tests(node, num_send)

            # Recompute idle after distribution.
            idle_nodes = [n for n in active_nodes if len(self.node2pending[n]) < MIN_PENDING]
            if not idle_nodes:
                return

        # Try work-stealing for each idle node.
        self._try_steal_or_shutdown(idle_nodes, active_nodes)

    def _send_tests(self, node: WorkerController, num: int) -> None:
        """Send *num* tests from global pending to *node*."""
        tests = self.pending[:num]
        if tests:
            del self.pending[:num]
            self.node2pending[node].extend(tests)
            node.send_runtest_some(tests)

    def _try_steal_or_shutdown(
        self,
        idle_nodes: list[WorkerController],
        active_nodes: list[WorkerController],
    ) -> None:
        """Attempt to steal work for idle nodes, or shut them down."""
        if self.steal_in_flight is not None:
            return

        # Find the busiest node with enough pending to steal from.
        busiest: WorkerController | None = None
        busiest_count = 0
        for node in active_nodes:
            count = len(self.node2pending[node])
            if count > busiest_count:
                busiest = node
                busiest_count = count

        if busiest is None or busiest_count <= MIN_STEAL_SOURCE:
            # Nothing to steal -- shut down idle workers so they finish
            # their last running test immediately.
            for node in idle_nodes:
                node.shutdown()
            return

        # Determine what to steal based on scope.
        pending_indices = self.node2pending[busiest]
        indices_to_steal = self._pick_steal_indices(pending_indices)

        if not indices_to_steal:
            for node in idle_nodes:
                node.shutdown()
            return

        busiest.send_steal(indices_to_steal)
        self.steal_in_flight = busiest

    def _pick_steal_indices(self, pending_indices: list[int]) -> list[int]:
        """Select indices to steal from *pending_indices*, respecting scope."""
        assert self.collection is not None

        if self.scope == Scope.TEST:
            # Steal individual tests from the end of the queue.
            max_steal = max(0, len(pending_indices) - MIN_PENDING)
            num_steal = min(len(pending_indices) // 2, max_steal)
            if num_steal == 0:
                return []
            return pending_indices[-num_steal:]

        # For scope-aware stealing, group pending indices by scope and steal
        # complete groups only.
        scope_groups: dict[str, list[int]] = {}
        for idx in pending_indices:
            scope_key = _extract_scope(self.collection[idx], self.scope)
            scope_groups.setdefault(scope_key, []).append(idx)

        if len(scope_groups) < 2:
            # Only one scope group -- can't steal a whole group without
            # starving the source.
            return []

        # Steal complete groups from the end, keeping at least one group.
        group_list = list(scope_groups.values())
        max_groups = len(group_list) // 2
        stolen: list[int] = []
        for group_indices in reversed(group_list):
            if max_groups <= 0:
                break
            stolen.extend(group_indices)
            max_groups -= 1

        return stolen

    def _check_nodes_have_same_collection(self) -> bool:
        """Return True if all nodes collected the same test items."""
        import pytest

        items = list(self.node2collection.items())
        first_node, first_col = items[0]
        same = True
        for node, collection in items[1:]:
            msg = report_collection_diff(
                first_col,
                collection,
                first_node.gateway.id,
                node.gateway.id,
            )
            if msg:
                same = False
                self.log(msg)
                if self.config is not None:
                    rep = pytest.CollectReport(
                        nodeid=node.gateway.id,
                        outcome="failed",
                        longrepr=msg,
                        result=[],
                    )
                    self.config.hook.pytest_collectreport(report=rep)
        return same
