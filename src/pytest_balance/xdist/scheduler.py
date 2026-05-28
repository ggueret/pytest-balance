"""Duration-aware xdist scheduler with work-stealing.

Subclass of xdist's WorkStealingScheduling. Replaces the default pending order
(collection order) with an LPT scope-adjacent order computed by compute_order,
and keeps the diagnostic invariant + bounded scheduling-event history
introduced for issue #18.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, NoReturn, cast

from xdist.scheduler.worksteal import WorkStealingScheduling  # type: ignore[import-untyped]

from pytest_balance.algorithms.lpt import compute_order
from pytest_balance.algorithms.partitioner import Scope
from pytest_balance.store.models import DurationEstimate

if TYPE_CHECKING:
    import pytest
    from xdist.remote import Producer  # type: ignore[import-untyped]
    from xdist.workermanage import WorkerController  # type: ignore[import-untyped]


class SchedulerInvariantError(RuntimeError):
    """A scheduler bookkeeping invariant was violated (see issue #18)."""


class BalanceScheduler(WorkStealingScheduling):  # type: ignore[misc]
    """LPT-ordered work-stealing scheduler.

    Inherits from xdist's WorkStealingScheduling. Only the initial ordering of
    self.pending changes: instead of the collection order, we use a LPT
    scope-adjacent order. Native xdist handles distribution, work-stealing,
    and termination unchanged.
    """

    collection: list[str] | None

    def __init__(
        self,
        config: pytest.Config | Any,
        log: Producer | Any,
        scope: Scope,
        estimates: dict[str, DurationEstimate],
    ) -> None:
        super().__init__(config, log)
        self.scope = scope
        self.estimates = estimates
        self._history: deque[str] = deque(maxlen=64)

    # -- Overrides ------------------------------------------------------------

    def schedule(self) -> None:
        """Initial dispatch. Reorders self.pending with compute_order, then
        delegates to native check_schedule for distribution.
        """
        assert self.collection_is_completed

        if self.collection is not None:
            # Already initialised; just rebalance.
            self.check_schedule()
            return

        if not self._check_nodes_have_same_collection():
            self.log("**Different tests collected, aborting run**")
            return

        self.collection = next(iter(self.node2collection.values()))
        if not self.collection:
            return

        order = compute_order(list(self.collection), self.estimates, self.scope)
        self.pending[:] = order
        self._record("schedule", None, len(order))
        self.check_schedule()

    def mark_test_complete(
        self,
        node: WorkerController,
        item_index: int,
        duration: float | None = None,
    ) -> None:
        """Mark a test as completed. Diagnostic guard from issue #18."""
        if item_index not in self.node2pending[node]:
            self._raise_invariant_violation(node, item_index)
        self._record("complete", node, item_index)
        super().mark_test_complete(node, item_index, duration)

    def remove_pending_tests_from_node(
        self,
        node: WorkerController,
        indices: Sequence[int],
    ) -> None:
        """Native unscheduled handling + diagnostic recording."""
        self._record("unscheduled", node, list(indices))
        super().remove_pending_tests_from_node(node, indices)

    def remove_node(self, node: WorkerController) -> str | None:
        """Native node removal + diagnostic recording."""
        self._record("remove_node", node, list(self.node2pending.get(node, [])))
        return cast("str | None", super().remove_node(node))

    # -- Internal helpers (diagnostic, from issue #19) ------------------------

    def _record(
        self,
        event: str,
        node: WorkerController | None,
        payload: object,
    ) -> None:
        """Append a compact scheduling event to the bounded history."""
        node_id = node.gateway.id if node is not None else "-"
        self._history.append(f"{event}({node_id}, {payload})")

    def _raise_invariant_violation(
        self,
        node: WorkerController,
        item_index: int,
    ) -> NoReturn:
        """Build a rich diagnostic, log it, then raise. Never returns."""
        if self.collection is not None and 0 <= item_index < len(self.collection):
            test_id = self.collection[item_index]
        else:
            test_id = "<unknown>"

        locations = [
            f"node2pending[{other.gateway.id}]"
            for other, pending in self.node2pending.items()
            if item_index in pending
        ]
        if item_index in self.pending:
            locations.append("global pending")
        where = (
            ", ".join(locations) if locations else "nowhere (already completed or never assigned?)"
        )

        steal = (
            self.steal_requested_from_node.gateway.id
            if self.steal_requested_from_node is not None
            else None
        )

        history = "\n".join(f"  {entry}" for entry in self._history) or "  <empty>"
        msg = (
            f"Scheduler invariant violated: node {node.gateway.id} reported completion "
            f"of item {item_index} ({test_id}), which is not in its pending list. "
            f"Item currently found in: {where}. steal_requested_from_node={steal!r}.\n"
            f"Recent scheduling events:\n{history}"
        )
        self.log(msg)
        raise SchedulerInvariantError(msg)
