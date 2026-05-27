"""Faithful fake-worker simulator driving the real BalanceScheduler.

Mirrors xdist's worker protocol (xdist/remote.py): torun is the only stealable
queue; the running item and the prefetched lookahead are pulled out of torun;
steal is all-or-nothing. Events (complete/unscheduled) are produced by workers
and delivered to the controller in an order chosen by a decision list, so a
property test can explore interleavings and shrink failures.
"""

from __future__ import annotations

import random
from collections import Counter, deque
from unittest.mock import MagicMock

from pytest_balance.store.models import DurationEstimate
from pytest_balance.xdist.scheduler import BalanceScheduler

# Unique sentinel meaning "no more work" (xdist signals this via Marker.SHUTDOWN).
# Must be a distinct object so the `is SHUTDOWN` identity check is well-defined.
SHUTDOWN = object()
INIT, PROMOTE, NEED_PULL, EXEC, DONE = "init", "promote", "pull", "exec", "done"


class FakeWorker:
    def __init__(self, gid, sim):
        self.gateway = MagicMock()
        self.gateway.id = gid
        self.shutting_down = False
        self.sim = sim
        self.torun: deque = deque()
        self.lookahead = None
        self.cur = None
        self.shutdown_requested = False
        self.phase = INIT
        self.alive = True

    def send_runtest_some(self, indices):
        self.torun.extend(indices)

    def send_steal(self, indices):
        self.sim.steals.append((self, list(indices)))

    def shutdown(self):
        # Two-step shutdown: shutting_down is read by the scheduler right away to
        # exclude this worker from dispatch; shutdown_requested is set later, when
        # the controller processes the queued "shutdown" action, which unblocks the
        # pull so the worker can drain its last test via the SHUTDOWN sentinel.
        self.shutting_down = True
        self.sim.shutdowns.append(self)

    def _can_pull(self):
        return bool(self.torun) or self.shutdown_requested

    def _pull(self):
        return self.torun.popleft() if self.torun else SHUTDOWN

    def step(self):
        if not self.alive:
            return None
        if self.phase == INIT:
            if self._can_pull():
                self.lookahead = self._pull()
                self.phase = PROMOTE
            return None
        if self.phase == PROMOTE:
            if self.lookahead is SHUTDOWN:
                self.phase = DONE
                self.alive = False
                return None
            self.cur = self.lookahead
            self.lookahead = None
            self.phase = NEED_PULL
            return None
        if self.phase == NEED_PULL:
            if self._can_pull():
                self.lookahead = self._pull()
                self.phase = EXEC
            return None
        if self.phase == EXEC:
            idx = self.cur
            self.cur = None
            self.phase = PROMOTE
            return ("complete", idx)
        return None

    def blocked(self):
        return self.alive and self.phase in (INIT, NEED_PULL) and not self._can_pull()

    def can_progress(self):
        return self.alive and self.phase != DONE and not self.blocked()

    def do_steal(self, indices):
        # All-or-nothing on torun only (matches xdist remote.py steal). An empty
        # request is a no-op; the real scheduler never sends an empty steal.
        requested = set(indices)
        present = [x for x in self.torun if x in requested]
        if requested and len(present) == len(requested):
            self.torun = deque(x for x in self.torun if x not in requested)
            return list(indices)
        return []


def _mock_config(n):
    config = MagicMock()
    config.getvalue.side_effect = lambda key: {"tx": [f"{n}*popen"]}[key]
    return config


def build_collection(n_modules, tests_per_module, shuffle_seed):
    collection = [
        f"mod_{m:02d}.py::test_{t}" for m in range(n_modules) for t in range(tests_per_module)
    ]
    if shuffle_seed is not None:
        random.Random(shuffle_seed).shuffle(collection)
    estimates = {tid: DurationEstimate(tid, 0.05, 1.0, 5) for tid in collection}
    return collection, estimates


class Sim:
    def __init__(self, collection, estimates, n_workers, scope):
        self.sched = BalanceScheduler(_mock_config(n_workers), MagicMock(), scope, estimates)
        self.workers = [FakeWorker(f"gw{i}", self) for i in range(n_workers)]
        for w in self.workers:
            self.sched.add_node(w)
            self.sched.add_node_collection(w, collection)
        self.steals = []
        self.shutdowns = []
        self.completes = {w: deque() for w in self.workers}
        self.unsched = []
        self.collection = collection
        self.completed: Counter = Counter()

    def _actions(self):
        acts = []
        for w in self.workers:
            if w.can_progress():
                acts.append(("step", w))
        for i in range(len(self.steals)):
            acts.append(("steal", i))
        for i in range(len(self.shutdowns)):
            acts.append(("shutdown", i))
        for w in self.workers:
            if self.completes[w]:
                acts.append(("deliver_complete", w))
        for i in range(len(self.unsched)):
            acts.append(("deliver_unsched", i))
        return acts

    def assert_unique_possession(self):
        seen: dict = {}
        for w, pend in self.sched.node2pending.items():
            for idx in pend:
                assert idx not in seen, (
                    f"index {idx} in both {seen[idx]} and node2pending[{w.gateway.id}]"
                )
                seen[idx] = f"node2pending[{w.gateway.id}]"
        for idx in self.sched.pending:
            assert idx not in seen, f"index {idx} in both {seen[idx]} and global pending"
            seen[idx] = "global pending"

    def _maybe_triggershutdown(self):
        # Mirror dsession.loop_once: after handling an event, if the scheduler
        # reports tests_finished, the controller shuts every worker down. This
        # unblocks any worker stalled in NEED_PULL waiting for a lookahead (the
        # case where each worker holds fewer than MIN_PENDING tests).
        if self.sched.tests_finished:
            for w in self.workers:
                if w.alive and not w.shutting_down:
                    w.shutdown()

    def run(self, decisions, max_ticks=200000):
        """Drive to exhaustion. `decisions` chooses which action fires each tick;
        when exhausted, fall back to action 0 to guarantee drainage."""
        self.sched.schedule()
        self.assert_unique_possession()
        self._maybe_triggershutdown()
        tick = 0
        while True:
            acts = self._actions()
            if not acts:
                break
            tick += 1
            if tick > max_ticks:
                raise AssertionError("simulation did not terminate (possible deadlock)")
            pick = decisions[tick - 1] if tick - 1 < len(decisions) else 0
            kind, ref = acts[pick % len(acts)]
            if kind == "step":
                ev = ref.step()
                if ev is not None:
                    self.completes[ref].append(ev[1])
            elif kind == "steal":
                w, indices = self.steals.pop(ref)
                self.unsched.append((w, w.do_steal(indices)))
            elif kind == "shutdown":
                self.shutdowns.pop(ref).shutdown_requested = True
            elif kind == "deliver_complete":
                idx = self.completes[ref].popleft()
                self.completed[idx] += 1
                self.sched.mark_test_complete(ref, idx, 1.0)
            elif kind == "deliver_unsched":
                w, stolen = self.unsched.pop(ref)
                self.sched.remove_pending_tests_from_node(w, stolen)
            self.assert_unique_possession()
            self._maybe_triggershutdown()

    def coherence_ok(self):
        if any(self.completed[i] != 1 for i in range(len(self.collection))):
            return False
        if self.sched.pending:
            return False
        return all(not self.sched.node2pending[w] for w in self.workers)
