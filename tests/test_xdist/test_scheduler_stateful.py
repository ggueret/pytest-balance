from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

pytest.importorskip("xdist")

from pytest_balance.algorithms.partitioner import Scope

from ._simulator import Sim, build_collection


def test_simulator_runs_to_coherent_completion():
    collection, estimates = build_collection(6, 2, shuffle_seed=1)
    sim = Sim(collection, estimates, n_workers=3, scope=Scope.MODULE)
    sim.run(decisions=[0, 1, 2, 3, 0, 1])  # short list -> falls back to drain
    assert sim.coherence_ok()


@settings(max_examples=200, deadline=None)
@given(
    n_modules=st.integers(min_value=2, max_value=8),
    tests_per_module=st.integers(min_value=1, max_value=4),
    n_workers=st.integers(min_value=2, max_value=6),
    shuffle_seed=st.integers(min_value=0, max_value=2**16),
    scope=st.sampled_from([Scope.MODULE, Scope.TEST]),
    decisions=st.lists(st.integers(min_value=0, max_value=2**16), max_size=2000),
)
def test_scheduler_preserves_coherence_under_any_interleaving(
    n_modules, tests_per_module, n_workers, shuffle_seed, scope, decisions
):
    collection, estimates = build_collection(n_modules, tests_per_module, shuffle_seed)
    sim = Sim(collection, estimates, n_workers, scope)
    # run() asserts unique possession at every step and raises
    # SchedulerInvariantError on a bookkeeping violation; Hypothesis shrinks
    # any failing decision list to the minimal reproducer.
    sim.run(decisions)
    assert sim.coherence_ok()
