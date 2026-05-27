from __future__ import annotations

import pytest

pytest.importorskip("xdist")

from pytest_balance.algorithms.partitioner import Scope

from ._simulator import Sim, build_collection


def test_simulator_runs_to_coherent_completion():
    collection, estimates = build_collection(6, 2, shuffle_seed=1)
    sim = Sim(collection, estimates, n_workers=3, scope=Scope.MODULE)
    sim.run(decisions=[0, 1, 2, 3, 0, 1])  # short list -> falls back to drain
    assert sim.coherence_ok()
