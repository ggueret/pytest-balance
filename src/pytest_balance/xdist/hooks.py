"""xdist hook implementations for pytest-balance."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any


def make_balance_scheduler(config: Any, log: Any) -> Any:
    if not config.getoption("balance", default=False):
        return None

    dist_mode = config.getoption("dist", default="no")
    if dist_mode == "each":
        warnings.warn(
            "--balance is incompatible with --dist each, ignoring balance",
            UserWarning,
            stacklevel=2,
        )
        return None

    try:
        from pytest_balance.xdist.scheduler import BalanceScheduler
    except ImportError:
        return None

    from pytest_balance.algorithms.partitioner import Scope
    from pytest_balance.store.reader import Estimator, load_estimates

    scope = Scope(config.getoption("balance_scope"))
    store_path = Path(config.getoption("balance_path")) / "durations.jsonl"
    estimator_name = config.getoption("balance_estimator")
    estimator = Estimator(estimator_name)
    estimates = load_estimates(store_path, estimator=estimator)
    return BalanceScheduler(config, log, scope, estimates)
