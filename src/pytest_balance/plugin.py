"""pytest-balance plugin entry point."""

from __future__ import annotations

import uuid
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pytest_balance.algorithms.partitioner import Scope
from pytest_balance.ci.detect import detect_ci
from pytest_balance.ci.splitter import split_tests
from pytest_balance.cli import add_pytest_options
from pytest_balance.report import BalanceReport, NodeReport
from pytest_balance.store.models import DurationEstimate, TestDuration
from pytest_balance.store.reader import Estimator, load_estimates
from pytest_balance.store.writer import append_durations

if TYPE_CHECKING:
    from _pytest.config import Config
    from _pytest.terminal import TerminalReporter


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register balance CLI options with pytest."""
    add_pytest_options(parser)


def pytest_collection_modifyitems(
    config: Config,
    items: list[pytest.Item],
) -> None:
    """Apply CI-based test splitting when --balance is active."""
    if not config.getoption("balance"):
        return

    store_path = Path(config.getoption("balance_path"))
    store_file = store_path / "durations.jsonl"
    estimator = Estimator(config.getoption("balance_estimator"))
    scope = Scope(config.getoption("balance_scope"))

    estimates = load_estimates(store_file, estimator)

    test_ids = [item.nodeid for item in items]

    node_index: int | None = config.getoption("balance_node_index")
    node_total: int | None = config.getoption("balance_node_total")

    # --balance-plan check BEFORE ci guard so plan works without CI
    if config.getoption("balance_plan"):
        _show_plan(test_ids, estimates, node_total or 2, scope)
        raise SystemExit(0)

    ci = detect_ci(explicit_index=node_index, explicit_total=node_total)
    if ci is None:
        return

    selected = split_tests(
        tests=test_ids,
        estimates=estimates,
        node_index=ci.node_index,
        node_total=ci.node_total,
        scope=scope,
    )
    selected_set = set(selected)

    deselected = [item for item in items if item.nodeid not in selected_set]
    items[:] = [item for item in items if item.nodeid in selected_set]

    if deselected:
        config.hook.pytest_deselected(items=deselected)

    # Stash data for reporting
    config.stash[_balance_key] = _BalanceData(
        node_index=ci.node_index,
        node_total=ci.node_total,
        estimates={tid: estimates[tid].estimate for tid in estimates if tid in selected_set},
    )


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Record test durations when --balance-store is active."""
    if not session.config.getoption("balance_store"):
        return

    reporter: TerminalReporter | None = session.config.pluginmanager.getplugin(
        "terminalreporter"
    )
    if reporter is None:
        return

    store_path = Path(session.config.getoption("balance_path"))

    # Detect CI to decide file naming
    node_index: int | None = session.config.getoption("balance_node_index")
    node_total: int | None = session.config.getoption("balance_node_total")
    ci = detect_ci(explicit_index=node_index, explicit_total=node_total)

    run_id = ci.run_id if ci else str(uuid.uuid4())[:8]
    worker = f"node{ci.node_index}" if ci else "local"
    now = datetime.now(timezone.utc)

    records: list[TestDuration] = []
    for report in reporter.stats.get("passed", []) + reporter.stats.get("failed", []):
        if hasattr(report, "when") and report.when == "call":
            records.append(
                TestDuration(
                    test_id=report.nodeid,
                    duration=report.duration,
                    timestamp=now,
                    run_id=run_id,
                    worker=worker,
                    phase="call",
                )
            )

    if not records:
        return

    # Write to partial file in CI, main file locally
    if ci is not None:
        output = store_path / f"durations-{ci.run_id}-{ci.node_index}.jsonl"
    else:
        output = store_path / "durations.jsonl"

    try:
        append_durations(output, records)
    except PermissionError:
        warnings.warn(
            f"Permission denied writing to {output}. Duration data was not saved.",
            UserWarning,
            stacklevel=2,
        )
        session.exitstatus = 1


def pytest_terminal_summary(
    terminalreporter: TerminalReporter,
    exitstatus: int,
    config: Config,
) -> None:
    """Show the balance report after test run."""
    if not config.getoption("balance"):
        return
    if config.getoption("balance_no_report"):
        return

    data: _BalanceData | None = config.stash.get(_balance_key, None)
    if data is None:
        return

    # Compute actual durations from terminal reporter stats
    actual_durations: dict[str, float] = {}
    for report in (
        terminalreporter.stats.get("passed", []) + terminalreporter.stats.get("failed", [])
    ):
        if hasattr(report, "when") and report.when == "call":
            actual_durations[report.nodeid] = report.duration

    estimated_time = sum(data.estimates.values())
    actual_time = sum(actual_durations.values())
    test_count = len(actual_durations)

    node_report = NodeReport(
        node_id=f"node{data.node_index}",
        test_count=test_count,
        estimated_time=estimated_time,
        actual_time=actual_time,
    )

    # Build worst predictions
    worst: list[tuple[str, float, float]] = []
    for tid, actual in actual_durations.items():
        est = data.estimates.get(tid, 0.0)
        worst.append((tid, est, actual))
    worst.sort(key=lambda x: abs(x[2] - x[1]), reverse=True)

    report = BalanceReport(nodes=[node_report], worst_predictions=worst[:3])
    terminalreporter.write_line("")
    terminalreporter.write_line(report.format())


def _show_plan(
    test_ids: list[str],
    estimates: dict[str, DurationEstimate],
    node_total: int,
    scope: Scope,
) -> None:
    """Display a distribution plan and exit."""
    from pytest_balance.algorithms.lpt import partition
    from pytest_balance.algorithms.partitioner import group_by_scope
    from pytest_balance.store.reader import default_estimate

    fallback = default_estimate(estimates)

    groups = group_by_scope(test_ids, scope)

    group_durations: dict[str, float] = {}
    for group in groups:
        group.estimated_duration = sum(
            estimates.get(tid, fallback).estimate for tid in group.test_ids
        )
        group_durations[group.scope_id] = group.estimated_duration

    buckets = partition(group_durations, node_total)

    for i, bucket in enumerate(buckets):
        bucket_time = sum(group_durations.get(scope_id, 0.0) for scope_id in bucket)
        print(f"Node {i}: {len(bucket)} group(s), {bucket_time:.1f}s estimated")
        for scope_id in bucket:
            print(f"  {scope_id} ({group_durations[scope_id]:.3f}s)")


# --- Internal stash key and data ---

_balance_key = pytest.StashKey["_BalanceData"]()


class _BalanceData:
    __slots__ = ("estimates", "node_index", "node_total")

    def __init__(self, node_index: int, node_total: int, estimates: dict[str, float]) -> None:
        self.node_index = node_index
        self.node_total = node_total
        self.estimates = estimates


# --- Conditional xdist hook ---

try:
    import xdist  # type: ignore[import-untyped]  # noqa: F401

    def pytest_xdist_make_scheduler(config: Config, log: object) -> object:
        from pytest_balance.xdist import hooks as _xdist_hooks

        return _xdist_hooks.make_balance_scheduler(config, log)

except ImportError:
    pass
