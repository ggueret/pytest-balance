"""CLI option registration and standalone entry point for pytest-balance."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest


def add_pytest_options(parser: pytest.Parser) -> None:
    """Register all --balance-* options with pytest."""
    group = parser.getgroup("balance", "Test distribution and balancing")

    group.addoption(
        "--balance",
        action="store_true",
        default=False,
        help="Enable balanced test distribution across CI nodes.",
    )

    group.addoption(
        "--balance-store",
        action="store_true",
        default=False,
        help="Record test durations to the balance store.",
    )

    group.addoption(
        "--balance-scope",
        choices=["test", "class", "module", "file", "group"],
        default="module",
        help="Scope for grouping tests (default: module).",
    )

    group.addoption(
        "--balance-path",
        default=".balance/",
        help="Path to the balance store directory (default: .balance/).",
    )

    group.addoption(
        "--balance-plan",
        action="store_true",
        default=False,
        help="Show the distribution plan without running tests.",
    )

    group.addoption(
        "--balance-node-index",
        type=int,
        default=None,
        help="Explicit node index (overrides CI auto-detection).",
    )

    group.addoption(
        "--balance-node-total",
        type=int,
        default=None,
        help="Explicit total node count (overrides CI auto-detection).",
    )

    group.addoption(
        "--balance-algorithm",
        choices=["lpt"],
        default="lpt",
        help="Partitioning algorithm (default: lpt).",
    )

    group.addoption(
        "--balance-estimator",
        choices=["ema", "median", "last"],
        default="ema",
        help="Duration estimation strategy (default: ema).",
    )

    group.addoption(
        "--balance-no-report",
        action="store_true",
        default=False,
        help="Suppress the balance report after test run.",
    )


def main() -> None:
    """Standalone CLI entry point with merge/prune/stats/plan subcommands."""
    parser = argparse.ArgumentParser(
        prog="pytest-balance",
        description="Manage pytest-balance duration store.",
    )
    parser.add_argument(
        "--path",
        default=".balance/",
        help="Path to the balance store directory (default: .balance/).",
    )

    subparsers = parser.add_subparsers(dest="command")

    # merge
    merge_parser = subparsers.add_parser(
        "merge", help="Merge partial duration files into durations.jsonl."
    )
    merge_parser.add_argument(
        "files",
        nargs="*",
        help="Partial files to merge (default: auto-detect partials).",
    )

    # prune
    prune_parser = subparsers.add_parser(
        "prune", help="Remove old entries from the duration store."
    )
    prune_parser.add_argument(
        "--max-runs",
        type=int,
        default=50,
        help="Maximum number of runs to keep per test (default: 50).",
    )

    # stats
    stats_parser = subparsers.add_parser(
        "stats", help="Show statistics about the duration store."
    )
    stats_parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output statistics as JSON.",
    )

    # plan
    plan_parser = subparsers.add_parser(
        "plan", help="Show the distribution plan for a given node count."
    )
    plan_parser.add_argument(
        "node_total",
        type=int,
        help="Total number of nodes to plan for.",
    )
    plan_parser.add_argument(
        "--scope",
        choices=["test", "class", "module", "file", "group"],
        default="module",
        help="Scope for grouping tests (default: module).",
    )
    plan_parser.add_argument(
        "--estimator",
        choices=["ema", "median", "last"],
        default="ema",
        help="Duration estimation strategy (default: ema).",
    )
    plan_parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output plan as JSON.",
    )

    args = parser.parse_args()
    store_path = Path(args.path)

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    if args.command == "merge":
        _cmd_merge(store_path, args.files)
    elif args.command == "prune":
        _cmd_prune(store_path, args.max_runs)
    elif args.command == "stats":
        _cmd_stats(store_path, args.output_json)
    elif args.command == "plan":
        _cmd_plan(store_path, args.node_total, args.scope, args.estimator, args.output_json)


def _cmd_merge(store_path: Path, files: list[str] | None) -> None:
    from pytest_balance.store.merger import merge_files

    output = store_path / "durations.jsonl"

    inputs = [Path(f) for f in files] if files else sorted(store_path.glob("durations-*.jsonl"))

    if not inputs:
        print("No partial files found to merge.")
        sys.exit(0)

    merge_files(inputs, output)
    print(f"Merged {len(inputs)} file(s) into {output}")

    # Clean up partials after successful merge
    for f in inputs:
        if f != output and f.exists():
            f.unlink()


def _cmd_prune(store_path: Path, max_runs: int) -> None:
    import json as json_mod

    store = store_path / "durations.jsonl"
    if not store.exists():
        print("No duration store found.")
        sys.exit(0)

    # Group records by test_id, keep only last max_runs
    records: dict[str, list[str]] = {}
    for line in store.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json_mod.loads(line)
            tid = data["test_id"]
            if tid not in records:
                records[tid] = []
            records[tid].append(line)
        except (json_mod.JSONDecodeError, KeyError):
            continue

    total_before = sum(len(v) for v in records.values())
    pruned_lines: list[str] = []
    for tid in records:
        pruned_lines.extend(records[tid][-max_runs:])

    total_after = len(pruned_lines)
    store.write_text("\n".join(pruned_lines) + "\n" if pruned_lines else "")
    print(f"Pruned {total_before - total_after} records ({total_before} -> {total_after})")


def _cmd_stats(store_path: Path, output_json: bool) -> None:
    from pytest_balance.store.reader import Estimator, load_estimates

    store = store_path / "durations.jsonl"
    estimates = load_estimates(store, Estimator.EMA)

    if not estimates:
        print("No duration data found.")
        sys.exit(0)

    total_tests = len(estimates)
    total_time = sum(e.estimate for e in estimates.values())
    avg_time = total_time / total_tests if total_tests else 0.0
    max_test = max(estimates.values(), key=lambda e: e.estimate)
    min_test = min(estimates.values(), key=lambda e: e.estimate)

    if output_json:
        print(
            json.dumps(
                {
                    "total_tests": total_tests,
                    "total_time": round(total_time, 3),
                    "avg_time": round(avg_time, 3),
                    "max": {"test_id": max_test.test_id, "estimate": round(max_test.estimate, 3)},
                    "min": {"test_id": min_test.test_id, "estimate": round(min_test.estimate, 3)},
                },
                indent=2,
            )
        )
    else:
        print(f"Tests: {total_tests}")
        print(f"Total estimated time: {total_time:.1f}s")
        print(f"Average: {avg_time:.3f}s")
        print(f"Slowest: {max_test.test_id} ({max_test.estimate:.3f}s)")
        print(f"Fastest: {min_test.test_id} ({min_test.estimate:.3f}s)")


def _cmd_plan(
    store_path: Path,
    node_total: int,
    scope: str,
    estimator: str,
    output_json: bool,
) -> None:
    from pytest_balance.algorithms.lpt import partition
    from pytest_balance.algorithms.partitioner import Scope, group_by_scope
    from pytest_balance.store.reader import Estimator, load_estimates

    store = store_path / "durations.jsonl"
    estimates = load_estimates(store, Estimator(estimator))

    if not estimates:
        print("No duration data found.")
        sys.exit(0)

    test_ids = list(estimates.keys())
    groups = group_by_scope(test_ids, Scope(scope))

    group_durations: dict[str, float] = {}
    for group in groups:
        group.estimated_duration = sum(estimates[tid].estimate for tid in group.test_ids)
        group_durations[group.scope_id] = group.estimated_duration

    buckets = partition(group_durations, node_total)

    if output_json:
        plan: list[dict[str, object]] = []
        for i, bucket in enumerate(buckets):
            bucket_time = sum(group_durations.get(scope_id, 0.0) for scope_id in bucket)
            plan.append(
                {
                    "node": i,
                    "groups": bucket,
                    "estimated_time": round(bucket_time, 3),
                }
            )
        print(json.dumps(plan, indent=2))
    else:
        for i, bucket in enumerate(buckets):
            bucket_time = sum(group_durations.get(scope_id, 0.0) for scope_id in bucket)
            print(f"Node {i}: {len(bucket)} group(s), {bucket_time:.1f}s estimated")
            for scope_id in bucket:
                print(f"  {scope_id} ({group_durations[scope_id]:.3f}s)")
