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
        choices=["test", "class", "module", "group"],
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
    merge_parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output file (default: <--path>/durations.jsonl).",
    )

    # prune
    prune_parser = subparsers.add_parser(
        "prune", help="Remove old entries from the duration store."
    )
    prune_parser.add_argument(
        "store",
        nargs="?",
        default=None,
        help="Path to the duration store file (default: <--path>/durations.jsonl).",
    )
    prune_parser.add_argument(
        "--keep-runs",
        "--max-runs",
        type=int,
        default=50,
        dest="keep_runs",
        help="Maximum number of runs to keep per test (default: 50).",
    )

    # stats
    stats_parser = subparsers.add_parser("stats", help="Show statistics about the duration store.")
    stats_parser.add_argument(
        "store",
        nargs="?",
        default=None,
        help="Path to the duration store file (default: <--path>/durations.jsonl).",
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
        choices=["test", "class", "module", "group"],
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
        _cmd_merge(store_path, args.files, args.output)
    elif args.command == "prune":
        _cmd_prune(store_path, args.keep_runs, args.store)
    elif args.command == "stats":
        _cmd_stats(store_path, args.output_json, args.store)
    elif args.command == "plan":
        _cmd_plan(store_path, args.node_total, args.scope, args.estimator, args.output_json)


def _cmd_merge(store_path: Path, files: list[str] | None, output_arg: str | None = None) -> None:
    from pytest_balance.store.merger import merge_files

    output = Path(output_arg) if output_arg else store_path / "durations.jsonl"

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


def _cmd_prune(store_path: Path, max_runs: int, store_arg: str | None = None) -> None:
    import json as json_mod

    store = Path(store_arg) if store_arg else store_path / "durations.jsonl"
    if not store.exists():
        print("No duration store found.")
        sys.exit(0)

    # Single-pass: parse each line once, group by run_id
    run_id_lines: dict[str, list[str]] = {}
    run_id_order: list[str] = []
    no_rid_lines: list[str] = []
    total_before = 0

    with open(store) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json_mod.loads(line)
            except (json_mod.JSONDecodeError, KeyError):
                continue
            total_before += 1
            rid = data.get("run_id", "")
            if not rid:
                no_rid_lines.append(line)
                continue
            if rid not in run_id_lines:
                run_id_lines[rid] = []
                run_id_order.append(rid)
            run_id_lines[rid].append(line)

    # Keep the last max_runs distinct run_ids
    keep_rids = run_id_order[-max_runs:]
    pruned_lines = list(no_rid_lines)
    for rid in keep_rids:
        pruned_lines.extend(run_id_lines[rid])

    total_after = len(pruned_lines)
    store.write_text("\n".join(pruned_lines) + "\n" if pruned_lines else "")
    print(f"Pruned {total_before - total_after} records ({total_before} -> {total_after})")


def _cmd_stats(store_path: Path, output_json: bool, store_arg: str | None = None) -> None:
    from pytest_balance.store.reader import Estimator, load_estimates

    store = Path(store_arg) if store_arg else store_path / "durations.jsonl"
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
        print(f"{total_tests} tests")
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
    from pytest_balance.store.reader import Estimator, default_estimate, load_estimates

    store = store_path / "durations.jsonl"
    estimates = load_estimates(store, Estimator(estimator))

    if not estimates:
        print("No duration data found.")
        sys.exit(0)

    fallback = default_estimate(estimates)
    test_ids = list(estimates.keys())
    groups = group_by_scope(test_ids, Scope(scope))

    group_durations: dict[str, float] = {}
    for group in groups:
        group.estimated_duration = sum(
            estimates.get(tid, fallback).estimate for tid in group.test_ids
        )
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
                print(f"  {scope_id} ({group_durations.get(scope_id, 0.0):.3f}s)")
