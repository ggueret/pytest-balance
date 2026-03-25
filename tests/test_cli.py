from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from pytest_balance.store.models import TestDuration
from pytest_balance.store.writer import append_durations


def _td(test_id: str, duration: float, run_id: str) -> TestDuration:
    return TestDuration(test_id, duration, datetime(2026, 1, 1, tzinfo=timezone.utc), run_id, "w0")


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "pytest_balance", *args],
        capture_output=True,
        text=True,
    )


def _seed_store(path: Path) -> None:
    """Write a small duration store useful for plan/stats tests."""
    append_durations(
        path,
        [
            _td("test_a.py::test_1", 1.0, "r1"),
            _td("test_a.py::test_2", 2.0, "r1"),
            _td("test_b.py::test_3", 3.0, "r1"),
        ],
    )


class TestCLIMerge:
    def test_merge_command(self, tmp_path: Path):
        p1 = tmp_path / "partial-0.jsonl"
        p2 = tmp_path / "partial-1.jsonl"
        out = tmp_path / "merged.jsonl"
        append_durations(p1, [_td("a", 1.0, "r1")])
        append_durations(p2, [_td("b", 2.0, "r1")])

        result = _run_cli("merge", str(p1), str(p2), "-o", str(out))
        assert result.returncode == 0
        assert out.exists()
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_merge_no_args(self, tmp_path: Path):
        """merge with no files and empty --path exits 0 with 'No partial' message."""
        result = _run_cli("--path", str(tmp_path), "merge")
        assert result.returncode == 0
        assert "No partial files found" in result.stdout


class TestCLIPrune:
    def test_prune_command(self, tmp_path: Path):
        store = tmp_path / "d.jsonl"
        for i in range(10):
            append_durations(store, [_td(f"t{i}", 1.0, f"r{i}")])

        result = _run_cli("prune", str(store), "--keep-runs", "3")
        assert result.returncode == 0
        lines = store.read_text().strip().split("\n")
        assert len(lines) == 3

    def test_prune_nothing_to_prune(self, tmp_path: Path):
        """When runs <= keep-runs nothing is removed."""
        store = tmp_path / "d.jsonl"
        for i in range(3):
            append_durations(store, [_td(f"t{i}", 1.0, f"r{i}")])

        result = _run_cli("prune", str(store), "--keep-runs", "5")
        assert result.returncode == 0
        assert "0 records" in result.stdout or "Pruned 0" in result.stdout
        lines = store.read_text().strip().split("\n")
        assert len(lines) == 3


class TestCLIStats:
    def test_stats_command(self, tmp_path: Path):
        store = tmp_path / "d.jsonl"
        append_durations(store, [_td("slow", 10.0, "r1"), _td("fast", 0.1, "r1")])

        result = _run_cli("stats", str(store))
        assert result.returncode == 0
        assert "2 tests" in result.stdout
        assert "slow" in result.stdout

    def test_stats_missing_file(self, tmp_path: Path):
        """stats on a non-existent file exits gracefully with 'No duration data'."""
        missing = tmp_path / "nope.jsonl"
        result = _run_cli("stats", str(missing))
        assert result.returncode == 0
        assert "No duration data" in result.stdout


class TestCLIPlan:
    def test_plan_command(self, tmp_path: Path):
        store = tmp_path / "durations.jsonl"
        _seed_store(store)

        result = _run_cli("--path", str(tmp_path), "plan", "2")
        assert result.returncode == 0
        assert "Node 0" in result.stdout
        assert "Node 1" in result.stdout

    def test_plan_command_json(self, tmp_path: Path):
        store = tmp_path / "durations.jsonl"
        _seed_store(store)

        result = _run_cli("--path", str(tmp_path), "plan", "2", "--json")
        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert isinstance(data, list)
        assert len(data) == 2
        for entry in data:
            assert "node" in entry
            assert "groups" in entry
            assert "estimated_time" in entry

    def test_plan_command_missing_file(self, tmp_path: Path):
        """plan with no duration data exits 0 with 'No duration data' message."""
        result = _run_cli("--path", str(tmp_path), "plan", "2")
        assert result.returncode == 0
        assert "No duration data" in result.stdout
