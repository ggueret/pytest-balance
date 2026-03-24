from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from pytest_balance.store.models import TestDuration
from pytest_balance.store.writer import append_durations


def _td(test_id: str, duration: float, run_id: str) -> TestDuration:
    return TestDuration(test_id, duration, datetime(2026, 1, 1, tzinfo=timezone.utc), run_id, "w0")


class TestCLIMerge:
    def test_merge_command(self, tmp_path: Path):
        p1 = tmp_path / "partial-0.jsonl"
        p2 = tmp_path / "partial-1.jsonl"
        out = tmp_path / "merged.jsonl"
        append_durations(p1, [_td("a", 1.0, "r1")])
        append_durations(p2, [_td("b", 2.0, "r1")])

        result = subprocess.run(
            [sys.executable, "-m", "pytest_balance", "merge", str(p1), str(p2), "-o", str(out)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert out.exists()
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 2


class TestCLIPrune:
    def test_prune_command(self, tmp_path: Path):
        store = tmp_path / "d.jsonl"
        for i in range(10):
            append_durations(store, [_td(f"t{i}", 1.0, f"r{i}")])

        result = subprocess.run(
            [sys.executable, "-m", "pytest_balance", "prune", str(store), "--keep-runs", "3"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        lines = store.read_text().strip().split("\n")
        assert len(lines) == 3


class TestCLIStats:
    def test_stats_command(self, tmp_path: Path):
        store = tmp_path / "d.jsonl"
        append_durations(store, [_td("slow", 10.0, "r1"), _td("fast", 0.1, "r1")])

        result = subprocess.run(
            [sys.executable, "-m", "pytest_balance", "stats", str(store)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "2 tests" in result.stdout
        assert "slow" in result.stdout
