from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pytest_balance.store.merger import merge_files
from pytest_balance.store.models import TestDuration
from pytest_balance.store.writer import append_durations


def _td(test_id: str, duration: float, run_id: str) -> TestDuration:
    return TestDuration(test_id, duration, datetime(2026, 1, 1, tzinfo=timezone.utc), run_id, "w0")


class TestMergeFiles:
    def test_merge_two_partials(self, tmp_path: Path):
        p1 = tmp_path / "partial-0.jsonl"
        p2 = tmp_path / "partial-1.jsonl"
        append_durations(p1, [_td("a", 1.0, "r1"), _td("b", 2.0, "r1")])
        append_durations(p2, [_td("c", 3.0, "r1"), _td("d", 4.0, "r1")])
        out = tmp_path / "merged.jsonl"
        merge_files([p1, p2], out)
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 4
        test_ids = {json.loads(l)["test_id"] for l in lines}
        assert test_ids == {"a", "b", "c", "d"}

    def test_dedup_by_test_id_and_run_id(self, tmp_path: Path):
        p1 = tmp_path / "partial-0.jsonl"
        p2 = tmp_path / "partial-1.jsonl"
        append_durations(p1, [_td("a", 1.0, "r1")])
        append_durations(p2, [_td("a", 1.0, "r1")])  # Duplicate
        out = tmp_path / "merged.jsonl"
        merge_files([p1, p2], out)
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 1

    def test_idempotent(self, tmp_path: Path):
        p1 = tmp_path / "partial-0.jsonl"
        append_durations(p1, [_td("a", 1.0, "r1")])
        out = tmp_path / "merged.jsonl"
        merge_files([p1], out)
        merge_files([p1], out)  # Re-merge into existing
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 1

    def test_merge_into_existing(self, tmp_path: Path):
        out = tmp_path / "merged.jsonl"
        append_durations(out, [_td("a", 1.0, "r1")])
        p1 = tmp_path / "partial-0.jsonl"
        append_durations(p1, [_td("b", 2.0, "r2")])
        merge_files([p1], out)
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_empty_partials_skipped(self, tmp_path: Path):
        p1 = tmp_path / "partial-0.jsonl"
        p1.touch()  # Empty file
        out = tmp_path / "merged.jsonl"
        merge_files([p1], out)
        assert not out.exists() or out.read_text().strip() == ""

    def test_no_files_raises(self, tmp_path: Path):
        import pytest
        with pytest.raises(ValueError, match="No input files"):
            merge_files([], tmp_path / "out.jsonl")
