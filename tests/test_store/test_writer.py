from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pytest_balance.store.models import TestDuration
from pytest_balance.store.writer import append_durations


class TestAppendDurations:
    def test_creates_file(self, tmp_path: Path):
        store = tmp_path / "durations.jsonl"
        records = [
            TestDuration("test_a", 1.5, datetime(2026, 3, 24, tzinfo=timezone.utc), "run-1", "w0"),
        ]
        append_durations(store, records)
        assert store.exists()
        line = json.loads(store.read_text().strip())
        assert line["test_id"] == "test_a"
        assert line["duration"] == 1.5

    def test_appends_to_existing(self, tmp_path: Path):
        store = tmp_path / "durations.jsonl"
        r1 = [TestDuration("a", 1.0, datetime(2026, 1, 1, tzinfo=timezone.utc), "r1", "w0")]
        r2 = [TestDuration("b", 2.0, datetime(2026, 1, 2, tzinfo=timezone.utc), "r2", "w0")]
        append_durations(store, r1)
        append_durations(store, r2)
        lines = store.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_multiple_records(self, tmp_path: Path):
        store = tmp_path / "durations.jsonl"
        records = [
            TestDuration("a", 1.0, datetime(2026, 1, 1, tzinfo=timezone.utc), "r1", "w0"),
            TestDuration("b", 2.0, datetime(2026, 1, 1, tzinfo=timezone.utc), "r1", "w0"),
            TestDuration("c", 3.0, datetime(2026, 1, 1, tzinfo=timezone.utc), "r1", "w0"),
        ]
        append_durations(store, records)
        lines = store.read_text().strip().split("\n")
        assert len(lines) == 3

    def test_creates_parent_directory(self, tmp_path: Path):
        store = tmp_path / "subdir" / "durations.jsonl"
        records = [
            TestDuration("a", 1.0, datetime(2026, 1, 1, tzinfo=timezone.utc), "r1", "w0"),
        ]
        append_durations(store, records)
        assert store.exists()

    def test_empty_records(self, tmp_path: Path):
        store = tmp_path / "durations.jsonl"
        append_durations(store, [])
        assert not store.exists()

    def test_timestamp_serialized_as_iso(self, tmp_path: Path):
        store = tmp_path / "durations.jsonl"
        ts = datetime(2026, 3, 24, 10, 30, 0, tzinfo=timezone.utc)
        records = [TestDuration("a", 1.0, ts, "r1", "w0")]
        append_durations(store, records)
        line = json.loads(store.read_text().strip())
        assert line["timestamp"] == "2026-03-24T10:30:00+00:00"
