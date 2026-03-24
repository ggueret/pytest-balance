from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pytest_balance.store.models import TestDuration
from pytest_balance.store.reader import Estimator, load_estimates
from pytest_balance.store.writer import append_durations


def _td(test_id: str, duration: float, run_id: str = "r1") -> TestDuration:
    return TestDuration(test_id, duration, datetime(2026, 1, 1, tzinfo=timezone.utc), run_id, "w0")


class TestLoadEstimates:
    def test_single_run(self, tmp_path: Path):
        store = tmp_path / "d.jsonl"
        append_durations(store, [_td("a", 1.5), _td("b", 3.0)])
        estimates = load_estimates(store)
        assert estimates["a"].estimate == 1.5
        assert estimates["b"].estimate == 3.0

    def test_ema_multiple_runs(self, tmp_path: Path):
        store = tmp_path / "d.jsonl"
        append_durations(store, [_td("a", 1.0, "r1")])
        append_durations(store, [_td("a", 5.0, "r2")])
        estimates = load_estimates(store, estimator=Estimator.EMA, alpha=0.3)
        # EMA: est = 0.3 * 5.0 + 0.7 * 1.0 = 2.2
        assert abs(estimates["a"].estimate - 2.2) < 0.01

    def test_median_estimator(self, tmp_path: Path):
        store = tmp_path / "d.jsonl"
        append_durations(store, [_td("a", 1.0, "r1")])
        append_durations(store, [_td("a", 3.0, "r2")])
        append_durations(store, [_td("a", 100.0, "r3")])
        estimates = load_estimates(store, estimator=Estimator.MEDIAN)
        assert estimates["a"].estimate == 3.0

    def test_last_estimator(self, tmp_path: Path):
        store = tmp_path / "d.jsonl"
        append_durations(store, [_td("a", 1.0, "r1")])
        append_durations(store, [_td("a", 5.0, "r2")])
        estimates = load_estimates(store, estimator=Estimator.LAST)
        assert estimates["a"].estimate == 5.0

    def test_confidence_increases_with_samples(self, tmp_path: Path):
        store = tmp_path / "d.jsonl"
        for i in range(10):
            append_durations(store, [_td("a", 1.0, f"r{i}")])
        estimates = load_estimates(store)
        assert estimates["a"].confidence > 0.5
        assert estimates["a"].sample_count == 10

    def test_missing_file_returns_empty(self, tmp_path: Path):
        store = tmp_path / "nonexistent.jsonl"
        estimates = load_estimates(store)
        assert estimates == {}

    def test_corrupted_line_skipped(self, tmp_path: Path):
        store = tmp_path / "d.jsonl"
        append_durations(store, [_td("a", 1.0)])
        with open(store, "a") as f:
            f.write("NOT VALID JSON\n")
        append_durations(store, [_td("b", 2.0, "r2")])
        estimates = load_estimates(store)
        assert "a" in estimates
        assert "b" in estimates

    def test_default_estimate_for_unknown(self, tmp_path: Path):
        store = tmp_path / "d.jsonl"
        append_durations(store, [_td("a", 1.0), _td("b", 3.0), _td("c", 5.0)])
        estimates = load_estimates(store)
        from pytest_balance.store.reader import default_estimate
        unknown = default_estimate(estimates)
        assert unknown.estimate == 3.0  # Median of known
        assert unknown.confidence == 0.0
