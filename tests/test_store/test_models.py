from __future__ import annotations

from datetime import datetime, timezone

from pytest_balance.store.models import DurationEstimate, TestDuration


class TestTestDuration:
    def test_create(self):
        td = TestDuration(
            test_id="tests/test_a.py::test_1",
            duration=1.234,
            timestamp=datetime(2026, 3, 24, tzinfo=timezone.utc),
            run_id="ci-456",
            worker="runner-1",
        )
        assert td.test_id == "tests/test_a.py::test_1"
        assert td.duration == 1.234
        assert td.phase == "call"

    def test_immutable(self):
        import pytest

        td = TestDuration(
            test_id="t",
            duration=1.0,
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            run_id="r",
            worker="w",
        )
        with pytest.raises(AttributeError):
            td.duration = 2.0  # type: ignore[misc]

    def test_negative_duration_clamped(self):
        td = TestDuration(
            test_id="t",
            duration=-1.0,
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            run_id="r",
            worker="w",
        )
        assert td.duration == 0.0


class TestDurationEstimate:
    def test_create(self):
        est = DurationEstimate(
            test_id="tests/test_a.py::test_1",
            estimate=2.5,
            confidence=0.8,
            sample_count=10,
        )
        assert est.estimate == 2.5
        assert est.confidence == 0.8
        assert est.sample_count == 10

    def test_zero_confidence_for_unknown(self):
        est = DurationEstimate(
            test_id="unknown",
            estimate=1.0,
            confidence=0.0,
            sample_count=0,
        )
        assert est.confidence == 0.0
        assert est.sample_count == 0
