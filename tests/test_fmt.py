from __future__ import annotations

import pytest

from pytest_balance._fmt import format_duration


class TestFormatDuration:
    @pytest.mark.parametrize(
        "seconds,expected",
        [
            (0.00042, "420µs"),
            (0.000123, "123µs"),
            (0.042, "42ms"),
            (0.42, "420ms"),
            (0.001, "1ms"),
            (1.0, "1.0s"),
            (12.3, "12.3s"),
        ],
    )
    def test_adaptive_unit(self, seconds: float, expected: str):
        assert format_duration(seconds) == expected
