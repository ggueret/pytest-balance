"""Data models for the duration store."""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class TestDuration:
    """A single recorded test duration from one run."""

    __test__ = False  # Not a pytest test class

    test_id: str
    duration: float
    timestamp: datetime
    run_id: str
    worker: str
    phase: str = "call"

    def __post_init__(self) -> None:
        if self.duration < 0:
            warnings.warn(
                f"Negative duration {self.duration} for {self.test_id}, clamping to 0.0",
                UserWarning,
                stacklevel=2,
            )
            object.__setattr__(self, "duration", 0.0)


@dataclass(frozen=True, slots=True)
class DurationEstimate:
    """Aggregated duration estimate for a test."""

    test_id: str
    estimate: float
    confidence: float
    sample_count: int
