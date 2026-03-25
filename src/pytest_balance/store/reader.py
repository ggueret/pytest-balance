"""JSONL duration store reader with aggregation strategies."""

from __future__ import annotations

import enum
import json
import statistics
import warnings
from collections import defaultdict
from pathlib import Path

from pytest_balance.store.models import DurationEstimate


class Estimator(enum.Enum):
    EMA = "ema"
    MEDIAN = "median"
    LAST = "last"


def load_estimates(
    path: Path,
    estimator: Estimator = Estimator.EMA,
    alpha: float = 0.3,
    max_runs: int = 50,
) -> dict[str, DurationEstimate]:
    if not path.exists():
        return {}

    records: defaultdict[str, list[float]] = defaultdict(list)

    with open(path) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                test_id = data["test_id"]
                duration = float(data["duration"])
                durations = records[test_id]
                durations.append(duration)
                if len(durations) > max_runs * 2:
                    records[test_id] = durations[-max_runs:]
            except (json.JSONDecodeError, KeyError, ValueError):
                warnings.warn(
                    f"Skipping corrupted line {line_num} in {path}",
                    UserWarning,
                    stacklevel=2,
                )
                continue

    for test_id in records:
        if len(records[test_id]) > max_runs:
            records[test_id] = records[test_id][-max_runs:]

    estimates: dict[str, DurationEstimate] = {}
    for test_id, durations in records.items():
        estimate = _aggregate(durations, estimator, alpha)
        confidence = min(1.0, len(durations) / 10.0)
        estimates[test_id] = DurationEstimate(
            test_id=test_id,
            estimate=estimate,
            confidence=confidence,
            sample_count=len(durations),
        )

    return estimates


def _aggregate(durations: list[float], estimator: Estimator, alpha: float) -> float:
    if not durations:
        return 0.0
    if estimator == Estimator.LAST:
        return durations[-1]
    if estimator == Estimator.MEDIAN:
        return statistics.median(durations)
    # EMA
    estimate = durations[0]
    for d in durations[1:]:
        estimate = alpha * d + (1 - alpha) * estimate
    return estimate


def default_estimate(estimates: dict[str, DurationEstimate]) -> DurationEstimate:
    if not estimates:
        return DurationEstimate(test_id="", estimate=1.0, confidence=0.0, sample_count=0)
    median_duration = statistics.median(e.estimate for e in estimates.values())
    return DurationEstimate(test_id="", estimate=median_duration, confidence=0.0, sample_count=0)
