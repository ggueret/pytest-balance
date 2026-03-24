"""JSONL duration store writer."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from pytest_balance.store.models import TestDuration


def _serialize(record: TestDuration) -> str:
    """Serialize a TestDuration to a JSON string."""
    return json.dumps(
        {
            "test_id": record.test_id,
            "duration": record.duration,
            "timestamp": record.timestamp.isoformat(),
            "run_id": record.run_id,
            "worker": record.worker,
            "phase": record.phase,
        },
        separators=(",", ":"),
    )


def append_durations(path: Path, records: list[TestDuration]) -> None:
    """Append test duration records to a JSONL file.

    Creates the file and parent directories if they don't exist.
    Uses atomic write via tempfile for the new content.
    """
    if not records:
        return

    path.parent.mkdir(parents=True, exist_ok=True)

    new_lines = "\n".join(_serialize(r) for r in records) + "\n"

    if path.exists():
        with open(path, "a") as f:
            f.write(new_lines)
    else:
        # Atomic write for new files
        fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        try:
            with open(fd, "w") as f:
                f.write(new_lines)
            Path(tmp).rename(path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
