"""Merge partial JSONL duration files from split CI runs."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path


def merge_files(inputs: list[Path], output: Path) -> None:
    if not inputs:
        raise ValueError("No input files provided")

    seen: dict[tuple[str, str], int] = {}
    all_lines: list[str] = []

    sources = []
    if output.exists():
        sources.append(output)
    sources.extend(inputs)

    for path in sources:
        if not path.exists():
            continue
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    key = (data["test_id"], data.get("run_id", ""))
                    if key in seen:
                        all_lines[seen[key]] = line
                    else:
                        seen[key] = len(all_lines)
                        all_lines.append(line)
                except (json.JSONDecodeError, KeyError):
                    continue

    if not all_lines:
        return

    output.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=output.parent, suffix=".tmp")
    try:
        with open(fd, "w") as f:
            for line in all_lines:
                f.write(line + "\n")
        Path(tmp).rename(output)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
