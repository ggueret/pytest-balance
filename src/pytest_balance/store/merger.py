"""Merge partial JSONL duration files from split CI runs."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path


def merge_files(inputs: list[Path], output: Path) -> None:
    if not inputs:
        raise ValueError("No input files provided")

    seen: set[tuple[str, str]] = set()
    all_lines: list[str] = []

    sources = []
    if output.exists():
        sources.append(output)
    sources.extend(inputs)

    for path in sources:
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                key = (data["test_id"], data.get("run_id", ""))
                if key not in seen:
                    seen.add(key)
                    all_lines.append(line)
            except (json.JSONDecodeError, KeyError):
                continue

    if not all_lines:
        return

    output.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=output.parent, suffix=".tmp")
    try:
        with open(fd, "w") as f:
            f.write("\n".join(all_lines) + "\n")
        Path(tmp).rename(output)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
