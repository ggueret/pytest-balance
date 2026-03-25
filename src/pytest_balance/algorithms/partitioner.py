"""Scope-aware test grouping for balanced partitioning."""

from __future__ import annotations

import enum
from collections import OrderedDict
from dataclasses import dataclass, field


class Scope(enum.Enum):
    TEST = "test"
    CLASS = "class"
    MODULE = "module"
    GROUP = "group"


@dataclass
class TestGroup:
    scope_id: str
    test_ids: list[str] = field(default_factory=list)
    estimated_duration: float = 0.0


def extract_scope(test_id: str, scope: Scope) -> str:
    """Extract the scope key from a pytest node ID."""
    if scope == Scope.TEST:
        return test_id

    if scope == Scope.GROUP:
        bracket_pos = test_id.rfind("]")
        at_pos = test_id.rfind("@")
        if at_pos > bracket_pos:
            return test_id[at_pos + 1 :]
        return test_id

    parts = test_id.split("::")
    file_part = parts[0] if parts else test_id

    if scope == Scope.MODULE:
        return file_part

    if scope == Scope.CLASS:
        if len(parts) >= 3:
            return f"{parts[0]}::{parts[1]}"
        return test_id

    return test_id


def group_by_scope(test_ids: list[str], scope: Scope) -> list[TestGroup]:
    """Group test IDs by scope, preserving order within groups."""
    if not test_ids:
        return []

    groups: OrderedDict[str, TestGroup] = OrderedDict()
    for test_id in test_ids:
        scope_key = extract_scope(test_id, scope)
        if scope_key not in groups:
            groups[scope_key] = TestGroup(scope_id=scope_key)
        groups[scope_key].test_ids.append(test_id)

    return list(groups.values())
