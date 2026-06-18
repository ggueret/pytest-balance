"""Human-readable duration formatting with an adaptive unit."""

from __future__ import annotations


def format_duration(seconds: float) -> str:
    """Render a duration with an adaptive unit (µs / ms / s).

    Keeps sub-millisecond values legible instead of collapsing them to
    ``0.000s`` under a fixed ``:.3f`` format. Full precision stays in the
    JSONL store; only the display adapts.
    """
    if seconds < 1e-3:
        return f"{seconds * 1e6:.0f}µs"
    if seconds < 1.0:
        return f"{seconds * 1e3:.0f}ms"
    return f"{seconds:.1f}s"
