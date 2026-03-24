"""Post-run balance report."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class NodeReport:
    node_id: str
    test_count: int
    estimated_time: float
    actual_time: float

    @property
    def deviation(self) -> float:
        if self.estimated_time == 0:
            return 0.0
        return (self.actual_time - self.estimated_time) / self.estimated_time


@dataclass(frozen=True, slots=True)
class BalanceReport:
    nodes: list[NodeReport]
    worst_predictions: list[tuple[str, float, float]]

    @property
    def balance_ratio(self) -> float:
        if not self.nodes:
            return 1.0
        times = [n.actual_time for n in self.nodes]
        total = sum(times)
        worst = max(times)
        if worst == 0:
            return 1.0
        ideal = total / len(times)
        return ideal / worst

    def format(self) -> str:
        lines = [
            "========================= balance report ==========================",
        ]
        for node in self.nodes:
            pct = (1.0 + node.deviation) * 100
            sign = "+" if node.deviation >= 0 else ""
            flag = " !!" if abs(node.deviation) > 0.2 else ""
            lines.append(
                f"{node.node_id}: {node.test_count} tests | "
                f"{node.estimated_time:.1f}s est. | "
                f"{node.actual_time:.1f}s actual | "
                f"{sign}{node.deviation * node.estimated_time:.1f}s ({pct:.0f}%){flag}"
            )
        if self.nodes:
            times = [n.actual_time for n in self.nodes]
            wall = max(times)
            ideal = sum(times) / len(times)
            lines.append(
                f"\nBalance: {self.balance_ratio * 100:.1f}% (perfect: 100%) | "
                f"Wall time: {wall:.1f}s | Ideal: {ideal:.1f}s"
            )
        for test_id, est, actual in self.worst_predictions[:3]:
            lines.append(f"Worst prediction: {test_id} (est. {est:.1f}s -> {actual:.1f}s)")
        lines.append(
            "==================================================================="
        )
        return "\n".join(lines)
