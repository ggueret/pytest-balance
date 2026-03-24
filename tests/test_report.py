from __future__ import annotations

from pytest_balance.report import BalanceReport, NodeReport


class TestBalanceReport:
    def test_perfect_balance(self):
        nodes = [
            NodeReport("node-0", 10, 5.0, 5.0),
            NodeReport("node-1", 10, 5.0, 5.0),
        ]
        report = BalanceReport(nodes=nodes, worst_predictions=[])
        assert report.balance_ratio == 1.0

    def test_imbalanced(self):
        nodes = [
            NodeReport("node-0", 10, 5.0, 10.0),
            NodeReport("node-1", 10, 5.0, 2.0),
        ]
        report = BalanceReport(nodes=nodes, worst_predictions=[])
        assert report.balance_ratio == 0.6

    def test_node_deviation(self):
        node = NodeReport("n", 10, 5.0, 7.5)
        assert node.deviation == 0.5

    def test_format_report(self):
        nodes = [
            NodeReport("node-0", 5, 3.0, 3.2),
            NodeReport("node-1", 5, 3.0, 2.8),
        ]
        report = BalanceReport(nodes=nodes, worst_predictions=[])
        text = report.format()
        assert "balance report" in text.lower()
        assert "node-0" in text or "Node 0" in text

    def test_empty_report(self):
        report = BalanceReport(nodes=[], worst_predictions=[])
        assert report.balance_ratio == 1.0
