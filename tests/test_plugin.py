from __future__ import annotations

import json

pytest_plugins = ["pytester"]


def _seed_balance_store(pytester) -> None:
    """Create a .balance/durations.jsonl with test data in the pytester directory."""
    balance_dir = pytester.path / ".balance"
    balance_dir.mkdir(exist_ok=True)
    store = balance_dir / "durations.jsonl"
    lines = []
    for name, dur in [
        ("test_a.py::test_1", 1),
        ("test_a.py::test_2", 1),
        ("test_b.py::test_3", 1),
        ("test_b.py::test_4", 1),
    ]:
        lines.append(
            json.dumps(
                {
                    "test_id": name,
                    "duration": dur,
                    "timestamp": "2026-01-01T00:00:00+00:00",
                    "run_id": "r1",
                    "worker": "w0",
                    "phase": "call",
                }
            )
        )
    store.write_text("\n".join(lines) + "\n")


class TestPluginCISplit:
    def test_balance_splits_tests(self, pytester):
        pytester.makepyfile(
            test_a="def test_1(): pass\ndef test_2(): pass",
            test_b="def test_3(): pass\ndef test_4(): pass",
        )
        _seed_balance_store(pytester)

        result = pytester.runpytest(
            "--balance",
            "--balance-node-index=0",
            "--balance-node-total=2",
        )
        result.assert_outcomes(passed=2)

    def test_balance_without_data_fallback(self, pytester):
        pytester.makepyfile(test_a="def test_1(): pass\ndef test_2(): pass")
        result = pytester.runpytest(
            "--balance",
            "--balance-node-index=0",
            "--balance-node-total=2",
        )
        outcomes = result.parseoutcomes()
        assert outcomes.get("passed", 0) > 0

    def test_no_balance_flag_runs_all(self, pytester):
        pytester.makepyfile(test_a="def test_1(): pass\ndef test_2(): pass")
        result = pytester.runpytest()
        result.assert_outcomes(passed=2)


class TestPluginStore:
    def test_balance_store_writes_jsonl(self, pytester):
        pytester.makepyfile(test_a="def test_1(): pass")
        result = pytester.runpytest("--balance-store")
        result.assert_outcomes(passed=1)
        partials = list(pytester.path.glob(".balance/durations*.jsonl"))
        assert len(partials) >= 1
        data = json.loads(partials[0].read_text().strip().split("\n")[0])
        assert data["test_id"] == "test_a.py::test_1"
        assert data["duration"] > 0

    def test_balance_store_partial_file_in_ci(self, pytester):
        """With explicit node index/total, store writes a partial file."""
        pytester.makepyfile(test_a="def test_1(): pass")
        result = pytester.runpytest(
            "--balance-store",
            "--balance-node-index=0",
            "--balance-node-total=2",
        )
        result.assert_outcomes(passed=1)
        partials = list(pytester.path.glob(".balance/durations-*-0.jsonl"))
        assert len(partials) == 1
        data = json.loads(partials[0].read_text().strip().split("\n")[0])
        assert data["test_id"] == "test_a.py::test_1"


class TestPluginPlan:
    def test_balance_plan_shows_plan(self, pytester):
        """--balance-plan displays the plan and exits without running tests."""
        pytester.makepyfile(
            test_a="def test_1(): pass\ndef test_2(): pass",
            test_b="def test_3(): pass\ndef test_4(): pass",
        )
        _seed_balance_store(pytester)

        result = pytester.runpytest(
            "--balance",
            "--balance-plan",
            "--balance-node-total=2",
        )
        result.stdout.fnmatch_lines(["*Node 0*", "*Node 1*"])
        # No tests should have been collected as outcomes
        outcomes = result.parseoutcomes()
        assert outcomes.get("passed", 0) == 0
        assert outcomes.get("failed", 0) == 0

    def test_balance_plan_without_ci(self, pytester):
        """--balance-plan without node info defaults to 2 nodes."""
        pytester.makepyfile(test_a="def test_1(): pass\ndef test_2(): pass")
        _seed_balance_store(pytester)

        result = pytester.runpytest(
            "--balance",
            "--balance-plan",
        )
        result.stdout.fnmatch_lines(["*Node 0*", "*Node 1*"])


class TestPluginReport:
    def test_balance_report_shown(self, pytester):
        """Balance report is displayed by default when --balance is active."""
        pytester.makepyfile(
            test_a="def test_1(): pass\ndef test_2(): pass",
        )
        _seed_balance_store(pytester)

        result = pytester.runpytest(
            "--balance",
            "--balance-node-index=0",
            "--balance-node-total=1",
        )
        result.stdout.fnmatch_lines(["*balance report*"])

    def test_balance_no_report_flag(self, pytester):
        """--balance-no-report suppresses the balance report."""
        pytester.makepyfile(
            test_a="def test_1(): pass\ndef test_2(): pass",
        )
        _seed_balance_store(pytester)

        result = pytester.runpytest(
            "--balance",
            "--balance-no-report",
            "--balance-node-index=0",
            "--balance-node-total=1",
        )
        assert "balance report" not in result.stdout.str()
