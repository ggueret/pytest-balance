from __future__ import annotations

import json
from pathlib import Path


pytest_plugins = ["pytester"]


class TestPluginCISplit:
    def test_balance_splits_tests(self, pytester):
        pytester.makepyfile(
            test_a="def test_1(): pass\ndef test_2(): pass",
            test_b="def test_3(): pass\ndef test_4(): pass",
        )
        balance_dir = pytester.path / ".balance"
        balance_dir.mkdir()
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
