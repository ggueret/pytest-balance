from __future__ import annotations

import pytest

from pytest_balance.ci.detect import detect_ci


class TestDetectCI:
    def test_github_actions(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        monkeypatch.setenv("PYTEST_BALANCE_NODE_INDEX", "2")
        monkeypatch.setenv("PYTEST_BALANCE_NODE_TOTAL", "4")
        monkeypatch.setenv("GITHUB_RUN_ID", "12345")
        monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
        monkeypatch.setenv("GITHUB_REF_NAME", "main")
        ci = detect_ci()
        assert ci is not None
        assert ci.provider == "github"
        assert ci.node_index == 2
        assert ci.node_total == 4
        assert ci.run_id == "12345-1"

    def test_gitlab_ci_normalizes_index(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("GITLAB_CI", "true")
        monkeypatch.setenv("CI_NODE_INDEX", "3")  # 1-based
        monkeypatch.setenv("CI_NODE_TOTAL", "4")
        monkeypatch.setenv("CI_PIPELINE_ID", "789")
        monkeypatch.setenv("CI_COMMIT_REF_NAME", "feat")
        ci = detect_ci()
        assert ci is not None
        assert ci.provider == "gitlab"
        assert ci.node_index == 2  # Normalized to 0-based

    def test_circleci(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("CIRCLECI", "true")
        monkeypatch.setenv("CIRCLE_NODE_INDEX", "0")
        monkeypatch.setenv("CIRCLE_NODE_TOTAL", "3")
        monkeypatch.setenv("CIRCLE_BUILD_NUM", "42")
        ci = detect_ci()
        assert ci is not None
        assert ci.provider == "circleci"
        assert ci.node_index == 0

    def test_azure_devops_normalizes_index(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("TF_BUILD", "True")
        monkeypatch.setenv("SYSTEM_JOBPOSITIONINPHASE", "1")  # Likely 1-based
        monkeypatch.setenv("SYSTEM_TOTALJOBSINPHASE", "2")
        monkeypatch.setenv("BUILD_BUILDID", "99")
        ci = detect_ci()
        assert ci is not None
        assert ci.provider == "azure"
        assert ci.node_index == 0  # Normalized

    def test_buildkite(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("BUILDKITE", "true")
        monkeypatch.setenv("BUILDKITE_PARALLEL_JOB", "1")
        monkeypatch.setenv("BUILDKITE_PARALLEL_JOB_COUNT", "5")
        monkeypatch.setenv("BUILDKITE_BUILD_ID", "abc")
        ci = detect_ci()
        assert ci is not None
        assert ci.provider == "buildkite"
        assert ci.node_index == 1

    def test_generic_env_vars(self, monkeypatch: pytest.MonkeyPatch):
        # Clear all CI provider variables so only the generic ones match
        for var in ["GITHUB_ACTIONS", "GITLAB_CI", "CIRCLECI", "TF_BUILD", "BUILDKITE"]:
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("PYTEST_BALANCE_NODE_INDEX", "1")
        monkeypatch.setenv("PYTEST_BALANCE_NODE_TOTAL", "3")
        ci = detect_ci()
        assert ci is not None
        assert ci.provider == "unknown"
        assert ci.node_index == 1
        assert ci.node_total == 3

    def test_no_ci_returns_none(self, monkeypatch: pytest.MonkeyPatch):
        for var in [
            "GITHUB_ACTIONS",
            "GITLAB_CI",
            "CIRCLECI",
            "TF_BUILD",
            "BUILDKITE",
            "PYTEST_BALANCE_NODE_INDEX",
            "PYTEST_BALANCE_NODE_TOTAL",
        ]:
            monkeypatch.delenv(var, raising=False)
        assert detect_ci() is None

    def test_explicit_overrides(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("GITLAB_CI", "true")
        monkeypatch.setenv("CI_NODE_INDEX", "1")
        monkeypatch.setenv("CI_NODE_TOTAL", "4")
        monkeypatch.setenv("CI_PIPELINE_ID", "789")
        ci = detect_ci(explicit_index=0, explicit_total=8)
        assert ci is not None
        assert ci.node_index == 0
        assert ci.node_total == 8

    def test_node_total_one_is_valid(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PYTEST_BALANCE_NODE_INDEX", "0")
        monkeypatch.setenv("PYTEST_BALANCE_NODE_TOTAL", "1")
        ci = detect_ci()
        assert ci is not None
        assert ci.node_total == 1

    def test_index_out_of_range(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PYTEST_BALANCE_NODE_INDEX", "5")
        monkeypatch.setenv("PYTEST_BALANCE_NODE_TOTAL", "4")
        with pytest.raises(ValueError, match="out of range"):
            detect_ci()

    def test_node_total_zero(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PYTEST_BALANCE_NODE_INDEX", "0")
        monkeypatch.setenv("PYTEST_BALANCE_NODE_TOTAL", "0")
        with pytest.raises(ValueError, match="must be >= 1"):
            detect_ci()
