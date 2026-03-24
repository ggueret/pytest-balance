"""CI environment auto-detection."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CIContext:
    provider: str
    node_index: int
    node_total: int
    run_id: str
    branch: str | None = None


def detect_ci(
    explicit_index: int | None = None,
    explicit_total: int | None = None,
) -> CIContext | None:
    ctx = _detect_provider()

    if explicit_index is not None and explicit_total is not None:
        if ctx is not None:
            ctx = CIContext(
                provider=ctx.provider,
                node_index=explicit_index,
                node_total=explicit_total,
                run_id=ctx.run_id,
                branch=ctx.branch,
            )
        else:
            ctx = CIContext(
                provider="explicit",
                node_index=explicit_index,
                node_total=explicit_total,
                run_id="local",
            )

    if ctx is None:
        return None

    _validate(ctx)
    return ctx


def _detect_provider() -> CIContext | None:
    for detector in [
        _detect_github, _detect_gitlab, _detect_circleci,
        _detect_azure, _detect_buildkite, _detect_generic,
    ]:
        ctx = detector()
        if ctx is not None:
            return ctx
    return None


def _detect_github() -> CIContext | None:
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return None
    index = os.environ.get("PYTEST_BALANCE_NODE_INDEX")
    total = os.environ.get("PYTEST_BALANCE_NODE_TOTAL")
    if index is None or total is None:
        return None
    run_id = os.environ.get("GITHUB_RUN_ID", "unknown")
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    branch = os.environ.get("GITHUB_REF_NAME")
    return CIContext("github", int(index), int(total), f"{run_id}-{attempt}", branch)


def _detect_gitlab() -> CIContext | None:
    if os.environ.get("GITLAB_CI") != "true":
        return None
    index = os.environ.get("CI_NODE_INDEX")
    total = os.environ.get("CI_NODE_TOTAL")
    if index is None or total is None:
        return None
    return CIContext("gitlab", int(index) - 1, int(total),
                     os.environ.get("CI_PIPELINE_ID", "unknown"),
                     os.environ.get("CI_COMMIT_REF_NAME"))


def _detect_circleci() -> CIContext | None:
    if os.environ.get("CIRCLECI") != "true":
        return None
    index = os.environ.get("CIRCLE_NODE_INDEX")
    total = os.environ.get("CIRCLE_NODE_TOTAL")
    if index is None or total is None:
        return None
    return CIContext("circleci", int(index), int(total),
                     os.environ.get("CIRCLE_BUILD_NUM", "unknown"),
                     os.environ.get("CIRCLE_BRANCH"))


def _detect_azure() -> CIContext | None:
    if os.environ.get("TF_BUILD") != "True":
        return None
    index = os.environ.get("SYSTEM_JOBPOSITIONINPHASE")
    total = os.environ.get("SYSTEM_TOTALJOBSINPHASE")
    if index is None or total is None:
        return None
    return CIContext("azure", int(index) - 1, int(total),
                     os.environ.get("BUILD_BUILDID", "unknown"),
                     os.environ.get("BUILD_SOURCEBRANCH"))


def _detect_buildkite() -> CIContext | None:
    if os.environ.get("BUILDKITE") != "true":
        return None
    index = os.environ.get("BUILDKITE_PARALLEL_JOB")
    total = os.environ.get("BUILDKITE_PARALLEL_JOB_COUNT")
    if index is None or total is None:
        return None
    return CIContext("buildkite", int(index), int(total),
                     os.environ.get("BUILDKITE_BUILD_ID", "unknown"),
                     os.environ.get("BUILDKITE_BRANCH"))


def _detect_generic() -> CIContext | None:
    index = os.environ.get("PYTEST_BALANCE_NODE_INDEX")
    total = os.environ.get("PYTEST_BALANCE_NODE_TOTAL")
    if index is None or total is None:
        return None
    return CIContext("unknown", int(index), int(total), "generic")


def _validate(ctx: CIContext) -> None:
    if ctx.node_total < 1:
        raise ValueError(f"node_total must be >= 1, got {ctx.node_total}")
    if not (0 <= ctx.node_index < ctx.node_total):
        raise ValueError(f"node_index {ctx.node_index} out of range for {ctx.node_total} nodes")
