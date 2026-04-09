# pytest-balance

[![PyPI](https://img.shields.io/pypi/v/pytest-balance?include_prereleases)](https://pypi.org/project/pytest-balance/)
[![Python](https://img.shields.io/pypi/pyversions/pytest-balance?include_prereleases)](https://pypi.org/project/pytest-balance/)
[![License](https://img.shields.io/github/license/ggueret/pytest-balance)](https://github.com/ggueret/pytest-balance/blob/main/LICENSE)

Intelligent test distribution for pytest. Split your test suite across CI runners and
xdist workers based on actual execution times, not file count.

Most CI parallelism strategies split tests naively: round-robin, alphabetical, or by file
count. The result is predictable. One runner finishes in 2 minutes, another grinds for
12, and your pipeline is only as fast as the slowest shard.

**pytest-balance fixes this.** It records test durations, learns from them, and uses a
scheduling algorithm with real guarantees to spread the load evenly.

### What makes it different

- **LPT scheduling.** The Longest Processing Time First algorithm assigns the heaviest
  test groups first and greedily fills the lightest bucket. This minimizes your total wall
  time with a proven worst-case bound of 4/3 optimal.
- **Deterministic partitioning.** Given the same duration data and the same test
  collection, every CI run produces the exact same split. No flaky ordering, no
  cache-busting surprises, no "works on my shard" mysteries. Ties are broken
  lexicographically, so the output is reproducible down to the test.
- **Scope-aware grouping.** Tests that share module or class fixtures stay together,
  avoiding expensive teardown/setup cycles across nodes.
- **Work-stealing.** When used with pytest-xdist, idle workers steal complete test groups
  from the busiest worker at runtime. Static estimates are never perfect;
  work-stealing closes the gap.
- **Adaptive estimation.** An exponential moving average (EMA) tracks duration trends
  over time, so a test that got slower last week weighs more than one that was slow six
  months ago.

## Installation

```bash
pip install pytest-balance

# With pytest-xdist support
pip install pytest-balance[xdist]
```

## Quick Start

**Step 1: record durations** on your first run (or in a baseline pipeline step):

```bash
pytest --balance-store
```

This writes `.balance/durations.jsonl` (locally) or `.balance/durations-<run_id>-<node>.jsonl`
(in CI). After a parallel CI run, merge the partial files:

```bash
pytest-balance merge
```

**Step 2: distribute tests** using the recorded data:

```bash
pytest --balance
```

In CI, the plugin auto-detects the node index and total from the environment and runs only
the slice assigned to the current node.

## CI Integration

### GitHub Actions

GitHub Actions does not expose parallel job indices natively. Pass them from the matrix:

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        shard: [0, 1, 2, 3]
    env:
      PYTEST_BALANCE_NODE_INDEX: ${{ matrix.shard }}
      PYTEST_BALANCE_NODE_TOTAL: 4
    steps:
      - uses: actions/checkout@v4
      - run: pip install pytest-balance
      - run: pytest --balance --balance-store
      - uses: actions/upload-artifact@v4
        with:
          name: durations-${{ matrix.shard }}
          path: .balance/durations-*.jsonl

  merge-durations:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v4
      - run: pip install pytest-balance
      - run: pytest-balance merge durations-*/durations-*.jsonl -o .balance/durations.jsonl
      - uses: actions/upload-artifact@v4
        with:
          name: balance-store
          path: .balance/durations.jsonl
```

### GitLab CI

GitLab's `parallel:` keyword sets `CI_NODE_INDEX` (1-based) and `CI_NODE_TOTAL`
automatically. The plugin converts the 1-based index to 0-based internally.

```yaml
test:
  image: python:3.14
  parallel: 4
  script:
    - pip install pytest-balance
    - pytest --balance --balance-store
  artifacts:
    paths:
      - .balance/durations-*.jsonl
    expire_in: 7 days

merge-durations:
  image: python:3.14
  stage: .post
  needs: [test]
  script:
    - pip install pytest-balance
    - pytest-balance merge .balance/durations-*.jsonl -o .balance/durations.jsonl
  artifacts:
    paths:
      - .balance/durations.jsonl
    expire_in: 30 days
```

### CircleCI

CircleCI's `parallelism:` sets `CIRCLE_NODE_INDEX` (0-based) and `CIRCLE_NODE_TOTAL`
automatically.

```yaml
jobs:
  test:
    docker:
      - image: cimg/python:3.14
    parallelism: 4
    steps:
      - checkout
      - run: pip install pytest-balance
      - run: pytest --balance --balance-store
      - store_artifacts:
          path: .balance/
```

### Azure DevOps

Azure Pipelines sets `SYSTEM_JOBPOSITIONINPHASE` (1-based) and `SYSTEM_TOTALJOBSINPHASE`
when using a matrix or parallel strategy. The plugin converts to 0-based internally.

### Buildkite

Buildkite sets `BUILDKITE_PARALLEL_JOB` (0-based) and `BUILDKITE_PARALLEL_JOB_COUNT`
when `parallelism:` is configured in the pipeline.

### Generic / other CI

Set `PYTEST_BALANCE_NODE_INDEX` and `PYTEST_BALANCE_NODE_TOTAL` manually on any CI system
that does not have native parallelism variables.

## xdist Integration

When `pytest-balance[xdist]` is installed, passing `--balance` alongside `-n` activates
the `BalanceScheduler` instead of the default xdist load scheduler:

```bash
pytest -n 4 --balance
```

The scheduler uses LPT pre-assignment (see How It Works) and falls back to work-stealing
at runtime when workers finish early. `--dist each` is incompatible with `--balance`.

## CLI Options

All options are available as pytest command-line flags:

| Flag | Default | Description |
|---|---|---|
| `--balance` | off | Enable balanced test distribution across CI nodes |
| `--balance-store` | off | Record test durations to the balance store |
| `--balance-scope` | `module` | Grouping scope: `test`, `class`, `module`, `group` |
| `--balance-path` | `.balance/` | Path to the balance store directory |
| `--balance-plan` | off | Show the distribution plan without running tests (requires `--balance`) |
| `--balance-node-index` | auto | Explicit node index (overrides CI auto-detection) |
| `--balance-node-total` | auto | Explicit total node count (overrides CI auto-detection) |
| `--balance-estimator` | `ema` | Duration estimation strategy: `ema`, `median`, `last` |
| `--balance-no-report` | off | Suppress the balance summary after the test run |

## Standalone CLI

The `pytest-balance` command manages the duration store outside of a test run.

```
pytest-balance [--path PATH] <command> [options]
```

### merge

Merge per-node partial files into a single `durations.jsonl`:

```bash
pytest-balance merge
pytest-balance merge .balance/durations-abc-0.jsonl .balance/durations-abc-1.jsonl
pytest-balance merge -o custom/path/durations.jsonl
```

After merging, the partial files are deleted automatically.

### prune

Remove old run data, keeping only the most recent runs per test:

```bash
pytest-balance prune
pytest-balance prune --keep-runs 20
```

Default is 50 runs. Entries without a `run_id` are always kept.

### stats

Display a summary of the duration store:

```bash
pytest-balance stats
pytest-balance stats --json
```

Output includes total tests, total and average estimated time, and the slowest and fastest
tests.

### plan

Preview how tests would be distributed for a given node count:

```bash
pytest-balance plan 4
pytest-balance plan 4 --scope class --estimator median --json
```

## Duration Store

Durations are stored as JSONL (one JSON object per line) in `.balance/durations.jsonl`.
Each line records a single test result:

```json
{"test_id":"tests/test_api.py::test_login","duration":0.42,"timestamp":"2024-01-15T10:30:00+00:00","run_id":"12345-1","worker":"node0","phase":"call"}
```

**In CI:** each parallel node writes to a separate partial file
(`durations-<run_id>-<node_index>.jsonl`) to avoid write conflicts. Run
`pytest-balance merge` after all nodes finish to consolidate them.

**Locally:** durations are appended directly to `durations.jsonl`.

Commit `durations.jsonl` to version control so all branches and CI runs share the same
history.

## Scope

The `--balance-scope` option controls how tests are grouped before partitioning:

| Scope | Grouping | When to use |
|---|---|---|
| `test` | Each test is its own unit | Tests are fully independent and durations vary widely |
| `class` | All tests in a class are kept together | Tests share class-level fixtures |
| `module` | All tests in a file are kept together (default) | Tests share module-level fixtures |
| `group` | Tests tagged with `@<group>` in their node ID | Custom grouping via markers |

Keeping related tests together avoids fixture teardown/setup overhead between nodes.
The xdist work-stealing also respects scope boundaries, stealing complete groups rather
than splitting them.

## How It Works

**CI-level splitting (--balance):**

1. On collection, the plugin reads duration estimates from the store.
2. Tests are grouped by the configured scope.
3. The Longest Processing Time First (LPT) algorithm assigns groups to nodes: sort groups
   by descending estimated duration, then greedily assign each group to the node with the
   currently lowest total load.
4. Only the slice for the current node index runs; the rest are deselected.

**xdist scheduling (--balance with -n):**

1. After all workers collect, the same LPT algorithm pre-assigns groups to workers and
   sends each worker its initial batch.
2. As workers finish, idle workers steal complete scope groups from the busiest worker.
   With `--balance-scope test`, individual tests can be stolen instead of groups.
3. Workers with no remaining work are shut down so the run ends as soon as all tests
   complete.

**Estimation strategies:**

- `ema` (default): exponential moving average (alpha=0.3) over the recorded history,
  giving more weight to recent runs.
- `median`: statistical median over all recorded durations.
- `last`: the single most recent recorded duration.

Unknown tests (not in the store) fall back to the median estimated duration of all known
tests.

## Status

Alpha. The API and file format may change between releases.
