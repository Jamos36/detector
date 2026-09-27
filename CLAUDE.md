# CLAUDE.md — netanomaly

Operating rules for Claude in this repo. Project state lives in files, not in chat history.

## Start of every session
1. Read `PROJECT_STATUS.md` (current milestone, next task), then `ARCHITECTURE.md`, `SCHEMA.md`, `DECISIONS.md` as relevant.
2. Run `git status` and `git log --oneline -10`.
3. Work on ONE bounded task with an observable definition of done.

## Commands
- `uv run pytest -q` — all tests (must pass before any commit)
- `uv run ruff check src tests`
- `uv run netanomaly [--root DIR] generate|ingest|features|baselines|novelty|train|score|alerts|evaluate|run|schema-doc|feature-doc`
- Synthetic data lives under `--root data/synth`; mock data under `data/`.
- Bash on Windows: prefix Python runs with `PYTHONIOENCODING=utf-8`.

## Hard rules
- This repo, its data and its models are mock/synthetic, for development and demonstration only (ADR-012).
  Never copy real company data, or artifacts trained or computed on it, into this repository; results here are
  not representative of real data, and models here are never production artifacts.
- Never load full datasets into pandas/Python. DuckDB over Parquet, or bounded Arrow batches.
- Every DuckDB connection comes from `netanomaly.db.connect()` (UTC, memory limit, spill dir).
  Quote every path/string interpolated into SQL with `netanomaly.db.sql_literal`.
- Never assume a field's meaning from its name. The contract
  (`src/netanomaly/contracts/netflow_v1.yaml`) is the source of truth; `validated: true` only
  with collector documentation. Regenerate `SCHEMA.md` with `schema-doc` after contract edits,
  and `FEATURES.md` with `feature-doc` after contract or feature-registry edits.
- No temporal leakage: baselines, normalizers and novelty features at time t use only data before t.
  Train/evaluate splits are by time. Add a test that future rows cannot change earlier values.
- Scores are rankings, not probabilities. Say "anomalous behavior consistent with…";
  ATT&CK mappings are hypotheses. Thresholds are alert budgets (top-K/day), not severities.
- Add a dependency only when it solves a concrete problem in the current version.
- Definition of done: implementation + tests pass + run on sample data + output inspected + edge cases considered.

## Git and GitHub safety
You may freely: inspect status/history/diffs, create local branches, create local commits, run tests.
Ask before: pushing, force-pushing, deleting branches, opening/merging PRs, posting comments/issues,
modifying shared GitHub resources, destructive git operations (reset --hard, history rewrites).
Never use `--no-verify`, force push, or resets to bypass a problem.
Commit format: `feat|fix|refactor|docs|test|chore|perf: description`. One conceptual change per commit.
The project uses mock/synthetic data only and `data/` is tracked by design (DECISIONS.md ADR-009).

## End of a task
Tests pass → review `git diff` → commit → update `PROJECT_STATUS.md` (and CHANGELOG/DECISIONS/TODO if affected).
