# CLAUDE.md — netanomaly

Operating rules for Claude in this repo. Project state lives in files, not in chat history.

## Start of every session
1. Read `PROJECT_STATUS.md` (current milestone, next task), then `ARCHITECTURE.md`, `SCHEMA.md`, `DECISIONS.md` as relevant.
2. Run `git status` and `git log --oneline -10`.
3. Work on ONE bounded task with an observable definition of done.

## Commands
- `uv run pytest -q` — all tests (must pass before any commit)
- `uv run ruff check src tests` (3 pre-existing ISC004 findings in `schema.py`)
- PoC (active workflow, ADR-024): `uv run netanomaly poc profile|features|train|score|report|experiment|search
  --config <poc.yaml>` — see `poc.example.yaml`; data and `work_dir` via `NETANOMALY_DATA` / `NETANOMALY_WORK`,
  always outside the checkout.
- Legacy lake pipeline (mock/synthetic only): `uv run netanomaly [--root DIR] generate|ingest|features|baselines|
  novelty|timing|feature-cards|train|score|stability|alerts|evaluate|run`; `schema-doc`, `feature-doc` regenerate
  SCHEMA.md / FEATURES.md. Synthetic data under `--root data/synth`; mock data under `data/`.
- Bash on Windows: prefix Python runs with `PYTHONIOENCODING=utf-8`.

## PoC expectations (ADR-024…030)
- Parquet input only, read in place through `input.field_map`; no CSV path, no synthetic generator, injected
  attacks or truth files in `src/netanomaly/poc/` (a test scans for them). Tests use tiny fixtures in `tmp_path`.
- Pentest ranges are weak annotations: never labels, never training positives; no accuracy/precision/recall/FPR.
- Chronological train/validation/test only; anything learned (imputer, scaler, screening, model) sees training
  rows only; bands calibrate on validation. Scores: higher = more anomalous, rankings, not probabilities; bands are
  review bands, `Benign` = below threshold.
- A feature/window/model change is a new experiment id; band changes are revisions of the same experiment.

## Hard rules
- This repo, its data and its models are mock/synthetic, for development and demonstration only (ADR-012).
  Never copy real company data, or artifacts trained or computed on it, into this repository; results here are
  not representative of real data, and models here are never production artifacts. The PoC code is generic and is
  pointed at real Parquet only in its authorised environment; it refuses in-repo inputs/outputs unless the config
  asserts mock/synthetic data (`allow_inside_repo: true`). Do not weaken that guard.
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
