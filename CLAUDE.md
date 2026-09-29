# CLAUDE.md — netanomaly

Operating rules for Claude in this repo. Project state lives in files, not in chat history.

## Start of every session
1. Read `PROJECT_STATUS.md`, then `README.md`, `ARCHITECTURE.md`, `DECISIONS.md` as relevant.
2. Run `git status` and `git log --oneline -10`.
3. Work on ONE bounded task with an observable definition of done.

## What this is
A proof of concept (ADR-024, ADR-031): rank unusual host x window activity in NetFlow Parquet with Isolation Forest
and One-Class SVM, and report it against user-supplied pentest date ranges. Only this code exists; the earlier lake
pipeline was removed. The owner works with the bundled mock/synthetic data only (no real data will arrive).
The owner is not a developer: keep running it to `uv run netanomaly` + `config.yaml`, keep README steps simple.

## Commands
- `uv run netanomaly` — full run with `config.yaml` (demo data `data/synth/raw`); prints the report.html path
- `uv run netanomaly profile|features|train|score|report|search|docs [--config FILE]`
- `uv run pytest -q` — all tests (must pass before any commit); `uv run ruff check src tests`
- `uv run netanomaly docs` after changing `featureset.FEATURES` or the contract (FEATURES.md / SCHEMA.md are
  generated and drift-tested)

## Hard rules
- Data: keep `data/raw` (mock), `data/synth/raw` and `data/synth/truth` (synthetic). Never add real data or anything
  computed from it (ADR-012); outputs go to `outputs/` (git-ignored). Keep the in-repo guard
  (`allow_inside_repo`) working.
- Parquet input only; no synthetic generator or truth-file dependency in `src/` (tests scan for it).
- Never load full datasets into Python: DuckDB over Parquet, bounded Arrow batches, bounded samples. Every DuckDB
  connection comes from `netanomaly.db.connect()`; quote paths/strings in SQL with `netanomaly.db.sql_literal`.
- Never assume a field's meaning from its name: the mapping is explicit and reported (contract 0/42 validated).
- No temporal leakage: anything learned (imputer, scaler, screening, model) uses training rows only; bands calibrate
  on validation; splits are chronological. Keep the leakage tests.
- Scores are rankings, not probabilities; bands are review bands (`Benign` = below threshold); pentest ranges are
  weak context, never labels; no accuracy/precision/recall/FPR.
- Add a dependency only when it solves a concrete problem.
- Definition of done: implementation + tests pass + `uv run netanomaly` runs + report inspected.

## Git and GitHub safety
You may freely: inspect status/history/diffs, create local branches, create local commits, run tests.
Ask before: pushing, force-pushing, deleting branches, opening/merging PRs, posting comments/issues,
modifying shared GitHub resources, destructive git operations (reset --hard, history rewrites).
Never use `--no-verify`, force push, or resets to bypass a problem.
Commit format: `feat|fix|refactor|docs|test|chore|perf: description`. One conceptual change per commit.

## End of a task
Tests pass → review `git diff` → commit → update `PROJECT_STATUS.md` (and CHANGELOG/DECISIONS/TODO if affected).
