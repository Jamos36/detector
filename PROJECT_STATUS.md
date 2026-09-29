# Project Status

_Last updated: 2026-09-28_

## Current state
The anomaly-ranking proof of concept is the whole project (ADR-024, ADR-031). It runs with one command
(`uv run netanomaly`, or `run.bat`) on the bundled synthetic data and writes `outputs/experiments/<id>/report.html`.
The earlier V0–V3 lake pipeline (CSV ingest, synthetic generator, legacy features/models) and its generated
artifacts were removed; they remain in git history (last commit containing them: `78f2d31`).

- Data kept: `data/raw` (mock: 6 days, 60,000 flows, ~58,000 source IPs, CSV + Parquet), `data/synth/raw`
  (synthetic: 6 days, 388,749 flows, 300 hosts) and `data/synth/truth` (the injected attacks, 2026-09-04..06).
  No real data will be provided; the code still refuses in-repo data unless `allow_inside_repo: true`.
- Pipeline: profile → host x 15-min features (12) → chronological split (train 09-01..02, validation 09-03,
  test 09-04..06 in `config.yaml`) → Isolation Forest + One-Class SVM → review bands from validation quantiles →
  Parquet outputs, diagnostics, Markdown + HTML report with 10 charts.
- Memory: one knob, `memory_gb` (default 4), derives DuckDB's limit, training-sample sizes, matrix guard and batch
  size (README "Change the memory").
- Tests: 43 passing (`uv run pytest -q`, ~30 s). Lint clean.
- Default run on the synthetic demo (~30 s), checked afterwards against `data/synth/truth/injections.csv` (best
  rank of a window overlapping each injection, among review-band windows ordered by that model's score):
  One-Class SVM ranks all 15 injections within its top 37 (the 3 vertical scans at 1-3). Isolation Forest does
  much worse: best 22 (exfil) and 25 (vertical scan), but 7 of 15 below rank 400 and the 3 beacons around 3,400 —
  consistent with its inability to extrapolate beyond a 2-day training range. These are self-designed synthetic
  attacks: a pipeline check, not evidence of detection quality on real traffic.

## Known limitations
- Isolation Forest cannot rank values far beyond the training range as extreme (test-pinned); see
  `beyond_train_range` and RESEARCH.md.
- Features are window-local: periodic (beaconing) and slow activity rank low.
- The mock dataset (`data/raw`) has almost no repeated hosts, so per-host models learn little from it.
- Field meanings are unvalidated (SCHEMA.md, 0/42).

## Next task
See TODO.md (first item: incident view that merges adjacent windows of a host).
