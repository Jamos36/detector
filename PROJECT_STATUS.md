# Project Status

_Last updated: 2026-09-28_

## Current state
The anomaly-ranking proof of concept (ADR-024, ADR-031) now has two explicit workflows (ADR-032) and an optional
source -> destination analysis (ADR-033). `uv run netanomaly` (or `run.bat`) on the bundled synthetic data writes a
development report, a frozen model bundle and a final test report (~40 s).

- Workflow A (historical): development on train 09-01..02 + validation 09-03 (`config.yaml`) ->
  `outputs/experiments/<id>/report.html` (training diagnostics only; the test period is reserved) -> model bundle
  `outputs/bundles/<id>-b<hash>/` -> final test on 09-04..06 with the frozen bundle ->
  `outputs/scoring/holdout-test-.../report.html`; every final test is logged in `outputs/holdout_ledger.json`
  (first / repeat / reused).
- Workflow B (new data): `uv run netanomaly score-new --bundle DIR --input PATH [--history RUN]` -> field contract
  checked, nothing fitted -> `outputs/scoring/new-data-.../`.
- Relationships: `relationship_analysis:` (demo: enabled, report-only, 1-day lookbacks; defaults 7 days). Sparse pair
  x window table, never-seen / recently-unseen / frequency change from strictly earlier windows, carried history state
  in the bundle; `include_model_features` adds six `rel_*` model inputs (off).
- Data kept: `data/raw` (mock), `data/synth/raw` + `data/synth/truth` (synthetic). Real data (about a year) may
  arrive and must stay outside the repository: `config.real.example.yaml`; input from outside the checkout cannot
  write inside it.
- Tests: 65 passing (`uv run pytest -q`, ~1.5 min). Lint clean.
- Demo final test (synthetic, self-designed injections on 09-04..06): the injected scans and exfiltration are the top
  review candidates of both models, and the relationship section lists their targets as never-seen destinations.
  A pipeline check, not evidence of detection quality on real traffic.
- Scale: not validated on real data. A synthetic replay (demo days x 26 = 10.1 M flows, 156 files, 156 days, only
  300 hosts) ran end to end in ~8 min (memory_gb 4, 4 threads); peak memory was not measured. See README "Real data".

## Known limitations
- Isolation Forest cannot rank values far beyond the training range as extreme; see `beyond_train_range`.
- Model features are window-local unless `rel_*` summaries are included: periodic (beaconing) and slow activity
  rank low.
- Relationship analysis evaluates active pair-windows only: a pair that goes silent is not flagged. "Never seen"
  means since the history start. Port/protocol grouping multiplies the pair count.
- Scoring data that starts before the carried relationship history ends: refused when `rel_*` are model inputs,
  relationship section skipped (with a warning) otherwise.
- Field meanings are unvalidated (SCHEMA.md, 0/42).

## Next task
See TODO.md (first item: run a real year outside the repository and record runtime, memory and pair counts).
