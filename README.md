# netanomaly

A proof of concept for exploring about a year of NetFlow **Parquet** and ranking unusual hosts and periods, to see
whether known penetration-test date ranges stand out. Two unsupervised baselines (Isolation Forest, One-Class SVM)
learn from a chosen baseline period and rank later windows; a Markdown + SVG report shows where the high-ranked
windows fall in time, on which hosts, and relative to the supplied pentest ranges.

It is an experiment workflow, not a production detector. Scores are **rankings**, not probabilities; the review
bands (Critical / High / Medium / Low / Benign) are alert-volume bands, not severities; pentest ranges are **weak
annotations**, never labels. There are no reliable per-flow labels, so nothing here measures detection accuracy.

## Data boundary (read first)

This checkout holds only mock/synthetic data and must never hold real company data or anything computed from it
(DECISIONS.md ADR-012). The code is generic: run it in the authorised environment, point it at Parquet there, and
write outputs there. The PoC refuses inputs, outputs or its spill directory inside the checkout unless the config
sets `allow_inside_repo: true`, which asserts the data is mock/synthetic.

## Quick start (PoC)

```bash
uv sync
cp poc.example.yaml /path/outside/repo/poc.yaml       # edit split dates, annotations, field_map
export NETANOMALY_DATA=/secure/netflow/parquet          # directory or glob with *.parquet (never in this repo)
export NETANOMALY_WORK=/secure/netanomaly-work          # features, models, scores, reports

uv run netanomaly poc profile    --config /path/outside/repo/poc.yaml   # schema, mapping, nulls, span, cardinality
uv run netanomaly poc experiment --config /path/outside/repo/poc.yaml   # features -> train -> score -> report
```

Stages can also be run one by one: `features`, `train`, `score` (also runs the seed/contamination robustness fits),
`report` (bands, tables, diagnostics, charts). Re-band without retraining:
`uv run netanomaly poc report --config poc.yaml --bands bands.yaml` (a YAML with the `bands:` settings).
Compare parameter candidates on the validation period: `uv run netanomaly poc search --config poc.yaml`.
On Windows bash, prefix commands with `PYTHONIOENCODING=utf-8`.

Everything is also a Python API (`netanomaly.poc.experiment`: `open_context`, `train_models`, `score_models`,
`robustness`, `finalize`, `run_search`), so features, windows, parameters and cutoffs can be changed in a notebook.

## Workflow

| stage | what it does | where |
|---|---|---|
| profile | files, row counts, union schema, field mapping (source -> canonical), null/invalid rates, UTC span, rows per day, approximate cardinalities and feature-row estimate — all DuckDB aggregates | `work_dir/profile/` |
| features | host (`src_ip`) x `window_minutes` rows with 12 window-local features (FEATURES.md); cached by input fingerprint + mapping + window | `work_dir/features/<key>/` |
| train | chronological train / validation / test periods; imputer + scaler + model fitted on a bounded training sample only | `experiments/<id>/models/` |
| score | every window scored in Arrow batches, `raw = -score_samples` (higher = more anomalous) | `scores_raw.parquet` |
| report | bands from validation quantiles (or a daily budget), outputs, diagnostics, charts | see below |

Outputs in `work_dir/experiments/<experiment_id>/`: `report.md` + `charts/`, `scores.parquet` (every window: period,
annotation category, features, `<model>_raw/_pct/_band`, `beyond_train_range`), `alerts.parquet` (review-band
windows with host baseline context, largest deviations, and source-file/row traces for the top ones),
`daily_summary.parquet`, `entity_summary.parquet`, `diagnostics.json`, `robustness.json`, `manifest.json` (inputs,
mapping, periods, features, parameters, seeds, cutoffs and their revisions, versions, code commit).

The experiment id hashes the input fingerprint, mapping, window, features, split and model settings, so any of
those changes creates a new experiment; changing bands does not.

## What the report shows

Year overview with pentest ranges shaded (buffers hatched) · zoomed views of the highest-ranked periods · review
bands over time and alert volume vs cutoff · score distributions per period with cutoffs · hosts x time heatmaps ·
ranked candidates with evidence and traces · Isolation Forest vs One-Class SVM agreement and disagreements · overlap
with the supplied ranges and its sensitivity to the buffer · seed stability, contamination variants and training
concentration · feature drift · field mapping, data quality and assumptions.

## Honest limitations

- No labels: overlap with pentest ranges is descriptive, not a detection rate; ranges contain clean traffic.
- A high rank means unusual relative to the chosen baseline; a baseline that contains attacks can hide them.
- Isolation Forest cannot extrapolate beyond its training range; see `beyond_train_range` and RESEARCH.md.
- Features are window-local: slow, low-volume and periodic activity may rank low.
- Field meanings are unvalidated against exporter documentation (SCHEMA.md, 0/42 validated).
- Scale: aggregation and scoring are bounded by DuckDB's memory limit and `batch_rows`; the model training sample is
  bounded (`max_train_rows`, `max_matrix_mb`). Not yet measured on a real year of flows.

Design and decisions: ARCHITECTURE.md, DECISIONS.md (ADR-024…030 for the PoC), RESEARCH.md (paper influence),
PROJECT_STATUS.md (current state), TODO.md.

## Legacy lake pipeline (V0–V3)

The original pipeline — CSV/Parquet ingest into a partitioned lake, data-quality reports, a synthetic generator with
injected attacks, prior-history features, feature cards and a single Isolation Forest — still works and is tested,
but is not part of the PoC (ADR-024). It operates on the committed mock/synthetic data only:

```bash
uv run netanomaly run                                   # data/raw -> lake -> features -> model -> scores -> alerts
uv run netanomaly --root data/synth run                 # synthetic lake
uv run netanomaly --root data/synth evaluate --k 50 100 500
```

Its stages, contract and guarantees are described in ARCHITECTURE.md ("Legacy lake pipeline").

## Tests

```bash
uv run pytest -q
uv run ruff check src tests
```
