# Architecture

How the anomaly-ranking proof of concept works. Status: PROJECT_STATUS.md; history: CHANGELOG.md.

## Stack
Python 3.13 (uv), DuckDB, Parquet/PyArrow, scikit-learn, pydantic, matplotlib (PoC report), pytest. CPU only,
small RAM budget.

# Design (ADR-024…031)

## Flow (ADR-032)
```
A. DEVELOPMENT (experiment.py)                         everything before split.test.start
external Parquet (read in place)  ->  profile            (DuckDB aggregates only)
        |  field_map -> canonical flow relation (+ source_file, source_row_index)
        v
host x window feature table (cached)  ->  split: train | validation | [test reserved]  (whole UTC days)
        |   [optional relationships.py: pair x window signals, past-only; rel_* summaries join only when
        |    include_model_features]
        |              fit on train only: log1p -> median imputer -> scaler -> IsolationForest / OneClassSVM
        v
score train + validation (Arrow batches, raw = -score_samples)  ->  bands from validation quantiles / budget
        -> scores / alerts / summaries -> diagnostics -> development report
        -> MODEL BUNDLE (bundle.py): pipelines, ordered inputs, mapping, window, cutoffs + grids, periods,
           training summaries, relationship history state

B. SCORING WITH A BUNDLE (scoring.py)                 nothing is fitted
   holdout test: bundle's split.test of the configured input  |  new data: any Parquet after the history end
   check field contract -> features (bundle window/mapping) -> [relationships continued from the history state]
   -> score with the frozen pipelines in the bundle's input order -> bundle cutoffs -> run report (run_report.py)
   holdout runs are logged in holdout_ledger.json (first / repeat / reused)
```

## Modules (`src/netanomaly/`)
| module | responsibility |
|---|---|
| `config.py` | pydantic config (`config.yaml`), `memory_gb`/`threads` -> derived memory settings, env-var expansion, validation (window divides a day, band order) |
| `source.py` | Parquet discovery (magic bytes), union schema, source -> canonical mapping with type checks and assumptions, the flow relation, repository-boundary guard |
| `profile.py` | input profile (footers, schema, mapping, null/invalid rates, span, per-day counts, approx. cardinalities) |
| `featureset.py` | feature definitions (`FEATURES`, rendered into FEATURES.md), feature table build + cache, training-row quality screening |
| `annotations.py` | pentest ranges (YAML), inside / buffer / outside categories as SQL |
| `splits.py` | explicit or fraction-based chronological periods and their SQL predicates |
| `models.py` | model specs, pipelines, bounded day-spread training sample, memory guards, fit, joblib save/load, batch scoring |
| `bands.py` | reference-quantile / budget cutoffs, band SQL, percentile grid |
| `diagnostics.py` | label-free and weak-annotation diagnostics (distributions, volume vs cutoff, concentration, overlap, agreement, drift, training concentration) |
| `outputs.py` | scores / alerts (deviations, host baseline, `beyond_train_range`, source traces) / daily and entity summaries |
| `experiment.py` | development orchestration (`open_context`, `train_models`, `score_models`, `robustness`, `finalize`, `write_bundle`, `run_search`), experiment id, manifest; never scores the test period |
| `relationships.py` | optional source -> destination analysis: sparse pair x window table, novelty / frequency-change signals from strictly earlier windows, host-window summary, evidence, carried history state, optional `rel_*` model inputs |
| `bundle.py` | frozen model bundle: save / load (sha256-checked), field-contract compatibility check |
| `scoring.py` | holdout test and new-data scoring with a bundle (no fitting), holdout ledger, `run.json` |
| `charts.py`, `report.py`, `run_report.py`, `relationship_report.py` | static charts from aggregated data; development report, final-test / new-data report, relationship section (`report.md` + `report.html`) |
| `cli.py` | `uv run netanomaly [run|profile|features|train|score|report|search|test|score-new|docs]` |
| `db.py`, `timestamps.py`, `schema.py` + `contracts/netflow_v1.yaml` | DuckDB connection (UTC, memory limit, spill), strict timestamp parsing, column dictionary (default field mapping, SCHEMA.md) |

## Data boundary and traceability (ADR-012, ADR-025)
- Inputs are read where they are; nothing is copied. Inputs, `work_dir` and the DuckDB spill directory inside the
  checkout are refused unless `allow_inside_repo: true` (mock/synthetic data only). Tests run on tiny fixtures in
  pytest's temporary directory.
- Every window keeps `src_ip`, `window_start`/`window_end`, `first/last_flow_start` and `n_source_files`; alerts for
  the top `trace_top_n` windows carry `trace_source_files` and `trace_rows` (file, 0-based row index), found with one
  scan of the flow relation.

## Time handling and leakage controls (ADR-026, ADR-029)
- UTC throughout (`db.connect()`); offset-free timestamps are assumed UTC and recorded; windows are
  `time_bucket(window_minutes, flow_start)`, half-open, never crossing a UTC day.
- Features use only the window's own flows, so no row depends on later data (tested).
- Periods are whole UTC days; windows are assigned by `window_start`. Training rows are filtered by period (and
  optionally by annotation category) before a per-day hash sample whose membership depends only on each row's key.
  Imputer, scaler, feature screening and the models see training rows only; band cutoffs come from validation.
  Tests check that extra future rows change neither training-period scores nor cutoffs.
- The development experiment scores, plots and summarises only windows before the test start; robustness variants
  use validation. The test period is scored once by `scoring.run_holdout` with the frozen bundle (ADR-032).
- Relationship signals (ADR-033) use DuckDB window frames that end before the current row (`lag`, `RANGE ...
  PRECEDING ... EXCLUDE CURRENT ROW`) plus a carried state (per-pair first/last seen and the last
  `baseline_lookback_days` of pair-windows). Development stops at the test start; the bundle carries that state;
  scoring runs continue it and refuse data that starts before it ends (report-only: skipped with a warning). Tests:
  future rows do not change earlier outputs; a split run with carried state equals a single pass.

## Scores, bands and comparison (ADR-027, ADR-028)
- `raw = -score_samples` (higher = more anomalous) for both models; raw scales differ between models, so the report
  compares reference percentiles (`<m>_pct`) and bands, never raw values across models.
- Bands: raw thresholds at validation quantiles (or budget-derived quantiles); re-banding reuses stored raw scores.
- Isolation Forest cannot extrapolate beyond the training range; `beyond_train_range` flags such windows.
- Robustness (label-free): extra seeds on validation; contamination variants (with/without annotated windows,
  trimmed refit) on test; top-1 % training-window concentration per host; daily/weekly PSI drift per input.

## Memory model (PoC)
- Aggregations, joins, ranking and chart data are DuckDB queries over Parquet (spill to `temp_directory`).
- Python holds: one scoring batch (`batch_rows` x features), one training sample (`max_train_rows` x features; the
  fit is refused above `max_matrix_mb`), validation raw scores for calibration (hash-sampled above 5M), and bounded
  chart data (e.g. a 50,000-row hexbin sample). One-Class SVM fit cost is roughly quadratic in training rows and
  scoring cost is proportional to support vectors x rows; both are bounded by configuration.
- Not yet measured on a real year; `profile` reports the expected number of feature rows first.

## Model bundle and scoring runs (ADR-032)
`bundles/<experiment id>-b<hash of cutoffs + relationship settings>/`: `bundle.json` (ordered model inputs, field
mapping and required fields, window, periods incl. the reserved test range, model settings and sha256, band settings,
validation cutoffs and percentile grids, versions, code commit, config snapshot, relationship settings and history
bounds), `models/*.joblib|json`, `train_stats.parquet` (per-feature training min/max, median, robust scale),
`train_entities.parquet` (per-host training context), `relationships/state/`. No flow rows; the entity table and the
relationship state are derived aggregates containing IPs, so bundles stay outside the checkout.
`scoring/<kind>-<bundle id>-<hash>/`: scores_raw / scores / alerts / summaries, `run.json`, report, relationships.
Compatibility: every required canonical field must come from the same source column and be usable, else nothing is
scored; pipelines must expect exactly the bundle's ordered inputs.

## Artifacts (development format, ADR-014)
`experiments/<id>/models/<model>.joblib` (whole pipeline) + `<model>.json`; `manifest.json` records inputs
(paths, sizes, fingerprint), mapping, periods, features and screening, training filter, model settings and
samples, seeds, band settings and every band revision, library versions and the code commit. Loading a pickle
executes code: only load artifacts you produced.
