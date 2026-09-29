# Architecture

Stable technical design. Status and history live in PROJECT_STATUS.md / CHANGELOG.md.

## Stack
Python 3.13 (uv), DuckDB, Parquet/PyArrow, scikit-learn, pydantic, matplotlib (PoC report), pytest. CPU only,
small RAM budget.

# Parquet-only PoC (active workflow, ADR-024…030)

## Flow
```
external Parquet (read in place)  ->  profile            (DuckDB aggregates only)
        |  field_map -> canonical flow relation (+ source_file, source_row_index)
        v
host x window feature table (cached)  ->  split: train | validation | test  (whole UTC days, chronological)
        |                                      |
        |              fit on train only: log1p -> median imputer -> scaler -> IsolationForest / OneClassSVM
        v                                      v
score every window (Arrow batches, raw = -score_samples)  ->  bands from validation quantiles / budget
        -> scores / alerts / summaries (Parquet) -> diagnostics (JSON) -> report.md + charts (SVG/PNG)
```

## Modules (`src/netanomaly/poc/`)
| module | responsibility |
|---|---|
| `config.py` | pydantic config (`poc.example.yaml`), env-var expansion, validation (window divides a day, band order) |
| `source.py` | Parquet discovery (magic bytes), union schema, source -> canonical mapping with type checks and assumptions, the flow relation, repository-boundary guard |
| `profile.py` | input profile (footers, schema, mapping, null/invalid rates, span, per-day counts, approx. cardinalities) |
| `featureset.py` | feature definitions (`FEATURES`, rendered into FEATURES.md), feature table build + cache, training-row quality screening |
| `annotations.py` | pentest ranges (YAML), inside / buffer / outside categories as SQL |
| `splits.py` | explicit or fraction-based chronological periods and their SQL predicates |
| `models.py` | model specs, pipelines, bounded day-spread training sample, memory guards, fit, joblib save/load, batch scoring |
| `bands.py` | reference-quantile / budget cutoffs, band SQL, percentile grid |
| `diagnostics.py` | label-free and weak-annotation diagnostics (distributions, volume vs cutoff, concentration, overlap, agreement, drift, training concentration) |
| `outputs.py` | scores / alerts (deviations, host baseline, `beyond_train_range`, source traces) / daily and entity summaries |
| `experiment.py` | orchestration (`open_context`, `train_models`, `score_models`, `robustness`, `finalize`, `run_search`), experiment id, manifest |
| `charts.py`, `report.py` | static charts from aggregated data; `report.md` |

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

## Artifacts (development format, ADR-014)
`experiments/<id>/models/<model>.joblib` (whole pipeline) + `<model>.json`; `manifest.json` records inputs
(paths, sizes, fingerprint), mapping, periods, features and screening, training filter, model settings and
samples, seeds, band settings and every band revision, library versions and the code commit. Loading a pickle
executes code: only load artifacts you produced.

# Legacy lake pipeline (V0–V3, not used by the PoC)

## Principle
Raw data → validation/normalization → behavioural features → feature analysis → Isolation Forest →
evaluation → incident aggregation → analyst feedback → advanced models only if justified.

## Data flow (`uv run netanomaly [--root DIR] <stage>`)

| Stage | Module | Reads | Writes |
|---|---|---|---|
| generate | `synth.py`, `inject.py` | — | `raw/synth_netflow_*.parquet`, `truth/{hosts,injections,injected_flows}.csv` |
| ingest | `ingest.py` | `raw/**/*.csv, *.parquet` | `lake/flows/flow_date=YYYY-MM-DD/src_<hash32>_<i>.parquet`, `lake/rejects/src_<hash32>.parquet`, `lake/_ingest_ledger.jsonl` |
| dq | `quality.py` | lake + rejects + ledger + raw headers | `outputs/dq/dq_<batch>.{json,md}` |
| features | `features.py` | lake | `features/host_window/flow_date=…/` |
| baselines | `baselines.py` | lake (`src_ip`, `flow_start`, `bytes`, `src_subnet`) | `features/host_baseline/flow_date=…/part-0.parquet` |
| novelty | `novelty.py` | lake (`src_ip`, `flow_start`, `dst_ip`, `dst_port`) + seen set | `features/host_novelty/flow_date=…/part-0.parquet`, `features/novelty_state/` |
| timing | `timing.py` | lake (`src_ip`, `dst_ip`, `flow_start`) | `features/host_timing/flow_date=…/part-0.parquet` |
| feature-cards | `feature_cards.py`, `feature_report.py`, `labels.py` | features (all four tables) + lake + truth (if present) | `outputs/feature_cards/feature_cards.{json,md}` |
| train | `iforest.py` | host_window rows on the training days (hash sample) + registry | `models/iforest-<ts>/{model.joblib, manifest.json}` |
| score | `iforest.py` | host_window rows after the training days (Arrow batches) | `outputs/scores.parquet` |
| stability | `stability.py` | host_window + registry (no truth) | `outputs/stability/stability.{json,md}` |
| alerts | `alerts.py` | scores + lake | `outputs/top_alerts.csv` |
| evaluate | `alerts.py` | scores + lake + truth | recall@K per attack type (log) |

Each stage reads the previous stage's files, so stages re-run independently. `--root` isolates datasets.

## Memory model
- DuckDB does all scans/aggregations over Parquet with `memory_limit` (default 2GB) and spills to `temp_directory`.
- Python holds at most `batch_rows` rows (scoring) or `train_sample_rows` (training sample).
- All settings in `config.yaml`; every connection via `db.connect()`.
- `memory_limit` bounds DuckDB's buffer manager, not the process: measured process peak is up to ~1.2x–1.5x of it
  (Python, Arrow, DuckDB allocations outside the buffer pool). Large single CSVs trade memory for spill disk
  (V1-3: ~14 GB spill for a 7.7 GB / 20M-row CSV). Measure with `scripts/memtest.py`.

## Ingestion guarantees
- Content-based type detection (Parquet magic bytes); raw files are never modified.
- CSV → staged all-VARCHAR Parquet in record order (`read_csv(store_rejects)`); records DuckDB cannot split into
  columns are held in a temp table, and surviving rows are renumbered past them (ASOF join), so
  `source_row_number` is always the 1-based data record in the original file (quoted multi-line fields count once).
- Contract types are applied with `TRY_CAST` for CSV and Parquet alike. A row is **rejected**, not the file, when a
  value cannot be cast, a required column (`flow_start`, `flow_end`, `src_ip`, `dst_ip`) is empty, or the CSV
  record is malformed. Rejects go to `lake/rejects/src_<hash32>.parquet`, one row per (record, reason):
  `flow_id, source_file, source_row_number, reason, column_name, raw_value, detail, source_file_hash, ingest_batch_id`.
  Reason codes: `cast_failed`, `missing_required`, and DuckDB's CSV errors snake_cased (`too_many_columns`,
  `missing_columns`, `unquoted_value`, `invalid_encoding`, `line_size_over_maximum`, `invalid_state`).
- If rejected rows exceed `ingest.max_reject_fraction` (default 5%) the whole file is quarantined instead
  (nothing written; ledger reason lists counts per reason). The ledger records `rows` (accepted) and `rejected_rows`.
- `flow_id = left(sha256(file_hash || ':' || row), 32)` — deterministic across re-runs. It is the only flow
  key: `flow_sequence` is kept as a raw column but never used to deduplicate, join or identify flows, except
  by synthetic recall@K, which checks that every truth value matches exactly one lake flow (ADR-016).
- Ledger (JSONL, latest entry per file hash wins): `in_progress` → `ingested | quarantined`.
  Output is written to `lake/_staging/<hash>` and swapped in only after success; a crash leaves the file retryable.
- Timestamps are TIMESTAMPTZ in UTC, microsecond precision (source nanoseconds truncated). Parsing follows
  `timestamps.py` (ADR-015): a value with `Z`/`UTC`/`±hh[:mm]` is converted; a valid value without an offset (text
  or Parquet `TIMESTAMP`) is assumed UTC via `timezone('UTC', …)`, independent of the session zone; anything
  else is a `cast_failed` reject. Offset-free values of kept rows are counted per column into the ledger field
  `timestamps_without_offset` (one extra scan of the source's timestamp columns, anti-joined with the rejects).

## Data-quality report (`netanomaly dq [--batch ID]`, also run after ingest in `run`)
- Scope: files whose latest ledger entry has that `ingest_batch_id` (default: newest batch).
- One DuckDB pass over the batch's lake files gives the row count, every plausibility check
  (applicable rows, violations, first violating `source_file:row`) and per-column null counts.
- Rejects by (reason, column) from `lake/rejects/`; quarantined files appear in the files table with their ledger reason.
- Timestamps without offset: per file and column from the ledger (`timestamps_without_offset`), with the rate of
  the file's ingested rows; any count > 0 adds a warning line to the top of the report and to the `dq` log.
- Column drift: raw files located by name and confirmed by hash; header vs contract (missing/unexpected);
  Parquet physical types vs contract as `width` (same family, cast on ingest) or `family` (different).
- Daily volume: flows per UTC day across the whole lake vs the median of the previous `dq.trailing_days`
  calendar days (strictly earlier, missing days = 0); `low`/`high` outside `dq.volume_ratio_low/high`,
  `insufficient_history` below `dq.min_history_days`.
- Observational only: the report never blocks ingestion or later stages.

## Schema contract
`src/netanomaly/contracts/netflow_v1.yaml` maps raw → canonical names and types and records meaning,
`confidence`, `validated`, and `model_use` (entity / feature / derive / provenance / exclude).
Low/unknown-confidence columns cannot be features (enforced on load). `SCHEMA.md` is generated from it.

## Feature registry (ADR-017)
`src/netanomaly/contracts/features_v1.yaml` (`registry_version`, bound to contract name + `schema_version`) lists
every candidate feature: `level` (flow / host_window), `status` (implemented / candidate, with the roadmap `task`),
`temporal_scope` (window / prior_history), `inputs` (canonical contract columns or declared derived lake columns,
e.g. `tcp_syn` <- `tcp_flags`), `transform`, `model_transform`, `rationale`, and `attack_hypotheses` (ATT&CK
technique IDs, hypotheses only). `feature_registry.py` loads it with pydantic (`extra="forbid"`, so trust fields
cannot be hand-set) and checks it against the contract on load.
Eligibility is computed, never declared: a feature is usable only if every contract source (derived inputs
expanded) has confidence high/medium and model_use entity/feature/derive. `validated` is reported per source and
per feature (all sources validated) but does not gate usability while 0/42 fields are validated.
`baselines.py`, `novelty.py` and `timing.py` check their features against the registry before computing (`require_usable`).
`usable_features()` returns the eligible set; `FEATURES.md` is generated by `feature-doc` (usable vs not usable
tables, per-source confidence/validated) and drift-tested. The model reads its inputs and log1p set from the registry
(`iforest.model_features`, V3); `features.py` still computes all 9 V0 columns, kept in sync with the registry by tests.

## Host baselines (V2-2, `netanomaly baselines`, ADR-018)
Not part of `run` and not a model input yet: the V0 model is unchanged; V3 decides whether to train on it.
- Row: one per host-window (same grid as `host_window`, `window_minutes`), `bytes_out = sum(bytes)` as in V0.
  Value `v = ln(1 + bytes_out)`; negative `bytes_out` gives NULL.
- Baseline window / prior-history rule: a row on UTC day D uses only rows with `flow_date` in
  [D - `baseline.lookback_days`, D - 1] (default 7 days). Day D itself — including its earlier windows — is never
  history, so no row at or after D can change a baseline on D. Baselines therefore refresh daily (up to 24 h stale).
  Only active windows exist: the baseline describes the host when it is active.
- Fallback: `host` (own history) → `peer` (all hosts whose windows carry the same `src_subnet`) → `global`
  (all hosts) → `none`. A level is used when its history has ≥ `min_windows` windows (30) on ≥ `min_days` distinct
  days (2) and MAD > 0; `peer`/`global` also need ≥ `min_peer_hosts` hosts (5). Peer group = `min(src_subnet)` of
  the window (the contract field, one subnet per host in all data seen); NULL subnet skips to `global`.
- `bytes_out_robust_z = (v - median) / (1.4826 * MAD)` of the chosen level; NULL at `none`. A ranking signal.
- Quality: `baseline_quality` (host > peer > global > none) plus the chosen level's support `baseline_windows`,
  `baseline_days`, `baseline_hosts`, the host's own `host_windows`, `host_days`, and `baseline_median`/`baseline_mad`.
- Eligibility: before computing, `require_usable_inputs` checks that every feature in
  `baselines.BASELINE_FEATURES` is usable in the registry against the contract; it refuses otherwise.
- Memory: one DuckDB pass writes a slim host-window input (`temp_directory/baseline_input`, deleted afterwards);
  then one query per day over lookback + 1 days of it. Output is staged and swapped in when all days succeed.

## Host novelty (V2-3, `netanomaly novelty [--rebuild]`, ADR-019)
Not part of `run` and not a model input yet (V0 model unchanged).
- Row: one per host-window (same grid as `host_window`; `window_minutes` must divide a day so windows never span
  two UTC days). Order comes from `flow_start` only, never from `flow_sequence` or file row order.
- History of window W: every flow of the same `src_ip` with `flow_start` before W's start, back to the start of the
  lake (no lookback limit). Flows inside W are never each other's history, so tied timestamps (always in one window)
  and row order cannot change a result; adding flows at or after W's start cannot change W or any earlier window.
- New: `dst_ip` is new in W iff W is the first window of the pair (`src_ip`, `dst_ip`); likewise `dst_port` (label;
  NULL ports ignored). `new_dst_ip_rate = new_dst_ip / uniq_dst_ip`, `new_dst_port_rate = new_dst_port /
  uniq_dst_port` (NULL without ports). Output also keeps the counts, `host_first_seen` (host's first window; rates
  are 1 there by definition) and `history_days` (lake days before the row's day). `uniq_*` equal V0 `host_window`.
- Seen set (state): `features/novelty_state/{seen_src_ip,seen_dst_ip,seen_dst_port}/first_date=D/part-0.parquet`,
  append-only, one row per key with its `first_window`, partitioned by the day it was first seen. Day D is computed
  from lake partition D plus state partitions before D, then D's first-seen keys are appended.
- `manifest.json`: `state_version`, `window_minutes`, and per lake day a fingerprint of its files (names + sizes).
  Rerun: the earliest day that is new, changed, removed, or lacks output/state files is the restart day; partitions
  at or after it are dropped and recomputed in order, earlier ones are not touched. No change → nothing recomputed.
  Other `window_minutes`/state version, an unreadable manifest, or `--rebuild` → full rebuild. The manifest is
  rewritten after each day (atomic replace), so an interrupted run resumes at the first unfinished day.
- Eligibility: `novelty.require_usable_inputs` checks `NOVELTY_FEATURES` against the registry before computing.
- Memory: per day, one lake partition plus the earlier seen-set entries (hash anti-join; DuckDB spills). State grows
  with distinct (host, destination) pairs and is never pruned.

## Timing regularity (V2-4, `netanomaly timing`, ADR-020)
Not part of `run` and not a model input yet (V0 model unchanged). Settings `timing:` in config.yaml.
- Row: one per host-window (same grid as `host_window`; `window_minutes` must divide a day). The row marks when to
  look; the window's own flows never enter its value.
- History window: flows of the same `src_ip` with `flow_start` in [W - `history_hours`, W) (default 2 h; 1–24 h).
  Flows at or after W — in W itself or later — cannot change the row. Order comes from UTC `flow_start` only, never
  from `flow_sequence` or file row order.
- Series (peer grouping): one per (`src_ip`, `dst_ip`), all ports/protocols; events are the pair's distinct
  `flow_start` instants (tied flows = one event, so ties add no zero gaps and cannot depend on row order). No
  subnet/global fallback. Gaps are between consecutive events with both endpoints in the history window.
- Minimum support: a series needs ≥ `min_events` events (default 10 = 9 gaps) in the history window.
- Statistic: `cv = stddev_pop(gap) / mean(gap)` per qualifying series; `interarrival_cv` = minimum over the host's
  series (ties: lowest `dst_ip`), reported with `timing_dst_ip`, `timing_events`, `timing_median_gap_s`. Low = regular.
- Missing/insufficient: `timing_quality` = `ok` | `insufficient` (flows in history, no series reaches min_events) |
  `none` (no flows in history); `interarrival_cv` is NULL unless `ok`. `history_complete` = the history window starts
  at or after the lake's earliest `flow_start`. Support: `timing_pairs`, `history_pairs`, `history_events`.
- Eligibility: `timing.require_usable_inputs` checks `TIMING_FEATURES` against the registry before computing.
- Memory: one lake pass writes a slim event table (distinct `src_ip, dst_ip, flow_start` + previous instant of the
  pair; `temp_directory/timing_events`, deleted afterwards), then one query per day over that day and the day before.
  Output is staged and swapped in when every day succeeded; every run recomputes all days (no incremental state).

## Feature cards (V2-5, `netanomaly feature-cards`, ADR-021)
Diagnostics, not a pipeline stage: not part of `run`, never an input to features, the model or alerts. Settings
`feature_cards:` in config.yaml. Output `outputs/feature_cards/feature_cards.{json,md}` (JSON floats rounded to 10
significant digits so reruns are byte-identical; DuckDB parallel sums differ in the last digits).
- Scope: registry features that are usable (computed eligibility) and implemented; `FEATURE_SOURCES` maps each to its
  table, pre-declared anomalous direction and quality column. Usable candidates and not-usable features are listed as
  not analysed with the reason. A usable implemented feature without a source entry is refused.
- Rows: TEMP TABLE `card_rows` = the `host_window` grid LEFT JOIN `host_baseline`, `host_novelty`, `host_timing` on
  (`src_ip`, `window_start`) plus labels; each table must cover exactly the grid (else refuse: stale features).
- Statistics (DuckDB aggregates only; Python gets aggregates): distribution (quantiles, mean, std, zero share),
  missingness (NULL share overall/per day, quality-level counts), cardinality (distinct, top 3 values), Spearman rho
  per pair over pairwise-complete rows (columns with NULLs re-ranked per pair), PSI per day, AUROC per attack type.
- PSI: reference = lake days [`warmup_days`, `warmup_days + reference_days`) by position (default day index 2, i.e.
  the first day with host baselines); edges = distinct `psi_bins`-quantiles of the reference (right-closed) + a NULL
  bin; shares floored at 1e-4.
- Labels (`labels.py`, synthetic evaluation only): `verify_truth` first checks the truth maps onto the lake — every
  truth `flow_sequence` matches exactly one lake flow (`alerts.check_truth_join`), every truth flow names a known
  injection, lake `src_ip` equals the injection's `src_ip`, and each injection's matched flow count equals `n_flows`.
  Host-window label: the window contains ≥ 1 injected flow of that type; negative only with no injected flow.
  Flow-level label: the flow itself is in the truth (no flow-level feature is implemented, so none is scored).
- AUROC: population = host-windows on days with injected flows; per type: its windows vs clean windows (other
  attacks excluded); Mann-Whitney with ties 1/2, NULL lowest, score oriented by the declared direction.
- Without truth files (mock data) the cards are produced without AUROC; with a lake too short for the reference,
  without PSI.

## Time-based split and model inputs (V3, `netanomaly train` / `score`, ADR-022)
- Days: the distinct `flow_date` partitions of `host_window` (UTC days, positional). The first
  `floor(n_days * split.train_fraction)` days (default 0.5) train; every later day is scored. Both sides need >= 1 day,
  so `train`/`run` refuse a one-day lake. The rule never reads labels.
- Training rows: `flow_date <= train_end` is filtered **before** sampling; the sample is the `train_sample_rows` rows
  with the lowest `hash(src_ip, window_start, seed)`, sorted by key before fitting. A row's inclusion depends only on
  its own key, so later rows, file order and thread order cannot change the training set or the fitted forest. The
  forest is the only learned step; `transform` (log1p, NaN -> 0) is stateless, so no fitted preprocessing can leak.
- Scoring: only rows with `flow_date > manifest.score_after` (= last training day), in Arrow batches; a row's score
  depends only on its own features and the model, so later rows cannot change it. Days added after training are
  scored as they arrive. `score` removes the old `scores.parquet` first and refuses when no later day exists.
- Inputs: `iforest.model_features` = registry features that are usable against the contract, implemented,
  `host_window` and `temporal_scope: window`, in registry order: `flows, bytes_out, packets_out, uniq_dst_ip,
  uniq_dst_port, internal_ratio, max_flow_bytes`; log1p set from `model_transform`. Prior-history features
  (baselines, novelty, timing) are separate tables and not model inputs yet.
- `check_scorable` (in `score`) refuses a manifest without `score_after` (V0) or with an input the registry rates
  not usable. `evaluate` logs injected host-windows on training vs scored days (synthetic truth, after training).

## Stability (V3, `netanomaly stability`, ADR-023)
Diagnostics, not a pipeline stage: not part of `run`; trains its own in-memory models (nothing in `models/`) with the
same split and registry inputs as `train`, scores the held-out days and compares rankings. **Never reads truth.**
Settings `stability:` in config.yaml; K = `alert_budget_per_day`.
- Reference = mean `anomaly_score` of `seeds` models (seeds `model.seed + i`) on `min(train_sample_rows, training
  rows)` rows; the mean uses an ordered aggregate so reruns are byte-identical.
- Seed stability: all pairs of those models. Sample-size curve: per size in `sample_sizes` (capped at the training rows,
  which are always a point) `curve_seeds` models with seeds `model.seed + 1000 + j` (disjoint from the reference),
  each against the reference. Seed changes both the hash sample and the forest.
- Agreement: Spearman rho over all scored rows (average ranks for ties) and top-K overlap per UTC day (share of a
  day's top-K shared; ties broken by `src_ip`, `window_start`), reported as median (min–max) and worst day.
- Memory/cost: each model's scores go to `temp_directory/stability/<variant>.parquet` in Arrow batches (deleted
  afterwards); ranks and pair joins run in DuckDB TEMP tables. One model = one fit + one pass over the score days
  (~1.7 s on synthetic data; 50 models ≈ 90 s).

`anomaly_score = -IsolationForest.score_samples(x)`: a ranking within a model version, not a probability.

## Model artifacts (current, development only)
- `train` writes `models/iforest-<UTC timestamp>/model.joblib` + `manifest.json`; there is no fixed
  `models/model.joblib`. With `--root DIR`, models live under `DIR/models/`.
- `model.joblib` is a `joblib.dump` (pickle) of a fitted scikit-learn `IsolationForest` only (200 trees,
  `max_samples` 256; 7 inputs in V3, 9 in the preserved V0 artifacts). It is fit on a plain NumPy array, so it has no `feature_names_in_`: column order
  comes solely from `manifest.features`. The `log1p` / NaN→0 transform is code (`iforest.transform`), not part of
  the artifact. Tree split thresholds are values derived from the training features, so an artifact trained on
  real data carries information about that data.
- `manifest.json` records model_version, features, log1p_features, train_rows (sample), train_period,
  ModelSettings, sklearn and Python versions, created_at, and since V3 `registry_version`, `train_fraction`,
  `train_dates`, `score_after`, `train_period_rows` and `duckdb_version` (the sample hash depends on it). V0
  manifests lack the V3 fields; they still load but are refused for scoring. It does **not** record numpy/joblib
  versions, `window_minutes`, contract version, code commit, or which lake batches/feature files were used.
- Loading (`cli._latest_model`): picks the lexicographically last `models/iforest-*` (= newest UTC timestamp),
  `joblib.load`s it and reads the manifest. No check that installed library versions match the manifest.
- Traceability: `scores.parquet` rows carry `model_version`; `top_alerts.csv` joins each top host-window back to
  the lake (`src_ip` + window) for `source_files` and sample `flow_id`s.
- Portability: loading a pickle executes code, so only artifacts from a trusted pipeline may be loaded.
  scikit-learn does not guarantee that pickles load or behave identically across versions, so scoring must use
  the versions in the manifest (pinned in `uv.lock`). Joblib is **not** a decided deployment format (ADR-014).
- The committed artifacts are demonstration artifacts only (ADR-012): V0 `data/models/iforest-20260927T191305Z`
  (mock) and `data/synth/models/iforest-20260927T191244Z` (synthetic; its scores/alerts kept in
  `data/synth/outputs/v0/`), and the V3 synthetic model `data/synth/models/iforest-20260928T055319Z`.

## Deployment target (not implemented)
The workflow is to be re-run in a separate company environment on real data (ADR-012) and exposed as a tool that
a company LLM agent can invoke alongside other data tools (ADR-013). Division of labour: the model and the
pipeline compute features, scores and top-K rankings; the LLM orchestrates calls and explains results. Every
finding stays traceable to `model_version`, source files and `flow_id`s. No interface is designed yet.
Alerts = top-K host-windows per UTC day (alert budget), each with source files and sample `flow_id`s.
