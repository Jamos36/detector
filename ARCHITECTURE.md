# Architecture

Stable technical design. Status and history live in PROJECT_STATUS.md / CHANGELOG.md.

## Principle
Raw data → validation/normalization → behavioural features → feature analysis → Isolation Forest →
evaluation → incident aggregation → analyst feedback → advanced models only if justified.

## Stack
Python 3.13 (uv), DuckDB, Parquet/PyArrow, scikit-learn, pydantic, pytest. CPU only, small RAM budget.

## Data flow (`uv run netanomaly [--root DIR] <stage>`)

| Stage | Module | Reads | Writes |
|---|---|---|---|
| generate | `synth.py`, `inject.py` | — | `raw/synth_netflow_*.parquet`, `truth/{hosts,injections,injected_flows}.csv` |
| ingest | `ingest.py` | `raw/**/*.csv, *.parquet` | `lake/flows/flow_date=YYYY-MM-DD/src_<hash32>_<i>.parquet`, `lake/rejects/src_<hash32>.parquet`, `lake/_ingest_ledger.jsonl` |
| dq | `quality.py` | lake + rejects + ledger + raw headers | `outputs/dq/dq_<batch>.{json,md}` |
| features | `features.py` | lake | `features/host_window/flow_date=…/` |
| baselines | `baselines.py` | lake (`src_ip`, `flow_start`, `bytes`, `src_subnet`) | `features/host_baseline/flow_date=…/part-0.parquet` |
| novelty | `novelty.py` | lake (`src_ip`, `flow_start`, `dst_ip`, `dst_port`) + seen set | `features/host_novelty/flow_date=…/part-0.parquet`, `features/novelty_state/` |
| train | `iforest.py` | features (reservoir sample) | `models/iforest-<ts>/{model.joblib, manifest.json}` |
| score | `iforest.py` | features (Arrow batches) | `outputs/scores.parquet` |
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
`baselines.py` and `novelty.py` check their features against the registry before computing (`require_usable`).
`usable_features()` returns the eligible set; `FEATURES.md` is generated by `feature-doc` (usable vs not usable
tables, per-source confidence/validated) and drift-tested. The pipeline does not read the registry yet: V0
`features.py`/`iforest.py` are kept in sync with it by tests (names, order, log1p set).

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

## Model outputs
`anomaly_score = -IsolationForest.score_samples(x)`: a ranking within a model version, not a probability.

## Model artifacts (current, development only)
- `train` writes `models/iforest-<UTC timestamp>/model.joblib` + `manifest.json`; there is no fixed
  `models/model.joblib`. With `--root DIR`, models live under `DIR/models/`.
- `model.joblib` is a `joblib.dump` (pickle) of a fitted scikit-learn `IsolationForest` only (200 trees,
  `max_samples` 256, 9 inputs). It is fit on a plain NumPy array, so it has no `feature_names_in_`: column order
  comes solely from `manifest.features`. The `log1p` / NaN→0 transform is code (`iforest.transform`), not part of
  the artifact. Tree split thresholds are values derived from the training features, so an artifact trained on
  real data carries information about that data.
- `manifest.json` records model_version, features, log1p_features, train_rows, train_period, ModelSettings,
  sklearn and Python versions, created_at. It does **not** record numpy/joblib versions, `window_minutes`,
  contract version, code commit, or which lake batches/feature files were used.
- Loading (`cli._latest_model`): picks the lexicographically last `models/iforest-*` (= newest UTC timestamp),
  `joblib.load`s it and reads the manifest. No check that installed library versions match the manifest.
- Traceability: `scores.parquet` rows carry `model_version`; `top_alerts.csv` joins each top host-window back to
  the lake (`src_ip` + window) for `source_files` and sample `flow_id`s.
- Portability: loading a pickle executes code, so only artifacts from a trusted pipeline may be loaded.
  scikit-learn does not guarantee that pickles load or behave identically across versions, so scoring must use
  the versions in the manifest (pinned in `uv.lock`). Joblib is **not** a decided deployment format (ADR-014).
- The committed artifacts (`data/models/…`, `data/synth/models/…`) come from the V0 mock/synthetic workflow and
  are demonstration artifacts only (ADR-012).

## Deployment target (not implemented)
The workflow is to be re-run in a separate company environment on real data (ADR-012) and exposed as a tool that
a company LLM agent can invoke alongside other data tools (ADR-013). Division of labour: the model and the
pipeline compute features, scores and top-K rankings; the LLM orchestrates calls and explains results. Every
finding stays traceable to `model_version`, source files and `flow_id`s. No interface is designed yet.
Alerts = top-K host-windows per UTC day (alert budget), each with source files and sample `flow_id`s.
