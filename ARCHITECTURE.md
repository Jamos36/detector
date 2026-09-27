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
| train | `iforest.py` | features (reservoir sample) | `models/iforest-<ts>/{model.joblib, manifest.json}` |
| score | `iforest.py` | features (Arrow batches) | `outputs/scores.parquet` |
| alerts | `alerts.py` | scores + lake | `outputs/top_alerts.csv` |
| evaluate | `alerts.py` | scores + lake + truth | recall@K per attack type (log) |

Each stage reads the previous stage's files, so stages re-run independently. `--root` isolates datasets.

## Memory model
- DuckDB does all scans/aggregations over Parquet with `memory_limit` (default 2GB) and spills to `temp_directory`.
- Python holds at most `batch_rows` rows (scoring) or `train_sample_rows` (training sample).
- All settings in `config.yaml`; every connection via `db.connect()`.

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
- `flow_id = left(sha256(file_hash || ':' || row), 32)` — deterministic across re-runs.
- Ledger (JSONL, latest entry per file hash wins): `in_progress` → `ingested | quarantined`.
  Output is written to `lake/_staging/<hash>` and swapped in only after success; a crash leaves the file retryable.
- Timestamps are TIMESTAMPTZ in UTC, microsecond precision (source nanoseconds truncated).

## Data-quality report (`netanomaly dq [--batch ID]`, also run after ingest in `run`)
- Scope: files whose latest ledger entry has that `ingest_batch_id` (default: newest batch).
- One DuckDB pass over the batch's lake files gives the row count, every plausibility check
  (applicable rows, violations, first violating `source_file:row`) and per-column null counts.
- Rejects by (reason, column) from `lake/rejects/`; quarantined files appear in the files table with their ledger reason.
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

## Model outputs
`anomaly_score = -IsolationForest.score_samples(x)`: a ranking within a model version, not a probability.
Alerts = top-K host-windows per UTC day (alert budget), each with source files and sample `flow_id`s.
