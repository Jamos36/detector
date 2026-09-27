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
| ingest | `ingest.py` | `raw/**/*.csv, *.parquet` | `lake/flows/flow_date=YYYY-MM-DD/src_<hash32>_<i>.parquet`, `lake/_ingest_ledger.jsonl` |
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
- CSV → staged Parquet with explicit contract types and preserved order, then `read_parquet(file_row_number)`
  gives `source_row_number` (1-based data row; CSV line = row + 1).
- `flow_id = left(sha256(file_hash || ':' || row), 32)` — deterministic across re-runs.
- Ledger (JSONL, latest entry per file hash wins): `in_progress` → `ingested | quarantined`.
  Output is written to `lake/_staging/<hash>` and swapped in only after success; a crash leaves the file retryable.
- Timestamps are TIMESTAMPTZ in UTC, microsecond precision (source nanoseconds truncated).

## Schema contract
`src/netanomaly/contracts/netflow_v1.yaml` maps raw → canonical names and types and records meaning,
`confidence`, `validated`, and `model_use` (entity / feature / derive / provenance / exclude).
Low/unknown-confidence columns cannot be features (enforced on load). `SCHEMA.md` is generated from it.

## Model outputs
`anomaly_score = -IsolationForest.score_samples(x)`: a ranking within a model version, not a probability.
Alerts = top-K host-windows per UTC day (alert budget), each with source files and sample `flow_id`s.
