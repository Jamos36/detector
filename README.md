# netanomaly

Unlabeled NetFlow anomaly detection: DuckDB + Parquet + Isolation Forest, built
for a CPU-only machine with little free RAM. Developed in the versioned steps of
the project brief (V0 prototype -> V8 production); each version is validated
before the next begins.

## Quick start

```bash
uv sync
uv run netanomaly run                      # data/raw -> lake -> features -> model -> scores -> alerts
uv run pytest -q
```

Synthetic data with persistent hosts and known injected attacks (kept in its own lake):

```bash
uv run netanomaly --root data/synth generate --days 6 --clean-days 3
uv run netanomaly --root data/synth run
uv run netanomaly --root data/synth evaluate --k 50 100 500
```

Stages can be run individually: `ingest`, `features`, `train`, `score`, `alerts`.

## Data flow

| Stage | Reads | Writes |
|---|---|---|
| ingest | `raw/*.csv, *.parquet` | `lake/flows/flow_date=YYYY-MM-DD/`, `lake/_ingest_ledger.jsonl` |
| features | lake | `features/host_window/flow_date=.../` |
| train | features (reservoir sample) | `models/iforest-<ts>/model.joblib`, `manifest.json` |
| score | features (Arrow batches) | `outputs/scores.parquet` |
| alerts | scores + lake | `outputs/top_alerts.csv` (top-K per day, with source files and flow_ids) |

## Ingestion rules

- File type is detected from content (Parquet magic bytes), not the extension.
- If `X.csv` and `X.parquet` both exist, only the Parquet is ingested (they hold the same rows).
- Every row gets `flow_id`, `source_file`, `source_row_number` (1-based data row; CSV line = row + 1),
  `source_file_hash`, `ingest_batch_id`.
- Re-runs skip files whose hash is already in the ledger; bad files are quarantined in the ledger, never crash the run.
- All timestamps are UTC (microsecond precision; source nanoseconds are truncated).

## Columns

`src/netanomaly/contracts/netflow_v1.yaml` is both the schema contract and the data dictionary:
each of the 42 raw columns has its inferred meaning, a confidence level, and whether the model may use it.
Columns with low/unknown confidence (`tcp_flag`, `packet_length`, `time_code`, `vlad_id_customer`) are
never model features until their meaning is confirmed from the collector's documentation.

## Memory

Tune in `config.yaml`: `duckdb.memory_limit` (DuckDB spills to `duckdb.temp_directory` beyond it),
`duckdb.threads`, and `batch_rows` (rows held in Python while scoring).
