# Decisions

Settled decisions and why. Reopen one only when its "revisit when" condition occurs.

## ADR-001: DuckDB over Parquet for all scans and aggregation
Decision: DuckDB computes normalization, features, sampling and joins directly on Parquet; Python sees bounded batches.
Reason: data will exceed free RAM (~2–3 GB free on the dev machine); CPU only.
Rejected: pandas (memory), Spark/Dask (operational complexity at this scale).
Revisit when: single-node DuckDB is a measured bottleneck.

## ADR-002: Schema contract with confidence AND validated
Decision: every raw field has an inferred meaning, a `confidence`, and a separate `validated` flag (collector docs only).
Only high/medium-confidence fields may be features; enforced in code.
Reason: field names are unreliable (`dist_` = destination?, `flow_length` = duration in ms, `time_code` not a timezone).
Revisit when: collector documentation is obtained — then set `validated` per field.

## ADR-003: Content-based file detection; Parquet wins over same-named CSV
Decision: detect Parquet by magic bytes, not extension; if `X.csv` and `X.parquet` both exist, ingest only the Parquet.
Reason: raw folder held both forms of the same data, which would double-count every flow.

## ADR-004: Row-level provenance and deterministic flow_id
Decision: `flow_id` = hash(file hash, data row number); every row keeps source file/row/hash/batch.
Reason: every anomaly must trace back to original records; IDs must be stable across re-runs.

## ADR-005: Crash-safe ingestion (ledger + staged swap)
Decision: `in_progress` ledger entry before writing; write to `lake/_staging`, swap in on success; tolerant ledger reader.
Reason: code review found a crash could brick future runs or delete good output (fixed, regression-tested).

## ADR-006: Synthetic generator with persistent hosts
Decision: build our own generator in the real 42-column schema with roles, diurnal cycles and injected attacks.
Reason: the mock data has no entity continuity, so host features and recall@K cannot be evaluated on it.
Revisit when: real data with persistent hosts is available (then inject attacks into real held-out days).

## ADR-007: Alert budgets, not severity tiers; scores are rankings
Decision: select top-K per day; no Critical/High/Medium labels; never call scores confidence/probability.
Reason: unlabeled data; operational meaning of score levels is unknown.

## ADR-008: Temporal model only if evaluation justifies it
Decision: no CNN/TCN until V4 shows Isolation Forest misses temporal patterns; if built, train independently of IF.
Reason: priority is data correctness and measurable detection quality over model complexity.

## ADR-009: `data/` stays tracked; the project uses mock and synthetic data only
Decision (owner, 2026-09-27): `data/` remains committed and is not git-ignored; no history scrub.
Reason: the project will never process real network traffic, so the repo holds only mock and synthetic data
(commit 1449dc1, ~99 MB including generated artifacts).
Revisit when: real traffic is ever introduced — then git-ignore `data/` before it lands in `data/raw`.
