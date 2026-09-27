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

## ADR-010: Row-level rejects with a file-level reject budget
Decision: bad rows (malformed CSV record, uncastable value, empty required column) go to a reason-coded reject table
and the rest of the file is ingested; the whole file is quarantined only when rejects exceed `max_reject_fraction` (5%).
CSV is staged as text and typed with `TRY_CAST`, so type checks are identical for CSV and Parquet sources.
Reason: one bad row used to quarantine a whole file; a high reject share signals a wrong export/format, where partial
ingestion would silently bias every downstream feature.
Rejected: DuckDB `ignore_errors` (drops rows without a trace); typed `read_csv` rejects (Parquet sources not covered).
Revisit when: real collector data shows a typical reject rate that makes 5% too tight or too loose.

## ADR-011: Data-quality report is per batch and observational
Decision: `netanomaly dq` reports one ingest batch (checks, nulls, rejects, drift) plus daily volume vs the median
of strictly earlier calendar days (missing days count as 0). It flags; it never blocks ingestion or modelling.
Reason: field meanings are unverified, so a violation may be legitimate traffic or a misread field; a human decides.
Daily volume uses only earlier days, like every other baseline in the project (no temporal leakage).
Revisit when: collector documentation validates the fields behind a check — then that check may become a reject rule.

## ADR-012: Data boundary — this repo is development/demonstration only
Decision (owner, 2026-09-27): all data, trained models, scores, alerts, DQ reports and evaluation numbers in this
repository come from mock or synthetic data and must not be treated as representative of real company data.
The workflow (code, contract, tests) will be moved to a separate company environment and trained and evaluated
there on a different, real dataset. Mock/synthetic data and models trained on them (`data/**/models/…`) must not
be copied into that environment as production artifacts; models there are trained from scratch on its data.
Reason: synthetic attacks are designed by us and the mock data has no host continuity, so these numbers say
nothing about real detection quality; field meanings are still unverified against the real collector.
Consequence: ADR-009 stays true for this repo (it never holds real traffic). Contract `validated` flags,
thresholds (reject budget, DQ volume band, alert budget) and model settings must be re-established on real data.

## ADR-013: Target use — a tool invoked by a company LLM agent
Decision (owner, 2026-09-27): the longer-term goal is to expose this workflow as a tool that a company LLM agent
can call alongside other tools/skills for inspecting data. The LLM orchestrates the analysis and presents
findings; the anomaly-detection model does the scoring. The LLM must not produce or alter scores.
Every result returned to the agent carries `model_version` and traceability to source files and `flow_id`s.
Scores are presented as rankings ("anomalous behaviour consistent with …"), never as probabilities or confidence,
unless calibration is later demonstrated on labelled real data; ATT&CK mappings remain hypotheses.
Status: goal only. No tool interface, service, or agent integration is designed or implemented yet.

## ADR-014: Joblib is the current development artifact format, not a deployment decision
Decision: keep `joblib.dump`/`joblib.load` of the scikit-learn estimator plus `manifest.json` for development.
Reason: simplest option; the model is small (~2.5 MB) and trained and scored by the same code and lock file.
Known limits: pickle loading executes code (trusted artifacts only); scikit-learn pickles are version-sensitive;
the preprocessing transform lives in code, not in the artifact; the manifest lacks numpy/joblib versions,
`window_minutes`, contract version, code commit and training-data lineage.
Revisit when: designing the company-environment deployment — choose the format there (e.g. keep joblib with pinned
versions and integrity checks, or a non-pickle format) and extend the manifest accordingly.
