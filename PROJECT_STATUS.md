# Project Status

_Last updated: 2026-09-27_

## Current milestone
V0 — Prototype: **complete**. V1 — Ingestion and Data Quality: **in progress** (V1-1, V1-2 done).

## Completed (V1)
- V1-1 row-level rejects: bad rows → `lake/rejects/src_<hash32>.parquet` with reason codes
  (`cast_failed`, `missing_required`, DuckDB CSV structure errors); original row numbers preserved;
  file quarantined only above `ingest.max_reject_fraction` (5%). Ledger records `rejected_rows` (ADR-010).
  Verified: output identical to the V0 lake on all 60k mock rows (CSV and Parquet paths); a corrupted
  mock CSV yields exactly the 5 injected rejects and 9,995 correctly numbered rows.
- V1-2 per-batch data-quality report `netanomaly dq` (also in `run`) → `outputs/dq/dq_<batch>.{json,md}`:
  all TODO checks, null rates, rejects by reason, column drift, daily volume vs trailing median (ADR-011).
  On mock data it independently reproduces the contract's profiling notes (vlan_id_customer > 4094: 65.6%;
  ingress == egress: 935 flows) and flags SYN-only flows with > 3 packets (12.2%) and bytes/packet > 1514 (47 flows).
  Synthetic data: all checks 0, daily volume within 1% of trailing median.

## Completed (V0)
- Schema contract + data dictionary for all 42 raw columns (`SCHEMA.md`, generated); 0/42 validated.
- Ingestion: content-based CSV/Parquet detection, explicit CSV types, UTC date partitions,
  provenance (`flow_id`, `source_file`, `source_row_number`, `source_file_hash`, `ingest_batch_id`),
  hash ledger with resume, file-level quarantine, staged write-then-swap, Parquet preferred over same-named CSV.
- Synthetic generator with persistent hosts (roles, diurnal cycle, consistent flags) + 5 injected attack types
  with ground truth (`data/synth/truth/`).
- V0 host × 5-minute features (9), Isolation Forest (reservoir sample, batched scoring, manifest),
  top-K/day alerts with provenance, recall@K.
- Code review: 2 CRITICAL + 2 HIGH findings fixed with regression tests.

## Tests
56 passing, 0 failing; coverage 97%. ruff: 3 pre-existing ISC004 findings in `schema.py` (rule new in
ruff 0.16.9; present on HEAD before V1-1); all other files clean.

## Known issues
- **Mock data has no host continuity** (58,311 src IPs in 60k flows; ≤2 flows per host per 5-min window).
  Host-window/baseline features cannot be evaluated on it; use `data/synth` until real data arrives.
- **V0 model has temporal leakage by design**: it trains on and scores the same 6 days, including attack days.
  Must be fixed in V3 (time-based split, train on clean/earlier periods).
- Field semantics unverified: `tcp_flag` (single label, not cumulative), `packet_length`, `time_code`,
  `vlad_id_customer` (66% > 4094), `flow_end_reason` (independent of flags in mock data).
- Beaconing recall is low (1/3 at K=100): 5-minute windows cannot show periodicity (V2 timing features).
- Synthetic recall numbers are optimistic: attacks are loud and designed by us.
- The 5% reject budget and the DQ volume band (0.5x–2x, ≥3 days of history) are untested against real exports.
- DQ daily volume counts the whole lake per day, so re-running `dq --batch <old>` after later batches touch
  the same dates shows the current lake, not the lake as it was. Same-stem CSVs skipped in favour of Parquet
  are not written to the ledger, so they do not appear in a batch's file list.
- DQ column drift re-hashes the batch's raw files to find them (the ledger stores names, not paths);
  cost on very large files is not measured yet (V1-3).
- Mock/synthetic Parquet stores 18 integer columns as BIGINT where the contract says INTEGER/SMALLINT
  (reported as `width` drift; values are cast on ingest).
- Structural reject types `unquoted_value` / `line_size_over_maximum` / `invalid_state` are mapped but not
  exercised by tests (could not be triggered in probes); row renumbering is tested for extra/missing columns
  and invalid encoding.
- Existing `data/lake` and `data/synth/lake` were built by V0 code; re-ingest is not needed (output is identical)
  and would only add `rejected_rows` to the ledger.

## Important decisions
See `DECISIONS.md` (ADR-001 … ADR-011).

## Next task
V1-3: 20M-row memory test (generate, ingest + dq under memory_limit, record peak RSS). See `TODO.md`.
