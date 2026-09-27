# Project Status

_Last updated: 2026-09-27_

## Current milestone
V0 — Prototype: **complete**. Next: V1 — Ingestion and Data Quality.

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
27 passing, 0 failing; coverage 97%; ruff clean.

## Known issues
- **Mock data has no host continuity** (58,311 src IPs in 60k flows; ≤2 flows per host per 5-min window).
  Host-window/baseline features cannot be evaluated on it; use `data/synth` until real data arrives.
- **V0 model has temporal leakage by design**: it trains on and scores the same 6 days, including attack days.
  Must be fixed in V3 (time-based split, train on clean/earlier periods).
- Field semantics unverified: `tcp_flag` (single label, not cumulative), `packet_length`, `time_code`,
  `vlad_id_customer` (66% > 4094), `flow_end_reason` (independent of flags in mock data).
- Beaconing recall is low (1/3 at K=100): 5-minute windows cannot show periodicity (V2 timing features).
- Synthetic recall numbers are optimistic: attacks are loud and designed by us.
- Row-level rejects not implemented: one unparseable row quarantines the whole file.
- Repo policy: `data/` (mock + synthetic + generated artifacts, ~99 MB) was committed and pushed in 1449dc1;
  `data/` is no longer git-ignored. See ADR-009 / TODO.

## Important decisions
See `DECISIONS.md` (ADR-001 … ADR-009).

## Next task
V1-1: row-level rejects (reason-coded reject table) so bad rows no longer quarantine whole files.
Then V1-2 data-quality report, V1-3 20M-row memory test (see `TODO.md`).
