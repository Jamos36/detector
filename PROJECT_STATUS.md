# Project Status

_Last updated: 2026-09-27_

## Current milestone
V0 — Prototype: **complete**. V1 — Ingestion and Data Quality: **complete** (V1-1 … V1-5 done). V2 not started.

## Deployment goal and data boundary (ADR-012, ADR-013, ADR-014)
- **Everything in this repo is development/demonstration only**: mock and synthetic data, the two committed models
  (`data/models/iforest-20260927T191305Z`, mock; `data/synth/models/iforest-20260927T191244Z`, synthetic; both V0,
  trained on all 6 days incl. attack days), scores, alerts, DQ reports and recall numbers. None of it represents
  real company data.
- The workflow moves to a separate company environment and is trained/evaluated there on real data. Do not copy
  mock/synthetic data or these models there as production artifacts.
- Goal: a tool a company LLM agent can invoke; the LLM orchestrates and presents, the model scores. Results stay
  traceable to source flows; scores are rankings, not probabilities, unless calibration is demonstrated.
- Model persistence today: joblib pickle of the scikit-learn IsolationForest + `manifest.json`, newest
  `models/iforest-*` loaded by the CLI (details in ARCHITECTURE.md → Model artifacts). Not a final deployment format.

### Open deployment questions (not scheduled)
- Artifact format and integrity in the company environment (joblib + pinned versions + checksums, or non-pickle).
- Manifest completeness: numpy/joblib versions, `window_minutes`, contract version, code commit, training-data lineage.
- How the agent calls the workflow (CLI, Python API or service), with what inputs/outputs, and access control.
- Which existing tools/skills the agent combines it with, and how results are returned (files vs structured data).
- Where outputs and reject/DQ tables live, retention, and who may see raw flow values (IPs) through the agent.
- Real-data prerequisites: collector documentation to validate contract fields; re-deriving thresholds.

## Completed (V1)
- V1-5 `flow_sequence` is not a key (ADR-016). Audit of `src/`, `scripts/` and `tests/`: no production step
  (ingest, DQ, features, scoring, alerts) deduplicates, joins or identifies flows by it; `flow_id` is the key.
  Uses found, all synthetic: the generator assigns it and writes `truth/injected_flows.csv` by it; `recall_at_k`
  joins truth to the lake on it; `test_injection_truth_points_at_injected_flows` does the same; the V1-3 run
  compared its three synthetic lakes by distinct `flow_sequence` (a manual check, not code). Change: `recall_at_k`
  now raises if a truth value matches no lake flow or more than one (e.g. a lake mixing two `generate` runs)
  instead of silently miscounting; a source-scan test keeps it out of production modules; contract meaning
  rewritten (uniqueness in real exports unknown, confidence `low`). `evaluate` on `data/synth` gives the same
  recall as before (e.g. beaconing 1/3, others 3/3 at K=100).
- V1-4 timestamps without offset (ADR-015, `src/netanomaly/timestamps.py`): offset-free values (text, or Parquet
  `TIMESTAMP` without isAdjustedToUTC) are assumed UTC explicitly, independent of the DuckDB session zone; ingested
  rows with them are counted per column in the ledger (`timestamps_without_offset`) and shown as a warning in the DQ
  report (top line + "Timestamps without offset" section, JSON field) and in `ingest`/`dq` logs. Text must match a
  strict ISO-8601 subset (offset at most ±14:00); everything else — including values DuckDB accepted before
  (`+25:00`, `24:00:00`, `infinity`, `EST`, date-only) and integer/DATE Parquet columns — is a `cast_failed` reject.
  Verified on a copy of the mock CSVs (in scratch storage, not committed) with `+00:00` stripped from `time_stamp`
  in file 01 and one invalid `flow_end_time`: the DQ report warns about 9,999 values (100% of that file), the bad row is the only reject,
  and the lake equals the committed `data/lake` except that row. Committed mock/synthetic data all carry
  offsets (0 warnings). Parse cost on 3M text values: 0.58 s vs 0.27 s for the previous `TRY_CAST` check.
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
- V1-3 memory test (`scripts/memtest.py`, 2026-09-27). Synthetic data only (31 days × 3,000 hosts, seed 7, no
  attacks), generated and ingested in temporary storage outside the repo, then deleted. Settings: config.yaml
  `memory_limit` 2GB, `threads` 4; DuckDB 1.5.5, Python 3.13.5, Windows 11, 32 GB RAM (3.6–5.8 GB available at
  start). Peak memory = process peak working set (RSS) and peak commit (private bytes), from the OS.

  | layout | rows | ingest | peak RSS | peak commit | peak DuckDB temp dir | lake |
  |---|---|---|---|---|---|---|
  | A: 31 daily Parquet (0.96 GB) | 19,856,906 | 203 s | 0.81 GB | 1.37 GB | 0 | 1.47 GB |
  | B: 1 Parquet (0.94 GB) | 19,856,906 | 72 s | 2.19 GB | 2.97 GB | 0.31 GB | 1.47 GB |
  | C: 1 CSV (7.66 GB) | 19,856,906 | 796 s | 2.42 GB | 3.06 GB | 15.2 GB | 1.47 GB |

  All three completed with 0 rejects; the three lakes are identical (row count, sum of bytes and packets,
  time range, distinct flow_sequence, 31 dates). Peak temp dir includes the staged CSV copy (1.6 GB).

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
108 passing, 0 failing (3 new for V1-5; 46 in `tests/test_timestamps.py`; 3 are tiny-scale smoke tests of `scripts/memtest.py`; the 20M-row run is manual). ruff: 3 pre-existing ISC004 findings in `schema.py` (rule new in
ruff 0.16.9; present on HEAD before V1-1); all other files clean.

## Known issues
- **Mock data has no host continuity** (58,311 src IPs in 60k flows; ≤2 flows per host per 5-min window).
  Host-window/baseline features cannot be evaluated on it; use `data/synth` here (real data is only for the
  company environment, ADR-012).
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
- V1-3 limits: `memory_limit` caps DuckDB's buffer pool, not the process — peaks reached 1.2x (RSS) to 1.5x
  (commit) of it, so budget ~3 GB of RAM for a 2GB limit. A single large CSV needs a lot of spill disk
  (~14 GB for a 7.7 GB CSV) and runs ~11x slower than the same rows as Parquet; the all-VARCHAR staging and
  row checks (V1-1) are the likely cause, not profiled. One run per layout on one machine with other load;
  timings are indicative. Synthetic data is cleaner and narrower than real exports (no rejects exercised at scale).
  The DQ report (`dq`) was not measured at 20M rows.
- V1-4 limits: the export zone is undocumented, so "assume UTC" may be wrong; if an exporter writes local time,
  flows are shifted by its offset and only the DQ warning signals it (no DST handling). Named IANA zones
  (`Europe/Berlin`) are rejected, not converted. Warning counts cover rows kept in the lake, per file and column;
  there is no per-flow flag in the lake. Counting adds one scan of the source's timestamp columns; the extra
  ingest time at 20M rows (V1-3 layouts) is not re-measured. `flow_date` and `duration_s` still rely on the
  UTC session zone from `db.connect()`.
- Mock/synthetic Parquet stores 18 integer columns as BIGINT where the contract says INTEGER/SMALLINT
  (reported as `width` drift; values are cast on ingest).
- Structural reject types `unquoted_value` / `line_size_over_maximum` / `invalid_state` are mapped but not
  exercised by tests (could not be triggered in probes); row renumbering is tested for extra/missing columns
  and invalid encoding.
- V1-5 limits: uniqueness of `flow_sequence` in real exports is still unknown, and no DQ check measures it
  (e.g. duplicates per exporter/observation domain); it only matters if a future step wants it as a key.
  The source-scan test matches the literal name only.
- Existing `data/lake` and `data/synth/lake` were built by V0 code; re-ingest is not needed (output is identical)
  and would only add `rejected_rows` to the ledger.

## Important decisions
See `DECISIONS.md` (ADR-001 … ADR-016).

## Next task
V1 is complete. Next: review V1 as a whole, then start V2 (feature research, see `TODO.md`) in a new session.
