# Project Status

_Last updated: 2026-09-28_

## Current milestone
**Parquet-only PoC (ADR-024…030): implemented and tested on fixtures and the committed synthetic demo data; not yet
run on real data.** Scope changed on 2026-09-28 from the V0–V8 roadmap to a focused proof of concept: explore about a
year of NetFlow Parquet, rank unusual host-windows with Isolation Forest and One-Class SVM trained on a chosen
chronological baseline, and compare the rankings with broad pentest date ranges in a visual report. The V0–V3
lake pipeline remains working and tested but is legacy (its status is archived below).

## PoC — implemented (commit 7ac7dc2 on `feat/v3-1-time-split`)
- `src/netanomaly/poc/` + `netanomaly poc profile|features|train|score|report|experiment|search --config <yaml>`
  (`poc.example.yaml`, `annotations.example.yaml`). Design: ARCHITECTURE.md (PoC part); decisions ADR-024…030;
  paper influence RESEARCH.md; features FEATURES.md (generated).
- Parquet read in place with an explicit, type-checked field mapping (contract raw names by default), traceability
  to source file + 0-based row; refuses in-repo inputs, work dir or spill dir unless `allow_inside_repo: true`.
- Host x window features (12, window-local), chronological train/validation/test (explicit dates or fractions),
  train-only imputer/scaler/screening, bounded day-spread training samples, memory guards; IF and OCSVM pipelines
  with raw = -score_samples (higher = more anomalous); bands from validation quantiles or a daily budget,
  re-bandable without retraining; weak annotations inside/buffer/outside with buffer sensitivity; seed stability,
  contamination variants (with/without annotated windows, trimmed refit), training-host concentration, drift (PSI),
  IF-vs-OCSVM agreement; `beyond_train_range` flag; Parquet outputs, manifest, Markdown + SVG/PNG report.
- Finding while building: scikit-learn's Isolation Forest cannot extrapolate beyond the training range (a held-out
  200-port sweep ranks first for OCSVM, not for IF; test-pinned). Hence the flag and the side-by-side comparison.
- Code review (python-reviewer agent): no CRITICAL/HIGH; the one MEDIUM (unverified joblib loading) fixed with a
  sha256 check before unpickling.

## PoC — data availability and runs
- **No real data was available to this process** (no Parquet outside the repo, no `NETANOMALY_*` variables). No
  real-data run has happened; nothing here says anything about detection on the target network.
- Tests use tiny Parquet fixtures in pytest's temp dir (8 days x 8 hosts + one port sweep).
- Demo run (scratch, not committed; `allow_inside_repo: true`) on the committed synthetic raw Parquet
  `data/synth/raw` (6 days, 300 hosts, 388,749 flows; 15-min windows -> 118,584 rows; train 09-01..03, validation
  09-04, test 09-05..06; a made-up 2-day annotation): profile + experiment + search ~30 s on this laptop. Both models
  put the synthetic port/host scans at the top of the candidate list (with traces to the right source rows); IF vs
  OCSVM Spearman ~0.60, top-200 Jaccard 0.13–0.17 (they disagree a lot); seed stability top-200 Jaccard IF 0.71,
  OCSVM 0.83. These are synthetic, self-designed attacks: a pipeline check, not evidence of performance.

## PoC — open questions / assumptions to resolve with the data owner
- Actual Parquet column names/types and exporter semantics (direction of bytes/packets, timestamp zone, flow
  timeouts, sampling) — `poc profile` output + `input.field_map`; contract remains 0/42 validated.
- Which months are a plausible clean baseline, and the precise pentest ranges (tester IPs would allow real
  per-engagement checks).
- Scale of a real year (rows, hosts, windows): memory/time not yet measured; `profile` estimates feature rows.

## PoC — next concrete step
In the authorised data environment (not in this checkout): run `netanomaly poc profile` on the real Parquet, fix
`input.field_map` / `input.epoch_unit` from its mapping table, choose a likely-clean training period and enter the
pentest ranges, then run `netanomaly poc experiment` and review the report with the data owner (which top
candidates are explainable,
which pentest ranges stand out, where IF and OCSVM disagree). Record the run outside the repo; bring back only
code changes and non-sensitive conclusions.

# Legacy lake pipeline (V0–V3) — archived status
The sections below describe the legacy path as of 2026-09-28 and are kept for reference. Their "next task" (V4) is
superseded by ADR-024.

V0 — Prototype: **complete**. V1 — Ingestion and Data Quality: **implementation complete** (V1-1 … V1-5
implemented and tested on mock/synthetic data). **Real-exporter semantic validation: pending** — an external
prerequisite for real-data use, not a V1 task (see below). V2 — Feature research: **implementation complete** (V2-1 … V2-5 done on mock/synthetic data). V3 — Isolation
Forest: **implementation complete** (V3-1 time split + registry inputs, V3-2 stability; mock/synthetic data only).

## Real-data onboarding — external prerequisite (pending)
- Exporter/collector documentation is needed to validate field meanings before any real-data use. It cannot be
  obtained or completed in this mock/synthetic repo (ADR-012). Contract: **0 of 42 fields validated**; no field is
  set to `validated: true` until that documentation confirms it (ADR-002).
- Items waiting on it: field semantics listed under Known issues; export timezone (ADR-015); `flow_sequence`
  uniqueness (ADR-016); promoting DQ checks to reject rules (ADR-011); re-deriving the 5% reject budget and thresholds.

## Deployment goal and data boundary (ADR-012, ADR-013, ADR-014)
- **Everything in this repo is development/demonstration only**: mock and synthetic data, the committed models
  (V0: `data/models/iforest-20260927T191305Z`, mock, and `data/synth/models/iforest-20260927T191244Z`, synthetic —
  both trained on all 6 days incl. attack days, kept for reference, refused by V3 `score`; V3:
  `data/synth/models/iforest-20260928T055319Z`, synthetic, trained on 2026-09-01..03 only), scores, alerts, DQ and
  stability reports and recall numbers. None of it represents real company data.
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

## Completed (V3)
Synthetic results below are diagnostics on self-designed data, **not real-world performance** (ADR-012).
- V3-1 time-based split + registry inputs (ADR-022): `iforest.time_split` = first floor(n_days × `split.train_fraction`
  0.5) UTC days of `host_window` train, only later days are scored; rows are filtered by date **before** a per-row
  hash sample (`hash(src_ip, window_start, seed)`), so later rows, file or thread order cannot change the model. The
  forest is the only learned step (log1p / NaN→0 are stateless). Inputs from the registry (`iforest.model_features`:
  usable, implemented, host_window, window scope): `flows, bytes_out, packets_out, uniq_dst_ip, uniq_dst_port,
  internal_ratio, max_flow_bytes` — V0's `syn_only_ratio` / `rst_ratio` (`tcp_flags`, confidence low) dropped, the
  prior-history features not used yet. Manifest adds `train_dates`, `score_after`, `train_fraction`,
  `train_period_rows`, `registry_version`, `duckdb_version`. V0 model + its scores/alerts preserved
  (`data/synth/outputs/v0/`); V0 manifests load but `score` refuses them (no split, not-usable inputs).
  **Synthetic run** (`data/synth`, committed): train 2026-09-01..03 (111,355 rows, all used; 0 injected windows —
  held-out check in `evaluate`), score 2026-09-04..06 (111,405 rows, 260 injected windows), 300 alerts. The split
  coincides with the generator's 3 clean days; the rule is positional and did not use labels, but real training days
  will not be known clean. recall@50/100: beaconing 0/3, brute force 3/3 (best rank 37), exfil 3/3 (21), horizontal
  3/3 (10), vertical 3/3 (12); @500 beaconing 2/3 (126). V0 @100: beaconing 1/3 (rank 96), others 3/3 at ranks 1–38.
  Ablation (scratch, not committed): V3 split with the 9 V0 features → beaconing 0/3, scans/brute force ranks 1–7,
  exfil 35 — the lost beacon is due to the split (V0 had trained on attack days), the rank drop of scans/brute force
  to dropping the `tcp_flags` features. Top 4 per day are busy benign-looking windows (5–8 flows, 0.4–1.4 MB), not
  injections. Mock lake (scratch copy): 29,999 train / 29,999 scored rows, runs end to end.
- V3-2 stability (ADR-023): `netanomaly stability` → `outputs/stability/stability.{json,md}` (committed for
  `data/synth`, byte-identical on rerun, ~90 s). Label-free, held-out days only; reference = mean of 10 seed models on
  all training rows. **Seed stability**: Spearman rho 0.991 (0.986–0.995) over 45 pairs; top-100 overlap per day
  0.75 median (0.59–0.86), worst day 0.57. **Sample-size curve** vs reference (5 seeds each, median top-100
  overlap / rho): 500 rows 0.77 / 0.979, 1k 0.77 / 0.991, 2k 0.84 / 0.992, 5k 0.85 / 0.992, 10k 0.82 / 0.993,
  20k 0.85 / 0.994, 50k 0.85 / 0.994, all 111,355 0.86 / 0.996. Plateau from ~2k–5k rows: forest randomness, not
  sample size, limits top-K stability. Mock (scratch): seed overlap 0.92, rho 0.987.
- Tests: 21 new (16 in `tests/test_model_split.py`, 5 in `tests/test_stability.py`); `test_synth_pipeline`,
  `test_feature_registry` and the apostrophe-path test in `test_ingest` adapted (the latter needs 2 days now).
  Mutation-checked: training date filter removed, score filter `>` → `>=`, row-order sample, sampling before the
  date filter, Spearman without tie averaging — each fails a test.

## Completed (V2)
- V2-5 feature cards (ADR-021): `src/netanomaly/feature_cards.py` (statistics), `feature_report.py` (rendering),
  `labels.py` (truth verification + labels); `netanomaly feature-cards` → `outputs/feature_cards/feature_cards.{json,md}`
  (committed for `data/synth`); settings `feature_cards:` in config.yaml. **Synthetic diagnostics only — no number
  here is a real-world performance estimate.** Analysed: the 11 usable implemented features (7 V0 + `bytes_out_robust_z`,
  `new_dst_ip_rate`, `new_dst_port_rate`, `interarrival_cv`); not analysed and listed as such: 5 usable candidates
  (not implemented) and 3 not-usable features (`syn_only_ratio`, `rst_ratio` — still V0 model inputs — and
  `mean_packet_length`). Rows = 222,760 host-windows (V0 grid; the 3 V2 tables checked to cover it exactly); ~5 s.
  Truth verified before use: 15 injections, 3,300/3,300 truth flows each match exactly one lake flow, 0 src_ip or
  count mismatches; 260 injected host-windows (beaconing 218, exfil 18, brute force 9, horizontal 9, vertical 6),
  median purity ≥ 84.5% (min 25% for a beacon window, 33% exfil). PSI reference = 2026-09-03 (lake day index 2).
  Findings (synthetic): every feature is stable on the compare days (max PSI 0.0032); warm-up shows as shift
  (`bytes_out_robust_z` PSI 16 on days 1–2 = 100% NULL; `new_dst_ip_rate` 0.42 on day 1). Missingness:
  `bytes_out_robust_z` 33.3% (baseline `none` on days 1–2), `interarrival_cv` 93.5% (93.1% insufficient, 0.4% none),
  others 0%. Redundant (|rho| ≥ 0.9): `bytes_out`–`bytes_out_robust_z` 0.993, –`max_flow_bytes` 0.99, –`packets_out`
  0.977, `flows`–`uniq_dst_ip` 0.969 (+3 pairs among the volume features); novelty and timing features are nearly
  uncorrelated with everything (|rho| ≤ 0.28). Best single-feature AUROC per attack: beaconing `flows` 0.986 /
  `interarrival_cv` 0.983; brute force `flows` 1.0; exfil `bytes_out_robust_z` 1.0 / `bytes_out` 0.999; horizontal
  scan `flows`, `uniq_dst_ip` 1.0, `new_dst_ip_rate` 0.988; vertical scan `flows`, `uniq_dst_port`,
  `new_dst_port_rate` 1.0. `internal_ratio` scores < 0.5 for beaconing/exfil (external destinations): its declared
  direction fits lateral movement only. The high `flows` AUROC for beaconing reflects a loud synthetic beacon (~5
  flows per 5-min window vs a median of 1), not a property to expect in real traffic. Mock lake (scratch copy, not
  committed): no truth → AUROC skipped; `flows`/`uniq_*` have 2 distinct values, `new_dst_ip_rate` is constant
  (rho undefined — this run found and fixed a NaN-in-JSON bug), `interarrival_cv` 100% NULL. Tests: 22 (21 in
  `tests/test_feature_cards.py` — PSI formula/floor/bins/NULL bin, AUROC vs scikit-learn with ties and NULLs,
  per-attack populations, Spearman vs SciPy incl. NULL re-ranking and constant features, feature selection, truth
  refusal cases, window-boundary labels, grid coverage, JSON rounding; 1 end-to-end in `tests/test_synth_pipeline.py`
  checking V0 features and model files are byte-identical). Mutation-checked: bin edge `<` → `<=`, NULLs ranked
  highest, other attacks kept as negatives, NULL rows kept in re-ranking, label bucket shifted by 1 µs, grid check
  disabled — each fails a test.
- V2-4 timing regularity (ADR-020): `src/netanomaly/timing.py`, `netanomaly timing` →
  `features/host_timing/flow_date=…/part-0.parquet`; settings `timing:` in config.yaml. For host-window W,
  `interarrival_cv` uses only the host's flows with `flow_start` in [W − `history_hours` (2 h; 1–24 allowed), W):
  never W's own flows or later. Series = (`src_ip`, `dst_ip`), events = distinct `flow_start` instants (ties = one
  event); gaps between consecutive events inside the history window; a series needs ≥ `min_events` (10); CV =
  stddev_pop / mean; host value = minimum CV (ties → lowest `dst_ip`) with `timing_dst_ip`, `timing_events`,
  `timing_median_gap_s`. `timing_quality` ok / insufficient / none (CV NULL unless ok), `history_complete`,
  `timing_pairs`, `history_pairs`, `history_events`. No subnet fallback. Registry-checked inputs (`src_ip`,
  `dst_ip`, `flow_start`: usable, provisional). Not in `run`, not a model input; V0 features byte-identical in test.
  Synthetic run (`data/synth`, committed): 222,760 rows in ~4 s, same grid as V0 host_window. Per day ~2,100–2,600
  rows `ok`, ~34,500 `insufficient` (no series with 10 events in 2 h), 71–384 `none`; first 2 h of the lake
  `history_complete = false` (1,611 rows). Clean-traffic CV: median ≈ 0.84, minimum 0.29–0.37 per day (regular
  server polling, ~10-min gaps). Each of the 3 injected beacons (60 s ± 10%, 6 h) ranks #1 on its day (min CV
  0.039–0.052, median gap ≈ 60 s); its 76–87 rows are the only rows with CV < 0.2, first ~10–15 min after the
  beacon starts and lasting until ~2 h after it stops (the feature describes the preceding 2 h). Mock lake (scratch
  copy, not committed): no host continuity, 0 `ok` rows (59,958 none, 40 insufficient). Tests: leakage (flows in
  the cutoff window and later, incl. ties with it), tied timestamps, CV ties, shuffled rows/files with reversed
  `flow_sequence`, day-crossing history, window edges; mutation-checked (own window or a future hour leaking,
  no tie dedupe, gap endpoint before the history window each fail tests).
- V2-3 host novelty (ADR-019): `src/netanomaly/novelty.py`, `netanomaly novelty [--rebuild]` →
  `features/host_novelty/flow_date=…/part-0.parquet` + seen set `features/novelty_state/`. A `dst_ip` (`dst_port`)
  is new in a host-window when the host has no flow to it with `flow_start` before the window start (whole lake, no
  lookback) = the window is the pair's first. `new_dst_ip_rate = new_dst_ip / uniq_dst_ip`, `new_dst_port_rate`
  likewise (NULL ports ignored, NULL without ports); counts, `host_first_seen`, `history_days` kept. Ties: flows in
  one window never see each other, so equal timestamps and row order cannot matter; `flow_sequence` unused.
  State: append-only seen set partitioned by first-seen day + `manifest.json` (state version, `window_minutes`,
  per-lake-day fingerprint of file names + sizes). Rerun recomputes from the earliest new/changed/removed day (or
  day with missing files) only; unchanged lake → nothing recomputed; crash → resumes at first unfinished day;
  other `window_minutes` or `--rebuild` → full. Registry-checked inputs (`src_ip`, `flow_start`, `dst_ip`,
  `dst_port`: usable, provisional). Not in `run`, not a model input; V0 features byte-identical in test.
  Synthetic run (`data/synth`, committed): 222,760 rows in 3 s, `uniq_dst_*` equal to V0 on every row; day 1 is
  warm-up (all 300 hosts' first windows; mean new_dst_ip_rate 0.20); days 2–6 mean 0.026–0.031 with ~1,600–1,900
  windows/day containing a new peer (the generator draws new external peers daily); new ports ≈ 0 on clean days.
  Seen set: 300 hosts, 19,645 (host, dst_ip), 2,373 (host, dst_port) keys. Rerun: nothing recomputed.
  Injections: horizontal scans rank #1 per day by `new_dst_ip` (42–45 new peers in a window), vertical scans #1 by
  `new_dst_port` (152–157); brute force, beaconing and exfil add ≤ 1 new peer, as expected. By rate alone 2,985
  windows on days 2–6 tie at 1.0 (99% with a single destination), so the rate needs its counts (feature cards, V2-5).
  Mock lake (scratch copy, not committed): no host continuity, 95–99.6% of rows are a host's first window, rates ≈ 1.
  Leakage test mutation-checked: a "not contacted again later" rule fails it.
- V2-2 host baselines (ADR-018): `src/netanomaly/baselines.py`, `netanomaly baselines` →
  `features/host_baseline/flow_date=…/part-0.parquet`; settings `baseline:` in config.yaml. `bytes_out_robust_z` =
  (ln(1 + bytes_out) − median) / (1.4826 · MAD) over the 7 whole UTC days before the window's day (never the same
  day); fallback host → `src_subnet` peer → global → none, recorded as `baseline_quality` with support counts
  (`baseline_windows/days/hosts`, `host_windows/days`). Inputs checked against the registry first (all usable:
  `src_ip`, `flow_start`, `bytes`, `src_subnet`, high/entity; nothing from `tcp_flags` or `packet_length`).
  Not in `run`, not a model input; V0 model and features unchanged (byte-identical in test).
  Synthetic run (`data/synth`, 6 days, 300 hosts): 222,760 rows, `bytes_out` equal to V0 host_window on every row;
  days 1–2 `none` (< 2 earlier days), days 3–6 `host` on every row (persistent hosts, so the fallback never
  triggers there); z p50 ≈ 0, p99 ≈ 1.8. The 3 exfil_burst injections rank #1 by z on their day (max z 4.9–5.1);
  scans, brute force and beaconing are not volume anomalies (max z −0.8…1.8), as expected for a bytes feature.
  Mock lake (scratch copy, not committed): no host continuity, so no `host` rows; days 3–6 fall back to `peer`
  (27 → 2,479 rows/day) or `global`. Leakage tests were mutation-checked: letting day D into its own history, or
  future rows into the peer history only, fails them.
- V2-1 feature registry (ADR-017): `src/netanomaly/contracts/features_v1.yaml` (registry_version 1, bound to
  `netflow_v1` schema version 1) + `src/netanomaly/feature_registry.py`; generated `FEATURES.md`
  (`netanomaly feature-doc`, drift-tested). 19 features: the 9 implemented V0 host-window features and 10
  candidates (flow-level `bytes_per_packet`, `flow_duration`; host-window `icmp_echo_ratio`, `dns_flow_ratio`,
  `active_timeout_ratio`, `mean_packet_length`; later tasks `bytes_out_robust_z` (V2-2), `new_dst_ip_rate` /
  `new_dst_port_rate` (V2-3), `interarrival_cv` (V2-4)). Eligibility is computed from the contract, never declared:
  **16 usable** (all provisional — 0 rest only on validated fields), **3 not usable**: `syn_only_ratio`,
  `rst_ratio` (`tcp_flags`, confidence low) and `mean_packet_length` (`packet_length`, low + exclude).
  The two V0 features stay in the V0 model (no model change); V3 must drop or replace them.
  Inputs checked against the committed `data/lake` and `data/synth/lake` (none missing) and a fresh synthetic lake
  in tests. Candidates are descriptions only; nothing new is computed.

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
257 passing, 0 failing (41 new for the PoC: 28 in `tests/test_poc_units.py`, 13 in `tests/test_poc_pipeline.py`; legacy counts: 21 new for V3: 16 in `tests/test_model_split.py`, 5 in `tests/test_stability.py`; 22 new for V2-5: 21 in `tests/test_feature_cards.py`, 1 in `tests/test_synth_pipeline.py`; 14 new for V2-4: 13 in `tests/test_timing.py`, 1 in `tests/test_synth_pipeline.py`; 14 for V2-3: 13 in `tests/test_novelty.py`, 1 in `tests/test_synth_pipeline.py`; 15 new for V2-2: 14 in `tests/test_baselines.py`, 1 in `tests/test_synth_pipeline.py`; 22 for V2-1: 21 in `tests/test_feature_registry.py`, 1 lake check in
`tests/test_synth_pipeline.py`; 46 in `tests/test_timestamps.py`; 3 are tiny-scale smoke tests of `scripts/memtest.py`; the 20M-row run is manual). ruff: 3 pre-existing ISC004 findings in `schema.py` (rule new in
ruff 0.16.9; present on HEAD before V1-1); all other files clean.

## Known issues
- **Mock data has no host continuity** (58,311 src IPs in 60k flows; ≤2 flows per host per 5-min window).
  Host-window/baseline features cannot be evaluated on it; use `data/synth` here (real data is only for the
  company environment, ADR-012).
- V0 temporal leakage (training on the scored/attack days) is fixed in V3 (ADR-022); the preserved V0 artifacts
  still have it and are refused for scoring.
- Field semantics unverified: `tcp_flag` (single label, not cumulative), `packet_length`, `time_code`,
  `vlad_id_customer` (66% > 4094), `flow_end_reason` (independent of flags in mock data).
- Beaconing recall is 0/3 at K=100 with V3 (V0's 1/3 came from training on attack days): 5-minute window features
  cannot show periodicity, and the timing feature (`interarrival_cv`, V2-4) is not a model input yet.
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
- V2-1 limits: the model reads the registry since V3; `features.py` still computes all 9 V0 columns incl. the
  not-usable `syn_only_ratio` / `rst_ratio` (kept so V0 artifacts stay reproducible; kept in sync by tests). Usability does
  not require `validated` (0/42), so every usable feature is provisional. ATT&CK entries are unreviewed hypotheses.
  Candidate transforms are prose, not executable, and are not yet checked against data (feature cards, V2-5).
- V2-2 limits: baselines are whole-day, so they lag up to 24 h and the first 2 days of any lake are `none`;
  a within-day level shift is not absorbed until the next day. No hour-of-day or weekday seasonality: diurnal hosts
  score night windows against all-day history (active windows only). Peer group = `src_subnet`, whose meaning is
  unvalidated and may not group similar hosts in real networks; the synthetic subnets partly follow roles, so the
  peer level looks better here than it may be. Fallback is exercised by tests and mock data, not by synthetic
  attacks. Thresholds (7 days, 30 windows, 2 days, 5 hosts) are untuned. Rebuilding after late flows for past
  days changes later baselines (backfill, not leakage); there is no as-of snapshot. Only `bytes_out` is baselined.
- V2-3 limits: rates are 1 in a host's first window and coarse for windows with few destinations (1 of 1 = 1.0);
  no minimum support or smoothing yet. The first days of any lake are warm-up (`history_days`); there is no
  hour/role context, so a host that routinely meets new external peers (web clients) looks as novel as one that
  never does. The seen set never expires and grows with distinct (host, destination) pairs — size on real volumes is
  unmeasured. Change detection uses file names + sizes (a same-name, same-size rewrite needs `--rebuild`); late flows
  recompute every later day. `dst_ip`/`dst_port` meanings are unvalidated (0/42), so the features are provisional.
  NAT/DHCP (one `src_ip` = several machines over time) would blur the seen set; not modelled.
- V2-4 limits: lagging by construction — a beacon is seen after ~`min_events` periods and stays visible up to
  `history_hours` after it stops, so rows after the beacon ends still rank high (V5 incident merging must account
  for it). Periods longer than ~13 min (2 h / 9 gaps) are invisible with defaults; jitter above ~50%, missed
  check-ins or sleep schedules raise the CV. Legitimate periodic traffic (NTP, update/monitoring polling, keep-alives)
  and exporter active-timeout splits of long flows also give low CV; no allow-list or role context. Series merge all
  ports/protocols to one `dst_ip`; NAT/proxy destinations mix many conversations. `min_events` 10 and 2 h are
  untuned; synthetic beacons are clean (fixed 60 s, ±10% uniform jitter), so separation here is optimistic.
  `flow_start` meaning is unvalidated (0/42). Every run recomputes all days (no incremental state); memory is one
  day plus the day before, but the history join replicates each event per active window of the host (≤ 24× at 2 h),
  unmeasured at real volumes.
- V2-5 limits (feature cards): synthetic diagnostics only — few, loud, self-designed attacks (6–18 positive
  windows per type except beaconing), so AUROC shows whether a feature *can* separate them, not real performance;
  single-feature AUROC ignores alert budgets and feature interactions. The label (window contains injected flows)
  penalises lagging prior-history features. PSI uses a one-day positional reference and reads ~0 on clean synthetic
  days by construction; drift on real data needs a rolling/seasonal reference (V4/real-data work). The `any` AUROC is
  dominated by beaconing windows. Directions are declared a priori (`internal_ratio` high is a lateral-movement
  assumption). The analysis table is one TEMP TABLE of the grid (DuckDB spills); cost at real volumes is unmeasured,
  and each pair with a NULL-bearing feature re-ranks its rows (O(features²) sorts). `labels.py` joins truth by
  `flow_sequence` (synthetic key, ADR-016) and is on the production-scan allow list with `alerts.py` and `synth.py`.
- V3 limits (split): one positional split, no rolling/expanding retraining and no gap between training and scoring
  days; the whole-day split assumes the training days are representative (no weekday/seasonality check on 3 days).
  Training data is not known to be clean in real use; contamination only shows in synthetic `evaluate`. A one-day
  lake cannot be trained (`run` refuses). `evaluate` reads the newest model's manifest, which may not be the model
  that wrote `scores.parquet` if models were trained since. Feature set is the 7 window features; redundancy
  (`bytes_out`/`packets_out`/`max_flow_bytes` |rho| ≥ 0.97) left in; prior-history features need a NULL policy first
  (NaN→0 would make a missing `interarrival_cv` look perfectly regular). Scores are rankings within one model version.
  `alerts.write_top_alerts` ranks without a tie-break, so equal scores at the K boundary can pick either window.
- V3 limits (stability): measures ranking agreement, not detection; the synthetic days are near-stationary, so real
  data may be less stable. Only seed and sample size vary (not `n_estimators`, `max_samples`, split point or
  feature set). 3 scored days → the worst-day figure rests on 3 values per model. Cost grows with models × score rows.
- Existing `data/lake` and `data/synth/lake` were built by V0 code; re-ingest is not needed (output is identical)
  and would only add `rejected_rows` to the ledger.

## Important decisions
See `DECISIONS.md` (ADR-001 … ADR-030; ADR-024…030 cover the PoC).

## Next task (legacy, superseded)
V4 evaluation of the legacy pipeline is superseded by ADR-024; see the PoC next step at the top of this file.
