# Changelog

## Unreleased
- feat (ADR-032): explicit train / validation / holdout-test and new-data workflows. The development experiment no
  longer scores the test period (robustness on validation); `finalize` writes a frozen model bundle
  (`<work_dir>/bundles/<id>`); `netanomaly test` scores the reserved test period once with it (holdout ledger:
  first / repeat / reused) and `netanomaly score-new --bundle --input [--history]` scores new Parquet with it (field
  contract checked first; no fitting). Separate reports: development (training + validation), final test and new
  data, each naming the bundle and data period (`run_report.py`). Training reference summaries moved to
  `train_stats.parquet` / `train_entities.parquet` so scoring needs no training rows. `run` = development + final
  test. Input from outside the checkout may no longer write inside it, even with `allow_inside_repo: true`;
  `config.real.example.yaml` template. POC_VERSION 2.
- feat (ADR-033): optional source -> destination behaviour tracking (`relationship_analysis:`, `relationships.py`,
  `relationship_report.py`): sparse pair x window table with never-seen / recently-unseen destinations and
  frequency change against the pair's own past (median + smoothed log2 ratio, minimum support), strictly past-only,
  host-window summary, evidence with reasons, carried history state; report section with daily/weekly trends and
  ranked traceable pair tables; `include_model_features` adds six `rel_*` summaries to the model inputs (off by
  default). Demo config enables it report-only.
- fix (ADR-034): avoid a DuckDB 1.5.5 internal error on min/max over a single hive partition.
- 65 tests (22 new: relationships, bundle, holdout, new data, config modes, boundary).
- refactor (ADR-031): removed all code, tests and generated data artifacts outside the proof of concept (lake
  pipeline, synthetic generator, legacy features/models, memtest); kept mock/synthetic raw data and truth. Package
  flattened (`netanomaly.poc` -> `netanomaly`); `uv run netanomaly` runs everything with the ready `config.yaml`
  (demo data) and prints the report to open; single `memory_gb` / `threads` knobs derive the memory settings;
  report also as `report.html` (new dependency `markdown`); `run.bat` / `run.sh`; README rewritten as a
  step-by-step guide. 43 tests.
- feat (PoC, ADR-024…030): scope refactor to a Parquet-only proof of concept, `netanomaly poc
  profile|features|train|score|report|experiment|search --config <yaml>` (`src/netanomaly/poc/`). Reads external
  Parquet in place through an explicit, type-checked field mapping with source-file/row traceability; host x window
  features (12, window-local, generated FEATURES.md); chronological train/validation/test with train-only learned
  transforms; Isolation Forest and One-Class SVM with one score contract (higher = more anomalous); Critical / High /
  Medium / Low / Benign review bands from validation quantiles or a daily budget, re-bandable without retraining;
  pentest ranges as weak inside/buffer/outside annotations; label-free diagnostics (distributions, volume vs cutoff,
  concentration, annotation overlap + buffer sensitivity, model agreement, seed stability, contamination variants,
  drift); `beyond_train_range` flag after finding that Isolation Forest cannot extrapolate; Parquet outputs,
  manifest and a Markdown + SVG report. Refuses in-repo inputs/outputs (ADR-012). New dependency: matplotlib.
  RESEARCH.md records what was taken from five papers and one code repository. The V0–V3 lake pipeline is kept,
  working and tested, as legacy; the V4–V8 roadmap is superseded (TODO.md archive). 41 new tests.
- feat (V3-2): `netanomaly stability` → `outputs/stability/stability.{json,md}` (ADR-023). Label-free ranking
  agreement on the held-out score days: seed stability (45 pairs of 10 seeds) and a sample-size curve (5 seeds per size,
  500 … all training rows) against the mean of the seed models; Spearman rho (average ranks) and top-K overlap per day.
  Settings `stability:` in config.yaml. Not part of `run`; writes no model artifacts.
- feat (V3-1): time-based train/score split (ADR-022). `train` fits on the first floor(n_days x `split.train_fraction`)
  (0.5) UTC days of host_window, filtered before a per-row hash sample; `score` writes only later days. Inputs come
  from the registry (usable, implemented, window-scope host_window): 7 features, dropping V0 `syn_only_ratio` /
  `rst_ratio`; log1p set from the registry. Manifest gains the split, registry and DuckDB versions. V0 artifacts kept
  (`data/synth/outputs/v0/`) but refused for scoring. `evaluate` logs injected windows on training vs scored days.
  `train`/`run` now need >= 2 days of features.
- feat (V2-5): feature cards `netanomaly feature-cards` → `outputs/feature_cards/feature_cards.{json,md}` (ADR-021).
  For the 11 usable implemented registry features: distribution, missingness with quality levels, cardinality,
  Spearman redundancy, PSI per day against a positional reference (day index 2), single-feature AUROC per injected
  attack type. Truth-to-lake mapping verified first (`labels.py`); host-window label = window contains an injected
  flow. Unimplemented candidates and not-usable features listed as not analysed. All detection numbers labelled
  synthetic diagnostics. Not part of `run`; V0 features and model unchanged. `alerts._check_truth_join` is now public
  (`check_truth_join`), reused by `labels.verify_truth`.
- feat (V2-4): timing regularity `netanomaly timing` → `features/host_timing/` (ADR-020). `interarrival_cv` = minimum,
  over the host's (`src_ip`, `dst_ip`) series with ≥ 10 distinct `flow_start` instants in the 2 h before the window
  (config `timing:`), of the CV of consecutive gaps; the window's own flows and anything later never count. Tied
  timestamps are one event; order is `flow_start` only. `timing_quality` ok/insufficient/none, `history_complete`
  and support counts. Refuses to run if the registry rates its inputs unusable. Leakage, tie and shuffle tests
  mutation-checked. Not part of `run`; V0 model unchanged. Registry entry now implemented (prior_history).
- feat (V2-3): host novelty `netanomaly novelty [--rebuild]` → `features/host_novelty/` (ADR-019).
  `new_dst_ip_rate` / `new_dst_port_rate` = share of a host-window's distinct destinations / ports that the host
  never used in any earlier window (`flow_start` before the window start); flows in the same window, incl. tied
  timestamps, are never each other's history. Persistent append-only seen set `features/novelty_state/` with a
  per-lake-day fingerprint manifest: reruns recompute only from the earliest changed day, resume after a crash.
  Refuses to run if the registry rates its inputs unusable. Leakage test mutation-checked. Not part of `run`;
  V0 model unchanged. Registry entries now implemented; `feature_registry.require_usable` shared with baselines.
- feat (V2-2): host baselines `netanomaly baselines` → `features/host_baseline/` (ADR-018). `bytes_out_robust_z` =
  robust z of `ln(1 + bytes_out)` against the median/MAD of the 7 whole UTC days before the window's day; fallback
  host → `src_subnet` peer group → global → none, recorded in `baseline_quality` with support counts. Refuses to run
  if the registry rates its inputs unusable. Leakage tests (incl. peer/global fallback) are mutation-checked.
  Not part of `run`; the V0 model is unchanged. Registry entry now implemented, inputs gain `src_subnet`.
- feat (V2-1): versioned feature registry `contracts/features_v1.yaml` (19 features: 9 implemented V0, 10 candidates)
  with inputs, level, transform, temporal scope, rationale and ATT&CK hypotheses. Eligibility is computed from the
  contract (ADR-017): 16 usable (all provisional, 0 on validated fields), 3 not usable — incl. V0 `syn_only_ratio`
  and `rst_ratio` (`tcp_flags`, low confidence), still used by the V0 model until V3. `netanomaly feature-doc`
  generates `FEATURES.md`.
- docs: V1 implementation complete on mock/synthetic data; collector documentation moved out of the V1 checklist
  into a "real-data onboarding — external prerequisite" section (semantic validation pending, 0/42 validated).
- fix (V1-5): `flow_sequence` is not a key (ADR-016). Audit found no production use; synthetic recall@K, the only
  join on it, now raises if a truth `flow_sequence` matches no lake flow or more than one, instead of silently
  miscounting. Contract meaning (confidence lowered to `low`) and SCHEMA.md say uniqueness is unknown.
- feat (V1-4): explicit timestamp policy (ADR-015). Offset-free timestamps (CSV text or naive Parquet `TIMESTAMP`)
  are assumed UTC independent of the DuckDB session zone and counted in the ledger (`timestamps_without_offset`);
  the DQ report and `ingest`/`dq` logs warn about them. Timestamp text must match a strict ISO-8601 subset;
  values DuckDB used to accept (`+25:00`, `24:00:00`, `infinity`, zone abbreviations, date-only) and non-timestamp
  Parquet types are now `cast_failed` rejects.
- test (V1-3): `scripts/memtest.py` memory test. 19.86M synthetic rows ingested under memory_limit 2GB in all three
  layouts (31 daily Parquet, one Parquet, one 7.7 GB CSV); peak working set 0.81 / 2.19 / 2.42 GB. Results in
  PROJECT_STATUS.md.
- docs: data boundary (repo is mock/synthetic, development only), LLM-agent tool goal, and current joblib model
  artifact format with its limits (ADR-012 … ADR-014); open deployment questions in PROJECT_STATUS.md.
- feat (V1-2): per-batch data-quality report `netanomaly dq` → `outputs/dq/dq_<batch>.{json,md}`: plausibility checks,
  null rates, rejects by reason, column drift vs contract, daily volume vs trailing median; also runs in `run`.
- feat (V1-1): row-level rejects. Bad rows go to `lake/rejects/` with reason codes instead of quarantining the file;
  files over `ingest.max_reject_fraction` (5%) are still quarantined. Ledger gains `rejected_rows`.
- docs: durable project memory (CLAUDE.md, PROJECT_STATUS.md, ARCHITECTURE.md, DECISIONS.md, TODO.md, CHANGELOG.md).
- feat: schema contract gains `validated` (all false); `SCHEMA.md` generated by `netanomaly schema-doc`, drift-tested.

## V0 — 2026-09-27 (commit 1449dc1)
- Schema contract and data dictionary for the 42-column NetFlow export.
- Crash-safe, resumable ingestion with row-level provenance into UTC date-partitioned Parquet.
- Synthetic generator with persistent hosts and five injected attack types with ground truth.
- Host × 5-minute features, Isolation Forest (sampled training, batched scoring), top-K/day alerts, recall@K.
- 25 tests, 97% coverage.
