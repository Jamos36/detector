# TODO

Actionable work for the Parquet-only PoC (ADR-024). The V1–V8 roadmap is archived at the end.

## External prerequisites (outside this repository)
- [ ] Run `netanomaly poc profile` on the real Parquet in the authorised environment; set `input.field_map` /
      `epoch_unit` from its mapping table; confirm field meanings (bytes/packets direction, timestamp zone, flow
      timeouts, sampling) with the exporter documentation — only then set `validated: true` in the contract
      (ADR-002). Currently 0/42 validated.
- [ ] Agree with the data owner on a likely-clean training period and on the pentest ranges (source, confidence;
      tester source IPs if they can be shared — they would allow per-engagement checks).
- [ ] First real run: `poc experiment`, review the report together; record conclusions outside this repo.

## PoC — next iterations (in order)
- [ ] Measure memory/time on the real year (profile's feature-row estimate vs actual; feature build, scoring,
      OCSVM fit/score at the configured sample sizes); tune `window_minutes`, `max_train_rows`, `batch_rows`.
- [ ] Review the IF-vs-OCSVM disagreements and the `beyond_train_range` windows that are in no band; decide whether
      a normalised combination is worth testing (ADR-028).
- [ ] Use `poc search` on validation to pick parameters; keep the test period untouched while tuning.
- [ ] Incident view: merge adjacent high-ranked windows of a host into episodes for the candidate list.
- [ ] History-aware features once the NULL policy is settled: per-host robust z against earlier days, new peers /
      ports, interarrival regularity (legacy ADR-018–020 designs), each with a leakage test.
- [ ] Per-host drill-down report page (all windows of one host around a candidate, with flows from the trace).
- [ ] If a short reference period makes quantile cutoffs noisy: EVT/GPD tail fit for band cutoffs (RESEARCH.md §4).
- [ ] Artifact format for the company environment (ADR-014): keep joblib + sha256 + pinned versions, or change.

## Archived — legacy roadmap (superseded by ADR-024 on 2026-09-28)
Completed items are kept for history; open items are not planned unless the PoC needs them.

### V1 — Ingestion and data quality (implementation complete on mock/synthetic data)
- [x] V1-1 Row-level rejects with reason codes (DuckDB `store_rejects`), instead of whole-file quarantine.
- [x] V1-2 Data-quality report per batch (null rates, plausibility checks, daily volume, column drift).
- [x] V1-3 Memory test: ~20M rows ingested with memory_limit, peak RSS recorded.
- [x] V1-4 Timestamps without offset: assume UTC + DQ warning (ADR-015).
- [x] V1-5 flow_sequence is not a key (ADR-016).

### V2 — Feature research (complete on mock/synthetic data)
- [x] V2-1 Feature registry (ADR-017); V2-2 host baselines (ADR-018); V2-3 novelty rates (ADR-019);
      V2-4 timing regularity (ADR-020); V2-5 feature cards (ADR-021).

### V3 — Isolation Forest (complete on mock/synthetic data)
- [x] Time-based train/score split (ADR-022); stability curve and seed stability (ADR-023).
- [x] Registry-usable model inputs only.

### V4–V8 (not started; superseded)
- V4 evaluation on synthetic attacks (intensity sweeps, recall@K across seeds, rule baselines), V5 analyst output,
  V6 temporal model, V7 multi-detector, V8 productionisation — replaced by the PoC items above where still useful.
- Tie-break in legacy `alerts.write_top_alerts` (equal scores at the K boundary) — legacy only.
