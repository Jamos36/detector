# TODO

Backlog by roadmap version. Complete and validate each version before starting the next.

## Real-data onboarding — external prerequisite (not a V1 implementation task)
Cannot be done in this mock/synthetic repo (ADR-012); must be completed before any real-data use.
- [ ] Obtain exporter/collector documentation → validate field meanings; only then set `validated: true`
      per field in the contract (ADR-002) and regenerate `SCHEMA.md`. Currently 0/42 validated.

## V1 — Ingestion and data quality (implementation complete on mock/synthetic data)
- [x] V1-1 Row-level rejects with reason codes (DuckDB `store_rejects`), instead of whole-file quarantine.
- [x] V1-2 Data-quality report per batch: null rates, end<start, ttl_min>ttl_max, bytes/packet > 1514,
      SYN-only flows with >3 packets, ICMP with ports, ingress==egress, VLAN > 4094, daily volume vs trailing median,
      column drift vs contract.
- [x] V1-3 Memory test: generate ~20M rows, ingest with memory_limit, record peak RSS.
- [x] V1-4 Timestamps without offset: policy (assume UTC + DQ warning) and test.
- [x] V1-5 flow_sequence is not unique in real exporters — do not rely on it outside synthetic evaluation (ADR-016).

## V2 — Feature research
- [x] V2-1 Feature registry (required columns, level, transform, rationale, ATT&CK hypothesis) — ADR-017,
      `contracts/features_v1.yaml`, generated `FEATURES.md`.
- [x] V2-2 Host baselines from strictly prior data (median/MAD) + leakage test; peer-group fallback + baseline_quality
      — ADR-018, `baselines.py`, `netanomaly baselines`.
- [x] V2-3 New-destination / new-port rates (persistent seen-set, prior data only) — ADR-019, `novelty.py`,
      `netanomaly novelty`.
- [x] V2-4 Timing regularity over ≥1 h windows (beaconing) — ADR-020, `timing.py`, `netanomaly timing`.
- [ ] Feature cards: distribution, missingness, cardinality, redundancy, PSI stability, single-feature AUROC on injections.

## V3 — Isolation Forest
- [ ] Time-based train/score split (fix V0 leakage); sample-size stability curve; seed stability.
- [ ] Train only on registry-usable features: drop or replace V0 `syn_only_ratio` / `rst_ratio`
      (`tcp_flags`, confidence low; ADR-017) and have the pipeline read the feature set from the registry.

## V4 — Evaluation
- [ ] Attack intensity sweeps; recall@K 50/100/500; robust-z and rule baselines; top-K Jaccard across seeds.

## V5 — Analyst output
- [ ] Incidents (merge adjacent windows), anomalies.csv + anomaly_flows.csv, report.md/.docx, charts, verdict field.

## Later (only if justified)
- V6 temporal model, V7 multi-detector, V8 productionization.
