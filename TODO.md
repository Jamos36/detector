# TODO

Backlog by roadmap version. Complete and validate each version before starting the next.

## Now
- [ ] Obtain exporter/collector documentation → set `validated` fields in the contract.

## V1 — Ingestion and data quality
- [x] V1-1 Row-level rejects with reason codes (DuckDB `store_rejects`), instead of whole-file quarantine.
- [x] V1-2 Data-quality report per batch: null rates, end<start, ttl_min>ttl_max, bytes/packet > 1514,
      SYN-only flows with >3 packets, ICMP with ports, ingress==egress, VLAN > 4094, daily volume vs trailing median,
      column drift vs contract.
- [ ] V1-3 Memory test: generate ~20M rows, ingest with memory_limit, record peak RSS.
- [ ] V1-4 Timestamps without offset: policy (assume UTC + DQ warning) and test.
- [ ] V1-5 flow_sequence is not unique in real exporters — do not rely on it outside synthetic evaluation.

## V2 — Feature research
- [ ] Feature registry (required columns, level, transform, rationale, ATT&CK hypothesis).
- [ ] Host baselines from strictly prior data (median/MAD) + leakage test; peer-group fallback + baseline_quality.
- [ ] New-destination / new-port rates (persistent seen-set, prior data only).
- [ ] Timing regularity over ≥1 h windows (beaconing).
- [ ] Feature cards: distribution, missingness, cardinality, redundancy, PSI stability, single-feature AUROC on injections.

## V3 — Isolation Forest
- [ ] Time-based train/score split (fix V0 leakage); sample-size stability curve; seed stability.

## V4 — Evaluation
- [ ] Attack intensity sweeps; recall@K 50/100/500; robust-z and rule baselines; top-K Jaccard across seeds.

## V5 — Analyst output
- [ ] Incidents (merge adjacent windows), anomalies.csv + anomaly_flows.csv, report.md/.docx, charts, verdict field.

## Later (only if justified)
- V6 temporal model, V7 multi-detector, V8 productionization.
