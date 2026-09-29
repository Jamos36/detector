# TODO

- [ ] Incident view: merge adjacent high-ranked windows of a host into episodes in the candidate list.
- [ ] Check the demo report against `data/synth/truth/injections.csv` and write down which injected attack types
      each model finds and misses (beaconing is expected to be missed by window-local features).
- [ ] History-aware features (per-host deviation from earlier days, new peers/ports, interarrival regularity),
      each with a leakage test, to address slow and periodic activity.
- [ ] Per-host drill-down page (all windows of one host around a candidate, with the traced flows).
- [ ] Decide whether a combined IF + OCSVM ranking is worth testing (ADR-028).
- [ ] EVT/GPD tail fit for band cutoffs if the validation period is short (RESEARCH.md §4).
