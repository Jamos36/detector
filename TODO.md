# TODO

- [ ] Run a real year once it is available (outside the repository, `config.real.example.yaml`): record runtime,
      peak memory and pair counts per stage; decide the relationship lookbacks (7 d default) and whether
      `include_model_features` is worth an experiment.
- [ ] Review the relationship signals on real data (share of never-seen / recently-unseen pairs per day; the top
      pairs) before feeding them to the models.
- [ ] Incident view: merge adjacent high-ranked windows of a host into episodes in the candidate list.
- [ ] Check the demo final-test report against `data/synth/truth/injections.csv` and write down which injected attack
      types each model finds and misses (beaconing is expected to be missed by window-local features).
- [ ] Pair silence ("a regular pair stopped talking"): needs inactive pair-windows, i.e. a bounded day-level
      expectation per regular pair; not implemented.
- [ ] Per-host drill-down page (all windows of one host around a candidate, with the traced flows and pairs).
- [ ] Decide whether a combined IF + OCSVM ranking is worth testing (ADR-028).
- [ ] EVT/GPD tail fit for band cutoffs if the validation period is short (RESEARCH.md §4).
- [ ] If models should be refitted periodically: a separately named, documented adaptation mode (ADR-032).
