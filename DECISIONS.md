# Decisions

Settled decisions and why. Reopen one only when its "revisit when" condition occurs.

**Current decisions: ADR-012, ADR-014, ADR-015 and ADR-024 … ADR-031.** ADR-001 … ADR-023 (except 012, 014, 015)
describe the removed V0–V3 lake pipeline and are kept only as history (ADR-031).

## ADR-001: DuckDB over Parquet for all scans and aggregation
Decision: DuckDB computes normalization, features, sampling and joins directly on Parquet; Python sees bounded batches.
Reason: data will exceed free RAM (~2–3 GB free on the dev machine); CPU only.
Rejected: pandas (memory), Spark/Dask (operational complexity at this scale).
Revisit when: single-node DuckDB is a measured bottleneck.

## ADR-002: Schema contract with confidence AND validated
Decision: every raw field has an inferred meaning, a `confidence`, and a separate `validated` flag (collector docs only).
Only high/medium-confidence fields may be features; enforced in code.
Reason: field names are unreliable (`dist_` = destination?, `flow_length` = duration in ms, `time_code` not a timezone).
Revisit when: collector documentation is obtained — then set `validated` per field.

## ADR-003: Content-based file detection; Parquet wins over same-named CSV
Decision: detect Parquet by magic bytes, not extension; if `X.csv` and `X.parquet` both exist, ingest only the Parquet.
Reason: raw folder held both forms of the same data, which would double-count every flow.

## ADR-004: Row-level provenance and deterministic flow_id
Decision: `flow_id` = hash(file hash, data row number); every row keeps source file/row/hash/batch.
Reason: every anomaly must trace back to original records; IDs must be stable across re-runs.

## ADR-005: Crash-safe ingestion (ledger + staged swap)
Decision: `in_progress` ledger entry before writing; write to `lake/_staging`, swap in on success; tolerant ledger reader.
Reason: code review found a crash could brick future runs or delete good output (fixed, regression-tested).

## ADR-006: Synthetic generator with persistent hosts
Decision: build our own generator in the real 42-column schema with roles, diurnal cycles and injected attacks.
Reason: the mock data has no entity continuity, so host features and recall@K cannot be evaluated on it.
Revisit when: real data with persistent hosts is available (then inject attacks into real held-out days).

## ADR-007: Alert budgets, not severity tiers; scores are rankings
Decision: select top-K per day; no Critical/High/Medium labels; never call scores confidence/probability.
Reason: unlabeled data; operational meaning of score levels is unknown.
Status: superseded in part by ADR-027 for the PoC (rank bands named Critical…Low; the rest still holds).

## ADR-008: Temporal model only if evaluation justifies it
Decision: no CNN/TCN until V4 shows Isolation Forest misses temporal patterns; if built, train independently of IF.
Reason: priority is data correctness and measurable detection quality over model complexity.

## ADR-009: `data/` stays tracked; the project uses mock and synthetic data only
Decision (owner, 2026-09-27): `data/` remains committed and is not git-ignored; no history scrub.
Reason: the project will never process real network traffic, so the repo holds only mock and synthetic data
(commit 1449dc1, ~99 MB including generated artifacts).
Revisit when: real traffic is ever introduced — then git-ignore `data/` before it lands in `data/raw`.

## ADR-010: Row-level rejects with a file-level reject budget
Decision: bad rows (malformed CSV record, uncastable value, empty required column) go to a reason-coded reject table
and the rest of the file is ingested; the whole file is quarantined only when rejects exceed `max_reject_fraction` (5%).
CSV is staged as text and typed with `TRY_CAST`, so type checks are identical for CSV and Parquet sources.
Reason: one bad row used to quarantine a whole file; a high reject share signals a wrong export/format, where partial
ingestion would silently bias every downstream feature.
Rejected: DuckDB `ignore_errors` (drops rows without a trace); typed `read_csv` rejects (Parquet sources not covered).
Revisit when: real collector data shows a typical reject rate that makes 5% too tight or too loose.

## ADR-011: Data-quality report is per batch and observational
Decision: `netanomaly dq` reports one ingest batch (checks, nulls, rejects, drift) plus daily volume vs the median
of strictly earlier calendar days (missing days count as 0). It flags; it never blocks ingestion or modelling.
Reason: field meanings are unverified, so a violation may be legitimate traffic or a misread field; a human decides.
Daily volume uses only earlier days, like every other baseline in the project (no temporal leakage).
Revisit when: collector documentation validates the fields behind a check — then that check may become a reject rule.

## ADR-012: Data boundary — this repo is development/demonstration only
Decision (owner, 2026-09-27): all data, trained models, scores, alerts, DQ reports and evaluation numbers in this
repository come from mock or synthetic data and must not be treated as representative of real company data.
The workflow (code, contract, tests) will be moved to a separate company environment and trained and evaluated
there on a different, real dataset. Mock/synthetic data and models trained on them (`data/**/models/…`) must not
be copied into that environment as production artifacts; models there are trained from scratch on its data.
Reason: synthetic attacks are designed by us and the mock data has no host continuity, so these numbers say
nothing about real detection quality; field meanings are still unverified against the real collector.
Consequence: ADR-009 stays true for this repo (it never holds real traffic). Contract `validated` flags,
thresholds (reject budget, DQ volume band, alert budget) and model settings must be re-established on real data.

## ADR-013: Target use — a tool invoked by a company LLM agent
Decision (owner, 2026-09-27): the longer-term goal is to expose this workflow as a tool that a company LLM agent
can call alongside other tools/skills for inspecting data. The LLM orchestrates the analysis and presents
findings; the anomaly-detection model does the scoring. The LLM must not produce or alter scores.
Every result returned to the agent carries `model_version` and traceability to source files and `flow_id`s.
Scores are presented as rankings ("anomalous behaviour consistent with …"), never as probabilities or confidence,
unless calibration is later demonstrated on labelled real data; ATT&CK mappings remain hypotheses.
Status: goal only. No tool interface, service, or agent integration is designed or implemented yet.

## ADR-014: Joblib is the current development artifact format, not a deployment decision
Decision: keep `joblib.dump`/`joblib.load` of the scikit-learn estimator plus `manifest.json` for development.
Reason: simplest option; the model is small (~2.5 MB) and trained and scored by the same code and lock file.
Known limits: pickle loading executes code (trusted artifacts only); scikit-learn pickles are version-sensitive;
the preprocessing transform lives in code, not in the artifact; the manifest lacks numpy/joblib versions,
`window_minutes`, contract version, code commit and training-data lineage.
Revisit when: designing the company-environment deployment — choose the format there (e.g. keep joblib with pinned
versions and integrity checks, or a non-pickle format) and extend the manifest accordingly.

## ADR-015: Timestamps without a UTC offset are assumed UTC, with a data-quality warning
Decision: for the TIMESTAMPTZ contract columns (`flow_start_time`, `flow_end_time`, `time_stamp`):
- text (CSV, or Parquet VARCHAR) must match `YYYY-MM-DD[T ]hh:mm[:ss[.f{1,9}]]` plus an optional zone `Z`, `UTC` or
  `±hh`, `±hhmm`, `±hh:mm` (at most 14:00). With a zone it is converted to UTC. Without one it is assumed UTC,
  explicitly via `timezone('UTC', …)`, so the result never depends on the DuckDB session zone;
- Parquet `TIMESTAMP`/`TIMESTAMP_S/MS/NS` (no isAdjustedToUTC) is treated like offset-free text;
  `TIMESTAMP WITH TIME ZONE` is taken as is;
- anything else (other text, named zones or abbreviations such as `EST`, date-only, `24:00`, `infinity`,
  offsets beyond ±14:00, impossible dates, integer or DATE columns) is invalid: a `cast_failed` row reject.
Offset-free values of ingested rows are counted per file and column in the ledger (`timestamps_without_offset`)
and reported as a warning in the batch DQ report and in the `ingest`/`dq` logs.
Reason: the collector's timezone behaviour is undocumented (`time_code` is not a timezone, ADR-002). Rejecting
offset-free values would drop whole exports that are probably UTC; silently assuming UTC would hide a possible
shift of every flow by the local offset. DuckDB's own parser is too lenient to define validity (it accepts
`+25:00`, `24:00:00` and `infinity`, and ignores unknown zone abbreviations).
Revisit when: collector documentation states the export zone. If exports are local time, convert with that zone
instead of assuming UTC; named IANA zones could then be accepted.

## ADR-016: flow_sequence is not a key; flow_id is the traceability key
Decision: no production step (ingest, DQ, features, scoring, alerts) uses `flow_sequence` to deduplicate, join or
identify flows. Traceability is `flow_id = left(sha256(source_file_hash || ':' || source_row_number), 32)` plus
`source_file`/`source_row_number`. `flow_sequence` stays in the lake as a raw column (contract `provenance`,
confidence `low`) for lookups in exporter tooling. The only use is synthetic recall@K (`alerts.recall_at_k`), where
the generator's truth file names injected flows by `flow_sequence`; it first checks that every truth value matches
exactly one lake flow and raises otherwise (e.g. a lake mixing two `generate` runs or other exports).
Audit (2026-09-27): no production use was found; the only joins were recall@K and its test.
Reason: there is no real exporter data or collector documentation to confirm uniqueness. NetFlow/IPFIX sequence
numbers are per exporter/observation domain and can reset or wrap, and several exporters feed one lake.
Revisit when: collector documentation defines the field. Even then, key on (exporter, domain, sequence) only if the
documentation guarantees uniqueness of that tuple; `flow_id` remains the lake key.

## ADR-017: Feature registry; eligibility is computed from the contract
Decision: candidate features live in a versioned registry (`contracts/features_v1.yaml`) with inputs, level,
transform, temporal scope, rationale and ATT&CK hypotheses. Whether a feature is usable is computed from the
schema contract: every source column (derived columns expanded to their contract sources) must have confidence
high/medium and model_use entity/feature/derive. The registry cannot declare confidence, validated or eligibility.
`validated` is tracked per source and feature but does not gate usability yet; every usable feature is therefore
provisional, and real-data use needs validated sources (real-data onboarding prerequisite).
Consequence: V0 `syn_only_ratio` and `rst_ratio` are **not usable** (derived from `tcp_flags`, confidence low:
the label may be one sampled packet, not the OR of the flow's flags). The V0 model still trains on them; changing
the model's feature set is left to V3, so this task does not alter scores.
Reason: the contract already gated `model_use: feature` columns by confidence, but derived columns (`tcp_*`) let
low-confidence fields reach the model unchecked. Computing eligibility from one source of truth keeps registry
and contract from disagreeing.
Revisit when: collector documentation validates fields (then consider requiring `validated` for usability), or a
feature needs a source rule beyond confidence/model_use.

## ADR-018: Host baselines from whole earlier days, median/MAD, subnet then global fallback
Decision (V2-2): `bytes_out_robust_z` compares `ln(1 + bytes_out)` of a host-window on UTC day D with the median and
MAD of the `lookback_days` (7) whole days before D. The same day is never history. Fallback order host → peer
(`src_subnet`) → global → none; a level needs ≥ 30 windows on ≥ 2 days and MAD > 0 (peer/global: ≥ 5 hosts).
The quality measure is the level used (`baseline_quality`) with its support counts, not a fitted number.
Reason: whole-day history makes the no-leakage rule easy to state and test (a row cannot see its own day), keeps
memory bounded (one day per query) and stops an attack early in a day from lowering later scores that day.
Median/MAD resist the outliers the baseline is meant to expose; the log makes deviations multiplicative.
`src_subnet` is a contract field (entity, high), so peers are observable in real data; synthetic roles are not.
An ordered level with counts is transparent; a single numeric score would need weights we cannot justify.
Rejected: rolling per-window frames (fresher, but quantile window frames over large peer/global partitions are
memory-heavy and the leakage rule is harder to audit); zero-filled inactive windows (changes the question to
"how busy", already covered by `flows`); MAD floor constants (magic numbers; MAD = 0 falls back instead).
Consequences: baselines lag up to 24 h; the first `min_days` days of any lake have `none`; late-arriving flows
for past days change later baselines when rebuilt (backfill, not leakage). Not a V0 model input.
Revisit when: V2-5 feature cards or V4 evaluation show the daily lag, the subnet peer group or the thresholds
hurt detection, or real data shows subnets that do not group similar hosts.

## ADR-019: Novelty = first window of a (host, destination) pair; append-only seen set with day fingerprints
Decision (V2-3): a `dst_ip` (or `dst_port`) is new in host-window W when the host has no flow to it with `flow_start`
before W's start, over the whole lake (no lookback). Equivalently W is the pair's first window. Rates divide by the
window's distinct destinations/ports. The seen set is stored append-only, partitioned by first-seen UTC day, with a
manifest of per-lake-day fingerprints (file names + sizes); a rerun recomputes from the earliest changed day only.
Reason: window granularity makes ties harmless: flows with equal `flow_start` share one window and one history, so
no tie-break rule (and no `flow_sequence`, ADR-016) is needed. "First window of the pair" is order-free and cannot
be moved by later rows. Unlimited history matches "never contacted before" and needs no expiry constant; keeping
history within the same day (unlike ADR-018) is safe because it only uses strictly earlier windows. Partitioning by
first-seen day makes the state append-only, and a late or changed lake day can only affect that day and later ones,
so recomputing from the earliest changed day is exact. Fingerprints are cheap (no data scan).
Rejected: per-flow novelty ordered by `flow_start` (needs a tie-break; same-time flows would see each other);
whole-day history like ADR-018 (a destination first contacted in the morning would still count as new in every
window that day: a 24 h lag with no leakage benefit, since earlier windows are already strictly prior); a lookback/expiry window (a constant with no evidence);
rewriting one state file per day (cost grows with state size every day); content hashing of lake files (full scan).
Consequences: rates are 1 in a host's first window and noisy for windows with few destinations (counts are kept);
the first days of any lake are warm-up (`history_days`); the state grows without bound; a file rewritten with the
same name and size is not detected (use `--rebuild`); late data triggers recomputation of all later days.
Revisit when: V2-5 feature cards show the rate needs smoothing/min-support, state size becomes a problem on real
volumes (expiry or bloom filter), or collector documentation changes the meaning of `dst_ip`/`dst_port`.

## ADR-020: Timing regularity = minimum interarrival CV per destination over the hours before the window
Decision (V2-4): for each host-window W, `interarrival_cv` is computed only from the host's flows with `flow_start`
in [W - `history_hours`, W) (default 2 h, allowed 1–24 h). Series = (`src_ip`, `dst_ip`) with distinct `flow_start`
instants as events; a series needs ≥ `min_events` (10) events; CV = population std / mean of consecutive gaps;
the host value is the minimum CV over its qualifying series, NULL with `timing_quality` insufficient/none otherwise.
Reason: the user requirement is that no flow in the scored window or later may affect it, so the window itself is
excluded, unlike ADR-019 where it is the object being scored. A trailing hour-scale window is what makes periodicity
visible (5-minute windows cannot) and keeps the result fresh (no whole-day lag as in ADR-018). Distinct instants make
ties harmless without a tie-break (ADR-016 forbids `flow_sequence`). CV is scale-free, so beacons with different
periods compare directly; the minimum over destinations surfaces one regular conversation among many irregular
ones. Per-destination series need no peer fallback: another host's flows do not describe this conversation.
Rejected: including W's own flows (explicitly disallowed, and would let a burst inside W move its own score);
per-(dst_ip, dst_port) series (fewer events per series; C2 can use one IP on several ports); robust CV (MAD/median)
or spectral/autocorrelation tests (more parameters, no evidence yet that CV is insufficient); counting tied flows as
zero gaps (duplicate exports would look irregular); whole-day history (24 h lag, as in ADR-018).
Consequences: the feature describes the preceding hours, so it lags a beacon's start by ~`min_events` periods and
stays low up to `history_hours` after it stops; periods above ~`history_hours / (min_events - 1)` (13 min by default)
are invisible; jitter > ~50% or missed check-ins raise the CV; exporter active-timeout splits of long flows and
legitimate polling (NTP, updates, monitoring) are also regular. Every run recomputes all days.
Revisit when: V2-5 feature cards show CV is dominated by legitimate periodic services, beacons with longer periods or
heavy jitter matter, or collector documentation shows `flow_start` is not the flow's first packet time.

## ADR-021: Feature cards — positional PSI reference, window-containment labels, a-priori direction
Decision (V2-5): `netanomaly feature-cards` reports, for every registry feature that is usable and implemented,
distribution, missingness (with the structural reason from its quality column), cardinality, Spearman redundancy,
PSI per day and single-feature AUROC per injected attack type, to `outputs/feature_cards/feature_cards.{json,md}`.
Every other registry feature is listed as not analysed with the reason (not usable, or candidate not implemented).
- PSI reference = lake days [`warmup_days`, `warmup_days + reference_days`) by position (default: day index 2, the
  first day with host baselines, 1 day). Bins = distinct reference deciles + a NULL bin, shares floored at 1e-4.
- Labels: the truth is verified against the lake first (one-to-one `flow_sequence` match, known injection ids,
  consistent `src_ip`, per-injection counts). A host-window is positive for attack A if it contains ≥ 1 injected
  flow of A, negative only if it contains none. A flow-level feature would be labelled by the flow itself.
- AUROC population = host-windows on days with injected flows; per type, other attack types are left out of the
  negatives. The anomalous end of each feature is declared in code before looking at labels; NULL ranks lowest.
- All detection numbers are labelled synthetic diagnostics, never real-world performance estimates (ADR-012).
Reason: a positional reference needs no labels, so the same rule works on unlabelled real data, and skipping the
warm-up keeps the prior-history features' start-up NULLs from being read as drift (they still show as warm-up PSI).
Containment is the only window label that follows directly from flow truth without a tuning constant; excluding
other attacks keeps one attack's windows from counting as another's false positives. Choosing the direction from the
labels (max(AUC, 1 − AUC)) would overstate separation; an AUROC below 0.5 is itself a finding. Restricting to
attack days keeps day-level effects (novelty warm-up on day 1) out of the comparison. The NULL bin makes a change in
coverage visible as instability.
Rejected: label-fitted direction; negatives from all days; a reference picked from known-clean days (uses labels);
lag-shifted labels for prior-history features (a constant per feature with no evidence, and it would hide the lag
the cards should show); sampling for correlations (exact ranks are cheap at this size).
Consequences: prior-history features are penalised for lag (timing: windows before 10 events and up to 2 h after a
beacon); `any` is dominated by the attack with the most windows (beaconing, 218 of 260); PSI on clean synthetic days
is near 0 by construction and says little about real drift; per-type AUROCs rest on 6–18 windows except beaconing.
Revisit when: V4 evaluates recall@K under alert budgets, real data needs a rolling or seasonal reference, or a
flow-level feature is implemented.

## ADR-022: Time-based split by whole UTC days; model inputs from the registry; V0 artifacts kept but not scorable
Decision (V3-1): the lake's host_window days are split by position — the first `floor(n_days * train_fraction)`
(default 0.5) train, only later days are scored (`manifest.score_after` = last training day). Training rows are
filtered by date before sampling; the sample is the lowest `hash(src_ip, window_start, seed)` rows, sorted by key.
The model's inputs are the registry features that are usable against the contract, implemented, host_window and
window-scope (7: the V0 set without `syn_only_ratio`/`rst_ratio`); the log1p set comes from the registry too.
V0 models and outputs stay on disk (`data/synth/outputs/v0/`) but `score` refuses a manifest without a split or with
an input the registry rates not usable.
Reason: V0 trained on and scored the same 6 days including attack days. Whole days keep every window of a day on
one side and match the prior-history features' day granularity; a positional fraction needs no labels and no dataset
dates in the config, so the same rule runs on unlabelled real data. The reservoir sample it replaces depended on row
order, so adding later rows or files could change it; a per-row hash cannot. Reading inputs from the registry makes
ADR-017's eligibility binding rather than a test.
Rejected: a fixed train-end date in config (dataset specific); choosing the training days from the generator's clean
days or the truth (uses labels) — the default happens to coincide with the synthetic clean days (3 of 6), which is
disclosed, and `evaluate` reports injected windows on training days as a held-out check; rolling/expanding
retraining (later, if evaluation asks for it); adding the prior-history features now (they need a NULL policy —
`interarrival_cv` is 93.5% NULL and NaN -> 0 would read as "perfectly regular" — and their own evaluation); keeping
the not-usable features as a V0-compatible option.
Consequences (synthetic diagnostics, not real-world performance): recall@100 changes from V0 beaconing 1/3, others
3/3 to beaconing 0/3, others 3/3. A V3-split model on the 9 V0 features also gives beaconing 0/3 (the V0 beacon hit
was at rank 96, by a model that had trained on the attack days). Dropping `syn_only_ratio`/`rst_ratio` moves scans and
brute force from best ranks 1–7 to 10–37 and exfil from 35 to 21. Training contamination is 0 injected windows here,
but real training days are not known to be clean. A lake needs >= 2 days; `run` refuses a one-day lake.
Revisit when: evaluation needs rolling retraining or a gap between training and scoring, prior-history features get
a NULL policy, or collector documentation changes a field's confidence (the model set follows automatically).

## ADR-023: Stability is label-free ranking agreement on the held-out days against a seed-ensemble reference
Decision (V3-2): `netanomaly stability` trains models with the V3 split and inputs and compares their rankings of the
score days, without labels. Reference = mean score of 10 seed models on the full training sample. Seed stability =
all 45 pairs of those models; sample-size curve = 5 models per size (500 … 50,000 rows plus all training rows,
seeds disjoint from the reference) against the reference. Metrics: Spearman rho over all scored rows (average ranks)
and top-K overlap per day with K = the alert budget (100). Output `outputs/stability/stability.{json,md}`.
Reason: the alert budget is what an analyst sees, so top-K overlap per day measures the stability that matters;
rho covers the whole ranking. Averaging seeds gives a low-noise reference, so the curve shows how close a single
model at size n gets to it; a single seed-42 reference would mix its own noise into every point. Seeds disjoint from
the reference keep the curve from comparing a model with itself. No labels, so the method runs on real data.
Rejected: recall-based stability (uses labels; recall@K across seeds belongs to V4 evaluation); stability over the
training days (not held out); Jaccard instead of overlap share (same order for equal-size sets, harder to read);
changing `n_estimators`, `max_samples` or ensembling in the model now (a model change, left to evaluation).
Findings (synthetic, label-free, not performance): rankings agree globally (seed rho 0.986–0.995), but two single
200-tree forests share only 75% of a day's top-100 (median; 59% worst pair, 57% worst day). The curve reaches
~0.85 overlap with the reference from 2,000–5,000 rows and does not improve with more rows (0.86 at all 111,355): above
a few thousand rows the forest's own randomness, not the sample size, limits top-K stability. Mock lake (scratch
copy): seed overlap 0.92, rho 0.987.
Consequences: the default `train_sample_rows` (200,000) is far past the plateau; top-K membership of a single model
is noticeably seed-dependent, so V4 should report recall@K across seeds and consider more trees or seed averaging.
Revisit when: V4 evaluates recall across seeds, or the model or its inputs change.

## ADR-024: Scope change — a Parquet-only proof of concept for exploring a year of flows around pentest dates
Decision (owner request, 2026-09-28): the active workflow is the PoC in `src/netanomaly/poc/` (`netanomaly poc
profile|features|train|score|report|experiment|search`): profile external Parquet, build host x window features,
fit Isolation Forest and One-Class SVM on a chosen chronological baseline, score later/held-out periods, calibrate
review bands, and produce a visual report that compares rankings with broad pentest date ranges. The V0–V3 lake
pipeline (CSV/Parquet ingest, synthetic generator and injected attacks, truth files, prior-history features, feature
cards, stability) stays in the repository, working and tested, but is legacy: not called by the PoC and not
extended. The V4–V8 roadmap is superseded by the PoC task list in TODO.md.
Reason: the goal changed from building a validated detector step by step on synthetic attacks to getting a first,
inspectable set of candidate periods and hosts from real Parquet with broad, weak annotations. Synthetic attacks
say nothing about that data (ADR-012), so the PoC path must not depend on them.
Consequences: no synthetic generator, injected attacks or truth files in the PoC path (enforced by a source-scan
test); legacy ADRs 010–023 describe the legacy path only. Deletion of legacy code was not needed and was not done.
Revisit when: the PoC's results justify productionising it, or the legacy path is no longer worth its tests.

## ADR-025: Read external Parquet in place through an explicit field mapping; keep the data boundary
Decision: the PoC reads only Parquet (magic bytes checked; CSV is not supported) from paths given in a config, via
DuckDB `read_parquet(union_by_name, filename, file_row_number)`, and never copies flows into a lake. Canonical fields
(`flow_start`, `src_ip` required; `flow_end`, `dst_ip`, `dst_port`, `protocol`, `bytes`, `packets` optional) are
mapped from source columns by `input.field_map`, defaulting to the netflow_v1 contract's raw names. Every field is
type-checked and reported (ok / missing / incompatible) with its conversion; unmapped source columns are listed
and never modelled; timestamps follow ADR-015 (offset-free = assumed UTC, recorded), integer epochs need an explicit
unit. Traceability = (`src_ip`, window) -> `source_file` + 0-based `source_row_index`. Inputs, `work_dir` and the
DuckDB spill directory inside the repository are refused unless `allow_inside_repo: true`, which asserts the data is
mock/synthetic.
Reason: a year of flows should not be duplicated; field meanings are unvalidated (0/42), so the mapping must be an
explicit, reviewable choice; ADR-012 must be enforced by code, not only by documentation.
Consequences: every stage rescans the source for aggregates (the feature table is cached by input fingerprint +
mapping + window + definitions); the fingerprint is paths + sizes + mtimes, not a content hash. ADR-012 and ADR-009
are unchanged: this checkout still holds no real data or real-data artifacts.
Revisit when: exporter documentation validates field meanings, or source files are rewritten in place (then use
content hashes).

## ADR-026: Pentest dates are weak interval annotations; evaluation is chronological and label-free
Decision: supplied ranges (`annotations:` YAML with name, start, end, source, notes, optional confidence; date ends
inclusive) only categorise windows as inside / buffer (± `annotation_buffer_hours`) / outside, for charts and
descriptive comparisons (shares of review-band and daily top-k windows vs base shares, and their sensitivity to the
buffer). They are never training labels. The optional `split.exclude_annotated_from_train` only removes annotated
windows from the baseline. Periods are explicit chronological train / validation / test date ranges (or fractions of
days in time order), never a random row split; learned transforms (imputer, scaler), feature screening and the
models see training rows only; validation calibrates bands; test is scored as a later "deployment" period. A
training period that overlaps a range raises a warning, and contamination variants (with/without annotated
windows, trimmed refit) are reported. No accuracy/precision/recall/FPR is reported.
Reason: the ranges are broad and incomplete, so treating them as labels would be wrong in both directions;
leakage would make any comparison meaningless; a baseline can contain attacks (Kamiguchi & Nishio, RESEARCH.md).
Consequences: results are descriptive; the report says that looking at test results and changing settings makes
later test figures optimistic.
Revisit when: precise, independent labels (tester source IPs and times) become available — then recall@K and
per-engagement hit lists become valid.

## ADR-027: Review bands Critical / High / Medium / Low / Benign are rank bands (supersedes ADR-007 in part)
Decision: per model, band cutoffs are raw-score thresholds at quantiles of the reference period's scores
(validation by default; `quantile` mode, default 0.999 / 0.995 / 0.99 / 0.975) or at quantiles derived from a daily
alert budget (`budget` mode: q = 1 - budget / reference windows per day). Below Low is `below_label` (default
"Benign") = below the review threshold, not safe. `score_pct` = share of reference windows scoring at or below.
Bands can be recalibrated without retraining (`poc report --bands FILE`), which reuses the experiment id and appends
a `band_revisions` entry to the manifest. ADR-007's "no Critical/High/Medium labels" is superseded for the PoC; its
substance stays: scores are rankings, the tiers are alert-volume bands, never probabilities or severities, and every
report says so.
Reason: the owner asked for operational bands; deriving them from a reference quantile or budget keeps them honest
about what they are.
Revisit when: labelled outcomes allow real calibration.

## ADR-028: Two independent baselines — Isolation Forest and One-Class SVM — behind one scoring contract
Decision: each model is a scikit-learn Pipeline: stateless log1p on heavy-tailed features -> median imputer ->
scaler (IF: none, OCSVM: standard; configurable) -> estimator, fitted on a bounded, day-spread hash sample of the
training windows (IF 200k, OCSVM 20k rows by default; OCSVM refuses more than `hard_max_train_rows`; a matrix-size
guard refuses oversized fits with an estimate). `raw = -score_samples(x)` for both, so higher = more anomalous;
`contamination` and `nu` are modelling settings and never feed the bands. Scoring streams Arrow batches. No
ensemble: the report compares the models (Spearman, top-N Jaccard, daily top-k overlap, disagreements). A compact
search compares user-listed parameter candidates on validation only and stores all of them; nothing is chosen
automatically. Artifacts are joblib pipelines (ADR-014) written only to the external work directory.
Finding: scikit-learn's Isolation Forest cannot extrapolate — a window far beyond the training range scores like the
most extreme training windows (test `test_ocsvm_extrapolates_but_isolation_forest_cannot`). Every window therefore
carries `beyond_train_range` (inputs outside the training min/max), and the report counts such windows that are not
in any band.
Reason: two model families with different notions of "unusual" give a useful first comparison; a single-model
ranking would hide the IF extrapolation blind spot. Ensembling needs normalised scores and evidence of benefit.
Revisit when: the comparison shows one model is consistently more useful, or a normalised ensemble is tested.

## ADR-029: Host x window rows with window-local features only
Decision: one row per `src_ip` x fixed UTC window (`window_minutes`, default 60, must divide a day). Twelve features
from the window's own flows (counts, bytes/packets totals and ratios, distinct peers/ports, internal/protocol shares,
mean duration), each computed only if its canonical sources are mapped, with documented NULL handling (FEATURES.md,
generated from `poc/featureset.py`). No history, novelty, periodicity or peer-group features yet. IPs, ports and
file names are trace columns, never numeric inputs.
Reason: window-local features cannot leak across time and need no state; hourly host rows keep a year tractable and
traceable. The legacy prior-history features (ADR-018–020) need a NULL policy and evaluation first.
Consequences: slow, low-volume and periodic (beacon-like) activity is under-represented; stated in every report.
Revisit when: the first real-data review shows which behaviours are missed.

## ADR-030: matplotlib for a static Markdown + SVG report
Decision: add `matplotlib` (the only new dependency) and write `report.md` with SVG (or PNG) charts from data that
DuckDB has already aggregated (per day/week/window bucket, histogram bins, bounded hash samples). No dashboard
framework, no browser-side raw points.
Reason: the report is the main product and must be readable offline and attachable; the project had no plotting
stack; hand-written SVG would be more code to maintain.
Revisit when: interactive exploration is needed beyond static charts.

## ADR-031: Remove everything outside the proof of concept; one-command run with one memory knob
Decision (owner, 2026-09-28): the owner will not receive real data and does not need anything but the PoC. Removed:
the V0–V3 lake pipeline code (ingest, DQ report, synthetic generator and injection, labels, legacy features,
baselines, novelty, timing, feature registry and cards, legacy Isolation Forest, stability, alerts), its tests, the
memory-test script, and its generated artifacts under `data/` (lakes, feature tables, models, outputs). Kept:
the mock data (`data/raw`), the synthetic data and its truth file (`data/synth/raw`, `data/synth/truth`), and the
modules the PoC uses (`db`, `timestamps`, `schema` + the netflow_v1 contract as column dictionary and default
mapping). The PoC package moved from `netanomaly.poc` to `netanomaly`; the CLI is `uv run netanomaly [command]`
(default `run`) with `config.yaml` in the repository pointing at the synthetic demo data (`allow_inside_repo: true`)
and outputs in git-ignored `outputs/`. Memory is set by `memory_gb` (+ `threads`), which derives DuckDB's limit,
training-sample sizes, the matrix guard and the batch size unless set explicitly. The report is also written as
`report.html`; `run.bat` / `run.sh` wrap install + run.
Reason: a non-developer owner must be able to run and adjust it; unused code and artifacts only add confusion.
Consequences: ADR-012 still holds (no real data or real-data artifacts in the repo) and the in-repo guard stays.
The removed code is recoverable from git history (commit 78f2d31 and earlier). New dependency: `markdown` (HTML
report). Supersedes the parts of ADR-024/025 that kept the legacy pipeline and the example-config workflow.
Revisit when: real data becomes available (then move the data and `work_dir` in `config.yaml` outside the
repository).
