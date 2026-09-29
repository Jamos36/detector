# netanomaly

Finds unusual network activity in NetFlow data stored as Parquet files. It learns what "normal" traffic looks
like for each host during a training period, then ranks every host and 15-minute window of a later period by how
unusual it is. Two different models do this side by side (Isolation Forest and One-Class SVM). The result is a
report you open in a browser: timelines, the most unusual hosts and windows, and how they line up with pentest
date ranges you supply.

The repository ships with demo data, so it runs out of the box.

## Run it (3 steps)

1. **Install uv** (it installs Python and everything else for you):
   - Windows (PowerShell): `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"`
   - macOS / Linux: `curl -LsSf https://astral.sh/uv/install.sh | sh`
   - Then close and reopen the terminal.
2. **Run** — in this folder, either double-click `run.bat` (Windows), or type:
   ```
   uv run netanomaly
   ```
   The first run downloads the dependencies (a minute or two); the analysis itself takes about 30 seconds on
   the demo data.
3. **Open the reports** — the command prints two files to open (double-click, any browser):
   - the **development report** (training + validation), e.g. `outputs/experiments/demo-81ae12366e/report.html`;
   - the **final test report** (the reserved test days, scored once with the frozen model), e.g.
     `outputs/scoring/holdout-test-demo-81ae12366e-b11a3b7-.../report.html`.

That's it. Everything is controlled by one file: **`config.yaml`**.

## The two ways to use it

**A. Learn from a historical period, then test once** (what `uv run netanomaly` does):

```
uv run netanomaly                  # 1. development: train on split.train, calibrate bands on split.validation,
                                   #    save a frozen "model bundle", 2. final test: score split.test once with it
```

- The models, their preprocessing and the review-band cutoffs are learned only from the training days (models) and
  the validation days (cutoffs). The test days are never used for any of that, and the development report does not
  show them.
- Everything learned is frozen into a **model bundle** folder: `outputs/bundles/<bundle id>/` (models, the exact
  ordered list of model inputs, the column mapping, the window length, the cutoffs, the date ranges, versions).
- The **final test** scores the test days with that bundle. It is meant to be looked at once. Every final test is
  logged in `outputs/holdout_ledger.json`, and its report says whether it is the *first* look, an identical *repeat*,
  or a test period that an earlier bundle already saw (*reused*: if you changed settings after looking at test
  results, the test is no longer independent and its figures are optimistic).
- Step by step instead: `train`, `score`, `report` (development) and `test` (final test, optionally `--bundle DIR`).

**B. Score new Parquet files with a saved bundle** (nothing is re-trained or re-tuned):

```
uv run netanomaly score-new --bundle outputs/bundles/<bundle id> --input D:\new\netflow
```

- `--input` takes files, folders or globs. The files must have the same columns as the training data (the bundle's
  column mapping); if not, the command stops before scoring and says which column is missing or has the wrong type.
- Results go to `outputs/scoring/new-data-<bundle id>-<hash>/` (`report.html`, `scores.parquet`,
  `alerts.parquet`, `run.json`). Every report names the bundle and the data period it scored.
- New-data scores are rankings against the training baseline, not accuracy: there are no labels.
- With relationship tracking on (below), history continues from the bundle; to chain later files after an earlier
  new-data run, add `--history outputs/scoring/<that run>`. Files that start before that history ends are still
  scored by the models, but the (report-only) relationship section is skipped with a warning.

## Track who talks to whom (optional)

`relationship_analysis:` in `config.yaml` follows each directed source -> destination pair over time (per 15-minute
window). It is **on in report-only mode** in the demo config: it adds a report section but does not change the
anomaly scores. Set `enabled: false` to switch it off entirely.

| signal | meaning |
|---|---|
| never seen | the source contacts this destination for the first time since the history began (not judged during the first `warmup_days`) |
| recently unseen | the source contacted it before, but not within the last `recent_lookback_days` |
| frequency increase / decrease | this window's flow count vs the pair's own median over its active windows in the last `baseline_lookback_days`: at least 2^`change_log2_threshold` times more or less (only with `min_support_windows` such windows; otherwise "insufficient history") |

- Only earlier windows are ever used ("past-only"); adding later data never changes earlier results (tested). The
  history is carried in the model bundle, so the final test and new data continue it instead of starting fresh.
- The report shows daily (weekly for long periods) counts of each signal and ranked tables with the actual source,
  destination, window, activity, baseline and the reason. Full tables: `relationships/*.parquet`.
- `include_model_features: true` also feeds six per-host summaries (e.g. how many never-seen destinations) to the
  models. That changes the models (a new experiment id); keep it off until the signals look sensible on your data.
- Demo values are 1 day for the lookbacks (6 days of data). For a year of data use 7 days (as in
  `config.real.example.yaml`).
- Limits: "never seen" means never seen *since the history start*; a pair that goes completely silent is not
  flagged (only active windows are evaluated); `group_by_port_protocol: true` multiplies the number of pairs.

## Real data

Do **not** point `config.yaml` at real data. Use `config.real.example.yaml`: it reads the data folder and the output
folder from two environment variables, keeps `allow_inside_repo: false` and puts DuckDB's temporary files outside the
repository. Real data and everything computed from it (features, bundles, reports, temporary files) must stay
outside this folder; the program refuses paths inside it, and refuses to write inside it whenever the input comes
from outside it.

```
$env:NETFLOW_DATA = "D:\netflow\parquet"; $env:NETANOMALY_WORK = "D:\netanomaly-work"
uv run netanomaly profile --config config.real.example.yaml   # check the column mapping first
uv run netanomaly --config config.real.example.yaml           # development + final test
```

Scale: the code keeps data in DuckDB and Parquet and only moves bounded samples and batches into Python. A **real**
year (~100 files x ~100,000 rows, ~10 million flows) has **not** been run yet. One synthetic stand-in was timed
(2026-09-28, this laptop, `memory_gb: 4`, 4 threads): the demo days replayed 26 times = 156 files, 10.1 million
flows, 156 days, 300 hosts. The whole `uv run netanomaly` (development + final test, relationships on) took about
8 minutes: relationship analysis ~40 s (6.9 million pair-windows), scoring 2.4 million development windows ~3.3 min
(mostly the One-Class SVM), robustness refits ~2 min, final test ~1 min. Real traffic has far more distinct hosts
and pairs than this replay, so expect more time and memory; peak memory was not measured. Time your own run
(`Measure-Command { uv run netanomaly --config config.real.example.yaml }`), watch memory in Task Manager, and lower
`memory_gb` if it runs out of memory.

## Change the memory (RAM) it uses

Open `config.yaml`; the first setting is:

```yaml
memory_gb: 4      # how much RAM the run may use
threads: 4        # CPU cores to use
```

- Crashes with "out of memory" or the computer becomes unresponsive → **lower `memory_gb`** (e.g. 2).
- Plenty of free RAM and it is slow on a big dataset → **raise it** (e.g. 8 or 16).

What `memory_gb` controls (you normally don't touch these; an explicit value in `config.yaml` overrides the
derived one):

| setting | derived from memory_gb | meaning |
|---|---|---|
| `duckdb.memory_limit` | half of `memory_gb` | RAM for the database engine; beyond it, it spills to disk (slower, not a crash) |
| `models.max_matrix_mb` | a quarter of `memory_gb` | largest training table a model may load; bigger fits are refused with a message |
| `models.iforest.max_train_rows` | 50,000 per GB, max 200,000 | training sample of the Isolation Forest |
| `models.ocsvm.max_train_rows` | 5,000 per GB, max 50,000 | training sample of the One-Class SVM (its time grows ~quadratically) |
| `batch_rows` | 25,000 per GB | rows scored at a time |
| `duckdb.threads`, `models.n_jobs` | `threads` | CPU cores |

## Change what it analyses (all in `config.yaml`)

| I want to… | change |
|---|---|
| use other Parquet files | `input.paths` (a folder, file or glob). The other bundled dataset is `data/raw`. |
| my columns have other names | `input.field_map`, e.g. `src_ip: source_address` — run `uv run netanomaly profile` first; `outputs/profile/profile.md` lists every column and what it was mapped to |
| change which days are "normal" / tested | `split.train`, `split.validation`, `split.test` (UTC dates; end is exclusive) |
| mark pentest dates | `annotations.yaml` (context for the charts only; never used to train) |
| more or fewer alerts | `bands.quantiles` or `bands.mode: budget` + `bands.budget_per_day` |
| window length | `window_minutes` (must divide 1440) |
| use fewer features | `features.exclude` (names in FEATURES.md) |

After changing only the bands you can redo the report without retraining:
`uv run netanomaly report --bands mybands.yaml` (a YAML with the same keys as `bands:`).

## Other commands

```
uv run netanomaly profile     # just look at the input: columns, mapping, rows per day, missing values
uv run netanomaly features    # build the feature table only
uv run netanomaly train       # fit the two models only
uv run netanomaly score       # score the training + validation windows only
uv run netanomaly report      # rebuild bands, tables, charts, the development report and the bundle
uv run netanomaly search      # compare the model settings listed under `search:` (validation days only)
uv run netanomaly test        # final test with the frozen bundle (--bundle DIR to pick one)
uv run netanomaly score-new --bundle DIR --input PATH   # score new files with a saved bundle
uv run netanomaly --help
```
Use `--config other.yaml` to run with another config file.

## What's in the output folder

Everything goes under `work_dir` (`outputs/` for the demo; outside the repository for real data):

| folder | what |
|---|---|
| `experiments/<name>-<id>/` | development (training + validation): `report.html`, `scores.parquet`, `alerts.parquet`, summaries, `manifest.json`, `models/`. A new id whenever data, features, split or model settings change. |
| `bundles/<experiment id>-b<hash>/` | the frozen model bundle (`bundle.json`, `models/`, training summaries, relationship history). A new id also when the bands change. |
| `scoring/holdout-test-<bundle id>-<hash>/` | the final test report and its tables (`run.json` says which bundle and period) |
| `scoring/new-data-<bundle id>-<hash>/` | a new-data scoring run |
| `holdout_ledger.json` | every final test run: bundle, test period, first / repeat / reused |
| `features/`, `relationships/`, `profile/`, `tmp/` | caches, the profile and DuckDB temporary files |

In every run folder:

- `report.html` / `report.md` — the report (charts in `charts/`)
- `scores.parquet` — every host x window: period, pentest context, feature values, score, percentile and band
  per model, `beyond_train_range`
- `alerts.parquet` — windows in a review band, with the host's normal behaviour, the most unusual feature values,
  and the source file + row numbers of the underlying flows
- `daily_summary.parquet`, `entity_summary.parquet` (+ `diagnostics.json`, `robustness.json` in development)
- `relationships/` — pair and host tables when relationship tracking is on

## How to read the results

- A score is a **rank** (higher = more unusual compared with the training days), not a probability of attack.
- **Critical / High / Medium / Low** are review buckets: e.g. "Critical" = in the top 0.1 % of the validation
  days' scores. **Benign** means "below the review threshold", not "proven safe".
- Pentest date ranges are only context. A window inside one is not a confirmed attack; activity outside them is
  not cleared.
- Isolation Forest cannot rank values far beyond anything seen in training as extreme; the `beyond_train_range`
  column and the One-Class SVM column cover that blind spot. See RESEARCH.md.
- Three kinds of results, never to be mixed up: the **development report** (training diagnostics; training days are
  in-sample), the **final test report** (the untouched test days, scored once with the frozen bundle) and
  **new-data reports** (files the model never saw). None of them is accuracy, precision or recall.
- The demo data is synthetic: its "attacks" were injected on 2026-09-04..06 (`data/synth/truth/injections.csv`,
  the demo's test period), so you can check whether the final test report finds them. Results on it say nothing
  about real networks.

## For developers

```
uv run pytest -q              # tests (about 30 s)
uv run ruff check src tests   # lint
uv run netanomaly docs        # regenerate FEATURES.md and SCHEMA.md
```
Design: ARCHITECTURE.md · decisions: DECISIONS.md · research background: RESEARCH.md · state: PROJECT_STATUS.md.
