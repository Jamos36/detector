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
3. **Open the report** — the last line printed is the file to open, e.g.
   `outputs/experiments/demo-14ab7a19d5/report.html`. Double-click it (any browser).

That's it. Everything is controlled by one file: **`config.yaml`**.

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
uv run netanomaly score       # score all windows only
uv run netanomaly report      # rebuild bands, tables, charts and the report only
uv run netanomaly search      # compare the model settings listed under `search:` (validation days only)
uv run netanomaly --help
```
Use `--config other.yaml` to run with another config file.

## What's in the output folder

`outputs/experiments/<name>-<id>/` (a new id whenever data, features, split or model settings change):

- `report.html` / `report.md` — the report (charts in `charts/`)
- `scores.parquet` — every host x window: period, pentest context, feature values, score, percentile and band
  per model, `beyond_train_range`
- `alerts.parquet` — windows in a review band, with the host's normal behaviour, the most unusual feature values,
  and the source file + row numbers of the underlying flows
- `daily_summary.parquet`, `entity_summary.parquet`, `diagnostics.json`, `robustness.json`
- `manifest.json` — everything needed to reproduce the run (inputs, settings, seeds, cutoffs, versions)
- `models/` — the fitted models

## How to read the results

- A score is a **rank** (higher = more unusual compared with the training days), not a probability of attack.
- **Critical / High / Medium / Low** are review buckets: e.g. "Critical" = in the top 0.1 % of the validation
  days' scores. **Benign** means "below the review threshold", not "proven safe".
- Pentest date ranges are only context. A window inside one is not a confirmed attack; activity outside them is
  not cleared.
- Isolation Forest cannot rank values far beyond anything seen in training as extreme; the `beyond_train_range`
  column and the One-Class SVM column cover that blind spot. See RESEARCH.md.
- The demo data is synthetic: its "attacks" were injected on 2026-09-04..06 (`data/synth/truth/injections.csv`),
  so you can check whether the report finds them. Results on it say nothing about real networks.

## For developers

```
uv run pytest -q              # tests (about 30 s)
uv run ruff check src tests   # lint
uv run netanomaly docs        # regenerate FEATURES.md and SCHEMA.md
```
Design: ARCHITECTURE.md · decisions: DECISIONS.md · research background: RESEARCH.md · state: PROJECT_STATUS.md.
