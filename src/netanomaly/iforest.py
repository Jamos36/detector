"""Isolation Forest with a time-based split (V3, ADR-022): train on earlier days, score only later days.

The lake's UTC days (host_window `flow_date` partitions) are split by position: the first
`floor(n_days * train_fraction)` days train, every later day is scored. Nothing from the score period reaches
training: rows are filtered by date before sampling, and the sample is a hash of each row's own key, so it does not
depend on which later rows exist. The only learned step is the forest itself; `transform` (log1p, NaN -> 0) is
stateless, so there is no fitted preprocessing that could see score-period data.

The feature set is read from the registry: usable (computed from the contract), implemented, window-scope
host_window features. The V0 inputs `syn_only_ratio` / `rst_ratio` (`tcp_flags`, confidence low) are therefore not
model inputs. V0 artifacts (no split, not-usable inputs) stay on disk for reference but are refused for scoring.

The score is `anomaly_score = -score_samples(x)`: higher = more isolated.
It is a ranking signal, not a probability of maliciousness.
"""

from __future__ import annotations

import json
import math
import platform
from collections.abc import Collection, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import joblib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import sklearn
from sklearn.ensemble import IsolationForest

from netanomaly.config import ModelSettings
from netanomaly.db import sql_literal
from netanomaly.feature_registry import (
    Level,
    ModelTransform,
    Registry,
    Status,
    TemporalScope,
    require_usable,
    usable_features,
)
from netanomaly.features import HOST_WINDOW_FEATURES
from netanomaly.schema import Contract

KEY_COLUMNS = ("src_ip", "window_start", "flow_date")


@dataclass(frozen=True)
class TimeSplit:
    """Whole UTC days: every training day is strictly earlier than every scored day."""

    train_dates: tuple[date, ...]
    score_dates: tuple[date, ...]
    train_fraction: float

    @property
    def train_end(self) -> date:
        return self.train_dates[-1]


@dataclass(frozen=True)
class ModelManifest:
    model_version: str
    features: list[str]
    log1p_features: list[str]
    train_rows: int
    train_period: tuple[str, str]
    settings: dict
    sklearn_version: str
    python_version: str
    created_at: str
    # V3 fields; absent (defaults) in V0 manifests, which is how a V0 artifact is recognised.
    registry_version: int | None = None
    train_fraction: float | None = None
    train_dates: list[str] = field(default_factory=list)
    score_after: str | None = None  # score only rows with flow_date > this UTC day
    train_period_rows: int | None = None  # rows available in the training days (train_rows is the sample)
    duckdb_version: str | None = None  # the sample is a DuckDB hash, so it depends on the DuckDB version


def feature_dates(con: duckdb.DuckDBPyConnection, features_dir: Path) -> list[date]:
    return [d for (d,) in con.execute(
        f"SELECT DISTINCT flow_date FROM read_parquet({sql_literal(_glob(features_dir))}, hive_partitioning = true) "
        "ORDER BY 1").fetchall()]


def time_split(dates: Sequence[date], train_fraction: float) -> TimeSplit:
    """First floor(n * train_fraction) distinct days train, the rest are scored; both sides need >= 1 day."""
    days = sorted(set(dates))
    n_train = math.floor(len(days) * train_fraction)
    if not 1 <= n_train < len(days):
        raise ValueError(f"time split needs >= 1 training and >= 1 later scoring day: {len(days)} day(s) with "
                         f"train_fraction {train_fraction} gives {n_train} training day(s)")
    return TimeSplit(tuple(days[:n_train]), tuple(days[n_train:]), train_fraction)


def model_features(registry: Registry, contract: Contract) -> list[str]:
    """Registry features the model may use, in registry order: usable against the contract, implemented,
    host_window level and window scope (prior-history features are separate tables, not model inputs yet)."""
    usable = set(usable_features(registry, contract, Level.HOST_WINDOW))
    names = [f.name for f in registry.features if f.name in usable and f.status is Status.IMPLEMENTED
             and f.temporal_scope is TemporalScope.WINDOW]
    if missing := [n for n in names if n not in HOST_WINDOW_FEATURES]:
        raise ValueError(f"registry marks {missing} implemented window features, but features.py does not compute them")
    if not names:
        raise ValueError("no usable implemented window features in the registry")
    return names


def log1p_features(registry: Registry, features: Collection[str]) -> list[str]:
    return sorted(f.name for f in registry.features
                  if f.name in features and f.model_transform is ModelTransform.LOG1P)


def check_scorable(manifest: ModelManifest, registry: Registry, contract: Contract) -> None:
    """Refuse artifacts without a time split (V0) or with inputs the registry does not rate usable."""
    if manifest.score_after is None:
        raise ValueError(f"{manifest.model_version} has no time split (V0 artifact, trained on every day); "
                         "run `netanomaly train` for a V3 model")
    require_usable(registry, contract, tuple(manifest.features))


def transform(x: np.ndarray, features: list[str], log1p: Collection[str]) -> np.ndarray:
    out = x.astype(np.float64, copy=True)
    for i, name in enumerate(features):
        if name in log1p:
            out[:, i] = np.log1p(np.clip(out[:, i], 0, None))
    return np.nan_to_num(out, nan=0.0)


def _glob(features_dir: Path) -> str:
    return (features_dir / "**" / "*.parquet").as_posix()


def _train_rows_sql(features_dir: Path, train_end: date) -> str:
    return (f"SELECT * FROM read_parquet({sql_literal(_glob(features_dir))}, hive_partitioning = true) "
            f"WHERE flow_date <= DATE {sql_literal(train_end.isoformat())}")


def training_sample_sql(features_dir: Path, features: list[str], train_end: date, sample_rows: int, seed: int) -> str:
    """Up to `sample_rows` training-period rows chosen by a seeded hash of each row's key, in key order.

    The date filter runs before the sample, and a row's hash depends only on its own key, so rows after
    `train_end` cannot change which training rows are drawn, nor their order.
    """
    cols = ", ".join(features)
    return f"""
SELECT src_ip, window_start, {cols} FROM (
  SELECT src_ip, window_start, {cols} FROM ({_train_rows_sql(features_dir, train_end)})
  ORDER BY hash(src_ip, window_start, {int(seed)}), src_ip, window_start
  LIMIT {int(sample_rows)}
) ORDER BY src_ip, window_start"""


def fit(con: duckdb.DuckDBPyConnection, features_dir: Path, features: list[str], log1p: Collection[str],
        split: TimeSplit, settings: ModelSettings, *, version: str, registry_version: int,
        sample_rows: int | None = None, seed: int | None = None) -> tuple[IsolationForest, ModelManifest]:
    """Fit on training-period rows only; nothing is written. `sample_rows`/`seed` override the settings."""
    sample_rows = settings.train_sample_rows if sample_rows is None else sample_rows
    seed = settings.seed if seed is None else seed
    sample = con.execute(training_sample_sql(features_dir, features, split.train_end, sample_rows, seed)).fetchnumpy()
    x = transform(np.column_stack([sample[f] for f in features]), features, log1p)
    if not len(x):
        raise ValueError(f"no feature rows on or before {split.train_end}")
    model = IsolationForest(n_estimators=settings.n_estimators, max_samples=min(settings.max_samples, len(x)),
                            random_state=seed, n_jobs=settings.n_jobs).fit(x)
    lo, hi, available = con.execute(
        f"SELECT min(window_start)::VARCHAR, max(window_start)::VARCHAR, count(*) "
        f"FROM ({_train_rows_sql(features_dir, split.train_end)})").fetchone()
    manifest = ModelManifest(
        model_version=version, features=list(features), log1p_features=sorted(log1p),
        train_rows=len(x), train_period=(lo, hi),
        settings={**settings.model_dump(), "train_sample_rows": sample_rows, "seed": seed},
        sklearn_version=sklearn.__version__, python_version=platform.python_version(),
        created_at=datetime.now(UTC).isoformat(), registry_version=registry_version,
        train_fraction=split.train_fraction, train_dates=[d.isoformat() for d in split.train_dates],
        score_after=split.train_end.isoformat(), train_period_rows=available, duckdb_version=duckdb.__version__,
    )
    return model, manifest


def train(con: duckdb.DuckDBPyConnection, features_dir: Path, features: list[str], log1p: Collection[str],
          split: TimeSplit, settings: ModelSettings, models_dir: Path,
          registry_version: int) -> tuple[IsolationForest, ModelManifest, Path]:
    version = datetime.now(UTC).strftime("iforest-%Y%m%dT%H%M%SZ")
    model, manifest = fit(con, features_dir, features, log1p, split, settings, version=version,
                          registry_version=registry_version)
    out = models_dir / version
    out.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, out / "model.joblib")
    (out / "manifest.json").write_text(json.dumps(asdict(manifest), indent=2), encoding="utf-8")
    return model, manifest, out


def score(con: duckdb.DuckDBPyConnection, features_dir: Path, model: IsolationForest, manifest: ModelManifest,
          out_path: Path, batch_rows: int) -> int:
    """Score only rows after the training days, streamed in Arrow batches (memory bounded by batch_rows).

    Each row's score depends only on its own features and the fitted model, so adding later rows cannot change
    an earlier score.
    """
    if manifest.score_after is None:
        raise ValueError(f"{manifest.model_version} has no time split; refusing to score (V0 artifact)")
    features = manifest.features
    select = ", ".join((*KEY_COLUMNS, *features))
    reader = con.execute(
        f"SELECT {select} FROM read_parquet({sql_literal(_glob(features_dir))}, hive_partitioning = true) "
        f"WHERE flow_date > DATE {sql_literal(manifest.score_after)}"
    ).to_arrow_reader(batch_rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.unlink(missing_ok=True)  # never leave an earlier model's scores behind
    writer: pq.ParquetWriter | None = None
    total = 0
    try:
        for batch in reader:
            x = transform(np.column_stack([batch.column(f).to_numpy(zero_copy_only=False) for f in features]),
                          features, manifest.log1p_features)
            scores = -model.score_samples(x)
            out = pa.table({**{k: batch.column(k) for k in (*KEY_COLUMNS, *features)},
                            "anomaly_score": pa.array(scores), "model_version": pa.array([manifest.model_version] * len(x))})
            writer = writer or pq.ParquetWriter(out_path, out.schema, compression="zstd")
            writer.write_table(out)
            total += len(x)
    finally:
        if writer is not None:
            writer.close()
    if not total:
        raise ValueError(f"no feature rows after the training days (score_after {manifest.score_after})")
    return total
