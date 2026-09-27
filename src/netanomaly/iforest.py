"""Isolation Forest: train on a sample, score in bounded batches, persist a manifest.

The score is `anomaly_score = -score_samples(x)`: higher = more isolated.
It is a ranking signal, not a probability of maliciousness.
"""

from __future__ import annotations

import json
import platform
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
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

KEY_COLUMNS = ("src_ip", "window_start", "flow_date")
# Heavy-tailed counts/volumes get log1p so random splits are not wasted on the tail.
LOG1P_FEATURES = frozenset({"flows", "bytes_out", "packets_out", "uniq_dst_ip", "uniq_dst_port", "max_flow_bytes"})


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


def transform(x: np.ndarray, features: list[str]) -> np.ndarray:
    out = x.astype(np.float64, copy=True)
    for i, name in enumerate(features):
        if name in LOG1P_FEATURES:
            out[:, i] = np.log1p(np.clip(out[:, i], 0, None))
    return np.nan_to_num(out, nan=0.0)


def _glob(features_dir: Path) -> str:
    return (features_dir / "**" / "*.parquet").as_posix()


def train(con: duckdb.DuckDBPyConnection, features_dir: Path, features: list[str], settings: ModelSettings,
          models_dir: Path) -> tuple[IsolationForest, ModelManifest, Path]:
    cols = ", ".join(features)
    sample = con.execute(
        f"SELECT {cols} FROM read_parquet({sql_literal(_glob(features_dir))}) "
        f"USING SAMPLE reservoir({int(settings.train_sample_rows)} ROWS) REPEATABLE ({int(settings.seed)})"
    ).fetchnumpy()
    x = transform(np.column_stack([sample[f] for f in features]), features)
    model = IsolationForest(n_estimators=settings.n_estimators, max_samples=min(settings.max_samples, len(x)),
                            random_state=settings.seed, n_jobs=settings.n_jobs).fit(x)
    lo, hi = con.execute(f"SELECT min(window_start)::VARCHAR, max(window_start)::VARCHAR "
                         f"FROM read_parquet({sql_literal(_glob(features_dir))})").fetchone()
    version = datetime.now(UTC).strftime("iforest-%Y%m%dT%H%M%SZ")
    manifest = ModelManifest(
        model_version=version, features=list(features), log1p_features=sorted(LOG1P_FEATURES & set(features)),
        train_rows=len(x), train_period=(lo, hi), settings=settings.model_dump(),
        sklearn_version=sklearn.__version__, python_version=platform.python_version(),
        created_at=datetime.now(UTC).isoformat(),
    )
    out = models_dir / version
    out.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, out / "model.joblib")
    (out / "manifest.json").write_text(json.dumps(asdict(manifest), indent=2), encoding="utf-8")
    return model, manifest, out


def score(con: duckdb.DuckDBPyConnection, features_dir: Path, model: IsolationForest, manifest: ModelManifest,
          out_path: Path, batch_rows: int) -> int:
    """Stream feature rows in Arrow batches; memory is bounded by batch_rows."""
    features = manifest.features
    select = ", ".join((*KEY_COLUMNS, *features))
    reader = con.execute(
        f"SELECT {select} FROM read_parquet({sql_literal(_glob(features_dir))}, hive_partitioning = true)"
    ).to_arrow_reader(batch_rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    writer: pq.ParquetWriter | None = None
    total = 0
    try:
        for batch in reader:
            x = transform(np.column_stack([batch.column(f).to_numpy(zero_copy_only=False) for f in features]), features)
            scores = -model.score_samples(x)
            out = pa.table({**{k: batch.column(k) for k in (*KEY_COLUMNS, *features)},
                            "anomaly_score": pa.array(scores), "model_version": pa.array([manifest.model_version] * len(x))})
            writer = writer or pq.ParquetWriter(out_path, out.schema, compression="zstd")
            writer.write_table(out)
            total += len(x)
    finally:
        if writer is not None:
            writer.close()
    return total
