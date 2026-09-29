"""Isolation Forest and One-Class SVM behind one contract.

Each model is a scikit-learn Pipeline fitted on training rows only:
  stateless log1p of heavy-tailed features -> median imputer -> scaler (none/standard/robust/maxabs) -> estimator.
The imputer and scaler are learned transforms, so they only ever see the training sample.

Score convention (both models): `raw_score = -pipeline.score_samples(X)`, so HIGHER = MORE ANOMALOUS.
- IsolationForest.score_samples is the negated average path-length score (higher = more normal).
- OneClassSVM.score_samples is the shifted decision function (higher = more inside the learned region).
Raw scores are rankings within one fitted model; they are not probabilities, and their scales differ between
models, which is why bands and percentiles are calibrated per model on a reference period (bands.py).

`contamination` (IF) and `nu` (OCSVM) are modelling settings. Neither is a measured attack rate, and neither feeds
the bands: sklearn's binary predict() is never used here.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import joblib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, MaxAbsScaler, RobustScaler, StandardScaler
from sklearn.svm import OneClassSVM

from netanomaly.poc.config import IForestSettings, ModelSettings, OCSVMSettings
from netanomaly.poc.featureset import FEATURE_BY_NAME, FeatureTable

KINDS = ("iforest", "ocsvm")
SETTINGS_CLASS = {"iforest": IForestSettings, "ocsvm": OCSVMSettings}
MATRIX_COPIES = 4  # fetched columns + stacked matrix + imputed + scaled copies held during a fit


class ModelError(ValueError):
    pass


@dataclass(frozen=True)
class ModelSpec:
    kind: str  # iforest | ocsvm
    settings: dict  # validated IForestSettings / OCSVMSettings as a dict
    seed: int
    label: str = ""  # model id used in column/file names; defaults to kind

    @property
    def model_id(self) -> str:
        return self.label or self.kind

    @property
    def params_hash(self) -> str:
        blob = json.dumps({"kind": self.kind, "settings": self.settings, "seed": self.seed}, sort_keys=True)
        return hashlib.sha256(blob.encode()).hexdigest()[:10]

    def with_overrides(self, overrides: dict, label: str | None = None, seed: int | None = None) -> ModelSpec:
        merged = SETTINGS_CLASS[self.kind].model_validate({**self.settings, **overrides}).model_dump()
        return ModelSpec(self.kind, merged, self.seed if seed is None else seed,
                         self.label if label is None else label)


def specs_from_config(models: ModelSettings) -> list[ModelSpec]:
    return [ModelSpec(kind, getattr(models, kind).model_dump(), models.seed)
            for kind in KINDS if getattr(models, kind).enabled]


def prepare(x: np.ndarray, log1p_idx: tuple[int, ...] = ()) -> np.ndarray:
    """Stateless: float64 copy, non-finite -> NaN, log1p(max(x, 0)) on the listed columns."""
    out = np.array(x, dtype=np.float64, copy=True)
    out[~np.isfinite(out)] = np.nan
    for i in log1p_idx:
        out[:, i] = np.log1p(np.clip(out[:, i], 0, None))
    return out


def _scaler(name: str):
    return {"none": "passthrough", "standard": StandardScaler(), "robust": RobustScaler(),
            "maxabs": MaxAbsScaler()}[name]


def make_pipeline(spec: ModelSpec, features: list[str], n_train: int, n_jobs: int) -> Pipeline:
    s = spec.settings
    if spec.kind == "iforest":
        max_samples = s["max_samples"]
        if isinstance(max_samples, int):
            max_samples = min(max_samples, n_train)
        estimator = IsolationForest(n_estimators=s["n_estimators"], max_samples=max_samples,
                                    max_features=s["max_features"], contamination=s["contamination"],
                                    random_state=spec.seed, n_jobs=n_jobs)
    elif spec.kind == "ocsvm":
        estimator = OneClassSVM(kernel=s["kernel"], nu=s["nu"], gamma=s["gamma"])
    else:
        raise ModelError(f"unknown model kind {spec.kind!r}")
    log1p_idx = tuple(i for i, f in enumerate(features) if FEATURE_BY_NAME[f].log1p)
    return Pipeline([
        ("prepare", FunctionTransformer(prepare, kw_args={"log1p_idx": log1p_idx})),
        ("impute", SimpleImputer(strategy="median")),
        ("scale", _scaler(s["scaler"])),
        ("model", estimator),
    ])


def raw_scores(pipeline: Pipeline, x: np.ndarray) -> np.ndarray:
    """Higher = more anomalous, for every model kind."""
    return -pipeline.score_samples(x)


# --- training sample ---------------------------------------------------------------------------------------------

def per_day_cap(max_rows: int, n_days: int) -> int:
    return math.ceil(max_rows / max(n_days, 1))


def sample_rule(max_rows: int, n_days: int, seed: int) -> str:
    return (f"training rows ranked per UTC day by hash(src_ip, window_start, seed={seed}); at most "
            f"{per_day_cap(max_rows, n_days)} rows per day (= ceil({max_rows} / {n_days} training days)), then at "
            f"most {max_rows} overall by the same hash. A row's inclusion depends only on its own key and day.")


def training_sample_sql(table: FeatureTable, features: list[str], train_where: str, max_rows: int,
                        n_days: int, seed: int) -> str:
    cols = ", ".join(features)
    h = f"hash(src_ip, window_start, {int(seed)})"
    return f"""
SELECT src_ip, window_start, {cols} FROM (
  SELECT src_ip, window_start, {cols}, h FROM (
    SELECT *, {h} AS h,
      row_number() OVER (PARTITION BY flow_date ORDER BY {h}, src_ip, window_start) AS rn_day
    FROM {table.relation()} WHERE {train_where}
  ) WHERE rn_day <= {per_day_cap(max_rows, n_days)}
  ORDER BY h, src_ip, window_start LIMIT {int(max_rows)}
) ORDER BY src_ip, window_start"""


@dataclass
class FittedModel:
    spec: ModelSpec
    pipeline: Pipeline
    features: list[str]
    train_rows: int
    train_rows_available: int
    sample_rule: str
    fit_seconds: float
    info: dict = field(default_factory=dict)

    @property
    def model_id(self) -> str:
        return self.spec.model_id

    def summary(self) -> dict:
        return {"model_id": self.model_id, "kind": self.spec.kind, "label": self.spec.label,
                "settings": self.spec.settings, "seed": self.spec.seed, "params_hash": self.spec.params_hash,
                "features": self.features, "train_rows": self.train_rows,
                "train_rows_available": self.train_rows_available, "sample_rule": self.sample_rule,
                "fit_seconds": round(self.fit_seconds, 2), "score_direction": "higher = more anomalous",
                **self.info}


def fit_model(con: duckdb.DuckDBPyConnection, table: FeatureTable, features: list[str], train_where: str,
              spec: ModelSpec, *, n_jobs: int, max_matrix_mb: int) -> FittedModel:
    """Fit one model on a bounded, time-spread sample of the training rows (nothing is written)."""
    available, n_days = con.execute(f"SELECT count(*), count(DISTINCT flow_date) FROM {table.relation()} "
                                    f"WHERE {train_where}").fetchone()
    if not available:
        raise ModelError("no feature rows in the training period")
    max_rows = int(spec.settings["max_train_rows"])
    n = min(max_rows, available)
    if spec.kind == "ocsvm" and n > spec.settings["hard_max_train_rows"]:
        raise ModelError(f"One-Class SVM would fit {n:,} rows (> hard_max_train_rows "
                         f"{spec.settings['hard_max_train_rows']:,}); the fit is roughly quadratic in rows. "
                         "Lower models.ocsvm.max_train_rows.")
    est_mb = n * len(features) * 8 * MATRIX_COPIES / 2**20
    if est_mb > max_matrix_mb:
        raise ModelError(f"training matrix estimate {est_mb:,.0f} MiB ({n:,} rows x {len(features)} features x "
                         f"{MATRIX_COPIES} copies) exceeds models.max_matrix_mb={max_matrix_mb}; lower "
                         f"max_train_rows for {spec.kind} or raise the limit if RAM allows.")
    sample = con.execute(training_sample_sql(table, features, train_where, max_rows, n_days, spec.seed)).fetchnumpy()
    x = np.column_stack([np.asarray(sample[f], dtype=np.float64) for f in features])
    start = time.perf_counter()
    pipeline = make_pipeline(spec, features, len(x), n_jobs).fit(x)
    fitted = FittedModel(spec, pipeline, list(features), len(x), available,
                         sample_rule(max_rows, n_days, spec.seed), time.perf_counter() - start)
    if spec.kind == "ocsvm":
        fitted.info["n_support_vectors"] = int(pipeline.named_steps["model"].support_vectors_.shape[0])
    return fitted


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save_model(fitted: FittedModel, out_dir: Path) -> Path:
    """joblib pickle of the whole pipeline (development format, ADR-014) plus a sidecar JSON with its sha256."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{fitted.model_id}.joblib"
    joblib.dump(fitted.pipeline, path)
    fitted.info["artifact_sha256"] = _sha256(path)
    (out_dir / f"{fitted.model_id}.json").write_text(json.dumps(fitted.summary(), indent=2, default=str),
                                                     encoding="utf-8")
    return path


def load_model(out_dir: Path, model_id: str) -> FittedModel:
    """Load a pipeline saved by `save_model`. Unpickling executes code, so the file's sha256 must match the one
    recorded at save time; this catches substituted or corrupted artifacts, but only load experiments you produced."""
    info = json.loads((out_dir / f"{model_id}.json").read_text(encoding="utf-8"))
    path = out_dir / f"{model_id}.joblib"
    if info.get("artifact_sha256") != _sha256(path):
        raise ModelError(f"{path}: sha256 does not match {model_id}.json; refusing to unpickle it (retrain instead)")
    spec = ModelSpec(info["kind"], info["settings"], info["seed"], info.get("label", ""))
    extra = {k: v for k, v in info.items() if k in ("n_support_vectors", "artifact_sha256")}
    return FittedModel(spec, joblib.load(path), info["features"], info["train_rows"], info["train_rows_available"],
                       info["sample_rule"], info["fit_seconds"], extra)


# --- scoring -----------------------------------------------------------------------------------------------------

def score_to_parquet(con: duckdb.DuckDBPyConnection, table: FeatureTable, models: list[FittedModel],
                     out_path: Path, batch_rows: int, where: str = "TRUE") -> int:
    """Score rows (optionally filtered) with every model in one pass of Arrow batches.

    Output: src_ip, window_start, window_end, flow_date, then `<model_id>_raw` per model, ordered by
    (window_start, src_ip). Memory is bounded by `batch_rows`. A row's score depends only on its own features and
    the fitted model.
    """
    features = sorted({f for m in models for f in m.features}, key=list(FEATURE_BY_NAME).index)
    reader = con.execute(f"SELECT src_ip, window_start, window_end, flow_date, {', '.join(features)} "
                         f"FROM {table.relation()} WHERE {where} ORDER BY window_start, src_ip"
                         ).to_arrow_reader(batch_rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.unlink(missing_ok=True)
    writer: pq.ParquetWriter | None = None
    total = 0
    try:
        for batch in reader:
            cols = {f: np.asarray(batch.column(f).to_numpy(zero_copy_only=False), dtype=np.float64)
                    for f in features}
            data = {k: batch.column(k) for k in ("src_ip", "window_start", "window_end", "flow_date")}
            for m in models:
                x = np.column_stack([cols[f] for f in m.features])
                data[f"{m.model_id}_raw"] = pa.array(raw_scores(m.pipeline, x))
            out = pa.table(data)
            writer = writer or pq.ParquetWriter(out_path, out.schema, compression="zstd")
            writer.write_table(out)
            total += batch.num_rows
    finally:
        if writer is not None:
            writer.close()
    return total
