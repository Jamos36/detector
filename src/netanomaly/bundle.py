"""Frozen model bundle: everything needed to score later Parquet data exactly as the development experiment decided,
without fitting or tuning anything on that data.

Layout (`<work_dir>/bundles/<bundle_id>/`, outside the checkout like every other output):

    bundle.json              id, versions, code commit, config snapshot, field mapping, window, ORDERED model inputs,
                             model settings, training / validation / reserved test periods, band settings, validation
                             cutoffs and percentile grids, relationship settings and history-state bounds
    models/<model>.joblib    fitted pipelines (log1p -> imputer -> scaler -> estimator) + <model>.json with sha256
    train_stats.parquet      one row: per-feature training min/max, median and robust scale
    train_entities.parquet   per host: training window count and medians (context in alert tables)
    relationships/state/     (optional) the pair history that later periods continue from

No raw flow rows are stored. `train_entities` and the relationship state are derived aggregates that contain IP
addresses: keep bundles outside the repository and treat them like the data they came from. Loading a bundle
unpickles code; the sha256 recorded at save time must match (models.load_model), but only load bundles you produced.

The bundle id is the experiment id plus a hash of the band cutoffs and relationship settings, so re-banding or a
different relationship configuration is a different, separately traceable bundle.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from netanomaly.bands import Cutoffs
from netanomaly.featureset import FEATURE_BY_NAME, RELATIONSHIP_FEATURES
from netanomaly.models import FittedModel, load_model
from netanomaly.relationships import RelationshipResult, RelParams
from netanomaly.source import FIELD_BY_NAME, Source
from netanomaly.splits import Period

BUNDLE_VERSION = 1
BUNDLE_JSON = "bundle.json"
REL_NAMES = {f.name for f in RELATIONSHIP_FEATURES}


class BundleError(ValueError):
    pass


def make_bundle_id(experiment_id: str, cutoffs: list[Cutoffs], rel: dict) -> str:
    blob = json.dumps({"cutoffs": [c.to_dict() for c in cutoffs], "relationships": rel}, sort_keys=True, default=str)
    return f"{experiment_id}-b{hashlib.sha256(blob.encode()).hexdigest()[:6]}"


def required_fields(model_features: list[str], rel: dict) -> list[str]:
    """Canonical fields the new data must provide: row keys, every model input's sources, the relationship keys."""
    need = {"src_ip", "flow_start"}
    for f in model_features:
        need.update(FEATURE_BY_NAME[f].sources)
    if rel.get("enabled"):
        need.update({"dst_ip", *(("dst_port", "protocol") if rel["params"]["group_by_port_protocol"] else ())})
    return [f for f in FIELD_BY_NAME if f in need]


def save_bundle(out_dir: Path, meta: dict, models_dir: Path, fitted_ids: list[str], train_stats: Path,
                train_entities: Path, rel_result: RelationshipResult | None) -> Path:
    """Copy the frozen pieces into `out_dir` and write bundle.json (`meta` is assembled by the experiment)."""
    shutil.rmtree(out_dir, ignore_errors=True)
    (out_dir / "models").mkdir(parents=True)
    for m in fitted_ids:
        for suffix in (".joblib", ".json"):
            shutil.copy2(models_dir / f"{m}{suffix}", out_dir / "models" / f"{m}{suffix}")
    shutil.copy2(train_stats, out_dir / train_stats.name)
    shutil.copy2(train_entities, out_dir / train_entities.name)
    if rel_result is not None:
        shutil.copytree(rel_result.state, out_dir / "relationships" / "state")
    meta = {**meta, "bundle_version": BUNDLE_VERSION, "created_at": datetime.now(UTC).isoformat()}
    (out_dir / BUNDLE_JSON).write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
    return out_dir


@dataclass
class Bundle:
    path: Path
    meta: dict

    @property
    def bundle_id(self) -> str:
        return self.meta["bundle_id"]

    @property
    def model_ids(self) -> list[str]:
        return [m["model_id"] for m in self.meta["models"]]

    @property
    def model_features(self) -> list[str]:
        return list(self.meta["features"]["model_inputs"])

    @property
    def window_minutes(self) -> int:
        return int(self.meta["window_minutes"])

    @property
    def field_map(self) -> dict[str, str]:
        return dict(self.meta["field_map"])

    @property
    def epoch_unit(self) -> dict:
        return dict(self.meta["epoch_unit"])

    @property
    def cutoffs(self) -> list[Cutoffs]:
        return [Cutoffs(**c) for c in self.meta["bands"]["cutoffs"]]

    @property
    def grids(self) -> dict[str, list[tuple[float, float]]]:
        return {m: [tuple(x) for x in g] for m, g in self.meta["percentile_grids"].items()}

    @property
    def rel(self) -> dict:
        return self.meta["relationship_analysis"]

    @property
    def rel_params(self) -> RelParams | None:
        return RelParams(**self.rel["params"]) if self.rel.get("enabled") else None

    @property
    def rel_state(self) -> Path:
        return self.path / "relationships" / "state"

    @property
    def train_stats(self) -> Path:
        return self.path / "train_stats.parquet"

    @property
    def train_entities(self) -> Path:
        return self.path / "train_entities.parquet"

    def period(self, name: str) -> Period | None:
        p = self.meta["periods"].get(name)
        return Period(name, date.fromisoformat(p["start"]), date.fromisoformat(p["end_exclusive"])) if p else None

    def load_models(self) -> list[FittedModel]:
        """Load the fitted pipelines (sha256-checked) and confirm each expects exactly the recorded, ordered inputs."""
        fitted = [load_model(self.path / "models", m) for m in self.model_ids]
        for f in fitted:
            n_in = f.pipeline.named_steps["impute"].n_features_in_
            if f.features != self.model_features or n_in != len(f.features):
                raise BundleError(f"{self.path}: model {f.model_id} expects {n_in} inputs {f.features}, the bundle "
                                  f"records {self.model_features}; the bundle is inconsistent, rebuild it")
        return fitted


def load_bundle(path: Path) -> Bundle:
    meta_path = Path(path) / BUNDLE_JSON
    if not meta_path.exists():
        raise BundleError(f"not a model bundle: {path} ({BUNDLE_JSON} missing). Bundles are written by "
                          "`uv run netanomaly` / `report` to <work_dir>/bundles/<bundle_id>/")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if meta.get("bundle_version") != BUNDLE_VERSION:
        raise BundleError(f"{path}: bundle version {meta.get('bundle_version')} is not supported "
                          f"(expected {BUNDLE_VERSION}); rebuild it with this code")
    bundle = Bundle(Path(path), meta)
    missing = [p for p in (bundle.train_stats, bundle.train_entities) if not p.exists()]
    if bundle.rel.get("enabled") and not (bundle.rel_state / "state.json").exists():
        missing.append(bundle.rel_state / "state.json")
    if missing:
        raise BundleError(f"{path}: incomplete bundle, missing {[str(m) for m in missing]}")
    return bundle


def check_compatible(bundle: Bundle, source: Source, computed_features: list[str]) -> list[str]:
    """Refuse new data that cannot be mapped to the bundle's feature contract; return softer warnings.

    Every required canonical field must be read from the SAME source column as in training (never a substitute) and
    be usable; every base model input must be computable. Inputs are never reordered: the models receive exactly
    the bundle's ordered list.
    """
    usable = source.mapping.usable
    by_name = {f.name: f for f in source.mapping.fields}
    trained = {f["name"]: f for f in bundle.meta["field_mapping"]["fields"]}
    problems, warnings = [], []
    for name in bundle.meta["required_fields"]:
        f = by_name.get(name)
        if name not in usable:
            found = f"{f.status} ({f.source_type or 'absent'}; {f.note})" if f else "unknown"
            problems.append(f"{name}: needs column {bundle.field_map.get(name)!r} "
                            f"({trained.get(name, {}).get('source_type')}), found {found}")
        elif usable[name].conversion != trained.get(name, {}).get("conversion"):
            warnings.append(f"{name} <- {usable[name].source}: conversion '{usable[name].conversion}' differs from "
                            f"training ('{trained.get(name, {}).get('conversion')}')")
    missing = [f for f in bundle.model_features if f not in REL_NAMES and f not in computed_features]
    if missing:
        problems.append(f"model inputs {missing} cannot be computed from these files")
    if problems:
        raise BundleError(
            f"the new Parquet files do not match model bundle {bundle.bundle_id}: " + "; ".join(problems) + ". The "
            f"bundle was trained with field_map {bundle.field_map}. Provide files with those columns and types (or "
            "rename the columns), or train a new bundle on data with the new layout. Nothing was scored.")
    return warnings
