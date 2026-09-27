"""Feature registry (V2-1): candidate features, their inputs, and eligibility computed from the contract.

The registry YAML (`contracts/features_v1.yaml`) records design intent: inputs, level, transform, rationale and
ATT&CK hypotheses. It never declares whether a feature may be used. Eligibility is derived from the schema
contract: a feature is usable only if every source column has high/medium confidence and a model_use other than
exclude/provenance. `validated` (collector documentation) is reported separately and does not gate eligibility,
so a usable feature stays provisional until all of its sources are validated.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from functools import cache
from importlib import resources

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from netanomaly.schema import Column, Confidence, Contract, ModelUse, load_contract

UNUSABLE_CONFIDENCE = frozenset({Confidence.LOW, Confidence.UNKNOWN})
UNUSABLE_MODEL_USE = frozenset({ModelUse.EXCLUDE, ModelUse.PROVENANCE})


class Level(StrEnum):
    FLOW = "flow"
    HOST_WINDOW = "host_window"


class Status(StrEnum):
    IMPLEMENTED = "implemented"
    CANDIDATE = "candidate"


class TemporalScope(StrEnum):
    WINDOW = "window"
    PRIOR_HISTORY = "prior_history"


class ModelTransform(StrEnum):
    NONE = "none"
    LOG1P = "log1p"


class _Strict(BaseModel):
    # extra="forbid": trust fields (confidence, validated, eligible) cannot be hand-set in the registry.
    model_config = ConfigDict(extra="forbid", frozen=True)


class AttackHypothesis(_Strict):
    technique: str = Field(pattern=r"^T\d{4}(\.\d{3})?$")
    name: str


class DerivedColumn(_Strict):
    name: str
    sources: tuple[str, ...] = Field(min_length=1)
    transform: str


class Feature(_Strict):
    name: str
    level: Level
    status: Status
    inputs: tuple[str, ...] = Field(min_length=1)
    transform: str
    rationale: str
    attack_hypotheses: tuple[AttackHypothesis, ...] = ()
    model_transform: ModelTransform = ModelTransform.NONE
    temporal_scope: TemporalScope = TemporalScope.WINDOW
    task: str | None = None

    @model_validator(mode="after")
    def _implemented_features_have_no_pending_task(self) -> Feature:
        if self.status is Status.IMPLEMENTED and self.task:
            raise ValueError(f"{self.name}: implemented features cannot name a pending task")
        return self


class Registry(_Strict):
    registry_version: int
    contract: str
    contract_schema_version: int
    derived_columns: tuple[DerivedColumn, ...] = ()
    features: tuple[Feature, ...]

    @model_validator(mode="after")
    def _names_are_unique(self) -> Registry:
        for kind, names in (("feature", [f.name for f in self.features]),
                            ("derived column", [d.name for d in self.derived_columns])):
            if dupes := sorted({n for n in names if names.count(n) > 1}):
                raise ValueError(f"duplicate {kind} names: {dupes}")
        return self


@dataclass(frozen=True)
class Eligibility:
    feature: Feature
    sources: tuple[Column, ...]
    reasons: tuple[str, ...]

    @property
    def eligible(self) -> bool:
        return not self.reasons

    @property
    def validated(self) -> bool:
        """True only when every source meaning is confirmed by collector documentation."""
        return all(c.validated for c in self.sources)


def resolve_sources(registry: Registry, contract: Contract, feature: Feature) -> tuple[Column, ...]:
    """Contract columns behind a feature's inputs, with derived lake columns expanded to their sources."""
    by_name = {c.name: c for c in contract.columns}
    derived = {d.name: d for d in registry.derived_columns}
    names: list[str] = []
    for inp in feature.inputs:
        for name in derived[inp].sources if inp in derived else (inp,):
            if name not in by_name:
                raise ValueError(f"{feature.name}: input {name!r} is neither a contract column nor a derived column")
            if name not in names:
                names.append(name)
    return tuple(by_name[n] for n in names)


def assess(registry: Registry, contract: Contract) -> list[Eligibility]:
    out = []
    for feature in registry.features:
        sources = resolve_sources(registry, contract, feature)
        reasons = [f"{c.name}: confidence {c.confidence}" for c in sources if c.confidence in UNUSABLE_CONFIDENCE]
        reasons += [f"{c.name}: model_use {c.model_use}" for c in sources if c.model_use in UNUSABLE_MODEL_USE]
        out.append(Eligibility(feature, sources, tuple(reasons)))
    return out


def usable_features(registry: Registry, contract: Contract, level: Level | None = None) -> list[str]:
    """Names of eligible features (optionally one level). Eligible is not validated: see `Eligibility.validated`."""
    return [e.feature.name for e in assess(registry, contract)
            if e.eligible and (level is None or e.feature.level is level)]


def require_usable(registry: Registry, contract: Contract, names: tuple[str, ...]) -> None:
    """Refuse to build features the registry does not rate usable against the contract."""
    usable = set(usable_features(registry, contract))
    if blocked := [f for f in names if f not in usable]:
        raise ValueError(f"features not usable under the contract (see FEATURES.md): {blocked}")


def check_against_contract(registry: Registry, contract: Contract) -> None:
    """Fail fast if the registry was written for another contract or references unknown columns."""
    if (registry.contract, registry.contract_schema_version) != (contract.name, contract.schema_version):
        raise ValueError(f"registry targets {registry.contract} v{registry.contract_schema_version}, "
                         f"contract is {contract.name} v{contract.schema_version}; review the registry")
    contract_names = {c.name for c in contract.columns}
    for d in registry.derived_columns:
        if d.name in contract_names:
            raise ValueError(f"derived column {d.name!r} shadows a contract column")
        if unknown := [s for s in d.sources if s not in contract_names]:
            raise ValueError(f"derived column {d.name!r}: unknown sources {unknown}")
    assess(registry, contract)  # resolves every input


@cache
def load_registry(name: str = "features_v1") -> Registry:
    text = resources.files("netanomaly.contracts").joinpath(f"{name}.yaml").read_text(encoding="utf-8")
    registry = Registry.model_validate(yaml.safe_load(text))
    check_against_contract(registry, load_contract(registry.contract))
    return registry


def to_markdown(registry: Registry, contract: Contract, name: str = "features_v1") -> str:
    """Render FEATURES.md. The YAML registry and the contract are the source of truth; never edit it by hand."""
    def cell(text: str) -> str:
        return " ".join(text.split()).replace("|", "\\|")

    def sources(e: Eligibility) -> str:
        return "<br>".join(f"`{c.name}` ({c.confidence}, {'validated' if c.validated else 'unvalidated'})"
                           for c in e.sources)

    def attack(f: Feature) -> str:
        return "<br>".join(f"{h.technique} {cell(h.name)}" for h in f.attack_hypotheses) or "—"

    def status(f: Feature) -> str:
        return f"{f.status} ({f.task})" if f.task else str(f.status)

    rows = assess(registry, contract)
    usable = [e for e in rows if e.eligible]
    blocked = [e for e in rows if not e.eligible]
    in_use_blocked = [e.feature.name for e in blocked if e.feature.status is Status.IMPLEMENTED]
    lines = [
        f"# Feature registry: {name} (version {registry.registry_version})",
        "",
        (
            f"<!-- GENERATED by `uv run netanomaly feature-doc` from src/netanomaly/contracts/{name}.yaml and the "
            f"{contract.name} contract. Do not edit by hand; tests fail if this file is stale. -->"
        ),
        "",
        (
            f"Contract: `{contract.name}` schema version {contract.schema_version}. "
            f"**{len(usable)} of {len(rows)} registered features are usable**; "
            f"**{sum(e.validated for e in usable)} of {len(usable)} usable features rest only on validated fields.** "
            "Every other usable feature is provisional: its source meanings are inferred, not confirmed by "
            "exporter/collector documentation."
        ),
        "",
    ]
    if in_use_blocked:
        lines += [
            (
                f"**Implemented but not usable:** {', '.join(f'`{n}`' for n in in_use_blocked)}. The V0 model still "
                "trains on them; removing or replacing them is a model change (V3), not done here."
            ),
            "",
        ]
    lines += [
        (
            "- **usable**: every source column has confidence high/medium and model_use entity/feature/derive "
            "(computed from the contract, never declared in the registry)."
        ),
        (
            "- **validated**: shown per source. It does not gate usability yet, because with 0 of "
            f"{len(contract.columns)} contract fields validated it would block every feature; it must be resolved "
            "before real-data use."
        ),
        (
            "- **ATT&CK**: hypotheses only. A high value is anomalous behaviour consistent with the technique, not "
            "evidence of it."
        ),
        "- **prior_history**: may use only data strictly earlier than the window (no temporal leakage).",
        "",
        "## Usable features",
        "",
        (
            "| feature | level | status | scope | sources (confidence, validated) | transform | ATT&CK hypotheses "
            "| rationale |"
        ),
        "|---|---|---|---|---|---|---|---|",
        *(f"| `{e.feature.name}` | {e.feature.level} | {status(e.feature)} | {e.feature.temporal_scope} "
          f"| {sources(e)} | {cell(e.feature.transform)} | {attack(e.feature)} | {cell(e.feature.rationale)} |"
          for e in usable),
        "",
        "## Not usable",
        "",
        "| feature | level | status | why not usable | sources (confidence, validated) | transform | rationale |",
        "|---|---|---|---|---|---|---|",
        *(f"| `{e.feature.name}` | {e.feature.level} | {status(e.feature)} | {'<br>'.join(e.reasons)} "
          f"| {sources(e)} | {cell(e.feature.transform)} | {cell(e.feature.rationale)} |"
          for e in blocked),
        "",
        "## Derived lake columns",
        "",
        "| column | contract sources | transform |",
        "|---|---|---|",
        *(f"| `{d.name}` | {', '.join(f'`{s}`' for s in d.sources)} | {cell(d.transform)} |"
          for d in registry.derived_columns),
        "",
    ]
    return "\n".join(lines)
