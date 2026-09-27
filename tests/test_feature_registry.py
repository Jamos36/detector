from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from netanomaly import baselines, features, iforest, novelty
from netanomaly.feature_registry import (
    DerivedColumn,
    Level,
    ModelTransform,
    Registry,
    Status,
    TemporalScope,
    assess,
    check_against_contract,
    load_registry,
    to_markdown,
    usable_features,
)
from netanomaly.schema import Confidence, Contract


@pytest.fixture
def registry() -> Registry:
    return load_registry()


def _registry(**feature) -> Registry:
    base = {"name": "f", "level": "flow", "status": "candidate", "inputs": ["bytes"], "transform": "t",
            "rationale": "r"}
    return Registry.model_validate({"registry_version": 1, "contract": "netflow_v1", "contract_schema_version": 1,
                                    "features": [base | feature]})


def _with_column(contract: Contract, name: str, **update) -> Contract:
    # model_copy skips validation, so a test can give a `feature` column low confidence.
    cols = [c.model_copy(update=update) if c.name == name else c for c in contract.columns]
    return contract.model_copy(update={"columns": cols})


def test_features_md_is_generated_from_registry(registry, contract):
    features_md = Path(__file__).parent.parent / "FEATURES.md"
    assert features_md.read_text(encoding="utf-8") == to_markdown(registry, contract), "run: uv run netanomaly feature-doc"


def test_implemented_features_match_the_pipeline(registry):
    implemented = [f for f in registry.features if f.status is Status.IMPLEMENTED]
    not_model_inputs = set(baselines.BASELINE_FEATURES) | set(novelty.NOVELTY_FEATURES)
    model_inputs = [f for f in implemented if f.name not in not_model_inputs]
    assert tuple(f.name for f in model_inputs) == features.HOST_WINDOW_FEATURES
    assert {f.name for f in implemented} - {f.name for f in model_inputs} == not_model_inputs
    assert {f.name for f in implemented if f.model_transform is ModelTransform.LOG1P} == iforest.LOG1P_FEATURES


def test_features_on_low_confidence_or_excluded_fields_are_not_usable(registry, contract):
    blocked = {e.feature.name: e.reasons for e in assess(registry, contract) if not e.eligible}
    assert blocked == {
        "syn_only_ratio": ("tcp_flags: confidence low",),
        "rst_ratio": ("tcp_flags: confidence low",),
        "mean_packet_length": ("packet_length: confidence low", "packet_length: model_use exclude"),
    }
    assert set(usable_features(registry, contract)).isdisjoint(blocked)


def test_usable_features_can_be_filtered_by_level(registry, contract):
    assert usable_features(registry, contract, Level.FLOW) == ["bytes_per_packet", "flow_duration"]


def test_no_feature_is_validated_while_its_sources_are_not(registry, contract):
    assert not any(e.validated for e in assess(registry, contract))


def test_validated_comes_only_from_the_contract_and_does_not_change_eligibility(registry, contract):
    confirmed = _with_column(_with_column(contract, "bytes", validated=True), "packets", validated=True)
    by_name = {e.feature.name: e for e in assess(registry, confirmed)}
    assert by_name["bytes_per_packet"].validated and by_name["bytes_per_packet"].eligible
    assert not by_name["bytes_out"].validated  # src_ip and flow_start are still unvalidated


def test_lowering_a_source_confidence_in_the_contract_blocks_its_features(registry, contract):
    usable = usable_features(registry, _with_column(contract, "bytes", confidence=Confidence.LOW))
    assert not {"bytes_out", "max_flow_bytes", "bytes_per_packet", "bytes_out_robust_z"} & set(usable)
    assert "flows" in usable


def test_derived_inputs_are_expanded_to_their_contract_sources(registry, contract):
    (e,) = [e for e in assess(registry, contract) if e.feature.name == "syn_only_ratio"]
    assert [c.name for c in e.sources] == ["src_ip", "flow_start", "tcp_flags"]


def test_provenance_fields_are_not_usable(contract):
    (e,) = assess(_registry(inputs=["exporter_ip"]), contract)
    assert e.reasons == ("exporter_ip: model_use provenance",)


@pytest.mark.parametrize("field", ["validated", "confidence", "eligible"])
def test_registry_cannot_declare_trust_fields(field):
    with pytest.raises(ValidationError, match=field):
        _registry(**{field: True})


def test_unknown_input_is_rejected(contract):
    with pytest.raises(ValueError, match="num_bytes"):
        check_against_contract(_registry(inputs=["num_bytes"]), contract)  # raw name, not canonical


@pytest.mark.parametrize(("derived", "message"), [({"name": "bytes", "sources": ["bytes"]}, "shadows"),
                                                  ({"name": "x", "sources": ["num_bytes"]}, "unknown sources")])
def test_invalid_derived_columns_are_rejected(contract, derived, message):
    reg = _registry().model_copy(update={"derived_columns": (DerivedColumn(transform="t", **derived),)})
    with pytest.raises(ValueError, match=message):
        check_against_contract(reg, contract)


def test_registry_for_another_contract_version_is_rejected(contract):
    reg = _registry().model_copy(update={"contract_schema_version": 2})
    with pytest.raises(ValueError, match="review the registry"):
        check_against_contract(reg, contract)


@pytest.mark.parametrize("bad", [{"attack_hypotheses": [{"technique": "TA0007", "name": "Discovery"}]},
                                 {"status": "implemented", "task": "V2-2"},
                                 {"level": "host_day"}])
def test_invalid_feature_entries_are_rejected(bad):
    with pytest.raises(ValidationError):
        _registry(**bad)


def test_duplicate_feature_names_are_rejected():
    entry = _registry().features[0].model_dump()
    with pytest.raises(ValidationError, match="duplicate feature"):
        Registry.model_validate({"registry_version": 1, "contract": "netflow_v1", "contract_schema_version": 1,
                                 "features": [entry, entry]})


def test_baseline_and_novelty_candidates_are_marked_prior_history(registry):
    # V2-2/V2-3 use past windows, so the no-leakage rule applies; the scope must say so.
    for f in registry.features:
        if f.name in baselines.BASELINE_FEATURES or f.name in novelty.NOVELTY_FEATURES:
            assert f.temporal_scope is TemporalScope.PRIOR_HISTORY, f.name
