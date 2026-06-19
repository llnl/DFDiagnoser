"""Tests for DFDiagnoser offline facts replay (diagnose_facts).

Covers: findings are produced from saved analysis_facts; JSONL and directory
inputs are equivalent; online (Mofka-decode) and offline ingest paths produce
byte-identical findings (parity); and the path runs with no mochi.mofka."""
import json
import sys

import pytest

from dfdiagnoser.diagnoser import Diagnoser


# ---- fixtures: synthetic analyzer.fact-envelope.v1 envelopes -----------------

def _fact(fact_type, epoch, score=1.0, label="critical", layer="app", entity=None,
          tags=None):
    return {
        "fact_type": fact_type,
        "window": {
            "run_id": "r1", "view_type": "epoch", "epoch": epoch, "step": None,
            "t0_ns": 0, "t1_ns": 1, "trigger": "epoch.block",
        },
        "scope": {
            "workload": "unet3d", "layer": layer, "entity": entity,
            "rank_set": None, "node": "",
        },
        "evidence": {"metrics": {"fetch_frac": 0.8, "compute_frac": 0.2}},
        "severity": {"score": score, "label": label, "method": "rule_weighted"},
        "confidence": 0.87,
        "opportunity_tags": tags or ["dataloader_prefetch", "reader_parallelism"],
        "suppresses_tags": [],
        "provenance": None,
        "schema_version": "analysisfact.v1",
        "fact_id": f"af_{fact_type}_{epoch}",
    }


def _envelope(facts):
    return {
        "schema_version": "analyzer.fact-envelope.v1",
        "context": {
            "run_id": "r1", "layers": ["app"], "view_types": ["epoch"],
            "time_granularity": 1.0, "time_resolution": 1.0,
            "total_event_count": 100, "window_type_counts": {"epoch": 1},
        },
        "facts": facts,
        "fact_count_by_view": {"epoch": len(facts)},
    }


def _persistent_pressure_envelopes(n=5):
    """fetch_pressure in all n consecutive windows -> persistent_pressure."""
    return [_envelope([_fact("fetch_pressure", epoch=i)]) for i in range(n)]


def _write_jsonl(envelopes, path):
    with open(path, "w", encoding="utf-8") as f:
        for env in envelopes:
            f.write(json.dumps(env) + "\n")
    return str(path)


def _write_dir(envelopes, dirpath):
    dirpath.mkdir(parents=True, exist_ok=True)
    for i, env in enumerate(envelopes):
        (dirpath / f"facts_{i:06d}.json").write_text(json.dumps(env))
    return str(dirpath)


# ---- tests -------------------------------------------------------------------

def test_produces_persistent_pressure_finding(tmp_path):
    path = _write_jsonl(_persistent_pressure_envelopes(5), tmp_path / "facts.jsonl")
    result = Diagnoser().diagnose_facts(path)

    assert len(result.findings) == 1
    f = result.findings[0]
    assert f.finding_type == "fetch_pressure"
    # epoch-view facts (numeric entity) are keyed by their view_type
    assert f.scope == "app:epoch"
    assert f.view_type == "epoch"
    assert f.motif == "persistent_pressure"
    assert f.trend.prevalence == 1.0
    assert f.trend.persistence == 5
    assert "dataloader_prefetch" in f.opportunity_tags


def test_jsonl_and_directory_inputs_are_equivalent(tmp_path):
    envs = _persistent_pressure_envelopes(5)
    jsonl_findings = Diagnoser().diagnose_facts(
        _write_jsonl(envs, tmp_path / "facts.jsonl")
    ).findings
    dir_findings = Diagnoser().diagnose_facts(
        _write_dir(envs, tmp_path / "facts_dir")
    ).findings
    assert [f.to_wire_dict() for f in jsonl_findings] == \
           [f.to_wire_dict() for f in dir_findings]


def test_online_offline_parity(tmp_path):
    """Same envelopes through the Mofka-decode wrapper (online) vs the offline
    reader must yield byte-identical findings."""
    envs = _persistent_pressure_envelopes(5)

    offline = Diagnoser().diagnose_facts(_write_jsonl(envs, tmp_path / "f.jsonl"))

    class _FakeEvent:
        def __init__(self, env):
            self.data = json.dumps(env).encode("utf-8")
            self.metadata = {"artifact_type": "analysis_facts"}

    online = Diagnoser()
    for env in envs:
        online._handle_analysis_facts(
            _FakeEvent(env), {"artifact_type": "analysis_facts"}
        )
        online.state.advance_window()
    online_findings = online._build_longitudinal_summary()

    assert [f.to_wire_dict() for f in offline.findings] == \
           [f.to_wire_dict() for f in online_findings]


def test_runs_without_mofka(tmp_path):
    path = _write_jsonl(_persistent_pressure_envelopes(3), tmp_path / "facts.jsonl")
    Diagnoser().diagnose_facts(path)
    # The offline path must not require the Mofka client.
    assert "mochi" not in sys.modules
    assert "mochi.mofka" not in sys.modules


def test_findings_json_shape_matches_wire(tmp_path):
    path = _write_jsonl(_persistent_pressure_envelopes(5), tmp_path / "facts.jsonl")
    result = Diagnoser().diagnose_facts(path)
    d = result.findings[0].to_wire_dict()
    # keys the optimizer consumes from the Mofka payload
    for key in ("finding_type", "scope", "layer", "motif", "severity_score",
                "confidence", "prevalence", "persistence", "trend_direction",
                "contributing_facts", "opportunity_tags", "key_metrics", "summary"):
        assert key in d


def test_missing_path_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        Diagnoser().diagnose_facts(str(tmp_path / "nope.jsonl"))


def test_empty_dir_raises(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(ValueError):
        Diagnoser().diagnose_facts(str(empty))


# ---- input=checkpoint reads facts.jsonl ; two-level scope --------------------

def test_diagnose_checkpoint_reads_facts_jsonl(tmp_path):
    # analyzer-style checkpoint dir containing a facts.jsonl artifact
    (tmp_path / "facts.jsonl").write_text(
        "\n".join(json.dumps(e) for e in _persistent_pressure_envelopes(5)) + "\n"
    )
    result = Diagnoser().diagnose_checkpoint(str(tmp_path))
    assert len(result.findings) == 1
    f = result.findings[0]
    assert f.finding_type == "fetch_pressure"
    assert f.motif == "persistent_pressure"
    assert f.scope == "app:epoch"  # aggregate scope


def test_aggregate_fact_keyed_by_view(tmp_path):
    env = _envelope([_fact("metadata_dominance", epoch=0, layer="reader_posix", entity=None)])
    d = Diagnoser()
    d._ingest_fact_envelope(env)
    keys = {k for k, _ in d.state.all_trackers()}
    assert ("metadata_dominance", "reader_posix:epoch") in keys


def test_detail_fact_keyed_by_view_and_entity(tmp_path):
    env = _envelope([_fact("metadata_dominance", epoch=0, layer="reader_posix",
                           entity="/d/x.npz")])
    d = Diagnoser()
    d._ingest_fact_envelope(env)
    keys = {k for k, _ in d.state.all_trackers()}
    assert ("metadata_dominance", "reader_posix:epoch:/d/x.npz") in keys
