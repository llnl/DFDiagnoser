"""Tests for DFDiagnoser ConsoleOutput (findings report) and FileOutput
(findings.jsonl), exercising the offline-mode rendering/persistence."""
import json

from dfdiagnoser.output import ConsoleOutput, FileOutput
from dfdiagnoser.types import DiagnosisFinding, DiagnosisResult, TrendEvidence


def _finding(finding_type="fetch_pressure", scope="app:global", severity="critical"):
    return DiagnosisFinding(
        finding_type=finding_type,
        scope=scope,
        layer=scope.split(":")[0] if ":" in scope else None,
        motif="persistent_pressure",
        severity=severity,
        severity_score=0.92,
        confidence=0.87,
        trend=TrendEvidence(
            prevalence=1.0, persistence=5, onset_window=0,
            peak_severity_window=4, last_seen_window=4,
            support_windows=5, trend_direction="stable",
        ),
        contributing_facts=[(finding_type, scope)],
        recommendation_bundle="input_pipeline_tuning",
        summary=f"{finding_type}({scope})",
        opportunity_tags=["dataloader_prefetch"],
        suppresses_tags=[],
        key_metrics={"fetch_frac": 0.8},
    )


def _result(findings):
    return DiagnosisResult(flat_view_paths=[], scored_flat_views=[], findings=findings)


# ---- FileOutput --------------------------------------------------------------

def test_file_output_writes_findings_jsonl(tmp_path):
    findings = [_finding(), _finding("open_pressure")]
    FileOutput(output_dir=str(tmp_path)).handle_result(_result(findings))

    path = tmp_path / "findings.jsonl"
    assert path.exists()
    lines = path.read_text().splitlines()
    assert len(lines) == 2
    rec = json.loads(lines[0])
    assert rec["finding_type"] == "fetch_pressure"
    assert rec["publish_mode"] == "summary"
    # nested trend fields are flattened into the wire shape
    assert rec["prevalence"] == 1.0 and rec["persistence"] == 5


def test_file_output_records_match_wire_dict(tmp_path):
    f = _finding()
    FileOutput(output_dir=str(tmp_path)).handle_result(_result([f]))
    rec = json.loads((tmp_path / "findings.jsonl").read_text().splitlines()[0])
    expected = f.to_wire_dict()
    expected["publish_mode"] = "summary"
    # contributing_facts tuples serialize to JSON arrays; normalize for compare
    expected = json.loads(json.dumps(expected))
    assert rec == expected


def test_file_output_no_findings_writes_no_file(tmp_path):
    FileOutput(output_dir=str(tmp_path)).handle_result(_result([]))
    assert not (tmp_path / "findings.jsonl").exists()


# ---- ConsoleOutput -----------------------------------------------------------

def test_console_output_renders_findings(capsys):
    ConsoleOutput().handle_result(_result([_finding(), _finding("open_pressure")]))
    out = capsys.readouterr().out
    assert "DFDiagnoser Diagnosis" in out
    assert "fetch_pressure" in out
    assert "persistent_pressure" in out
    assert "app:global" in out


def test_console_output_empty(capsys):
    ConsoleOutput().handle_result(_result([]))
    out = capsys.readouterr().out
    assert "No findings" in out
