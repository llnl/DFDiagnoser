"""End-to-end CLI test for the offline path:
`python -m dfdiagnoser input=file input.path=<analyzer output=file dir>` -> findings."""
import json
import os
import subprocess
import sys


def _fact(fact_type, epoch):
    return {
        "fact_type": fact_type,
        "window": {"run_id": "r1", "view_type": "epoch", "epoch": epoch, "step": None,
                   "t0_ns": 0, "t1_ns": 1, "trigger": "epoch.block"},
        "scope": {"workload": "unet3d", "layer": "app", "entity": None,
                  "rank_set": None, "node": ""},
        "evidence": {"metrics": {"fetch_frac": 0.8}},
        "severity": {"score": 1.0, "label": "critical", "method": "rule_weighted"},
        "confidence": 0.87,
        "opportunity_tags": ["dataloader_prefetch"],
        "suppresses_tags": [],
        "provenance": None,
        "schema_version": "analysisfact.v1",
        "fact_id": f"af_{fact_type}_{epoch}",
    }


def _envelope(facts):
    return {
        "schema_version": "analyzer.fact-envelope.v1",
        "context": {"run_id": "r1", "layers": ["app"], "view_types": ["epoch"],
                    "time_granularity": 1.0, "time_resolution": 1.0,
                    "total_event_count": 100, "window_type_counts": {"epoch": 1}},
        "facts": facts,
        "fact_count_by_view": {"epoch": len(facts)},
    }


def _write_persistent_facts(path, n=5):
    with open(path, "w", encoding="utf-8") as f:
        for i in range(n):
            f.write(json.dumps(_envelope([_fact("fetch_pressure", i)])) + "\n")


def test_cli_file_input_writes_findings(tmp_path):
    # the offline path: analyzer output=file writes the bundle, diagnoser reads its folder
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    _write_persistent_facts(bundle / "facts.jsonl")
    out_dir = tmp_path / "out"
    run_dir = tmp_path / "run"

    proc = subprocess.run(
        [sys.executable, "-m", "dfdiagnoser",
         "input=file", f"input.path={bundle}",
         "output=file", f"output.output_dir={out_dir}",
         f"hydra.run.dir={run_dir}"],
        capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr

    findings_path = out_dir / "findings.jsonl"
    assert findings_path.exists(), proc.stderr
    lines = findings_path.read_text().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["finding_type"] == "fetch_pressure"
    assert rec["motif"] == "persistent_pressure"
    assert rec["publish_mode"] == "summary"


def test_cli_file_input_console_output(tmp_path):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    _write_persistent_facts(bundle / "facts.jsonl")
    run_dir = tmp_path / "run"

    proc = subprocess.run(
        [sys.executable, "-m", "dfdiagnoser",
         "input=file", f"input.path={bundle}",
         "output=console", f"hydra.run.dir={run_dir}"],
        capture_output=True, text=True,
        env={**os.environ, "COLUMNS": "200"},
    )
    assert proc.returncode == 0, proc.stderr
    assert "fetch_pressure" in proc.stdout
