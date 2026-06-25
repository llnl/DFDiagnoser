import json
import os
import pathlib

import pytest

from dfdiagnoser import init_with_hydra


pytestmark = [pytest.mark.smoke, pytest.mark.full]


def _facts_jsonl(n_epochs: int = 5) -> str:
    """Per-epoch fact envelopes (one window each) with a persistent fetch_pressure."""
    lines = []
    for epoch in range(1, n_epochs + 1):
        lines.append(json.dumps({
            "schema_version": "analyzer.fact-envelope.v1",
            "context": {"run_id": "r1", "layers": ["app"], "view_types": ["epoch"]},
            "facts": [{
                "fact_type": "fetch_pressure",
                "window": {"run_id": "r1", "view_type": "epoch", "epoch": epoch,
                           "step": None, "t0_ns": 0, "t1_ns": 1, "trigger": "rule_eval"},
                "scope": {"workload": "unet3d", "layer": "app", "entity": None,
                          "rank_set": "all", "node": None},
                "evidence": {"metrics": {"fetch_frac": 0.82}},
                "severity": {"score": 0.9, "label": "critical", "method": "rule_expr"},
                "confidence": 0.85, "opportunity_tags": ["dataloader_prefetch"],
                "suppresses_tags": [], "provenance": None,
                "schema_version": "analysisfact.v1", "fact_id": f"af_{epoch}",
            }],
            "fact_count_by_view": {},
        }))
    return "\n".join(lines) + "\n"


def test_e2e_file_bundle_to_findings(tmp_path: pathlib.Path) -> None:
    """input=file reads the analyzer's output=file bundle (facts.jsonl) -> findings.

    The diagnoser is a pure fact consumer now (scoring moved to the analyzer), so
    the offline path is the bundle's facts.jsonl -> longitudinal findings.
    """
    ckpt = tmp_path / "ckpt"
    ckpt.mkdir()
    (ckpt / "facts.jsonl").write_text(_facts_jsonl())
    output_dir = f"{tmp_path}/output"

    dfd = init_with_hydra(hydra_overrides=[
        "input=file",
        f"input.path={ckpt}",
        "output=file",
        f"output.output_dir={output_dir}",
    ])
    assert dfd.hydra_config.input.path == str(ckpt)
    assert dfd.hydra_config.output._target_ == "dfdiagnoser.output.FileOutput"

    result = dfd.diagnose_file()

    assert len(result.findings) == 1, "expected one longitudinal finding"
    finding = result.findings[0]
    assert finding.finding_type == "fetch_pressure"
    assert finding.motif == "persistent_pressure"
    assert finding.scope == "app:epoch"

    os.makedirs(output_dir, exist_ok=True)
    dfd.handle_result(result)

    findings_path = pathlib.Path(output_dir) / "findings.jsonl"
    assert findings_path.exists(), "findings.jsonl not written"
    records = [json.loads(line) for line in findings_path.read_text().splitlines()]
    assert len(records) == 1
    assert records[0]["finding_type"] == "fetch_pressure"
