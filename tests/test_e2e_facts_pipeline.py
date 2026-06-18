"""Cross-tool plumbing e2e: real DFAnalyzer (output=file) -> facts.jsonl ->
DFDiagnoser.diagnose_facts.

Validates the analyzer->file->diagnoser artifact contract on a bundled trace.
The bundled traces are minimal and typically fire no rules, so findings may be
empty; this test asserts the handoff runs cleanly, not that findings exist.

Slow (runs the full analyzer + a dask cluster). Self-skips if the analyzer or
its bundled data is not available.
"""
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

from dfdiagnoser.diagnoser import Diagnoser
from dfdiagnoser.types import DiagnosisResult

# Bundled analyzer trace tarball, relative to this repo's optimization/ layout.
_ANALYZER_DATA = Path(__file__).resolve().parents[2] / "dfanalyzer" / "tests" / "data"
_TARBALL = _ANALYZER_DATA / "dftracer-dlio.tar.gz"
_DFANALYZER_CLI = Path(sys.executable).parent / "dfanalyzer"

pytestmark = pytest.mark.skipif(
    not (_TARBALL.exists() and _DFANALYZER_CLI.exists()),
    reason="DFAnalyzer CLI or bundled dftracer-dlio trace not available",
)


def test_analyzer_filedump_to_diagnoser(tmp_path):
    # 1) Extract a bundled dftracer trace (tarball holds .pfw files directly).
    trace_dir = tmp_path / "trace"
    trace_dir.mkdir()
    with tarfile.open(_TARBALL) as tar:
        tar.extractall(trace_dir)
    assert list(trace_dir.glob("*.pfw")), "no .pfw trace files extracted"

    facts_path = tmp_path / "facts.jsonl"
    run_dir = tmp_path / "run"

    # 2) Run the real analyzer with the new output=file facts dump.
    proc = subprocess.run(
        [
            str(_DFANALYZER_CLI),
            "analyzer=dftracer",
            "analyzer/preset=dlio",
            "analyzer.checkpoint=False",
            "input=file",
            f"input.path={trace_dir}",
            "output=facts",
            f"output.file_path={facts_path}",
            "facts.enabled=true",
            "facts.emit_analysis_facts=true",
            "facts.rule_file=dlio",
            f"hydra.run.dir={run_dir}",
        ],
        capture_output=True,
        text=True,
        timeout=400,
    )
    assert proc.returncode == 0, f"analyzer failed:\n{proc.stderr}"

    # 3) The analyzer must have produced the dump file (may be empty if no rules fired).
    assert facts_path.exists(), "analyzer did not create the facts dump"

    # 4) The diagnoser consumes the dump without error and returns a result.
    result = Diagnoser().diagnose_facts(str(facts_path))
    assert isinstance(result, DiagnosisResult)
    assert isinstance(result.findings, list)  # possibly empty for a clean trace
