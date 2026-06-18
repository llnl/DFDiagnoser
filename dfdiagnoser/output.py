import json
import os
from collections import defaultdict
from typing import Optional

import structlog
from rich.console import Console
from rich.panel import Panel
from rich.tree import Tree

from .types import DiagnosisResult, FileOutputFormat

logger = structlog.get_logger()

# Single-letter severity tags for compact rendering (mirrors WisIO/analyzer style).
_SEVERITY_INITIALS = {
    "critical": "C",
    "very high": "V",
    "high": "H",
    "medium": "M",
    "low": "L",
    "very low": "v",
    "trivial": "t",
    "none": "-",
}


class Output:
    def __init__(self):
        pass

    def handle_result(self, result: DiagnosisResult):
        pass


class ConsoleOutput(Output):
    """Render findings as a summary panel plus a scope-grouped tree.

    Mirrors DFAnalyzer's ConsoleOutput style. Reads only DiagnosisResult.findings,
    so it works for offline replay (diagnose_facts) and streaming alike.
    """

    def __init__(
        self,
        output_dir: Optional[str] = None,
        output_format: FileOutputFormat = "json",
        show_debug: bool = False,
        show_header: bool = True,
    ):
        super().__init__()
        # output_dir/output_format are accepted (config inherits FileOutputConfig)
        # but ConsoleOutput only prints.
        self.output_dir = output_dir
        self.output_format = output_format
        self.show_debug = show_debug
        self.show_header = show_header

    def handle_result(self, result: DiagnosisResult):
        findings = list(result.findings or [])
        console = Console()

        if self.show_header:
            console.print(self._summary_panel(findings))

        if not findings:
            console.print("[dim]No findings.[/dim]")
            return

        console.print(self._findings_tree(findings))

    @staticmethod
    def _summary_panel(findings) -> Panel:
        severity_counts: dict = {}
        scopes = set()
        for f in findings:
            severity_counts[f.severity] = severity_counts.get(f.severity, 0) + 1
            scopes.add(f.scope)
        severity_line = ", ".join(
            f"{label}: {count}" for label, count in sorted(severity_counts.items())
        ) or "none"
        body = (
            f"Findings   {len(findings)}\n"
            f"Scopes     {len(scopes)}\n"
            f"Severity   {severity_line}"
        )
        return Panel(body, title="DFDiagnoser Diagnosis", expand=False)

    @staticmethod
    def _findings_tree(findings) -> Tree:
        by_scope = defaultdict(list)
        for f in findings:
            by_scope[f.scope].append(f)

        tree = Tree("Findings")
        for scope in sorted(by_scope):
            scope_findings = by_scope[scope]
            scope_node = tree.add(f"[bold]{scope}[/bold] ({len(scope_findings)})")
            for f in scope_findings:
                initial = _SEVERITY_INITIALS.get(f.severity, "?")
                header = (
                    f"[{initial}] {f.finding_type}: {f.motif} "
                    f"(severity {f.severity} {f.severity_score:.2f}, "
                    f"conf {f.confidence:.2f})"
                )
                detail = (
                    f"prevalence {f.trend.prevalence:.2f}, "
                    f"persistence {f.trend.persistence}, "
                    f"trend {f.trend.trend_direction} -> {f.recommendation_bundle}"
                )
                finding_node = scope_node.add(header)
                finding_node.add(f"[dim]{detail}[/dim]")
                for cf_type, cf_scope in f.contributing_facts:
                    finding_node.add(f"[dim](fact) {cf_type} @ {cf_scope}[/dim]")
        return tree


class FileOutput(Output):
    def __init__(self, output_dir: Optional[str] = None, output_format: FileOutputFormat = "json"):
        super().__init__()
        self.output_dir = output_dir
        self.output_format = output_format
        self._seq = 0

    def handle_result(self, result: DiagnosisResult):
        for i, scored_flat_view in enumerate(result.scored_flat_views):
            # Use original path if available, otherwise generate a sequential filename
            if i < len(result.flat_view_paths) and result.flat_view_paths[i]:
                flat_view_path = result.flat_view_paths[i]
                if self.output_dir:
                    output_path = f"{self.output_dir}/{flat_view_path.split('/')[-1].split('.')[0]}_scored.{self.output_format}"
                else:
                    output_path = f"{flat_view_path.split('.')[0]}_scored.{self.output_format}"
            else:
                # Streaming mode: no source path available
                self._seq += 1
                if not self.output_dir:
                    self.output_dir = "dfdiagnoser_output"
                output_path = f"{self.output_dir}/scored_{self._seq:06d}.{self.output_format}"

            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            if self.output_format == "json":
                scored_flat_view.to_json(output_path, orient="index")
            elif self.output_format == "csv":
                scored_flat_view.to_csv(output_path, index=True)
            elif self.output_format == "parquet":
                scored_flat_view.to_parquet(output_path, index=True)
            else:
                raise ValueError(
                    f"Unsupported output format: {self.output_format}")

        self._write_findings(result)

    def _write_findings(self, result: DiagnosisResult):
        """Write findings as JSONL (one to_wire_dict record per line), the same
        serialization the Mofka publisher uses, so the file and the optimizer's
        input are byte-identical."""
        findings = list(result.findings or [])
        if not findings:
            return
        out_dir = self.output_dir or "dfdiagnoser_output"
        os.makedirs(out_dir, exist_ok=True)
        findings_path = os.path.join(out_dir, "findings.jsonl")
        with open(findings_path, "w", encoding="utf-8") as fh:
            for finding in findings:
                record = finding.to_wire_dict()
                record["publish_mode"] = "summary"
                fh.write(json.dumps(record) + "\n")
        logger.info("diagnoser.findings.written", path=findings_path, count=len(findings))
