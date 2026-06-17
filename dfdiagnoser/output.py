import os
import pandas as pd
from typing import Optional

from .types import DiagnosisResult, FileOutputFormat


class Output:
    def __init__(self):
        pass

    def handle_result(self, result: DiagnosisResult):
        pass


class ConsoleOutput(Output):
    def __init__(self):
        super().__init__()

    def handle_result(self, result: DiagnosisResult):
        pass


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
