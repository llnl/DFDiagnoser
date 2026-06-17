import multiprocessing
import os
from pathlib import Path

import pytest

from dfdiagnoser.diagnoser import Diagnoser
from dfdiagnoser.streaming.mofka_io import open_producer


pytestmark = [pytest.mark.smoke, pytest.mark.full]


def _load_flat_view_files(root: Path):
    return sorted(root.glob("_flat_view_*.parquet"))


def test_e2e_streaming_mofka(bedrock_mofka):
    group_file, topic_name = bedrock_mofka

    checkpoints_dir = Path(__file__).parent / "data" / "dfanalyzer_checkpoints" / "unet3d_v100"
    flat_view_files = _load_flat_view_files(checkpoints_dir)
    assert flat_view_files, f"No flat view parquet files found in {checkpoints_dir}"

    def run_producer():
        driver, producer = open_producer(group_file, topic_name)
        for path in flat_view_files:
            payload = path.read_bytes()
            metadata = {"view_type": path.name}
            producer.push(metadata=metadata, data=payload)
        producer.push(metadata={"name": "end"}, data=b"")
        producer.flush()
        del producer
        del driver

    proc = multiprocessing.Process(target=run_producer, daemon=True)
    proc.start()

    diagnoser = Diagnoser()
    collected = []

    def handle_result(result):
        collected.append(result)

    diagnoser.diagnose_mofka(
        group_file=group_file,
        topic_name=topic_name,
        output_handler=handle_result,
    )

    assert len(collected) == len(flat_view_files)
    assert all(r.scored_flat_views for r in collected)
    assert all(r.scored_flat_views[0].shape[0] > 0 for r in collected)

    proc.join(timeout=5)
