import glob
import os
import pathlib
import pytest
import random
from dfdiagnoser import init_with_hydra


# Full test matrix for comprehensive testing
full_test_params = [
    ("tests/data/dfanalyzer_checkpoints/unet3d_v100", "json"),
    ("tests/data/dfanalyzer_checkpoints/unet3d_v100", "csv"),
    ("tests/data/dfanalyzer_checkpoints/unet3d_v100", "parquet"),
]

# Reduced matrix for smoke testing (fast runs)
smoke_test_params = [random.choice(full_test_params)]


@pytest.mark.full
@pytest.mark.parametrize("checkpoint_path, output_format", full_test_params)
def test_e2e_full(
    checkpoint_path: str,
    output_format: str,
    tmp_path: pathlib.Path,
) -> None:
    """Full test suite with all parameter combinations."""
    _test_e2e(checkpoint_path, output_format, tmp_path)


@pytest.mark.smoke
@pytest.mark.parametrize("checkpoint_path, output_format", smoke_test_params)
def test_e2e_smoke(
    checkpoint_path: str,
    output_format: str,
    tmp_path: pathlib.Path,
) -> None:
    """Smoke test with minimal parameter combinations for quick validation."""
    _test_e2e(checkpoint_path, output_format, tmp_path)


def _test_e2e(
    checkpoint_path: str,
    output_format: str,
    tmp_path: pathlib.Path,
) -> None:
    """Common test logic extracted to avoid duplication."""
    output_dir = f"{tmp_path}/output"

    # Build hydra overrides
    hydra_overrides = [
        f"input.checkpoint_dir={checkpoint_path}",
        "output=file",
        f"output.output_dir={output_dir}",
        f"output.output_format={output_format}",
    ]

    # Initialize dfdiagnoser with hydra
    dfd = init_with_hydra(hydra_overrides=hydra_overrides)

    # Validate configuration
    assert dfd.hydra_config.input.checkpoint_dir == checkpoint_path
    assert dfd.hydra_config.output._target_ == "dfdiagnoser.output.FileOutput"
    assert dfd.hydra_config.output.output_dir == output_dir
    assert dfd.hydra_config.output.output_format == output_format

    # Run the diagnosis
    result = dfd.diagnose_checkpoint()

    # Validate results
    assert len(result.flat_view_paths) > 0, "No flat view paths found in result"
    assert len(result.scored_flat_views) > 0, "No scored flat views found in result"
    assert len(result.flat_view_paths) == len(result.scored_flat_views), (
        f"Mismatch: {len(result.flat_view_paths)} paths vs {len(result.scored_flat_views)} views"
    )

    # Check that all flat view paths exist and are from the checkpoint directory
    for path in result.flat_view_paths:
        assert checkpoint_path in path, f"Flat view path {path} not from checkpoint directory {checkpoint_path}"
        assert path.endswith('.parquet'), f"Flat view path {path} is not a parquet file"

    # Check that scored views have the expected structure
    for df in result.scored_flat_views:
        assert not df.empty, "Scored flat view is empty"
        # Check for score columns (should have some _score suffixed columns)
        score_columns = [col for col in df.columns if col.endswith('_score')]
        assert len(score_columns) > 0, f"No score columns found in scored flat view. Columns: {df.columns.tolist()}"

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Test output handling
    dfd.handle_result(result)

    # Check that output files were created
    expected_output_files = glob.glob(f"{output_dir}/*.{output_format}")
    assert len(expected_output_files) > 0, f"No {output_format} output files found in {output_dir}"
    for output_file in expected_output_files:
        assert os.path.exists(output_file), f"Output file {output_file} was not created"
        assert os.path.getsize(output_file) > 0, f"Output file {output_file} is empty"
