import pytest
import pandas as pd
import numpy as np
from dfdiagnoser.scoring import score_metrics


pytestmark = [pytest.mark.smoke, pytest.mark.full]


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'cpu_pct': [0.1, 0.5, 0.8],
        'memory_per': [0.2, 0.6, 0.9],
        'disk_util': [0.3, 0.7, 1.0],
        'bw_slope': [0.1, 1.0, 2.0],
        'io_intensity_mean': [1e-6, 1e-5, 1e-4],
        'bw_mean': [50, 75, 100],
        'cpu_mean': [40, 60, 80],
        'd_non_metric': [1, 2, 3]
    })


@pytest.fixture
def empty_df():
    return pd.DataFrame()


@pytest.fixture
def df_with_nans():
    return pd.DataFrame({
        'cpu_pct': [0.1, np.nan, 0.8],
        'bw_slope': [0.1, 1.0, np.nan]
    })


@pytest.fixture
def metric_boundaries_sample():
    return {'bw_mean': 100, 'cpu_mean': 50}


def test_score_metrics_empty_df(empty_df):
    result = score_metrics(empty_df, {})
    pd.testing.assert_frame_equal(result, empty_df)


def test_score_metrics_no_matching_columns():
    df = pd.DataFrame({'d_col1': [1, 2], 'd_col2': [3, 4]})
    result = score_metrics(df, {})
    pd.testing.assert_frame_equal(result, df)


def test_score_metrics_percentage_metrics(sample_df):
    df = sample_df[['cpu_pct', 'memory_per', 'disk_util']].copy()
    result = score_metrics(df, {})
    # cpu_pct: 0.1 -> 1, 0.5 -> 2, 0.8 -> 4
    assert result['cpu_pct_score'].tolist() == [1, 2, 4]
    # memory_per: 0.2 -> 1, 0.6 -> 3, 0.9 -> 4
    assert result['memory_per_score'].tolist() == [1, 3, 4]
    # disk_util: 1-0.3=0.7 -> 3, 1-0.7=0.3 -> 2, 1-1.0=0.0 -> 0
    assert result['disk_util_score'].tolist() == [3, 2, 0]


def test_score_metrics_slope_metrics(sample_df):
    df = sample_df[['bw_slope']].copy()
    result = score_metrics(df, {})
    # SLOPE_BINS ≈ [0.268, 0.577, 1.0, 1.732, 3.732]
    # 0.1 -> 0, 1.0 -> 3, 2.0 -> 4
    assert result['bw_slope_score'].tolist() == [0, 3, 4]


def test_score_metrics_intensity_metrics(sample_df):
    df = sample_df[['io_intensity_mean']].copy()
    result = score_metrics(df, {})
    # INTENSITY_BINS: approx [9.31e-10, 3.02e-8, 9.78e-7, 3.16e-5, 9.77e-4]
    # 1e-6 -> 3, 1e-5 -> 3, 1e-4 -> 4
    assert result['io_intensity_mean_score'].tolist() == [3, 3, 4]


def test_score_metrics_boundary_metrics(sample_df, metric_boundaries_sample):
    df = sample_df[['bw_mean', 'cpu_mean']].copy()
    result = score_metrics(df, metric_boundaries_sample)
    # bw_mean: 50/100=0.5 -> 2, but 1-0.5=0.5 -> 2
    # 75/100=0.75 -> 3, 1-0.75=0.25 -> 1
    # 100/100=1.0 -> 5, 1-1.0=0.0 -> 0
    assert result['bw_mean_score'].tolist() == [2, 1, 0]
    # cpu_mean: 40/50=0.8 -> 4, 60/50=1.2 -> 5, 80/50=1.6 -> 5
    assert result['cpu_mean_score'].tolist() == [4, 5, 5]


def test_score_metrics_with_nans(df_with_nans):
    result = score_metrics(df_with_nans, {})
    assert pd.isna(result['cpu_pct_score'].iloc[1])
    assert pd.isna(result['bw_slope_score'].iloc[2])


@pytest.mark.parametrize("value,expected", [
    (0.0, 0), (0.25, 1), (0.26, 2), (0.5, 2), (0.75, 3), (0.9, 4), (1.0, 5)
])
def test_score_metrics_percentage_boundary_values(value, expected):
    df = pd.DataFrame({'test_pct': [value]})
    result = score_metrics(df, {})
    assert result['test_pct_score'].iloc[0] == expected


@pytest.mark.parametrize("value,expected", [
    (0.0, 0), (0.268, 1), (0.577, 1), (1.0, 3), (1.732, 3), (4.0, 5)
])
def test_score_metrics_slope_boundary_values(value, expected):
    df = pd.DataFrame({'test_slope': [value]})
    result = score_metrics(df, {})
    assert result['test_slope_score'].iloc[0] == expected


def test_score_metrics_mixed_metrics(sample_df, metric_boundaries_sample):
    result = score_metrics(sample_df, metric_boundaries_sample)
    assert 'cpu_pct_score' in result.columns
    assert 'bw_slope_score' in result.columns
    assert 'bw_mean_score' in result.columns
    assert 'cpu_mean_score' in result.columns


def test_score_metrics_column_sorting(sample_df):
    result = score_metrics(sample_df, {})
    assert result.columns.is_monotonic_increasing


def test_score_metrics_score_types(sample_df):
    result = score_metrics(sample_df, {})
    for col in result.columns:
        if col.endswith('_score'):
            assert str(result[col].dtype) == 'Int64'


def test_score_metrics_no_boundaries(sample_df):
    result = score_metrics(sample_df, {})
    assert 'bw_mean_score' not in result.columns


def test_score_metrics_overlapping_metrics(sample_df, metric_boundaries_sample):
    result = score_metrics(sample_df, metric_boundaries_sample)
    # bw_mean is in both df and boundaries, should still score
    assert 'bw_mean_score' in result.columns


def test_score_metrics_large_df():
    df = pd.DataFrame({'cpu_pct': list(range(100))})
    result = score_metrics(df, {})
    assert len(result) == 100
    assert 'cpu_pct_score' in result.columns