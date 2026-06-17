import numpy as np
import pandas as pd
from enum import Enum


class Score(Enum):
    TRIVIAL = 'trivial'
    LOW = 'low'
    MEDIUM = 'medium'
    HIGH = 'high'
    CRITICAL = 'critical'


INTENSITY_MIN = 1 / 1024
INTENSITY_MAX = 1 / 1024**3
INTENSITY_BINS = np.geomspace(INTENSITY_MAX, INTENSITY_MIN, num=5)
PERCENTAGE_BINS = [0, 0.25, 0.5, 0.75, 0.9]
SCORE_NAMES = [
    Score.TRIVIAL.value,
    Score.LOW.value,
    Score.MEDIUM.value,
    Score.HIGH.value,
    Score.CRITICAL.value,
]
SCORE_BINS = [1, 2, 3, 4, 5]
SLOPE_BINS = [
    np.tan(np.deg2rad(15)),  # ~0.27
    np.tan(np.deg2rad(30)),  # ~0.58
    np.tan(np.deg2rad(45)),  # 1.0
    np.tan(np.deg2rad(60)),  # ~1.73
    np.tan(np.deg2rad(75)),  # ~3.73
]


def score_metrics(df: pd.DataFrame, metric_boundaries: dict) -> pd.DataFrame:
    metrics = [col for col in df.columns if not col.startswith('d_')]

    df = df.copy()
    df[metrics] = df[metrics].apply(pd.to_numeric, errors='coerce')

    score_cols = {}

    for metric in metrics:
        score_col = f"{metric}_score"
        if metric.endswith('_pct') or metric.endswith('_per') or metric.endswith('_util'):
            metric_value = df[metric]
            if metric.endswith('_util'):
                metric_value = 1 - metric_value
            score_cols[score_col] = np.digitize(
                metric_value, bins=PERCENTAGE_BINS, right=True)
        elif metric.endswith('_slope'):
            score_cols[score_col] = np.digitize(
                df[metric], bins=SLOPE_BINS, right=True)
        elif metric.endswith('_intensity_mean'):
            score_cols[score_col] = np.digitize(
                df[metric], bins=INTENSITY_BINS, right=True)
        if score_col in score_cols:
            score_cols[score_col] = np.where(
                pd.isna(df[metric]), np.nan, score_cols[score_col])

    for metric in metric_boundaries:
        score_col = f"{metric}_score"
        metric_pct = df[metric] / metric_boundaries[metric]
        if 'bw_mean' in metric:
            metric_pct = 1 - metric_pct
        score_cols[score_col] = np.digitize(
            metric_pct, bins=PERCENTAGE_BINS, right=True)
        score_cols[score_col] = np.where(
            np.isnan(df[metric]), np.nan, score_cols[score_col])

    if score_cols:
        score_df = pd.DataFrame(score_cols, index=df.index)
        score_df = score_df.astype('Int64')
        df = pd.concat([df, score_df], axis=1)

    return df.sort_index(axis=1)
