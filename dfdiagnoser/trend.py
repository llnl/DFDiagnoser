"""Trend detection strategies for longitudinal severity analysis.

Three strategies are provided:

- **FixedWindowTrend** (default): Compares mean severity of the last N
  observations against the preceding N.  N defaults to ``cooldown_windows``
  so the comparison spans exactly one action–effect cycle.

- **EWMATrend**: Exponentially weighted moving average.  Uses the same
  technique as tf.data Autotune (step-level EMA) but applied to
  window-level severity scores.  λ = 2/(lookback+1).

- **CUSUMTrend**: Page's CUSUM change-point detector (1954).  Accumulates
  evidence of severity shifts without a fixed lookback window.
  Parameters *k* (reference value) and *h* (decision threshold) can be
  estimated from the baseline standard deviation.
"""

from abc import ABC, abstractmethod
import math
from typing import List, Literal

TrendDirection = Literal["improving", "worsening", "stable", "insufficient_data"]

# Relative change thresholds for classifying trend direction.
# If recent/older ratio < (1 - IMPROVING_MARGIN), trend is improving.
# If recent/older ratio > (1 + WORSENING_MARGIN), trend is worsening.
IMPROVING_MARGIN = 0.15
WORSENING_MARGIN = 0.15


class TrendStrategy(ABC):
    """Base class for trend detection strategies."""

    @abstractmethod
    def compute(self, severity_history: List[float]) -> TrendDirection:
        """Classify the trend direction from a severity time series."""


class FixedWindowTrend(TrendStrategy):
    """Compare mean severity of the last *lookback* observations vs the
    preceding *lookback*.

    With ``lookback = cooldown_windows`` (the default), the comparison
    spans exactly one optimisation action–effect cycle, directly answering
    "did the most recent action improve things?"
    """

    def __init__(self, lookback: int = 3):
        self.lookback = max(lookback, 1)

    def compute(self, severity_history: List[float]) -> TrendDirection:
        n = len(severity_history)
        if n < 2:
            return "insufficient_data"

        lb = min(self.lookback, n // 2)
        if lb < 1:
            return "insufficient_data"

        recent = severity_history[-lb:]
        older = severity_history[-2 * lb : -lb]
        if not older:
            return "insufficient_data"

        avg_recent = sum(recent) / len(recent)
        avg_older = sum(older) / len(older)

        if avg_older <= 0:
            return "stable"

        ratio = avg_recent / avg_older
        if ratio < (1.0 - IMPROVING_MARGIN):
            return "improving"
        if ratio > (1.0 + WORSENING_MARGIN):
            return "worsening"
        return "stable"


class EWMATrend(TrendStrategy):
    """Exponentially weighted moving average trend detector.

    Uses the same EMA technique as tf.data Autotune, but applied to
    window-level severity scores rather than per-step processing times.
    The smoothing factor λ = 2/(lookback+1) gives an effective window
    of *lookback* observations.
    """

    def __init__(self, lookback: int = 3):
        self.lam = 2.0 / (lookback + 1)

    def compute(self, severity_history: List[float]) -> TrendDirection:
        n = len(severity_history)
        if n < 3:
            return "insufficient_data"

        # Compute EWMA for the full series
        ewma = severity_history[0]
        ewma_series = [ewma]
        for s in severity_history[1:]:
            ewma = self.lam * s + (1.0 - self.lam) * ewma
            ewma_series.append(ewma)

        # Compare EWMA at midpoint vs end
        mid = n // 2
        ewma_mid = ewma_series[mid]
        ewma_end = ewma_series[-1]

        if ewma_mid <= 0:
            return "stable"

        ratio = ewma_end / ewma_mid
        if ratio < (1.0 - IMPROVING_MARGIN):
            return "improving"
        if ratio > (1.0 + WORSENING_MARGIN):
            return "worsening"
        return "stable"


class CUSUMTrend(TrendStrategy):
    """Page's CUSUM (Cumulative Sum) change-point detector.

    Detects shifts in severity without a fixed lookback window.
    Two-sided: tracks both positive shifts (worsening) and negative
    shifts (improving).

    Parameters
    ----------
    k : float or None
        Reference value (slack).  Recommended: σ/2 where σ is the
        baseline severity standard deviation.  If None, estimated as
        ``std(severity_history) / 2``.
    h : float
        Decision threshold.  Higher values reduce false alarms but
        increase detection delay.  Default 1.0 works well when k is
        estimated from data.
    """

    def __init__(self, k: float = None, h: float = 1.0):
        self._k_fixed = k
        self.h = h

    def compute(self, severity_history: List[float]) -> TrendDirection:
        n = len(severity_history)
        if n < 3:
            return "insufficient_data"

        mean = sum(severity_history) / n

        # Estimate k from data if not provided
        if self._k_fixed is not None:
            k = self._k_fixed
        else:
            variance = sum((x - mean) ** 2 for x in severity_history) / n
            k = math.sqrt(variance) / 2.0
            k = max(k, 1e-6)  # avoid zero

        # Two-sided CUSUM
        s_pos = 0.0  # detects positive shift (worsening)
        s_neg = 0.0  # detects negative shift (improving)

        for x in severity_history:
            s_pos = max(0.0, s_pos + (x - mean) - k)
            s_neg = max(0.0, s_neg - (x - mean) - k)

        if s_neg > self.h:
            return "improving"
        if s_pos > self.h:
            return "worsening"
        return "stable"


def get_trend_strategy(name: str = "fixed", lookback: int = 3, **kwargs) -> TrendStrategy:
    """Factory for trend strategies.

    Parameters
    ----------
    name : str
        One of "fixed", "ewma", "cusum".
    lookback : int
        Lookback window (used by fixed and ewma).
    **kwargs
        Extra parameters forwarded to the strategy constructor.
    """
    if name == "fixed":
        return FixedWindowTrend(lookback=lookback)
    if name == "ewma":
        return EWMATrend(lookback=lookback)
    if name == "cusum":
        return CUSUMTrend(**kwargs)
    raise ValueError(f"Unknown trend strategy: {name!r}")
