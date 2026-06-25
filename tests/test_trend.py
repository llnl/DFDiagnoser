import pytest

from dfdiagnoser.trend import (
    CUSUMTrend,
    EWMATrend,
    FixedWindowTrend,
    get_trend_strategy,
)


class TestFixedWindowTrend:
    def test_improving(self):
        strategy = FixedWindowTrend(lookback=3)
        # Older windows high, recent windows low
        history = [0.8, 0.7, 0.75, 0.3, 0.25, 0.2]
        assert strategy.compute(history) == "improving"

    def test_worsening(self):
        strategy = FixedWindowTrend(lookback=3)
        # Older windows low, recent windows high
        history = [0.2, 0.25, 0.3, 0.7, 0.75, 0.8]
        assert strategy.compute(history) == "worsening"

    def test_stable(self):
        strategy = FixedWindowTrend(lookback=3)
        # Similar severity throughout
        history = [0.5, 0.52, 0.48, 0.51, 0.49, 0.50]
        assert strategy.compute(history) == "stable"

    def test_insufficient_data(self):
        strategy = FixedWindowTrend(lookback=3)
        assert strategy.compute([0.5]) == "insufficient_data"
        assert strategy.compute([]) == "insufficient_data"

    def test_lookback_respects_short_history(self):
        """Lookback adapts when history is shorter than 2*lookback."""
        strategy = FixedWindowTrend(lookback=5)
        # Only 4 observations — lookback truncated to 2
        history = [0.8, 0.7, 0.3, 0.2]
        assert strategy.compute(history) == "improving"

    def test_matches_cooldown_span(self):
        """With lookback=3 (cooldown), compares last 3 vs prior 3 windows."""
        strategy = FixedWindowTrend(lookback=3)
        # Windows 0-2: sev ~0.7, Windows 3-5: sev ~0.4
        history = [0.7, 0.72, 0.68, 0.4, 0.38, 0.42]
        assert strategy.compute(history) == "improving"


class TestEWMATrend:
    def test_improving(self):
        strategy = EWMATrend(lookback=3)
        history = [0.8, 0.7, 0.6, 0.4, 0.3, 0.2]
        assert strategy.compute(history) == "improving"

    def test_worsening(self):
        strategy = EWMATrend(lookback=3)
        history = [0.2, 0.3, 0.4, 0.6, 0.7, 0.8]
        assert strategy.compute(history) == "worsening"

    def test_stable(self):
        strategy = EWMATrend(lookback=3)
        history = [0.5, 0.52, 0.48, 0.51, 0.49, 0.50]
        assert strategy.compute(history) == "stable"

    def test_insufficient_data(self):
        strategy = EWMATrend(lookback=3)
        assert strategy.compute([0.5, 0.4]) == "insufficient_data"

    def test_smooths_noise(self):
        """EWMA should classify a noisy-but-flat series as stable."""
        strategy = EWMATrend(lookback=5)
        # Noisy around 0.5
        history = [0.5, 0.6, 0.4, 0.55, 0.45, 0.5, 0.52, 0.48]
        assert strategy.compute(history) == "stable"


class TestCUSUMTrend:
    def test_improving(self):
        # CUSUM needs longer series or lower h for short shifts
        strategy = CUSUMTrend(h=0.3)
        history = [0.8, 0.8, 0.8, 0.3, 0.3, 0.3]
        assert strategy.compute(history) == "improving"

    def test_worsening(self):
        strategy = CUSUMTrend(h=0.3)
        history = [0.3, 0.3, 0.3, 0.8, 0.8, 0.8]
        assert strategy.compute(history) == "worsening"

    def test_stable(self):
        strategy = CUSUMTrend(h=0.3)
        history = [0.5, 0.52, 0.48, 0.51, 0.49, 0.50]
        assert strategy.compute(history) == "stable"

    def test_insufficient_data(self):
        strategy = CUSUMTrend(h=1.0)
        assert strategy.compute([0.5, 0.4]) == "insufficient_data"

    def test_improving_long_series(self):
        """CUSUM excels at detecting gradual shifts in longer series."""
        strategy = CUSUMTrend(h=1.0)
        history = [0.8] * 8 + [0.3] * 8
        assert strategy.compute(history) == "improving"

    def test_fixed_k_parameter(self):
        """With explicit k, CUSUM uses it instead of estimating from data."""
        strategy = CUSUMTrend(k=0.05, h=0.25)
        history = [0.7, 0.7, 0.7, 0.4, 0.4, 0.4]
        assert strategy.compute(history) == "improving"


class TestTrendFactory:
    def test_fixed(self):
        s = get_trend_strategy("fixed", lookback=5)
        assert isinstance(s, FixedWindowTrend)
        assert s.lookback == 5

    def test_ewma(self):
        s = get_trend_strategy("ewma", lookback=4)
        assert isinstance(s, EWMATrend)

    def test_cusum(self):
        s = get_trend_strategy("cusum", k=0.1, h=2.0)
        assert isinstance(s, CUSUMTrend)
        assert s.h == 2.0

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown trend strategy"):
            get_trend_strategy("unknown")
