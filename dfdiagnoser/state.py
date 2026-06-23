import dataclasses as dc
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


@dc.dataclass
class FactObservation:
    window_index: int
    epoch: Optional[int]
    severity_score: float
    severity_label: str
    evidence: Dict[str, Any] = dc.field(default_factory=dict)
    opportunity_tags: List[str] = dc.field(default_factory=list)
    suppresses_tags: List[str] = dc.field(default_factory=list)
    view_type: Optional[str] = None  # the analyzer view_type (epoch/proc_name/file_name/...)


class FactTracker:
    """Tracks all observations of one (fact_type, scope) combination."""

    def __init__(self):
        self.observations: List[FactObservation] = []
        self._windows_seen: set = set()
        self._total_windows: int = 0

    def record(self, obs: FactObservation):
        self.observations.append(obs)
        self._windows_seen.add(obs.window_index)

    def prevalence(self, total_windows: Optional[int] = None) -> float:
        """Fraction of total windows where this fact was observed."""
        effective_total = total_windows if total_windows is not None else self._total_windows
        if effective_total == 0:
            return 0.0
        return len(self._windows_seen) / effective_total

    def persistence(self) -> int:
        """Longest consecutive run of windows with this fact."""
        if not self._windows_seen:
            return 0
        sorted_wins = sorted(self._windows_seen)
        longest = 1
        current = 1
        for i in range(1, len(sorted_wins)):
            if sorted_wins[i] == sorted_wins[i - 1] + 1:
                current += 1
                longest = max(longest, current)
            else:
                current = 1
        return longest

    def update_total_windows(self, total: int):
        self._total_windows = total

    def support_windows(self) -> int:
        return len(self._windows_seen)

    def last_seen_window(self) -> Optional[int]:
        if not self._windows_seen:
            return None
        return max(self._windows_seen)

    def observed_in_window(self, window_index: int) -> bool:
        return window_index in self._windows_seen


class DiagnosisStateStore:
    """In-memory store for longitudinal diagnosis state."""

    def __init__(self):
        self.current_window: int = 0
        self._min_window: Optional[int] = None
        self._trackers: Dict[Tuple[str, str], FactTracker] = defaultdict(FactTracker)
        self._scored_summaries: List[Dict[str, Any]] = []

    def record_fact(self, key: Tuple[str, str], obs: FactObservation):
        self._trackers[key].record(obs)
        # `current_window` tracks the LATEST window coordinate ingested (the real
        # analysis-window number), so the online control path finds just-arrived
        # facts via observed_in_window(current_window) and effective_total_windows
        # spans the true window range. See dfanalyzer docs/window-as-longitudinal-axis.md.
        if obs.window_index is not None:
            self.current_window = max(self.current_window, obs.window_index)
            self._min_window = (obs.window_index if self._min_window is None
                                else min(self._min_window, obs.window_index))

    def advance_window(self):
        # Explicit step (e.g. an empty/spatial window with no temporal facts to align on).
        self.current_window += 1
        if self._min_window is None:
            self._min_window = self.current_window
        for tracker in self._trackers.values():
            tracker.update_total_windows(self.current_window)

    def effective_total_windows(self) -> int:
        # Span of windows observed: [_min_window, current_window]. Base-agnostic, so
        # 1-based `window` (1..N) and 0-based epoch/step both yield the correct count.
        if self._min_window is None:
            return max(self.current_window, 1)
        return self.current_window - self._min_window + 1

    def record_scored_summary(self, scored_df: pd.DataFrame):
        """Extract and store summary stats from a scored flat view."""
        score_cols = [c for c in scored_df.columns if c.endswith("_score")]
        if not score_cols:
            return
        summary = {
            "window_index": self.current_window,
            "n_rows": len(scored_df),
        }
        for col in score_cols:
            vals = scored_df[col].dropna()
            if len(vals) > 0:
                summary[f"{col}_mean"] = float(vals.mean())
                summary[f"{col}_max"] = float(vals.max())
        self._scored_summaries.append(summary)

    def all_trackers(self) -> List[Tuple[Tuple[str, str], FactTracker]]:
        return list(self._trackers.items())
