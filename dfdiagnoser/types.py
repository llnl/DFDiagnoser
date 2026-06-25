import dataclasses as dc
import pandas as pd
from typing import Any, Dict, List, Literal, Optional, Tuple


FileOutputFormat = Literal["csv", "json", "parquet"]


@dc.dataclass
class TrendEvidence:
    prevalence: float
    persistence: int
    onset_window: int
    peak_severity_window: int
    last_seen_window: int
    support_windows: int
    trend_direction: str  # "worsening", "improving", "stable", "insufficient_data"


@dc.dataclass
class DiagnosisFinding:
    finding_type: str
    scope: str
    layer: Optional[str]
    motif: str  # "warmup_transient", "persistent_pressure", "rank_skew_induced", "checkpoint_tail_risk", "unclassified"
    severity: str  # human-readable label for logging
    severity_score: float  # continuous [0.0, 1.0] for gating/scaling
    confidence: float
    trend: TrendEvidence
    contributing_facts: List[Tuple[str, str]]  # list of (fact_type, scope)
    recommendation_bundle: str
    summary: str
    opportunity_tags: List[str] = dc.field(default_factory=list)
    suppresses_tags: List[str] = dc.field(default_factory=list)
    key_metrics: Dict[str, float] = dc.field(default_factory=dict)
    view_type: Optional[str] = None  # analyzer view_type this finding came from

    def to_wire_dict(self) -> Dict[str, Any]:
        """Serialize to the dict shape published to Mofka for the optimizer.

        Shared by the streaming publisher (DFOptimizer's input) and the offline
        ``findings.json`` writer so the two are byte-identical. The contextual
        ``publish_mode`` is added by the caller, not here.
        """
        return {
            "finding_type": self.finding_type,
            "scope": self.scope,
            "layer": self.layer,
            "view_type": self.view_type,
            "motif": self.motif,
            "severity": self.severity,
            "severity_score": self.severity_score,
            "confidence": self.confidence,
            "prevalence": self.trend.prevalence,
            "persistence": self.trend.persistence,
            "support_windows": self.trend.support_windows,
            "trend_direction": self.trend.trend_direction,
            "last_seen_window": self.trend.last_seen_window,
            "contributing_facts": self.contributing_facts,
            "recommendation_bundle": self.recommendation_bundle,
            "opportunity_tags": self.opportunity_tags,
            "suppresses_tags": self.suppresses_tags,
            "summary": self.summary,
            "window_index": self.trend.last_seen_window,
            "key_metrics": self.key_metrics,
        }


@dc.dataclass
class DiagnosisResult:
    findings: List[DiagnosisFinding] = dc.field(default_factory=list)
    # Deprecated: scoring moved to the analyzer; kept (empty) for output compat.
    flat_view_paths: List[str] = dc.field(default_factory=list)
    scored_flat_views: List[pd.DataFrame] = dc.field(default_factory=list)
