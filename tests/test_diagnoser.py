import json

import pytest

from dfdiagnoser.diagnoser import Diagnoser


pytestmark = [pytest.mark.smoke, pytest.mark.full]


class _FakeEvent:
    def __init__(self, payload: bytes):
        self.data = payload


def _record_window(diagnoser: Diagnoser, facts):
    envelope = {
        "view_type": "epoch",
        "facts": facts,
    }
    diagnoser._handle_analysis_facts(
        _FakeEvent(json.dumps(envelope).encode("utf-8")),
        metadata={},
    )
    diagnoser.state.advance_window()


def _finding_for(findings, fact_type: str, scope: str):
    for finding in findings:
        if finding.finding_type != fact_type:
            continue
        if (fact_type, scope) in finding.contributing_facts:
            return finding
    raise AssertionError(f"Missing finding for {fact_type}@{scope}")


def test_handle_analysis_facts_separates_layer_scoped_trackers():
    diagnoser = Diagnoser()
    envelope = {
        "view_type": "epoch",
        "facts": [
            {
                "fact_type": "small_read_dominance",
                "scope": {"layer": "reader_posix", "entity": "1", "rank_set": "all"},
                "window": {"epoch": 1},
                "severity": {"score": 0.7, "label": "high"},
                "opportunity_tags": ["small_io_reduction"],
                "evidence": {"metrics": {"reader_posix_read_time_frac_parent": 0.8}},
            },
            {
                "fact_type": "small_read_dominance",
                "scope": {"layer": "checkpoint_posix", "entity": "1", "rank_set": "all"},
                "window": {"epoch": 1},
                "severity": {"score": 0.6, "label": "high"},
                "opportunity_tags": ["small_io_reduction"],
                "evidence": {"metrics": {"checkpoint_posix_read_time_frac_parent": 0.9}},
            },
        ],
    }

    diagnoser._handle_analysis_facts(
        _FakeEvent(json.dumps(envelope).encode("utf-8")),
        metadata={},
    )

    tracker_keys = {key for key, _ in diagnoser.state.all_trackers()}
    assert ("small_read_dominance", "reader_posix:epoch") in tracker_keys
    assert ("small_read_dominance", "checkpoint_posix:epoch") in tracker_keys


def test_build_longitudinal_summary_classifies_reader_metadata_bound():
    diagnoser = Diagnoser()

    for share in (0.61, 0.64, 0.66, 0.63):
        _record_window(
            diagnoser,
            [
                {
                    "fact_type": "excessive_metadata_access",
                    "scope": {"layer": "reader_posix", "entity": "1", "rank_set": "all"},
                    "window": {"epoch": diagnoser.state.current_window + 1},
                    "severity": {"score": 0.7, "label": "high"},
                    "opportunity_tags": ["metadata_reduction"],
                    "evidence": {
                        "metrics": {
                            "reader_posix_metadata_time_frac_parent": share,
                            "reader_posix_open_time_frac_parent": share * 0.6,
                            "reader_posix_close_time_frac_parent": share * 0.2,
                            "reader_posix_seek_time_frac_parent": share * 0.1,
                            "reader_posix_stat_time_frac_parent": share * 0.1,
                        }
                    },
                }
            ],
        )

    finding = _finding_for(
        diagnoser._build_longitudinal_summary(),
        "excessive_metadata_access",
        "reader_posix:epoch",
    )

    assert finding.motif == "metadata_bound"
    assert finding.recommendation_bundle == "metadata_reduction"
    assert "peak_metadata_share=" in finding.summary


def test_build_longitudinal_summary_classifies_checkpoint_fragmentation():
    diagnoser = Diagnoser()

    for share, size_mib in ((0.68, 0.40), (0.72, 0.35), (0.70, 0.25)):
        _record_window(
            diagnoser,
            [
                {
                    "fact_type": "small_write_dominance",
                    "scope": {"layer": "checkpoint_posix", "entity": "1", "rank_set": "all"},
                    "window": {"epoch": diagnoser.state.current_window + 1},
                    "severity": {"score": 0.75, "label": "high"},
                    "opportunity_tags": ["small_io_reduction"],
                    "evidence": {
                        "metrics": {
                            "checkpoint_posix_write_time_frac_parent": share,
                            "checkpoint_posix_write_size_mean": size_mib * 1024 * 1024,
                        }
                    },
                }
            ],
        )

    finding = _finding_for(
        diagnoser._build_longitudinal_summary(),
        "small_write_dominance",
        "checkpoint_posix:epoch",
    )

    assert finding.motif == "checkpoint_fragmentation"
    assert finding.recommendation_bundle == "checkpoint_io_batching"
    assert "peak_write_share=" in finding.summary


def test_build_longitudinal_summary_classifies_read_dominant_steady_state():
    diagnoser = Diagnoser()

    for epoch in range(1, 5):
        _record_window(
            diagnoser,
            [
                {
                    "fact_type": "operation_imbalance",
                    "scope": {"layer": "reader_posix", "entity": "1", "rank_set": "all"},
                    "window": {"epoch": epoch},
                    "severity": {"score": 0.8, "label": "high"},
                    "opportunity_tags": ["read_write_rebalancing"],
                    "evidence": {
                        "metrics": {
                            "reader_posix_read_count_sum": 120.0,
                            "reader_posix_write_count_sum": 12.0,
                            "reader_posix_operation_imbalance_ratio": 9.0,
                        }
                    },
                },
                {
                    "fact_type": "size_imbalance",
                    "scope": {"layer": "reader_posix", "entity": "1", "rank_set": "all"},
                    "window": {"epoch": epoch},
                    "severity": {"score": 0.78, "label": "high"},
                    "opportunity_tags": ["read_write_rebalancing"],
                    "evidence": {
                        "metrics": {
                            "reader_posix_read_size_sum": 128.0 * 1024 * 1024,
                            "reader_posix_write_size_sum": 8.0 * 1024 * 1024,
                            "reader_posix_size_imbalance_ratio": 15.0,
                        }
                    },
                },
            ],
        )

    finding = _finding_for(
        diagnoser._build_longitudinal_summary(),
        "operation_imbalance",
        "reader_posix:epoch",
    )

    assert finding.motif == "read_dominant_steady_state"
    assert finding.recommendation_bundle == "investigate_read_heavy_phase"
    assert ("size_imbalance", "reader_posix:epoch") in finding.contributing_facts
    assert "dominant=read" in finding.summary


def test_build_control_findings_uses_current_window_and_fresh_scope():
    diagnoser = Diagnoser()

    first_envelope = {
        "view_type": "epoch",
        "facts": [
            {
                "fact_type": "excessive_metadata_access",
                "scope": {"layer": "reader_posix", "entity": "1", "rank_set": "all"},
                "window": {"epoch": 1},
                "severity": {"score": 0.7, "label": "high"},
                "opportunity_tags": ["metadata_reduction"],
                "evidence": {
                    "metrics": {
                        "reader_posix_metadata_time_frac_parent": 0.64,
                    }
                },
            }
        ],
    }

    first_keys = diagnoser._handle_analysis_facts(
        _FakeEvent(json.dumps(first_envelope).encode("utf-8")),
        metadata={},
    )
    first_control = diagnoser._build_control_findings(
        window_index=diagnoser.state.current_window,
        touched_keys=first_keys,
    )

    assert len(first_control) == 1
    assert first_control[0].scope == "reader_posix:epoch"
    assert first_control[0].trend.prevalence == pytest.approx(1.0)
    assert first_control[0].trend.support_windows == 1
    assert first_control[0].trend.last_seen_window == 0

    diagnoser.state.advance_window()

    second_envelope = {
        "view_type": "epoch",
        "facts": [
            {
                "fact_type": "small_write_dominance",
                "scope": {"layer": "checkpoint_posix", "entity": "1", "rank_set": "all"},
                "window": {"epoch": 2},
                "severity": {"score": 0.75, "label": "high"},
                "opportunity_tags": ["small_io_reduction"],
                "evidence": {
                    "metrics": {
                        "checkpoint_posix_write_time_frac_parent": 0.7,
                        "checkpoint_posix_write_size_mean": 0.4 * 1024 * 1024,
                    }
                },
            }
        ],
    }

    second_keys = diagnoser._handle_analysis_facts(
        _FakeEvent(json.dumps(second_envelope).encode("utf-8")),
        metadata={},
    )
    second_control = diagnoser._build_control_findings(
        window_index=diagnoser.state.current_window,
        touched_keys=second_keys,
    )

    assert len(second_control) == 1
    assert second_control[0].scope == "checkpoint_posix:epoch"
    assert second_control[0].finding_type == "small_write_dominance"
    assert all(
        finding.finding_type != "excessive_metadata_access"
        for finding in second_control
    )
