import glob
import io
import json
import os
import signal
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import structlog

from .scoring import score_metrics
from .trend import TrendStrategy, get_trend_strategy
from .types import DiagnosisResult
from .utils.log_utils import console_block

logger = structlog.get_logger()

_shutdown_requested = False


def _sigterm_handler(signum, frame):
    del signum, frame
    global _shutdown_requested
    _shutdown_requested = True


def install_shutdown_handler():
    global _shutdown_requested
    _shutdown_requested = False
    signal.signal(signal.SIGTERM, _sigterm_handler)


class Diagnoser:
    def __init__(self, trend_strategy: str = "fixed", trend_lookback: int = 3,
                 **trend_kwargs):
        from .state import DiagnosisStateStore

        self.state = DiagnosisStateStore()
        self._trend: TrendStrategy = get_trend_strategy(
            trend_strategy, lookback=trend_lookback, **trend_kwargs,
        )

    def diagnose_checkpoint(self, checkpoint_dir: str, metric_boundaries: dict = {}):
        if not os.path.exists(checkpoint_dir):
            raise FileNotFoundError(
                f"Checkpoint directory {checkpoint_dir} does not exist"
            )
        if not os.path.isdir(checkpoint_dir):
            raise NotADirectoryError(
                f"Checkpoint directory {checkpoint_dir} is not a directory"
            )
        if not os.listdir(checkpoint_dir):
            raise ValueError(f"Checkpoint directory {checkpoint_dir} is empty")

        with console_block("Load raw stats"):
            raw_stats_paths = glob.glob(
                os.path.join(checkpoint_dir, "_raw_stats_*.json")
            )
            if not raw_stats_paths:
                raise ValueError(
                    f"Checkpoint directory {checkpoint_dir} does not contain any raw stats files"
                )
            with open(raw_stats_paths[0], "r") as f:
                raw_stats = json.load(f)
        flat_view_paths = glob.glob(
            os.path.join(checkpoint_dir, "_flat_view_*.parquet")
        )
        if not flat_view_paths:
            raise ValueError(
                f"Checkpoint directory {checkpoint_dir} does not contain any flat view files"
            )

        with console_block("Score flat views"):
            scored_flat_views = []
            for flat_view_path in flat_view_paths:
                flat_view = pd.read_parquet(flat_view_path)
                scored_flat_view = score_metrics(flat_view, metric_boundaries)
                scored_flat_views.append(scored_flat_view)

        return DiagnosisResult(
            flat_view_paths=flat_view_paths,
            scored_flat_views=scored_flat_views,
        )

    def diagnose_facts(self, facts_path: str, output_handler=None) -> DiagnosisResult:
        """Offline replay: read saved analyzer fact envelopes and accumulate them
        through the same longitudinal pipeline the Mofka stream uses, then build
        and return findings. Requires no Mofka/streaming dependency.

        ``facts_path`` may be a ``.jsonl`` file (one ``analyzer.fact-envelope.v1``
        object per line) or a directory of per-window envelope ``.json`` files
        (read in sorted order).
        """
        if not os.path.exists(facts_path):
            raise FileNotFoundError(f"Facts path {facts_path} does not exist")

        with console_block("Replay fact envelopes"):
            window_count = 0
            for envelope in self._read_fact_envelopes(facts_path):
                self._ingest_fact_envelope(envelope)
                # Advance the window after each envelope, exactly as the stream
                # loop does after each analysis_facts event, so persistence and
                # prevalence match the online path (online/offline parity).
                self.state.advance_window()
                window_count += 1

        logger.info("diagnoser.facts.replayed", windows=window_count, path=facts_path)

        findings = self._build_longitudinal_summary()
        for finding in findings:
            logger.info(
                "diagnoser.finding",
                finding_type=finding.finding_type,
                scope=finding.scope,
                layer=finding.layer,
                motif=finding.motif,
                severity=finding.severity,
                confidence=round(finding.confidence, 4),
                prevalence=round(finding.trend.prevalence, 4),
                persistence=finding.trend.persistence,
                trend_direction=finding.trend.trend_direction,
                summary=finding.summary,
            )

        result = DiagnosisResult(
            flat_view_paths=[],
            scored_flat_views=[],
            findings=findings,
        )
        if output_handler is not None:
            output_handler(result)
        return result

    @staticmethod
    def _read_fact_envelopes(facts_path: str):
        """Yield fact-envelope dicts from a ``.jsonl`` file (one per line) or a
        directory of per-window envelope ``.json`` files (sorted by name)."""
        if os.path.isdir(facts_path):
            paths = sorted(glob.glob(os.path.join(facts_path, "*.json")))
            if not paths:
                raise ValueError(f"No .json envelope files found in {facts_path}")
            for p in paths:
                with open(p, "r", encoding="utf-8") as f:
                    yield json.load(f)
        else:
            with open(facts_path, "r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise ValueError(
                            f"Invalid JSON on line {line_no} of {facts_path}: {exc}"
                        ) from exc

    def diagnose_mofka(
        self,
        group_file: str,
        topic_name: str,
        metric_boundaries: dict = {},
        stop_name: str = "end",
        output_handler=None,
        consumer_name: str = "",
        idle_timeout_sec: int = 0,
        pull_timeout_ms: int = 1000,
        output_topic: str = "",
    ):
        from .streaming.mofka_io import open_consumer, open_producer

        output_handler = output_handler or (lambda result: None)

        driver, consumer = open_consumer(
            group_file, topic_name, consumer_name=consumer_name or None
        )

        # Open producer for publishing findings to optimizer
        findings_producer = None
        if output_topic:
            try:
                _, findings_producer = open_producer(group_file, output_topic)
                logger.info("diagnoser.findings_producer.open", topic=output_topic)
            except Exception:
                logger.warning("diagnoser.findings_producer.failed", exc_info=True)

        event_count = 0
        flat_view_count = 0
        facts_count = 0
        error_count = 0
        last_event_time = None  # None until first event received

        logger.info(
            "diagnoser.stream.start",
            topic=topic_name,
            idle_timeout_sec=idle_timeout_sec,
            pull_timeout_ms=pull_timeout_ms,
        )

        try:
            install_shutdown_handler()
            timeout_count = 0
            wait_ms = pull_timeout_ms if pull_timeout_ms > 0 else 1000

            future = consumer.pull()
            while not _shutdown_requested:
                # Check idle timeout (only after first event received)
                now = time.monotonic()
                if (
                    last_event_time is not None
                    and idle_timeout_sec > 0
                    and (now - last_event_time) >= idle_timeout_sec
                ):
                    logger.info(
                        "diagnoser.stream.idle_timeout",
                        idle_sec=round(now - last_event_time, 1),
                        threshold_sec=idle_timeout_sec,
                        timeout_count=timeout_count,
                    )
                    break

                # Wait on current future; timeout is raised as exception
                try:
                    event = future.wait(timeout_ms=wait_ms)
                except Exception as ex:
                    ex_msg = str(ex).lower()
                    if "timeout" in ex_msg:
                        timeout_count += 1
                        continue
                    raise

                if event is None:
                    timeout_count += 1
                    continue

                last_event_time = time.monotonic()
                event_count += 1
                raw_metadata = event.metadata if hasattr(event, "metadata") else None
                if isinstance(raw_metadata, dict):
                    metadata = raw_metadata
                elif isinstance(raw_metadata, str):
                    try:
                        metadata = json.loads(raw_metadata)
                    except (ValueError, TypeError):
                        metadata = {"raw": raw_metadata}
                else:
                    metadata = {}

                artifact_type = metadata.get("artifact_type", "flat_view")
                payload = event.data
                payload_size = 0
                if payload is not None:
                    if isinstance(payload, list):
                        payload_size = sum(len(p) for p in payload)
                    elif isinstance(payload, (bytes, bytearray)):
                        payload_size = len(payload)

                logger.info(
                    "diagnoser.event.received",
                    event_index=event_count,
                    artifact_type=artifact_type,
                    metadata_keys=list(metadata.keys()),
                    payload_size=payload_size,
                    timeouts_before=timeout_count,
                )
                timeout_count = 0

                # Check for stop sentinel
                if metadata.get("name") == stop_name:
                    logger.info("diagnoser.stream.stop_sentinel", event_count=event_count)
                    event.acknowledge()
                    break

                try:
                    if artifact_type == "analysis_facts":
                        touched_keys = self._handle_analysis_facts(event, metadata)
                        facts_count += 1
                        # Emit only current-window control findings so the
                        # optimizer acts on fresh state rather than replayed
                        # longitudinal snapshots.
                        if findings_producer is not None:
                            control_findings = self._build_control_findings(
                                window_index=self.state.current_window,
                                touched_keys=touched_keys,
                            )
                            if control_findings:
                                self._publish_findings(
                                    findings_producer,
                                    control_findings,
                                    publish_mode="control",
                                )
                                logger.info(
                                    "diagnoser.findings.control",
                                    count=len(control_findings),
                                    window=self.state.current_window,
                                )
                    else:
                        self._handle_flat_view(
                            event, metadata, metric_boundaries, output_handler
                        )
                        flat_view_count += 1
                except Exception:
                    error_count += 1
                    logger.exception(
                        "diagnoser.event.error",
                        artifact_type=artifact_type,
                        event_index=event_count,
                    )

                # Only advance the analysis window after facts events (epoch
                # boundaries), not after flat_view events which are just scored
                # data.  This ensures consecutive epochs produce consecutive
                # window indices so persistence tracking works correctly.
                if artifact_type == "analysis_facts":
                    self.state.advance_window()
                event.acknowledge()
                future = consumer.pull()

            if _shutdown_requested:
                logger.info("diagnoser.stream.stop_signal", signal="SIGTERM")

        finally:
            logger.info(
                "diagnoser.stream.done",
                event_count=event_count,
                flat_view_count=flat_view_count,
                facts_count=facts_count,
                error_count=error_count,
            )

            # Build longitudinal summary
            findings = self._build_longitudinal_summary()
            if findings:
                for finding in findings:
                    logger.info(
                        "diagnoser.finding",
                        finding_type=finding.finding_type,
                        scope=finding.scope,
                        layer=finding.layer,
                        motif=finding.motif,
                        severity=finding.severity,
                        confidence=round(finding.confidence, 4),
                        prevalence=round(finding.trend.prevalence, 4),
                        persistence=finding.trend.persistence,
                        support_windows=finding.trend.support_windows,
                        last_seen_window=finding.trend.last_seen_window,
                        trend_direction=finding.trend.trend_direction,
                        opportunity_tags=finding.opportunity_tags,
                        contributing_facts=finding.contributing_facts,
                        summary=finding.summary,
                    )

                # Publish findings to Mofka for optimizer consumption
                if findings_producer is not None:
                    self._publish_findings(
                        findings_producer,
                        findings,
                        publish_mode="summary",
                    )

            del consumer
            del driver

    def _handle_flat_view(self, event, metadata, metric_boundaries, output_handler):
        payload = event.data
        if payload is None:
            logger.warning("diagnoser.flat_view.no_data")
            return
        if isinstance(payload, list):
            if not payload:
                logger.warning("diagnoser.flat_view.empty_payload")
                return
            payload = b"".join(payload)

        flat_view = pd.read_parquet(io.BytesIO(payload))
        scored_flat_view = score_metrics(flat_view, metric_boundaries)

        # Record score summaries into state
        self.state.record_scored_summary(scored_flat_view)

        result = DiagnosisResult(
            flat_view_paths=[],
            scored_flat_views=[scored_flat_view],
        )
        output_handler(result)

        logger.info(
            "diagnoser.flat_view.scored",
            rows=len(flat_view),
            view_type=metadata.get("view_type", "unknown"),
        )

    def _handle_analysis_facts(self, event, metadata):
        """Mofka-path wrapper: decode the event payload into a fact envelope,
        then delegate to the shared, transport-agnostic ingest core so the
        streaming and offline paths accumulate state identically."""
        payload = event.data
        if payload is None:
            logger.warning("diagnoser.analysis_facts.no_data")
            return set()
        if isinstance(payload, list):
            if not payload:
                logger.warning("diagnoser.analysis_facts.empty_payload")
                return set()
            payload = b"".join(payload)

        envelope = json.loads(payload.decode("utf-8"))
        return self._ingest_fact_envelope(envelope)

    def _ingest_fact_envelope(self, envelope: dict):
        """Accumulate one analyzer fact envelope into longitudinal state.

        Transport-agnostic: shared verbatim by the Mofka stream path
        (``_handle_analysis_facts``) and the offline replay path
        (``diagnose_facts``). Because both feed the same envelope dicts produced
        by ``AnalysisResult.to_fact_envelope()``, online and offline produce
        byte-identical findings. Returns the set of (fact_type, scope) keys
        touched in this window.
        """
        from .state import FactObservation

        facts = envelope.get("facts", [])

        logger.info(
            "analysis_facts.received",
            fact_count=len(facts),
            view_type=envelope.get("view_type", "unknown"),
        )

        touched_keys = set()
        for fact in facts:
            logger.debug("diagnoser.fact.detail", **fact)
            # severity is a nested dict: {"score": float, "label": str, ...}
            severity = fact.get("severity", {})
            if isinstance(severity, dict):
                severity_score = severity.get("score", 0)
                severity_label = severity.get("label", "unknown")
            else:
                severity_score = float(severity) if severity else 0
                severity_label = "unknown"

            # The fact's window carries its view_type and epoch.
            window = fact.get("window", {})
            view_type = window.get("view_type") if isinstance(window, dict) else None
            epoch = window.get("epoch") if isinstance(window, dict) else None

            # scope is a nested dict: {"entity": str, "layer": str|null, ...}.
            # Per-row (numeric) and whole-window entities have no stable identity
            # across windows, so key them by the fact's own view_type
            # (window.view_type) — this keeps e.g. epoch and time_range distinct
            # and lets each accumulate longitudinally. Named entities (proc_name,
            # file_name) key by the entity, giving per-process / per-file findings.
            scope = fact.get("scope", "global")
            if isinstance(scope, dict):
                node = scope.get("node", "")
                layer = scope.get("layer")
                entity = str(scope.get("entity", "global"))
                if entity.isdigit() or entity == "window":
                    scope_key = view_type or "global"
                else:
                    scope_key = entity
                if layer:
                    scope_key = f"{layer}:{scope_key}"
                # Per-node scope: prepend node for independent tracking
                if node:
                    scope_key = f"node:{node}:{scope_key}" if scope_key else f"node:{node}"
            else:
                scope_key = str(scope)

            obs = FactObservation(
                window_index=self.state.current_window,
                epoch=epoch,
                severity_score=severity_score,
                severity_label=severity_label,
                evidence=fact.get("evidence", {}),
                opportunity_tags=fact.get("opportunity_tags", []),
                suppresses_tags=fact.get("suppresses_tags", []),
                view_type=view_type,
            )
            key = (fact.get("fact_type", "unknown"), scope_key)
            self.state.record_fact(key, obs)
            touched_keys.add(key)

            logger.info(
                "diagnoser.fact.recorded",
                window_index=self.state.current_window,
                fact_type=fact.get("fact_type"),
                scope=scope_key,
                view_type=view_type,
                severity_score=round(severity_score, 3),
                severity_label=severity_label,
                opportunity_tags=fact.get("opportunity_tags", []),
                epoch=epoch,
            )

        return touched_keys

    def _build_control_findings(self, window_index: int, touched_keys):
        return self._build_findings(
            window_index=window_index,
            touched_keys=touched_keys,
        )

    def _build_longitudinal_summary(self):
        return self._build_findings()

    def _build_findings(self, window_index: Optional[int] = None, touched_keys=None):
        from .types import DiagnosisFinding, TrendEvidence

        findings = []
        tracker_map = dict(self.state.all_trackers())
        total_windows = self.state.effective_total_windows()

        for key in sorted(tracker_map):
            tracker = tracker_map[key]
            if touched_keys is not None and key not in touched_keys:
                continue
            if window_index is not None and not tracker.observed_in_window(window_index):
                continue

            fact_type, scope = key
            prevalence = tracker.prevalence(total_windows=total_windows)
            persistence = tracker.persistence()
            support_windows = tracker.support_windows()
            last_seen_window = tracker.last_seen_window()

            if not tracker.observations:
                continue

            onset_window = tracker.observations[0].window_index
            peak_obs = max(tracker.observations, key=lambda o: o.severity_score)
            peak_window = peak_obs.window_index
            if last_seen_window is None:
                last_seen_window = peak_window

            # Determine trend direction via pluggable strategy
            severity_series = [o.severity_score for o in tracker.observations]
            trend_direction = self._trend.compute(severity_series)

            trend = TrendEvidence(
                prevalence=prevalence,
                persistence=persistence,
                onset_window=onset_window,
                peak_severity_window=peak_window,
                last_seen_window=last_seen_window,
                support_windows=support_windows,
                trend_direction=trend_direction,
            )

            # Motif classification
            motif, recommendation, confidence, contributing_facts = self._classify_motif(
                fact_type,
                scope,
                tracker,
                prevalence,
                persistence,
                onset_window,
                trend_direction,
                tracker_map,
                total_windows,
            )

            # Collect all opportunity_tags from observations (deduplicated, ordered)
            all_tags = []
            seen_tags = set()
            all_suppresses = []
            seen_suppresses = set()
            for obs in tracker.observations:
                for tag in obs.opportunity_tags:
                    if tag not in seen_tags and tag != "none":
                        all_tags.append(tag)
                        seen_tags.add(tag)
                for tag in obs.suppresses_tags:
                    if tag not in seen_suppresses:
                        all_suppresses.append(tag)
                        seen_suppresses.add(tag)

            summary = self._build_finding_summary(
                fact_type=fact_type,
                scope=scope,
                motif=motif,
                prevalence=prevalence,
                persistence=persistence,
                trend_direction=trend_direction,
                peak_obs=peak_obs,
                contributing_facts=contributing_facts,
            )

            layer, _ = self._split_scope(scope)
            # view_type is consistent within a tracker since the scope key
            # derives from it; take it from the peak obs.
            view_type = peak_obs.view_type
            # Forward evidence metrics from the peak observation so the
            # optimizer can compute target values (e.g., Amdahl's Law).
            peak_metrics = self._observation_metrics(peak_obs)
            # Filter to float-convertible, non-null values only.
            # The fact engine converts NaN/NA to None via _to_scalar,
            # so we must handle None explicitly.
            key_metrics = {}
            for k, v in peak_metrics.items():
                if v is None:
                    continue
                try:
                    fv = float(v)
                    # Skip NaN (can appear if _to_scalar didn't catch it)
                    if fv != fv:  # NaN check
                        continue
                    key_metrics[k] = fv
                except (TypeError, ValueError):
                    pass
            finding = DiagnosisFinding(
                finding_type=fact_type,
                scope=scope,
                layer=layer,
                motif=motif,
                severity=peak_obs.severity_label,
                severity_score=peak_obs.severity_score,
                confidence=confidence,
                trend=trend,
                contributing_facts=contributing_facts,
                recommendation_bundle=recommendation,
                summary=summary,
                opportunity_tags=all_tags,
                suppresses_tags=all_suppresses,
                key_metrics=key_metrics,
                view_type=view_type,
            )
            findings.append(finding)

        return findings

    @staticmethod
    def _split_scope(scope: str) -> Tuple[Optional[str], str]:
        layer, sep, entity = scope.partition(":")
        if sep:
            return layer, entity
        return None, scope

    @staticmethod
    def _observation_metrics(observation) -> Dict[str, Any]:
        if not isinstance(observation.evidence, dict):
            return {}
        metrics = observation.evidence.get("metrics", {})
        return metrics if isinstance(metrics, dict) else {}

    @staticmethod
    def _metric_by_suffix(metrics: Dict[str, Any], suffix: str) -> Optional[float]:
        for key, value in metrics.items():
            if not key.endswith(suffix):
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    def _dominant_imbalance_side(self, fact_type: str, observation) -> Optional[str]:
        metrics = self._observation_metrics(observation)
        if fact_type == "operation_imbalance":
            read_value = self._metric_by_suffix(metrics, "read_count_sum")
            write_value = self._metric_by_suffix(metrics, "write_count_sum")
        elif fact_type == "size_imbalance":
            read_value = self._metric_by_suffix(metrics, "read_size_sum")
            write_value = self._metric_by_suffix(metrics, "write_size_sum")
        else:
            return None

        if read_value is None or write_value is None or read_value == write_value:
            return None
        return "read" if read_value > write_value else "write"

    def _build_finding_summary(
        self,
        *,
        fact_type: str,
        scope: str,
        motif: str,
        prevalence: float,
        persistence: int,
        trend_direction: str,
        peak_obs,
        contributing_facts: List[Tuple[str, str]],
    ) -> str:
        metrics = self._observation_metrics(peak_obs)
        parts = [
            f"{fact_type}({scope})",
            f"motif={motif}",
            f"prevalence={prevalence:.2f}",
            f"persistence={persistence}",
            f"trend={trend_direction}",
        ]

        if fact_type == "metadata_dominance":
            peak_share = self._metric_by_suffix(metrics, "metadata_time_frac_parent")
            if peak_share is not None:
                parts.append(f"peak_metadata_share={peak_share:.2f}")
        elif fact_type == "small_read_dominance":
            peak_share = self._metric_by_suffix(metrics, "read_time_frac_parent")
            peak_size = self._metric_by_suffix(metrics, "read_size_mean")
            if peak_share is not None:
                parts.append(f"peak_read_share={peak_share:.2f}")
            if peak_size is not None:
                parts.append(f"peak_mean_size_mib={peak_size / (1024 ** 2):.3f}")
        elif fact_type == "small_write_dominance":
            peak_share = self._metric_by_suffix(metrics, "write_time_frac_parent")
            peak_size = self._metric_by_suffix(metrics, "write_size_mean")
            if peak_share is not None:
                parts.append(f"peak_write_share={peak_share:.2f}")
            if peak_size is not None:
                parts.append(f"peak_mean_size_mib={peak_size / (1024 ** 2):.3f}")
        elif fact_type in {"operation_imbalance", "size_imbalance"}:
            ratio_suffix = "operation_imbalance_ratio" if fact_type == "operation_imbalance" else "size_imbalance_ratio"
            peak_ratio = self._metric_by_suffix(metrics, ratio_suffix)
            dominant_side = self._dominant_imbalance_side(fact_type, peak_obs)
            if dominant_side:
                parts.append(f"dominant={dominant_side}")
            if peak_ratio is not None:
                parts.append(f"peak_ratio={peak_ratio:.2f}")

        paired_facts = sorted({name for name, fact_scope in contributing_facts if fact_scope == scope and name != fact_type})
        if paired_facts:
            parts.append(f"paired_with={'+'.join(paired_facts)}")

        return ", ".join(parts)

    def _classify_motif(
        self,
        fact_type,
        scope,
        tracker,
        prevalence,
        persistence,
        onset_window,
        trend_direction,
        tracker_map: Dict[Tuple[str, str], Any],
        total_windows: int,
    ):
        contributing_facts = [(fact_type, scope)]
        layer, _ = self._split_scope(scope)

        # warmup_transient: high severity in first 1-2 windows, declining after
        if onset_window <= 1 and trend_direction == "improving" and prevalence < 0.4:
            return "warmup_transient", "none", 0.7, contributing_facts

        if fact_type == "metadata_dominance":
            if layer == "reader_posix" and prevalence > 0.5 and persistence > 2:
                return "metadata_bound", "metadata_reduction", 0.8, contributing_facts
            if layer == "checkpoint_posix" and prevalence > 0.3 and persistence > 1:
                return "checkpoint_metadata_overhead", "checkpoint_metadata_reduction", 0.75, contributing_facts

        if fact_type in {"small_read_dominance", "small_write_dominance"}:
            if layer == "reader_posix" and prevalence > 0.5 and persistence > 3:
                return "small_io_input_pressure", "investigate_small_io_reader", 0.8, contributing_facts
            if layer == "checkpoint_posix" and fact_type == "small_write_dominance" and prevalence > 0.3 and persistence > 2:
                return "checkpoint_fragmentation", "checkpoint_io_batching", 0.8, contributing_facts

        if fact_type in {"operation_imbalance", "size_imbalance"}:
            paired_fact_type = "size_imbalance" if fact_type == "operation_imbalance" else "operation_imbalance"
            paired_tracker = tracker_map.get((paired_fact_type, scope))
            if paired_tracker and paired_tracker.observations:
                current_side = self._dominant_imbalance_side(fact_type, tracker.observations[-1])
                paired_side = self._dominant_imbalance_side(paired_fact_type, paired_tracker.observations[-1])
                joint_prevalence = min(
                    prevalence,
                    paired_tracker.prevalence(total_windows=total_windows),
                )
                joint_persistence = min(persistence, paired_tracker.persistence())
                if (
                    current_side
                    and paired_side
                    and current_side == paired_side
                    and joint_prevalence > 0.4
                    and joint_persistence > 2
                ):
                    recommendation = (
                        "checkpoint_io_batching"
                        if layer == "checkpoint_posix" and current_side == "write"
                        else f"investigate_{current_side}_heavy_phase"
                    )
                    motif = (
                        "read_dominant_steady_state"
                        if current_side == "read"
                        else "write_dominant_steady_state"
                    )
                    confidence = 0.85 if joint_persistence > 3 else 0.75
                    return motif, recommendation, confidence, [(fact_type, scope), (paired_fact_type, scope)]

        # rank_skew_induced: co-occurrence of fetch_imbalance + straggler
        all_fact_types = {k[0] for k, _ in self.state.all_trackers()}
        if (
            {"fetch_rank_imbalance", "epoch_straggler"}.issubset(all_fact_types)
            and fact_type in ("fetch_rank_imbalance", "epoch_straggler")
        ):
            return "rank_skew_induced", "rank_balance_repartition", 0.75, contributing_facts

        # checkpoint_tail_risk
        if fact_type == "checkpoint_tail_skew" and prevalence > 0.3:
            return "checkpoint_tail_risk", "checkpoint_io_batching", 0.65, contributing_facts

        # persistent_pressure: prevalence > 0.5, persistence > 3 for pipeline pressure facts
        if fact_type in {"fetch_pressure", "fetch_interval_pressure"} and prevalence > 0.5 and persistence > 3:
            return "persistent_pressure", "input_pipeline_tuning", 0.8, contributing_facts

        return "unclassified", "investigate", 0.5, contributing_facts

    def _publish_findings(self, producer, findings, publish_mode: str):
        """Publish DiagnosisFindings to Mofka for optimizer consumption."""
        for finding in findings:
            logger.debug(
                "diagnoser.finding.detail",
                finding_type=finding.finding_type,
                opportunity_tags=finding.opportunity_tags,
                key_metrics=finding.key_metrics,
                severity=finding.severity,
                persistence=finding.trend.persistence,
                trend_direction=finding.trend.trend_direction,
                publish_mode=publish_mode,
            )
            # Shared serializer (parity with offline findings.json output).
            payload_dict = finding.to_wire_dict()
            payload_dict["publish_mode"] = publish_mode
            payload = json.dumps(payload_dict).encode("utf-8")
            metadata = {
                "type": "diagnosis_finding",
                "finding_type": finding.finding_type,
                "scope": finding.scope,
                "layer": finding.layer,
                "motif": finding.motif,
                "publish_mode": publish_mode,
            }
            try:
                producer.push(metadata=metadata, data=payload)
                logger.info(
                    "diagnoser.finding.published",
                    finding_type=finding.finding_type,
                    scope=finding.scope,
                    layer=finding.layer,
                    motif=finding.motif,
                    publish_mode=publish_mode,
                    tags=finding.opportunity_tags,
                )
            except Exception:
                logger.exception("diagnoser.finding.publish_failed")

        try:
            producer.flush()
            logger.info("diagnoser.findings.flushed", count=len(findings))
        except Exception:
            logger.exception("diagnoser.findings.flush_failed")

    def _diagnose(self, data: dict):
        pass
