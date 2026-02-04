"""Correlation engine for L2 Reasoner."""

from __future__ import annotations

from dataclasses import dataclass

from .entity_tracker import TrackedSignal
from .rules import evaluate_multi_source_confirmation
from ..schemas.insight import (
    Insight,
    InsightContext,
    InsightMetadata,
    InsightMetrics,
    InsightTriggerSignal,
    utc_now_iso,
)


@dataclass
class CorrelationConfig:
    window_hours: int


class CorrelationEngine:
    def __init__(self, window_hours: int):
        self.window_hours = window_hours

    def evaluate_entity(
        self,
        entity_display: str,
        signals: list[TrackedSignal],
        processing_latency_ms: float,
    ) -> Insight | None:
        result = evaluate_multi_source_confirmation(
            signals=signals,
            window_hours=self.window_hours,
            min_avg_confidence=0.6,
        )
        if not result:
            return None

        trigger_signals = self._build_trigger_signals(signals)
        entities = self._collect_entities(signals, entity_display)
        categories = sorted({signal.category for signal in signals if signal.category})
        correlation_score = self._correlation_score(
            avg_confidence=result.avg_confidence,
            distinct_sources=result.distinct_sources,
            signal_count=result.signal_count,
        )
        severity = self._severity(result.signal_count, result.avg_confidence)

        title = f"Multi-source confirmation: {entity_display}"
        sources_text = ", ".join(sorted({signal.source for signal in signals if signal.source}))
        description = (
            f"{result.signal_count} signals from {result.distinct_sources} sources ({sources_text}) "
            f"confirm {entity_display} within {result.time_window_hours:.2f}h window"
        )

        metrics = InsightMetrics(
            correlation_score=correlation_score,
            confidence=result.avg_confidence,
            signal_count=result.signal_count,
            time_window_hours=result.time_window_hours,
        )
        metadata = InsightMetadata(
            created_at=utc_now_iso(),
            processing_latency_ms=processing_latency_ms,
        )

        return Insight(
            id=Insight.new_id(),
            timestamp=utc_now_iso(),
            type="multi_source_confirmation",
            severity=severity,
            title=title,
            description=description,
            trigger_signals=trigger_signals,
            entities=entities,
            categories=categories,
            metrics=metrics,
            context=InsightContext(),
            metadata=metadata,
        )

    def _build_trigger_signals(self, signals: list[TrackedSignal]) -> list[InsightTriggerSignal]:
        total_confidence = sum(signal.confidence for signal in signals) or 1.0
        return [
            InsightTriggerSignal(
                id=signal.id,
                source=signal.source,
                weight=signal.confidence / total_confidence,
            )
            for signal in signals
        ]

    def _collect_entities(self, signals: list[TrackedSignal], primary: str) -> list[str]:
        entities = [primary]
        for signal in signals:
            for entity in signal.entities:
                if entity and entity not in entities:
                    entities.append(entity)
        return entities

    def _correlation_score(self, avg_confidence: float, distinct_sources: int, signal_count: int) -> float:
        if signal_count <= 0:
            return 0.0
        source_factor = distinct_sources / signal_count
        score = avg_confidence * (0.6 + 0.4 * source_factor)
        return min(1.0, max(0.0, score))

    def _severity(self, signal_count: int, avg_confidence: float) -> str:
        if signal_count >= 4 and avg_confidence >= 0.85:
            return "critical"
        if signal_count >= 3 and avg_confidence >= 0.75:
            return "alert"
        return "warning"
