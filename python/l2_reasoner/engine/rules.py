"""Correlation rules for L2 Reasoner."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .entity_tracker import TrackedSignal


@dataclass
class MultiSourceResult:
    signal_count: int
    distinct_sources: int
    avg_confidence: float
    time_window_hours: float


def evaluate_multi_source_confirmation(
    signals: list[TrackedSignal],
    window_hours: int,
    min_avg_confidence: float = 0.6,
) -> MultiSourceResult | None:
    if len(signals) < 2:
        return None

    sources = {signal.source for signal in signals if signal.source}
    distinct_sources = len(sources)
    if distinct_sources < 2:
        return None

    avg_confidence = sum(signal.confidence for signal in signals) / len(signals)
    if avg_confidence < min_avg_confidence:
        return None

    timestamps = [signal.timestamp for signal in signals if isinstance(signal.timestamp, datetime)]
    if not timestamps:
        return None
    earliest = min(timestamps)
    latest = max(timestamps)
    time_window_hours = (latest - earliest).total_seconds() / 3600.0
    if time_window_hours > window_hours:
        return None

    return MultiSourceResult(
        signal_count=len(signals),
        distinct_sources=distinct_sources,
        avg_confidence=avg_confidence,
        time_window_hours=time_window_hours,
    )
