"""
L2 Insight schema - matches docs/L2_L3_SPEC.md.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import uuid

SCHEMA_VERSION = "2.0.0"
REASONER_VERSION = "1.0.0"


@dataclass
class InsightTriggerSignal:
    id: str
    source: str
    weight: float

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "source": self.source,
            "weight": self.weight,
        }


@dataclass
class InsightMetrics:
    correlation_score: float
    confidence: float
    signal_count: int
    time_window_hours: float

    def to_dict(self) -> dict[str, object]:
        return {
            "correlation_score": self.correlation_score,
            "confidence": self.confidence,
            "signal_count": self.signal_count,
            "time_window_hours": self.time_window_hours,
        }


@dataclass
class InsightContext:
    historical_frequency: str = "unknown"
    last_similar_event: str = ""
    baseline_deviation_sigma: float = 0.0

    def to_dict(self) -> dict[str, object]:
        return {
            "historical_frequency": self.historical_frequency,
            "last_similar_event": self.last_similar_event,
            "baseline_deviation_sigma": self.baseline_deviation_sigma,
        }


@dataclass
class InsightMetadata:
    created_at: str
    schema_version: str = SCHEMA_VERSION
    reasoner_version: str = REASONER_VERSION
    processing_latency_ms: float = 0.0

    def to_dict(self) -> dict[str, object]:
        return {
            "created_at": self.created_at,
            "schema_version": self.schema_version,
            "reasoner_version": self.reasoner_version,
            "processing_latency_ms": self.processing_latency_ms,
        }


@dataclass
class Insight:
    id: str
    timestamp: str
    type: str
    severity: str
    title: str
    description: str
    trigger_signals: list[InsightTriggerSignal] = field(default_factory=list)
    entities: list[str] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)
    metrics: InsightMetrics | None = None
    context: InsightContext = field(default_factory=InsightContext)
    metadata: InsightMetadata | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "type": self.type,
            "severity": self.severity,
            "title": self.title,
            "description": self.description,
            "trigger_signals": [signal.to_dict() for signal in self.trigger_signals],
            "entities": self.entities,
            "categories": self.categories,
            "metrics": self.metrics.to_dict() if self.metrics and hasattr(self.metrics, 'to_dict') else self.metrics,
            "context": self.context.to_dict() if hasattr(self.context, 'to_dict') else self.context,
            "metadata": self.metadata.to_dict() if self.metadata and hasattr(self.metadata, 'to_dict') else self.metadata,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def kafka_key(self) -> str:
        entity = self.entities[0] if self.entities else "unknown"
        return f"{self.type}:{entity}"

    @classmethod
    def new_id(cls) -> str:
        return str(uuid.uuid4())


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
