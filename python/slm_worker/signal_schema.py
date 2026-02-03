"""
Signal Schema - matches Go internal/signal/signal.go
====================================================
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional
from enum import Enum
import json
import uuid


class Source(str, Enum):
    """Signal source providers."""
    GDELT = "gdelt"
    FRED = "fred"
    BINANCE = "binance"
    CME_COT = "cme_cot"
    NEWSAPI = "newsapi"
    REDDIT = "reddit"
    TWITTER = "twitter"


class Category(str, Enum):
    """Signal categories."""
    GEOPOLITICAL = "geopolitical"
    MACRO = "macro"
    CRYPTO = "crypto"
    SENTIMENT = "sentiment"


class UrgencyLevel(str, Enum):
    """Signal urgency levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Metadata:
    """Signal metadata."""
    ingested_at: str
    schema_version: str = "1.0.0"
    processed_at: Optional[str] = None
    provider_meta: Optional[dict[str, Any]] = None
    trace_id: Optional[str] = None
    
    # SLM enrichment fields (added during processing)
    slm_model: Optional[str] = None
    slm_version: Optional[str] = None
    enrichment_latency_ms: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "ingested_at": self.ingested_at,
            "schema_version": self.schema_version,
        }
        if self.processed_at:
            result["processed_at"] = self.processed_at
        if self.provider_meta:
            result["provider_meta"] = self.provider_meta
        if self.trace_id:
            result["trace_id"] = self.trace_id
        if self.slm_model:
            result["slm_model"] = self.slm_model
        if self.slm_version:
            result["slm_version"] = self.slm_version
        if self.enrichment_latency_ms is not None:
            result["enrichment_latency_ms"] = self.enrichment_latency_ms
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "Metadata":
        return cls(
            ingested_at=data.get("ingested_at", ""),
            schema_version=data.get("schema_version", "1.0.0"),
            processed_at=data.get("processed_at"),
            provider_meta=data.get("provider_meta"),
            trace_id=data.get("trace_id"),
            slm_model=data.get("slm_model"),
            slm_version=data.get("slm_version"),
            enrichment_latency_ms=data.get("enrichment_latency_ms"),
        )


@dataclass
class Signal:
    """
    Core signal structure matching Go schema.
    
    Fields:
        id: Unique identifier (UUID)
        timestamp: When the signal event occurred
        source: Data source (gdelt, binance, etc.)
        category: Signal category (geopolitical, crypto, macro)
        subject: What/who the signal is about
        action: What happened
        object: Target of the action (optional)
        confidence: Confidence score 0.0-1.0
        sentiment: Sentiment score -1.0 to 1.0
        urgency: Urgency level (low, medium, high, critical)
        tags: List of tags
        raw_data: Original raw data from source
        metadata: Processing metadata
    """
    id: str
    timestamp: str
    source: str
    category: str
    subject: str
    action: str
    confidence: float
    sentiment: float
    urgency: str
    metadata: Metadata
    object: Optional[str] = None
    tags: list[str] = field(default_factory=list)
    raw_data: Optional[dict[str, Any]] = None
    
    # Enrichment fields (added by SLM)
    enrichment: Optional[dict[str, Any]] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        result = {
            "id": self.id,
            "timestamp": self.timestamp,
            "source": self.source,
            "category": self.category,
            "subject": self.subject,
            "action": self.action,
            "confidence": self.confidence,
            "sentiment": self.sentiment,
            "urgency": self.urgency,
            "metadata": self.metadata.to_dict(),
        }
        if self.object:
            result["object"] = self.object
        if self.tags:
            result["tags"] = self.tags
        if self.raw_data:
            result["raw_data"] = self.raw_data
        if self.enrichment:
            result["enrichment"] = self.enrichment
        return result

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict) -> "Signal":
        """Create Signal from dictionary."""
        metadata_data = data.get("metadata", {})
        if isinstance(metadata_data, dict):
            metadata = Metadata.from_dict(metadata_data)
        else:
            metadata = Metadata(ingested_at=datetime.utcnow().isoformat() + "Z")
        
        return cls(
            id=data.get("id", str(uuid.uuid4())),
            timestamp=data.get("timestamp", ""),
            source=data.get("source", ""),
            category=data.get("category", ""),
            subject=data.get("subject", ""),
            action=data.get("action", ""),
            object=data.get("object"),
            confidence=float(data.get("confidence", 0.0)),
            sentiment=float(data.get("sentiment", 0.0)),
            urgency=data.get("urgency", "low"),
            tags=data.get("tags", []),
            raw_data=data.get("raw_data"),
            metadata=metadata,
            enrichment=data.get("enrichment"),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "Signal":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(json_str))

    def kafka_key(self) -> str:
        """Generate Kafka key for partitioning."""
        return f"{self.source}:{self.subject}"

    def validate(self) -> list[str]:
        """Validate signal fields, return list of errors."""
        errors = []
        if not self.subject:
            errors.append("subject is required")
        if not self.action:
            errors.append("action is required")
        if not self.source:
            errors.append("source is required")
        if not self.category:
            errors.append("category is required")
        if not (0.0 <= self.confidence <= 1.0):
            errors.append(f"confidence must be 0.0-1.0, got {self.confidence}")
        if not (-1.0 <= self.sentiment <= 1.0):
            errors.append(f"sentiment must be -1.0 to 1.0, got {self.sentiment}")
        return errors
