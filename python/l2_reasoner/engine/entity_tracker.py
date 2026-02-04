"""Entity tracking for L2 Reasoner."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Mapping, Sequence


@dataclass
class TrackedSignal:
    id: str
    timestamp: datetime
    source: str
    category: str
    subject: str
    action: str
    confidence: float
    sentiment: float
    urgency: str
    entities: list[str] = field(default_factory=list)
    raw: dict[str, object] = field(default_factory=dict)


def parse_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc)
        return parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


def safe_float(value: object | None, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (float, int)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def normalize_entity(name: str) -> str:
    return " ".join(name.strip().split()).lower()


def extract_entities(signal: Mapping[str, object]) -> list[str]:
    entities: list[str] = []
    existing = signal.get("entities")
    if isinstance(existing, list):
        entities.extend([str(item).strip() for item in existing if str(item).strip()])
    enrichment = signal.get("enrichment") or {}
    enrichment_entities = enrichment.get("entities") if isinstance(enrichment, dict) else None
    if isinstance(enrichment_entities, list):
        entities.extend([str(item).strip() for item in enrichment_entities if str(item).strip()])
    subject = str(signal.get("subject", "")).strip()
    if subject:
        entities.append(subject)
    seen: set[str] = set()
    unique_entities: list[str] = []
    for entity in entities:
        key = normalize_entity(entity)
        if not key or key in seen:
            continue
        seen.add(key)
        unique_entities.append(entity)
    return unique_entities


class EntityTracker:
    def __init__(self, window_hours: int):
        self.window = timedelta(hours=window_hours)
        self._entity_signals: dict[str, list[TrackedSignal]] = {}
        self._entity_display: dict[str, str] = {}
        self._signal_ids: set[str] = set()

    def add_signal(self, signal: Mapping[str, object]) -> list[str]:
        signal_id = str(signal.get("id", ""))
        if not signal_id or signal_id in self._signal_ids:
            return []
        self._signal_ids.add(signal_id)

        entities = extract_entities(signal)
        if not entities:
            return []

        timestamp_value = signal.get("timestamp")
        timestamp_str = timestamp_value if isinstance(timestamp_value, str) else None

        tracked = TrackedSignal(
            id=signal_id,
            timestamp=parse_timestamp(timestamp_str),
            source=str(signal.get("source", "")),
            category=str(signal.get("category", "")),
            subject=str(signal.get("subject", "")),
            action=str(signal.get("action", "")),
            confidence=safe_float(signal.get("confidence", 0.0)),
            sentiment=safe_float(signal.get("sentiment", 0.0)),
            urgency=str(signal.get("urgency", "low")),
            entities=entities,
            raw=dict(signal),
        )

        entity_keys: list[str] = []
        for entity in entities:
            key = normalize_entity(entity)
            if not key:
                continue
            entity_keys.append(key)
            if key not in self._entity_display:
                self._entity_display[key] = entity
            self._entity_signals.setdefault(key, []).append(tracked)

        self.prune()
        return entity_keys

    def load_signals(self, signals: Sequence[Mapping[str, object]]) -> None:
        for signal in signals:
            self.add_signal(signal)

    def prune(self) -> None:
        cutoff = datetime.now(timezone.utc) - self.window
        empty_keys: list[str] = []
        for key, signals in self._entity_signals.items():
            filtered = [signal for signal in signals if signal.timestamp >= cutoff]
            if filtered:
                self._entity_signals[key] = filtered
            else:
                empty_keys.append(key)
        for key in empty_keys:
            self._entity_signals.pop(key, None)
            self._entity_display.pop(key, None)

    def get_entity_signals(self, entity_key: str) -> list[TrackedSignal]:
        return list(self._entity_signals.get(entity_key, []))

    def get_display_name(self, entity_key: str) -> str:
        return self._entity_display.get(entity_key, entity_key)
