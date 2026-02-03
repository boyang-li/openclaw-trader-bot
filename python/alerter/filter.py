import time
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from .config import FilterConfig

logger = logging.getLogger(__name__)

URGENCY_LEVELS = {"low": 0, "medium": 1, "high": 2, "critical": 3}


@dataclass
class FilterResult:
    should_alert: bool
    reasons: list[str]
    suppression_reason: str | None = None


class SignalFilter:
    def __init__(self, config: FilterConfig):
        self.config = config
        self.min_urgency_level = URGENCY_LEVELS.get(config.min_urgency, 2)
        self._recent_subjects: dict[str, float] = {}
        self._alert_timestamps: list[float] = []

    def should_alert(self, signal: dict[str, Any]) -> FilterResult:
        reasons: list[str] = []
        
        suppression = self._check_rate_limit()
        if suppression:
            return FilterResult(should_alert=False, reasons=[], suppression_reason=suppression)

        suppression = self._check_dedup(signal)
        if suppression:
            return FilterResult(should_alert=False, reasons=[], suppression_reason=suppression)

        if self.config.alert_sources:
            source = signal.get("source", "")
            if source not in self.config.alert_sources:
                return FilterResult(should_alert=False, reasons=[], 
                    suppression_reason=f"source '{source}' not in allowed list")

        if self.config.alert_categories:
            category = signal.get("category", "")
            if category not in self.config.alert_categories:
                return FilterResult(should_alert=False, reasons=[],
                    suppression_reason=f"category '{category}' not in allowed list")

        urgency = signal.get("urgency", "low")
        enrichment = signal.get("enrichment", {}) or {}
        refined_urgency = enrichment.get("refined_urgency", urgency)
        urgency_level = URGENCY_LEVELS.get(refined_urgency, URGENCY_LEVELS.get(urgency, 0))
        
        if urgency_level >= self.min_urgency_level:
            reasons.append(f"urgency={refined_urgency}")

        sentiment = signal.get("sentiment", 0.0)
        refined_sentiment = enrichment.get("refined_sentiment", sentiment)
        if abs(refined_sentiment) >= self.config.min_sentiment_magnitude:
            direction = "bullish" if refined_sentiment > 0 else "bearish"
            reasons.append(f"strong {direction} sentiment ({refined_sentiment:.2f})")

        market_impact = enrichment.get("market_impact", "neutral")
        if market_impact in self.config.alert_market_impacts:
            reasons.append(f"market_impact={market_impact}")

        should_alert = len(reasons) > 0
        
        if should_alert:
            self._record_alert(signal)
        
        return FilterResult(should_alert=should_alert, reasons=reasons)

    def _check_rate_limit(self) -> str | None:
        now = time.time()
        cutoff = now - 60
        self._alert_timestamps = [t for t in self._alert_timestamps if t > cutoff]
        
        if len(self._alert_timestamps) >= self.config.rate_limit_per_minute:
            return f"rate limit exceeded ({self.config.rate_limit_per_minute}/min)"
        return None

    def _check_dedup(self, signal: dict[str, Any]) -> str | None:
        now = time.time()
        subject = signal.get("subject", "")
        source = signal.get("source", "")
        dedup_key = f"{source}:{subject}"
        
        cutoff = now - self.config.dedup_window_seconds
        self._recent_subjects = {k: v for k, v in self._recent_subjects.items() if v > cutoff}
        
        if dedup_key in self._recent_subjects:
            return f"duplicate subject '{subject}' within {self.config.dedup_window_seconds}s window"
        return None

    def _record_alert(self, signal: dict[str, Any]):
        now = time.time()
        self._alert_timestamps.append(now)
        
        subject = signal.get("subject", "")
        source = signal.get("source", "")
        dedup_key = f"{source}:{subject}"
        self._recent_subjects[dedup_key] = now
