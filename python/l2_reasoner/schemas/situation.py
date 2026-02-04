"""
L2 Situation schema (Phase 2 placeholder).
"""

from dataclasses import dataclass, field


@dataclass
class Situation:
    id: str
    entity: str
    type: str
    status: str
    title: str
    summary: str
    timeline: dict[str, object] = field(default_factory=dict)
    current_state: dict[str, object] = field(default_factory=dict)
    risk_assessment: dict[str, object] = field(default_factory=dict)
    related_entities: list[str] = field(default_factory=list)
    related_insights: list[str] = field(default_factory=list)
    metadata: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "entity": self.entity,
            "type": self.type,
            "status": self.status,
            "title": self.title,
            "summary": self.summary,
            "timeline": self.timeline,
            "current_state": self.current_state,
            "risk_assessment": self.risk_assessment,
            "related_entities": self.related_entities,
            "related_insights": self.related_insights,
            "metadata": self.metadata,
        }
