"""
L2 Entity schema (Phase 2 placeholder).
"""

from dataclasses import dataclass, field


@dataclass
class EntityState:
    id: str
    entity: str
    entity_type: str
    canonical_name: str
    aliases: list[str] = field(default_factory=list)
    current_state: dict[str, object] = field(default_factory=dict)
    relationships: list[dict[str, object]] = field(default_factory=list)
    active_situations: list[str] = field(default_factory=list)
    metadata: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "entity": self.entity,
            "entity_type": self.entity_type,
            "canonical_name": self.canonical_name,
            "aliases": self.aliases,
            "current_state": self.current_state,
            "relationships": self.relationships,
            "active_situations": self.active_situations,
            "metadata": self.metadata,
        }
