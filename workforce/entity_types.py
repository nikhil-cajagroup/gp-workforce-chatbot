from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Literal, Optional


@dataclass(frozen=True)
class ResolvedEntity:
    entity_type: str
    canonical_name: str
    code: Optional[str] = None
    source_column: Optional[str] = None
    resolution_source: Literal["state_extract", "follow_up", "explicit_in_question", "clarified"] = "state_extract"
    confidence: float = 1.0


def resolved_entity_to_dict(entity: ResolvedEntity) -> Dict[str, Any]:
    return asdict(entity)


def resolved_entity_from_dict(payload: Dict[str, Any]) -> ResolvedEntity:
    return ResolvedEntity(
        entity_type=str(payload.get("entity_type") or ""),
        canonical_name=str(payload.get("canonical_name") or ""),
        code=str(payload.get("code") or "").strip() or None,
        source_column=str(payload.get("source_column") or "").strip() or None,
        resolution_source=str(payload.get("resolution_source") or "state_extract"),  # type: ignore[arg-type]
        confidence=float(payload.get("confidence") or 0.0),
    )

