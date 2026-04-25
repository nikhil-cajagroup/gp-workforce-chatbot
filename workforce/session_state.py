from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from workforce.entity_types import (
    ResolvedEntity,
    resolved_entity_from_dict,
    resolved_entity_to_dict,
)
from workforce.turn_outcome import TurnOutcome, turn_outcome_from_dict, turn_outcome_to_dict


@dataclass(frozen=True)
class SessionState:
    session_id: str
    previous_turn: Optional[TurnOutcome]
    entity_memory: tuple[ResolvedEntity, ...]
    dataset_hint: Optional[str] = None
    metric_hint: Optional[str] = None


def session_state_to_dict(state: SessionState) -> Dict[str, Any]:
    return {
        "session_id": state.session_id,
        "previous_turn": turn_outcome_to_dict(state.previous_turn) if state.previous_turn else None,
        "entity_memory": [resolved_entity_to_dict(entity) for entity in state.entity_memory],
        "dataset_hint": state.dataset_hint,
        "metric_hint": state.metric_hint,
    }


def session_state_from_dict(payload: Dict[str, Any]) -> SessionState:
    previous_turn_payload = payload.get("previous_turn")
    return SessionState(
        session_id=str(payload.get("session_id") or ""),
        previous_turn=turn_outcome_from_dict(previous_turn_payload) if isinstance(previous_turn_payload, dict) else None,
        entity_memory=tuple(
            resolved_entity_from_dict(item)
            for item in list(payload.get("entity_memory") or [])
            if isinstance(item, dict)
        ),
        dataset_hint=str(payload.get("dataset_hint") or "").strip() or None,
        metric_hint=str(payload.get("metric_hint") or "").strip() or None,
    )
