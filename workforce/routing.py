from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class RoutingDecision:
    intent: Optional[str]
    intent_confidence: float
    path_chosen: str
    path_reason: str
    alternatives_considered: Tuple[str, ...] = ()
    source: Optional[str] = None


def routing_decision_to_dict(decision: RoutingDecision) -> Dict[str, Any]:
    return asdict(decision)


def routing_decision_from_dict(payload: Dict[str, Any]) -> RoutingDecision:
    return RoutingDecision(
        intent=str(payload.get("intent") or "").strip() or None,
        intent_confidence=float(payload.get("intent_confidence") or 0.0),
        path_chosen=str(payload.get("path_chosen") or ""),
        path_reason=str(payload.get("path_reason") or ""),
        alternatives_considered=tuple(
            str(item) for item in list(payload.get("alternatives_considered") or [])
        ),
        source=str(payload.get("source") or "").strip() or None,
    )


def build_routing_decision(
    *,
    intent: Optional[str],
    intent_confidence: float,
    path_chosen: str,
    path_reason: str,
    alternatives_considered: Tuple[str, ...] = (),
    source: Optional[str] = None,
) -> RoutingDecision:
    return RoutingDecision(
        intent=intent,
        intent_confidence=float(intent_confidence),
        path_chosen=path_chosen,
        path_reason=path_reason,
        alternatives_considered=tuple(alternatives_considered),
        source=(source or "").strip() or None,
    )

