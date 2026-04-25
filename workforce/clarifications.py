from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class ClarificationResponse:
    question: str
    reason: str
    missing_slots: Tuple[str, ...] = ()
    suggestions: Tuple[str, ...] = ()


def clarification_response_to_dict(response: ClarificationResponse) -> Dict[str, Any]:
    return asdict(response)


def clarification_response_from_dict(payload: Dict[str, Any]) -> ClarificationResponse:
    return ClarificationResponse(
        question=str(payload.get("question") or ""),
        reason=str(payload.get("reason") or ""),
        missing_slots=tuple(
            str(item) for item in list(payload.get("missing_slots") or [])
        ),
        suggestions=tuple(
            str(item) for item in list(payload.get("suggestions") or [])
        ),
    )


def build_clarification_response(
    *,
    question: str,
    reason: str,
    missing_slots: Tuple[str, ...] = (),
    suggestions: Tuple[str, ...] = (),
) -> ClarificationResponse:
    return ClarificationResponse(
        question=question,
        reason=reason,
        missing_slots=tuple(missing_slots),
        suggestions=tuple(suggestions),
    )

