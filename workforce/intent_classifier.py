from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class IntentResult:
    intent: Optional[str]
    confidence: float
    source: str
    dataset_hint: Optional[str] = None
    metric_hint: Optional[str] = None


def intent_result_to_dict(result: IntentResult) -> Dict[str, Any]:
    return asdict(result)


def intent_result_from_dict(payload: Dict[str, Any]) -> IntentResult:
    return IntentResult(
        intent=str(payload.get("intent") or "").strip() or None,
        confidence=float(payload.get("confidence") or 0.0),
        source=str(payload.get("source") or ""),
        dataset_hint=str(payload.get("dataset_hint") or "").strip() or None,
        metric_hint=str(payload.get("metric_hint") or "").strip() or None,
    )


def build_intent_result(
    *,
    intent: Optional[str],
    confidence: float,
    source: str,
    dataset_hint: Optional[str] = None,
    metric_hint: Optional[str] = None,
) -> IntentResult:
    return IntentResult(
        intent=intent,
        confidence=float(confidence),
        source=source,
        dataset_hint=(dataset_hint or "").strip() or None,
        metric_hint=(metric_hint or "").strip() or None,
    )

