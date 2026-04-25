from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from workforce.entity_types import (
    ResolvedEntity,
    resolved_entity_from_dict,
    resolved_entity_to_dict,
)
from workforce.intent_classifier import intent_result_from_dict, intent_result_to_dict


@dataclass(frozen=True)
class ResultSummary:
    rows_returned: int
    empty: bool
    answer_preview: str
    preview_markdown_present: bool
    last_error: Optional[str] = None


@dataclass(frozen=True)
class TurnOutcome:
    intent: Optional[str]
    intent_confidence: float
    intent_source: str
    intent_result: Optional[Dict[str, Any]]
    query_plan_v1: Optional[Dict[str, Any]]
    resolved_entities: tuple[ResolvedEntity, ...]
    chosen_metric: Optional[str]
    chosen_dataset: Optional[str]
    query_plan: Optional[Dict[str, Any]]
    sql_executed: Optional[str]
    result_summary: ResultSummary
    follow_up_context: Dict[str, Any]
    routing_decision: Dict[str, Any]
    clarification_question: Optional[str] = None
    generated_at: str = ""
    # Typed outcome of the planner-v1 live admission decision for this turn.
    # Empty dict when the supervisor never reached the admission check (e.g.
    # cross-dataset compound scope short-circuit, knowledge/greeting route).
    planner_v1_live_outcome: Optional[Dict[str, Any]] = None
    # Shadow retirement report — populated only when the shadow intent
    # points at a metric in LEGACY_RETIREMENT_SHADOW_METRICS. None for
    # turns outside the retirement window so aggregate logs stay cheap.
    legacy_retirement_report: Optional[Dict[str, Any]] = None


def new_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def turn_outcome_to_dict(outcome: TurnOutcome) -> Dict[str, Any]:
    payload = asdict(outcome)
    payload["resolved_entities"] = [
        resolved_entity_to_dict(entity) for entity in outcome.resolved_entities
    ]
    return payload


def turn_outcome_from_dict(payload: Dict[str, Any]) -> TurnOutcome:
    summary_payload = dict(payload.get("result_summary") or {})
    return TurnOutcome(
        intent=str(payload.get("intent") or "").strip() or None,
        intent_confidence=float(payload.get("intent_confidence") or 0.0),
        intent_source=str(payload.get("intent_source") or ""),
        intent_result=(
            intent_result_to_dict(intent_result_from_dict(payload["intent_result"]))
            if isinstance(payload.get("intent_result"), dict) else None
        ),
        query_plan_v1=dict(payload.get("query_plan_v1") or {}) or None,
        resolved_entities=tuple(
            resolved_entity_from_dict(item)
            for item in list(payload.get("resolved_entities") or [])
            if isinstance(item, dict)
        ),
        chosen_metric=str(payload.get("chosen_metric") or "").strip() or None,
        chosen_dataset=str(payload.get("chosen_dataset") or "").strip() or None,
        query_plan=dict(payload.get("query_plan") or {}) or None,
        sql_executed=str(payload.get("sql_executed") or "").strip() or None,
        result_summary=ResultSummary(
            rows_returned=int(summary_payload.get("rows_returned") or 0),
            empty=bool(summary_payload.get("empty", False)),
            answer_preview=str(summary_payload.get("answer_preview") or ""),
            preview_markdown_present=bool(summary_payload.get("preview_markdown_present", False)),
            last_error=str(summary_payload.get("last_error") or "").strip() or None,
        ),
        follow_up_context=dict(payload.get("follow_up_context") or {}),
        routing_decision=dict(payload.get("routing_decision") or {}),
        clarification_question=str(payload.get("clarification_question") or "").strip() or None,
        generated_at=str(payload.get("generated_at") or ""),
        planner_v1_live_outcome=dict(payload.get("planner_v1_live_outcome") or {}) or None,
        legacy_retirement_report=dict(payload.get("legacy_retirement_report") or {}) or None,
    )


def build_turn_outcome(
    *,
    intent: Optional[str],
    intent_confidence: float,
    intent_source: str,
    intent_result: Optional[Dict[str, Any]] = None,
    query_plan_v1: Optional[Dict[str, Any]] = None,
    resolved_entities: List[ResolvedEntity],
    chosen_metric: Optional[str],
    chosen_dataset: Optional[str],
    query_plan: Optional[Dict[str, Any]],
    sql_executed: Optional[str],
    rows_returned: int,
    empty: bool,
    answer: str,
    preview_markdown: str,
    last_error: Optional[str],
    follow_up_context: Optional[Dict[str, Any]],
    routing_decision: Optional[Dict[str, Any]],
    clarification_question: Optional[str] = None,
    planner_v1_live_outcome: Optional[Dict[str, Any]] = None,
    legacy_retirement_report: Optional[Dict[str, Any]] = None,
) -> TurnOutcome:
    return TurnOutcome(
        intent=intent,
        intent_confidence=float(intent_confidence),
        intent_source=intent_source,
        intent_result=dict(intent_result or {}) or None,
        query_plan_v1=dict(query_plan_v1 or {}) or None,
        resolved_entities=tuple(resolved_entities),
        chosen_metric=chosen_metric,
        chosen_dataset=chosen_dataset,
        query_plan=dict(query_plan or {}) or None,
        sql_executed=(sql_executed or "").strip() or None,
        result_summary=ResultSummary(
            rows_returned=int(rows_returned or 0),
            empty=bool(empty),
            answer_preview=(answer or "").strip()[:280],
            preview_markdown_present=bool(preview_markdown.strip()),
            last_error=(last_error or "").strip() or None,
        ),
        follow_up_context=dict(follow_up_context or {}),
        routing_decision=dict(routing_decision or {}),
        clarification_question=(clarification_question or "").strip() or None,
        generated_at=new_timestamp(),
        planner_v1_live_outcome=dict(planner_v1_live_outcome or {}) or None,
        legacy_retirement_report=dict(legacy_retirement_report or {}) or None,
    )
