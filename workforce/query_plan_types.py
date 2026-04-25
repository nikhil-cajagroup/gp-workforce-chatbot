from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class QueryPlanV1:
    dataset: Optional[str]
    metric: Optional[str]
    grain: Optional[str]
    group_by: Tuple[str, ...] = ()
    entity_filters: Dict[str, str] = field(default_factory=dict)
    time_scope: Dict[str, Any] = field(default_factory=dict)
    transforms: Tuple[Dict[str, Any], ...] = ()
    compare: Optional[Dict[str, Any]] = None
    requires_clarification: bool = False
    clarification_question: Optional[str] = None
    source: str = ""
    table_hint: Optional[str] = None


def query_plan_v1_to_dict(plan: QueryPlanV1) -> Dict[str, Any]:
    return asdict(plan)


def query_plan_v1_from_dict(payload: Dict[str, Any]) -> QueryPlanV1:
    return QueryPlanV1(
        dataset=str(payload.get("dataset") or "").strip() or None,
        metric=str(payload.get("metric") or "").strip() or None,
        grain=str(payload.get("grain") or "").strip() or None,
        group_by=tuple(str(item) for item in list(payload.get("group_by") or [])),
        entity_filters={
            str(k): str(v)
            for k, v in dict(payload.get("entity_filters") or {}).items()
            if str(k).strip() and str(v).strip()
        },
        time_scope=dict(payload.get("time_scope") or {}),
        transforms=tuple(
            dict(item) for item in list(payload.get("transforms") or [])
            if isinstance(item, dict)
        ),
        compare=dict(payload.get("compare") or {}) or None,
        requires_clarification=bool(payload.get("requires_clarification", False)),
        clarification_question=str(payload.get("clarification_question") or "").strip() or None,
        source=str(payload.get("source") or ""),
        table_hint=str(payload.get("table_hint") or "").strip() or None,
    )


def build_query_plan_v1(
    *,
    dataset: Optional[str],
    metric: Optional[str],
    grain: Optional[str],
    group_by: Tuple[str, ...] = (),
    entity_filters: Optional[Dict[str, str]] = None,
    time_scope: Optional[Dict[str, Any]] = None,
    transforms: Tuple[Dict[str, Any], ...] = (),
    compare: Optional[Dict[str, Any]] = None,
    requires_clarification: bool = False,
    clarification_question: Optional[str] = None,
    source: str,
    table_hint: Optional[str] = None,
) -> QueryPlanV1:
    return QueryPlanV1(
        dataset=(dataset or "").strip() or None,
        metric=(metric or "").strip() or None,
        grain=(grain or "").strip() or None,
        group_by=tuple(group_by),
        entity_filters={
            str(k): str(v)
            for k, v in dict(entity_filters or {}).items()
            if str(k).strip() and str(v).strip()
        },
        time_scope=dict(time_scope or {}),
        transforms=tuple(dict(item) for item in transforms),
        compare=dict(compare or {}) or None,
        requires_clarification=bool(requires_clarification),
        clarification_question=(clarification_question or "").strip() or None,
        source=source,
        table_hint=(table_hint or "").strip() or None,
    )

