from __future__ import annotations

from typing import Any, Dict, Optional

from workforce.query_plan_types import build_query_plan_v1, query_plan_v1_to_dict

LIVE_QUERY_PLAN_V1_METRICS = {
    "gp_fte",
    "gp_headcount",
    "total_appointments",
    "dna_rate",
    "face_to_face_share",
    "telephone_share",
    "patients_per_gp",
    "registered_patients",
    "appointments_per_gp_fte",
    "appointments_per_gp_headcount",
}

# Hard intents that planner-v1 live is allowed to absorb when v9 can compile a
# clean request. Keep this set small and conservative — each entry is an
# explicit opt-in that bypasses the deterministic-override SQL branch. Follow-
# up turns and ambiguous entity references are still rejected by the v9 gate.
SAFE_HARD_INTENTS_FOR_PLANNER_V1 = {
    "patients_per_gp",
    "trainee_gp_count",
    # practice_patient_count → v9 registered_patients metric. Safe because
    # _try_query_plan_v1_live additionally requires v9 to have resolved
    # non-empty entity_filters/group_by before admitting any hard intent,
    # so a bare "how many patients at Keele Practice?" that v9 cannot pin
    # to a practice_code will still fall through to the deterministic
    # override path.
    "practice_patient_count",
    # practice_gp_count / practice_gp_count_soft → v9 gp_headcount metric.
    # Same safety story as practice_patient_count: the scope-resolution
    # guard keeps bare practice-name questions on the override path until
    # v9 can pin them to a practice_code.
    "practice_gp_count",
    "practice_gp_count_soft",
}


def build_shadow_query_plan(
    *,
    dataset: str | None,
    semantic_request_v9: Dict[str, Any] | None,
    semantic_path: Dict[str, Any] | None,
    legacy_plan: Dict[str, Any] | None,
    clarification_question: str | None,
    needs_clarification: bool,
) -> Dict[str, Any]:
    semantic_request_v9 = dict(semantic_request_v9 or {})
    semantic_path = dict(semantic_path or {})
    legacy_plan = dict(legacy_plan or {})

    if semantic_path.get("used"):
        metrics = list(semantic_request_v9.get("metrics") or semantic_path.get("metric_keys") or [])
        return query_plan_v1_to_dict(
            build_query_plan_v1(
                dataset=str(semantic_path.get("dataset") or dataset or "").strip() or None,
                metric=str(metrics[0] if metrics else "") or None,
                grain=str(semantic_path.get("grain") or "").strip() or None,
                group_by=tuple(str(item) for item in list(semantic_request_v9.get("group_by") or [])),
                entity_filters={
                    str(k): str(v)
                    for k, v in dict(semantic_request_v9.get("entity_filters") or {}).items()
                },
                time_scope=dict(semantic_request_v9.get("time") or {}),
                transforms=tuple(
                    dict(item) for item in list(semantic_request_v9.get("transforms") or [])
                    if isinstance(item, dict)
                ),
                compare=dict(semantic_request_v9.get("compare") or {}) or None,
                requires_clarification=bool(needs_clarification),
                clarification_question=clarification_question,
                source="semantic_v9",
                table_hint=str(legacy_plan.get("table") or "").strip() or None,
            )
        )

    if legacy_plan:
        return query_plan_v1_to_dict(
            build_query_plan_v1(
                dataset=dataset,
                metric=str(legacy_plan.get("intent") or "").strip() or None,
                grain=None,
                group_by=tuple(str(item) for item in list(legacy_plan.get("group_by") or [])),
                entity_filters={},
                time_scope={},
                transforms=(),
                compare=None,
                requires_clarification=bool(needs_clarification),
                clarification_question=clarification_question,
                source="legacy_plan",
                table_hint=str(legacy_plan.get("table") or "").strip() or None,
            )
        )

    return query_plan_v1_to_dict(
        build_query_plan_v1(
            dataset=dataset,
            metric=None,
            grain=None,
            requires_clarification=bool(needs_clarification),
            clarification_question=clarification_question,
            source="shadow_pending",
        )
    )


def build_live_query_plan(
    *,
    dataset: str | None,
    semantic_request_v9: Dict[str, Any] | None,
    semantic_path: Dict[str, Any] | None,
    table_hint: Optional[str] = None,
    clarification_question: str | None = None,
    needs_clarification: bool = False,
) -> Optional[Dict[str, Any]]:
    semantic_request_v9 = dict(semantic_request_v9 or {})
    semantic_path = dict(semantic_path or {})
    metrics = list(semantic_request_v9.get("metrics") or semantic_path.get("metric_keys") or [])
    metric = str(metrics[0] if metrics else "").strip()
    if metric not in LIVE_QUERY_PLAN_V1_METRICS:
        return None

    return query_plan_v1_to_dict(
        build_query_plan_v1(
            dataset=str(semantic_path.get("dataset") or dataset or "").strip() or None,
            metric=metric,
            grain=str(semantic_path.get("grain") or "").strip() or None,
            group_by=tuple(str(item) for item in list(semantic_request_v9.get("group_by") or [])),
            entity_filters={
                str(k): str(v)
                for k, v in dict(semantic_request_v9.get("entity_filters") or {}).items()
            },
            time_scope=dict(semantic_request_v9.get("time") or {}),
            transforms=tuple(
                dict(item) for item in list(semantic_request_v9.get("transforms") or [])
                if isinstance(item, dict)
            ),
            compare=dict(semantic_request_v9.get("compare") or {}) or None,
            requires_clarification=bool(needs_clarification),
            clarification_question=clarification_question,
            source="planner_v1_live",
            table_hint=table_hint,
        )
    )
