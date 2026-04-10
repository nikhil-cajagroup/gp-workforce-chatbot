from __future__ import annotations

import re
from typing import Any, Callable, MutableMapping, Optional


def init_appointments_query_plan(state: MutableMapping[str, Any], hcp_type: Optional[str]) -> None:
    state["plan"] = {
        "in_scope": True,
        "table": "practice",
        "intent": "total",
        "notes": "Appointments pipeline",
    }
    if hcp_type:
        state["plan"]["hcp_type"] = hcp_type


def reset_appointments_query_fallthrough(
    state: MutableMapping[str, Any],
    *,
    log_info: Callable[[str], None],
) -> MutableMapping[str, Any]:
    state["sql"] = ""
    state["df_preview_md"] = ""
    state["_rows"] = 0
    state["_empty"] = False
    state["answer"] = ""
    state["suggestions"] = []
    state["plan"] = {}
    log_info("node_appointments_query | no hard override matched, falling through to planner")
    return state


def appointments_scope_table(practice_hint: str, geo_hint: str) -> str:
    return "practice" if practice_hint or not geo_hint else "pcn_subicb"


def apply_appointments_top_practices(
    state: MutableMapping[str, Any],
    question: str,
    *,
    sql_appointments_top_practices: Callable[[str], str],
) -> MutableMapping[str, Any]:
    state["plan"]["table"] = "practice"
    state["plan"]["intent"] = "topn"
    state["plan"]["notes"] = "Appointments hard override: top practices by appointments"
    state["sql"] = sql_appointments_top_practices(question)
    return state


def apply_appointments_dna_rate(
    state: MutableMapping[str, Any],
    question: str,
    geo_hint: str,
    practice_hint: str,
    hcp_type: Optional[str],
    *,
    sql_appointments_dna_rate: Callable[[str, str, Optional[str]], str],
) -> MutableMapping[str, Any]:
    state["plan"]["table"] = appointments_scope_table(practice_hint, geo_hint)
    state["plan"]["intent"] = "ratio"
    state["plan"]["notes"] = "Appointments hard override: DNA rate"
    state["sql"] = sql_appointments_dna_rate(question, geo_hint, hcp_type)
    return state


def apply_appointments_mode_breakdown(
    state: MutableMapping[str, Any],
    question: str,
    geo_hint: str,
    practice_hint: str,
    hcp_type: Optional[str],
    *,
    sql_appointments_mode_breakdown: Callable[[str, str, Optional[str]], str],
) -> MutableMapping[str, Any]:
    state["plan"]["table"] = appointments_scope_table(practice_hint, geo_hint)
    state["plan"]["intent"] = "breakdown"
    state["plan"]["notes"] = "Appointments hard override: appointment mode breakdown"
    state["plan"]["group_by"] = ["appt_mode"]
    state["sql"] = sql_appointments_mode_breakdown(question, geo_hint, hcp_type)
    return state


def apply_appointments_hcp_breakdown(
    state: MutableMapping[str, Any],
    question: str,
    geo_hint: str,
    practice_hint: str,
    *,
    sql_appointments_hcp_breakdown: Callable[[str, str], str],
) -> MutableMapping[str, Any]:
    state["plan"]["table"] = appointments_scope_table(practice_hint, geo_hint)
    state["plan"]["intent"] = "breakdown"
    state["plan"]["notes"] = "Appointments hard override: HCP type breakdown"
    state["plan"]["group_by"] = ["hcp_type"]
    state["sql"] = sql_appointments_hcp_breakdown(question, geo_hint)
    return state


def apply_appointments_trend(
    state: MutableMapping[str, Any],
    question: str,
    geo_hint: str,
    practice_hint: str,
    hcp_type: Optional[str],
    *,
    sql_appointments_trend: Callable[..., str],
) -> MutableMapping[str, Any]:
    state["plan"]["table"] = appointments_scope_table(practice_hint, geo_hint)
    state["plan"]["intent"] = "trend"
    state["plan"]["notes"] = "Appointments hard override: appointment trend"
    state["sql"] = sql_appointments_trend(question, geo_hint, hcp_type=hcp_type)
    return state


def apply_appointments_total(
    state: MutableMapping[str, Any],
    question: str,
    geo_hint: str,
    practice_hint: str,
    hcp_type: Optional[str],
    *,
    sql_appointments_total_latest: Callable[[str, str, Optional[str]], str],
) -> MutableMapping[str, Any]:
    state["plan"]["table"] = appointments_scope_table(practice_hint, geo_hint)
    state["plan"]["intent"] = "total"
    state["plan"]["notes"] = "Appointments hard override: total appointments"
    state["sql"] = sql_appointments_total_latest(question, geo_hint, hcp_type)
    return state


def appointments_query_strategy(
    state: MutableMapping[str, Any],
    *,
    extract_appointments_geo_hint: Callable[[str], str],
    appointments_geo_hint_from_context: Callable[[dict[str, Any]], str],
    appointments_hcp_filter: Callable[[str], Optional[str]],
    specific_entity_hint: Callable[[str, str], str],
    sql_appointments_top_practices: Callable[[str], str],
    sql_appointments_dna_rate: Callable[[str, str, Optional[str]], str],
    sql_appointments_mode_breakdown: Callable[[str, str, Optional[str]], str],
    sql_appointments_hcp_breakdown: Callable[[str, str], str],
    sql_appointments_trend: Callable[..., str],
    sql_appointments_total_latest: Callable[[str, str, Optional[str]], str],
    log_info: Callable[[str], None],
) -> MutableMapping[str, Any]:
    q = state.get("original_question", state.get("question", ""))
    q_lower = q.lower()
    follow_ctx = state.get("follow_up_context") or {}
    geo_hint = extract_appointments_geo_hint(q)
    if not geo_hint and follow_ctx:
        geo_hint = appointments_geo_hint_from_context(follow_ctx)
    hcp_type = appointments_hcp_filter(q)
    practice_hint = specific_entity_hint(q, "practice")
    if not practice_hint and str(follow_ctx.get("entity_type") or "").lower() == "practice":
        practice_hint = str(follow_ctx.get("previous_entity_code") or follow_ctx.get("entity_name") or "").strip()

    init_appointments_query_plan(state, hcp_type)

    if any(term in q_lower for term in ["top practice", "top practices", "most appointments", "highest appointments"]):
        return apply_appointments_top_practices(
            state,
            q,
            sql_appointments_top_practices=sql_appointments_top_practices,
        )
    if "dna" in q_lower or "did not attend" in q_lower:
        return apply_appointments_dna_rate(
            state,
            q,
            geo_hint,
            practice_hint,
            hcp_type,
            sql_appointments_dna_rate=sql_appointments_dna_rate,
        )
    if (
        "face-to-face" in q_lower or "face to face" in q_lower or "telephone" in q_lower
        or "video" in q_lower or "online" in q_lower or "home visit" in q_lower
        or "appointment mode" in q_lower or "by mode" in q_lower
    ):
        return apply_appointments_mode_breakdown(
            state,
            q,
            geo_hint,
            practice_hint,
            hcp_type,
            sql_appointments_mode_breakdown=sql_appointments_mode_breakdown,
        )
    if "hcp type" in q_lower or re.search(r"\bhcp\b", q_lower):
        return apply_appointments_hcp_breakdown(
            state,
            q,
            geo_hint,
            practice_hint,
            sql_appointments_hcp_breakdown=sql_appointments_hcp_breakdown,
        )
    if any(term in q_lower for term in ["trend", "over time", "last 12 months", "past year", "month by month"]):
        return apply_appointments_trend(
            state,
            q,
            geo_hint,
            practice_hint,
            hcp_type,
            sql_appointments_trend=sql_appointments_trend,
        )
    if "appointment" in q_lower or "appointments" in q_lower or "consultation" in q_lower:
        return apply_appointments_total(
            state,
            q,
            geo_hint,
            practice_hint,
            hcp_type,
            sql_appointments_total_latest=sql_appointments_total_latest,
        )

    return reset_appointments_query_fallthrough(state, log_info=log_info)
