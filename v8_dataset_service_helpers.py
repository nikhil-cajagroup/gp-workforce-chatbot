from __future__ import annotations

import re
from typing import Any, Callable, MutableMapping, Optional


def load_simple_vocab_from_config(
    state: MutableMapping[str, Any],
    config: dict[str, Any],
    *,
    get_latest_year_month: Callable[[str, str], dict[str, str | None]],
    list_distinct_values: Callable[..., list[Any]],
) -> MutableMapping[str, Any]:
    dataset_name = config["name"]
    athena_db = config.get("athena_database")
    latest_table = config.get("latest_table") or config.get("vocab_table") or config["default_table"]
    vocab_table = config.get("vocab_table") or latest_table
    vocab_columns = config.get("vocab_columns") or {}

    state["dataset"] = dataset_name
    latest = get_latest_year_month(latest_table, database=athena_db)
    state["latest_year"] = latest.get("year")
    state["latest_month"] = latest.get("month")
    y, m = state["latest_year"], state["latest_month"]
    where_latest = f"year = '{y}' AND month = '{m}'" if y and m else None

    for state_key, spec in vocab_columns.items():
        column = str(spec.get("column") or "").strip()
        limit = int(spec.get("limit", 50))
        if not column:
            state[state_key] = []
            continue
        try:
            state[state_key] = list_distinct_values(
                vocab_table,
                column,
                where_sql=where_latest,
                limit=limit,
                database=athena_db,
            )
        except Exception:
            state[state_key] = []
    return state


def load_workforce_latest_and_vocab(
    state: MutableMapping[str, Any],
    *,
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    resolve_time_range: Callable[[str, str, str], dict[str, Any]],
    list_distinct_values: Callable[..., list[Any]],
) -> MutableMapping[str, Any]:
    state["dataset"] = "workforce"
    latest = get_latest_year_month("individual")
    state["latest_year"] = latest.get("year")
    state["latest_month"] = latest.get("month")

    y, m = state["latest_year"], state["latest_month"]
    where_latest = f"year = '{y}' AND month = '{m}'" if y and m else None

    if y and m:
        state["time_range"] = resolve_time_range(str(state.get("question", "")), y, m)

    state["staff_groups"] = list_distinct_values("individual", "staff_group", where_sql=where_latest, limit=300)
    try:
        state["staff_roles"] = list_distinct_values("individual", "staff_role", where_sql=where_latest, limit=400)
    except Exception:
        state["staff_roles"] = []
    try:
        state["detailed_staff_roles"] = list_distinct_values("individual", "detailed_staff_role", where_sql=where_latest, limit=600)
    except Exception:
        state["detailed_staff_roles"] = []

    latest_ph = get_latest_year_month("practice_high")
    yp, mp = latest_ph.get("year"), latest_ph.get("month")
    where_ph = f"year = '{yp}' AND month = '{mp}'" if yp and mp else None
    try:
        state["practice_high_measures"] = list_distinct_values("practice_high", "measure", where_sql=where_ph, limit=50)
    except Exception:
        state["practice_high_measures"] = ["FTE", "Headcount"]
    try:
        state["practice_high_staff_groups"] = list_distinct_values("practice_high", "staff_group", where_sql=where_ph, limit=50)
    except Exception:
        state["practice_high_staff_groups"] = []
    try:
        state["practice_high_detailed_roles"] = list_distinct_values("practice_high", "detailed_staff_role", where_sql=where_ph, limit=200)
    except Exception:
        state["practice_high_detailed_roles"] = []
    return state


def default_semantic_issue_checker(
    state: MutableMapping[str, Any],
    issues: list[str],
) -> list[str]:
    return issues


def appointments_semantic_issue_checker(
    state: MutableMapping[str, Any],
    issues: list[str],
    *,
    extract_practice_code: Callable[[str], Optional[str]],
) -> list[str]:
    sql_low = (state.get("sql") or "").strip().lower()
    original_q = (state.get("original_question") or state.get("question") or "").lower()
    if not sql_low:
        return issues

    if any(tbl in sql_low for tbl in [" individual", " practice_high", " practice_detailed"]):
        issues.append("Appointments queries must not use workforce tables.")
    if any(term in original_q for term in ["appointment", "appointments", "consultation", "consultations"]) and "count_of_appointments" not in sql_low:
        issues.append("Appointments queries should use count_of_appointments.")
    if "dna rate" in original_q or ("dna" in original_q and "rate" in original_q):
        if "appt_status" not in sql_low or "'dna'" not in sql_low:
            issues.append("DNA rate queries must filter appt_status = 'DNA'.")
    if any(term in original_q for term in ["appointment mode", "by mode", "face-to-face", "telephone", "video", "online", "home visit"]):
        if "appt_mode" not in sql_low:
            issues.append("Appointment mode queries should use appt_mode.")
    if any(term in original_q for term in ["hcp type", "health care professional", "by hcp", "by clinician type"]):
        if "hcp_type" not in sql_low:
            issues.append("HCP queries should use hcp_type.")
    if any(term in original_q for term in ["time between booking", "time between book and appt", "booking lead time", "book and appt"]):
        if "time_between_book_and_appt" not in sql_low:
            issues.append("Booking lead-time queries should use time_between_book_and_appt.")
    if any(term in original_q for term in [" icb", "region", "sub-icb", "sub icb", "pcn"]) and "from pcn_subicb" not in sql_low:
        issues.append("Appointments geography queries should usually use pcn_subicb.")
    if (extract_practice_code(original_q) or re.search(r"\bpractice\b", original_q)) and any(term in original_q for term in ["appointments", "consultations"]):
        if "from practice" not in sql_low and "from pcn_subicb" not in sql_low:
            issues.append("Practice-level appointments queries should use the practice table.")
    return issues


def dataset_schema_text(
    table: str,
    config: dict[str, Any],
    *,
    get_table_schema: Callable[[str], list[tuple[str, str]]],
    max_columns: Optional[int] = None,
) -> str:
    schema = get_table_schema(table)
    display_rules = ((config.get("schema_display") or {}).get(table) or {})

    selected = schema
    prefixes = list(display_rules.get("prefixes") or [])
    if prefixes:
        selected = [(c, t) for c, t in schema if any(c.startswith(p) or c == p for p in prefixes)]
        if not selected:
            selected = schema

    limit = int(max_columns or display_rules.get("max_columns") or 120)
    schema_text = "\n".join([f"- {c} ({t})" for c, t in selected[:limit]])
    if display_rules.get("append_total_count") and len(schema) > len(selected):
        schema_text += f"\n... ({len(schema)} total columns, showing key ones only)"
    return schema_text


def dataset_valid_values_block(
    state: MutableMapping[str, Any],
    config: dict[str, Any],
) -> str:
    sections = list(config.get("valid_values_specs") or [])
    if not sections:
        return ""

    rendered_sections: list[str] = []
    for section in sections:
        title = str(section.get("title") or "").strip()
        items = list(section.get("items") or [])
        lines = [title] if title else []
        for column_name, state_key, limit in items:
            values = state.get(str(state_key), [])
            try:
                limit_n = int(limit)
            except Exception:
                limit_n = 50
            rendered_value = values[:limit_n] if isinstance(values, list) else values
            lines.append(f"- {column_name}: {rendered_value}")
        rendered_sections.append("\n".join(lines).strip())
    return "\n\n".join(section for section in rendered_sections if section).strip()
