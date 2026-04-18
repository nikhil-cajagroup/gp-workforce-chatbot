from __future__ import annotations

import re
from typing import Any, Callable, MutableMapping, Optional


def infer_staff_filter_from_state(
    state: MutableMapping[str, Any],
    follow_ctx: Optional[dict[str, Any]] = None,
) -> tuple[str, str]:
    follow_ctx = follow_ctx or {}
    text = " ".join(
        [
            str(state.get("conversation_history") or ""),
            str(state.get("question") or ""),
            str(state.get("original_question") or ""),
        ]
    ).lower()

    if any(term in text for term in ["trainee", "training", "registrar", "registrars"]):
        return "staff_group = 'GP' AND staff_role LIKE '%Training%'", "trainee"

    prev_sg = str(follow_ctx.get("previous_staff_group") or "")
    if prev_sg == "Nurses" or "nurse" in text:
        return "staff_group = 'Nurses'", "nurse"
    if prev_sg.startswith("Admin") or "admin" in text or "clerical" in text:
        return "staff_group = 'Admin'", "admin"
    if prev_sg in {"DPC", "Direct Patient Care"} or any(
        term in text for term in ["dpc", "pharmacist", "paramedic", "physiotherapist"]
    ):
        return "staff_group = 'DPC'", "dpc"
    return "staff_group = 'GP'", "gp"


def followup_group_dimension(follow_ctx: dict[str, Any]) -> str:
    explicit = str(follow_ctx.get("previous_group_dim") or "").lower().strip()
    if explicit in {"region", "icb", "pcn"}:
        return explicit
    grain = str(follow_ctx.get("previous_grain") or "").lower()
    if grain.startswith("region_"):
        return "region"
    if grain.startswith("icb_"):
        return "icb"
    if grain.startswith("pcn_"):
        return "pcn"
    return ""


def build_grouped_followup_sql(
    state: MutableMapping[str, Any],
    dim: str,
    *,
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    region_column_for_table: Callable[[str], str],
    infer_staff_filter_from_state_fn: Callable[[MutableMapping[str, Any], Optional[dict[str, Any]]], tuple[str, str]],
) -> Optional[dict[str, Any]]:
    follow_ctx = state.get("follow_up_context") or {}
    if not follow_ctx or follow_ctx.get("entity_name"):
        return None

    if follow_ctx.get("previous_subject") == "practice_count":
        latest = get_latest_year_month("practice_detailed")
        y, m = latest.get("year"), latest.get("month")
        if not y or not m:
            return None
        if dim == "region":
            group_col = region_column_for_table("practice_detailed")
        elif dim == "icb":
            group_col = "icb_name"
        elif dim == "pcn":
            group_col = "pcn_name"
        else:
            return None
        return {
            "plan": {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "total",
                "notes": f"Hard override: national regroup by {dim} for practice count",
                "group_by": [group_col],
            },
            "sql": f"""
SELECT
  {group_col},
  COUNT(DISTINCT prac_code) AS practice_count
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND {group_col} IS NOT NULL
  AND TRIM({group_col}) != ''
GROUP BY {group_col}
ORDER BY practice_count DESC NULLS LAST, {group_col}
LIMIT 200
""".strip(),
        }

    metric = str(follow_ctx.get("previous_metric") or "headcount")
    staff_filter, sg_label = infer_staff_filter_from_state_fn(state, follow_ctx)

    if dim == "pcn":
        if metric not in {"headcount", "fte"} or sg_label != "gp":
            return None
        latest = get_latest_year_month("practice_detailed")
        y, m = latest.get("year"), latest.get("month")
        if not y or not m:
            return None
        value_sql = (
            "SUM(CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE)) AS gp_fte"
            if metric == "fte"
            else "SUM(CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE)) AS gp_headcount"
        )
        return {
            "plan": {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "total",
                "notes": f"Hard override: national regroup by PCN for {metric}",
                "group_by": ["pcn_name"],
            },
            "sql": f"""
SELECT
  pcn_name,
  {value_sql}
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND pcn_name IS NOT NULL
  AND TRIM(pcn_name) != ''
GROUP BY pcn_name
ORDER BY 2 DESC NULLS LAST
LIMIT 200
""".strip(),
        }

    if dim not in {"region", "icb"}:
        return None

    if metric == "patients_per_gp":
        latest = get_latest_year_month("practice_detailed")
        y, m = latest.get("year"), latest.get("month")
        if not y or not m:
            return None
        group_col = "region_name" if dim == "region" else "icb_name"
        return {
            "plan": {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "ratio",
                "notes": f"Hard override: national regroup by {dim} for patients-per-GP",
                "group_by": [group_col],
            },
            "sql": f"""
SELECT
  {group_col},
  ROUND(
    SUM(TRY_CAST(NULLIF(total_patients, 'NA') AS DOUBLE)) /
    NULLIF(SUM(TRY_CAST(NULLIF(total_gp_extgl_fte, 'NA') AS DOUBLE)), 0), 1
  ) AS patients_per_gp
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND {group_col} IS NOT NULL
  AND TRIM({group_col}) != ''
GROUP BY {group_col}
ORDER BY patients_per_gp DESC NULLS LAST
LIMIT 200
""".strip(),
        }

    latest = get_latest_year_month("individual")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        return None
    group_col = region_column_for_table("individual") if dim == "region" else "icb_name"
    value_sql = (
        f"ROUND(SUM(fte), 1) AS {sg_label}_fte"
        if metric == "fte"
        else f"COUNT(DISTINCT unique_identifier) AS {sg_label}_headcount"
    )
    return {
        "plan": {
            "in_scope": True,
            "table": "individual",
            "intent": "total",
            "notes": f"Hard override: national regroup by {dim} for {sg_label}",
            "group_by": [group_col],
        },
        "sql": f"""
SELECT
  {group_col},
  {value_sql}
FROM individual
WHERE year = '{y}' AND month = '{m}'
  AND {staff_filter}
  AND {group_col} IS NOT NULL
  AND TRIM({group_col}) != ''
GROUP BY {group_col}
ORDER BY 2 DESC NULLS LAST
LIMIT 200
""".strip(),
    }


def build_group_extreme_followup_sql(
    state: MutableMapping[str, Any],
    extreme: str,
    *,
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    region_column_for_table: Callable[[str], str],
    followup_group_dimension_fn: Callable[[dict[str, Any]], str],
    infer_staff_filter_from_state_fn: Callable[[MutableMapping[str, Any], Optional[dict[str, Any]]], tuple[str, str]],
) -> Optional[dict[str, Any]]:
    follow_ctx = state.get("follow_up_context") or {}
    dim = followup_group_dimension_fn(follow_ctx)
    if dim not in {"region", "icb", "pcn"}:
        return None

    if follow_ctx.get("previous_subject") == "practice_count":
        latest = get_latest_year_month("practice_detailed")
        y, m = latest.get("year"), latest.get("month")
        if not y or not m:
            return None
        group_col = region_column_for_table("practice_detailed") if dim == "region" else ("icb_name" if dim == "icb" else "pcn_name")
        direction = "ASC" if extreme in {"least", "lowest", "fewest"} else "DESC"
        note_word = "least" if direction == "ASC" else "most"
        return {
            "plan": {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "topn",
                "notes": f"Hard override: {note_word} practices by {dim}",
                "group_by": [group_col],
            },
            "sql": f"""
SELECT
  {group_col},
  COUNT(DISTINCT prac_code) AS practice_count
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND {group_col} IS NOT NULL
  AND TRIM({group_col}) != ''
GROUP BY {group_col}
ORDER BY practice_count {direction} NULLS LAST, {group_col}
LIMIT 20
""".strip(),
        }

    metric = str(follow_ctx.get("previous_metric") or "headcount")
    staff_filter, sg_label = infer_staff_filter_from_state_fn(state, follow_ctx)
    direction = "ASC" if extreme in {"least", "lowest", "fewest"} else "DESC"

    if metric == "patients_per_gp":
        latest = get_latest_year_month("practice_detailed")
        y, m = latest.get("year"), latest.get("month")
        if not y or not m:
            return None
        group_col = region_column_for_table("practice_detailed") if dim == "region" else "icb_name"
        if dim == "pcn":
            group_col = "pcn_name"
        return {
            "plan": {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "topn",
                "notes": f"Hard override: {extreme} {dim} by patients-per-GP",
                "group_by": [group_col],
            },
            "sql": f"""
SELECT
  {group_col},
  ROUND(
    SUM(TRY_CAST(NULLIF(total_patients, 'NA') AS DOUBLE)) /
    NULLIF(SUM(TRY_CAST(NULLIF(total_gp_extgl_fte, 'NA') AS DOUBLE)), 0), 1
  ) AS patients_per_gp
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND {group_col} IS NOT NULL
  AND TRIM({group_col}) != ''
GROUP BY {group_col}
ORDER BY patients_per_gp {direction} NULLS LAST, {group_col}
LIMIT 20
""".strip(),
        }

    latest = get_latest_year_month("individual")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        return None
    group_col = region_column_for_table("individual") if dim == "region" else ("icb_name" if dim == "icb" else "pcn_name")
    value_sql = (
        f"ROUND(SUM(fte), 1) AS {sg_label}_fte"
        if metric == "fte"
        else f"COUNT(DISTINCT unique_identifier) AS {sg_label}_headcount"
    )
    value_col = f"{sg_label}_fte" if metric == "fte" else f"{sg_label}_headcount"
    return {
        "plan": {
            "in_scope": True,
            "table": "individual",
            "intent": "topn",
            "notes": f"Hard override: {extreme} {dim} by {sg_label} {metric}",
            "group_by": [group_col],
        },
        "sql": f"""
SELECT
  {group_col},
  {value_sql}
FROM individual
WHERE year = '{y}' AND month = '{m}'
  AND {staff_filter}
  AND {group_col} IS NOT NULL
  AND TRIM({group_col}) != ''
GROUP BY {group_col}
ORDER BY {value_col} {direction} NULLS LAST, {group_col}
LIMIT 20
""".strip(),
    }


def build_total_change_followup_sql(
    state: MutableMapping[str, Any],
    *,
    get_latest_year_month: Callable[[str], dict[str, str | None]],
) -> Optional[dict[str, Any]]:
    follow_ctx = state.get("follow_up_context") or {}
    question = (state.get("original_question") or state.get("question") or "").lower()
    if "last year" not in question and "over the last year" not in question and "changed" not in question:
        return None

    if follow_ctx.get("previous_subject") == "practice_count":
        latest = get_latest_year_month("practice_detailed")
        y, m = latest.get("year"), latest.get("month")
        if not y or not m:
            return None
        prev_y = str(int(y) - 1)
        return {
            "plan": {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "comparison",
                "notes": "Hard override: year-over-year change in total practice count",
            },
            "sql": f"""
SELECT
  COUNT(DISTINCT CASE WHEN year = '{y}' AND month = '{m}' THEN prac_code END) AS practice_count_{y},
  COUNT(DISTINCT CASE WHEN year = '{prev_y}' AND month = '{m}' THEN prac_code END) AS practice_count_{prev_y}
FROM practice_detailed
WHERE (year = '{y}' AND month = '{m}')
   OR (year = '{prev_y}' AND month = '{m}')
LIMIT 200
""".strip(),
        }
    return None


def build_geo_compare_followup_sql(
    state: MutableMapping[str, Any],
    target_hint: str,
    *,
    clean_entity_hint: Callable[[str], str],
    normalise_region_name: Callable[[str], str],
    sanitise_entity_input: Callable[[str, str], str],
    region_column_for_table: Callable[[str], str],
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    infer_staff_filter_from_state_fn: Callable[[MutableMapping[str, Any], Optional[dict[str, Any]]], tuple[str, str]],
) -> Optional[dict[str, Any]]:
    follow_ctx = state.get("follow_up_context") or {}
    entity_name = str(follow_ctx.get("entity_name") or "").strip().lower()
    entity_type = str(follow_ctx.get("entity_type") or "").strip().lower()
    if entity_type not in {"region", "icb", "sub_icb"} or not entity_name:
        return None

    target_name = clean_entity_hint(target_hint).lower()
    if not target_name:
        return None

    if entity_type == "region":
        current_value = normalise_region_name(entity_name)
        target_value = normalise_region_name(target_name)
        table = "practice_detailed" if (
            follow_ctx.get("previous_subject") == "practice_count"
            or str(follow_ctx.get("previous_metric") or "") == "patients_per_gp"
        ) else "individual"
        group_col = region_column_for_table(table)
    elif entity_type == "icb":
        current_value = sanitise_entity_input(entity_name, "icb_name")
        target_value = sanitise_entity_input(target_name, "icb_name")
        group_col = "icb_name"
    else:
        current_value = sanitise_entity_input(entity_name, "sub_icb_name")
        target_value = sanitise_entity_input(target_name, "sub_icb_name")
        group_col = "sub_icb_name"

    if not current_value or not target_value:
        return None

    metric = str(follow_ctx.get("previous_metric") or "headcount")
    current_sql = current_value.lower().replace("'", "''")
    target_sql = target_value.lower().replace("'", "''")

    if follow_ctx.get("previous_subject") == "practice_count":
        latest = get_latest_year_month("practice_detailed")
        y, m = latest.get("year"), latest.get("month")
        if not y or not m:
            return None
        return {
            "plan": {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "comparison",
                "notes": f"Hard override: compare practice counts for {current_value} and {target_value}",
                "group_by": [group_col],
            },
            "sql": f"""
SELECT
  {group_col},
  COUNT(DISTINCT prac_code) AS practice_count
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND LOWER(TRIM({group_col})) IN ('{current_sql}', '{target_sql}')
GROUP BY {group_col}
ORDER BY practice_count DESC NULLS LAST, {group_col}
LIMIT 20
""".strip(),
        }

    if metric == "patients_per_gp":
        latest = get_latest_year_month("practice_detailed")
        y, m = latest.get("year"), latest.get("month")
        if not y or not m:
            return None
        return {
            "plan": {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "comparison",
                "notes": f"Hard override: compare patients-per-GP for {current_value} and {target_value}",
                "group_by": [group_col],
            },
            "sql": f"""
SELECT
  {group_col},
  ROUND(
    SUM(TRY_CAST(NULLIF(total_patients, 'NA') AS DOUBLE)) /
    NULLIF(SUM(TRY_CAST(NULLIF(total_gp_extgl_fte, 'NA') AS DOUBLE)), 0), 1
  ) AS patients_per_gp
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND LOWER(TRIM({group_col})) IN ('{current_sql}', '{target_sql}')
GROUP BY {group_col}
ORDER BY patients_per_gp DESC NULLS LAST, {group_col}
LIMIT 20
""".strip(),
        }

    latest = get_latest_year_month("individual")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        return None
    staff_filter, sg_label = infer_staff_filter_from_state_fn(state, follow_ctx)
    value_sql = (
        f"ROUND(SUM(fte), 1) AS {sg_label}_fte"
        if metric == "fte"
        else f"COUNT(DISTINCT unique_identifier) AS {sg_label}_headcount"
    )
    value_col = f"{sg_label}_fte" if metric == "fte" else f"{sg_label}_headcount"
    return {
        "plan": {
            "in_scope": True,
            "table": "individual",
            "intent": "comparison",
            "notes": f"Hard override: compare {sg_label} {metric} for {current_value} and {target_value}",
            "group_by": [group_col],
        },
        "sql": f"""
SELECT
  {group_col},
  {value_sql}
FROM individual
WHERE year = '{y}' AND month = '{m}'
  AND {staff_filter}
  AND LOWER(TRIM({group_col})) IN ('{current_sql}', '{target_sql}')
GROUP BY {group_col}
ORDER BY {value_col} DESC NULLS LAST, {group_col}
LIMIT 20
""".strip(),
    }


def build_top_practices_followup_sql(
    state: MutableMapping[str, Any],
    *,
    effective_scope_context: Callable[[dict[str, Any], str], dict[str, Any]],
    geo_filter_from_context: Callable[[dict[str, Any], str], Optional[str]],
    get_latest_year_month: Callable[[str], dict[str, str | None]],
) -> Optional[dict[str, Any]]:
    raw_follow_ctx = state.get("follow_up_context") or {}
    orig_q = (state.get("original_question") or "").lower()
    follow_ctx = effective_scope_context(raw_follow_ctx, orig_q)
    if not follow_ctx or not follow_ctx.get("entity_name"):
        return None

    entity_type = str(follow_ctx.get("entity_type") or "")
    if entity_type not in {"city", "icb", "sub_icb", "region"}:
        return None

    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        return None

    geo_filter = geo_filter_from_context(follow_ctx, table_hint="practice_detailed")
    if not geo_filter:
        return None

    select_metric_cols = "total_gp_hc, total_gp_fte"
    ranking_words = ["top", "most", "highest", "lowest", "largest", "biggest", "fewest", "least"]
    is_patient_ranking = any(term in orig_q for term in ["patients", "patient", "list size", "registered"])
    is_ratio_ranking = any(term in orig_q for term in ["patients per gp", "patient to gp", "gp ratio"])
    is_practice_ranking = "practice" in orig_q and any(term in orig_q for term in ranking_words)
    is_scope_relative_ranking = (
        any(term in orig_q for term in ranking_words)
        and any(term in orig_q for term in ["patients per gp", "patient to gp", "gp ratio", "patients", "patient", "gps", "gp", "fte"])
        and not any(term in orig_q for term in [" icb", " region", "sub-icb", "sub icb", "pcn"])
    )

    if not is_practice_ranking and not is_scope_relative_ranking and not re.search(r"\btop\s+(?:\d+\s+)?practices?\b", orig_q):
        return None

    if is_ratio_ranking:
        select_metric_cols = (
            "total_patients, total_gp_hc, total_gp_extgl_fte, "
            "ROUND(TRY_CAST(NULLIF(total_patients, 'NA') AS DOUBLE) / "
            "NULLIF(TRY_CAST(NULLIF(total_gp_extgl_fte, 'NA') AS DOUBLE), 0), 1) AS patients_per_gp_fte"
        )
        order_expr = (
            "ROUND(TRY_CAST(NULLIF(total_patients, 'NA') AS DOUBLE) / "
            "NULLIF(TRY_CAST(NULLIF(total_gp_extgl_fte, 'NA') AS DOUBLE), 0), 1)"
        )
        notes = f"Hard override: top practices within {follow_ctx.get('entity_name')} by patients-per-GP ratio"
    elif is_patient_ranking:
        select_metric_cols = "total_patients, total_gp_hc, total_gp_fte"
        order_expr = "TRY_CAST(NULLIF(total_patients, 'NA') AS DOUBLE)"
        notes = f"Hard override: largest practices within {follow_ctx.get('entity_name')} by patient count"
    else:
        metric = str(follow_ctx.get("previous_metric") or "fte")
        if "headcount" in orig_q or "head count" in orig_q:
            metric = "headcount"
        elif "fte" in orig_q:
            metric = "fte"

        if metric == "headcount":
            order_expr = "TRY_CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE)"
            notes = f"Hard override: top practices within {follow_ctx.get('entity_name')} by GP headcount"
        else:
            order_expr = "TRY_CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE)"
            notes = f"Hard override: top practices within {follow_ctx.get('entity_name')} by GP FTE"

    return {
        "plan": {
            "in_scope": True,
            "table": "practice_detailed",
            "intent": "topn",
            "notes": notes,
        },
        "sql": f"""
SELECT
  prac_code, prac_name, pcn_name, sub_icb_name, icb_name,
  {select_metric_cols},
  '{y}' AS year, '{m}' AS month
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_name IS NOT NULL
  AND {geo_filter}
ORDER BY {order_expr} DESC NULLS LAST, prac_name
LIMIT 10
""".strip(),
    }
