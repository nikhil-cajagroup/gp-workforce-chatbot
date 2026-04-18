from __future__ import annotations

from typing import Any, Callable, MutableMapping, Optional


def build_national_patients_per_gp_yoy_override(
    state: MutableMapping[str, Any],
    *,
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    log_info: Callable[[str], None],
) -> bool:
    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not (y and m):
        return False
    prev_y = str(int(y) - 1)
    state["plan"] = {
        "in_scope": True,
        "table": "practice_detailed",
        "intent": "comparison",
        "notes": "Hard override: year-over-year change in national patients-per-GP ratio",
    }
    state["sql"] = f"""
SELECT
  ROUND(
    SUM(CASE WHEN year = '{prev_y}' AND month = '{m}' THEN TRY_CAST(NULLIF(total_patients, 'NA') AS DOUBLE) ELSE 0 END) /
    NULLIF(SUM(CASE WHEN year = '{prev_y}' AND month = '{m}' THEN TRY_CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE) ELSE 0 END), 0),
    1
  ) AS patients_per_gp_{prev_y},
  ROUND(
    SUM(CASE WHEN year = '{y}' AND month = '{m}' THEN TRY_CAST(NULLIF(total_patients, 'NA') AS DOUBLE) ELSE 0 END) /
    NULLIF(SUM(CASE WHEN year = '{y}' AND month = '{m}' THEN TRY_CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE) ELSE 0 END), 0),
    1
  ) AS patients_per_gp_{y}
FROM practice_detailed
WHERE (year = '{prev_y}' AND month = '{m}')
   OR (year = '{y}' AND month = '{m}')
LIMIT 200
""".strip()
    log_info("node_hard_override | national patients-per-GP year-over-year change")
    return True


def apply_workforce_ratio_overrides(
    state: MutableMapping[str, Any],
    orig_q: str,
    follow_ctx: Optional[dict[str, Any]],
    *,
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    build_national_patients_per_gp_yoy_override_fn: Callable[[MutableMapping[str, Any]], bool],
    log_info: Callable[[str], None],
) -> bool:
    if __import__("re").search(r"\bwhich\s+icb\b.*\bpatients[\s-]?per[\s-]?gp\b", orig_q):
        latest = get_latest_year_month("practice_detailed")
        y, m = latest.get("year"), latest.get("month")
        if y and m:
            state["plan"] = {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "topn",
                "notes": "Hard override: highest patients-per-GP ratio by ICB",
                "group_by": ["icb_name"],
            }
            state["sql"] = f"""
SELECT
  icb_name,
  ROUND(
    SUM(TRY_CAST(NULLIF(total_patients, 'NA') AS DOUBLE)) /
    NULLIF(SUM(TRY_CAST(NULLIF(total_gp_extgl_fte, 'NA') AS DOUBLE)), 0),
    1
  ) AS patients_per_gp
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND icb_name IS NOT NULL
  AND TRIM(icb_name) != ''
GROUP BY icb_name
ORDER BY patients_per_gp DESC NULLS LAST, icb_name
LIMIT 20
""".strip()
            log_info("node_hard_override | top-level patients-per-GP by ICB")
            return True

    if __import__("re").search(r"\bpatients[\s-]?per[\s-]?gp\b.*\bacross all practices\b", orig_q):
        latest = get_latest_year_month("practice_detailed")
        y, m = latest.get("year"), latest.get("month")
        if y and m:
            state["plan"] = {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "ratio",
                "notes": "Hard override: national patients per GP across all practices",
            }
            state["sql"] = f"""
SELECT
  ROUND(
    SUM(TRY_CAST(NULLIF(total_patients, 'NA') AS DOUBLE)) /
    NULLIF(SUM(TRY_CAST(NULLIF(total_gp_extgl_fte, 'NA') AS DOUBLE)), 0),
    1
  ) AS patients_per_gp
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
LIMIT 200
""".strip()
            log_info("node_hard_override | top-level national patients-per-GP across all practices")
            return True

    if (
        follow_ctx
        and str(follow_ctx.get("previous_metric") or "") == "patients_per_gp"
        and str(follow_ctx.get("previous_grain") or "").startswith("national_")
        and any(term in orig_q for term in ["changed", "change over", "past year", "last year", "over the past year"])
    ):
        return build_national_patients_per_gp_yoy_override_fn(state)

    return False


def resolve_workforce_override_hint(
    state: MutableMapping[str, Any],
    hi: str,
    follow_ctx: Optional[dict[str, Any]],
    geo_hint: str,
    *,
    specific_entity_hint: Callable[[str, str], str],
    extract_entity_hint: Callable[[str], str],
    extract_practice_code: Callable[[str], Optional[str]],
    log_info: Callable[[str], None],
) -> tuple[str, str, set[str]]:
    raw_q = str(state.get("original_question", "") or "")
    if hi in ("practice_gp_count", "practice_gp_count_soft", "practice_patient_count", "practice_staff_breakdown", "practice_to_icb_lookup"):
        hint = specific_entity_hint(raw_q, "practice")
    else:
        hint = extract_entity_hint(raw_q)
    log_info(f"node_hard_override | intent={hi} hint='{hint[:60]}'")

    pronouns = {"this", "that", "it", "them", "they", "these", "those", "the same", "same"}
    if not hint or hint == raw_q or len(hint) > 50 or hint.lower().strip() in pronouns:
        if follow_ctx and str(follow_ctx.get("entity_type") or "") == "practice" and follow_ctx.get("previous_entity_code"):
            hint = str(follow_ctx.get("previous_entity_code"))
        elif follow_ctx and follow_ctx.get("entity_name"):
            hint = str(follow_ctx["entity_name"])
        elif hi in ("practice_gp_count", "practice_gp_count_soft", "practice_patient_count", "practice_staff_breakdown", "practice_to_icb_lookup"):
            return "", raw_q, pronouns

    if hi == "patients_per_gp":
        practice_code = extract_practice_code(raw_q)
        hint_low = (hint or "").strip().lower()
        orig_q_low = raw_q.strip().lower().rstrip(" ?!.")
        looks_like_metric_phrase = any(
            term in hint_low
            for term in [
                "patients per gp",
                "patient to gp",
                "gp ratio",
                "average",
                "ratio",
                "what",
                "how many",
                "show",
                "count",
                "number",
            ]
        )
        if practice_code:
            hint = practice_code
        elif follow_ctx and str(follow_ctx.get("entity_type") or "") == "practice":
            hint = str(follow_ctx.get("previous_entity_code") or follow_ctx.get("entity_name") or "").strip()
        elif geo_hint:
            hint = geo_hint
        elif (
            not hint
            or hint == raw_q
            or len(hint) > 60
            or hint_low in pronouns
            or hint_low.rstrip(" ?!.") == orig_q_low
            or looks_like_metric_phrase
        ):
            hint = ""

    return hint, raw_q, pronouns


def apply_workforce_practice_gp_count_override(
    state: MutableMapping[str, Any],
    hi: str,
    hint: str,
    *,
    city_to_icb_for_hint: Callable[[str], Optional[str]],
    is_known_icb_fragment_hint: Callable[[str], bool],
    is_national_scope_hint: Callable[[str], bool],
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    build_sql_practice_gp_count_latest: Callable[[str], str],
) -> bool:
    if hi not in ("practice_gp_count", "practice_gp_count_soft"):
        return False
    hint_lower = hint.strip().lower()
    mapped_icb = city_to_icb_for_hint(hint_lower)
    is_known_icb = is_known_icb_fragment_hint(hint_lower)
    if is_national_scope_hint(hint_lower):
        is_known_icb = False
    if mapped_icb or is_known_icb:
        latest = get_latest_year_month("individual")
        y, m = latest.get("year"), latest.get("month")
        if y and m:
            geo_filter = (
                f"(LOWER(TRIM(icb_name)) LIKE '%{mapped_icb}%' OR LOWER(TRIM(sub_icb_name)) LIKE '%{hint_lower}%')"
                if mapped_icb
                else f"LOWER(TRIM(icb_name)) LIKE '%{hint_lower}%'"
            )
            state["plan"] = {
                "in_scope": True,
                "table": "individual",
                "intent": "total",
                "notes": f"Hard override: area GP count for '{hint}'",
            }
            state["sql"] = f"""SELECT COUNT(DISTINCT unique_identifier) AS gp_count, ROUND(SUM(fte), 1) AS gp_fte
FROM individual
WHERE year = '{y}' AND month = '{m}'
  AND staff_group = 'GP'
  AND {geo_filter}
LIMIT 200""".strip()
            return True
    if is_national_scope_hint(hint_lower):
        latest = get_latest_year_month("individual")
        y, m = latest.get("year"), latest.get("month")
        if y and m:
            state["plan"] = {
                "in_scope": True,
                "table": "individual",
                "intent": "total",
                "notes": "Hard override: national GP count",
            }
            state["sql"] = f"""SELECT COUNT(DISTINCT unique_identifier) AS gp_count, ROUND(SUM(fte), 1) AS gp_fte
FROM individual
WHERE year = '{y}' AND month = '{m}'
  AND staff_group = 'GP'
LIMIT 200""".strip()
            return True
    state["plan"] = {
        "in_scope": True,
        "table": "practice_detailed",
        "intent": "lookup",
        "notes": f"Hard override: GP count for '{hint}'",
    }
    state["sql"] = build_sql_practice_gp_count_latest(hint)
    return True


def apply_workforce_practice_lookup_intents(
    state: MutableMapping[str, Any],
    hi: str,
    hint: str,
    *,
    build_sql_pcn_gp_count: Callable[[], str],
    build_sql_practice_to_icb_latest: Callable[[str], str],
    build_sql_practice_patient_count: Callable[[str], str],
    build_sql_practice_staff_breakdown: Callable[[str], str],
) -> bool:
    if hi == "pcn_gp_count":
        state["plan"] = {
            "in_scope": True,
            "table": "practice_detailed",
            "intent": "total",
            "notes": "Hard override: GP count grouped by PCN",
            "group_by": ["pcn_name"],
        }
        state["sql"] = build_sql_pcn_gp_count()
        return True
    if hi == "practice_to_icb_lookup":
        state["plan"] = {
            "in_scope": True,
            "table": "practice_detailed",
            "intent": "lookup",
            "notes": f"Hard override: ICB lookup for '{hint}'",
        }
        state["sql"] = build_sql_practice_to_icb_latest(hint)
        return True
    if hi == "practice_patient_count":
        state["plan"] = {
            "in_scope": True,
            "table": "practice_detailed",
            "intent": "lookup",
            "notes": f"Hard override: patient count for '{hint}'",
        }
        state["sql"] = build_sql_practice_patient_count(hint)
        return True
    if hi == "practice_staff_breakdown":
        state["plan"] = {
            "in_scope": True,
            "table": "practice_detailed",
            "intent": "lookup",
            "notes": f"Hard override: staff breakdown for '{hint}'",
        }
        state["sql"] = build_sql_practice_staff_breakdown(hint)
        return True
    return False


def apply_workforce_patients_per_gp_intent(
    state: MutableMapping[str, Any],
    hi: str,
    hint: str,
    raw_q: str,
    *,
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    geo_filter_from_hint_text: Callable[[str, str, str], Optional[str]],
    build_sql_patients_per_gp: Callable[[str], str],
) -> bool:
    if hi != "patients_per_gp":
        return False
    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not (y and m):
        return False

    if any(term in raw_q for term in ["which icb", "top icb", "highest"]) and "icb" in raw_q:
        state["plan"] = {
            "in_scope": True,
            "table": "practice_detailed",
            "intent": "topn",
            "notes": "Hard override: highest patients-per-GP ratio by ICB",
            "group_by": ["icb_name"],
        }
        state["sql"] = f"""
SELECT
  icb_name,
  ROUND(
    SUM(TRY_CAST(NULLIF(total_patients, 'NA') AS DOUBLE)) /
    NULLIF(SUM(TRY_CAST(NULLIF(total_gp_extgl_fte, 'NA') AS DOUBLE)), 0),
    1
  ) AS patients_per_gp
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND icb_name IS NOT NULL
  AND TRIM(icb_name) != ''
GROUP BY icb_name
ORDER BY patients_per_gp DESC NULLS LAST, icb_name
LIMIT 20
""".strip()
        return True

    if hint:
        geo_filter = geo_filter_from_hint_text(raw_q, hint, table_hint="practice_detailed")
        if geo_filter:
            state["plan"] = {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "ratio",
                "notes": f"Hard override: patients per GP for geographic scope '{hint}'",
            }
            state["sql"] = f"""
SELECT
  ROUND(
    SUM(TRY_CAST(NULLIF(total_patients, 'NA') AS DOUBLE)) /
    NULLIF(SUM(TRY_CAST(NULLIF(total_gp_extgl_fte, 'NA') AS DOUBLE)), 0),
    1
  ) AS patients_per_gp
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND {geo_filter}
LIMIT 200
""".strip()
            return True

    if not hint:
        state["plan"] = {
            "in_scope": True,
            "table": "practice_detailed",
            "intent": "ratio",
            "notes": "Hard override: national patients per GP",
        }
        state["sql"] = f"""
SELECT ROUND(
  SUM(TRY_CAST(NULLIF(total_patients, 'NA') AS DOUBLE)) /
  NULLIF(SUM(TRY_CAST(NULLIF(total_gp_extgl_fte, 'NA') AS DOUBLE)), 0), 1
) AS patients_per_gp
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
LIMIT 200
""".strip()
        return True

    state["plan"] = {
        "in_scope": True,
        "table": "practice_detailed",
        "intent": "ratio",
        "notes": f"Hard override: patients per GP for '{hint}'",
    }
    state["sql"] = build_sql_patients_per_gp(hint)
    return True
