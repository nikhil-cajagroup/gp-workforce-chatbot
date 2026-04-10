from __future__ import annotations

import re
from typing import Any, Callable, MutableMapping, Optional


def apply_workforce_followup_lookup_overrides(
    state: MutableMapping[str, Any],
    orig_q: str,
    follow_ctx: Optional[dict[str, Any]],
    *,
    sql_practice_staff_breakdown: Callable[[str], str],
    log_info: Callable[[str], None],
) -> bool:
    if not follow_ctx:
        return False
    if (
        str(follow_ctx.get("entity_type") or "") == "practice"
        and any(term in orig_q for term in ["staff breakdown", "full staff breakdown", "all staff", "show the full staff"])
    ):
        practice_hint = str(follow_ctx.get("previous_entity_code") or follow_ctx.get("entity_name") or "").strip()
        if practice_hint:
            state["plan"] = {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "lookup",
                "notes": f"Hard override: practice staff breakdown follow-up for '{practice_hint}'",
            }
            state["sql"] = sql_practice_staff_breakdown(practice_hint)
            log_info(f"node_hard_override | practice staff breakdown follow-up for {practice_hint}")
            return True
    return False


def apply_workforce_demographic_overrides(
    state: MutableMapping[str, Any],
    orig_q: str,
    geo_hint: str,
    *,
    geo_filter_from_hint_text: Callable[[str, str, str], Optional[str]],
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    age_60_plus_filter: Callable[[], str],
    log_info: Callable[[str], None],
) -> bool:
    if geo_hint and any(term in orig_q for term in ["percentage", "proportion", "percent"]) and "gp" in orig_q and any(term in orig_q for term in ["full time", "full-time"]):
        geo_filter = geo_filter_from_hint_text(orig_q, geo_hint, table_hint="individual")
        latest = get_latest_year_month("individual")
        y, m = latest.get("year"), latest.get("month")
        if geo_filter and y and m:
            state["plan"] = {
                "in_scope": True,
                "table": "individual",
                "intent": "ratio",
                "notes": f"Hard override: full-time GP proportion for '{geo_hint}'",
            }
            state["sql"] = f"""
SELECT
  COUNT(DISTINCT unique_identifier) AS gp_headcount,
  ROUND(SUM(fte), 1) AS gp_fte,
  ROUND(100.0 * SUM(fte) / NULLIF(COUNT(DISTINCT unique_identifier), 0), 1) AS full_time_pct_proxy
FROM individual
WHERE year = '{y}' AND month = '{m}'
  AND staff_group = 'GP'
  AND {geo_filter}
LIMIT 200
""".strip()
            log_info(f"node_hard_override | full-time GP proportion for {geo_hint}")
            return True

    if re.search(r"\bage distribution\b.*\bgps?\b", orig_q) and geo_hint:
        geo_filter = geo_filter_from_hint_text(orig_q, geo_hint, table_hint="individual")
        latest = get_latest_year_month("individual")
        y, m = latest.get("year"), latest.get("month")
        if geo_filter and y and m:
            state["plan"] = {
                "in_scope": True,
                "table": "individual",
                "intent": "demographics",
                "notes": f"Hard override: GP age distribution for '{geo_hint}'",
                "group_by": ["age_band"],
            }
            state["sql"] = f"""
SELECT
  age_band,
  COUNT(DISTINCT unique_identifier) AS gp_headcount,
  ROUND(SUM(fte), 1) AS gp_fte
FROM individual
WHERE year = '{y}' AND month = '{m}'
  AND staff_group = 'GP'
  AND {geo_filter}
  AND age_band IS NOT NULL
GROUP BY age_band
ORDER BY age_band
LIMIT 200
""".strip()
            log_info(f"node_hard_override | GP age distribution for {geo_hint}")
            return True

    if re.search(r"\baged\b.*\bover\s+60\b.*\beach icb\b", orig_q) or re.search(r"\bover\s+60\b.*\beach icb\b", orig_q):
        latest = get_latest_year_month("individual")
        y, m = latest.get("year"), latest.get("month")
        if y and m:
            age_filter = age_60_plus_filter()
            state["plan"] = {
                "in_scope": True,
                "table": "individual",
                "intent": "demographics",
                "notes": "Hard override: proportion of GPs aged 60+ by ICB",
                "group_by": ["icb_name"],
            }
            state["sql"] = f"""
SELECT
  icb_name,
  COUNT(DISTINCT CASE WHEN {age_filter} THEN unique_identifier END) AS gp_over_60_count,
  COUNT(DISTINCT unique_identifier) AS gp_total_count,
  ROUND(
    100.0 * COUNT(DISTINCT CASE WHEN {age_filter} THEN unique_identifier END) /
    NULLIF(COUNT(DISTINCT unique_identifier), 0),
    1
  ) AS gp_over_60_pct
FROM individual
WHERE year = '{y}' AND month = '{m}'
  AND staff_group = 'GP'
  AND icb_name IS NOT NULL
  AND TRIM(icb_name) != ''
GROUP BY icb_name
ORDER BY gp_over_60_pct DESC NULLS LAST, icb_name
LIMIT 200
""".strip()
            log_info("node_hard_override | over-60 GP proportion by ICB")
            return True
    return False


def apply_workforce_grouped_comparison_overrides(
    state: MutableMapping[str, Any],
    orig_q: str,
    *,
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    region_column_for_table: Callable[[str], str],
    log_info: Callable[[str], None],
) -> bool:
    if re.search(r"\bcompare\b.*\bgp\b.*\b(?:numbers|count|headcount)\b.*\ball regions\b", orig_q) or re.search(r"\ball regions comparison\b", orig_q):
        latest = get_latest_year_month("individual")
        y, m = latest.get("year"), latest.get("month")
        region_col = region_column_for_table("individual")
        if y and m:
            state["plan"] = {
                "in_scope": True,
                "table": "individual",
                "intent": "comparison",
                "notes": "Hard override: GP headcount by region",
                "group_by": [region_col],
            }
            state["sql"] = f"""
SELECT
  {region_col},
  COUNT(DISTINCT unique_identifier) AS gp_headcount,
  ROUND(SUM(fte), 1) AS gp_fte
FROM individual
WHERE year = '{y}' AND month = '{m}'
  AND staff_group = 'GP'
  AND {region_col} IS NOT NULL
  AND TRIM({region_col}) != ''
GROUP BY {region_col}
ORDER BY gp_headcount DESC NULLS LAST, {region_col}
LIMIT 200
""".strip()
            log_info("node_hard_override | GP comparison across all regions")
            return True

    if re.search(r"\bgp\b.*\bheadcount\b.*\bby icb\b.*\bsouth east\b", orig_q):
        latest = get_latest_year_month("individual")
        y, m = latest.get("year"), latest.get("month")
        region_col = region_column_for_table("individual")
        if y and m:
            state["plan"] = {
                "in_scope": True,
                "table": "individual",
                "intent": "comparison",
                "notes": "Hard override: GP headcount by ICB in South East",
                "group_by": ["icb_name"],
            }
            state["sql"] = f"""
SELECT
  icb_name,
  COUNT(DISTINCT unique_identifier) AS gp_headcount,
  ROUND(SUM(fte), 1) AS gp_fte
FROM individual
WHERE year = '{y}' AND month = '{m}'
  AND staff_group = 'GP'
  AND LOWER(TRIM({region_col})) = 'south east'
  AND icb_name IS NOT NULL
  AND TRIM(icb_name) != ''
GROUP BY icb_name
ORDER BY gp_headcount DESC NULLS LAST, icb_name
LIMIT 200
""".strip()
            log_info("node_hard_override | GP headcount by ICB in South East")
            return True
    return False


def apply_workforce_misc_lookup_overrides(
    state: MutableMapping[str, Any],
    orig_q: str,
    *,
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    log_info: Callable[[str], None],
) -> bool:
    if re.search(r"\bpractices?\b.*\bmore\s+fte\s+than\s+headcount\b", orig_q):
        latest = get_latest_year_month("practice_detailed")
        y, m = latest.get("year"), latest.get("month")
        if y and m:
            state["plan"] = {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "lookup",
                "notes": "Hard override: practices with GP FTE greater than GP headcount",
            }
            state["sql"] = f"""
SELECT
  prac_code, prac_name, pcn_name, sub_icb_name, icb_name,
  TRY_CAST(total_gp_hc AS DOUBLE) AS total_gp_hc,
  TRY_CAST(total_gp_fte AS DOUBLE) AS total_gp_fte,
  ROUND(
    TRY_CAST(total_gp_fte AS DOUBLE) -
    TRY_CAST(total_gp_hc AS DOUBLE),
    3
  ) AS fte_minus_headcount,
  '{y}' AS year, '{m}' AS month
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_name IS NOT NULL
  AND TRY_CAST(total_gp_hc AS DOUBLE) > 0
  AND TRY_CAST(total_gp_fte AS DOUBLE) > 0
  AND TRY_CAST(total_gp_fte AS DOUBLE) > TRY_CAST(total_gp_hc AS DOUBLE)
ORDER BY TRY_CAST(total_gp_fte AS DOUBLE) DESC NULLS LAST, prac_name
LIMIT 200
""".strip()
            log_info("node_hard_override | practices with FTE greater than headcount")
            return True
    return False


def apply_workforce_partner_salaried_trend(
    state: MutableMapping[str, Any],
    orig_q: str,
    hi: str,
    *,
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    log_info: Callable[[str], None],
) -> bool:
    if not (
        hi == "partner_salaried_trend"
        or (
            any(term in orig_q for term in ["salaried", "partner"])
            and any(term in orig_q for term in ["trend", "over the years", "over years", "over time"])
            and "gp" in orig_q
        )
    ):
        return False
    latest = get_latest_year_month("individual")
    latest_month = latest.get("month")
    if not latest_month:
        return False
    state["plan"] = {
        "in_scope": True,
        "table": "individual",
        "intent": "trend",
        "notes": "Hard override: salaried vs partner GP trend by year",
    }
    state["sql"] = f"""
SELECT
  year,
  COUNT(DISTINCT CASE
    WHEN staff_group = 'GP' AND (staff_role LIKE '%Partner%' OR staff_role LIKE '%Provider%')
    THEN unique_identifier END
  ) AS partner_hc,
  ROUND(SUM(CASE
    WHEN staff_group = 'GP' AND (staff_role LIKE '%Partner%' OR staff_role LIKE '%Provider%')
    THEN fte ELSE 0 END
  ), 1) AS partner_fte,
  COUNT(DISTINCT CASE
    WHEN staff_group = 'GP' AND staff_role LIKE '%Salaried%'
    THEN unique_identifier END
  ) AS salaried_hc,
  ROUND(SUM(CASE
    WHEN staff_group = 'GP' AND staff_role LIKE '%Salaried%'
    THEN fte ELSE 0 END
  ), 1) AS salaried_fte
FROM individual
WHERE month = '{latest_month}'
GROUP BY year
ORDER BY CAST(year AS INTEGER)
""".strip()
    log_info("node_hard_override | salaried vs partner GP trend")
    return True


def apply_workforce_benchmark_and_group_followups(
    state: MutableMapping[str, Any],
    orig_q: str,
    follow_ctx: Optional[dict[str, Any]],
    followup_intent: str,
    *,
    build_benchmark_followup: Callable[[MutableMapping[str, Any], str], Optional[dict[str, Any]]],
    parse_benchmark_request: Callable[[str], Optional[str]],
    build_geo_compare_followup_sql: Callable[[MutableMapping[str, Any], str], Optional[dict[str, Any]]],
    build_grouped_followup_sql: Callable[[MutableMapping[str, Any], str], Optional[dict[str, Any]]],
    build_group_extreme_followup_sql: Callable[[MutableMapping[str, Any], str], Optional[dict[str, Any]]],
    build_total_change_followup_sql: Callable[[MutableMapping[str, Any]], Optional[dict[str, Any]]],
    build_national_patients_per_gp_yoy_override: Callable[[MutableMapping[str, Any]], bool],
    log_info: Callable[[str], None],
) -> bool:
    if follow_ctx and followup_intent == "benchmark_probe":
        # Check if user specified a specific benchmark type (e.g. "PCN average")
        probe_type = parse_benchmark_request(orig_q) or "national_average"
        benchmark_result = build_benchmark_followup(state, probe_type)
        if benchmark_result:
            clarification_q = benchmark_result.get("clarification_question")
            if clarification_q:
                state["_needs_clarification"] = True
                state["_clarification_question"] = clarification_q
                state["plan"] = {
                    "in_scope": True,
                    "table": follow_ctx.get("table"),
                    "intent": "comparison",
                    "notes": "Hard override requested clarification for benchmark probe",
                }
                log_info(f"node_hard_override | benchmark probe clarification: {clarification_q[:120]}")
                return True
            state["plan"] = benchmark_result["plan"]
            state["sql"] = benchmark_result["sql"]
            log_info(f"node_hard_override | benchmark probe for {follow_ctx.get('entity_name')}")
            return True

    benchmark_request = parse_benchmark_request(orig_q)
    if follow_ctx and benchmark_request:
        benchmark_result = build_benchmark_followup(state, benchmark_request)
        if benchmark_result:
            clarification_q = benchmark_result.get("clarification_question")
            if clarification_q:
                state["_needs_clarification"] = True
                state["_clarification_question"] = clarification_q
                state["plan"] = {
                    "in_scope": True,
                    "table": follow_ctx.get("table"),
                    "intent": "comparison",
                    "notes": "Hard override requested clarification for benchmark comparison",
                }
                log_info(f"node_hard_override | benchmark clarification: {clarification_q[:120]}")
                return True
            state["plan"] = benchmark_result["plan"]
            state["sql"] = benchmark_result["sql"]
            log_info(f"node_hard_override | benchmark comparison for {follow_ctx.get('entity_name')}")
            return True

    if follow_ctx and follow_ctx.get("entity_name"):
        compare_geo_m = re.match(r"^compare\s+(?:with|to|against)\s+(.+?)[\?!.]?$", orig_q)
        if compare_geo_m:
            compare_sql = build_geo_compare_followup_sql(state, compare_geo_m.group(1))
            if compare_sql:
                state["plan"] = compare_sql["plan"]
                state["sql"] = compare_sql["sql"]
                log_info(f"node_hard_override | geo comparison follow-up with {compare_geo_m.group(1)}")
                return True

    if follow_ctx and not follow_ctx.get("entity_name"):
        ranking_group_m = re.match(
            r"^(?:which|what)\s+(region|icb|pcn)\s+has\s+(?:the\s+)?(?:most|highest|largest|fewest|lowest|least)\b.*$",
            orig_q,
        )
        if ranking_group_m:
            grouped = build_grouped_followup_sql(state, ranking_group_m.group(1).lower())
            if grouped:
                state["plan"] = grouped["plan"]
                state["sql"] = grouped["sql"]
                log_info(f"node_hard_override | ranking regroup follow-up by {ranking_group_m.group(1).lower()}")
                return True

        regroup_m = re.match(
            r"^(?:can\s+you\s+)?(?:break\s+(?:this|that|it)\s+down\s+by|split\s+(?:this|that|it)\s+by|"
            r"show\s+(?:this|that|it\s+)?by|now\s+by|by)\s+(region|icb|pcn)\s*\??$",
            orig_q,
        )
        if regroup_m:
            grouped = build_grouped_followup_sql(state, regroup_m.group(1).lower())
            if grouped:
                state["plan"] = grouped["plan"]
                state["sql"] = grouped["sql"]
                log_info(f"node_hard_override | regroup follow-up by {regroup_m.group(1).lower()}")
                return True

        extreme_followup_m = re.match(
            r"^(?:what\s+about|how\s+about|and)\s+(?:the\s+)?(most|highest|largest|fewest|lowest|least)\b.*$",
            orig_q,
        )
        if extreme_followup_m:
            grouped = build_group_extreme_followup_sql(state, extreme_followup_m.group(1).lower())
            if grouped:
                state["plan"] = grouped["plan"]
                state["sql"] = grouped["sql"]
                log_info(f"node_hard_override | grouped extreme follow-up={extreme_followup_m.group(1).lower()}")
                return True

        if re.match(r"^(?:has|how\s+has)\s+(?:the\s+)?(?:total|count|number|practice count|headcount|fte|it|that|this)\b.*\bchanged\b", orig_q):
            changed_sql = build_total_change_followup_sql(state)
            if changed_sql:
                state["plan"] = changed_sql["plan"]
                state["sql"] = changed_sql["sql"]
                log_info(f"node_hard_override | total-changed follow-up for previous subject={follow_ctx.get('previous_subject')}")
                return True

        if (
            str(follow_ctx.get("previous_metric") or "") == "patients_per_gp"
            and any(term in orig_q for term in ["changed", "change over", "last year", "past year", "over the last year"])
        ):
            return build_national_patients_per_gp_yoy_override(state)
    return False


def apply_workforce_large_practice_threshold(
    state: MutableMapping[str, Any],
    orig_q: str,
    *,
    extract_geo_scope_hint: Callable[[str], str],
    geo_filter_from_hint_text: Callable[[str, str, str], Optional[str]],
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    log_info: Callable[[str], None],
) -> bool:
    gp_threshold_m = re.search(r"\bpractices?\b.*\bmore than\s+(\d+)\s+gps?\b", orig_q)
    if not gp_threshold_m:
        return False
    threshold = gp_threshold_m.group(1)
    geo_hint = extract_geo_scope_hint(state.get("original_question", ""))
    geo_filter = geo_filter_from_hint_text(orig_q, geo_hint, table_hint="practice_detailed") if geo_hint else None
    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not (y and m):
        return False
    geo_clause = f"\n  AND {geo_filter}" if geo_filter else ""
    geo_note = f" within '{geo_hint}'" if geo_hint else ""
    state["plan"] = {
        "in_scope": True,
        "table": "practice_detailed",
        "intent": "lookup",
        "notes": f"Hard override: practices with more than {threshold} GPs{geo_note}",
    }
    state["sql"] = f"""
SELECT
  prac_code, prac_name, pcn_name, sub_icb_name, icb_name,
  TRY_CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE) AS gp_headcount,
  TRY_CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE) AS gp_fte
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_code IS NOT NULL
  AND TRY_CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE) > {threshold}{geo_clause}
ORDER BY TRY_CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE) DESC NULLS LAST, prac_name
LIMIT 200
""".strip()
    log_info(f"node_hard_override | large GP practices threshold={threshold} geo={geo_hint or 'national'}")
    return True


def apply_workforce_geo_scoped_simple_queries(
    state: MutableMapping[str, Any],
    orig_q: str,
    geo_hint: str,
    *,
    is_national_scope_hint: Callable[[str], bool],
    geo_filter_from_hint_text: Callable[[str, str, str], Optional[str]],
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    log_info: Callable[[str], None],
) -> bool:
    if not geo_hint:
        return False
    is_national_geo_hint = is_national_scope_hint(geo_hint)
    practice_geo_m = (
        "practice" in orig_q
        and any(token in orig_q for token in [" in ", " within ", " across "])
        and any(term in orig_q for term in ["how many", "number of", "count"])
        and not any(term in orig_q for term in ["which practice", "top practice", "top practices", "most", "highest", "lowest", "fewest"])
    )
    gp_geo_m = (
        not practice_geo_m
        and any(term in orig_q for term in [" gp ", " gps", "gps ", "general practitioner", "general practitioners"])
        and any(token in orig_q for token in [" in ", " within ", " across "])
        and any(term in orig_q for term in ["how many", "number of", "count", "total"])
        and not any(term in orig_q for term in ["which gp", "top gp", "most gp", "highest gp", "lowest gp", "fewest gp"])
        and not any(term in orig_q for term in ["percentage", "percent", "proportion", "full time", "full-time", "age distribution", "aged over", "patients per gp", "patients-per-gp", "ratio", " by icb", " by region"])
    )
    large_practices_m = re.search(r"\bwhich practices\b.*\bmore than\s+(\d+)\s+patients?\b", orig_q)

    if gp_geo_m:
        geo_filter = None if is_national_geo_hint else geo_filter_from_hint_text(orig_q, geo_hint, table_hint="individual")
        latest = get_latest_year_month("individual")
        y, m = latest.get("year"), latest.get("month")
        if y and m and (geo_filter or is_national_geo_hint):
            state["plan"] = {
                "in_scope": True,
                "table": "individual",
                "intent": "total",
                "notes": "Hard override: national GP count" if is_national_geo_hint else f"Hard override: GP count for geo scope '{geo_hint}'",
            }
            geo_clause = f"\n  AND {geo_filter}" if geo_filter else ""
            state["sql"] = f"""
SELECT COUNT(DISTINCT unique_identifier) AS gp_count, ROUND(SUM(fte), 1) AS gp_fte
FROM individual
WHERE year = '{y}' AND month = '{m}'
  AND staff_group = 'GP'{geo_clause}
LIMIT 200
""".strip()
            log_info(f"node_hard_override | geo GP count for {geo_hint}")
            return True

    if practice_geo_m:
        geo_filter = None if is_national_geo_hint else geo_filter_from_hint_text(orig_q, geo_hint, table_hint="practice_detailed")
        latest = get_latest_year_month("practice_detailed")
        y, m = latest.get("year"), latest.get("month")
        if y and m and (geo_filter or is_national_geo_hint):
            state["plan"] = {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "total",
                "notes": "Hard override: national practice count" if is_national_geo_hint else f"Hard override: practice count for geo scope '{geo_hint}'",
            }
            geo_clause = f"\n  AND {geo_filter}" if geo_filter else ""
            state["sql"] = f"""
SELECT COUNT(DISTINCT prac_code) AS practice_count
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_code IS NOT NULL{geo_clause}
LIMIT 200
""".strip()
            log_info(f"node_hard_override | geo practice count for {geo_hint}")
            return True

    if large_practices_m:
        threshold = large_practices_m.group(1)
        geo_filter = geo_filter_from_hint_text(orig_q, geo_hint, table_hint="practice_detailed")
        latest = get_latest_year_month("practice_detailed")
        y, m = latest.get("year"), latest.get("month")
        if geo_filter and y and m:
            state["plan"] = {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "lookup",
                "notes": f"Hard override: practices over patient threshold within '{geo_hint}'",
            }
            state["sql"] = f"""
SELECT
  prac_code, prac_name, pcn_name, sub_icb_name, icb_name, total_patients
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_code IS NOT NULL
  AND {geo_filter}
  AND CAST(NULLIF(total_patients, 'NA') AS DOUBLE) > {threshold}
ORDER BY CAST(NULLIF(total_patients, 'NA') AS DOUBLE) DESC NULLS LAST, prac_name
LIMIT 200
""".strip()
            log_info(f"node_hard_override | large practices threshold={threshold} geo={geo_hint}")
            return True
    return False


def apply_workforce_verbose_national_total(
    state: MutableMapping[str, Any],
    orig_q: str,
    *,
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    log_info: Callable[[str], None],
) -> bool:
    if not re.search(r"\b(?:total number of|how many|number of)\b.*\b(?:general practitioners?|gps?)\b.*\bengland\b", orig_q):
        return False
    latest = get_latest_year_month("individual")
    y, m = latest.get("year"), latest.get("month")
    if not (y and m):
        return False
    state["plan"] = {
        "in_scope": True,
        "table": "individual",
        "intent": "total",
        "notes": "Hard override: national GP total from verbose phrasing",
    }
    state["sql"] = f"""
SELECT COUNT(DISTINCT unique_identifier) AS gp_count, ROUND(SUM(fte), 1) AS gp_fte
FROM individual
WHERE year = '{y}' AND month = '{m}'
  AND staff_group = 'GP'
LIMIT 200
""".strip()
    log_info("node_hard_override | verbose national GP total")
    return True


def geo_filter_from_follow_context(
    entity_name: str,
    entity_type: str,
    follow_ctx: Optional[dict[str, Any]] = None,
    *,
    city_to_icb_for_hint: Callable[[str], str],
    region_column_for_table: Callable[[str], str],
) -> Optional[str]:
    entity_name = (entity_name or "").lower()
    follow_ctx = follow_ctx or {}
    if entity_type == "city":
        mapped_icb = follow_ctx.get("mapped_icb", city_to_icb_for_hint(entity_name))
        if mapped_icb:
            return f"(LOWER(TRIM(icb_name)) LIKE '%{mapped_icb}%' OR LOWER(TRIM(sub_icb_name)) LIKE '%{entity_name}%')"
        return f"(LOWER(TRIM(icb_name)) LIKE '%{entity_name}%' OR LOWER(TRIM(sub_icb_name)) LIKE '%{entity_name}%')"
    if entity_type == "icb":
        return f"LOWER(TRIM(icb_name)) LIKE '%{entity_name}%'"
    if entity_type == "sub_icb":
        return f"LOWER(TRIM(sub_icb_name)) LIKE '%{entity_name}%'"
    if entity_type == "region":
        return f"LOWER(TRIM({region_column_for_table('individual')})) LIKE '%{entity_name}%'"
    return None


def apply_workforce_geo_context_followups(
    state: MutableMapping[str, Any],
    orig_q: str,
    follow_ctx: Optional[dict[str, Any]],
    *,
    build_top_practices_followup_sql: Callable[[MutableMapping[str, Any]], Optional[dict[str, Any]]],
    staff_group_map: dict[str, str],
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    geo_filter_from_follow_context_fn: Callable[[str, str, Optional[dict[str, Any]]], Optional[str]],
    region_column_for_table: Callable[[str], str],
    log_info: Callable[[str], None],
) -> bool:
    if not (follow_ctx and follow_ctx.get("entity_name")):
        return False

    if (
        re.match(r"^(?:show\s+(?:me\s+)?|list\s+|what\s+are\s+)?(?:the\s+)?top\s+(?:\d+\s+)?practices?\b", orig_q)
        or ("practice" in orig_q and any(term in orig_q for term in ["most", "highest", "lowest", "largest", "biggest", "least", "fewest"]))
    ):
        top_practices = build_top_practices_followup_sql(state)
        if top_practices:
            state["plan"] = top_practices["plan"]
            state["sql"] = top_practices["sql"]
            log_info(f"node_hard_override | top practices follow-up for {follow_ctx.get('entity_name')}")
            return True

    staff_switch_m = re.match(r"^(?:what about|how about|and|now|show me|what about the)\s+(\w+)\s*\??$", orig_q)
    if staff_switch_m:
        staff_word = staff_switch_m.group(1).lower()
        staff_group = staff_group_map.get(staff_word)
        if staff_group:
            entity_name = follow_ctx["entity_name"].lower()
            entity_type = follow_ctx.get("entity_type", "")
            latest = get_latest_year_month("individual")
            y, m = latest.get("year"), latest.get("month")
            geo_filter = geo_filter_from_follow_context_fn(entity_name, entity_type, follow_ctx)
            if y and m and geo_filter:
                state["plan"] = {"in_scope": True, "table": "individual", "intent": "total",
                                 "notes": f"Hard override: {staff_group} count follow-up for '{entity_name}'"}
                state["sql"] = (
                    f"SELECT COUNT(DISTINCT unique_identifier) AS {staff_word}_count, "
                    f"ROUND(SUM(fte), 1) AS {staff_word}_fte\n"
                    f"FROM individual\n"
                    f"WHERE year = '{y}' AND month = '{m}'\n"
                    f"  AND staff_group = '{staff_group}'\n"
                    f"  AND {geo_filter}\n"
                    f"LIMIT 200"
                )
                log_info(f"node_hard_override | staff group switch: {staff_group} for {entity_name}")
                return True

    metric_correction = None
    if re.search(r"\bi\s+meant\s+fte\b|\bfte\s+(?:not|instead)\b|\bwant\s+fte\b|\bshow\s+fte\b|\buse\s+fte\b", orig_q):
        metric_correction = "fte"
    elif re.search(r"\bi\s+meant\s+headcount\b|\bheadcount\s+(?:not|instead)\b|\bwant\s+headcount\b|\bshow\s+headcount\b|\buse\s+headcount\b", orig_q):
        metric_correction = "headcount"
    if metric_correction:
        entity_name = follow_ctx["entity_name"].lower()
        entity_type = follow_ctx.get("entity_type", "")
        latest = get_latest_year_month("individual")
        y, m = latest.get("year"), latest.get("month")
        conv_hist = (state.get("conversation_history") or "").lower()
        enriched_q = (state.get("question") or "").lower()
        if "nurse" in conv_hist or "nurse" in enriched_q:
            sg_filter = "staff_group = 'Nurses'"
            sg_label = "nurse"
        else:
            sg_filter = "staff_group = 'GP'"
            sg_label = "gp"
        geo_filter = geo_filter_from_follow_context_fn(entity_name, entity_type, follow_ctx)
        if y and m and geo_filter:
            select_clause = f"ROUND(SUM(fte), 1) AS {sg_label}_fte" if metric_correction == "fte" else f"COUNT(DISTINCT unique_identifier) AS {sg_label}_count"
            state["plan"] = {"in_scope": True, "table": "individual", "intent": "total",
                             "notes": f"Hard override: metric correction to {metric_correction} for '{entity_name}'"}
            state["sql"] = (
                f"SELECT {select_clause}\n"
                f"FROM individual\n"
                f"WHERE year = '{y}' AND month = '{m}'\n"
                f"  AND {sg_filter}\n"
                f"  AND {geo_filter}\n"
                f"LIMIT 200"
            )
            log_info(f"node_hard_override | metric correction: {metric_correction} for {entity_name}")
            return True

    breakdown_m = re.match(
        r"^(?:can\s+you\s+)?(?:break\s+(?:this|that|it)\s+down\s+by|split\s+(?:this|that|it)\s+by|"
        r"show\s+(?:this|that|it\s+)?by|now\s+by|by)\s+([\w\s]+?)\s*\??$",
        orig_q,
    )
    if breakdown_m:
        dim = re.sub(r"\s+", " ", breakdown_m.group(1).lower()).strip()
        dim_col_map = {
            "gender": "gender", "sex": "gender", "age": "age_band",
            "region": region_column_for_table("individual"), "icb": "icb_name",
            "role": "detailed_staff_role", "staff role": "detailed_staff_role",
            "staff roles": "detailed_staff_role",
            "qualification": "country_qualification_group",
        }
        dim_col = dim_col_map.get(dim)
        if dim_col:
            entity_name = follow_ctx["entity_name"].lower()
            entity_type = follow_ctx.get("entity_type", "")
            latest = get_latest_year_month("individual")
            y, m = latest.get("year"), latest.get("month")
            prev_sg = follow_ctx.get("previous_staff_group", "")
            if prev_sg:
                sg_filter = f"staff_group = '{prev_sg}'"
                sg_label = prev_sg.lower().replace(" ", "_")
            else:
                conv_hist = (state.get("conversation_history") or "").lower()
                enriched_q = (state.get("question") or "").lower()
                if "nurse" in conv_hist or "nurse" in enriched_q:
                    sg_filter = "staff_group = 'Nurses'"
                    sg_label = "nurse"
                elif "admin" in conv_hist:
                    sg_filter = "staff_group = 'Admin'"
                    sg_label = "admin"
                elif "dpc" in conv_hist or "pharmacist" in conv_hist or "paramedic" in conv_hist:
                    sg_filter = "staff_group = 'DPC'"
                    sg_label = "dpc"
                else:
                    sg_filter = "staff_group = 'GP'"
                    sg_label = "gp"
            geo_filter = geo_filter_from_follow_context_fn(entity_name, entity_type, follow_ctx)
            if y and m and geo_filter:
                state["plan"] = {"in_scope": True, "table": "individual", "intent": "demographics",
                                 "notes": f"Hard override: {dim} breakdown for '{entity_name}'",
                                 "group_by": [dim_col]}
                state["sql"] = (
                    f"SELECT {dim_col}, COUNT(DISTINCT unique_identifier) AS {sg_label}_count, "
                    f"ROUND(SUM(fte), 1) AS {sg_label}_fte\n"
                    f"FROM individual\n"
                    f"WHERE year = '{y}' AND month = '{m}'\n"
                    f"  AND {sg_filter}\n"
                    f"  AND {geo_filter}\n"
                    f"GROUP BY {dim_col}\n"
                    f"ORDER BY {dim_col}\n"
                    f"LIMIT 200"
                )
                log_info(f"node_hard_override | breakdown by {dim} for {entity_name}")
                return True
    return False


def apply_workforce_clinical_staff_breakdown(
    state: MutableMapping[str, Any],
    effective_q: str,
    *,
    get_latest_year_month: Callable[[str], dict[str, str | None]],
    log_info: Callable[[str], None],
) -> bool:
    if not (
        "clinical staff" in effective_q
        and "fte" in effective_q
        and not any(term in effective_q for term in ["admin", "non-clinical", "gender", "age", "trend", "over time"])
    ):
        return False
    latest = get_latest_year_month("individual")
    y, m = latest.get("year"), latest.get("month")
    if not (y and m):
        return False
    state["plan"] = {
        "in_scope": True,
        "table": "individual",
        "intent": "breakdown",
        "notes": "Hard override: clinical staff FTE by staff group",
        "group_by": ["staff_group"],
    }
    state["sql"] = f"""
SELECT
  staff_group,
  ROUND(SUM(fte), 1) AS total_fte
FROM individual
WHERE year = '{y}' AND month = '{m}'
  AND staff_group IN ('GP', 'Nurses', 'DPC')
GROUP BY staff_group
ORDER BY total_fte DESC NULLS LAST, staff_group
LIMIT 200
""".strip()
    log_info("node_hard_override | clinical staff FTE breakdown")
    return True
