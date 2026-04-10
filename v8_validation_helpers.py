from __future__ import annotations

import re
from typing import Any, Callable, MutableMapping


def detect_sql_semantic_issues(
    state: MutableMapping[str, Any],
    *,
    dataset_config: Callable[[str], dict[str, Any]],
    clear_entity_matches: Callable[[dict[str, Any]], dict[str, str]],
) -> list[str]:
    sql = (state.get("sql") or "").strip()
    if not sql:
        return []

    sql_low = sql.lower()
    plan = state.get("plan", {}) or {}
    dataset = state.get("dataset", "workforce")
    original_q = (state.get("original_question") or state.get("question") or "").lower()
    issues: list[str] = []
    issue_checker = dataset_config(dataset).get("semantic_issue_checker")
    if callable(issue_checker):
        issues = issue_checker(state, issues)

    clear_matches = clear_entity_matches(state.get("resolved_entities", {}))
    entity_specs = {
        "prac_name_candidates": ("practice", [r"\bprac_name\b"]),
        "gp_name_candidates": ("practice", [r"\bgp_name\b"]),
        "icb_name_candidates": ("ICB", [r"(?<!sub_)\bicb_name\b"]),
        "sub_icb_name_candidates": ("sub-ICB", [r"\bsub_icb_name\b"]),
        "sub_icb_location_name_candidates": ("sub-ICB", [r"\bsub_icb_location_name\b"]),
        "region_candidates": ("region", [r"\bcomm_region_name\b", r"\bregion_name\b"]),
        "region_name_candidates": ("region", [r"\bregion_name\b"]),
        "pcn_name_candidates": ("PCN", [r"\bpcn_name\b"]),
    }
    for key, (label, patterns) in entity_specs.items():
        clear_value = clear_matches.get(key)
        if not clear_value:
            continue
        if any(re.search(pattern, sql_low, re.IGNORECASE) for pattern in patterns):
            if clear_value.lower() not in sql_low:
                issues.append(f"Use resolved {label} name '{clear_value}' in SQL filters.")

    if plan.get("intent") == "trend":
        if not re.search(r"\bgroup\s+by\b[\s\S]{0,80}\b(year|month)\b", sql_low, re.IGNORECASE):
            issues.append("Trend queries should group by year and/or month.")

    if "average" in original_q:
        if "avg(" not in sql_low and "benchmark_value" not in sql_low and "national_average" not in sql_low:
            issues.append("Average benchmark requested, but SQL does not compute an average.")

    compare_years = []
    time_range = state.get("time_range") or {}
    if isinstance(time_range, dict):
        compare_years = [str(y) for y in time_range.get("compare_years", [])]
    if plan.get("intent") == "comparison" or compare_years:
        if compare_years and not all(year in sql for year in compare_years):
            issues.append("Comparison query should reference both comparison years.")
        if not any(token in sql_low for token in ["case when", " with ", " join "]):
            issues.append("Comparison query should separate periods with CASE WHEN or separate subqueries/CTEs.")

    patients_ratio_requested = (
        "patients per gp" in original_q
        or "patients-per-gp" in original_q
        or ("patient" in original_q and "ratio" in original_q)
    )
    follow_ctx = state.get("follow_up_context") or {}
    if follow_ctx.get("previous_metric") == "patients_per_gp":
        patients_ratio_requested = True
    if patients_ratio_requested:
        if "total_patients" not in sql_low or "total_gp_fte" not in sql_low:
            issues.append("Patients-per-GP queries should use total_patients and total_gp_fte.")
        if re.search(r"total_gp_fte[\s\S]{0,220}/\s*nullif\([\s\S]{0,220}total_patients", sql_low, re.IGNORECASE):
            issues.append("Patients-per-GP ratio appears inverted; use patients divided by GP FTE.")

    if plan.get("table") == "practice_detailed" and plan.get("intent") in {"trend", "comparison"}:
        if "month" not in sql_low and "year" not in sql_low:
            issues.append("Time-based practice_detailed queries should reference year and/or month.")

    prev_grain = str(follow_ctx.get("previous_grain") or "")
    if "national average" in original_q and prev_grain.endswith("_total"):
        if "avg(" not in sql_low and "benchmark_value" not in sql_low and "national_average" not in sql_low:
            issues.append("Do not compare a local total with a national total and call it an average.")

    return issues
