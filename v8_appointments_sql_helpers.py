from __future__ import annotations

import re
from typing import Any, Callable, Dict, Optional


def build_appointments_geo_filter(
    question: str,
    hint: str,
    table: str,
    *,
    clean_entity_hint: Callable[[str], str],
    looks_like_time_only_hint: Callable[[str], bool],
    is_national_scope_hint: Callable[[str], bool],
    extract_practice_code: Callable[[str], str],
    sanitise_entity_input: Callable[[str, str], str],
    is_valid_region: Callable[[str], bool],
    normalise_region_name: Callable[[str], str],
    city_to_icb_for_hint: Callable[[str], str],
) -> Optional[str]:
    q = (question or "").lower()
    cleaned = clean_entity_hint(hint)
    if not cleaned:
        return None
    if looks_like_time_only_hint(cleaned):
        return None
    if is_national_scope_hint(cleaned):
        return None
    if table == "practice":
        code = extract_practice_code(cleaned)
        if code:
            return f"UPPER(TRIM(gp_code)) = '{code}'"
        safe = sanitise_entity_input(cleaned, "practice_name")
        return f"LOWER(TRIM(gp_name)) LIKE LOWER('%{safe}%')"

    cleaned_low = cleaned.lower()
    if is_valid_region(cleaned):
        region_name = sanitise_entity_input(normalise_region_name(cleaned), "region_name")
        return f"LOWER(TRIM(region_name)) = '{region_name.lower()}'"

    mapped_icb = city_to_icb_for_hint(cleaned_low)
    if mapped_icb:
        icb_safe = sanitise_entity_input(mapped_icb, "icb_name")
        city_safe = sanitise_entity_input(cleaned, "sub_icb_name")
        return (
            f"(LOWER(TRIM(icb_name)) LIKE LOWER('%{icb_safe}%') "
            f"OR LOWER(TRIM(sub_icb_location_name)) LIKE LOWER('%{city_safe}%'))"
        )

    if "icb" in q or cleaned_low.startswith("nhs ") or cleaned_low.endswith(" icb") or " icb" in cleaned_low:
        icb_core = re.sub(r"\bnhs\b", "", cleaned, flags=re.IGNORECASE)
        icb_core = re.sub(r"\bintegrated care board\b", "", icb_core, flags=re.IGNORECASE)
        icb_core = re.sub(r"\bicb\b", "", icb_core, flags=re.IGNORECASE).strip(" ,-")
        safe = sanitise_entity_input(icb_core or cleaned, "icb_name")
        return f"LOWER(TRIM(icb_name)) LIKE LOWER('%{safe}%')"

    safe = sanitise_entity_input(cleaned, "sub_icb_name")
    if "region" in q:
        return f"LOWER(TRIM(region_name)) LIKE LOWER('%{safe}%')"
    return (
        f"(LOWER(TRIM(sub_icb_location_name)) LIKE LOWER('%{safe}%') "
        f"OR LOWER(TRIM(icb_name)) LIKE LOWER('%{safe}%'))"
    )


def build_appointments_geo_hint_from_context(follow_ctx: Dict[str, Any]) -> str:
    entity_type = str(follow_ctx.get("entity_type") or "").strip().lower()
    entity_name = str(follow_ctx.get("entity_name") or "").strip()
    entity_code = str(follow_ctx.get("previous_entity_code") or "").strip().upper()
    if entity_type == "practice":
        return entity_code or entity_name
    if entity_type in {"icb", "sub_icb", "region", "pcn", "city"}:
        return entity_name
    return entity_name


def build_sql_appointments_total_latest(
    question: str,
    geo_hint: str,
    hcp_type: Optional[str],
    *,
    specific_entity_hint: Callable[[str, str], str],
    get_latest_year_month: Callable[..., Dict[str, str | None]],
    appointments_db: str,
    sanitise_entity_input: Callable[[str, str], str],
    extract_practice_code: Callable[[str], str],
    appointments_geo_filter: Callable[[str, str, str], Optional[str]],
) -> str:
    q = (question or "").lower()
    practice_hint = specific_entity_hint(question, "practice")
    latest = get_latest_year_month("practice", database=appointments_db)
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in appointments dataset.")
    hcp_sql = f"\n  AND hcp_type = '{sanitise_entity_input(hcp_type, 'hcp_type')}'" if hcp_type else ""

    if practice_hint or "practice" in q or extract_practice_code(question):
        geo_filter = appointments_geo_filter(question, practice_hint or geo_hint or question, "practice")
        where = f"\n  AND {geo_filter}" if geo_filter else ""
        return f"""
SELECT
  gp_code, gp_name,
  SUM(count_of_appointments) AS total_appointments,
  '{y}' AS year, '{m}' AS month
FROM practice
WHERE year = '{y}' AND month = '{m}'{where}{hcp_sql}
GROUP BY gp_code, gp_name
ORDER BY total_appointments DESC NULLS LAST, gp_name
LIMIT 20
""".strip()

    if geo_hint:
        geo_filter = appointments_geo_filter(question, geo_hint, "pcn_subicb")
        return f"""
SELECT
  SUM(count_of_appointments) AS total_appointments,
  '{y}' AS year, '{m}' AS month
FROM pcn_subicb
WHERE year = '{y}' AND month = '{m}'
  AND {geo_filter}{hcp_sql}
LIMIT 200
""".strip()

    return f"""
SELECT
  SUM(count_of_appointments) AS total_appointments,
  '{y}' AS year, '{m}' AS month
FROM practice
WHERE year = '{y}' AND month = '{m}'{hcp_sql}
LIMIT 200
""".strip()


def build_sql_appointments_trend(
    question: str,
    geo_hint: str,
    months_back: int,
    hcp_type: Optional[str],
    *,
    specific_entity_hint: Callable[[str, str], str],
    sanitise_entity_input: Callable[[str, str], str],
    extract_practice_code: Callable[[str], str],
    appointments_geo_filter: Callable[[str, str, str], Optional[str]],
) -> str:
    q = (question or "").lower()
    practice_hint = specific_entity_hint(question, "practice")
    hcp_sql = f"\n  AND hcp_type = '{sanitise_entity_input(hcp_type, 'hcp_type')}'" if hcp_type else ""
    if practice_hint or "practice" in q or extract_practice_code(question):
        geo_filter = appointments_geo_filter(question, practice_hint or geo_hint or question, "practice")
        where = f"\n  AND {geo_filter}" if geo_filter else ""
        return f"""
SELECT
  year, month,
  SUM(count_of_appointments) AS total_appointments
FROM practice
WHERE 1 = 1{where}{hcp_sql}
GROUP BY year, month
ORDER BY CAST(year AS INTEGER) DESC, CAST(month AS INTEGER) DESC
LIMIT {int(months_back)}
""".strip()

    if geo_hint:
        geo_filter = appointments_geo_filter(question, geo_hint, "pcn_subicb")
        return f"""
SELECT
  year, month,
  SUM(count_of_appointments) AS total_appointments
FROM pcn_subicb
WHERE {geo_filter}{hcp_sql}
GROUP BY year, month
ORDER BY CAST(year AS INTEGER) DESC, CAST(month AS INTEGER) DESC
LIMIT {int(months_back)}
""".strip()

    return f"""
SELECT
  year, month,
  SUM(count_of_appointments) AS total_appointments
FROM practice
WHERE 1 = 1{hcp_sql}
GROUP BY year, month
ORDER BY CAST(year AS INTEGER) DESC, CAST(month AS INTEGER) DESC
LIMIT {int(months_back)}
""".strip()


def build_sql_appointments_mode_breakdown(
    question: str,
    geo_hint: str,
    hcp_type: Optional[str],
    *,
    specific_entity_hint: Callable[[str, str], str],
    get_latest_year_month: Callable[..., Dict[str, str | None]],
    appointments_db: str,
    sanitise_entity_input: Callable[[str, str], str],
    appointments_geo_filter: Callable[[str, str, str], Optional[str]],
) -> str:
    latest = get_latest_year_month("practice", database=appointments_db)
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in appointments dataset.")
    practice_hint = specific_entity_hint(question, "practice")
    hcp_sql = f"\n  AND hcp_type = '{sanitise_entity_input(hcp_type, 'hcp_type')}'" if hcp_type else ""

    if practice_hint:
        geo_filter = appointments_geo_filter(question, practice_hint, "practice")
        return f"""
SELECT
  appt_mode,
  SUM(count_of_appointments) AS total_appointments
FROM practice
WHERE year = '{y}' AND month = '{m}'
  AND {geo_filter}{hcp_sql}
GROUP BY appt_mode
ORDER BY total_appointments DESC NULLS LAST, appt_mode
LIMIT 50
""".strip()

    if geo_hint:
        geo_filter = appointments_geo_filter(question, geo_hint, "pcn_subicb")
        return f"""
SELECT
  appt_mode,
  SUM(count_of_appointments) AS total_appointments
FROM pcn_subicb
WHERE year = '{y}' AND month = '{m}'
  AND {geo_filter}{hcp_sql}
GROUP BY appt_mode
ORDER BY total_appointments DESC NULLS LAST, appt_mode
LIMIT 50
""".strip()

    return f"""
SELECT
  appt_mode,
  SUM(count_of_appointments) AS total_appointments
FROM practice
WHERE year = '{y}' AND month = '{m}'{hcp_sql}
GROUP BY appt_mode
ORDER BY total_appointments DESC NULLS LAST, appt_mode
LIMIT 50
""".strip()


def build_sql_appointments_hcp_breakdown(
    question: str,
    geo_hint: str,
    *,
    specific_entity_hint: Callable[[str, str], str],
    get_latest_year_month: Callable[..., Dict[str, str | None]],
    appointments_db: str,
    extract_practice_code: Callable[[str], str],
    appointments_geo_filter: Callable[[str, str, str], Optional[str]],
) -> str:
    q = (question or "").lower()
    practice_hint = specific_entity_hint(question, "practice")
    latest = get_latest_year_month("practice", database=appointments_db)
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in appointments dataset.")

    if practice_hint or "practice" in q or extract_practice_code(question):
        geo_filter = appointments_geo_filter(question, practice_hint or geo_hint or question, "practice")
        where = f"\n  AND {geo_filter}" if geo_filter else ""
        return f"""
SELECT
  hcp_type,
  SUM(count_of_appointments) AS total_appointments
FROM practice
WHERE year = '{y}' AND month = '{m}'{where}
GROUP BY hcp_type
ORDER BY total_appointments DESC NULLS LAST, hcp_type
LIMIT 50
""".strip()

    if geo_hint:
        geo_filter = appointments_geo_filter(question, geo_hint, "pcn_subicb")
        return f"""
SELECT
  hcp_type,
  SUM(count_of_appointments) AS total_appointments
FROM pcn_subicb
WHERE year = '{y}' AND month = '{m}'
  AND {geo_filter}
GROUP BY hcp_type
ORDER BY total_appointments DESC NULLS LAST, hcp_type
LIMIT 50
""".strip()

    return f"""
SELECT
  hcp_type,
  SUM(count_of_appointments) AS total_appointments
FROM practice
WHERE year = '{y}' AND month = '{m}'
GROUP BY hcp_type
ORDER BY total_appointments DESC NULLS LAST, hcp_type
LIMIT 50
""".strip()


def build_sql_appointments_dna_rate(
    question: str,
    geo_hint: str,
    hcp_type: Optional[str],
    *,
    get_latest_year_month: Callable[..., Dict[str, str | None]],
    appointments_db: str,
    specific_entity_hint: Callable[[str, str], str],
    sanitise_entity_input: Callable[[str, str], str],
    appointments_geo_filter: Callable[[str, str, str], Optional[str]],
) -> str:
    latest = get_latest_year_month("practice", database=appointments_db)
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in appointments dataset.")
    practice_hint = specific_entity_hint(question, "practice")
    hcp_sql = f"\n  AND hcp_type = '{sanitise_entity_input(hcp_type, 'hcp_type')}'" if hcp_type else ""

    if practice_hint:
        geo_filter = appointments_geo_filter(question, practice_hint, "practice")
        return f"""
SELECT
  ROUND(
    100.0 * SUM(CASE WHEN appt_status = 'DNA' THEN count_of_appointments ELSE 0 END) /
    NULLIF(SUM(count_of_appointments), 0), 1
  ) AS dna_rate_pct,
  SUM(CASE WHEN appt_status = 'DNA' THEN count_of_appointments ELSE 0 END) AS dna_appointments,
  SUM(count_of_appointments) AS total_appointments
FROM practice
WHERE year = '{y}' AND month = '{m}'
  AND {geo_filter}{hcp_sql}
LIMIT 200
""".strip()

    if geo_hint:
        geo_filter = appointments_geo_filter(question, geo_hint, "pcn_subicb")
        return f"""
SELECT
  ROUND(
    100.0 * SUM(CASE WHEN appt_status = 'DNA' THEN count_of_appointments ELSE 0 END) /
    NULLIF(SUM(count_of_appointments), 0), 1
  ) AS dna_rate_pct,
  SUM(CASE WHEN appt_status = 'DNA' THEN count_of_appointments ELSE 0 END) AS dna_appointments,
  SUM(count_of_appointments) AS total_appointments
FROM pcn_subicb
WHERE year = '{y}' AND month = '{m}'
  AND {geo_filter}{hcp_sql}
LIMIT 200
""".strip()

    return f"""
SELECT
  ROUND(
    100.0 * SUM(CASE WHEN appt_status = 'DNA' THEN count_of_appointments ELSE 0 END) /
    NULLIF(SUM(count_of_appointments), 0), 1
  ) AS dna_rate_pct,
  SUM(CASE WHEN appt_status = 'DNA' THEN count_of_appointments ELSE 0 END) AS dna_appointments,
  SUM(count_of_appointments) AS total_appointments
FROM practice
WHERE year = '{y}' AND month = '{m}'{hcp_sql}
LIMIT 200
""".strip()


def build_sql_appointments_top_practices(
    limit: int,
    *,
    get_latest_year_month: Callable[..., Dict[str, str | None]],
    appointments_db: str,
) -> str:
    latest = get_latest_year_month("practice", database=appointments_db)
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in appointments dataset.")
    return f"""
SELECT
  gp_code, gp_name, SUM(count_of_appointments) AS total_appointments,
  '{y}' AS year, '{m}' AS month
FROM practice
WHERE year = '{y}' AND month = '{m}'
GROUP BY gp_code, gp_name
ORDER BY total_appointments DESC NULLS LAST, gp_name
LIMIT {int(limit)}
""".strip()
