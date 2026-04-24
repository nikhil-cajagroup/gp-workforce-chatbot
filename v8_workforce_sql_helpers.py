from __future__ import annotations

from typing import Callable, Dict


def build_practice_lookup_filter(
    practice_like: str,
    extract_practice_code: Callable[[str], str],
    sanitise_entity_input: Callable[[str, str], str],
) -> str:
    code = extract_practice_code(practice_like)
    if code:
        return f"UPPER(TRIM(prac_code)) = '{code}'"
    raw = str(practice_like or "").strip()
    variants: list[str] = []

    def _add(candidate: str) -> None:
        candidate = str(candidate or "").strip()
        if candidate and candidate.lower() not in {v.lower() for v in variants}:
            variants.append(candidate)

    _add(raw)
    tail = __import__("re").sub(r"^.*\b(?:in|at|for)\s+(.+)$", r"\1", raw, flags=__import__("re").IGNORECASE).strip()
    _add(tail)
    stripped = __import__("re").sub(
        r"\b(?:practice|surgery|medical centre|health centre|clinic)\b$",
        "",
        tail or raw,
        flags=__import__("re").IGNORECASE,
    ).strip()
    _add(stripped)
    if stripped:
        _add(f"{stripped} Practice")

    filters = [
        f"LOWER(TRIM(prac_name)) LIKE LOWER('%{sanitise_entity_input(variant, 'practice_name')}%')"
        for variant in variants
    ]
    return f"({' OR '.join(filters)})"


def build_sql_practice_gp_count_latest(
    practice_like: str,
    get_latest_year_month: Callable[[str], Dict[str, str | None]],
    extract_practice_code: Callable[[str], str],
    sanitise_entity_input: Callable[[str, str], str],
) -> str:
    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in practice_detailed table.")
    where_filter = build_practice_lookup_filter(practice_like, extract_practice_code, sanitise_entity_input)
    return f"""
SELECT
  prac_code, prac_name, pcn_name, sub_icb_name, icb_name,
  total_gp_hc, total_gp_fte,
  total_gp_extgl_hc AS gp_excl_trainees_locums_hc,
  total_gp_extgl_fte AS gp_excl_trainees_locums_fte,
  '{y}' AS year, '{m}' AS month
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_name IS NOT NULL
  AND {where_filter}
ORDER BY total_gp_hc DESC NULLS LAST
LIMIT 10
""".strip()


def build_sql_practice_to_icb_latest(
    practice_like: str,
    get_latest_year_month: Callable[[str], Dict[str, str | None]],
    extract_practice_code: Callable[[str], str],
    sanitise_entity_input: Callable[[str, str], str],
) -> str:
    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in practice_detailed table.")
    where_filter = build_practice_lookup_filter(practice_like, extract_practice_code, sanitise_entity_input)
    return f"""
SELECT
  prac_code, prac_name, pcn_name,
  sub_icb_code, sub_icb_name,
  icb_code, icb_name,
  region_code, region_name,
  '{y}' AS year, '{m}' AS month
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_name IS NOT NULL
  AND {where_filter}
ORDER BY prac_name
LIMIT 10
""".strip()


def build_sql_practice_patient_count(
    practice_like: str,
    get_latest_year_month: Callable[[str], Dict[str, str | None]],
    extract_practice_code: Callable[[str], str],
    sanitise_entity_input: Callable[[str, str], str],
) -> str:
    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in practice_detailed table.")
    where_filter = build_practice_lookup_filter(practice_like, extract_practice_code, sanitise_entity_input)
    return f"""
SELECT
  prac_code, prac_name, icb_name,
  total_patients, total_male, total_female,
  '{y}' AS year, '{m}' AS month
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_name IS NOT NULL
  AND {where_filter}
ORDER BY total_patients DESC NULLS LAST
LIMIT 10
""".strip()


def build_sql_patients_per_gp(
    practice_like: str,
    get_latest_year_month: Callable[[str], Dict[str, str | None]],
    extract_practice_code: Callable[[str], str],
    sanitise_entity_input: Callable[[str, str], str],
) -> str:
    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in practice_detailed table.")
    where_filter = build_practice_lookup_filter(practice_like, extract_practice_code, sanitise_entity_input)
    return f"""
SELECT
  prac_code, prac_name, icb_name,
  total_patients, total_gp_extgl_fte,
  CASE WHEN TRY_CAST(NULLIF(total_gp_extgl_fte, 'NA') AS DOUBLE) > 0
    THEN ROUND(
      TRY_CAST(NULLIF(total_patients, 'NA') AS DOUBLE) /
      NULLIF(TRY_CAST(NULLIF(total_gp_extgl_fte, 'NA') AS DOUBLE), 0), 1
    )
    ELSE NULL
  END AS patients_per_gp_fte,
  '{y}' AS year, '{m}' AS month
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_name IS NOT NULL
  AND {where_filter}
ORDER BY patients_per_gp_fte DESC NULLS LAST
LIMIT 10
""".strip()


def build_sql_practice_staff_breakdown(
    practice_like: str,
    get_latest_year_month: Callable[[str], Dict[str, str | None]],
    extract_practice_code: Callable[[str], str],
    sanitise_entity_input: Callable[[str, str], str],
) -> str:
    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in practice_detailed table.")
    where_filter = build_practice_lookup_filter(practice_like, extract_practice_code, sanitise_entity_input)
    return f"""
SELECT
  prac_code, prac_name, icb_name,
  total_gp_hc, total_gp_fte,
  total_nurses_hc, total_nurses_fte,
  total_dpc_hc, total_dpc_fte,
  total_admin_hc, total_admin_fte,
  total_patients,
  '{y}' AS year, '{m}' AS month
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_name IS NOT NULL
  AND {where_filter}
ORDER BY prac_name
LIMIT 10
""".strip()


def build_sql_pcn_gp_count(
    get_latest_year_month: Callable[[str], Dict[str, str | None]],
) -> str:
    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in practice_detailed table.")
    return f"""
SELECT
  pcn_name,
  SUM(CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE)) AS gp_headcount,
  SUM(CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE)) AS gp_fte,
  COUNT(DISTINCT prac_code) AS practice_count,
  '{y}' AS year, '{m}' AS month
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND pcn_name IS NOT NULL
  AND TRIM(pcn_name) != ''
GROUP BY pcn_name
ORDER BY gp_headcount DESC NULLS LAST
LIMIT 200
""".strip()
