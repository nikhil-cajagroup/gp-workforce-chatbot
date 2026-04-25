from __future__ import annotations

import re

from typing import Callable, Dict, List


_PRACTICE_SUFFIX_RE = re.compile(
    r"\b(?:practice|surgery|medical\s+(?:centre|center|ctr\.?)|health\s+(?:centre|center|ctr\.?)|clinic)\b$",
    flags=re.IGNORECASE,
)

_GENERIC_PRACTICE_TOKENS = {
    "the",
    "practice",
    "surgery",
    "medical",
    "centre",
    "center",
    "ctr",
    "health",
    "clinic",
}


def _clean_practice_hint(candidate: str) -> str:
    candidate = str(candidate or "").strip()
    candidate = re.sub(r"^[\s,;:()'\"-]+|[\s,;:()'\"-]+$", "", candidate)
    candidate = re.sub(r"\s+", " ", candidate)
    return candidate.strip()


def _practice_hint_has_specific_token(candidate: str) -> bool:
    tokens = re.findall(r"[a-z0-9]+", str(candidate or "").lower())
    return any(token not in _GENERIC_PRACTICE_TOKENS and len(token) >= 4 for token in tokens)


def practice_hint_variants(raw_hint: str) -> List[str]:
    """Generate safe practice-name search variants for NHS naming aliases.

    NHS practice names often abbreviate common words in the source data, e.g.
    "Medical Centre" appears as "MEDICAL CTR".  The lookup SQL and the Python
    resolver should use the same variants so they do not disagree.
    """
    hint = _clean_practice_hint(raw_hint)
    if not hint:
        return []
    variants: list[str] = []

    def _add(candidate: str, *, allow_generic: bool = False) -> None:
        candidate = _clean_practice_hint(candidate)
        if not candidate:
            return
        if not allow_generic and not _practice_hint_has_specific_token(candidate):
            return
        if candidate.lower() not in {v.lower() for v in variants}:
            variants.append(candidate)

    _add(hint, allow_generic=True)

    tail = re.sub(r"^.*\b(?:in|at|for)\s+(.+)$", r"\1", hint, flags=re.IGNORECASE).strip()
    if tail and tail.lower() != hint.lower():
        _add(tail, allow_generic=True)

    current = tail or hint
    stripped_values: list[str] = []
    while True:
        stripped = _PRACTICE_SUFFIX_RE.sub("", current).strip()
        if not stripped or stripped.lower() == current.lower():
            break
        stripped_values.append(stripped)
        current = stripped

    for stripped in stripped_values:
        _add(stripped)
        _add(f"{stripped} Practice")

    if not _PRACTICE_SUFFIX_RE.search(hint):
        _add(f"{hint} Practice")

    for base in list(variants):
        centre_to_ctr = re.sub(r"\b(?:centre|center)\b", "CTR", base, flags=re.IGNORECASE)
        ctr_to_centre = re.sub(r"\bctr\.?\b", "Centre", base, flags=re.IGNORECASE)
        _add(centre_to_ctr)
        _add(ctr_to_centre)
        if not re.match(r"^the\s+", base, flags=re.IGNORECASE):
            _add(f"The {base}")

    return variants


def build_practice_lookup_filter(
    practice_like: str,
    extract_practice_code: Callable[[str], str],
    sanitise_entity_input: Callable[[str, str], str],
) -> str:
    code = extract_practice_code(practice_like)
    if code:
        return f"UPPER(TRIM(prac_code)) = '{code}'"
    variants = practice_hint_variants(practice_like)
    if not variants:
        return "1 = 0"

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
