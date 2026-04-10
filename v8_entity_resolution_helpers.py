from __future__ import annotations

from typing import Any, Callable, Optional


def resolve_entities_via_config(
    dataset: str,
    question: str,
    plan: dict[str, Any],
    config: dict[str, Any],
    *,
    specific_entity_hint: Callable[[str, str], str],
    looks_like_specific_icb_hint: Callable[[str, str], bool],
    region_column_for_table: Callable[[str], str],
    get_latest_year_month: Callable[[str, str], dict[str, str | None]],
    resolve_entity_fuzzy: Callable[[str, str, str, Optional[str], Optional[str]], list[Any]],
) -> dict[str, Any]:
    specs = list(config.get("entity_resolution_specs") or [])
    if not specs:
        return {}

    q_lower = (question or "").lower()
    entities_to_resolve = set(plan.get("entities_to_resolve", []) or [])
    resolved: dict[str, Any] = {}
    hint_cache: dict[str, str] = {}
    latest_cache: dict[str, tuple[Optional[str], Optional[str]]] = {}
    athena_db = str(config.get("athena_database") or "")

    for spec in specs:
        entity_type = str(spec.get("entity_type") or "").strip()
        if not entity_type:
            continue

        hint = hint_cache.get(entity_type)
        if hint is None:
            hint = specific_entity_hint(question, entity_type)
            hint_cache[entity_type] = hint
        if not hint:
            continue

        if spec.get("skip_if_specific_icb_hint") and looks_like_specific_icb_hint(question, hint):
            continue

        trigger_keywords = list(spec.get("trigger_keywords") or [])
        plan_keys = set(spec.get("plan_keys") or [])
        should_attempt = bool(spec.get("always_if_hint"))
        if plan_keys & entities_to_resolve:
            should_attempt = True
        if any(kw in q_lower for kw in trigger_keywords):
            should_attempt = True
        if spec.get("accept_specific_icb_hint") and looks_like_specific_icb_hint(question, hint):
            should_attempt = True
        if not should_attempt:
            continue

        table = str(spec.get("table") or "").strip()
        column = str(spec.get("column") or "").strip()
        if not table or not column:
            continue
        if column == "__region_column__":
            column = region_column_for_table(str(spec.get("region_table_hint") or table))

        latest_table = str(spec.get("latest_table") or table)
        if latest_table not in latest_cache:
            latest = get_latest_year_month(latest_table, database=athena_db)
            latest_cache[latest_table] = (latest.get("year"), latest.get("month"))
        y, m = latest_cache[latest_table]

        result_key = str(spec.get("result_key") or f"{column}_candidates")
        try:
            resolved[result_key] = resolve_entity_fuzzy(table, column, hint, y, m)
        except Exception:
            resolved[result_key] = []

    return resolved
