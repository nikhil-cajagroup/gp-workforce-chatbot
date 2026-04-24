"""
Appointments follow-up regression tests for v8.

Covers:
- practice-level continuity across follow-ups
- ICB-level continuity across regrouping follow-ups
- view continuity for appointment-mode breakdowns
"""
import time
import uuid

from test_http_harness import chat_json, exit_for_results

BASE_URL = "http://localhost:8000"
RESULTS = []


def chat(question: str, session_id: str | None = None) -> dict:
    sid = session_id or str(uuid.uuid4())
    return chat_json(question, sid, timeout=120)


def check(name: str, result: dict, checks: list[tuple[str, str]]):
    failures = []
    if result["status"] != 200:
        failures.append(f"HTTP {result['status']}: {result.get('error', '')[:200]}")
    answer = (result.get("answer") or "").lower()
    sql = (result.get("sql") or "").lower()
    rows = int(((result.get("meta") or {}).get("rows_returned")) or 0)
    semantic = (result.get("meta") or {}).get("semantic_state") or {}

    for kind, value in checks:
        if kind == "rows_or_sql" and not (rows > 0 or sql.strip()):
            failures.append("Expected rows > 0 or SQL")
        elif kind == "answer_contains" and value.lower() not in answer:
            failures.append(f"Answer missing '{value}'")
        elif kind == "sql_contains" and value.lower() not in sql:
            failures.append(f"SQL missing '{value}'")
        elif kind == "sql_contains_any":
            options = [v.strip().lower() for v in value.split("|") if v.strip()]
            if not any(opt in sql for opt in options):
                failures.append(f"SQL missing any of '{value}'")
        elif kind == "semantic_metric" and str(semantic.get("metric", "")).lower() != value.lower():
            failures.append(f"Semantic metric mismatch: expected '{value}', got '{semantic.get('metric', '')}'")
        elif kind == "semantic_metric_any":
            options = [v.strip().lower() for v in value.split("|") if v.strip()]
            actual = str(semantic.get("metric", "")).lower()
            if actual not in options:
                failures.append(f"Semantic metric mismatch: expected one of '{value}', got '{actual}'")
        elif kind == "semantic_view" and str(semantic.get("view", "")).lower() != value.lower():
            failures.append(f"Semantic view mismatch: expected '{value}', got '{semantic.get('view', '')}'")
        elif kind == "semantic_entity_type" and str(semantic.get("entity_type", "")).lower() != value.lower():
            failures.append(f"Semantic entity_type mismatch: expected '{value}', got '{semantic.get('entity_type', '')}'")

    status = "PASS" if not failures else "FAIL"
    RESULTS.append((status, name, result["question"], failures, result["elapsed"]))
    print(f"[{status}] {name} ({result['elapsed']:.1f}s)")
    for failure in failures:
        print(f"  - {failure}")
    if failures:
        print(f"  answer: {(result.get('answer') or '')[:240]}")
        print(f"  sql:    {(result.get('sql') or '')[:240]}")
        print(f"  semantic: {semantic}")


if __name__ == "__main__":
    sid = str(uuid.uuid4())
    r = chat("Show appointments for Queens Park Medical Centre", sid)
    check("F1 practice open", r, [
        ("rows_or_sql", ""),
        # v9 resolves named practices to codes, so SQL may use gp_code instead of gp_name
        ("sql_contains_any", "gp_name|gp_code"),
        ("semantic_entity_type", "practice"),
        # v9 uses 'total_appointments'; v8 used 'appointments_total'
        ("semantic_metric_any", "appointments_total|total_appointments"),
    ])
    r = chat("What about DNA rate?", sid)
    check("F2 practice DNA follow-up", r, [
        ("rows_or_sql", ""),
        ("sql_contains_any", "gp_name|gp_code"),
        ("sql_contains", "appt_status = 'dna'"),
        ("semantic_metric", "dna_rate"),
        ("semantic_entity_type", "practice"),
    ])

    sid = str(uuid.uuid4())
    r = chat("How many appointments were there in NHS Greater Manchester ICB?", sid)
    check("F3 ICB open", r, [
        ("rows_or_sql", ""),
        ("sql_contains", "icb_name"),
        ("semantic_entity_type", "icb"),
        # v9 uses 'total_appointments'; v8 used 'appointments_total'
        ("semantic_metric_any", "appointments_total|total_appointments"),
    ])
    r = chat("Break that down by HCP type", sid)
    check("F4 ICB HCP follow-up", r, [
        ("rows_or_sql", ""),
        ("sql_contains", "hcp_type"),
        ("sql_contains", "icb_name"),
        ("semantic_view", "hcp_type_breakdown"),
        ("semantic_entity_type", "icb"),
    ])

    sid = str(uuid.uuid4())
    r = chat("Show appointment mode breakdown nationally", sid)
    check("F5 mode open", r, [
        ("rows_or_sql", ""),
        ("sql_contains", "appt_mode"),
        ("semantic_view", "appointment_mode_breakdown"),
    ])
    r = chat("What about in London region?", sid)
    check("F6 mode geo follow-up", r, [
        ("rows_or_sql", ""),
        ("sql_contains", "appt_mode"),
        ("sql_contains", "region_name"),
        ("answer_contains", "london"),
        ("semantic_view", "appointment_mode_breakdown"),
        ("semantic_entity_type", "region"),
    ])

    passed = sum(1 for status, *_ in RESULTS if status == "PASS")
    total = len(RESULTS)
    print(f"\nSummary: {passed}/{total} passed")
    exit_for_results(RESULTS)
