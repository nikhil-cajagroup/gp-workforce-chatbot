"""
Authored supervisor-level regression tests for v8.

Covers:
- workforce-only chains
- appointments-only chains
- mixed-session dataset switching
- follow-up continuity across dataset boundaries
"""
import time
import uuid
import requests

BASE_URL = "http://localhost:8000"
RESULTS = []


def chat(question: str, session_id: str | None = None) -> dict:
    sid = session_id or str(uuid.uuid4())
    t0 = time.time()
    r = requests.post(
        f"{BASE_URL}/chat",
        json={"session_id": sid, "question": question},
        timeout=120,
    )
    elapsed = time.time() - t0
    payload = {"session_id": sid, "status": r.status_code, "elapsed": elapsed, "question": question}
    if r.status_code == 200:
        payload.update(r.json())
    else:
        payload["error"] = r.text[:500]
    return payload


def check(name: str, result: dict, checks: list[tuple[str, str]]):
    failures = []
    if result["status"] != 200:
        failures.append(f"HTTP {result['status']}: {result.get('error', '')[:200]}")

    answer = (result.get("answer") or "").lower()
    sql = (result.get("sql") or "").lower()
    meta = result.get("meta") or {}
    rows = int(meta.get("rows_returned") or 0)
    semantic = meta.get("semantic_state") or {}

    for kind, value in checks:
        if kind == "rows_or_sql" and not (rows > 0 or sql.strip()):
            failures.append("Expected rows > 0 or non-empty SQL")
        elif kind == "answer_contains" and value.lower() not in answer:
            failures.append(f"Answer missing '{value}'")
        elif kind == "answer_not_contains" and value.lower() in answer:
            failures.append(f"Answer should not contain '{value}'")
        elif kind == "sql_contains" and value.lower() not in sql:
            failures.append(f"SQL missing '{value}'")
        elif kind == "sql_not_contains" and value.lower() in sql:
            failures.append(f"SQL should not contain '{value}'")
        elif kind == "no_sql" and sql.strip():
            failures.append(f"Expected no SQL, got '{sql[:120]}'")
        elif kind == "semantic_dataset" and str(semantic.get("dataset", "")).lower() != value.lower():
            failures.append(f"Semantic dataset mismatch: expected '{value}', got '{semantic.get('dataset', '')}'")
        elif kind == "semantic_entity_type" and str(semantic.get("entity_type", "")).lower() != value.lower():
            failures.append(f"Semantic entity_type mismatch: expected '{value}', got '{semantic.get('entity_type', '')}'")
        elif kind == "semantic_metric" and str(semantic.get("metric", "")).lower() != value.lower():
            failures.append(f"Semantic metric mismatch: expected '{value}', got '{semantic.get('metric', '')}'")
        elif kind == "semantic_view" and str(semantic.get("view", "")).lower() != value.lower():
            failures.append(f"Semantic view mismatch: expected '{value}', got '{semantic.get('view', '')}'")

    status = "PASS" if not failures else "FAIL"
    RESULTS.append((status, name, result["question"], failures, result["elapsed"]))
    print(f"[{status}] {name} ({result['elapsed']:.1f}s)")
    for failure in failures:
        print(f"  - {failure}")
    if failures:
        print(f"  answer: {(result.get('answer') or '')[:260]}")
        print(f"  sql:    {(result.get('sql') or '')[:260]}")
        print(f"  semantic: {semantic}")


if __name__ == "__main__":
    print("\n=== Workforce Chain ===")
    sid = str(uuid.uuid4())
    r = chat("How many GPs are in Leeds?", sid)
    check("W1 Leeds GP count", r, [
        ("rows_or_sql", ""),
        ("answer_contains", "leeds"),
        ("answer_contains", "gps"),
    ])

    r = chat("What about nurses?", sid)
    check("W2 Leeds nurses follow-up", r, [
        ("rows_or_sql", ""),
        ("answer_contains", "leeds"),
        ("answer_contains", "nurse"),
    ])

    r = chat("Break this down by gender", sid)
    check("W3 Leeds nurses by gender", r, [
        ("rows_or_sql", ""),
        ("sql_contains", "gender"),
        ("answer_contains", "female"),
    ])

    r = chat("What is the patients-per-GP ratio nationally?", sid)
    check("W4 workforce topic switch", r, [
        ("rows_or_sql", ""),
        ("answer_contains", "patients-per-gp"),
        ("sql_contains", "from practice_detailed"),
    ])

    print("\n=== Appointments Chain ===")
    sid = str(uuid.uuid4())
    r = chat("How many appointments were there in NHS Greater Manchester ICB?", sid)
    check("A1 ICB appointments total", r, [
        ("rows_or_sql", ""),
        ("answer_contains", "greater manchester"),
        ("sql_contains", "icb_name"),
        ("semantic_dataset", "appointments"),
    ])

    r = chat("What about DNA rate?", sid)
    check("A2 ICB DNA follow-up", r, [
        ("rows_or_sql", ""),
        ("answer_contains", "greater manchester"),
        ("sql_contains", "appt_status = 'dna'"),
        ("semantic_metric", "dna_rate"),
    ])

    r = chat("Break that down by HCP type", sid)
    check("A3 ICB HCP breakdown", r, [
        ("rows_or_sql", ""),
        ("sql_contains", "hcp_type"),
        ("sql_contains", "icb_name"),
        ("semantic_view", "hcp_type_breakdown"),
    ])

    r = chat("What does DNA mean?", sid)
    check("A4 appointments knowledge follow-up", r, [
        ("answer_contains", "did not attend"),
        ("no_sql", ""),
    ])

    print("\n=== Mixed Sessions ===")
    sid = str(uuid.uuid4())
    r = chat("How many GPs are there nationally?", sid)
    check("M1 workforce open", r, [
        ("rows_or_sql", ""),
        ("answer_contains", "gps"),
    ])

    r = chat("Show total appointments nationally in the latest month", sid)
    check("M2 switch to appointments", r, [
        ("rows_or_sql", ""),
        ("sql_contains", "from practice"),
        ("semantic_dataset", "appointments"),
    ])

    r = chat("What does appointment mode mean?", sid)
    check("M3 appointments knowledge", r, [
        ("answer_contains", "appointment mode"),
        ("no_sql", ""),
    ])

    r = chat("How many patients are registered at practice P82001?", sid)
    check("M4 switch back to workforce practice", r, [
        ("rows_or_sql", ""),
        ("answer_contains", "p82001"),
        ("sql_contains", "practice_detailed"),
    ])

    sid = str(uuid.uuid4())
    r = chat("Show appointments for Queens Park Medical Centre", sid)
    check("M5 appointments practice open", r, [
        ("rows_or_sql", ""),
        ("sql_contains", "gp_name"),
        ("semantic_dataset", "appointments"),
        ("semantic_entity_type", "practice"),
    ])

    r = chat("What about DNA rate?", sid)
    check("M6 appointments practice DNA", r, [
        ("rows_or_sql", ""),
        ("sql_contains", "gp_name"),
        ("sql_contains", "appt_status = 'dna'"),
    ])

    r = chat("What is the patients-per-GP ratio at practice P82001?", sid)
    check("M7 cross-dataset reset to workforce ratio", r, [
        ("rows_or_sql", ""),
        ("answer_contains", "p82001"),
        ("answer_contains", "patients-per-gp"),
        ("sql_contains", "practice_detailed"),
    ])

    passed = sum(1 for status, *_ in RESULTS if status == "PASS")
    total = len(RESULTS)
    print(f"\nSummary: {passed}/{total} passed")
