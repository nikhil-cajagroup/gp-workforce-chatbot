"""
Focused dual-dataset smoke tests for v8.

Covers:
- workforce basics still working
- appointments basics from practice / pcn_subicb tables
- mixed-session dataset switching
- appointments knowledge routing
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
    payload = {"session_id": sid, "status": r.status_code, "elapsed": elapsed}
    if r.status_code == 200:
        payload.update(r.json())
    else:
        payload["error"] = r.text[:500]
    payload["question"] = question
    return payload


def check(name: str, result: dict, checks: list[tuple[str, str]]):
    failures = []
    if result["status"] != 200:
        failures.append(f"HTTP {result['status']}: {result.get('error', '')[:200]}")
    answer = (result.get("answer") or "").lower()
    sql = (result.get("sql") or "").lower()
    meta = result.get("meta") or {}
    rows = int(meta.get("rows_returned") or 0)

    for kind, value in checks:
        if kind == "answer_contains" and value.lower() not in answer:
            failures.append(f"Answer missing '{value}'")
        elif kind == "answer_not_contains" and value.lower() in answer:
            failures.append(f"Answer should not contain '{value}'")
        elif kind == "sql_contains" and value.lower() not in sql:
            failures.append(f"SQL missing '{value}'")
        elif kind == "sql_not_contains" and value.lower() in sql:
            failures.append(f"SQL should not contain '{value}'")
        elif kind == "rows_or_sql" and not (rows > 0 or sql.strip()):
            failures.append("Expected rows > 0 or non-empty SQL")
        elif kind == "no_sql" and sql.strip():
            failures.append(f"Expected no SQL, got '{sql[:120]}'")

    status = "PASS" if not failures else "FAIL"
    RESULTS.append((status, name, result["question"], failures, result["elapsed"]))
    icon = "PASS" if status == "PASS" else "FAIL"
    print(f"[{icon}] {name} ({result['elapsed']:.1f}s)")
    for failure in failures:
        print(f"  - {failure}")
    if failures:
        print(f"  answer: {(result.get('answer') or '')[:220]}")
        print(f"  sql:    {(result.get('sql') or '')[:220]}")


if __name__ == "__main__":
    print("\n=== Workforce ===")
    r = chat("How many GPs are there nationally?")
    check("W1 national GP count", r, [
        ("rows_or_sql", ""),
        ("answer_contains", "gp"),
    ])

    print("\n=== Appointments ===")
    r = chat("Show total appointments nationally in the latest month")
    check("A1 national appointments", r, [
        ("rows_or_sql", ""),
        ("sql_contains", "from practice"),
    ])

    r = chat("What is the DNA rate in NHS Greater Manchester ICB?")
    check("A2 DNA rate by ICB", r, [
        ("rows_or_sql", ""),
        ("answer_contains", "greater manchester"),
    ])

    r = chat("What does DNA mean in the appointments data?")
    check("A3 appointments knowledge routing", r, [
        ("answer_contains", "did not attend"),
        ("no_sql", ""),
    ])

    r = chat("Show appointments by HCP type in NHS Greater Manchester ICB")
    check("A4 appointments planner fallback by HCP type", r, [
        ("rows_or_sql", ""),
        ("sql_contains", "hcp_type"),
    ])

    r = chat("Show appointments by time between booking and appointment nationally")
    check("A5 appointments planner fallback by booking lead time", r, [
        ("rows_or_sql", ""),
        ("sql_contains", "time_between_book_and_appt"),
    ])

    print("\n=== Mixed Session ===")
    sid = str(uuid.uuid4())
    r = chat("How many GPs are there nationally?", sid)
    check("M1 workforce opening turn", r, [
        ("rows_or_sql", ""),
        ("answer_contains", "gp"),
    ])

    r = chat("Show total appointments nationally in the latest month", sid)
    check("M2 dataset switch to appointments", r, [
        ("rows_or_sql", ""),
        ("sql_contains", "from practice"),
    ])

    r = chat("What does DNA mean?", sid)
    check("M3 knowledge follow-up in appointments context", r, [
        ("answer_contains", "did not attend"),
        ("no_sql", ""),
    ])

    passed = sum(1 for status, *_ in RESULTS if status == "PASS")
    total = len(RESULTS)
    print(f"\nSummary: {passed}/{total} passed")
