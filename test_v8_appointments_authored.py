"""
Authored appointments-focused tests for v8.

Covers:
- national totals / DNA / modes / trends
- named geography resolution
- practice code / practice name resolution
- planner fallback paths
- knowledge questions
- mixed follow-up inside appointments sessions
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
    rows = int(((result.get("meta") or {}).get("rows_returned")) or 0)

    for kind, value in checks:
        if kind == "rows_or_sql" and not (rows > 0 or sql.strip()):
            failures.append("Expected rows > 0 or SQL")
        elif kind == "answer_contains" and value.lower() not in answer:
            failures.append(f"Answer missing '{value}'")
        elif kind == "sql_contains" and value.lower() not in sql:
            failures.append(f"SQL missing '{value}'")
        elif kind == "sql_not_contains" and value.lower() in sql:
            failures.append(f"SQL should not contain '{value}'")
        elif kind == "no_sql" and sql.strip():
            failures.append(f"Expected no SQL, got '{sql[:120]}'")

    status = "PASS" if not failures else "FAIL"
    RESULTS.append((status, name, result["question"], failures, result["elapsed"]))
    print(f"[{status}] {name} ({result['elapsed']:.1f}s)")
    for failure in failures:
        print(f"  - {failure}")
    if failures:
        print(f"  answer: {(result.get('answer') or '')[:240]}")
        print(f"  sql:    {(result.get('sql') or '')[:240]}")


if __name__ == "__main__":
    r = chat("Show total appointments nationally in the latest month")
    check("A1 national total", r, [("rows_or_sql", ""), ("sql_contains", "from practice")])

    r = chat("What is the DNA rate nationally?")
    check("A2 national DNA", r, [("rows_or_sql", ""), ("sql_contains", "appt_status = 'dna'")])

    r = chat("Show appointment mode breakdown nationally")
    check("A3 national mode breakdown", r, [("rows_or_sql", ""), ("sql_contains", "appt_mode")])

    r = chat("Show GP appointments trend over the past year")
    check("A4 national trend", r, [("rows_or_sql", ""), ("sql_contains", "group by year, month")])

    r = chat("How many appointments were there in NHS Kent and Medway ICB?")
    check("A5 ICB total", r, [("rows_or_sql", ""), ("sql_contains", "from pcn_subicb"), ("answer_contains", "kent")])

    r = chat("What is the DNA rate in London region?")
    check("A6 region DNA", r, [("rows_or_sql", ""), ("sql_contains", "region_name"), ("answer_contains", "london")])

    r = chat("Show appointments by HCP type in NHS Greater Manchester ICB")
    check("A7 HCP type fallback", r, [("rows_or_sql", ""), ("sql_contains", "hcp_type"), ("answer_contains", "greater manchester")])

    r = chat("Show appointments by time between booking and appointment nationally")
    check("A8 booking lead time fallback", r, [("rows_or_sql", ""), ("sql_contains", "time_between_book_and_appt")])

    r = chat("Show appointments for P82001")
    check("A9 practice code", r, [("rows_or_sql", ""), ("sql_contains", "gp_code"), ("answer_contains", "p82001")])

    r = chat("Show appointments for Queens Park Medical Centre")
    check("A10 practice name", r, [("rows_or_sql", ""), ("sql_contains", "gp_name")])

    r = chat("What does DNA mean in the appointments data?")
    check("A11 knowledge DNA", r, [("answer_contains", "did not attend"), ("no_sql", "")])

    sid = str(uuid.uuid4())
    r = chat("Show total appointments nationally in the latest month", sid)
    check("A12 chain open", r, [("rows_or_sql", ""), ("sql_contains", "from practice")])
    r = chat("What about DNA rate?", sid)
    check("A13 chain follow-up DNA", r, [("rows_or_sql", ""), ("answer_contains", "dna")])

    passed = sum(1 for status, *_ in RESULTS if status == "PASS")
    total = len(RESULTS)
    print(f"\nSummary: {passed}/{total} passed")
