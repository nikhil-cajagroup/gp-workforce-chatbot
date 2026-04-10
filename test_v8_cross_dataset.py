"""
Focused cross-dataset join tests for v8.

Current supported slice:
- practice-level joined rankings across appointments + workforce
"""
import time
import uuid

import requests

BASE_URL = "http://localhost:8000"
RESULTS = []


def chat(question: str) -> dict:
    sid = str(uuid.uuid4())
    t0 = time.time()
    r = requests.post(
        f"{BASE_URL}/chat",
        json={"session_id": sid, "question": question},
        timeout=180,
    )
    elapsed = time.time() - t0
    payload = {"status": r.status_code, "elapsed": elapsed, "question": question}
    if r.status_code == 200:
        payload.update(r.json())
    else:
        payload["error"] = r.text[:500]
    return payload


def chat_chain(session_id: str, question: str) -> dict:
    t0 = time.time()
    r = requests.post(
        f"{BASE_URL}/chat",
        json={"session_id": session_id, "question": question},
        timeout=180,
    )
    elapsed = time.time() - t0
    payload = {"status": r.status_code, "elapsed": elapsed, "question": question}
    if r.status_code == 200:
        payload.update(r.json())
    else:
        payload["error"] = r.text[:500]
    return payload


def check(name: str, result: dict, checks: list[tuple[str, str]]):
    failures = []
    if result["status"] != 200:
        failures.append(f"HTTP {result['status']}: {result.get('error', '')[:200]}")

    answer = str(result.get("answer") or "").lower()
    sql = str(result.get("sql") or "").lower()

    for kind, value in checks:
        if kind == "answer_contains" and value.lower() not in answer:
            failures.append(f"Answer missing '{value}'")
        elif kind == "sql_contains" and value.lower() not in sql:
            failures.append(f"SQL missing '{value}'")

    status = "PASS" if not failures else "FAIL"
    RESULTS.append((status, name, failures, result["elapsed"]))
    print(f"[{status}] {name} ({result['elapsed']:.1f}s)")
    for failure in failures:
        print(f"  - {failure}")
    if failures:
        print(f"  answer: {(result.get('answer') or '')[:500]}")
        print(f"  sql:    {(result.get('sql') or '')[:500]}")


if __name__ == "__main__":
    r = chat("Which practices have the most appointments and fewest GPs?")
    check("X1 appointments + GP count join", r, [
        ("answer_contains", "appointments"),
        ("answer_contains", "gps"),
        ("sql_contains", '"test-gp-appointments".practice'),
        ("sql_contains", '"test-gp-workforce".practice_detailed'),
        ("sql_contains", "join wf on appt.practice_code = wf.practice_code"),
    ])

    r = chat("Top 5 practices by appointments per GP")
    check("X2 appointments per GP join ranking", r, [
        ("answer_contains", "appointments-per-gp"),
        ("sql_contains", "appointments_per_gp_fte"),
        ("sql_contains", '"test-gp-appointments".practice'),
        ("sql_contains", '"test-gp-workforce".practice_detailed'),
    ])

    sid = str(uuid.uuid4())
    _ = chat_chain(sid, "Top 5 practices by appointments per GP")
    r = chat_chain(sid, "What about the lowest 5?")
    check("X3 cross-dataset follow-up lowest 5", r, [
        ("answer_contains", "lowest"),
        ("sql_contains", "appointments_per_gp_fte"),
        ("sql_contains", "order by appointments_per_gp_fte asc"),
    ])

    sid = str(uuid.uuid4())
    _ = chat_chain(sid, "Top 5 practices by appointments per GP")
    r = chat_chain(sid, "Show this with GP headcount instead of FTE")
    check("X4 cross-dataset follow-up headcount basis", r, [
        ("answer_contains", "headcount"),
        ("sql_contains", "appointments_per_gp_headcount"),
    ])

    r = chat("Top 5 practices by appointments per GP in NHS Greater Manchester ICB")
    check("X5 cross-dataset ICB-scoped ranking", r, [
        ("answer_contains", "appointments-per-gp"),
        ("sql_contains", "workforce_icb_name"),
        ("sql_contains", "greater manchester"),
    ])

    sid = str(uuid.uuid4())
    _ = chat_chain(sid, "Top 5 practices by appointments per GP")
    r = chat_chain(sid, "Compare this with national average")
    check("X6 cross-dataset benchmark follow-up", r, [
        ("answer_contains", "national average"),
        ("sql_contains", "avg(appointments_per_gp_fte)"),
        ("sql_contains", "comparison_basis"),
    ])

    sid = str(uuid.uuid4())
    _ = chat_chain(sid, "Top 5 practices by appointments per GP")
    r = chat_chain(sid, "Show this by ICB")
    check("X7 cross-dataset by ICB follow-up", r, [
        ("answer_contains", "by icb"),
        ("sql_contains", "group by icb_name"),
        ("sql_contains", "appointments_per_gp_fte"),
    ])

    r = chat("Which regions have the highest appointments per GP?")
    check("X8 cross-dataset region rollup", r, [
        ("answer_contains", "by region"),
        ("sql_contains", "group by region_name"),
        ("sql_contains", "appointments_per_gp_fte"),
    ])

    sid = str(uuid.uuid4())
    _ = chat_chain(sid, "Top 5 practices by appointments per GP")
    _ = chat_chain(sid, "Show this by ICB")
    r = chat_chain(sid, "Compare this with national average")
    check("X9 grouped cross-dataset benchmark follow-up", r, [
        ("answer_contains", "average icb"),
        ("sql_contains", "avg(appointments_per_gp_fte)"),
        ("sql_contains", "comparison_basis"),
    ])

    passed = sum(1 for status, *_ in RESULTS if status == "PASS")
    total = len(RESULTS)
    print(f"\nSummary: {passed}/{total} passed")
