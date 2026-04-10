"""
Targeted regression checks for the v8 single-worker vs multi-worker supervisor path.

These tests verify that mixed data + knowledge prompts:
- are routed through the supervisor layer
- preserve the data answer
- append the knowledge answer
"""
import time
import uuid

import requests

BASE_URL = "http://localhost:8000"
RESULTS: list[tuple[str, str, list[str], float]] = []


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


def check(name: str, result: dict, checks: list[tuple[str, str]]):
    failures: list[str] = []
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
    RESULTS.append((status, name, failures, float(result["elapsed"])))
    print(f"[{status}] {name} ({result['elapsed']:.1f}s)")
    for failure in failures:
        print(f"  - {failure}")
    if failures:
        print(f"  answer: {(result.get('answer') or '')[:500]}")
        print(f"  sql:    {(result.get('sql') or '')[:300]}")


if __name__ == "__main__":
    r = chat("How many GPs are there nationally and what does FTE mean?")
    check("MW1 workforce data + knowledge", r, [
        ("answer_contains", "there are"),
        ("answer_contains", "gps nationally"),
        ("answer_contains", "fte stands for"),
        ("answer_contains", "context:"),
        ("sql_contains", "from individual"),
    ])

    r = chat("How many appointments were there nationally and what does DNA mean in the appointments data?")
    check("MW2 appointments data + knowledge", r, [
        ("answer_contains", "appointments nationally"),
        ("answer_contains", "dna"),
        ("answer_contains", "did not attend"),
        ("answer_contains", "context:"),
        ("sql_contains", "from practice"),
    ])

    passed = sum(1 for status, *_ in RESULTS if status == "PASS")
    total = len(RESULTS)
    print(f"\nSummary: {passed}/{total} passed")
