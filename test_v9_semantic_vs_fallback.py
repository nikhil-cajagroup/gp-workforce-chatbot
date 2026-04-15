"""
Focused regression bank that checks the current boundary between governed
semantic coverage and legacy-v8 fallback behavior.

Run this against a v8 server started with:
  USE_SEMANTIC_PATH=true
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
import uuid


BASE_URL = "http://localhost:8000"
RESULTS = []


def chat(question: str) -> dict:
    sid = str(uuid.uuid4())
    t0 = time.time()
    req = urllib.request.Request(
        f"{BASE_URL}/chat",
        data=json.dumps({"session_id": sid, "question": question}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    payload = {"session_id": sid, "question": question}
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            body = response.read().decode("utf-8")
            payload["status"] = response.status
            payload["elapsed"] = time.time() - t0
            payload.update(json.loads(body))
    except urllib.error.HTTPError as exc:
        payload["status"] = exc.code
        payload["elapsed"] = time.time() - t0
        payload["error"] = exc.read().decode("utf-8")[:500]
    except urllib.error.URLError as exc:
        payload["status"] = 0
        payload["elapsed"] = time.time() - t0
        payload["error"] = str(exc)[:500]
    return payload


def check(name: str, question: str, expect_semantic: bool) -> None:
    result = chat(question)
    failures = []
    if result["status"] != 200:
        failures.append(f"HTTP {result['status']}: {result.get('error', '')[:200]}")
    meta = result.get("meta") or {}
    semantic_path = meta.get("semantic_path") or {}
    used = bool(semantic_path.get("used"))
    if used is not expect_semantic:
        failures.append(f"semantic_path.used mismatch: expected {expect_semantic}, got {used}")
    status = "PASS" if not failures else "FAIL"
    RESULTS.append((status, name, failures, result["elapsed"]))
    print(f"[{status}] {name} ({result['elapsed']:.1f}s)")
    for failure in failures:
        print(f"  - {failure}")
    if failures:
        print(f"  question: {question}")
        print(f"  meta: {meta}")
        print(f"  answer: {(result.get('answer') or '')[:220]}")


if __name__ == "__main__":
    semantic_cases = [
        ("B1", "Show nurse FTE by region", True),
        ("B2", "Show registered patients by ICB compared with national average", True),
        ("B3", "Show appointments per patient by region compared with national average", True),
        ("B4", "Top 5 appointments per GP headcount by ICB", True),
        ("B5", "Which ICB has the highest patients per GP ratio?", True),
        ("B6", "How many appointments were there in Leeds?", True),
        ("B7", "Show appointments for Queens Park Medical Centre", True),
        ("B8", "Show total appointments trend over the past year", True),
    ]
    fallback_cases = [
        ("F1", "Show appointments by HCP type in NHS Greater Manchester ICB", False),
        ("F2", "Show appointment mode breakdown by region", False),
        ("F3", "Show the full staff breakdown for practice P82001", False),
    ]
    for name, question, expect_semantic in semantic_cases + fallback_cases:
        check(name, question, expect_semantic)
    passed = sum(1 for status, *_ in RESULTS if status == "PASS")
    total = len(RESULTS)
    print(f"\nSummary: {passed}/{total} passed")
