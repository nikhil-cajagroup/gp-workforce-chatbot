"""
Live end-to-end regression for the guarded v9 semantic path inside v8.

Run this against a v8 server started with:
  USE_SEMANTIC_PATH=true
"""
from __future__ import annotations

import time
import uuid
import json
import urllib.error
import urllib.request


BASE_URL = "http://localhost:8000"
RESULTS = []
REQUEST_SPACING_SECONDS = 6.5
RETRY_SLEEP_SECONDS = 65
_LAST_CALL_TS = 0.0


def chat(question: str, session_id: str | None = None) -> dict:
    global _LAST_CALL_TS
    sid = session_id or str(uuid.uuid4())
    payload = {"session_id": sid, "question": question}

    for attempt in range(2):
        since_last = time.time() - _LAST_CALL_TS
        if since_last < REQUEST_SPACING_SECONDS:
            time.sleep(REQUEST_SPACING_SECONDS - since_last)
        t0 = time.time()
        req = urllib.request.Request(
            f"{BASE_URL}/chat",
            data=json.dumps({"session_id": sid, "question": question}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=180) as response:
                body = response.read().decode("utf-8")
                elapsed = time.time() - t0
                _LAST_CALL_TS = time.time()
                payload["status"] = response.status
                payload["elapsed"] = elapsed
                payload.update(json.loads(body))
                return payload
        except urllib.error.HTTPError as exc:
            elapsed = time.time() - t0
            body = exc.read().decode("utf-8")[:500]
            _LAST_CALL_TS = time.time()
            payload["status"] = exc.code
            payload["elapsed"] = elapsed
            payload["error"] = body
            if exc.code == 429 and attempt == 0:
                time.sleep(RETRY_SLEEP_SECONDS)
                continue
            return payload
        except urllib.error.URLError as exc:
            elapsed = time.time() - t0
            _LAST_CALL_TS = time.time()
            payload["status"] = 0
            payload["elapsed"] = elapsed
            payload["error"] = str(exc)[:500]
            return payload
    return payload


def check(name: str, result: dict, checks: list[tuple[str, str]]):
    failures = []
    if result["status"] != 200:
        failures.append(f"HTTP {result['status']}: {result.get('error', '')[:200]}")

    answer = str(result.get("answer") or "").lower()
    sql = str(result.get("sql") or "").lower()
    meta = result.get("meta") or {}
    semantic_path = meta.get("semantic_path") or {}
    semantic_request = meta.get("semantic_request_v9") or {}

    for kind, value in checks:
        if kind == "answer_contains" and value.lower() not in answer:
            failures.append(f"Answer missing '{value}'")
        elif kind == "sql_contains" and value.lower() not in sql:
            failures.append(f"SQL missing '{value}'")
        elif kind == "semantic_used" and bool(semantic_path.get("used")) is not (value.lower() == "true"):
            failures.append(f"semantic_path.used mismatch: expected {value}, got {semantic_path.get('used')}")
        elif kind == "semantic_metric" and str((semantic_request.get("metrics") or [""])[0]).lower() != value.lower():
            failures.append(f"semantic metric mismatch: expected '{value}', got '{semantic_request.get('metrics')}'")
        elif kind == "semantic_dataset" and str(semantic_path.get("dataset") or "").lower() != value.lower():
            failures.append(f"semantic dataset mismatch: expected '{value}', got '{semantic_path.get('dataset')}'")
        elif kind == "needs_clarification" and bool(meta.get("needs_clarification")) is not (value.lower() == "true"):
            failures.append(f"needs_clarification mismatch: expected {value}, got {meta.get('needs_clarification')}")
        elif kind == "clarification_contains" and value.lower() not in str(meta.get("clarification_question") or "").lower():
            failures.append(f"Clarification question missing '{value}'")

    status = "PASS" if not failures else "FAIL"
    RESULTS.append((status, name, failures, result["elapsed"]))
    print(f"[{status}] {name} ({result['elapsed']:.1f}s)")
    for failure in failures:
        print(f"  - {failure}")
    if failures:
        print(f"  answer: {(result.get('answer') or '')[:260]}")
        print(f"  sql:    {(result.get('sql') or '')[:260]}")
        print(f"  meta:   {meta}")


if __name__ == "__main__":
    r = chat("How many GPs are there nationally?")
    check("S1 semantic GP headcount", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "gp_headcount"),
        ("semantic_dataset", "workforce"),
        ("sql_contains", 'from "test-gp-workforce".individual'),
    ])

    r = chat("Show GP FTE by ICB")
    check("S2 semantic GP FTE by ICB", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "gp_fte"),
        ("semantic_dataset", "workforce"),
        ("sql_contains", "group by 1"),
        ("sql_contains", "icb_name"),
    ])

    r = chat("What is the patients-per-GP ratio at practice P82001?")
    check("S3 semantic patients per GP practice", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "patients_per_gp"),
        ("semantic_dataset", "workforce"),
        ("sql_contains", "practice_detailed"),
        ("sql_contains", "p82001"),
    ])

    r = chat("Show total appointments nationally in the latest month")
    check("S4 semantic total appointments", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "total_appointments"),
        ("semantic_dataset", "appointments"),
        ("sql_contains", 'from "test-gp-appointments".practice'),
    ])

    r = chat("What is the DNA rate in NHS Greater Manchester ICB?")
    check("S5 semantic DNA rate", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "dna_rate"),
        ("semantic_dataset", "appointments"),
        ("sql_contains", "appt_status = 'dna'"),
        ("sql_contains", "greater manchester"),
    ])

    r = chat("Show total appointments by region compared with national average")
    check("S6 semantic total appointments by region benchmark", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "total_appointments"),
        ("semantic_dataset", "appointments"),
        ("sql_contains", 'from "test-gp-appointments".pcn_subicb'),
        ("sql_contains", "national_average"),
    ])

    r = chat("Show DNA rate by ICB compared with national average")
    check("S7 semantic DNA rate by ICB benchmark", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "dna_rate"),
        ("semantic_dataset", "appointments"),
        ("sql_contains", 'from "test-gp-appointments".pcn_subicb'),
        ("sql_contains", "national_average"),
    ])

    r = chat("Top 10 appointments per GP by region compared with national average")
    check("S8 semantic appointments per GP by region benchmark", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "appointments_per_gp_fte"),
        ("semantic_dataset", "cross"),
        ("sql_contains", 'from "test-gp-appointments".pcn_subicb'),
        ("sql_contains", "national_average"),
    ])

    r = chat("Show nurse FTE by region")
    check("S9 semantic nurse FTE by region", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "nurse_fte"),
        ("semantic_dataset", "workforce"),
        ("sql_contains", "staff_group = 'nurses'"),
        ("sql_contains", "region_name"),
    ])

    r = chat("Top 5 appointments per GP headcount by ICB")
    check("S10 semantic appointments per GP headcount by ICB", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "appointments_per_gp_headcount"),
        ("semantic_dataset", "cross"),
        ("sql_contains", 'from "test-gp-appointments".pcn_subicb'),
        ("sql_contains", "appointments_per_gp_headcount"),
    ])

    r = chat("Show appointments per nurse by region compared with national average")
    check("S11 semantic appointments per nurse by region benchmark", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "appointments_per_nurse_fte"),
        ("semantic_dataset", "cross"),
        ("sql_contains", 'from "test-gp-appointments".pcn_subicb'),
        ("sql_contains", "national_average"),
    ])

    r = chat("Show registered patients by ICB compared with national average")
    check("S12 semantic registered patients by ICB benchmark", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "registered_patients"),
        ("semantic_dataset", "workforce"),
        ("sql_contains", 'from "test-gp-workforce".practice_detailed'),
        ("sql_contains", "national_average"),
    ])

    r = chat("Show appointments per patient by region compared with national average")
    check("S13 semantic appointments per patient by region benchmark", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "appointments_per_patient"),
        ("semantic_dataset", "cross"),
        ("sql_contains", 'from "test-gp-appointments".pcn_subicb'),
        ("sql_contains", "appointments_per_patient"),
    ])

    r = chat("How many appointments were there in Leeds?")
    check("S14 semantic city alias", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "total_appointments"),
        ("semantic_dataset", "appointments"),
        ("sql_contains", "west yorkshire"),
    ])

    r = chat("Show appointments for Queens Park Medical Centre")
    check("S15 semantic practice resolver", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "total_appointments"),
        ("semantic_dataset", "appointments"),
        ("sql_contains", "gp_code"),
    ])

    r = chat("How many GP appointments were made at Keele Practice in the latest month?")
    check("S15b semantic Keele practice fast path", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "total_appointments"),
        ("semantic_dataset", "appointments"),
        ("sql_contains", "gp_code"),
    ])

    r = chat("Show total appointments trend over the past year")
    check("S16 semantic trend", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "total_appointments"),
        ("semantic_dataset", "appointments"),
        ("sql_contains", "year"),
        ("sql_contains", "month"),
        ("sql_contains", "order by cast(year as integer) asc"),
    ])

    r = chat("What share of appointments were face to face in NHS Greater Manchester ICB?")
    check("S17 semantic face-to-face share", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "face_to_face_share"),
        ("semantic_dataset", "appointments"),
        ("sql_contains", "face-to-face"),
    ])

    r = chat("Show appointments by HCP type in NHS Greater Manchester ICB")
    check("S18 semantic HCP breakdown", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "total_appointments"),
        ("semantic_dataset", "appointments"),
        ("sql_contains", "hcp_type"),
        ("sql_contains", "group by 1"),
    ])

    r = chat("Show appointment mode breakdown in NHS Greater Manchester ICB")
    check("S19 semantic appointment mode breakdown", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "total_appointments"),
        ("semantic_dataset", "appointments"),
        ("sql_contains", "appt_mode"),
        ("sql_contains", "group by 1"),
    ])

    r = chat("Show booking lead time breakdown in NHS Greater Manchester ICB")
    check("S20 semantic booking lead-time breakdown", r, [
        ("semantic_used", "true"),
        ("semantic_metric", "total_appointments"),
        ("semantic_dataset", "appointments"),
        ("sql_contains", "time_between_book_and_appt"),
        ("sql_contains", "group by 1"),
    ])

    r = chat("How many appointments were there within 2 weeks at my practice?")
    check("S21 clarification for my practice booking-window question", r, [
        ("semantic_used", "false"),
        ("needs_clarification", "true"),
        ("clarification_contains", "which practice"),
    ])

    passed = sum(1 for status, *_ in RESULTS if status == "PASS")
    total = len(RESULTS)
    print(f"\nSummary: {passed}/{total} passed")
