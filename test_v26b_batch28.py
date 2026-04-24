"""
28-question batch regression test for v26b.
Covers workforce, appointments, ARRS caveats, national_category filters,
same-day %, geographic breakdowns, trends and cross-dataset metrics.

Run against the local server:
    python3 test_v26b_batch28.py
"""
from __future__ import annotations

import json
import time
import uuid
import urllib.error
import urllib.request

BASE_URL = "http://localhost:8000"
REQUEST_SPACING = 3.0          # seconds between calls (local server, no rate-limit)
RETRY_SLEEP     = 65.0
RESULTS: list[dict] = []
_last_call = 0.0


# ── HTTP helper ────────────────────────────────────────────────────────────────

def chat(question: str, session_id: str | None = None) -> dict:
    global _last_call
    sid = session_id or str(uuid.uuid4())
    for attempt in range(2):
        gap = REQUEST_SPACING - (time.time() - _last_call)
        if gap > 0:
            time.sleep(gap)
        t0 = time.time()
        req = urllib.request.Request(
            f"{BASE_URL}/chat",
            data=json.dumps({"session_id": sid, "question": question}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                body = json.loads(resp.read().decode())
                _last_call = time.time()
                body.update({"_q": question, "_sid": sid, "_elapsed": time.time() - t0, "_status": resp.status})
                return body
        except urllib.error.HTTPError as exc:
            body_text = exc.read().decode()[:400]
            _last_call = time.time()
            if exc.code == 429 and attempt == 0:
                time.sleep(RETRY_SLEEP)
                continue
            return {"_q": question, "_sid": sid, "_elapsed": time.time() - t0,
                    "_status": exc.code, "answer": "", "sql": "", "meta": {}, "_error": body_text}
        except urllib.error.URLError as exc:
            return {"_q": question, "_sid": sid, "_elapsed": time.time() - t0,
                    "_status": 0, "answer": "", "sql": "", "meta": {}, "_error": str(exc)}
    return {"_q": question, "_sid": sid, "_elapsed": 0, "_status": -1, "answer": "", "sql": "", "meta": {}}


# ── Check runner ───────────────────────────────────────────────────────────────

def check(qid: str, result: dict, checks: list[tuple[str, str]]):
    failures: list[str] = []
    if result["_status"] != 200:
        failures.append(f"HTTP {result['_status']}: {result.get('_error','')[:120]}")

    answer = str(result.get("answer") or "").lower()
    sql    = str(result.get("sql")    or "").lower()
    meta   = result.get("meta") or {}
    rows   = meta.get("rows_returned", 0)
    sp     = meta.get("semantic_path") or {}

    for kind, value in checks:
        v = value.lower() if value else ""
        if kind == "answer_contains" and v not in answer:
            failures.append(f"Answer missing '{value}'")
        elif kind == "answer_not_contains" and v in answer:
            failures.append(f"Answer should not contain '{value}'")
        elif kind == "sql_contains" and v not in sql:
            failures.append(f"SQL missing '{value}'")
        elif kind == "sql_not_contains" and v in sql:
            failures.append(f"SQL should not contain '{value}'")
        elif kind == "has_sql" and len(sql.strip()) < 10:
            failures.append("No SQL generated")
        elif kind == "has_data" and rows < 1:
            failures.append("0 rows returned")
        elif kind == "has_number" and not any(c.isdigit() for c in answer):
            failures.append("Answer has no numbers")
        elif kind == "semantic_used" and bool(sp.get("used")) is not (v == "true"):
            failures.append(f"semantic_path.used expected={value} got={sp.get('used')}")

    status = "PASS" if not failures else "FAIL"
    elapsed = result.get("_elapsed", 0)
    RESULTS.append({"id": qid, "status": status, "question": result["_q"],
                    "failures": failures, "elapsed": elapsed,
                    "answer": answer[:220], "sql": sql[:220]})

    icon = "✅" if not failures else "❌"
    print(f"  {icon} {qid} ({elapsed:.1f}s)  {result['_q'][:72]}")
    for f in failures:
        print(f"       ⚠  {f}")
    if failures:
        print(f"       ans: {answer[:180]}")
        if sql:
            print(f"       sql: {sql[:180]}")


# ══════════════════════════════════════════════════════════════════════════════
#  THE 28 QUESTIONS
# ══════════════════════════════════════════════════════════════════════════════

print("=" * 72)
print("  GP WORKFORCE CHATBOT — v26b 28-QUESTION BATCH REGRESSION")
print("=" * 72)

# ── WORKFORCE: DPC / ARRS roles  (Q1-Q5) ──────────────────────────────────────
print("\n── Workforce: DPC/ARRS roles ──")

# Q1 FIX: ARRS caveat for physiotherapists
r = chat("How many physiotherapists work in GP practices nationally?")
check("Q01", r, [
    ("has_sql", ""),
    ("has_number", ""),
    ("answer_contains", "arrs"),            # ARRS caveat must appear
    ("semantic_used", "true"),
])

# Q2 FIX: ARRS caveat for paramedics
r = chat("What is the current headcount of paramedics employed by GP practices?")
check("Q02", r, [
    ("has_sql", ""),
    ("has_number", ""),
    ("answer_contains", "arrs"),
    ("semantic_used", "true"),
])

# Q3: Counsellors — ARRS caveat
r = chat("How many counsellors are directly employed by GP practices?")
check("Q03", r, [
    ("has_sql", ""),
    ("has_number", ""),
    ("answer_contains", "arrs"),
    ("semantic_used", "true"),
])

# Q4: Social prescribing link workers
r = chat("How many social prescribing link workers are there in GP practices?")
check("Q04", r, [
    ("has_sql", ""),
    ("has_number", ""),
    ("semantic_used", "true"),
])

# Q5: Physician associates
r = chat("What is the headcount of physician associates employed by GP practices?")
check("Q05", r, [
    ("has_sql", ""),
    ("has_number", ""),
    ("semantic_used", "true"),
])

# ── WORKFORCE: Core staff headcount & FTE  (Q6-Q10) ──────────────────────────
print("\n── Workforce: Core staff ──")

# Q6: GP headcount nationally
r = chat("How many GPs are there in England nationally?")
check("Q06", r, [
    ("has_sql", ""),
    ("has_data", ""),
    ("has_number", ""),
    ("semantic_used", "true"),
])

# Q7: GP FTE by ICB
r = chat("Show GP FTE broken down by ICB")
check("Q07", r, [
    ("has_sql", ""),
    ("has_data", ""),
    ("sql_contains", "icb"),
    ("semantic_used", "true"),
])

# Q8: Nurse FTE by region
r = chat("What is the total nurse FTE by NHS region?")
check("Q08", r, [
    ("has_sql", ""),
    ("has_data", ""),
    ("sql_contains", "region"),
    ("semantic_used", "true"),
])

# Q9: GP trainees nationally
r = chat("How many GP trainees are there nationally?")
check("Q09", r, [
    ("has_sql", ""),
    ("has_number", ""),
    ("semantic_used", "true"),
])

# Q10: Patients per GP ratio
r = chat("What is the average patients-per-GP ratio nationally?")
check("Q10", r, [
    ("has_sql", ""),
    ("has_number", ""),
    ("semantic_used", "true"),
])

# ── APPOINTMENTS: Core totals & DNA  (Q11-Q14) ───────────────────────────────
print("\n── Appointments: Totals & DNA ──")

# Q11: Total appointments nationally
r = chat("How many total appointments were there in GP practices nationally in the latest month?")
check("Q11", r, [
    ("has_sql", ""),
    ("has_data", ""),
    ("has_number", ""),
    ("semantic_used", "true"),
])

# Q12: DNA rate nationally
r = chat("What is the national DNA rate for GP appointments?")
check("Q12", r, [
    ("has_sql", ""),
    ("has_number", ""),
    ("sql_contains", "dna"),
    ("semantic_used", "true"),
])

# Q13: DNA rate by ICB
r = chat("Show the DNA rate for appointments by ICB")
check("Q13", r, [
    ("has_sql", ""),
    ("has_data", ""),
    ("sql_contains", "dna"),
    ("sql_contains", "icb"),
    ("semantic_used", "true"),
])

# Q14: Top practices by appointments
r = chat("Which practices have the most appointments nationally?")
check("Q14", r, [
    ("has_sql", ""),
    ("has_data", ""),
    ("semantic_used", "true"),
])

# ── APPOINTMENTS: Mode & HCP breakdowns  (Q15-Q17) ────────────────────────────
print("\n── Appointments: Mode & HCP breakdown ──")

# Q15: Face-to-face vs telephone
r = chat("What proportion of appointments are face-to-face versus telephone nationally?")
check("Q15", r, [
    ("has_sql", ""),
    ("has_number", ""),
    ("sql_contains", "face-to-face"),
    ("semantic_used", "true"),
])

# Q16: Appointment mode breakdown by ICB
r = chat("Show the appointment mode breakdown in NHS Greater Manchester ICB")
check("Q16", r, [
    ("has_sql", ""),
    ("has_data", ""),
    ("sql_contains", "appt_mode"),
    ("sql_contains", "greater manchester"),
    ("semantic_used", "true"),
])

# Q17 FIX: Mental health appointments — national_category filter
r = chat("How many mental health appointments were there nationally?")
check("Q17", r, [
    ("has_sql", ""),
    ("sql_contains", "national_category"),
    ("sql_contains", "mental health"),
    ("semantic_used", "true"),
])

# ── APPOINTMENTS: Same-day & booking windows  (Q18-Q20) ───────────────────────
print("\n── Appointments: Booking windows ──")

# Q18: Booked on the day appointments share
r = chat("What percentage of appointments are booked on the same day nationally?")
check("Q18", r, [
    ("has_sql", ""),
    ("has_number", ""),
    ("sql_contains", "case when"),
    ("semantic_used", "true"),
])

# Q19 FIX: Same-day booking percentage
r = chat("What share of GP appointments were same-day bookings in the latest month?")
check("Q19", r, [
    ("has_sql", ""),
    ("has_number", ""),
    ("sql_contains", "case when"),
    ("sql_not_contains", "sum(count_of_appointments)"),   # must not be the raw total query
    ("semantic_used", "true"),
])

# Q20: Booking lead time breakdown
r = chat("Show the booking lead time breakdown for appointments in NHS Greater Manchester ICB")
check("Q20", r, [
    ("has_sql", ""),
    ("has_data", ""),
    ("sql_contains", "time_between_book_and_appt"),
    ("semantic_used", "true"),
])

# ── APPOINTMENTS: National categories  (Q21-Q24) ─────────────────────────────
print("\n── Appointments: National categories ──")

# Q21: Care home visit appointments
r = chat("How many care home visit appointments were there nationally?")
check("Q21", r, [
    ("has_sql", ""),
    ("sql_contains", "national_category"),
    ("sql_contains", "care home visit"),
    ("semantic_used", "true"),
])

# Q22: Flu vaccination appointments
r = chat("How many flu vaccination appointments were there nationally?")
check("Q22", r, [
    ("has_sql", ""),
    ("sql_contains", "national_category"),
    ("sql_contains", "flu vaccination"),
    ("semantic_used", "true"),
])

# Q23: Structured medication review
r = chat("How many structured medication review appointments were there nationally?")
check("Q23", r, [
    ("has_sql", ""),
    ("sql_contains", "national_category"),
    ("sql_contains", "structured medication review"),
    ("semantic_used", "true"),
])

# Q24: Planned clinical procedure
r = chat("How many planned clinical procedure appointments were there nationally?")
check("Q24", r, [
    ("has_sql", ""),
    ("sql_contains", "national_category"),
    ("sql_contains", "planned clinical procedure"),
    ("semantic_used", "true"),
])

# ── WORKFORCE: More ARRS roles  (Q25-Q26) ─────────────────────────────────────
print("\n── Workforce: More ARRS roles ──")

# Q25 FIX: Dietician ARRS caveat
r = chat("How many dieticians are employed directly by GP practices?")
check("Q25", r, [
    ("has_sql", ""),
    ("has_number", ""),
    ("answer_contains", "arrs"),
    ("semantic_used", "true"),
])

# Q26: Pharmacist headcount
r = chat("What is the current number of pharmacists working in GP practices nationally?")
check("Q26", r, [
    ("has_sql", ""),
    ("has_number", ""),
    ("semantic_used", "true"),
])

# ── TREND & CROSS-DATASET  (Q27-Q28) ──────────────────────────────────────────
print("\n── Trend & cross-dataset ──")

# Q27: Appointments trend over time
r = chat("Show total appointments nationally month by month over the past year")
check("Q27", r, [
    ("has_sql", ""),
    ("has_data", ""),
    ("sql_contains", "order by"),
    ("semantic_used", "true"),
])

# Q28: Appointments per GP by region
r = chat("Show appointments per GP FTE by region compared with the national average")
check("Q28", r, [
    ("has_sql", ""),
    ("has_data", ""),
    ("sql_contains", "national_average"),
    ("semantic_used", "true"),
])

# ── Summary ───────────────────────────────────────────────────────────────────
passed = sum(1 for r in RESULTS if r["status"] == "PASS")
failed = sum(1 for r in RESULTS if r["status"] == "FAIL")
total  = len(RESULTS)

print()
print("=" * 72)
print(f"  RESULT: {passed}/{total} PASSED  |  {failed} FAILED")
print("=" * 72)

if failed:
    print("\nFailed questions:")
    for r in RESULTS:
        if r["status"] == "FAIL":
            print(f"  ❌ {r['id']}  {r['question'][:65]}")
            for f in r["failures"]:
                print(f"       {f}")

print()
