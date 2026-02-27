"""
Publication-aligned tests for GP Workforce Chatbot v5.3
Tests questions derived from the NHS Digital GP Workforce publication
(December 2025 release) to verify the bot can answer stakeholder questions.

Groups:
  H = Headline figures from publication
  T = Trainee / pipeline questions
  R = Role-type breakdowns (partner, salaried, locum)
  G = Geographic / organisational queries
  DQ = Data quality / methodology
  P = Practice-level metrics
  S = Staff group breakdowns (nurses, DPC, admin)
"""
import requests
import json
import time
import sys
import uuid

BASE_URL = "http://localhost:8000"
RESULTS = []

def chat(question, session_id=None):
    if session_id is None:
        session_id = str(uuid.uuid4())
    payload = {"question": question, "session_id": session_id}
    t0 = time.time()
    try:
        r = requests.post(f"{BASE_URL}/chat", json=payload, timeout=120)
        elapsed = time.time() - t0
        if r.status_code != 200:
            return {"question": question, "session_id": session_id, "status": r.status_code,
                    "error": r.text[:500], "elapsed": elapsed, "answer": "", "sql": "",
                    "preview_markdown": "", "meta": {}, "suggestions": []}
        data = r.json()
        data["question"] = question
        data["session_id"] = session_id
        data["status"] = 200
        data["elapsed"] = elapsed
        return data
    except Exception as e:
        return {"question": question, "session_id": session_id, "status": -1,
                "error": str(e), "elapsed": time.time() - t0, "answer": "", "sql": "",
                "preview_markdown": "", "meta": {}, "suggestions": []}

def check_result(result, test_name, checks):
    fails = []
    if result["status"] != 200:
        fails.append(f"HTTP {result['status']}: {result.get('error', 'unknown')[:200]}")
        RESULTS.append(("FAIL", test_name, result["question"], fails, result["elapsed"]))
        return
    answer = result.get("answer", "")
    sql = result.get("sql", "")
    preview = result.get("preview_markdown", "")
    meta = result.get("meta", {})
    rows = meta.get("rows_returned", 0)
    for check_type, check_val in checks:
        if check_type == "has_answer":
            if not answer or len(answer.strip()) < 10:
                fails.append("No meaningful answer returned")
        elif check_type == "has_sql":
            if not sql or len(sql.strip()) < 10:
                fails.append("No SQL generated")
        elif check_type == "no_sql":
            # Allow SQL that is ONLY comments (starts with --)
            sql_stripped = "\n".join(l for l in sql.strip().split("\n") if not l.strip().startswith("--"))
            if sql_stripped.strip() and len(sql_stripped.strip()) > 5:
                fails.append(f"Expected no SQL but got: {sql[:100]}")
        elif check_type == "has_data":
            if rows == 0:
                fails.append("No data rows returned")
        elif check_type == "min_rows":
            if rows < check_val:
                fails.append(f"Expected >= {check_val} rows, got {rows}")
        elif check_type == "answer_contains":
            if check_val.lower() not in answer.lower():
                fails.append(f"Answer missing '{check_val}'")
        elif check_type == "answer_not_contains":
            if check_val.lower() in answer.lower():
                fails.append(f"Answer should NOT contain '{check_val}'")
        elif check_type == "sql_contains":
            if check_val.lower() not in sql.lower():
                fails.append(f"SQL missing '{check_val}'")
        elif check_type == "has_preview":
            if not preview or len(preview.strip()) < 5:
                fails.append("No data preview table returned")
        elif check_type == "answer_mentions_out_of_scope":
            lower = answer.lower()
            if not any(k in lower for k in ["not available", "out of scope", "not in", "cannot",
                                              "doesn't include", "does not include", "not contain",
                                              "not cover", "outside"]):
                fails.append("Expected out-of-scope indication in answer")
    status = "PASS" if not fails else "FAIL"
    RESULTS.append((status, test_name, result["question"], fails, result["elapsed"]))

def print_result(status, test_name, question, fails, elapsed):
    icon = "✅" if status == "PASS" else "❌"
    print(f"  {icon} {test_name} ({elapsed:.1f}s)")
    if fails:
        for f in fails:
            print(f"      ↳ {f}")

# ═══════════════════════════════════════════════════════════════════
# HEADLINE FIGURES (H1-H6) — Key numbers from publication
# ═══════════════════════════════════════════════════════════════════
print("\n═══ HEADLINE FIGURES ═══")

r = chat("How many GPs are there in England as of December 2025?")
check_result(r, "H1 Total GPs Dec 2025", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("What is the total GP FTE in England?")
check_result(r, "H2 Total GP FTE", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("How many fully qualified GPs are there, excluding trainees and locums?")
check_result(r, "H3 Qualified GPs excl trainees", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("How many GP practices are there in England?")
check_result(r, "H4 Total practices", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("What is the average number of patients per practice?")
check_result(r, "H5 Avg patients per practice", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("How has the total number of GPs changed over the last 3 years?")
check_result(r, "H6 GP trend 3 years", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

# ═══════════════════════════════════════════════════════════════════
# TRAINEE / PIPELINE (T1-T5)
# ═══════════════════════════════════════════════════════════════════
print("\n═══ TRAINEE / PIPELINE ═══")

r = chat("How many GP trainees are there currently?")
check_result(r, "T1 Current trainee count", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
    ("sql_contains", "train"),
])

# KEY FIX TEST: trainee trend over multiple years
r = chat("Show me the trend in GP trainee numbers over the last 3 years")
check_result(r, "T2 Trainee trend 3 years", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("What is the ratio of trainees to qualified GPs?")
check_result(r, "T3 Trainee to GP ratio", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("How many GP registrars are at ST3 level?")
check_result(r, "T4 ST3 registrars", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("How many trainee GPs will be turning into qualified doctors soon?")
check_result(r, "T5 Trainee pipeline question", [
    ("has_answer", None),
])

# ═══════════════════════════════════════════════════════════════════
# ROLE-TYPE BREAKDOWNS (R1-R5)
# ═══════════════════════════════════════════════════════════════════
print("\n═══ ROLE-TYPE BREAKDOWNS ═══")

r = chat("How many salaried GPs are there compared to partner GPs?")
check_result(r, "R1 Salaried vs partner", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("What percentage of GPs are locums?")
check_result(r, "R2 Locum percentage", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("How many GP retainers are there?")
check_result(r, "R3 GP retainers", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

# KEY FIX TEST: partner vs salaried trend
r = chat("Show me the trend in salaried vs partner GPs over the years")
check_result(r, "R4 Partner vs salaried trend", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("What is the gender breakdown of GPs?")
check_result(r, "R5 Gender breakdown", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

# ═══════════════════════════════════════════════════════════════════
# GEOGRAPHIC / ORGANISATIONAL (G1-G5)
# ═══════════════════════════════════════════════════════════════════
print("\n═══ GEOGRAPHIC / ORGANISATIONAL ═══")

r = chat("Which ICB has the most GPs?")
check_result(r, "G1 ICB with most GPs", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
    ("sql_contains", "icb"),
])

r = chat("How many GPs are in NHS Greater Manchester ICB?")
check_result(r, "G2 GPs in specific ICB", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("Which region has the highest GP FTE?")
check_result(r, "G3 Region highest FTE", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

# KEY FIX TEST: PCN query — should NOT say out of scope
r = chat("Can I see GP numbers grouped by PCN?")
check_result(r, "G4 PCN grouping", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
    ("answer_not_contains", "out of scope"),
    ("answer_not_contains", "not available"),
])

r = chat("Which practices have the highest patient-to-GP ratio?")
check_result(r, "G5 Patient to GP ratio by practice", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

# ═══════════════════════════════════════════════════════════════════
# DATA QUALITY / METHODOLOGY (DQ1-DQ6)
# ═══════════════════════════════════════════════════════════════════
print("\n═══ DATA QUALITY / METHODOLOGY ═══")

r = chat("What is the difference between headcount and FTE?")
check_result(r, "DQ1 HC vs FTE definition", [
    ("has_answer", None), ("no_sql", None),
    ("answer_contains", "headcount"),
])

r = chat("How are partial estimates calculated?")
check_result(r, "DQ2 Partial estimates", [
    ("has_answer", None), ("no_sql", None),
])

# KEY FIX TEST: data sources knowledge question
r = chat("What data sources feed into the GP workforce publication?")
check_result(r, "DQ3 Data sources", [
    ("has_answer", None), ("no_sql", None),
])

r = chat("How often is the GP workforce data published?")
check_result(r, "DQ4 Publication frequency", [
    ("has_answer", None),
    ("answer_contains", "month"),
])

# KEY FIX TEST: seasonality knowledge question
r = chat("Why do trainee numbers fluctuate between months?")
check_result(r, "DQ5 Trainee seasonality", [
    ("has_answer", None), ("no_sql", None),
])

r = chat("What geographic levels are covered in the data?")
check_result(r, "DQ6 Geographic coverage", [
    ("has_answer", None), ("no_sql", None),
])

# ═══════════════════════════════════════════════════════════════════
# PRACTICE-LEVEL METRICS (P1-P3)
# ═══════════════════════════════════════════════════════════════════
print("\n═══ PRACTICE-LEVEL METRICS ═══")

r = chat("Which practice has the lowest FTE per GP ratio?")
check_result(r, "P1 Lowest FTE per GP", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("Show me practices with more than 20 GPs")
check_result(r, "P2 Practices with 20+ GPs", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("How many practice nurses are there in England?")
check_result(r, "P3 Practice nurses total", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

# ═══════════════════════════════════════════════════════════════════
# STAFF GROUP BREAKDOWNS (S1-S3)
# ═══════════════════════════════════════════════════════════════════
print("\n═══ STAFF GROUP BREAKDOWNS ═══")

r = chat("How many nurses work in general practice?")
check_result(r, "S1 Nurses in GP", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("What is the breakdown of direct patient care staff?")
check_result(r, "S2 DPC breakdown", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

r = chat("How many admin and clerical staff are in general practice?")
check_result(r, "S3 Admin staff", [
    ("has_answer", None), ("has_sql", None), ("has_data", None),
])

# ═══════════════════════════════════════════════════════════════════
# PRINT SUMMARY
# ═══════════════════════════════════════════════════════════════════
print("\n" + "═"*70)
print("PUBLICATION-ALIGNED TEST RESULTS")
print("═"*70)
passes = sum(1 for r in RESULTS if r[0] == "PASS")
fails = sum(1 for r in RESULTS if r[0] == "FAIL")
total = len(RESULTS)

for status, name, question, fail_reasons, elapsed in RESULTS:
    print_result(status, name, question, fail_reasons, elapsed)

print(f"\n{'═'*70}")
print(f"TOTAL: {passes}/{total} PASS  |  {fails} FAIL")
print(f"{'═'*70}")

# Print failed tests detail
if fails > 0:
    print("\nFAILED TESTS DETAIL:")
    for status, name, question, fail_reasons, elapsed in RESULTS:
        if status == "FAIL":
            print(f"\n  ❌ {name}")
            print(f"     Q: {question}")
            for f in fail_reasons:
                print(f"     → {f}")

sys.exit(0 if fails == 0 else 1)
