"""
Comprehensive realistic test suite: GP Workforce Chatbot
Simulates real questions from PCN Managers, GP Partners, ICB Workforce Leads.
74 tests across 10 phases.
"""
import requests, json, time, uuid, sys

BASE = "http://localhost:8000"
RESULTS = []   # (status, phase, tid, question, fails, elapsed)

def chat(q, sid=None):
    sid = sid or str(uuid.uuid4())
    t0 = time.time()
    try:
        r = requests.post(f"{BASE}/chat", json={"question": q, "session_id": sid}, timeout=180)
        elapsed = time.time() - t0
        if r.status_code != 200:
            return {"answer": "", "sql": "", "meta": {}, "elapsed": elapsed, "status": r.status_code, "sid": sid}
        d = r.json()
        d["elapsed"] = elapsed
        d["status"] = 200
        d["sid"] = sid
        return d
    except Exception as e:
        return {"answer": "", "sql": "", "meta": {}, "elapsed": time.time() - t0, "status": -1, "error": str(e), "sid": sid}

def run_checks(phase, tid, question, resp, checks):
    answer = resp.get("answer", "")
    sql = resp.get("sql", "")
    rows = resp.get("meta", {}).get("rows_returned", 0)
    fails = []
    for chk, val in checks:
        if chk == "has_sql" and (not sql or len(sql) < 10):
            fails.append("No SQL generated")
        elif chk == "has_data" and rows < 1:
            fails.append("0 rows returned")
        elif chk == "min_rows" and rows < val:
            fails.append(f"Expected >= {val} rows, got {rows}")
        elif chk == "answer_has_number" and not any(c.isdigit() for c in answer):
            fails.append("No numbers in answer")
        elif chk == "answer_contains" and val.lower() not in answer.lower():
            fails.append(f"Answer missing '{val}'")
        elif chk == "answer_not_contains" and val.lower() in answer.lower():
            fails.append(f"Answer should NOT contain '{val}'")
        elif chk == "sql_contains" and val.lower() not in sql.lower():
            fails.append(f"SQL missing '{val}'")
        elif chk == "sql_not_contains" and val.lower() in sql.lower():
            fails.append(f"SQL should NOT contain '{val}'")
        elif chk == "no_sql" and sql and len(sql.strip()) > 5:
            fails.append(f"Expected no SQL but got one")
        elif chk == "answer_mentions_out_of_scope":
            lw = answer.lower()
            if not any(k in lw for k in ["not available", "out of scope", "not in", "cannot", "doesn't include",
                                          "does not include", "not cover", "outside", "not contain",
                                          "beyond", "not able", "unable", "don't have", "do not have",
                                          "isn't available", "not supported", "not something",
                                          "can't answer", "can't provide", "can not answer",
                                          "i can't", "i cannot", "doesn't contain", "doesn't cover",
                                          "not designed", "specifically designed", "not include",
                                          "i'm specifically", "i am specifically"]):
                fails.append("Expected out-of-scope indication")
        elif chk == "http_ok" and resp.get("status") != 200:
            fails.append(f"HTTP {resp.get('status')}")
    status = "PASS" if not fails else "FAIL"
    RESULTS.append((status, phase, tid, question, fails, resp["elapsed"]))
    icon = "✅" if status == "PASS" else "❌"
    print(f"  {icon} {tid} ({resp['elapsed']:.1f}s): {question[:80]}")
    if fails:
        for f in fails:
            print(f"     ⚠ {f}")
    return status


# ============================================================================
# PHASE 1: PCN Manager — Daily Operational Questions (10 tests)
# ============================================================================
def phase_1():
    print("\n" + "=" * 80)
    print("PHASE 1: PCN Manager — Daily Operational Questions")
    print("=" * 80)

    r = chat("How many GPs are there in England right now?")
    run_checks("P1", "P1.1", "Total GPs in England", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None), ("sql_contains", "staff_group")])

    r = chat("What's the total nursing staff in primary care?")
    run_checks("P1", "P1.2", "Nursing staff count", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("How many ARRS roles are filled across England?")
    run_checks("P1", "P1.3", "ARRS roles", r,
               [("has_sql", None), ("answer_has_number", None)])

    r = chat("What's the current number of pharmacists working in primary care?")
    run_checks("P1", "P1.4", "Pharmacist count (bug fix)", r,
               [("has_sql", None), ("answer_has_number", None)])

    r = chat("Show me the FTE for all clinical staff")
    run_checks("P1", "P1.5", "Clinical staff FTE", r,
               [("has_sql", None), ("has_data", None), ("sql_contains", "fte")])

    r = chat("How many practices are there in England?")
    run_checks("P1", "P1.6", "Practice count", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("What's the average patients per GP?")
    run_checks("P1", "P1.7", "Patients per GP", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("How many admin and non-clinical staff are in primary care?")
    run_checks("P1", "P1.8", "Admin staff", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("Give me a breakdown of all staff groups in primary care")
    run_checks("P1", "P1.9", "Staff group breakdown", r,
               [("has_sql", None), ("has_data", None), ("min_rows", 2)])

    r = chat("What's the GP to patient ratio nationally?")
    run_checks("P1", "P1.10", "GP patient ratio", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])


# ============================================================================
# PHASE 2: ICB Workforce Lead — Regional Benchmarking (8 tests)
# ============================================================================
def phase_2():
    print("\n" + "=" * 80)
    print("PHASE 2: ICB Workforce Lead — Regional Benchmarking")
    print("=" * 80)

    r = chat("How many GPs are in NHS Greater Manchester ICB?")
    run_checks("P2", "P2.1", "GPs in Manchester ICB", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None), ("sql_contains", "manchester")])

    r = chat("Compare GP numbers across all regions")
    run_checks("P2", "P2.2", "Regional GP comparison", r,
               [("has_sql", None), ("has_data", None), ("min_rows", 2)])

    r = chat("Which ICB has the most GPs?")
    run_checks("P2", "P2.3", "Top ICB by GP count", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("How many practices are in the North East and Yorkshire region?")
    run_checks("P2", "P2.4", "Practices in NE&Y", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("What's the total FTE of nurses in London?")
    run_checks("P2", "P2.5", "Nurse FTE in London", r,
               [("has_sql", None), ("has_data", None), ("sql_contains", "london")])

    r = chat("Show me GP headcount by ICB in the South East")
    run_checks("P2", "P2.6", "GP by ICB in South East", r,
               [("has_sql", None), ("has_data", None), ("min_rows", 2)])

    r = chat("What's the patients per GP ratio in the Midlands?")
    run_checks("P2", "P2.7", "Patients per GP Midlands", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("How does the East of England compare to the South West for nurse numbers?")
    run_checks("P2", "P2.8", "Regional nurse comparison", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])


# ============================================================================
# PHASE 3: GP Partner — Workforce Planning & Trends (10 tests)
# ============================================================================
def phase_3():
    print("\n" + "=" * 80)
    print("PHASE 3: GP Partner — Workforce Planning & Trends")
    print("=" * 80)

    r = chat("How many GP trainees are there nationally?")
    run_checks("P3", "P3.1", "GP trainees national", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("How many salaried GPs vs GP partners are there?")
    run_checks("P3", "P3.2", "Salaried vs partners", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("What percentage of GPs are female?")
    run_checks("P3", "P3.3", "GP gender split", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("Show me GP numbers broken down by age band")
    run_checks("P3", "P3.4", "GP age profile", r,
               [("has_sql", None), ("has_data", None), ("min_rows", 2)])

    r = chat("How many locum GPs are there?")
    run_checks("P3", "P3.5", "Locum GPs", r,
               [("has_sql", None), ("answer_has_number", None)])

    r = chat("What's the FTE of GP partners nationally?")
    run_checks("P3", "P3.6", "GP partner FTE", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("How many physiotherapists work in primary care?")
    run_checks("P3", "P3.7", "Physiotherapists", r,
               [("has_sql", None), ("answer_has_number", None)])

    r = chat("Show me the split between full-time and part-time GPs")
    run_checks("P3", "P3.8", "FT vs PT GPs", r,
               [("has_sql", None), ("has_data", None)])

    r = chat("How many paramedics are working in primary care?")
    run_checks("P3", "P3.9", "Paramedics", r,
               [("has_sql", None), ("answer_has_number", None)])

    r = chat("What is the male to female ratio of nurses in primary care?")
    run_checks("P3", "P3.10", "Nurse gender ratio", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])


# ============================================================================
# PHASE 4: Multi-Turn Conversation Session (7 tests)
# ============================================================================
def phase_4():
    print("\n" + "=" * 80)
    print("PHASE 4: Multi-Turn Conversation (follow-ups, corrections, topic changes)")
    print("=" * 80)
    sid = f"p4_{uuid.uuid4().hex[:8]}"

    r = chat("How many GPs are there in the North West region?", sid)
    run_checks("P4", "P4.1", "North West GPs", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("And how many of those are in Greater Manchester?", sid)
    run_checks("P4", "P4.2", "Greater Manchester follow-up", r,
               [("has_sql", None), ("has_data", None), ("sql_contains", "manchester")])

    r = chat("Can you break that down by staff role?", sid)
    run_checks("P4", "P4.3", "Break down by role (bug fix)", r,
               [("has_sql", None), ("has_data", None)])

    r = chat("How has that changed over the last 2 years?", sid)
    run_checks("P4", "P4.4", "Trend over 2 years (LLM-dependent)", r,
               [("has_sql", None)])

    r = chat("Actually, can you show me headcount instead of FTE?", sid)
    run_checks("P4", "P4.5", "Headcount correction (bug fix)", r,
               [("has_sql", None), ("answer_has_number", None)])

    r = chat("Switching topic — how many GP trainees are there nationally?", sid)
    run_checks("P4", "P4.6", "Topic change to trainees", r,
               [("has_sql", None), ("has_data", None), ("sql_not_contains", "manchester")])

    r = chat("And what about in London?", sid)
    run_checks("P4", "P4.7", "London trainees follow-up", r,
               [("has_sql", None), ("has_data", None), ("sql_contains", "london")])


# ============================================================================
# PHASE 5: Practice-Level Questions (6 tests)
# ============================================================================
def phase_5():
    print("\n" + "=" * 80)
    print("PHASE 5: Practice-Level Questions")
    print("=" * 80)

    r = chat("How many GPs are at practice P82001?")
    run_checks("P5", "P5.1", "GPs at specific practice", r,
               [("has_sql", None), ("has_data", None), ("sql_contains", "P82001")])

    r = chat("What's the patient list size at practice P82002?")
    run_checks("P5", "P5.2", "Patient list size", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("Show me all staff at practice P82003")
    run_checks("P5", "P5.3", "All staff at practice", r,
               [("has_sql", None), ("has_data", None)])

    r = chat("What's the patients per GP ratio at practice P82004?")
    run_checks("P5", "P5.4", "PPG ratio at practice", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("Compare the FTE between practices P82001 and P82002")
    run_checks("P5", "P5.5", "Compare two practices", r,
               [("has_sql", None), ("has_data", None)])

    r = chat("Which practices in NHS Greater Manchester have more than 15000 patients?")
    run_checks("P5", "P5.6", "Large practices in Manchester", r,
               [("has_sql", None), ("has_data", None)])


# ============================================================================
# PHASE 6: Knowledge & Methodology Questions (6 tests)
# ============================================================================
def phase_6():
    print("\n" + "=" * 80)
    print("PHASE 6: Knowledge & Methodology Questions")
    print("=" * 80)

    r = chat("What data sources does this chatbot use?")
    run_checks("P6", "P6.1", "Data sources", r,
               [("no_sql", None), ("answer_contains", "NHS")])

    r = chat("How is FTE calculated in the workforce data?")
    run_checks("P6", "P6.2", "FTE methodology", r,
               [("no_sql", None)])

    r = chat("What does ARRS stand for and what roles are included?")
    run_checks("P6", "P6.3", "ARRS definition", r,
               [("no_sql", None), ("answer_contains", "additional")])

    r = chat("What's the difference between headcount and FTE?")
    run_checks("P6", "P6.4", "Headcount vs FTE", r,
               [("no_sql", None)])

    r = chat("What time period does the data cover?")
    run_checks("P6", "P6.5", "Data period", r,
               [("no_sql", None)])

    r = chat("What are the different staff groups in primary care?")
    run_checks("P6", "P6.6", "Staff groups overview", r,
               [])


# ============================================================================
# PHASE 7: Out-of-Scope Boundary Tests (6 tests)
# ============================================================================
def phase_7():
    print("\n" + "=" * 80)
    print("PHASE 7: Out-of-Scope Boundary Tests")
    print("=" * 80)

    r = chat("How many hospital consultants are there in the NHS?")
    run_checks("P7", "P7.1", "Hospital consultants (OOS)", r,
               [("answer_mentions_out_of_scope", None)])

    r = chat("What's the average salary for a GP?")
    run_checks("P7", "P7.2", "GP salary (OOS)", r,
               [("answer_mentions_out_of_scope", None)])

    r = chat("Can you prescribe medication for my patient?")
    run_checks("P7", "P7.3", "Clinical advice (OOS)", r,
               [("answer_mentions_out_of_scope", None)])

    r = chat("What's the weather like in London?")
    run_checks("P7", "P7.4", "Weather (OOS)", r,
               [("answer_mentions_out_of_scope", None)])

    r = chat("How do I claim for my pension as a GP partner?")
    run_checks("P7", "P7.5", "Pension (OOS)", r,
               [("answer_mentions_out_of_scope", None)])

    r = chat("Compare NHS dentist numbers with GP numbers")
    run_checks("P7", "P7.6", "Dentists (OOS)", r,
               [("answer_mentions_out_of_scope", None)])


# ============================================================================
# PHASE 8: Natural Language Robustness (8 tests)
# ============================================================================
def phase_8():
    print("\n" + "=" * 80)
    print("PHASE 8: Natural Language Robustness (typos, caps, slang, injection)")
    print("=" * 80)

    r = chat("HOW MANY GPS ARE THERE IN ENGLAND")
    run_checks("P8", "P8.1", "ALL CAPS query", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("how many gps in london mate?")
    run_checks("P8", "P8.2", "Informal slang", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("Hw many nurss in primary care?")
    run_checks("P8", "P8.3", "Typos", r,
               [("has_sql", None), ("answer_has_number", None)])

    r = chat("gp count")
    run_checks("P8", "P8.4", "Minimal query", r,
               [("has_sql", None), ("answer_has_number", None)])

    r = chat("'; DROP TABLE individual; --")
    run_checks("P8", "P8.5", "SQL injection attempt", r,
               [("sql_not_contains", "drop")])

    r = chat("I need to know the total number of general practitioners working in the National Health Service primary care setting across England")
    run_checks("P8", "P8.6", "Verbose formal query", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("GPs?")
    run_checks("P8", "P8.7", "One-word question", r,
               [])  # Acceptable for model to ask for clarification or provide data

    r = chat("Can u tell me how many docs r in manchester")
    run_checks("P8", "P8.8", "Abbreviations/text speak", r,
               [("has_sql", None), ("answer_has_number", None)])


# ============================================================================
# PHASE 9: Complex Analytical Questions (8 tests)
# ============================================================================
def phase_9():
    print("\n" + "=" * 80)
    print("PHASE 9: Complex Analytical Questions")
    print("=" * 80)

    r = chat("Which region has the highest patients per GP ratio?")
    run_checks("P9", "P9.1", "Highest PPG by region", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("Show me the top 10 ICBs by total GP FTE")
    run_checks("P9", "P9.2", "Top 10 ICBs by FTE", r,
               [("has_sql", None), ("has_data", None), ("min_rows", 5)])

    r = chat("What percentage of the primary care workforce is nursing staff?")
    run_checks("P9", "P9.3", "Nursing percentage", r,
               [("has_sql", None), ("answer_has_number", None)])

    r = chat("Compare the GP workforce size across all 7 regions")
    run_checks("P9", "P9.4", "All regions comparison", r,
               [("has_sql", None), ("has_data", None), ("min_rows", 5)])

    r = chat("Which sub-ICBs have the worst patients per GP ratio?")
    run_checks("P9", "P9.5", "Worst PPG sub-ICBs", r,
               [("has_sql", None), ("has_data", None)])

    r = chat("How many DPC roles are there by detailed staff role?")
    run_checks("P9", "P9.6", "DPC breakdown", r,
               [("has_sql", None), ("has_data", None), ("min_rows", 3)])

    r = chat("Show me practices with zero GPs but positive patient list")
    run_checks("P9", "P9.7", "Practices no GPs", r,
               [("has_sql", None)])

    r = chat("What's the average practice size by region?")
    run_checks("P9", "P9.8", "Avg practice size by region", r,
               [("has_sql", None), ("has_data", None), ("min_rows", 3)])


# ============================================================================
# PHASE 10: Practice Benchmarking Session (5 tests, multi-turn)
# ============================================================================
def phase_10():
    print("\n" + "=" * 80)
    print("PHASE 10: Practice Benchmarking Session (multi-turn)")
    print("=" * 80)
    sid = f"p10_{uuid.uuid4().hex[:8]}"

    r = chat("Show me all practices in NHS Kent and Medway ICB", sid)
    run_checks("P10", "P10.1", "Practices in Kent ICB", r,
               [("has_sql", None), ("has_data", None)])

    r = chat("How many GPs do they have in total?", sid)
    run_checks("P10", "P10.2", "Total GPs in Kent ICB", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("Which practice has the most patients?", sid)
    run_checks("P10", "P10.3", "Largest practice", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("What about the highest patients per GP ratio?", sid)
    run_checks("P10", "P10.4", "Highest PPG in Kent", r,
               [("has_sql", None), ("has_data", None), ("answer_has_number", None)])

    r = chat("Show me their ARRS staff breakdown", sid)
    run_checks("P10", "P10.5", "ARRS breakdown follow-up", r,
               [])  # ARRS roles are PCN-employed, not in practice data — model may correctly explain this


# ============================================================================
# REPORT
# ============================================================================
def print_report():
    print("\n\n" + "=" * 80)
    print("FINAL REPORT: GP Workforce Chatbot — Realistic Testing")
    print("=" * 80)

    total = len(RESULTS)
    passes = sum(1 for r in RESULTS if r[0] == "PASS")
    fails = total - passes

    # Per-phase summary
    phases = {}
    for status, phase, tid, q, f, e in RESULTS:
        if phase not in phases:
            phases[phase] = {"pass": 0, "fail": 0, "times": []}
        if status == "PASS":
            phases[phase]["pass"] += 1
        else:
            phases[phase]["fail"] += 1
        phases[phase]["times"].append(e)

    print(f"\n{'Phase':<10} {'Pass':>6} {'Fail':>6} {'Total':>6} {'Rate':>8} {'Avg Time':>10}")
    print("-" * 52)
    for ph in sorted(phases.keys()):
        p = phases[ph]
        t = p["pass"] + p["fail"]
        rate = p["pass"] / t * 100
        avg = sum(p["times"]) / len(p["times"])
        print(f"{ph:<10} {p['pass']:>6} {p['fail']:>6} {t:>6} {rate:>7.1f}% {avg:>9.1f}s")

    total_time = sum(e for _, _, _, _, _, e in RESULTS)
    avg_time = total_time / total if total else 0

    print("-" * 52)
    print(f"{'TOTAL':<10} {passes:>6} {fails:>6} {total:>6} {passes/total*100:>7.1f}% {avg_time:>9.1f}s")
    print(f"\nTotal elapsed: {total_time:.0f}s ({total_time/60:.1f} min)")

    if fails:
        print(f"\n❌ FAILURES ({fails}):")
        for status, phase, tid, q, f, e in RESULTS:
            if status == "FAIL":
                print(f"  {tid}: {q[:70]}")
                for reason in f:
                    print(f"     ⚠ {reason}")

    print(f"\n{'='*80}")
    print(f"RESULT: {passes}/{total} PASS ({passes/total*100:.0f}%)")
    print(f"{'='*80}\n")


# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    print("=" * 80)
    print("GP WORKFORCE CHATBOT — REALISTIC TEST SUITE")
    print("Simulating real PCN Manager, GP Partner, ICB Workforce Lead questions")
    print("=" * 80)

    # Health check
    try:
        r = requests.get(f"{BASE}/health", timeout=5)
        print(f"Server health: {r.json()}")
    except:
        print("ERROR: Server not reachable at", BASE)
        sys.exit(1)

    t_start = time.time()

    phase_1()
    phase_2()
    phase_3()
    phase_4()
    phase_5()
    phase_6()
    phase_7()
    phase_8()
    phase_9()
    phase_10()

    print_report()
