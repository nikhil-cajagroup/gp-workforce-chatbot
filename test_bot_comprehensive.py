"""
Comprehensive end-to-end test suite for GP Workforce Chatbot v5.3
Tests against the running FastAPI server on localhost:8000
"""
import requests
import json
import time
import sys
import uuid

BASE_URL = "http://localhost:8000"
RESULTS = []

def chat(question, session_id=None):
    """Send a question to the chatbot and return the full response."""
    if session_id is None:
        session_id = str(uuid.uuid4())

    payload = {
        "question": question,
        "session_id": session_id,
    }

    t0 = time.time()
    try:
        r = requests.post(f"{BASE_URL}/chat", json=payload, timeout=120)
        elapsed = time.time() - t0

        if r.status_code != 200:
            return {
                "question": question,
                "session_id": session_id,
                "status": r.status_code,
                "error": r.text[:500],
                "elapsed": elapsed,
                "answer": "",
                "sql": "",
                "preview_markdown": "",
                "meta": {},
                "suggestions": [],
            }

        data = r.json()
        data["question"] = question
        data["session_id"] = session_id
        data["status"] = 200
        data["elapsed"] = elapsed
        return data
    except Exception as e:
        return {
            "question": question,
            "session_id": session_id,
            "status": -1,
            "error": str(e),
            "elapsed": time.time() - t0,
            "answer": "",
            "sql": "",
            "preview_markdown": "",
            "meta": {},
            "suggestions": [],
        }

def check_result(result, test_name, checks):
    """Validate a result against a list of checks. Returns (pass, fail_reasons)."""
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
            if sql and len(sql.strip()) > 5:
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
        elif check_type == "sql_contains_any":
            # check_val is a list of alternatives, at least one must be present
            if not any(v.lower() in sql.lower() for v in check_val):
                fails.append(f"SQL missing any of {check_val}")
        elif check_type == "sql_not_contains":
            if check_val.lower() in sql.lower():
                fails.append(f"SQL should NOT contain '{check_val}'")
        elif check_type == "has_preview":
            if not preview or len(preview.strip()) < 5:
                fails.append("No data preview table returned")
        elif check_type == "no_error":
            if meta.get("last_error"):
                fails.append(f"Error in meta: {meta['last_error'][:200]}")
        elif check_type == "max_attempts":
            if meta.get("attempts", 0) > check_val:
                fails.append(f"Too many attempts: {meta['attempts']} > {check_val}")
        elif check_type == "has_suggestions":
            suggestions = result.get("suggestions", [])
            if not suggestions:
                fails.append("No suggestions returned")
        elif check_type == "answer_mentions_out_of_scope":
            lower = answer.lower()
            if not any(k in lower for k in ["not available", "out of scope", "not in", "cannot", "doesn't include", "does not include", "not contain", "not cover", "outside"]):
                fails.append("Expected out-of-scope indication in answer")
        elif check_type == "has_confidence":
            conf = meta.get("confidence", {})
            if not conf or "score" not in conf:
                fails.append("Missing confidence score in meta")
        elif check_type == "sql_balanced_parens":
            if sql.count("(") != sql.count(")"):
                fails.append(f"Unbalanced parens: {sql.count('(')} open vs {sql.count(')')} close")
        elif check_type == "answer_has_number":
            if not any(c.isdigit() for c in answer):
                fails.append("Answer should contain at least one number")

    status = "PASS" if not fails else "FAIL"
    RESULTS.append((status, test_name, result["question"], fails, result["elapsed"]))


def print_result(status, test_name, question, fails, elapsed):
    icon = "✅" if status == "PASS" else "❌"
    print(f"  {icon} [{elapsed:.1f}s] {test_name}")
    if fails:
        for f in fails:
            print(f"       ↳ {f}")


# ============================================================================
# CATEGORY 1: Basic Data Queries
# ============================================================================
def test_category_1():
    print("\n" + "="*80)
    print("CATEGORY 1: Basic Data Queries")
    print("="*80)

    # 1.1 Total GP FTE nationally
    r = chat("Total GP FTE nationally in the latest month")
    check_result(r, "1.1 Total GP FTE nationally", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("no_error", None),
        ("sql_contains", "fte"),
        ("has_preview", None),
    ])
    print_result(*RESULTS[-1])

    # 1.2 How many trainee GPs
    r = chat("How many trainee GPs are currently in the workforce across England")
    check_result(r, "1.2 Trainee GP count", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("no_error", None),
        ("sql_contains", "train"),
    ])
    print_result(*RESULTS[-1])

    # 1.3 Top 10 ICBs by GP FTE
    r = chat("Top 10 ICBs by GP FTE")
    check_result(r, "1.3 Top 10 ICBs by GP FTE", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains", "icb"),
        ("sql_contains", "limit"),
    ])
    print_result(*RESULTS[-1])

    # 1.4 Average nurses per practice
    r = chat("What is the average number of nurses per GP practice")
    check_result(r, "1.4 Avg nurses per practice", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains", "nurse"),
    ])
    print_result(*RESULTS[-1])

    # 1.5 GP headcount trend over 12 months
    r = chat("GP headcount trend over the last 12 months")
    check_result(r, "1.5 GP headcount trend 12m", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("min_rows", 2),
    ])
    print_result(*RESULTS[-1])

    # 1.6 Gender breakdown of GPs
    r = chat("Gender breakdown of GPs nationally")
    check_result(r, "1.6 Gender breakdown", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains", "gender"),
    ])
    print_result(*RESULTS[-1])

    # 1.7 Show me locum GPs data
    r = chat("Show me locum GPs data")
    check_result(r, "1.7 Locum GPs data", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains", "locum"),
    ])
    print_result(*RESULTS[-1])

    # 1.8 Show me trainee GPs data
    r = chat("Show me trainee GPs data")
    check_result(r, "1.8 Trainee GPs data", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
    ])
    print_result(*RESULTS[-1])

    # 1.9 Total number of GP practices in England
    r = chat("What is the total number of GP practices in England")
    check_result(r, "1.9 Total GP practices", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains", "count"),
    ])
    print_result(*RESULTS[-1])

    # 1.10 Show trend in total GP numbers by year
    r = chat("Show trend in total GP numbers by year")
    check_result(r, "1.10 GP trend by year", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("min_rows", 2),
        ("sql_contains", "year"),
    ])
    print_result(*RESULTS[-1])


# ============================================================================
# CATEGORY 2: Complex Analytical Queries
# ============================================================================
def test_category_2():
    print("\n" + "="*80)
    print("CATEGORY 2: Complex Analytical Queries")
    print("="*80)

    # 2.1 Retirement eligible GPs
    r = chat("What proportion of qualified GPs are eligible to retire in the next 5 years")
    check_result(r, "2.1 Retirement eligible GPs", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains", "age"),
    ])
    print_result(*RESULTS[-1])

    # 2.2 FTE to headcount ratio
    r = chat("What is the FTE to headcount ratio for GPs showing part time working patterns")
    check_result(r, "2.2 FTE/headcount ratio", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
    ])
    print_result(*RESULTS[-1])

    # 2.3 Most reliant on locums
    r = chat("Which practices or ICBs are most reliant on locum GPs")
    check_result(r, "2.3 Highest locum reliance", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains", "locum"),
    ])
    print_result(*RESULTS[-1])

    # 2.4 Age distribution nearing retirement
    r = chat("What is the age distribution of current GPs and how many are nearing retirement")
    check_result(r, "2.4 Age distribution + retirement", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains", "age"),
    ])
    print_result(*RESULTS[-1])

    # 2.5 Workforce change over 3-5 years
    r = chat("How has the total GP workforce changed over the past 3 to 5 years")
    check_result(r, "2.5 Workforce change 3-5 years", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("min_rows", 2),
    ])
    print_result(*RESULTS[-1])

    # 2.6 Workforce composition by ICB
    r = chat("How does workforce composition vary by ICB or region")
    check_result(r, "2.6 Composition by ICB/region", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains_any", ["icb", "region"]),
    ])
    print_result(*RESULTS[-1])

    # 2.7 Compare GPs vs advanced practitioners
    r = chat("Compare GPs vs other advanced care practitioners like paramedics")
    check_result(r, "2.7 GPs vs advanced practitioners", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
    ])
    print_result(*RESULTS[-1])

    # 2.8 Practices with >50% GPs over 55
    r = chat("Show practices where more than 50 percent of GPs are over 55")
    check_result(r, "2.8 Practices >50% over 55", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains", "55"),
    ])
    print_result(*RESULTS[-1])

    # 2.9 GPs per 10000 patients by ICB
    r = chat("Show number of GPs per 10000 patients by ICB")
    check_result(r, "2.9 GPs per 10k patients by ICB", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains", "patient"),
    ])
    print_result(*RESULTS[-1])

    # 2.10 Lowest FTE per GP ratio practices
    r = chat("Show practices with lowest FTE per GP ratio")
    check_result(r, "2.10 Lowest FTE per GP ratio", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
    ])
    print_result(*RESULTS[-1])

    # 2.11 Multi-period comparison: trainees now vs 3 years ago
    r = chat("Compare the number of trainee GPs this year versus 3 years ago")
    # Note: SQL may use CASE WHEN or CTE approach — both are valid for multi-period
    check_result(r, "2.11 Trainee comparison multi-period", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains", "train"),
    ])
    print_result(*RESULTS[-1])

    # 2.12 GP workforce by gender and age band
    r = chat("Show the breakdown of GP workforce by gender and age band")
    check_result(r, "2.12 Gender x age band breakdown", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains", "gender"),
        ("sql_contains", "age"),
    ])
    print_result(*RESULTS[-1])


# ============================================================================
# CATEGORY 3: Follow-up and Topic Change Sequences
# ============================================================================
def test_category_3():
    print("\n" + "="*80)
    print("CATEGORY 3: Follow-up & Topic Change Sequences")
    print("="*80)

    session = str(uuid.uuid4())

    # 3.1 Base question: trainee data
    r = chat("How many trainee GPs are there in England", session)
    check_result(r, "3.1 Base: trainee count", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
    ])
    print_result(*RESULTS[-1])

    # 3.2 Follow-up: same topic, different angle
    r = chat("now show me by ICB", session)
    check_result(r, "3.2 Follow-up: trainees by ICB", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains", "icb"),
    ])
    print_result(*RESULTS[-1])

    # 3.3 Topic change: retirement (completely different topic)
    # Note: SQL may contain "Trainee" in NOT LIKE '%Trainee%' to EXCLUDE trainees —
    # that's correct business logic. The key check is that it's not querying FOR trainees.
    r = chat("What proportion of GPs are eligible to retire in the next 5 years", session)
    check_result(r, "3.3 Topic change: retirement", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_contains", "age"),        # Should query by age band for retirement
        ("sql_contains", "55"),          # Should reference age 55+ for retirement
    ])
    print_result(*RESULTS[-1])

    # 3.4 New session: follow-up without base question
    session2 = str(uuid.uuid4())
    r = chat("and by region", session2)
    check_result(r, "3.4 Follow-up without base question", [
        ("has_answer", None),
        # Should still try to answer something, even if no context
    ])
    print_result(*RESULTS[-1])

    # 3.5 Correction follow-up
    session3 = str(uuid.uuid4())
    r = chat("Show me GP FTE nationally", session3)
    check_result(r, "3.5a Base: GP FTE", [
        ("has_answer", None),
        ("has_sql", None),
    ])
    print_result(*RESULTS[-1])

    r = chat("i dont want FTE, i want headcount", session3)
    check_result(r, "3.5b Correction: headcount instead", [
        ("has_answer", None),
        ("has_sql", None),
        ("sql_contains", "headcount"),
    ])
    print_result(*RESULTS[-1])

    # 3.6 Multi-hop follow-up sequence
    session4 = str(uuid.uuid4())
    r = chat("Total nurses per practice nationally", session4)
    check_result(r, "3.6a Base: nurses per practice", [
        ("has_answer", None),
        ("has_sql", None),
    ])
    print_result(*RESULTS[-1])

    r = chat("what about pharmacists", session4)
    check_result(r, "3.6b Follow-up: pharmacists", [
        ("has_answer", None),
        ("has_sql", None),
        ("sql_contains", "pharma"),
    ])
    print_result(*RESULTS[-1])


# ============================================================================
# CATEGORY 4: Edge Cases & Error Handling
# ============================================================================
def test_category_4():
    print("\n" + "="*80)
    print("CATEGORY 4: Edge Cases & Error Handling")
    print("="*80)

    # 4.1 Misspelled practice name
    r = chat("How many GPs work at the keele practise")
    check_result(r, "4.1 Misspelled 'practise'", [
        ("has_answer", None),
        ("has_sql", None),
    ])
    print_result(*RESULTS[-1])

    # 4.2 Very short question
    r = chat("GP")
    check_result(r, "4.2 Very short question", [
        ("has_answer", None),
    ])
    print_result(*RESULTS[-1])

    # 4.3 Nonsense question
    r = chat("asdfghjkl")
    check_result(r, "4.3 Nonsense input", [
        ("has_answer", None),
    ])
    print_result(*RESULTS[-1])

    # 4.4 SQL injection attempt
    r = chat("'; DROP TABLE individual; --")
    check_result(r, "4.4 SQL injection attempt", [
        ("has_answer", None),
        ("sql_not_contains", "drop"),
    ])
    print_result(*RESULTS[-1])

    # 4.5 Out-of-scope: patient wait times
    r = chat("How long do patients typically wait to see a GP")
    check_result(r, "4.5 Out of scope: wait times", [
        ("has_answer", None),
        ("answer_mentions_out_of_scope", None),
    ])
    print_result(*RESULTS[-1])

    # 4.6 Out-of-scope: salary data
    r = chat("What is the average salary of a GP in England")
    check_result(r, "4.6 Out of scope: salary", [
        ("has_answer", None),
        ("answer_mentions_out_of_scope", None),
    ])
    print_result(*RESULTS[-1])

    # 4.7 Out-of-scope: Scotland specific
    r = chat("How many GPs work in Scotland")
    check_result(r, "4.7 Out of scope: Scotland", [
        ("has_answer", None),
    ])
    print_result(*RESULTS[-1])

    # 4.8 Empty question — Pydantic validation returns 422, which is correct
    r = chat("")
    if r["status"] == 422:
        RESULTS.append(("PASS", "4.8 Empty question (422 validation)", "", [], r["elapsed"]))
    else:
        check_result(r, "4.8 Empty question", [("has_answer", None)])
    print_result(*RESULTS[-1])

    # 4.9 Question with special characters
    r = chat("What % of GPs are female? (latest data)")
    check_result(r, "4.9 Special chars (%, parens)", [
        ("has_answer", None),
        ("has_sql", None),
    ])
    print_result(*RESULTS[-1])

    # 4.10 Very long question
    long_q = "Can you tell me the total number of general practitioners who are currently working " \
             "in general practice in England broken down by their ICB and also showing the gender " \
             "split and whether they are full time or part time based on the latest available data " \
             "from NHS Digital workforce statistics"
    r = chat(long_q)
    check_result(r, "4.10 Very long question", [
        ("has_answer", None),
        ("has_sql", None),
    ])
    print_result(*RESULTS[-1])


# ============================================================================
# CATEGORY 5: Knowledge-Only Questions
# ============================================================================
def test_category_5():
    print("\n" + "="*80)
    print("CATEGORY 5: Knowledge-Only Questions")
    print("="*80)

    # 5.1 What data is available
    r = chat("What data is available in this chatbot")
    check_result(r, "5.1 What data is available", [
        ("has_answer", None),
        ("no_sql", None),  # Should use knowledge path, no SQL
    ])
    print_result(*RESULTS[-1])

    # 5.2 Methodology question
    r = chat("How is FTE calculated in the GP workforce data")
    check_result(r, "5.2 How is FTE calculated", [
        ("has_answer", None),
        ("no_sql", None),
    ])
    print_result(*RESULTS[-1])

    # 5.3 What does DPC stand for
    r = chat("What does DPC stand for in the workforce context")
    check_result(r, "5.3 What does DPC mean", [
        ("has_answer", None),
        ("no_sql", None),
    ])
    print_result(*RESULTS[-1])

    # 5.4 Source of data
    r = chat("Where does this data come from")
    check_result(r, "5.4 Data source question", [
        ("has_answer", None),
        ("no_sql", None),
    ])
    print_result(*RESULTS[-1])

    # 5.5 Trainee conversion rate (out of scope data, knowledge answer)
    r = chat("What is the trainee to qualified GP conversion rate or pipeline effectiveness")
    check_result(r, "5.5 Trainee pipeline/conversion", [
        ("has_answer", None),
    ])
    print_result(*RESULTS[-1])


# ============================================================================
# CATEGORY 6: SQL Safety & Multi-Period Bug Regression
# ============================================================================
def test_category_6():
    print("\n" + "="*80)
    print("CATEGORY 6: SQL Safety & Multi-Period Bug Regression")
    print("="*80)

    # 6.1 Multi-period comparison: trainees this year vs 3 years ago (OR bug)
    r = chat("How many trainee GPs this year versus 3 years ago")
    check_result(r, "6.1 Multi-period OR bug regression", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("sql_balanced_parens", None),
    ])
    print_result(*RESULTS[-1])

    # 6.2 Three-period comparison
    r = chat("Compare GP FTE in December 2023, December 2024, and December 2025")
    check_result(r, "6.2 Three-period comparison", [
        ("has_answer", None),
        ("has_sql", None),
        ("sql_balanced_parens", None),
    ])
    print_result(*RESULTS[-1])

    # 6.3 SQL injection via entity name
    # Note: The LLM may include the user-supplied string inside a SQL string literal
    # (e.g., LIKE '%DROP TABLE%') which is harmless — Athena never executes DDL.
    # We verify the bot doesn't crash, returns a meaningful response,
    # and doesn't leak internal error stack traces.
    r = chat("How many GPs at '; DROP TABLE--")
    check_result(r, "6.3 SQL injection via entity", [
        ("has_answer", None),
        ("answer_not_contains", "traceback"),
        ("answer_not_contains", "exception"),
    ])
    print_result(*RESULTS[-1])

    # 6.4 Confidence scoring present
    r = chat("Total GP FTE in England")
    check_result(r, "6.4 Confidence score present", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("has_confidence", None),
    ])
    print_result(*RESULTS[-1])

    # 6.5 Answer contains numbers for data queries
    r = chat("How many GP practices are there nationally")
    check_result(r, "6.5 Answer contains numbers", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("answer_has_number", None),
    ])
    print_result(*RESULTS[-1])

    # 6.6 Follow-up suggestions present
    r = chat("Total nurses in GP practices")
    check_result(r, "6.6 Follow-up suggestions", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_suggestions", None),
    ])
    print_result(*RESULTS[-1])

    # 6.7 Preview markdown table for data queries
    r = chat("Top 10 ICBs by GP headcount")
    check_result(r, "6.7 Preview table present", [
        ("has_answer", None),
        ("has_sql", None),
        ("has_data", None),
        ("has_preview", None),
    ])
    print_result(*RESULTS[-1])


# ============================================================================
# CATEGORY 7: Correction & Topic Change Follow-ups
# ============================================================================
def test_category_7():
    print("\n" + "="*80)
    print("CATEGORY 7: Correction & Topic Change Follow-ups")
    print("="*80)

    # 7.1 Correction: FTE → headcount
    session = str(uuid.uuid4())
    r = chat("Show me GP FTE nationally", session)
    check_result(r, "7.1a Base: GP FTE", [
        ("has_answer", None),
        ("has_sql", None),
    ])
    print_result(*RESULTS[-1])

    r = chat("I dont want FTE, I want headcount", session)
    check_result(r, "7.1b Correction: FTE → headcount", [
        ("has_answer", None),
        ("has_sql", None),
        ("sql_contains", "headcount"),
    ])
    print_result(*RESULTS[-1])

    # 7.2 Topic change: trainees → retirement (no bleeding)
    session2 = str(uuid.uuid4())
    r = chat("How many trainee GPs are there?", session2)
    check_result(r, "7.2a Base: trainee count", [
        ("has_answer", None),
        ("has_sql", None),
        ("sql_contains", "train"),
    ])
    print_result(*RESULTS[-1])

    r = chat("What proportion of GPs are eligible for retirement?", session2)
    check_result(r, "7.2b Topic change: retirement", [
        ("has_answer", None),
        ("has_sql", None),
        ("sql_contains", "age"),
    ])
    print_result(*RESULTS[-1])

    # 7.3 Scope refinement: national → by region
    session3 = str(uuid.uuid4())
    r = chat("How many nurses work in GP practices?", session3)
    check_result(r, "7.3a Base: national nurse count", [
        ("has_answer", None),
        ("has_sql", None),
    ])
    print_result(*RESULTS[-1])

    r = chat("Break it down by region", session3)
    check_result(r, "7.3b Refinement: nurses by region", [
        ("has_answer", None),
        ("has_sql", None),
        ("sql_contains_any", ["region", "comm_region"]),
        ("min_rows", 2),
    ])
    print_result(*RESULTS[-1])


# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    print("="*80)
    print("GP WORKFORCE CHATBOT v5.9 — COMPREHENSIVE END-TO-END TEST")
    print("="*80)

    # Check server is up
    try:
        r = requests.get(f"{BASE_URL}/health", timeout=5)
        print(f"Server health: {r.json()}")
    except:
        print("ERROR: Server not running at", BASE_URL)
        sys.exit(1)

    categories = [
        ("Category 1", test_category_1),
        ("Category 2", test_category_2),
        ("Category 3", test_category_3),
        ("Category 4", test_category_4),
        ("Category 5", test_category_5),
        ("Category 6", test_category_6),
        ("Category 7", test_category_7),
    ]

    # Run specific category if requested
    if len(sys.argv) > 1:
        cat_num = int(sys.argv[1])
        categories = [categories[cat_num - 1]]

    for name, func in categories:
        func()

    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)

    passes = sum(1 for r in RESULTS if r[0] == "PASS")
    fails = sum(1 for r in RESULTS if r[0] == "FAIL")
    total = len(RESULTS)
    total_time = sum(r[4] for r in RESULTS)

    print(f"Total: {total} tests | ✅ {passes} passed | ❌ {fails} failed | ⏱  {total_time:.1f}s")

    if fails > 0:
        print(f"\n--- Failed Tests ---")
        for status, name, q, reasons, elapsed in RESULTS:
            if status == "FAIL":
                print(f"  ❌ {name}: {q}")
                for r in reasons:
                    print(f"     ↳ {r}")

    print()
