#!/usr/bin/env python3
"""V8 comprehensive test suite — 10 chains, 30 questions. Correct API contract."""
import time
import sys
import uuid

from test_http_harness import chat_json

RUN_ID = uuid.uuid4().hex[:8]

def ask(question, session_id, timeout=120):
    result = chat_json(question, session_id, timeout=timeout)
    if result.get("status") != 200:
        return {"error": result.get("error", f"HTTP {result.get('status')}"), "sql": ""}
    return {
        "answer": result.get("answer", ""),
        "sql": result.get("sql", ""),
        "suggestions": result.get("suggestions", []),
    }

chains = {
    "C1": {
        "thread": f"v8c1_{RUN_ID}",
        "questions": [
            ("C1Q1", "How many GPs are there in Birmingham?"),
            ("C1Q2", "What about nurses?"),
            ("C1Q3", "Compare that with Manchester"),
        ],
        "checks": {
            "C1Q1": lambda r: "birmingham" in r.get("answer","").lower() or "icb" in r.get("sql","").lower(),
            "C1Q2": lambda r: "nurse" in r.get("answer","").lower() or "nurse" in r.get("sql","").lower(),
            "C1Q3": lambda r: "manchester" in r.get("answer","").lower() or "manchester" in r.get("sql","").lower(),
        },
    },
    "C2": {
        "thread": f"v8c2_{RUN_ID}",
        "questions": [
            ("C2Q1", "What is the total GP FTE in NHS Devon ICB?"),
            ("C2Q2", "How does that compare to the national average?"),
            ("C2Q3", "Show the trend over the last 4 quarters"),
        ],
        "checks": {
            "C2Q1": lambda r: "devon" in r.get("answer","").lower() or "devon" in r.get("sql","").lower(),
            "C2Q2": lambda r: not r.get("error") and len(r.get("answer","")) > 20,
            "C2Q3": lambda r: "quarter" in r.get("sql","").lower() or "year_month" in r.get("sql","").lower() or "year" in r.get("sql","").lower(),
        },
    },
    "C3": {
        "thread": f"v8c3_{RUN_ID}",
        "questions": [
            ("C3Q1", "List the top 5 practices by total patients in London"),
            ("C3Q2", "Now show their patients per GP ratio"),
            ("C3Q3", "Which one has the best ratio?"),
        ],
        "checks": {
            "C3Q1": lambda r: "practice" in r.get("answer","").lower() or "prac_name" in r.get("sql","").lower(),
            "C3Q2": lambda r: "patient" in r.get("answer","").lower() or "ratio" in r.get("answer","").lower() or "per" in r.get("answer","").lower(),
            "C3Q3": lambda r: not r.get("error") and len(r.get("answer","")) > 20,
        },
    },
    "C4": {
        "thread": f"v8c4_{RUN_ID}",
        "questions": [
            ("C4Q1", "How many pharmacists work in NHS Norfolk and Waveney ICB?"),
            ("C4Q2", "What percentage of total staff are they?"),
            ("C4Q3", "Compare with NHS Suffolk and North East Essex ICB"),
        ],
        "checks": {
            "C4Q1": lambda r: "norfolk" in r.get("answer","").lower() or "norfolk" in r.get("sql","").lower(),
            "C4Q2": lambda r: "%" in r.get("answer","") or "percent" in r.get("answer","").lower() or "proportion" in r.get("answer","").lower(),
            "C4Q3": lambda r: "suffolk" in r.get("answer","").lower() or "suffolk" in r.get("sql","").lower(),
        },
    },
    "C5": {
        "thread": f"v8c5_{RUN_ID}",
        "questions": [
            ("C5Q1", "Which region has the most GPs per capita?"),
            ("C5Q2", "Break that down by ICB within that region"),
            ("C5Q3", "What about the region with the least?"),
        ],
        "checks": {
            "C5Q1": lambda r: "region" in r.get("answer","").lower() or "region" in r.get("sql","").lower(),
            "C5Q2": lambda r: "icb" in r.get("answer","").lower() or "icb_name" in r.get("sql","").lower(),
            "C5Q3": lambda r: not r.get("error") and len(r.get("answer","")) > 20,
        },
    },
    "C6": {
        "thread": f"v8c6_{RUN_ID}",
        "questions": [
            ("C6Q1", "What is the GP headcount at The Limes Medical Centre?"),
            ("C6Q2", "Show all staff groups for that practice"),
            ("C6Q3", "How does it compare to the PCN average?"),
        ],
        "checks": {
            "C6Q1": lambda r: "limes" in r.get("answer","").lower() or "limes" in r.get("sql","").lower(),
            "C6Q2": lambda r: "staff" in r.get("answer","").lower() or "staff_group" in r.get("sql","").lower(),
            "C6Q3": lambda r: "pcn" in r.get("answer","").lower() or "pcn" in r.get("sql","").lower(),
        },
    },
    "C7": {
        "thread": f"v8c7_{RUN_ID}",
        "questions": [
            ("C7Q1", "How many trainees are there nationally?"),
            ("C7Q2", "What about locums?"),
            ("C7Q3", "Show the split by gender"),
        ],
        "checks": {
            "C7Q1": lambda r: not r.get("error") and ("trainee" in r.get("answer","").lower() or "trainee" in r.get("sql","").lower() or "registrar" in r.get("sql","").lower()),
            "C7Q2": lambda r: "locum" in r.get("answer","").lower() or "locum" in r.get("sql","").lower(),
            "C7Q3": lambda r: "gender" in r.get("answer","").lower() or "gender" in r.get("sql","").lower() or "male" in r.get("answer","").lower(),
        },
    },
    "C8": {
        "thread": f"v8c8_{RUN_ID}",
        "questions": [
            ("C8Q1", "What is the average practice list size in the North East?"),
            ("C8Q2", "Compare that with the South West"),
            ("C8Q3", "Show the top 10 practices by list size in South West"),
        ],
        "checks": {
            "C8Q1": lambda r: "north east" in r.get("answer","").lower() or "north east" in r.get("sql","").lower() or "patient" in r.get("answer","").lower(),
            "C8Q2": lambda r: "south west" in r.get("answer","").lower() or "south west" in r.get("sql","").lower(),
            "C8Q3": lambda r: "practice" in r.get("answer","").lower() or "prac_name" in r.get("sql","").lower(),
        },
    },
    "C9": {
        "thread": f"v8c9_{RUN_ID}",
        "questions": [
            ("C9Q1", "How many direct patient care staff are in NHS Leeds ICB?"),
            ("C9Q2", "Show the breakdown by detailed staff role"),
            ("C9Q3", "What percentage are physician associates?"),
        ],
        "checks": {
            "C9Q1": lambda r: "leeds" in r.get("answer","").lower() or "leeds" in r.get("sql","").lower(),
            "C9Q2": lambda r: "role" in r.get("answer","").lower() or "detailed_staff_role" in r.get("sql","").lower(),
            "C9Q3": lambda r: "physician" in r.get("answer","").lower() or "%" in r.get("answer",""),
        },
    },
    "C10": {
        "thread": f"v8c10_{RUN_ID}",
        "questions": [
            ("C10Q1", "Tpyo test: How many GPS are in Brimingham?"),
            ("C10Q2", "What about Manch3ster?"),
            ("C10Q3", "Show nuses in Lodnon"),
        ],
        "checks": {
            "C10Q1": lambda r: not r.get("error") and len(r.get("answer","")) > 20,
            "C10Q2": lambda r: not r.get("error") and len(r.get("answer","")) > 20,
            "C10Q3": lambda r: not r.get("error") and len(r.get("answer","")) > 20,
        },
    },
}

results = {}
total_pass = total_fail = total_err = 0

for chain_id in sorted(chains.keys()):
    chain = chains[chain_id]
    thread = chain["thread"]
    print(f"\n{'='*60}")
    print(f"CHAIN {chain_id} (session: {thread})")
    print(f"{'='*60}")

    for qid, question in chain["questions"]:
        print(f"\n  [{qid}] {question}")
        t0 = time.time()
        result = ask(question, thread)
        elapsed = time.time() - t0

        if result.get("error"):
            status = "ERROR"
            total_err += 1
            print(f"  {elapsed:.1f}s | ERROR: {result['error'][:100]}")
        else:
            check_fn = chain["checks"][qid]
            passed = check_fn(result)
            status = "PASS" if passed else "FAIL"
            if passed:
                total_pass += 1
            else:
                total_fail += 1
            icon = "PASS" if passed else "FAIL"
            print(f"  {elapsed:.1f}s | {icon}")
            ans_preview = result["answer"][:200].replace("\n", " ")
            print(f"  Answer: {ans_preview}")
            if result["sql"]:
                sql_preview = result["sql"][:150].replace("\n", " ")
                print(f"  SQL: {sql_preview}")

        results[qid] = {"status": status, "elapsed": elapsed, "result": result}

total = total_pass + total_fail + total_err
print(f"\n{'='*60}")
print(f"V8 TEST RESULTS: {total_pass}/{total} PASSED ({100*total_pass/total:.0f}%)")
print(f"  Pass: {total_pass}  Fail: {total_fail}  Error: {total_err}")
print(f"{'='*60}")

if total_fail + total_err > 0:
    print("\nFAILURES:")
    for qid, r in sorted(results.items()):
        if r["status"] != "PASS":
            print(f"  {qid}: {r['status']}")
            if r["result"].get("error"):
                print(f"    Error: {r['result']['error'][:200]}")
            else:
                print(f"    Answer: {r['result']['answer'][:200]}")

sys.exit(0 if total_fail + total_err == 0 else 1)
