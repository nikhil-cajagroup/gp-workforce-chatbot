#!/usr/bin/env python3
"""
V8 comprehensive test suite — 10 chains, 30 questions.
Same test suite used for v5/v6/v7 comparison.
"""
import requests
import json
import time
import sys

BASE = "http://localhost:8000"

def ask(question, thread_id, timeout=120):
    """Send a question and collect the SSE complete event."""
    try:
        resp = requests.post(
            f"{BASE}/chat",
            json={"message": question, "thread_id": thread_id},
            stream=True,
            timeout=timeout,
        )
        resp.raise_for_status()
        answer = None
        sql = None
        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue
            if line.startswith("event:"):
                evt = line[6:].strip()
            elif line.startswith("data:"):
                data_str = line[5:].strip()
                if not data_str:
                    continue
                try:
                    data = json.loads(data_str)
                except json.JSONDecodeError:
                    continue
                if evt == "complete":
                    answer = data.get("answer", "")
                    sql = data.get("sql", "")
                    break
                elif evt == "error":
                    return {"error": data.get("error", "Unknown"), "sql": ""}
        return {"answer": answer or "", "sql": sql or ""}
    except Exception as e:
        return {"error": str(e), "sql": ""}

# ── Test chains ──────────────────────────────────────────────────────
chains = {
    "C1": {
        "thread": "test_v8_c1",
        "questions": [
            ("C1Q1", "How many GPs are there in Birmingham?"),
            ("C1Q2", "What about nurses?"),
            ("C1Q3", "Compare that with Manchester"),
        ],
        "checks": {
            "C1Q1": lambda r: "birmingham" in r["answer"].lower() or "icb" in r["sql"].lower(),
            "C1Q2": lambda r: "nurse" in r["answer"].lower() or "nurse" in r["sql"].lower(),
            "C1Q3": lambda r: "manchester" in r["answer"].lower() or "manchester" in r["sql"].lower(),
        },
    },
    "C2": {
        "thread": "test_v8_c2",
        "questions": [
            ("C2Q1", "What is the total GP FTE in NHS Devon ICB?"),
            ("C2Q2", "How does that compare to the national average?"),
            ("C2Q3", "Show the trend over the last 4 quarters"),
        ],
        "checks": {
            "C2Q1": lambda r: "devon" in r["answer"].lower() or "devon" in r["sql"].lower(),
            "C2Q2": lambda r: not r.get("error") and len(r["answer"]) > 20,
            "C2Q3": lambda r: "quarter" in r["sql"].lower() or "year_month" in r["sql"].lower() or "trend" in r["answer"].lower(),
        },
    },
    "C3": {
        "thread": "test_v8_c3",
        "questions": [
            ("C3Q1", "List the top 5 practices by total patients in London"),
            ("C3Q2", "Now show their patients per GP ratio"),
            ("C3Q3", "Which one has the best ratio?"),
        ],
        "checks": {
            "C3Q1": lambda r: "practice" in r["answer"].lower() or "prac_name" in r["sql"].lower(),
            "C3Q2": lambda r: "patient" in r["answer"].lower() or "ratio" in r["answer"].lower() or "per" in r["answer"].lower(),
            "C3Q3": lambda r: not r.get("error") and len(r["answer"]) > 20,
        },
    },
    "C4": {
        "thread": "test_v8_c4",
        "questions": [
            ("C4Q1", "How many pharmacists work in NHS Norfolk and Waveney ICB?"),
            ("C4Q2", "What percentage of total staff are they?"),
            ("C4Q3", "Compare with NHS Suffolk and North East Essex ICB"),
        ],
        "checks": {
            "C4Q1": lambda r: "norfolk" in r["answer"].lower() or "norfolk" in r["sql"].lower(),
            "C4Q2": lambda r: "%" in r["answer"] or "percent" in r["answer"].lower() or "proportion" in r["answer"].lower(),
            "C4Q3": lambda r: "suffolk" in r["answer"].lower() or "suffolk" in r["sql"].lower(),
        },
    },
    "C5": {
        "thread": "test_v8_c5",
        "questions": [
            ("C5Q1", "Which region has the most GPs per capita?"),
            ("C5Q2", "Break that down by ICB within that region"),
            ("C5Q3", "What about the region with the least?"),
        ],
        "checks": {
            "C5Q1": lambda r: "region" in r["answer"].lower() or "region" in r["sql"].lower(),
            "C5Q2": lambda r: "icb" in r["answer"].lower() or "icb_name" in r["sql"].lower(),
            "C5Q3": lambda r: not r.get("error") and len(r["answer"]) > 20,
        },
    },
    "C6": {
        "thread": "test_v8_c6",
        "questions": [
            ("C6Q1", "What is the GP headcount at The Limes Medical Centre?"),
            ("C6Q2", "Show all staff groups for that practice"),
            ("C6Q3", "How does it compare to the PCN average?"),
        ],
        "checks": {
            "C6Q1": lambda r: "limes" in r["answer"].lower() or "limes" in r["sql"].lower(),
            "C6Q2": lambda r: "staff" in r["answer"].lower() or "staff_group" in r["sql"].lower(),
            "C6Q3": lambda r: "pcn" in r["answer"].lower() or "pcn" in r["sql"].lower(),
        },
    },
    "C7": {
        "thread": "test_v8_c7",
        "questions": [
            ("C7Q1", "How many trainees are there nationally?"),
            ("C7Q2", "What about locums?"),
            ("C7Q3", "Show the split by gender"),
        ],
        "checks": {
            "C7Q1": lambda r: not r.get("error") and ("trainee" in r["answer"].lower() or "trainee" in r["sql"].lower()),
            "C7Q2": lambda r: "locum" in r["answer"].lower() or "locum" in r["sql"].lower(),
            "C7Q3": lambda r: "gender" in r["answer"].lower() or "gender" in r["sql"].lower() or "male" in r["answer"].lower(),
        },
    },
    "C8": {
        "thread": "test_v8_c8",
        "questions": [
            ("C8Q1", "What is the average practice list size in the North East?"),
            ("C8Q2", "Compare that with the South West"),
            ("C8Q3", "Show the top 10 practices by list size in South West"),
        ],
        "checks": {
            "C8Q1": lambda r: "north east" in r["answer"].lower() or "north east" in r["sql"].lower() or "patient" in r["answer"].lower(),
            "C8Q2": lambda r: "south west" in r["answer"].lower() or "south west" in r["sql"].lower(),
            "C8Q3": lambda r: "practice" in r["answer"].lower() or "prac_name" in r["sql"].lower(),
        },
    },
    "C9": {
        "thread": "test_v8_c9",
        "questions": [
            ("C9Q1", "How many direct patient care staff are in NHS Leeds ICB?"),
            ("C9Q2", "Show the breakdown by detailed staff role"),
            ("C9Q3", "What percentage are physician associates?"),
        ],
        "checks": {
            "C9Q1": lambda r: "leeds" in r["answer"].lower() or "leeds" in r["sql"].lower(),
            "C9Q2": lambda r: "role" in r["answer"].lower() or "detailed_staff_role" in r["sql"].lower(),
            "C9Q3": lambda r: "physician" in r["answer"].lower() or "%" in r["answer"],
        },
    },
    "C10": {
        "thread": "test_v8_c10",
        "questions": [
            ("C10Q1", "Tpyo test: How many GPS are in Brimingham?"),
            ("C10Q2", "What about Manch3ster?"),
            ("C10Q3", "Show nuses in Lodnon"),
        ],
        "checks": {
            "C10Q1": lambda r: not r.get("error") and len(r["answer"]) > 20,
            "C10Q2": lambda r: not r.get("error") and len(r["answer"]) > 20,
            "C10Q3": lambda r: not r.get("error") and len(r["answer"]) > 20,
        },
    },
}

# ── Run tests ────────────────────────────────────────────────────────
results = {}
total_pass = 0
total_fail = 0
total_err = 0

for chain_id in sorted(chains.keys()):
    chain = chains[chain_id]
    thread = chain["thread"]
    print(f"\n{'='*60}")
    print(f"CHAIN {chain_id} (thread: {thread})")
    print(f"{'='*60}")

    for qid, question in chain["questions"]:
        print(f"\n  [{qid}] {question}")
        t0 = time.time()
        result = ask(question, thread)
        elapsed = time.time() - t0

        if result.get("error"):
            status = "ERROR"
            total_err += 1
            print(f"  ⏱ {elapsed:.1f}s | ❌ ERROR: {result['error'][:100]}")
        else:
            check_fn = chain["checks"][qid]
            passed = check_fn(result)
            status = "PASS" if passed else "FAIL"
            if passed:
                total_pass += 1
            else:
                total_fail += 1
            icon = "✅" if passed else "❌"
            print(f"  ⏱ {elapsed:.1f}s | {icon} {status}")
            # Print first 200 chars of answer
            ans_preview = result["answer"][:200].replace("\n", " ")
            print(f"  Answer: {ans_preview}...")
            if result["sql"]:
                sql_preview = result["sql"][:150].replace("\n", " ")
                print(f"  SQL: {sql_preview}...")

        results[qid] = {"status": status, "elapsed": elapsed, "result": result}

# ── Summary ──────────────────────────────────────────────────────────
total = total_pass + total_fail + total_err
print(f"\n{'='*60}")
print(f"V8 TEST RESULTS: {total_pass}/{total} PASSED ({100*total_pass/total:.0f}%)")
print(f"  ✅ Pass: {total_pass}")
print(f"  ❌ Fail: {total_fail}")
print(f"  💥 Error: {total_err}")
print(f"{'='*60}")

# Print failures
if total_fail + total_err > 0:
    print("\nFAILURES:")
    for qid, r in sorted(results.items()):
        if r["status"] != "PASS":
            print(f"  {qid}: {r['status']}")
            if r["result"].get("error"):
                print(f"    Error: {r['result']['error'][:200]}")
            else:
                print(f"    Answer preview: {r['result']['answer'][:150]}")
