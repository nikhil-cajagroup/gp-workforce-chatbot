"""Quick verification of the 4 fixes."""
import sys
import uuid

from test_http_harness import chat_json

def chat(q, sid=None):
    sid = sid or str(uuid.uuid4())
    result = chat_json(q, sid, timeout=120)
    result.setdefault("answer", "")
    result.setdefault("sql", "")
    result.setdefault("meta", {})
    return result

def show(tid, q, r, checks):
    answer = r.get("answer", "")
    sql = r.get("sql", "")
    rows = r.get("meta", {}).get("rows_returned", 0)
    fails = []
    if r.get("status") != 200:
        fails.append(f"HTTP {r.get('status')}: {str(r.get('error', ''))[:200]}")
    for chk, val in checks:
        if chk == "has_sql" and (not sql or len(sql) < 10): fails.append("No SQL")
        elif chk == "has_data" and rows < 1: fails.append(f"0 rows")
        elif chk == "answer_has_number" and not any(c.isdigit() for c in answer): fails.append("No numbers")
        elif chk == "answer_contains" and val.lower() not in answer.lower(): fails.append(f"Missing '{val}'")
        elif chk == "sql_contains" and val.lower() not in sql.lower(): fails.append(f"SQL missing '{val}'")
        elif chk == "sql_not_contains" and val.lower() in sql.lower(): fails.append(f"SQL has '{val}'")
    icon = "✅" if not fails else "❌"
    print(f"  {icon} {tid} ({r['elapsed']:.1f}s): {q[:70]}")
    if fails:
        for f in fails: print(f"     ⚠ {f}")
    if sql: print(f"     SQL: {sql[:200]}")
    print(f"     Answer: {answer[:200]}")
    print()
    return not fails

runs_ok = []
print("=" * 70)
print("  VERIFYING 4 FIXES")
print("=" * 70)

# FIX 1: Pharmacist question should now generate SQL
print("\n--- FIX 1: Pharmacist data query ---")
r = chat("What's the current number of pharmacists working in primary care?")
runs_ok.append(show("P1.4", "Pharmacist count", r, [("has_sql", None), ("answer_has_number", None)]))

# FIX 2-4: Multi-turn session with region context
print("\n--- FIX 2-4: Multi-turn follow-ups with region context ---")
sid = str(uuid.uuid4())

r1 = chat("How many GPs are there in the North West region?", sid)
runs_ok.append(show("P4.1", "North West GPs", r1, [("has_sql", None), ("has_data", None)]))

r2 = chat("And how many of those are in Greater Manchester?", sid)
runs_ok.append(show("P4.2", "Greater Manchester follow-up", r2, [("has_sql", None), ("has_data", None), ("sql_contains", "manchester")]))

r3 = chat("Can you break that down by staff role?", sid)
runs_ok.append(show("P4.3", "Break down by staff role", r3, [("has_sql", None), ("has_data", None)]))

r4 = chat("How has that changed over the last 2 years?", sid)
runs_ok.append(show("P4.4", "Trend over 2 years", r4, [("has_sql", None), ("has_data", None)]))

r5 = chat("Actually, can you show me headcount instead of FTE?", sid)
runs_ok.append(show("P4.5", "Headcount correction", r5, [("has_sql", None), ("answer_contains", "headcount")]))

r6 = chat("Switching topic — how many GP trainees are there nationally?", sid)
runs_ok.append(show("P4.6", "Topic change to trainees", r6, [("has_sql", None), ("has_data", None), ("sql_not_contains", "manchester")]))

print("Done!")
sys.exit(0 if all(runs_ok) else 1)
