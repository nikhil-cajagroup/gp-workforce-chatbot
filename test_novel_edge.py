#!/usr/bin/env python3
"""Novel edge-case tests for GP Workforce Chatbot."""

import requests
import json
import time

BASE = "http://localhost:8000/chat"

def has_digits(s):
    return any(c.isdigit() for c in s)

def run():
    tests = [
        # Greetings & social
        ("Hello! How are you?", "greeting",
         lambda a, s: "hello" in a.lower() or "welcome" in a.lower() or "workforce" in a.lower()),
        ("Thank you so much!", "thanks",
         lambda a, s: "welcome" in a.lower() or "here" in a.lower()),
        ("Goodbye, have a nice day", "goodbye",
         lambda a, s: "goodbye" in a.lower() or "bye" in a.lower() or "come back" in a.lower()),

        # Out-of-scope: Non-England
        ("How many GPs are there in Scotland?", "scotland_oos",
         lambda a, s: "england only" in a.lower() or "scotland" in a.lower()),
        ("GP workforce in Wales", "wales_oos",
         lambda a, s: "england only" in a.lower() or "wales" in a.lower()),
        ("Belfast GP practices", "ni_oos",
         lambda a, s: "england only" in a.lower() or "northern ireland" in a.lower()),

        # Out-of-scope: Non-GP
        ("What is the weather today?", "weather_oos",
         lambda a, s: "scope" in a.lower() or "gp workforce" in a.lower() or "can help" in a.lower() or "data" in a.lower()),

        # Region queries (LIKE matching)
        ("How many GPs in North East?", "north_east",
         lambda a, s: has_digits(a) and len(s) > 0),
        ("GP FTE in South West", "south_west",
         lambda a, s: has_digits(a) and len(s) > 0),
        ("Show me London GP numbers", "london",
         lambda a, s: has_digits(a) and len(s) > 0),

        # Trainee queries (staff_role not detailed_staff_role)
        ("GP trainees by gender", "trainee_gender",
         lambda a, s: ("female" in a.lower() or "male" in a.lower()) and has_digits(a)),
        ("How many GP trainees are there?", "trainee_count",
         lambda a, s: has_digits(a)),

        # PCN grouping (hard intent)
        ("Can I see GP numbers grouped by PCN?", "pcn_group",
         lambda a, s: has_digits(a) and "pcn" in a.lower()),
        ("GP headcount per PCN", "pcn_per",
         lambda a, s: has_digits(a)),
        ("Show all PCN GP numbers", "pcn_all",
         lambda a, s: has_digits(a)),

        # Core national queries
        ("Total number of GPs in England", "total_gps",
         lambda a, s: has_digits(a)),
        ("How many practices are there?", "total_practices",
         lambda a, s: has_digits(a)),
        ("Average patients per practice", "avg_patients",
         lambda a, s: has_digits(a)),

        # Demographic queries
        ("GP age distribution", "age_dist",
         lambda a, s: has_digits(a) and ("age" in a.lower() or "under" in a.lower() or "over" in a.lower() or "band" in a.lower())),
        ("Gender split of all GPs", "gender_split",
         lambda a, s: ("female" in a.lower() or "male" in a.lower()) and has_digits(a)),

        # Practice-level queries
        ("How many GPs at Keele practice?", "keele_gps",
         lambda a, s: has_digits(a) and "keele" in a.lower()),
        ("Top 10 practices by GP headcount", "top10",
         lambda a, s: has_digits(a)),

        # Trend queries
        ("GP FTE trend over 3 years", "fte_trend",
         lambda a, s: has_digits(a)),

        # Knowledge queries
        ("What is the difference between headcount and FTE?", "hc_vs_fte",
         lambda a, s: "headcount" in a.lower() or "full" in a.lower()),
        ("When was this data published?", "pub_date",
         lambda a, s: "publish" in a.lower() or "quarter" in a.lower() or "nhs" in a.lower() or "data" in a.lower()),

        # Edge cases
        ("asdfjkl", "gibberish",
         lambda a, s: True),  # should handle gracefully
        ("SELECT * FROM users;", "sql_inject",
         lambda a, s: "scope" in a.lower() or "gp" in a.lower() or "help" in a.lower() or len(a) > 20),
    ]

    print(f"Running {len(tests)} novel edge-case tests...")
    print("=" * 70)
    passed = 0
    failed = []
    for q, label, check in tests:
        t0 = time.time()
        try:
            r = requests.post(BASE, json={
                "question": q,
                "session_id": f"novel_{label}_{int(time.time())}"
            }, timeout=120)
            d = r.json()
            ans = d.get("answer", "")
            sql = d.get("sql", "")
            ok = check(ans, sql)
            elapsed = time.time() - t0
            if ok:
                print(f"  \u2705 {label} ({elapsed:.1f}s)")
                passed += 1
            else:
                print(f"  \u274c {label} ({elapsed:.1f}s)")
                print(f"      Answer: {ans[:200]}")
                print(f"      SQL: {sql[:120]}")
                failed.append(label)
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  \u274c {label} ({elapsed:.1f}s) ERROR: {str(e)[:100]}")
            failed.append(label)

    print()
    print("=" * 70)
    print(f"TOTAL: {passed}/{len(tests)} PASS  |  {len(failed)} FAIL")
    if failed:
        print(f"FAILED: {failed}")
    print("=" * 70)


if __name__ == "__main__":
    run()
