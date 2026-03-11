"""
Run realistic GP workforce questions and capture full answers for team review.
Outputs JSON file with all Q&A pairs for Word doc generation.
"""
import requests, json, time, uuid, sys

BASE = "http://localhost:8000"
MODEL_LABEL = sys.argv[1] if len(sys.argv) > 1 else "unknown"

def chat(q, sid=None):
    sid = sid or str(uuid.uuid4())
    t0 = time.time()
    try:
        r = requests.post(f"{BASE}/chat", json={"question": q, "session_id": sid}, timeout=120)
        data = r.json() if r.status_code == 200 else {"answer": f"ERROR {r.status_code}", "sql": ""}
    except Exception as e:
        data = {"answer": f"ERROR: {e}", "sql": ""}
    data["elapsed"] = round(time.time() - t0, 1)
    return data

# ─── REALISTIC QUESTIONS (25 representative questions) ───
questions = [
    # --- PCN Manager Questions ---
    {
        "id": "Q1",
        "category": "PCN Manager — Daily Operations",
        "question": "How many GPs are currently working in England?"
    },
    {
        "id": "Q2",
        "category": "PCN Manager — Daily Operations",
        "question": "What's the total number of nursing staff in primary care?"
    },
    {
        "id": "Q3",
        "category": "PCN Manager — Daily Operations",
        "question": "How many pharmacists are working in primary care?"
    },
    {
        "id": "Q4",
        "category": "PCN Manager — Daily Operations",
        "question": "Can you give me a breakdown of all staff groups nationally?"
    },
    {
        "id": "Q5",
        "category": "PCN Manager — Daily Operations",
        "question": "What is the current patients per GP ratio nationally?"
    },

    # --- ICB Workforce Lead Questions ---
    {
        "id": "Q6",
        "category": "ICB Workforce Lead — Regional Analysis",
        "question": "How many GPs are there in NHS Greater Manchester ICB?"
    },
    {
        "id": "Q7",
        "category": "ICB Workforce Lead — Regional Analysis",
        "question": "Compare the number of GPs across all regions in England"
    },
    {
        "id": "Q8",
        "category": "ICB Workforce Lead — Regional Analysis",
        "question": "Which ICB has the highest number of GPs?"
    },
    {
        "id": "Q9",
        "category": "ICB Workforce Lead — Regional Analysis",
        "question": "Show me the patients per GP ratio by region"
    },
    {
        "id": "Q10",
        "category": "ICB Workforce Lead — Regional Analysis",
        "question": "What are the top 10 ICBs by total clinical FTE?"
    },

    # --- GP Partner Questions ---
    {
        "id": "Q11",
        "category": "GP Partner — Workforce Planning",
        "question": "How many GP trainees are there nationally?"
    },
    {
        "id": "Q12",
        "category": "GP Partner — Workforce Planning",
        "question": "What's the split between salaried GPs and GP partners?"
    },
    {
        "id": "Q13",
        "category": "GP Partner — Workforce Planning",
        "question": "What is the gender split among GPs?"
    },
    {
        "id": "Q14",
        "category": "GP Partner — Workforce Planning",
        "question": "What does the GP age profile look like nationally?"
    },
    {
        "id": "Q15",
        "category": "GP Partner — Workforce Planning",
        "question": "How many locum GPs are there in England?"
    },

    # --- Complex Analytical Questions ---
    {
        "id": "Q16",
        "category": "Complex Analytics",
        "question": "Which region has the highest patients per GP ratio?"
    },
    {
        "id": "Q17",
        "category": "Complex Analytics",
        "question": "What percentage of the primary care workforce are nurses?"
    },
    {
        "id": "Q18",
        "category": "Complex Analytics",
        "question": "Show me average practice size by region"
    },
    {
        "id": "Q19",
        "category": "Complex Analytics",
        "question": "Which practices have no GPs registered?"
    },
    {
        "id": "Q20",
        "category": "Complex Analytics",
        "question": "Give me the DPC staff breakdown nationally"
    },

    # --- Knowledge Questions ---
    {
        "id": "Q21",
        "category": "Knowledge & Methodology",
        "question": "What data sources does this chatbot use?"
    },
    {
        "id": "Q22",
        "category": "Knowledge & Methodology",
        "question": "What is the difference between headcount and FTE?"
    },
    {
        "id": "Q23",
        "category": "Knowledge & Methodology",
        "question": "What are ARRS roles?"
    },

    # --- Robustness Questions ---
    {
        "id": "Q24",
        "category": "Natural Language Handling",
        "question": "HOW MANY GPS ARE THERE IN THE NORTH WEST?"
    },
    {
        "id": "Q25",
        "category": "Natural Language Handling",
        "question": "yo can u tell me how many docs r in london innit"
    },
]

# ─── MULTI-TURN CONVERSATION ───
multi_turn = [
    {
        "id": "MT1",
        "category": "Multi-Turn Conversation",
        "question": "How many GPs are there in the North West region?"
    },
    {
        "id": "MT2",
        "category": "Multi-Turn Conversation (follow-up)",
        "question": "And how many of those are in Greater Manchester?"
    },
    {
        "id": "MT3",
        "category": "Multi-Turn Conversation (follow-up)",
        "question": "Can you break that down by staff role?"
    },
    {
        "id": "MT4",
        "category": "Multi-Turn Conversation (follow-up)",
        "question": "How has that changed over the last 2 years?"
    },
    {
        "id": "MT5",
        "category": "Multi-Turn Conversation (correction)",
        "question": "Actually, can you show me headcount instead of FTE?"
    },
]

print(f"{'='*70}")
print(f"  RUNNING COMPARISON TEST — Model: {MODEL_LABEL}")
print(f"  {len(questions)} single questions + {len(multi_turn)} multi-turn = {len(questions)+len(multi_turn)} total")
print(f"{'='*70}\n")

results = []

# Single-turn questions
for i, q in enumerate(questions, 1):
    print(f"  [{i}/{len(questions)}] {q['id']}: {q['question'][:60]}...", end=" ", flush=True)
    r = chat(q["question"])
    print(f"({r['elapsed']}s)")
    results.append({
        "id": q["id"],
        "category": q["category"],
        "question": q["question"],
        "answer": r.get("answer", ""),
        "sql": r.get("sql", ""),
        "rows": r.get("meta", {}).get("rows_returned", 0),
        "elapsed": r["elapsed"],
    })

# Multi-turn conversation
print(f"\n  --- Multi-Turn Conversation ---")
mt_sid = str(uuid.uuid4())
for i, q in enumerate(multi_turn, 1):
    print(f"  [MT {i}/{len(multi_turn)}] {q['id']}: {q['question'][:60]}...", end=" ", flush=True)
    r = chat(q["question"], mt_sid)
    print(f"({r['elapsed']}s)")
    results.append({
        "id": q["id"],
        "category": q["category"],
        "question": q["question"],
        "answer": r.get("answer", ""),
        "sql": r.get("sql", ""),
        "rows": r.get("meta", {}).get("rows_returned", 0),
        "elapsed": r["elapsed"],
    })

# Save results
output_file = f"/Users/CajaLtd/Chatbot/comparison_results_{MODEL_LABEL.lower().replace(' ', '_')}.json"
with open(output_file, "w") as f:
    json.dump({"model": MODEL_LABEL, "timestamp": time.strftime("%Y-%m-%d %H:%M"), "results": results}, f, indent=2)

total_time = sum(r["elapsed"] for r in results)
print(f"\n{'='*70}")
print(f"  DONE — {len(results)} questions in {total_time:.0f}s ({total_time/60:.1f} min)")
print(f"  Results saved to: {output_file}")
print(f"{'='*70}")
