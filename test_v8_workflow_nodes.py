"""
Focused checks for new workflow-oriented v8 nodes.

These are direct node tests so we can validate rewrite/schema/viz behavior
without needing a live HTTP server for every case.
"""
from gp_workforce_chatbot_backend_agent_v8 import (
    node_query_rewriter,
    node_schema_narrow,
    node_visualization_plan,
)


def assert_true(name: str, cond: bool, detail: str = ""):
    if cond:
        print(f"[PASS] {name}")
        return
    print(f"[FAIL] {name}")
    if detail:
        print(f"  - {detail}")
    raise SystemExit(1)


if __name__ == "__main__":
    state = {
        "question": "Which areas are most understaffed for GPs?",
        "original_question": "Which areas are most understaffed for GPs?",
        "conversation_history": "",
        "follow_up_context": None,
        "_needs_clarification": False,
    }
    out = node_query_rewriter(state)
    rewritten = str(out.get("question") or "").lower()
    assert_true(
        "R1 query rewriter clarifies gp pressure wording",
        "patients-per-gp" in rewritten or "patients per gp" in rewritten,
        rewritten,
    )

    state = {
        "dataset": "appointments",
        "question": "Show appointment mode breakdown in NHS Greater Manchester ICB",
        "original_question": "Show appointment mode breakdown in NHS Greater Manchester ICB",
        "plan": {},
        "sql": "",
        "_needs_clarification": False,
    }
    out = node_schema_narrow(state)
    assert_true(
        "S1 appointments schema narrowing prefers geography table",
        out.get("candidate_tables", [None])[0] == "pcn_subicb",
        str(out.get("candidate_tables")),
    )

    state = {
        "dataset": "workforce",
        "question": "Show GP age distribution by region",
        "original_question": "Show GP age distribution by region",
        "plan": {},
        "sql": "",
        "_needs_clarification": False,
    }
    out = node_schema_narrow(state)
    candidates = out.get("candidate_tables", [])
    assert_true(
        "S2 workforce schema narrowing keeps individual available for demographics",
        "individual" in candidates,
        str(candidates),
    )

    state = {
        "_is_knowledge": False,
        "_rows": 8,
        "plan": {"group_by": ["region_name"], "intent": "breakdown"},
        "sql": "SELECT region_name, COUNT(*) FROM x GROUP BY region_name",
        "semantic_state": {"metric": "appointments_total"},
    }
    out = node_visualization_plan(state)
    viz = out.get("viz_plan", {})
    assert_true(
        "V1 viz planner recommends bar chart for grouped result",
        bool(viz.get("recommended")) and viz.get("chart_type") == "bar",
        str(viz),
    )

    state = {
        "_is_knowledge": False,
        "_rows": 12,
        "plan": {"group_by": ["year", "month"], "intent": "trend"},
        "sql": "SELECT year, month, SUM(x) FROM y GROUP BY year, month",
        "semantic_state": {"metric": "gp_fte"},
    }
    out = node_visualization_plan(state)
    viz = out.get("viz_plan", {})
    assert_true(
        "V2 viz planner recommends line chart for trend result",
        bool(viz.get("recommended")) and viz.get("chart_type") == "line",
        str(viz),
    )

    print("\nSummary: 5/5 passed")
