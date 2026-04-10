"""
Focused checks for explicit routing confidence in v8.
"""

from gp_workforce_chatbot_backend_agent_v8 import (
    _classify_query_route_decision,
    _decide_dataset_route,
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
    d = _decide_dataset_route("Show total appointments nationally in the latest month")
    assert_true(
        "D1 appointments deterministic route is high-confidence",
        d.get("value") == "appointments" and d.get("source") == "deterministic_signal" and d.get("confidence") == "high",
        str(d),
    )

    d = _decide_dataset_route(
        "What about DNA rate?",
        {
            "dataset": "appointments",
            "semantic_state": {"dataset": "appointments"},
            "previous_metric": "appointments_total",
        },
    )
    assert_true(
        "D2 vague follow-up inherits prior appointments context",
        d.get("value") == "appointments" and d.get("source") == "follow_context_rule",
        str(d),
    )

    q = _classify_query_route_decision("How many GPs are there nationally?")
    assert_true(
        "Q1 simple workforce question stays deterministic",
        q.get("value") == "data_simple" and q.get("source") == "deterministic_rule" and q.get("confidence") == "high",
        str(q),
    )

    q = _classify_query_route_decision("What does appointment mode mean?")
    assert_true(
        "Q2 knowledge question stays deterministic",
        q.get("value") == "knowledge" and q.get("source") == "deterministic_rule",
        str(q),
    )

    q = _classify_query_route_decision("Tell me something useful about this data")
    assert_true(
        "Q3 ambiguous prompt falls back semantically instead of pretending confidence",
        q.get("source") == "llm_fallback" and q.get("confidence") in {"medium", "low"},
        str(q),
    )

    print("\nSummary: 5/5 passed")
