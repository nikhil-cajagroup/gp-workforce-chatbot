from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gp_workforce_chatbot_backend_agent_v8 import (
    _maybe_force_pipeline_clarification,
    _maybe_force_planner_clarification,
    _maybe_force_routing_clarification,
)


def test_low_confidence_llm_fallback_routes_to_clarification():
    state = {
        "_query_route": "data_complex",
        "_query_routing": {
            "value": "data_complex",
            "confidence": "low",
            "source": "llm_fallback",
            "reason": "deterministic route was uncertain",
        },
        "_hard_intent": "",
        "follow_up_context": {},
        "semantic_path": {},
        "plan": {},
        "semantic_state": {},
    }

    triggered = _maybe_force_routing_clarification(state)

    assert triggered is True
    assert state["_needs_clarification"] is True
    assert "which dataset or measure" in state["_clarification_question"].lower()
    assert state["_clarification_response_v1"]["missing_slots"] == ("metric",)


def test_unknown_fallback_plan_routes_to_clarification():
    state = {
        "_query_route": "data_complex",
        "_query_routing": {"value": "data_complex", "confidence": "medium", "source": "llm_fallback"},
        "_clarification_resolved": False,
        "sql": "",
        "answer": "",
        "_rows": 0,
        "plan": {
            "in_scope": True,
            "intent": "unknown",
            "notes": "fallback plan (structured output + JSON both failed)",
        },
        "semantic_path": {},
        "semantic_state": {},
    }

    triggered = _maybe_force_planner_clarification(state)

    assert triggered is True
    assert state["_needs_clarification"] is True
    assert "what result you want" in state["_clarification_question"].lower()


def test_empty_in_scope_pipeline_result_routes_to_clarification():
    state = {
        "_query_route": "data_complex",
        "_clarification_resolved": False,
        "answer": "",
        "sql": "",
        "df_preview_md": "",
        "_rows": 0,
        "plan": {"in_scope": True, "intent": "gp_fte"},
        "follow_up_context": {},
    }

    triggered = _maybe_force_pipeline_clarification(state)

    assert triggered is True
    assert state["_needs_clarification"] is True
    assert "couldn’t safely turn that into a concrete data query" in state["_clarification_question"].lower()
