from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gp_workforce_chatbot_backend_agent_v8 import (
    _classify_intent_shadow_fast,
    _finalize_shadow_intent_result,
)
from workforce.intent_classifier import (
    build_intent_result,
    intent_result_from_dict,
    intent_result_to_dict,
)


def test_intent_result_round_trip_preserves_fields():
    result = build_intent_result(
        intent="gp_fte",
        confidence=0.97,
        source="regex",
        dataset_hint="workforce",
        metric_hint="gp_fte",
    )

    payload = intent_result_to_dict(result)
    hydrated = intent_result_from_dict(payload)

    assert hydrated.intent == "gp_fte"
    assert hydrated.confidence == 0.97
    assert hydrated.source == "regex"
    assert hydrated.dataset_hint == "workforce"
    assert hydrated.metric_hint == "gp_fte"


def test_shadow_classifier_fast_path_uses_hard_intent_when_available():
    payload = _classify_intent_shadow_fast("How many GPs are there in Keele Practice?", {})

    assert payload["intent"] == "practice_gp_count"
    assert payload["source"] == "regex"
    assert payload["metric_hint"] == "practice_gp_count"


def test_shadow_classifier_finalize_uses_route_when_fast_path_is_unknown():
    state = {
        "_intent_result_v1": {"intent": "unknown", "confidence": 0.0, "source": "shadow_pending"},
        "_query_route": "knowledge",
        "_query_routing": {"value": "knowledge", "confidence": "high", "source": "deterministic_rule"},
        "_hard_intent": "",
        "semantic_state": {},
        "question": "What is FTE?",
        "original_question": "What is FTE?",
    }

    _finalize_shadow_intent_result(state)

    assert state["_intent_result_v1"]["intent"] == "knowledge"
    assert state["_intent_result_v1"]["source"] == "regex"

