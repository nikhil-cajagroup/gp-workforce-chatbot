from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from workforce.clarifications import (
    build_clarification_response,
    clarification_response_from_dict,
    clarification_response_to_dict,
)
from workforce.routing import (
    build_routing_decision,
    routing_decision_from_dict,
    routing_decision_to_dict,
)


def test_routing_decision_round_trip_preserves_fields():
    decision = build_routing_decision(
        intent="gp_fte",
        intent_confidence=0.98,
        path_chosen="semantic",
        path_reason="semantic fast path succeeded",
        alternatives_considered=("planner", "deterministic_override"),
        source="semantic_path",
    )

    payload = routing_decision_to_dict(decision)
    hydrated = routing_decision_from_dict(payload)

    assert hydrated.intent == "gp_fte"
    assert hydrated.intent_confidence == 0.98
    assert hydrated.path_chosen == "semantic"
    assert hydrated.path_reason == "semantic fast path succeeded"
    assert hydrated.alternatives_considered == ("planner", "deterministic_override")
    assert hydrated.source == "semantic_path"


def test_clarification_response_round_trip_preserves_slots_and_suggestions():
    response = build_clarification_response(
        question="Which practice do you mean?",
        reason="practice name was ambiguous",
        missing_slots=("practice",),
        suggestions=("Keele Practice", "Wolstanton Medical Centre"),
    )

    payload = clarification_response_to_dict(response)
    hydrated = clarification_response_from_dict(payload)

    assert hydrated.question == "Which practice do you mean?"
    assert hydrated.reason == "practice name was ambiguous"
    assert hydrated.missing_slots == ("practice",)
    assert hydrated.suggestions == ("Keele Practice", "Wolstanton Medical Centre")

