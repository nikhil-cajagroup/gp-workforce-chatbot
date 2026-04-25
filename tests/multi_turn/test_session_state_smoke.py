from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from workforce.entity_types import ResolvedEntity
from workforce.session_state import (
    SessionState,
    session_state_from_dict,
    session_state_to_dict,
)
from workforce.turn_outcome import build_turn_outcome


def test_session_state_round_trip_preserves_previous_turn_and_entity_memory():
    previous_turn = build_turn_outcome(
        intent="gp_fte",
        intent_confidence=0.95,
        intent_source="regex",
        resolved_entities=[
            ResolvedEntity(
                entity_type="practice",
                canonical_name="Keele Practice",
                code="M83014",
                source_column="gp_code",
                resolution_source="explicit_in_question",
                confidence=1.0,
            )
        ],
        chosen_metric="gp_fte",
        chosen_dataset="workforce",
        query_plan={"metric": "gp_fte", "grain": "practice"},
        sql_executed="SELECT 1",
        rows_returned=1,
        empty=False,
        answer="There are 2.35 GP FTE at Keele Practice.",
        preview_markdown="| value |\n| --- |\n| 2.35 |",
        last_error=None,
        follow_up_context={"entity_name": "Keele Practice", "entity_type": "practice"},
        routing_decision={"path_chosen": "semantic"},
    )
    state = SessionState(
        session_id="session-123",
        previous_turn=previous_turn,
        entity_memory=(
            ResolvedEntity(
                entity_type="practice",
                canonical_name="Keele Practice",
                code="M83014",
                source_column="gp_code",
                resolution_source="follow_up",
                confidence=1.0,
            ),
        ),
        dataset_hint="workforce",
        metric_hint="gp_fte",
    )

    payload = session_state_to_dict(state)
    hydrated = session_state_from_dict(payload)

    assert hydrated.session_id == "session-123"
    assert hydrated.dataset_hint == "workforce"
    assert hydrated.metric_hint == "gp_fte"
    assert hydrated.previous_turn is not None
    assert hydrated.previous_turn.intent == "gp_fte"
    assert hydrated.entity_memory[0].canonical_name == "Keele Practice"
    assert hydrated.entity_memory[0].code == "M83014"
