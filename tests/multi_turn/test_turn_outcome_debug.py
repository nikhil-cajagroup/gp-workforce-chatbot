from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from workforce.entity_types import ResolvedEntity
from workforce.turn_outcome import (
    build_turn_outcome,
    turn_outcome_from_dict,
    turn_outcome_to_dict,
)


def test_turn_outcome_round_trip_preserves_entities_and_summary():
    outcome = build_turn_outcome(
        intent="total_appointments",
        intent_confidence=1.0,
        intent_source="semantic",
        resolved_entities=[
            ResolvedEntity(
                entity_type="pcn",
                canonical_name="Newcastle South PCN",
                code=None,
                source_column="pcn_name",
                resolution_source="explicit_in_question",
                confidence=1.0,
            )
        ],
        chosen_metric="total_appointments",
        chosen_dataset="appointments",
        query_plan={"metric": "total_appointments", "grain": "pcn"},
        sql_executed="SELECT SUM(count_of_appointments) FROM practice",
        rows_returned=1,
        empty=False,
        answer="There were 18,604 appointments in Newcastle South PCN.",
        preview_markdown="| value |\n| --- |\n| 18604 |",
        last_error=None,
        follow_up_context={"entity_name": "Newcastle South PCN", "entity_type": "pcn"},
        routing_decision={"path_chosen": "semantic"},
    )

    payload = turn_outcome_to_dict(outcome)
    hydrated = turn_outcome_from_dict(payload)

    assert hydrated.intent == "total_appointments"
    assert hydrated.chosen_dataset == "appointments"
    assert hydrated.query_plan == {"metric": "total_appointments", "grain": "pcn"}
    assert hydrated.resolved_entities[0].canonical_name == "Newcastle South PCN"
    assert hydrated.result_summary.rows_returned == 1
    assert hydrated.result_summary.preview_markdown_present is True
    assert hydrated.routing_decision["path_chosen"] == "semantic"
