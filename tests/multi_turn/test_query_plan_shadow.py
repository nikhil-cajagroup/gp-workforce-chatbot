from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from workforce.query_plan_types import (
    build_query_plan_v1,
    query_plan_v1_from_dict,
    query_plan_v1_to_dict,
)
from workforce.query_planner import build_live_query_plan, build_shadow_query_plan


def test_query_plan_v1_round_trip_preserves_semantic_fields():
    plan = build_query_plan_v1(
        dataset="appointments",
        metric="total_appointments",
        grain="pcn",
        group_by=("pcn_name",),
        entity_filters={"pcn_name": "Newcastle South PCN"},
        time_scope={"mode": "latest"},
        transforms=({"type": "topn", "n": 5},),
        compare={"dimension": "pcn_name", "values": ["A", "B"]},
        source="semantic_v9",
        table_hint="practice",
    )

    payload = query_plan_v1_to_dict(plan)
    hydrated = query_plan_v1_from_dict(payload)

    assert hydrated.dataset == "appointments"
    assert hydrated.metric == "total_appointments"
    assert hydrated.grain == "pcn"
    assert hydrated.group_by == ("pcn_name",)
    assert hydrated.entity_filters == {"pcn_name": "Newcastle South PCN"}
    assert hydrated.time_scope == {"mode": "latest"}
    assert hydrated.transforms == ({"type": "topn", "n": 5},)
    assert hydrated.compare == {"dimension": "pcn_name", "values": ["A", "B"]}
    assert hydrated.source == "semantic_v9"
    assert hydrated.table_hint == "practice"


def test_build_shadow_query_plan_prefers_semantic_request_when_present():
    payload = build_shadow_query_plan(
        dataset="appointments",
        semantic_request_v9={
            "metrics": ["total_appointments"],
            "entity_filters": {"pcn_name": "Newcastle South PCN"},
            "group_by": ["pcn_name"],
            "time": {"mode": "latest"},
            "transforms": [{"type": "topn", "n": 5}],
        },
        semantic_path={
            "used": True,
            "dataset": "appointments",
            "grain": "pcn",
            "metric_keys": ["total_appointments"],
        },
        legacy_plan={"table": "practice", "intent": "semantic_metric"},
        clarification_question=None,
        needs_clarification=False,
    )

    assert payload["dataset"] == "appointments"
    assert payload["metric"] == "total_appointments"
    assert payload["grain"] == "pcn"
    assert payload["group_by"] == ("pcn_name",)
    assert payload["entity_filters"] == {"pcn_name": "Newcastle South PCN"}
    assert payload["time_scope"] == {"mode": "latest"}
    assert payload["source"] == "semantic_v9"
    assert payload["table_hint"] == "practice"


def test_build_shadow_query_plan_uses_legacy_plan_when_semantic_absent():
    payload = build_shadow_query_plan(
        dataset="workforce",
        semantic_request_v9={},
        semantic_path={},
        legacy_plan={"intent": "trend", "group_by": ["region_name"], "table": "individual"},
        clarification_question="Which region do you mean?",
        needs_clarification=True,
    )

    assert payload["dataset"] == "workforce"
    assert payload["metric"] == "trend"
    assert payload["group_by"] == ("region_name",)
    assert payload["requires_clarification"] is True
    assert payload["clarification_question"] == "Which region do you mean?"
    assert payload["source"] == "legacy_plan"
    assert payload["table_hint"] == "individual"


def test_build_live_query_plan_allows_gp_fte_and_marks_live_source():
    payload = build_live_query_plan(
        dataset="workforce",
        semantic_request_v9={
            "metrics": ["gp_fte"],
            "entity_filters": {},
            "group_by": [],
            "time": {"mode": "latest"},
            "transforms": [],
        },
        semantic_path={
            "used": True,
            "dataset": "workforce",
            "grain": "national",
            "metric_keys": ["gp_fte"],
        },
        table_hint="individual",
    )

    assert payload is not None
    assert payload["metric"] == "gp_fte"
    assert payload["source"] == "planner_v1_live"


def test_build_live_query_plan_rejects_non_allowlisted_metric():
    payload = build_live_query_plan(
        dataset="workforce",
        semantic_request_v9={
            "metrics": ["nurse_fte"],
            "entity_filters": {},
            "group_by": [],
            "time": {"mode": "latest"},
            "transforms": [],
        },
        semantic_path={
            "used": True,
            "dataset": "workforce",
            "grain": "national",
            "metric_keys": ["nurse_fte"],
        },
        table_hint="individual",
    )

    assert payload is None
