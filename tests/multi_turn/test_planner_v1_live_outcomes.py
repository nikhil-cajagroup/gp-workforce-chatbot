"""Structured outcome coverage for planner-v1 live admission decisions.

Every exit path of `_try_query_plan_v1_live` must record a typed outcome
into `worker_plan["planner_v1_live_outcome"]`. Without that, we cannot
measure whether legacy override branches are dead code before deleting
them. These tests lock down one example per category.
"""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from types import SimpleNamespace

import gp_workforce_chatbot_backend_agent_v8 as backend
from gp_workforce_chatbot_backend_agent_v8 import _try_query_plan_v1_live


def _outcome(state):
    return dict((state.get("worker_plan") or {}).get("planner_v1_live_outcome") or {})


def test_outcome_admitted_for_national_gp_fte(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "workforce",
        "follow_up_context": {},
        "_hard_intent": "",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "Show GP FTE by ICB")

    assert triggered is True
    outcome = _outcome(state)
    assert outcome["outcome"] == "admitted"
    assert outcome["metric"] == "gp_fte"
    # No hard intent → admitted (not admitted_hard_intent).
    assert "hard_intent" not in outcome


def test_outcome_admitted_hard_intent_for_patients_per_gp(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "workforce",
        "follow_up_context": {},
        "_hard_intent": "patients_per_gp",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "Patients per GP in P82001")

    assert triggered is True
    outcome = _outcome(state)
    assert outcome["outcome"] == "admitted_hard_intent"
    assert outcome["hard_intent"] == "patients_per_gp"
    assert outcome["metric"] == "patients_per_gp"


def test_outcome_rejected_follow_up(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "workforce",
        "follow_up_context": {
            "entity_type": "practice",
            "entity_name": "Keele Practice",
        },
        "_hard_intent": "",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "And the GPs there?")

    assert triggered is False
    assert _outcome(state) == {"outcome": "rejected_follow_up"}


def test_outcome_rejected_unsafe_hard_intent(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "workforce",
        "follow_up_context": {},
        "_hard_intent": "practice_staff_breakdown",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "Full staff breakdown at P82001")

    assert triggered is False
    outcome = _outcome(state)
    assert outcome["outcome"] == "rejected_unsafe_hard_intent"
    assert outcome["hard_intent"] == "practice_staff_breakdown"


def test_outcome_rejected_v9_compile_failed(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    monkeypatch.setattr(
        backend,
        "_compile_v9_semantic_request",
        lambda question, dataset_hint="": None,
    )
    state = {
        "dataset": "workforce",
        "follow_up_context": {},
        "_hard_intent": "",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "Something v9 cannot parse")

    assert triggered is False
    assert _outcome(state) == {"outcome": "rejected_v9_compile_failed"}


def test_outcome_rejected_gate_confidence_low(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)

    fake_request = SimpleNamespace(
        metrics=["gp_fte"],
        entity_filters={},
        group_by=[],
        time={},
        transforms=[],
        compare=None,
        confidence="low",
        clarification_needed=False,
    )
    fake_compiled = SimpleNamespace(
        dataset="workforce",
        grain="national",
        metric_keys=["gp_fte"],
        sql="SELECT 1",
    )
    monkeypatch.setattr(
        backend,
        "_compile_v9_semantic_request",
        lambda question, dataset_hint="": (fake_request, fake_compiled),
    )

    state = {
        "dataset": "workforce",
        "question": "Ambiguous question",
        "follow_up_context": {},
        "_hard_intent": "",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "Ambiguous question")

    assert triggered is False
    outcome = _outcome(state)
    assert outcome["outcome"] == "rejected_gate"
    assert outcome["gate_reason"] == "confidence_low"


def test_outcome_rejected_scope_unresolved(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)

    fake_request = SimpleNamespace(
        metrics=["registered_patients"],
        entity_filters={},
        group_by=[],
        time={},
        transforms=[],
        compare=None,
        confidence="high",
        clarification_needed=False,
    )
    fake_compiled = SimpleNamespace(
        dataset="workforce",
        grain="national",
        metric_keys=["registered_patients"],
        sql="SELECT 1",
    )
    monkeypatch.setattr(
        backend,
        "_compile_v9_semantic_request",
        lambda question, dataset_hint="": (fake_request, fake_compiled),
    )

    state = {
        "dataset": "workforce",
        "question": "How many patients at Unknown Practice?",
        "follow_up_context": {},
        "_hard_intent": "practice_patient_count",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(
        state, "How many patients at Unknown Practice?"
    )

    assert triggered is False
    outcome = _outcome(state)
    assert outcome["outcome"] == "rejected_scope_unresolved"
    assert outcome["hard_intent"] == "practice_patient_count"


def test_outcome_rejected_metric_not_live(monkeypatch):
    """A metric v9 compiles cleanly but that isn't in
    LIVE_QUERY_PLAN_V1_METRICS should record `rejected_metric_not_live`.

    Uses the real v9 parser so the semantic_request has proper typed
    submodels (time/transforms/compare) — a SimpleNamespace stub fails
    inside `semantic_request_to_dict`.
    """
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)

    state = {
        "dataset": "workforce",
        "follow_up_context": {},
        "_hard_intent": "",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    # Nurse FTE is a supported v9 metric but is NOT in
    # LIVE_QUERY_PLAN_V1_METRICS, so planner-v1 live must reject it at
    # the `build_live_query_plan` step and record the reason.
    triggered = _try_query_plan_v1_live(
        state, "Total nurse FTE nationally in the latest month"
    )

    assert triggered is False
    outcome = _outcome(state)
    assert outcome["outcome"] == "rejected_metric_not_live"
    assert outcome["metric"] == "nurse_fte"
