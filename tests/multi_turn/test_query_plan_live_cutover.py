from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import gp_workforce_chatbot_backend_agent_v8 as backend
from gp_workforce_chatbot_backend_agent_v8 import _try_query_plan_v1_live


def test_try_query_plan_v1_live_accepts_gp_fte_grouped_question(monkeypatch):
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
    assert state["_query_plan_v1"]["metric"] == "gp_fte"
    assert state["_query_plan_v1"]["source"] == "planner_v1_live"
    assert state["worker_plan"]["planner_v1_live"] is True
    assert state["semantic_path"]["used"] is True
    assert state["_query_plan_v1"]["grain"] == "icb"


def test_try_query_plan_v1_live_rejects_non_allowlisted_semantic_metric(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "workforce",
        "follow_up_context": {},
        "_hard_intent": "",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "Total nurse FTE nationally in the latest month")

    assert triggered is False


def test_try_query_plan_v1_live_accepts_gp_headcount_question(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "workforce",
        "follow_up_context": {},
        "_hard_intent": "",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "Show GP headcount by ICB")

    assert triggered is True
    assert state["_query_plan_v1"]["metric"] == "gp_headcount"
    assert state["_query_plan_v1"]["source"] == "planner_v1_live"
    assert state["_query_plan_v1"]["grain"] == "icb"


def test_try_query_plan_v1_live_accepts_dna_rate_question(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "appointments",
        "follow_up_context": {},
        "_hard_intent": "",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "What is the DNA rate in London in the latest month?")

    assert triggered is True
    assert state["_query_plan_v1"]["metric"] == "dna_rate"
    assert state["_query_plan_v1"]["source"] == "planner_v1_live"


def test_try_query_plan_v1_live_accepts_patients_per_gp_question(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "workforce",
        "follow_up_context": {},
        "_hard_intent": "",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "Patients per GP in P82001")

    assert triggered is True
    assert state["_query_plan_v1"]["metric"] == "patients_per_gp"
    assert state["_query_plan_v1"]["source"] == "planner_v1_live"
    assert state["_query_plan_v1"]["grain"] == "practice"


def test_try_query_plan_v1_live_accepts_registered_patients_question(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "workforce",
        "follow_up_context": {},
        "_hard_intent": "",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "Registered patients in P82001")

    assert triggered is True
    assert state["_query_plan_v1"]["metric"] == "registered_patients"
    assert state["_query_plan_v1"]["source"] == "planner_v1_live"
    assert state["_query_plan_v1"]["grain"] == "practice"


def test_try_query_plan_v1_live_accepts_face_to_face_share_question(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "appointments",
        "follow_up_context": {},
        "_hard_intent": "",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(
        state,
        "What share of appointments were face to face in NHS Greater Manchester ICB in the latest month?",
    )

    assert triggered is True
    assert state["_query_plan_v1"]["metric"] == "face_to_face_share"
    assert state["_query_plan_v1"]["source"] == "planner_v1_live"
    assert state["_query_plan_v1"]["grain"] == "icb"
    assert state["worker_plan"]["planner_v1_live"] is True


def test_try_query_plan_v1_live_accepts_appointments_per_gp_fte_question(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "cross_dataset",
        "follow_up_context": {},
        "_hard_intent": "",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(
        state,
        "What are appointments per GP FTE in NHS Greater Manchester ICB?",
    )

    assert triggered is True
    assert state["_query_plan_v1"]["metric"] == "appointments_per_gp_fte"
    assert state["_query_plan_v1"]["source"] == "planner_v1_live"
    assert state["_query_plan_v1"]["grain"] == "icb"
    assert state["semantic_path"]["dataset"] == "cross"


# -- Phase 4a: widen hard_intent gate for a small safe allowlist --------------


def test_try_query_plan_v1_live_admits_safe_hard_intent_patients_per_gp(monkeypatch):
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
    assert state["_query_plan_v1"]["metric"] == "patients_per_gp"
    assert state["_query_plan_v1"]["source"] == "planner_v1_live"
    assert state["worker_plan"]["planner_v1_live"] is True
    assert (
        state["worker_plan"]["planner_v1_live_admitted_hard_intent"]
        == "patients_per_gp"
    )


def test_try_query_plan_v1_live_rejects_unsafe_hard_intent(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "workforce",
        "follow_up_context": {},
        # Not in SAFE_HARD_INTENTS_FOR_PLANNER_V1: must still defer to the
        # deterministic-override path. practice_staff_breakdown is a good
        # negative example because its override emits multi-row staff-group
        # SQL that planner-v1 live does not currently reproduce.
        "_hard_intent": "practice_staff_breakdown",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "Full staff breakdown at P82001")

    assert triggered is False
    assert "_query_plan_v1" not in state
    # worker_plan now carries the structured rejection outcome for
    # telemetry, but no planner-v1 admission marker.
    assert "planner_v1_live" not in state["worker_plan"]
    assert (
        state["worker_plan"]["planner_v1_live_outcome"]["outcome"]
        == "rejected_unsafe_hard_intent"
    )


def test_try_query_plan_v1_live_still_rejects_followups_even_with_safe_hard_intent(
    monkeypatch,
):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "workforce",
        # Follow-up context must always take precedence and block planner-v1
        # live regardless of how safe the hard intent is.
        "follow_up_context": {
            "entity_type": "practice",
            "entity_name": "Keele Practice",
        },
        "_hard_intent": "patients_per_gp",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "Patients per GP there")

    assert triggered is False
    assert "_query_plan_v1" not in state


def test_try_query_plan_v1_live_admits_safe_hard_intent_practice_patient_count(
    monkeypatch,
):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "workforce",
        "follow_up_context": {},
        "_hard_intent": "practice_patient_count",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(
        state, "How many patients are registered at P82001?"
    )

    assert triggered is True
    assert state["_query_plan_v1"]["metric"] == "registered_patients"
    assert state["_query_plan_v1"]["source"] == "planner_v1_live"
    assert state["_query_plan_v1"]["entity_filters"] == {"practice_code": "P82001"}
    assert (
        state["worker_plan"]["planner_v1_live_admitted_hard_intent"]
        == "practice_patient_count"
    )


def test_try_query_plan_v1_live_admits_safe_hard_intent_practice_gp_count(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "workforce",
        "follow_up_context": {},
        "_hard_intent": "practice_gp_count",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "How many GPs at P82001?")

    assert triggered is True
    assert state["_query_plan_v1"]["metric"] == "gp_headcount"
    assert state["_query_plan_v1"]["source"] == "planner_v1_live"
    assert state["_query_plan_v1"]["entity_filters"] == {"practice_code": "P82001"}
    assert (
        state["worker_plan"]["planner_v1_live_admitted_hard_intent"]
        == "practice_gp_count"
    )


def test_try_query_plan_v1_live_admits_safe_hard_intent_practice_gp_count_soft(
    monkeypatch,
):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)
    state = {
        "dataset": "workforce",
        "follow_up_context": {},
        "_hard_intent": "practice_gp_count_soft",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(state, "Number of GPs at practice P82001")

    assert triggered is True
    assert state["_query_plan_v1"]["metric"] == "gp_headcount"
    assert (
        state["worker_plan"]["planner_v1_live_admitted_hard_intent"]
        == "practice_gp_count_soft"
    )


def test_try_query_plan_v1_live_rejects_safe_hard_intent_when_scope_unresolved(
    monkeypatch,
):
    """Safety net: if the override path kicked in because the question
    references a specific practice, planner-v1 live must confirm v9 actually
    resolved that scope. Otherwise we'd quietly answer a national aggregate.

    We simulate this by stubbing the v9 compile step to return a semantic
    request with empty entity_filters and no group_by — the shape you'd see
    if the practice-name resolver came back empty on a live deploy.
    """

    from types import SimpleNamespace

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

    def _fake_compile(question, dataset_hint=""):
        return (fake_request, fake_compiled)

    monkeypatch.setattr(backend, "_compile_v9_semantic_request", _fake_compile)

    state = {
        "dataset": "workforce",
        "question": "How many patients are registered at Unknown Practice?",
        "follow_up_context": {},
        "_hard_intent": "practice_patient_count",
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
    }

    triggered = _try_query_plan_v1_live(
        state, "How many patients are registered at Unknown Practice?"
    )

    assert triggered is False
    assert "_query_plan_v1" not in state
    assert "planner_v1_live" not in state["worker_plan"]
    assert (
        state["worker_plan"]["planner_v1_live_outcome"]["outcome"]
        == "rejected_scope_unresolved"
    )


