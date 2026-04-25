from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import gp_workforce_chatbot_backend_agent_v8 as backend


def test_appointments_query_handoff_for_fresh_dna_rate(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)

    def _unexpected_delegate(*args, **kwargs):
        raise AssertionError("legacy appointments query strategy should not run")

    monkeypatch.setattr(backend, "external_appointments_query_strategy", _unexpected_delegate)

    state = {
        "original_question": "What is the DNA rate in London in the latest month?",
        "question": "What is the DNA rate in London in the latest month?",
        "follow_up_context": {},
        "_hard_intent": "",
        "_intent_result_v1": {"intent": "dna_rate"},
        "worker_plan": {},
        "sql": "SELECT 1",
        "plan": {"intent": "stale"},
    }

    result = backend._appointments_query_strategy(state)

    assert result["sql"] == ""
    assert result["plan"] == {}
    assert result["worker_plan"]["planner_v1_handoff"] == "dna_rate"


def test_appointments_query_handoff_for_fresh_total_appointments(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)

    def _unexpected_delegate(*args, **kwargs):
        raise AssertionError("legacy appointments query strategy should not run")

    monkeypatch.setattr(backend, "external_appointments_query_strategy", _unexpected_delegate)

    state = {
        "original_question": "How many appointments were there in Newcastle South PCN?",
        "question": "How many appointments were there in Newcastle South PCN?",
        "follow_up_context": {},
        "_hard_intent": "",
        "_intent_result_v1": {"intent": "total_appointments"},
        "worker_plan": {},
        "sql": "SELECT 1",
        "plan": {"intent": "stale"},
    }

    result = backend._appointments_query_strategy(state)

    assert result["sql"] == ""
    assert result["plan"] == {}
    assert result["worker_plan"]["planner_v1_handoff"] == "total_appointments"


def test_appointments_query_keeps_legacy_delegate_for_followup(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)

    def _delegate(state, **kwargs):
        state["sql"] = "SELECT delegated"
        state["plan"] = {"intent": "legacy"}
        return state

    monkeypatch.setattr(backend, "external_appointments_query_strategy", _delegate)

    state = {
        "original_question": "What about the DNA rate there?",
        "question": "What about the DNA rate there?",
        "follow_up_context": {"entity_type": "icb", "entity_name": "NHS Greater Manchester ICB"},
        "_hard_intent": "",
        "_intent_result_v1": {"intent": "dna_rate"},
        "worker_plan": {},
        "sql": "",
        "plan": {},
    }

    result = backend._appointments_query_strategy(state)

    assert result["sql"] == "SELECT delegated"
    assert result["plan"] == {"intent": "legacy"}
    assert result["worker_plan"] == {}


def test_appointments_query_handoff_for_fresh_face_to_face_share(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)

    def _unexpected_delegate(*args, **kwargs):
        raise AssertionError("legacy appointments query strategy should not run")

    monkeypatch.setattr(backend, "external_appointments_query_strategy", _unexpected_delegate)

    state = {
        "original_question": "What share of appointments were face to face in NHS Greater Manchester ICB?",
        "question": "What share of appointments were face to face in NHS Greater Manchester ICB?",
        "follow_up_context": {},
        "_hard_intent": "",
        "_intent_result_v1": {"intent": "face_to_face_share"},
        "worker_plan": {},
        "sql": "SELECT 1",
        "plan": {"intent": "stale"},
    }

    result = backend._appointments_query_strategy(state)

    assert result["sql"] == ""
    assert result["plan"] == {}
    assert result["worker_plan"]["planner_v1_handoff"] == "face_to_face_share"


def test_shadow_classifier_recognises_bare_face_to_face_share():
    result = backend._classify_intent_shadow_fast("Face to face share nationally", {})

    assert result["intent"] == "face_to_face_share"
    assert result["dataset_hint"] == "appointments"
    assert result["metric_hint"] == "face_to_face_share"


def test_appointments_query_handoff_for_bare_face_to_face_share(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)

    def _unexpected_delegate(*args, **kwargs):
        raise AssertionError("legacy appointments query strategy should not run")

    monkeypatch.setattr(backend, "external_appointments_query_strategy", _unexpected_delegate)

    state = {
        "original_question": "Face to face share nationally",
        "question": "Face to face share nationally",
        "follow_up_context": {},
        "_hard_intent": "",
        "_intent_result_v1": backend._classify_intent_shadow_fast("Face to face share nationally", {}),
        "worker_plan": {},
        "sql": "SELECT 1",
        "plan": {"intent": "stale"},
    }

    result = backend._appointments_query_strategy(state)

    assert result["sql"] == ""
    assert result["plan"] == {}
    assert result["worker_plan"]["planner_v1_handoff"] == "face_to_face_share"


def test_appointments_query_handoff_for_fresh_telephone_share(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)

    def _unexpected_delegate(*args, **kwargs):
        raise AssertionError("legacy appointments query strategy should not run")

    monkeypatch.setattr(backend, "external_appointments_query_strategy", _unexpected_delegate)

    state = {
        "original_question": "What share of appointments were by telephone in NHS Greater Manchester ICB?",
        "question": "What share of appointments were by telephone in NHS Greater Manchester ICB?",
        "follow_up_context": {},
        "_hard_intent": "",
        "_intent_result_v1": {"intent": "telephone_share"},
        "worker_plan": {},
        "sql": "SELECT 1",
        "plan": {"intent": "stale"},
    }

    result = backend._appointments_query_strategy(state)

    assert result["sql"] == ""
    assert result["plan"] == {}
    assert result["worker_plan"]["planner_v1_handoff"] == "telephone_share"


def test_appointments_query_keeps_legacy_delegate_for_generic_mode_breakdown(monkeypatch):
    monkeypatch.setattr(backend, "USE_SEMANTIC_PATH", True)

    def _delegate(state, **kwargs):
        state["sql"] = "SELECT delegated"
        state["plan"] = {"intent": "legacy"}
        return state

    monkeypatch.setattr(backend, "external_appointments_query_strategy", _delegate)

    state = {
        "original_question": "Show appointment mode breakdown in NHS Greater Manchester ICB",
        "question": "Show appointment mode breakdown in NHS Greater Manchester ICB",
        "follow_up_context": {},
        "_hard_intent": "",
        "_intent_result_v1": {"intent": "face_to_face_share"},
        "worker_plan": {},
        "sql": "",
        "plan": {},
    }

    result = backend._appointments_query_strategy(state)

    assert result["sql"] == "SELECT delegated"
    assert result["plan"] == {"intent": "legacy"}
    assert result["worker_plan"] == {}
