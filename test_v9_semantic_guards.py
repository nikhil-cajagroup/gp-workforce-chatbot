from __future__ import annotations

import gp_workforce_chatbot_backend_agent_v8 as backend
from v9_compiler import compile_request
from v9_parser import parse_semantic_request_deterministic


def test_semantic_clarification_for_my_practice_without_context() -> None:
    state = {
        "original_question": "How many appointments were there within 2 weeks at my practice?",
        "question": "How many appointments were there within 2 weeks at my practice?",
        "follow_up_context": None,
    }
    clarification = backend._semantic_clarification_question(state, "appointments")
    assert clarification == "Which practice do you mean? Share the exact practice name or ODS code."


def test_semantic_clarification_skips_placeholder_when_practice_context_exists() -> None:
    state = {
        "original_question": "How many appointments were there within 2 weeks at my practice?",
        "question": "How many appointments were there within 2 weeks at my practice?",
        "follow_up_context": {"entity_type": "practice", "entity_name": "Keele Practice"},
    }
    clarification = backend._semantic_clarification_question(state, "appointments")
    assert clarification is None


def test_semantic_clarification_uses_ambiguous_practice_resolution(monkeypatch) -> None:
    monkeypatch.setattr(
        backend,
        "_resolve_v9_practice_reference",
        lambda question, dataset_hint, metric_key: {
            "status": "ambiguous",
            "clarification_question": "I found multiple matching practices. Did you mean High Street Surgery (A12345) or High Street Surgery (B12345)?",
        },
    )
    state = {
        "original_question": "Show appointments at High Street Surgery",
        "question": "Show appointments at High Street Surgery",
        "follow_up_context": None,
    }
    clarification = backend._semantic_clarification_question(state, "appointments")
    assert clarification is not None
    assert "multiple matching practices" in clarification


def test_appointments_hcp_filter_requires_explicit_hcp_language_for_gp() -> None:
    assert backend._appointments_hcp_filter(
        "How many GP appointments were made at Keele Practice in the latest month?"
    ) is None
    assert backend._appointments_hcp_filter(
        "How many appointments were with a GP at Keele Practice in the latest month?"
    ) == "GP"


def test_appointments_hcp_filter_still_detects_other_explicit_hcps() -> None:
    assert backend._appointments_hcp_filter(
        "How many nurse appointments were there in NHS Kent and Medway ICB?"
    ) == "Nurse"
    assert backend._appointments_hcp_filter(
        "How many appointments were with pharmacists in NHS Kent and Medway ICB?"
    ) == "Other Practice staff"


def test_semantic_clarification_for_unlinked_cross_dataset_practice(monkeypatch) -> None:
    monkeypatch.setattr(backend, "_cross_practice_code_is_linked", lambda practice_code: False)

    class DummyRequest:
        metrics = ["appointments_per_patient"]
        entity_filters = {"practice_code": "M83670"}

    state = {
        "original_question": "What is appointments per patient at practice M83670?",
        "question": "What is appointments per patient at practice M83670?",
        "follow_up_context": None,
    }
    clarification = backend._semantic_clarification_question(state, "appointments", semantic_request=DummyRequest())
    assert clarification is not None
    assert "cross-dataset practice metric" in clarification


def test_cross_icb_metric_uses_normalized_icb_join() -> None:
    request = parse_semantic_request_deterministic(
        "Top 5 appointments per GP headcount by ICB",
        dataset_hint="appointments",
    )
    assert request is not None
    compiled = compile_request(request)
    sql = compiled.sql.lower()
    assert "icb_join_key" in sql
    assert "join wf on appt.icb_join_key = wf.icb_join_key" in sql
