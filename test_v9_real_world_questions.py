from __future__ import annotations

from v9_parser import parse_semantic_request_deterministic, semantic_request_to_dict


def test_real_world_appointments_total_by_icb() -> None:
    request = parse_semantic_request_deterministic(
        "How many appointments were there in NHS Kent and Medway ICB?",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["entity_filters"]["icb_name"] == "NHS Kent and Medway ICB"


def test_real_world_dna_rate_by_region() -> None:
    request = parse_semantic_request_deterministic(
        "What is the DNA rate in London region?",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["dna_rate"]
    assert payload["entity_filters"]["region_name"] == "London"


def test_real_world_named_practice_appointments_with_resolver() -> None:
    request = parse_semantic_request_deterministic(
        "How many GP appointments were made at Keele Practice in the latest month?",
        dataset_hint="appointments",
        practice_name_resolver=lambda question, dataset_hint, metric: "P82001",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["entity_filters"]["practice_code"] == "P82001"


def test_real_world_patients_per_gp_ranking() -> None:
    request = parse_semantic_request_deterministic(
        "Which ICB has the highest patients per GP ratio?",
        dataset_hint="workforce",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["patients_per_gp"]
    assert payload["group_by"] == ["icb_name"]
    assert any(t["type"] == "topn" for t in payload["transforms"])


def test_real_world_nurse_fte_by_region() -> None:
    request = parse_semantic_request_deterministic(
        "Show nurse FTE by region",
        dataset_hint="workforce",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["nurse_fte"]
    assert payload["group_by"] == ["region_name"]


def test_real_world_face_to_face_share_supported() -> None:
    request = parse_semantic_request_deterministic(
        "What share of appointments were face to face in NHS Greater Manchester ICB?",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["face_to_face_share"]
    assert payload["entity_filters"]["icb_name"] == "NHS Greater Manchester ICB"


def test_real_world_booking_window_question_requests_clarification() -> None:
    request = parse_semantic_request_deterministic(
        "How many appointments were there within 2 weeks at my practice?",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["within_2_weeks_appointments"]
    assert payload["clarification_needed"] is True
    assert payload["confidence"] == "low"


def test_real_world_hcp_type_breakdown_supported() -> None:
    request = parse_semantic_request_deterministic(
        "Show appointments by HCP type in NHS Greater Manchester ICB",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["group_by"] == ["hcp_type"]


def test_real_world_appointment_mode_breakdown_supported() -> None:
    request = parse_semantic_request_deterministic(
        "Show appointment mode breakdown in NHS Greater Manchester ICB",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["group_by"] == ["appt_mode"]


def test_real_world_booking_lead_time_breakdown_supported() -> None:
    request = parse_semantic_request_deterministic(
        "Show booking lead time breakdown in NHS Greater Manchester ICB",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["group_by"] == ["time_between_book_and_appt"]
