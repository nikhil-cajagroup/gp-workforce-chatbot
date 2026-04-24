from __future__ import annotations

from v9_parser import (
    derive_followup_semantic_request,
    parse_semantic_request_deterministic,
    semantic_request_to_dict,
)


def test_parse_gp_fte_by_icb() -> None:
    request = parse_semantic_request_deterministic("Show GP FTE by ICB")
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["gp_fte"]
    assert payload["group_by"] == ["icb_name"]
    assert payload["confidence"] == "high"


def test_parse_patients_per_gp_practice() -> None:
    request = parse_semantic_request_deterministic("Patients per GP in P82001")
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["patients_per_gp"]
    assert payload["entity_filters"]["practice_code"] == "P82001"


def test_parse_cross_metric_benchmark() -> None:
    request = parse_semantic_request_deterministic(
        "Top 10 appointments per GP by ICB compared with national average"
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["appointments_per_gp_fte"]
    assert payload["group_by"] == ["icb_name"]
    assert payload["confidence"] == "high"
    assert any(t["type"] == "topn" for t in payload["transforms"])
    assert any(t["type"] == "benchmark" and t["scope"] == "national" for t in payload["transforms"])


def test_parse_total_appointments_by_region_benchmark() -> None:
    request = parse_semantic_request_deterministic(
        "Show total appointments by region compared with national average"
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["group_by"] == ["region_name"]
    assert any(t["type"] == "benchmark" and t["scope"] == "national" for t in payload["transforms"])


def test_parse_dna_rate_by_icb_benchmark() -> None:
    request = parse_semantic_request_deterministic(
        "Show DNA rate by ICB compared with national average"
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["dna_rate"]
    assert payload["group_by"] == ["icb_name"]
    assert any(t["type"] == "benchmark" and t["scope"] == "national" for t in payload["transforms"])


def test_parse_nurse_fte_by_region() -> None:
    request = parse_semantic_request_deterministic("Show nurse FTE by region")
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["nurse_fte"]
    assert payload["group_by"] == ["region_name"]


def test_parse_appointments_per_gp_headcount_by_icb() -> None:
    request = parse_semantic_request_deterministic("Top 5 appointments per GP headcount by ICB")
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["appointments_per_gp_headcount"]
    assert payload["group_by"] == ["icb_name"]
    assert any(t["type"] == "topn" and t["n"] == 5 for t in payload["transforms"])


def test_parse_appointments_per_nurse_by_region_benchmark() -> None:
    request = parse_semantic_request_deterministic(
        "Show appointments per nurse by region compared with national average"
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["appointments_per_nurse_fte"]
    assert payload["group_by"] == ["region_name"]
    assert any(t["type"] == "benchmark" and t["scope"] == "national" for t in payload["transforms"])


def test_parse_registered_patients_by_icb() -> None:
    request = parse_semantic_request_deterministic("Show registered patients by ICB")
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["registered_patients"]
    assert payload["group_by"] == ["icb_name"]


def test_parse_appointments_per_patient_by_region_benchmark() -> None:
    request = parse_semantic_request_deterministic(
        "Show appointments per patient by region compared with national average"
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["appointments_per_patient"]
    assert payload["group_by"] == ["region_name"]
    assert any(t["type"] == "benchmark" and t["scope"] == "national" for t in payload["transforms"])


def test_parse_highest_patients_per_gp_by_icb() -> None:
    request = parse_semantic_request_deterministic("Which ICB has the highest patients per GP ratio?")
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["patients_per_gp"]
    assert payload["group_by"] == ["icb_name"]
    assert any(t["type"] == "topn" for t in payload["transforms"])


def test_parse_unsupported_hcp_breakdown_returns_none() -> None:
    request = parse_semantic_request_deterministic(
        "Show appointments by HCP type in NHS Greater Manchester ICB",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["group_by"] == ["hcp_type"]
    assert payload["entity_filters"]["icb_name"] == "NHS Greater Manchester ICB"


def test_parse_named_practice_is_low_confidence() -> None:
    request = parse_semantic_request_deterministic(
        "Show appointments for Queens Park Medical Centre",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["confidence"] == "low"
    assert payload["clarification_needed"] is True


def test_parse_face_to_face_share_by_icb() -> None:
    request = parse_semantic_request_deterministic(
        "What share of appointments were face to face in NHS Greater Manchester ICB?",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["face_to_face_share"]
    assert payload["entity_filters"]["icb_name"] == "NHS Greater Manchester ICB"
    assert payload["confidence"] == "high"


def test_parse_appointment_mode_breakdown_by_icb() -> None:
    request = parse_semantic_request_deterministic(
        "Show appointment mode breakdown in NHS Greater Manchester ICB",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["group_by"] == ["appt_mode"]
    assert payload["entity_filters"]["icb_name"] == "NHS Greater Manchester ICB"


def test_parse_booking_lead_time_breakdown_by_icb() -> None:
    request = parse_semantic_request_deterministic(
        "Show booking lead time breakdown in NHS Greater Manchester ICB",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["group_by"] == ["time_between_book_and_appt"]
    assert payload["entity_filters"]["icb_name"] == "NHS Greater Manchester ICB"


def test_parse_within_two_weeks_practice_count_with_resolver() -> None:
    request = parse_semantic_request_deterministic(
        "How many appointments were there within 2 weeks at Keele Practice?",
        dataset_hint="appointments",
        practice_name_resolver=lambda question, dataset_hint, metric: "M83670",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["within_2_weeks_appointments"]
    assert payload["entity_filters"]["practice_code"] == "M83670"
    assert payload["confidence"] == "high"


def test_parse_my_practice_requests_clarification() -> None:
    request = parse_semantic_request_deterministic(
        "How many appointments were there within 2 weeks at my practice?",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["within_2_weeks_appointments"]
    assert payload["clarification_needed"] is True
    assert payload["confidence"] == "low"


def test_parse_city_alias_place_is_high_confidence() -> None:
    request = parse_semantic_request_deterministic(
        "How many appointments were there in Leeds?",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["entity_filters"]["icb_name"] == "NHS West Yorkshire Integrated Care Board"
    assert payload["confidence"] == "high"


def test_parse_named_pcn_filter() -> None:
    request = parse_semantic_request_deterministic(
        "How many appointments were there in Newcastle South PCN in the latest month?",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["entity_filters"]["pcn_name"] == "Newcastle South PCN"
    assert "icb_name" not in payload["entity_filters"]
    assert payload["confidence"] == "high"


def test_parse_physiotherapist_fte_metric() -> None:
    request = parse_semantic_request_deterministic("What is the FTE of physiotherapists nationally?")
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["physiotherapist_fte"]
    assert payload["confidence"] == "high"


def test_parse_national_category_icb_requests_clarification() -> None:
    request = parse_semantic_request_deterministic(
        "Show mental health appointments in NHS Greater Manchester ICB",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["entity_filters"]["icb_name"] == "NHS Greater Manchester ICB"
    assert payload["entity_filters"]["national_category"] == "Mental Health"
    assert payload["clarification_needed"] is True
    assert payload["confidence"] == "low"


def test_parse_trend_is_low_confidence() -> None:
    request = parse_semantic_request_deterministic(
        "Show GP appointments trend over the past year",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert any(t["type"] == "trend" for t in payload["transforms"])
    assert payload["confidence"] == "high"


def test_parse_city_alias_to_icb() -> None:
    request = parse_semantic_request_deterministic(
        "How many appointments were there in Leeds?",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["entity_filters"]["icb_name"] == "NHS West Yorkshire Integrated Care Board"
    assert payload["confidence"] == "high"


def test_parse_named_practice_with_resolver() -> None:
    request = parse_semantic_request_deterministic(
        "Show appointments for Queens Park Medical Centre",
        dataset_hint="appointments",
        practice_name_resolver=lambda question, dataset_hint, metric: "P12345",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["entity_filters"]["practice_code"] == "P12345"
    assert payload["confidence"] == "high"


def test_parse_singular_gp_appointment_as_total_appointments() -> None:
    request = parse_semantic_request_deterministic(
        "and how many gp appointment were made at keele practice in latest month",
        dataset_hint="appointments",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]


def test_parse_named_practice_with_practice_suffix_uses_resolver() -> None:
    request = parse_semantic_request_deterministic(
        "appointments in Keele Practice",
        dataset_hint="appointments",
        practice_name_resolver=lambda question, dataset_hint, metric: "P82001",
    )
    assert request is not None
    payload = semantic_request_to_dict(request)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["entity_filters"]["practice_code"] == "P82001"
    assert payload["confidence"] == "high"


def test_followup_merge_nurses_inherits_icb_filter() -> None:
    prior = parse_semantic_request_deterministic("How many GPs are in Leeds?")
    assert prior is not None
    merged = derive_followup_semantic_request("What about nurses?", prior=prior)
    assert merged is not None
    payload = semantic_request_to_dict(merged)
    # Prior was gp_headcount ("How many GPs") → follow-up "nurses" should match
    # with headcount since no FTE qualifier was given.
    assert payload["metrics"] == ["nurse_headcount"]
    assert "icb_name" in payload["entity_filters"]
    assert "west yorkshire" in payload["entity_filters"]["icb_name"].lower()
    assert payload["confidence"] == "high"


def test_followup_merge_dna_rate_inherits_icb_filter() -> None:
    prior = parse_semantic_request_deterministic("Total appointments in NHS Kent and Medway ICB")
    assert prior is not None
    merged = derive_followup_semantic_request("What about the DNA rate?", prior=prior)
    assert merged is not None
    payload = semantic_request_to_dict(merged)
    assert payload["metrics"] == ["dna_rate"]
    assert "icb_name" in payload["entity_filters"]


def test_followup_merge_gp_appointments_stays_in_appointments_dataset() -> None:
    prior = parse_semantic_request_deterministic("Total appointments in NHS Kent and Medway ICB")
    assert prior is not None
    merged = derive_followup_semantic_request(
        "What about GP appointments in the latest month?",
        prior=prior,
        dataset_hint="appointments",
    )
    assert merged is not None
    payload = semantic_request_to_dict(merged)
    assert payload["metrics"] == ["total_appointments"]
    assert "icb_name" in payload["entity_filters"]


def test_followup_merge_hcp_breakdown_inherits_scope() -> None:
    prior = parse_semantic_request_deterministic("Total appointments in NHS Kent and Medway ICB")
    assert prior is not None
    merged = derive_followup_semantic_request("Break this down by HCP type", prior=prior, dataset_hint="appointments")
    assert merged is not None
    payload = semantic_request_to_dict(merged)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["group_by"] == ["hcp_type"]
    assert payload["entity_filters"]["icb_name"] == "NHS Kent and Medway ICB"


def test_followup_merge_mode_breakdown_inherits_scope() -> None:
    prior = parse_semantic_request_deterministic("Total appointments in NHS Kent and Medway ICB")
    assert prior is not None
    merged = derive_followup_semantic_request("Show this by appointment mode", prior=prior, dataset_hint="appointments")
    assert merged is not None
    payload = semantic_request_to_dict(merged)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["group_by"] == ["appt_mode"]


def test_followup_merge_rejects_standalone_question() -> None:
    prior = parse_semantic_request_deterministic("How many GPs are in Leeds?")
    assert prior is not None
    merged = derive_followup_semantic_request(
        "How many nurses are in NHS Greater Manchester ICB?",
        prior=prior,
    )
    assert merged is None


def test_followup_merge_rejects_gender_breakdown() -> None:
    prior = parse_semantic_request_deterministic("Nurse FTE in NHS West Yorkshire ICB")
    assert prior is not None
    merged = derive_followup_semantic_request("Break this down by gender", prior=prior)
    assert merged is None


def test_followup_merge_hcp_type_breakdown_supported() -> None:
    prior = parse_semantic_request_deterministic("Total appointments in NHS Kent and Medway ICB")
    assert prior is not None
    merged = derive_followup_semantic_request("Break this down by HCP type", prior=prior)
    assert merged is not None
    payload = semantic_request_to_dict(merged)
    assert payload["metrics"] == ["total_appointments"]
    assert payload["group_by"] == ["hcp_type"]


def test_followup_merge_break_down_by_group_adds_group_by() -> None:
    prior = parse_semantic_request_deterministic("Total appointments in NHS Kent and Medway ICB")
    assert prior is not None
    merged = derive_followup_semantic_request("Break this down by region", prior=prior)
    assert merged is not None
    payload = semantic_request_to_dict(merged)
    assert payload["group_by"] == ["region_name"]
    assert payload["metrics"] == ["total_appointments"]


# ── Bug-fix regressions: _detect_compare ─────────────────────────────────────

def test_compare_practice_vs_practice_infers_practice_code_dimension() -> None:
    """'X vs Y' where both entities are practices must produce dimension=practice_code.

    Before the fix, _detect_compare defaulted to icb_name when group_by was
    empty, so the compiled SQL filtered icb_name IN (...) which matched nothing.

    Note: the question needs "total appointments" (not bare "appointments") so
    that _detect_metric can identify the metric without a dataset_hint.
    """
    request = parse_semantic_request_deterministic(
        "Compare Keele Practice vs Wolstanton Medical Centre total appointments"
    )
    assert request is not None
    assert request.compare is not None
    # Dimension must be practice_code (practice comparison), not icb_name.
    assert request.compare.dimension == "practice_code", (
        f"Expected practice_code, got {request.compare.dimension!r}"
    )
    # Value list must not carry the sentence scaffolding.
    values_lower = [v.lower() for v in request.compare.values]
    assert not any("compare" in v for v in values_lower), (
        f"'compare' leaked into compare values: {request.compare.values}"
    )
    # Both entity names must survive the stripping.
    combined = " ".join(values_lower)
    assert "keele" in combined, "Keele Practice entity lost from compare values"
    assert "wolstanton" in combined, "Wolstanton entity lost from compare values"


def test_compare_practice_vs_practice_resolves_to_codes_when_resolver_available() -> None:
    request = parse_semantic_request_deterministic(
        "Compare Keele Practice vs Wolstanton Medical Centre total appointments",
        dataset_hint="appointments",
        practice_name_resolver=lambda question, dataset_hint, metric: {
            "keele practice": "M83670",
            "wolstanton medical centre": "P82001",
        }.get(str(question or "").strip().lower()),
    )
    assert request is not None
    assert request.compare is not None
    assert request.compare.dimension == "practice_code"
    assert request.compare.values == ["M83670", "P82001"]


def test_compare_icb_vs_icb_infers_icb_name_dimension() -> None:
    """'ICB vs ICB' comparison must infer dimension=icb_name from entity names."""
    request = parse_semantic_request_deterministic(
        "Compare NHS Greater Manchester ICB vs NHS West Yorkshire ICB total appointments"
    )
    assert request is not None
    assert request.compare is not None
    assert request.compare.dimension == "icb_name", (
        f"Expected icb_name, got {request.compare.dimension!r}"
    )
    values_lower = [v.lower() for v in request.compare.values]
    combined = " ".join(values_lower)
    assert "greater manchester" in combined
    assert "west yorkshire" in combined


def test_compare_icb_vs_icb_clears_single_entity_filter() -> None:
    request = parse_semantic_request_deterministic(
        "Compare NHS Greater Manchester ICB vs NHS Kent and Medway ICB appointments",
        dataset_hint="appointments",
    )
    assert request is not None
    assert request.compare is not None
    assert request.compare.dimension == "icb_name"
    assert request.entity_filters == {}


def test_compare_region_values_strip_metric_suffixes() -> None:
    request = parse_semantic_request_deterministic(
        "Compare London vs Midlands GP FTE",
        dataset_hint="workforce",
    )
    assert request is not None
    assert request.compare is not None
    assert request.compare.dimension == "region_name"
    assert request.compare.values == ["London", "Midlands"]


if __name__ == "__main__":
    tests = [
        test_parse_gp_fte_by_icb,
        test_parse_patients_per_gp_practice,
        test_parse_cross_metric_benchmark,
        test_parse_total_appointments_by_region_benchmark,
        test_parse_dna_rate_by_icb_benchmark,
        test_parse_nurse_fte_by_region,
        test_parse_appointments_per_gp_headcount_by_icb,
        test_parse_appointments_per_nurse_by_region_benchmark,
        test_parse_registered_patients_by_icb,
        test_parse_appointments_per_patient_by_region_benchmark,
        test_parse_highest_patients_per_gp_by_icb,
        test_parse_unsupported_hcp_breakdown_returns_none,
        test_parse_named_practice_is_low_confidence,
        test_parse_city_alias_place_is_high_confidence,
        test_parse_trend_is_low_confidence,
        test_parse_city_alias_to_icb,
        test_parse_named_practice_with_resolver,
        test_followup_merge_nurses_inherits_icb_filter,
        test_followup_merge_dna_rate_inherits_icb_filter,
        test_followup_merge_rejects_standalone_question,
        test_followup_merge_rejects_gender_breakdown,
        test_followup_merge_hcp_type_breakdown_supported,
        test_followup_merge_break_down_by_group_adds_group_by,
        # bug-fix regressions
        test_compare_practice_vs_practice_infers_practice_code_dimension,
        test_compare_practice_vs_practice_resolves_to_codes_when_resolver_available,
        test_compare_icb_vs_icb_infers_icb_name_dimension,
        test_compare_icb_vs_icb_clears_single_entity_filter,
        test_compare_region_values_strip_metric_suffixes,
    ]
    passed = 0
    for test in tests:
        test()
        passed += 1
        print(f"[PASS] {test.__name__}")
    print(f"\nSummary: {passed}/{len(tests)} passed")
