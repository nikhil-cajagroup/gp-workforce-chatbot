from __future__ import annotations

from v9_compiler import compile_request
from v9_semantic_types import CompareSpec, SemanticRequest, TransformSpec


def test_gp_headcount_national_latest() -> None:
    compiled = compile_request(SemanticRequest(metrics=["gp_headcount"]))
    sql = compiled.sql.lower()
    assert compiled.dataset == "workforce"
    assert compiled.grain == "national"
    assert 'from "test-gp-workforce".individual' in sql
    assert "count(distinct unique_identifier) as gp_headcount" in sql
    assert "staff_group = 'gp'" in sql
    assert "year = '2025'" in sql
    assert "month = '12'" in sql


def test_gp_fte_by_icb_topn() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["gp_fte"],
            group_by=["icb_name"],
            transforms=[TransformSpec(type="topn", n=5, order="desc")],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "workforce"
    assert compiled.grain == "icb"
    assert "select icb_name, round(sum(fte), 1) as gp_fte" in sql
    assert "group by 1" in sql
    assert "order by gp_fte desc" in sql
    assert "limit 5" in sql


def test_nurse_fte_by_region_benchmark() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["nurse_fte"],
            group_by=["region_name"],
            transforms=[TransformSpec(type="benchmark", scope="national")],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "workforce"
    assert compiled.grain == "region"
    assert "staff_group = 'nurses'" in sql
    assert "national_average" in sql


def test_registered_patients_by_icb_benchmark() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["registered_patients"],
            group_by=["icb_name"],
            transforms=[TransformSpec(type="benchmark", scope="national")],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "workforce"
    assert compiled.grain == "icb"
    assert 'from "test-gp-workforce".practice_detailed' in sql
    assert "total_patients" in sql
    assert "national_average" in sql


def test_patients_per_gp_practice_filter() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["patients_per_gp"],
            entity_filters={"practice_code": "P82001"},
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "workforce"
    assert compiled.grain == "practice"
    assert 'from "test-gp-workforce".practice_detailed' in sql
    assert "total_patients" in sql
    assert "total_gp_fte" in sql
    assert "lower(trim(prac_code)) = lower('p82001')" in sql


def test_appointments_per_gp_fte_by_icb() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["appointments_per_gp_fte"],
            group_by=["icb_name"],
            transforms=[TransformSpec(type="topn", n=10, order="desc")],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "cross"
    assert compiled.grain == "icb"
    assert '"test-gp-appointments".pcn_subicb' in sql
    assert '"test-gp-workforce".individual' in sql
    assert "join wf using (icb_name)" in sql
    assert "appointments_per_gp_fte" in sql
    assert "order by appointments_per_gp_fte desc" in sql


def test_appointments_per_gp_fte_compare_icbs() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["appointments_per_gp_fte"],
            compare=CompareSpec(
                dimension="icb_name",
                values=["NHS Greater Manchester ICB", "NHS West Yorkshire ICB"],
            ),
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "cross"
    assert compiled.grain == "icb"
    assert "greater manchester" in sql
    assert "west yorkshire" in sql
    assert "join wf using (icb_name)" in sql


def test_gp_fte_grouped_benchmark() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["gp_fte"],
            group_by=["icb_name"],
            transforms=[TransformSpec(type="benchmark", scope="national")],
        )
    )
    sql = compiled.sql.lower()
    assert "with result as (" in sql
    assert "avg(gp_fte) over ()" in sql
    assert "national_average" in sql


def test_total_appointments_by_icb_uses_pcn_subicb() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["total_appointments"],
            group_by=["icb_name"],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "appointments"
    assert compiled.grain == "icb"
    assert 'from "test-gp-appointments".pcn_subicb' in sql
    assert "select icb_name, round(sum(cast(count_of_appointments as double)), 0) as total_appointments" in sql


def test_dna_rate_icb_filter_uses_pcn_subicb() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["dna_rate"],
            entity_filters={"icb_name": "NHS Greater Manchester ICB"},
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "appointments"
    assert compiled.grain == "icb"
    assert 'from "test-gp-appointments".pcn_subicb' in sql
    assert "greater manchester" in sql


def test_dna_rate_icb_filter_normalizes_icb_labels() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["dna_rate"],
            entity_filters={"icb_name": "NHS Kent and Medway ICB"},
        )
    )
    sql = compiled.sql.lower()
    assert "integrated care board" in sql
    assert "replace(replace(lower(trim(icb_name))" in sql
    assert "replace(replace(lower(trim('nhs kent and medway icb'))" in sql


def test_total_appointments_by_region_benchmark() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["total_appointments"],
            group_by=["region_name"],
            transforms=[TransformSpec(type="benchmark", scope="national")],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "appointments"
    assert compiled.grain == "region"
    assert 'from "test-gp-appointments".pcn_subicb' in sql
    assert "select region_name, round(sum(cast(count_of_appointments as double)), 0) as total_appointments" in sql
    assert "national_average" in sql


def test_dna_rate_by_icb_benchmark() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["dna_rate"],
            group_by=["icb_name"],
            transforms=[TransformSpec(type="benchmark", scope="national")],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "appointments"
    assert compiled.grain == "icb"
    assert 'from "test-gp-appointments".pcn_subicb' in sql
    assert "dna_rate" in sql
    assert "national_average" in sql


def test_appointments_per_gp_fte_by_region_benchmark() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["appointments_per_gp_fte"],
            group_by=["region_name"],
            transforms=[TransformSpec(type="benchmark", scope="national")],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "cross"
    assert compiled.grain == "region"
    assert '"test-gp-appointments".pcn_subicb' in sql
    assert "join wf using (region_name)" in sql
    assert "appointments_per_gp_fte" in sql
    assert "national_average" in sql


def test_appointments_per_gp_headcount_by_icb() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["appointments_per_gp_headcount"],
            group_by=["icb_name"],
            transforms=[TransformSpec(type="topn", n=5, order="desc")],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "cross"
    assert compiled.grain == "icb"
    assert '"test-gp-appointments".pcn_subicb' in sql
    assert "count(distinct case when staff_group = 'gp' then unique_identifier end) as gp_headcount" in sql
    assert "appointments_per_gp_headcount" in sql
    assert "order by appointments_per_gp_headcount desc" in sql


def test_appointments_per_nurse_fte_by_region_benchmark() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["appointments_per_nurse_fte"],
            group_by=["region_name"],
            transforms=[TransformSpec(type="benchmark", scope="national")],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "cross"
    assert compiled.grain == "region"
    assert '"test-gp-appointments".pcn_subicb' in sql
    assert "staff_group = 'nurses'" in sql
    assert "appointments_per_nurse_fte" in sql
    assert "national_average" in sql


def test_appointments_per_patient_by_region_benchmark() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["appointments_per_patient"],
            group_by=["region_name"],
            transforms=[TransformSpec(type="benchmark", scope="national")],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "cross"
    assert compiled.grain == "region"
    assert '"test-gp-appointments".pcn_subicb' in sql
    assert '"test-gp-workforce".practice_detailed' in sql
    assert "registered_patients" in sql
    assert "appointments_per_patient" in sql
    assert "national_average" in sql


def test_total_appointments_trend_last_12_months() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["total_appointments"],
            transforms=[TransformSpec(type="trend", n=12)],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "appointments"
    assert "from \"test-gp-appointments\".practice" in sql
    assert "select year, month, round(sum(cast(count_of_appointments as double)), 0) as total_appointments" in sql
    assert "between 24300 and 24311" in sql
    assert "group by 1, 2" in sql
    assert "order by cast(year as integer) asc, cast(month as integer) asc" in sql


def test_gp_fte_by_icb_trend_last_12_months() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["gp_fte"],
            group_by=["icb_name"],
            transforms=[TransformSpec(type="trend", n=12)],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "workforce"
    assert compiled.grain == "icb"
    assert "from \"test-gp-workforce\".individual" in sql
    assert "select icb_name, year, month, round(sum(fte), 1) as gp_fte" in sql
    assert "group by 1, 2, 3" in sql
    assert "order by cast(year as integer) asc, cast(month as integer) asc" in sql


if __name__ == "__main__":
    tests = [
        test_gp_headcount_national_latest,
        test_gp_fte_by_icb_topn,
        test_nurse_fte_by_region_benchmark,
        test_registered_patients_by_icb_benchmark,
        test_patients_per_gp_practice_filter,
        test_appointments_per_gp_fte_by_icb,
        test_appointments_per_gp_fte_compare_icbs,
        test_gp_fte_grouped_benchmark,
        test_total_appointments_by_icb_uses_pcn_subicb,
        test_dna_rate_icb_filter_uses_pcn_subicb,
        test_dna_rate_icb_filter_normalizes_icb_labels,
        test_total_appointments_by_region_benchmark,
        test_dna_rate_by_icb_benchmark,
        test_appointments_per_gp_fte_by_region_benchmark,
        test_appointments_per_gp_headcount_by_icb,
        test_appointments_per_nurse_fte_by_region_benchmark,
        test_appointments_per_patient_by_region_benchmark,
        test_total_appointments_trend_last_12_months,
        test_gp_fte_by_icb_trend_last_12_months,
    ]
    passed = 0
    for test in tests:
        test()
        passed += 1
        print(f"[PASS] {test.__name__}")
    print(f"\nSummary: {passed}/{len(tests)} passed")
