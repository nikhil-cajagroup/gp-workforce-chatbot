from __future__ import annotations

import pytest

import v9_metric_registry
from v9_compiler import compile_request
from v9_semantic_types import CompareSpec, SemanticRequest, TransformSpec


@pytest.fixture(autouse=True)
def reset_latest_periods_for_compiler_tests():
    v9_metric_registry.WORKFORCE_LATEST.clear()
    v9_metric_registry.WORKFORCE_LATEST.update({"year": "2025", "month": "12"})
    v9_metric_registry.APPOINTMENTS_LATEST.clear()
    v9_metric_registry.APPOINTMENTS_LATEST.update({"year": "2025", "month": "11"})
    yield
    v9_metric_registry.WORKFORCE_LATEST.clear()
    v9_metric_registry.WORKFORCE_LATEST.update({"year": "2025", "month": "12"})
    v9_metric_registry.APPOINTMENTS_LATEST.clear()
    v9_metric_registry.APPOINTMENTS_LATEST.update({"year": "2025", "month": "11"})


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
    assert "total_gp_extgl_fte" in sql
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
    # Cross-dataset ICB joins now use a normalized icb_join_key rather than
    # a raw USING(icb_name) so punctuation / "NHS " prefix variations don't drop rows.
    assert "icb_join_key" in sql
    assert "join wf on appt.icb_join_key = wf.icb_join_key" in sql
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
    assert "icb_join_key" in sql
    assert "join wf on appt.icb_join_key = wf.icb_join_key" in sql


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


def test_total_appointments_by_pcn_uses_practice_table() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["total_appointments"],
            group_by=["pcn_name"],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "appointments"
    assert compiled.grain == "pcn"
    assert 'from "test-gp-appointments".practice' in sql
    assert "select pcn_name, round(sum(cast(count_of_appointments as double)), 0) as total_appointments" in sql


def test_appointments_per_gp_fte_by_pcn_uses_practice_table() -> None:
    compiled = compile_request(
        SemanticRequest(
            metrics=["appointments_per_gp_fte"],
            group_by=["pcn_name"],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "cross"
    assert compiled.grain == "pcn"
    assert '"test-gp-appointments".practice' in sql
    assert "select pcn_name, total_appointments, gp_fte, appointments_per_gp_fte" in sql


def test_national_category_with_icb_scope_raises() -> None:
    with pytest.raises(ValueError, match="national_category queries do not support region or ICB"):
        compile_request(
            SemanticRequest(
                metrics=["total_appointments"],
                entity_filters={
                    "icb_name": "NHS Greater Manchester ICB",
                    "national_category": "Mental Health",
                },
            )
        )


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


# ── Bug-fix regressions ────────────────────────────────────────────────────────

def test_benchmark_scope_partition_by_region() -> None:
    """Non-national scope should produce PARTITION BY rather than a mislabelled OVER().

    Request: GP FTE by ICB, compared to its *regional* average.
    Expected: scope column (region_name) is injected into the query so the window
    function can PARTITION BY region_name → each ICB compared to its own region.
    """
    compiled = compile_request(
        SemanticRequest(
            metrics=["gp_fte"],
            group_by=["icb_name"],
            transforms=[TransformSpec(type="benchmark", scope="region")],
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "workforce"
    assert compiled.grain == "icb"
    # Scope column must be injected into the inner query so it's available.
    assert "region_name" in sql
    # PARTITION BY must be used — not the unscoped OVER () that gives a global mean.
    assert "over (partition by region_name)" in sql
    # Label must match the scope, not "national_average".
    assert "region_average" in sql
    assert "national_average" not in sql


def test_benchmark_scope_fallback_honest_label() -> None:
    """When scope col is unavailable, fall back to OVER() with label 'national_average'.

    This verifies the honest-fallback path: if scope='pcn' is requested but
    pcn_name cannot be injected (e.g. at region grain where it doesn't exist),
    the result says 'national_average' — which accurately reflects OVER ().
    """
    compiled = compile_request(
        SemanticRequest(
            metrics=["gp_fte"],
            group_by=["region_name"],
            # Requesting PCN average at region grain is invalid (PCN is below
            # region in the hierarchy) — should fall back honestly.
            transforms=[TransformSpec(type="benchmark", scope="pcn")],
        )
    )
    sql = compiled.sql.lower()
    # Must NOT produce the misleading "pcn_average" label.
    assert "pcn_average" not in sql
    # Falls back to national_average (honest reflection of OVER ()).
    assert "national_average" in sql
    assert "over ()" in sql


def test_gp_headcount_practice_grain() -> None:
    """gp_headcount should now compile at practice grain (entity_filters path)."""
    compiled = compile_request(
        SemanticRequest(
            metrics=["gp_headcount"],
            entity_filters={"practice_code": "P82001"},
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "workforce"
    assert compiled.grain == "practice"
    assert 'from "test-gp-workforce".individual' in sql
    assert "count(distinct unique_identifier) as gp_headcount" in sql
    assert "staff_group = 'gp'" in sql
    assert "lower(trim(prac_code)) = lower('p82001')" in sql


def test_gp_fte_practice_grain() -> None:
    """gp_fte should now compile at practice grain (entity_filters path)."""
    compiled = compile_request(
        SemanticRequest(
            metrics=["gp_fte"],
            entity_filters={"practice_code": "P82001"},
        )
    )
    sql = compiled.sql.lower()
    assert compiled.dataset == "workforce"
    assert compiled.grain == "practice"
    assert 'from "test-gp-workforce".individual' in sql
    assert "round(sum(fte), 1) as gp_fte" in sql
    assert "staff_group = 'gp'" in sql
    assert "lower(trim(prac_code)) = lower('p82001')" in sql


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
        # bug-fix regressions
        test_benchmark_scope_partition_by_region,
        test_benchmark_scope_fallback_honest_label,
        test_gp_headcount_practice_grain,
        test_gp_fte_practice_grain,
    ]
    passed = 0
    for test in tests:
        test()
        passed += 1
        print(f"[PASS] {test.__name__}")
    print(f"\nSummary: {passed}/{len(tests)} passed")
