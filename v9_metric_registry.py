from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Literal, Mapping, Optional

from v9_semantic_types import DatasetName, GrainName


@dataclass(frozen=True)
class MetricDefinition:
    key: str
    dataset: DatasetName
    description: str
    base_table: Optional[str] = None
    expr: Optional[str] = None
    format: Literal["number", "percent"] = "number"
    filter_sql: Optional[str] = None
    valid_grains: List[GrainName] = field(default_factory=list)
    valid_dimensions: List[str] = field(default_factory=list)
    valid_benchmarks: List[str] = field(default_factory=list)
    derived: bool = False
    requires: List[str] = field(default_factory=list)
    formula: Optional[str] = None


WORKFORCE_LATEST = {"year": "2025", "month": "12"}
APPOINTMENTS_LATEST = {"year": "2025", "month": "11"}

WORKFORCE_DATABASE = "test-gp-workforce"
APPOINTMENTS_DATABASE = "test-gp-appointments"

DIMENSIONS: Dict[GrainName, Dict[str, str]] = {
    "national": {},
    "region": {"workforce": "region_name", "appointments": "region_name", "cross": "region_name"},
    "icb": {"workforce": "icb_name", "appointments": "icb_name", "cross": "icb_name"},
    "sub_icb": {"workforce": "sub_icb_name", "appointments": "sub_icb_location_name", "cross": "sub_icb_name"},
    "pcn": {"workforce": "pcn_name", "appointments": "pcn_name", "cross": "pcn_name"},
    "practice": {"workforce": "prac_code", "appointments": "gp_code", "cross": "practice_code"},
    "appt_mode": {"appointments": "appt_mode"},
    "hcp_type": {"appointments": "hcp_type"},
    "booking_window": {"appointments": "time_between_book_and_appt"},
}


METRICS: Dict[str, MetricDefinition] = {
    "gp_headcount": MetricDefinition(
        key="gp_headcount",
        dataset="workforce",
        description="Distinct GP headcount from the individual workforce table.",
        base_table="individual",
        expr="COUNT(DISTINCT unique_identifier)",
        filter_sql="staff_group = 'GP'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn"],
    ),
    "gp_fte": MetricDefinition(
        key="gp_fte",
        dataset="workforce",
        description="GP full-time equivalent (37.5 hours = 1.0 FTE) from the individual workforce table.",
        base_table="individual",
        expr="ROUND(SUM(fte), 1)",
        filter_sql="staff_group = 'GP'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn"],
    ),
    "nurse_fte": MetricDefinition(
        key="nurse_fte",
        dataset="workforce",
        description="Nurse full-time equivalent from the individual workforce table.",
        base_table="individual",
        expr="ROUND(SUM(fte), 1)",
        filter_sql="staff_group = 'Nurses'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn"],
    ),
    "patients_per_gp": MetricDefinition(
        key="patients_per_gp",
        dataset="workforce",
        description="Registered patients divided by GP FTE from the practice_detailed table.",
        base_table="practice_detailed",
        expr=(
            "ROUND("
            "SUM(CAST(NULLIF(total_patients, 'NA') AS DOUBLE)) / "
            "NULLIF(SUM(CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE)), 0), 1)"
        ),
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "registered_patients": MetricDefinition(
        key="registered_patients",
        dataset="workforce",
        description="Registered patients from the practice_detailed table.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_patients, 'NA') AS DOUBLE)), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "total_appointments": MetricDefinition(
        key="total_appointments",
        dataset="appointments",
        description="Total number of GP appointments.",
        base_table="practice",
        expr="ROUND(SUM(CAST(count_of_appointments AS DOUBLE)), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice", "appt_mode", "hcp_type", "booking_window"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code", "appt_mode", "hcp_type", "time_between_book_and_appt"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "face_to_face_appointments": MetricDefinition(
        key="face_to_face_appointments",
        dataset="appointments",
        description="Appointments delivered face to face.",
        base_table="practice",
        expr="ROUND(SUM(CAST(count_of_appointments AS DOUBLE)), 0)",
        filter_sql="appt_mode = 'Face-to-Face'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice", "appt_mode", "hcp_type", "booking_window"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code", "appt_mode", "hcp_type", "time_between_book_and_appt"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "face_to_face_share": MetricDefinition(
        key="face_to_face_share",
        dataset="appointments",
        description="Face-to-face appointments divided by total appointments in the same scope.",
        derived=True,
        requires=["face_to_face_appointments", "total_appointments"],
        formula="face_to_face_appointments / NULLIF(total_appointments, 0)",
        format="percent",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice", "appt_mode", "hcp_type", "booking_window"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code", "appt_mode", "hcp_type", "time_between_book_and_appt"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "telephone_appointments": MetricDefinition(
        key="telephone_appointments",
        dataset="appointments",
        description="Appointments delivered by telephone.",
        base_table="practice",
        expr="ROUND(SUM(CAST(count_of_appointments AS DOUBLE)), 0)",
        filter_sql="appt_mode = 'Telephone'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice", "appt_mode", "hcp_type", "booking_window"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code", "appt_mode", "hcp_type", "time_between_book_and_appt"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "telephone_share": MetricDefinition(
        key="telephone_share",
        dataset="appointments",
        description="Telephone appointments divided by total appointments in the same scope.",
        derived=True,
        requires=["telephone_appointments", "total_appointments"],
        formula="telephone_appointments / NULLIF(total_appointments, 0)",
        format="percent",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "video_online_appointments": MetricDefinition(
        key="video_online_appointments",
        dataset="appointments",
        description="Appointments delivered by video conference or online.",
        base_table="practice",
        expr="ROUND(SUM(CAST(count_of_appointments AS DOUBLE)), 0)",
        filter_sql="appt_mode = 'Video Conference/Online'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "video_online_share": MetricDefinition(
        key="video_online_share",
        dataset="appointments",
        description="Video or online appointments divided by total appointments in the same scope.",
        derived=True,
        requires=["video_online_appointments", "total_appointments"],
        formula="video_online_appointments / NULLIF(total_appointments, 0)",
        format="percent",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "home_visit_appointments": MetricDefinition(
        key="home_visit_appointments",
        dataset="appointments",
        description="Appointments delivered as home visits.",
        base_table="practice",
        expr="ROUND(SUM(CAST(count_of_appointments AS DOUBLE)), 0)",
        filter_sql="appt_mode = 'Home Visit'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "home_visit_share": MetricDefinition(
        key="home_visit_share",
        dataset="appointments",
        description="Home visit appointments divided by total appointments in the same scope.",
        derived=True,
        requires=["home_visit_appointments", "total_appointments"],
        formula="home_visit_appointments / NULLIF(total_appointments, 0)",
        format="percent",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "dna_count": MetricDefinition(
        key="dna_count",
        dataset="appointments",
        description="Appointment count where status is DNA (Did Not Attend).",
        base_table="practice",
        expr="ROUND(SUM(CASE WHEN appt_status = 'DNA' THEN CAST(count_of_appointments AS DOUBLE) ELSE 0 END), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
    ),
    "dna_rate": MetricDefinition(
        key="dna_rate",
        dataset="appointments",
        description="DNA appointments divided by total appointments in the same scope.",
        derived=True,
        requires=["dna_count", "total_appointments"],
        formula="dna_count / NULLIF(total_appointments, 0)",
        format="percent",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "within_2_weeks_appointments": MetricDefinition(
        key="within_2_weeks_appointments",
        dataset="appointments",
        description="Appointments booked within 14 days, including same-day appointments.",
        base_table="practice",
        expr="ROUND(SUM(CAST(count_of_appointments AS DOUBLE)), 0)",
        filter_sql=(
            "time_between_book_and_appt IN "
            "('Same Day', '1 Day', '2 to 7 Days', '8  to 14 Days')"
        ),
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "within_2_weeks_share": MetricDefinition(
        key="within_2_weeks_share",
        dataset="appointments",
        description="Appointments booked within 14 days divided by total appointments in the same scope.",
        derived=True,
        requires=["within_2_weeks_appointments", "total_appointments"],
        formula="within_2_weeks_appointments / NULLIF(total_appointments, 0)",
        format="percent",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "over_2_weeks_appointments": MetricDefinition(
        key="over_2_weeks_appointments",
        dataset="appointments",
        description="Appointments booked more than 14 days ahead.",
        base_table="practice",
        expr="ROUND(SUM(CAST(count_of_appointments AS DOUBLE)), 0)",
        filter_sql=(
            "time_between_book_and_appt IN "
            "('15  to 21 Days', '22  to 28 Days', 'More than 28 Days')"
        ),
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "over_2_weeks_share": MetricDefinition(
        key="over_2_weeks_share",
        dataset="appointments",
        description="Appointments booked more than 14 days ahead divided by total appointments in the same scope.",
        derived=True,
        requires=["over_2_weeks_appointments", "total_appointments"],
        formula="over_2_weeks_appointments / NULLIF(total_appointments, 0)",
        format="percent",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "appointments_per_gp_fte": MetricDefinition(
        key="appointments_per_gp_fte",
        dataset="cross",
        description="Total appointments divided by GP FTE at a shared grain across both datasets.",
        derived=True,
        requires=["total_appointments", "gp_fte"],
        formula="total_appointments / NULLIF(gp_fte, 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "practice_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "appointments_per_gp_headcount": MetricDefinition(
        key="appointments_per_gp_headcount",
        dataset="cross",
        description="Total appointments divided by GP headcount at a shared grain across both datasets.",
        derived=True,
        requires=["total_appointments", "gp_headcount"],
        formula="total_appointments / NULLIF(gp_headcount, 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn"],
    ),
    "appointments_per_nurse_fte": MetricDefinition(
        key="appointments_per_nurse_fte",
        dataset="cross",
        description="Total appointments divided by nurse FTE at a shared grain across both datasets.",
        derived=True,
        requires=["total_appointments", "nurse_fte"],
        formula="total_appointments / NULLIF(nurse_fte, 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn"],
    ),
    "appointments_per_patient": MetricDefinition(
        key="appointments_per_patient",
        dataset="cross",
        description="Total appointments divided by registered patients at a shared grain across both datasets.",
        derived=True,
        requires=["total_appointments", "registered_patients"],
        formula="total_appointments / NULLIF(registered_patients, 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "practice_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
}


def metric_catalog_for_prompt() -> str:
    lines: List[str] = []
    for metric in METRICS.values():
        grain_text = ", ".join(metric.valid_grains)
        lines.append(f"- {metric.key}: {metric.description} | dataset={metric.dataset} | grains={grain_text}")
    return "\n".join(lines)


def get_metric(metric_key: str) -> MetricDefinition:
    if metric_key not in METRICS:
        raise KeyError(f"Unknown metric: {metric_key}")
    return METRICS[metric_key]


def all_metrics() -> Mapping[str, MetricDefinition]:
    return METRICS


def latest_for_dataset(dataset: DatasetName) -> Dict[str, str]:
    if dataset == "appointments":
        return dict(APPOINTMENTS_LATEST)
    if dataset == "workforce":
        return dict(WORKFORCE_LATEST)
    raise ValueError(f"No single latest period for dataset: {dataset}")


def database_for_dataset(dataset: DatasetName) -> str:
    if dataset == "appointments":
        return APPOINTMENTS_DATABASE
    if dataset == "workforce":
        return WORKFORCE_DATABASE
    raise ValueError(f"No single database for dataset: {dataset}")
