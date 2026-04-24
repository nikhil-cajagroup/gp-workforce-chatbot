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
    "national_category": {"appointments": "national_category"},
}


METRICS: Dict[str, MetricDefinition] = {
    "gp_headcount": MetricDefinition(
        key="gp_headcount",
        dataset="workforce",
        description="Distinct GP headcount from the individual workforce table.",
        base_table="individual",
        expr="COUNT(DISTINCT unique_identifier)",
        filter_sql="staff_group = 'GP'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "gp_fte": MetricDefinition(
        key="gp_fte",
        dataset="workforce",
        description="GP full-time equivalent (37.5 hours = 1.0 FTE) from the individual workforce table.",
        base_table="individual",
        expr="ROUND(SUM(fte), 1)",
        filter_sql="staff_group = 'GP'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
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
        description=(
            "Registered patients divided by fully-qualified & permanent GP FTE "
            "(excludes trainees, locums, and retainers — total_gp_extgl_fte), "
            "matching BMA / NHS Digital standard methodology."
        ),
        base_table="practice_detailed",
        expr=(
            "ROUND("
            "SUM(CAST(NULLIF(total_patients, 'NA') AS DOUBLE)) / "
            "NULLIF(SUM(CAST(NULLIF(total_gp_extgl_fte, 'NA') AS DOUBLE)), 0), 1)"
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
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice", "appt_mode", "hcp_type", "booking_window", "national_category"],
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
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice", "appt_mode", "hcp_type", "booking_window", "national_category"],
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
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice", "appt_mode", "hcp_type", "booking_window", "national_category"],
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
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice", "appt_mode", "hcp_type", "booking_window", "national_category"],
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
    # --- HCP-type filtered metrics ---
    "gp_hcp_appointments": MetricDefinition(
        key="gp_hcp_appointments",
        dataset="appointments",
        description="Appointments where the healthcare professional was a GP.",
        base_table="practice",
        expr="ROUND(SUM(CAST(count_of_appointments AS DOUBLE)), 0)",
        filter_sql="hcp_type = 'GP'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice", "appt_mode", "booking_window", "national_category"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code", "appt_mode", "time_between_book_and_appt"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "gp_hcp_share": MetricDefinition(
        key="gp_hcp_share",
        dataset="appointments",
        description="GP appointments divided by total appointments in the same scope.",
        derived=True,
        requires=["gp_hcp_appointments", "total_appointments"],
        formula="gp_hcp_appointments / NULLIF(total_appointments, 0)",
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
    # ── Workforce: additional staff-group headcount / FTE ────────
    "nurse_headcount": MetricDefinition(
        key="nurse_headcount",
        dataset="workforce",
        description="Distinct nurse headcount from the individual workforce table.",
        base_table="individual",
        expr="COUNT(DISTINCT unique_identifier)",
        filter_sql="staff_group = 'Nurses'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn"],
    ),
    "dpc_headcount": MetricDefinition(
        key="dpc_headcount",
        dataset="workforce",
        description="Distinct Direct Patient Care staff headcount (HCAs, pharmacists, physios, physician associates, etc.).",
        base_table="individual",
        expr="COUNT(DISTINCT unique_identifier)",
        filter_sql="staff_group = 'Direct Patient Care'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn"],
    ),
    "dpc_fte": MetricDefinition(
        key="dpc_fte",
        dataset="workforce",
        description="Direct Patient Care staff FTE (HCAs, pharmacists, physios, physician associates, etc.).",
        base_table="individual",
        expr="ROUND(SUM(fte), 1)",
        filter_sql="staff_group = 'Direct Patient Care'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn"],
    ),
    "admin_headcount": MetricDefinition(
        key="admin_headcount",
        dataset="workforce",
        description="Distinct Admin/Non-clinical staff headcount.",
        base_table="individual",
        expr="COUNT(DISTINCT unique_identifier)",
        filter_sql="staff_group = 'Admin/Non-Clinical'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn"],
    ),
    "admin_fte": MetricDefinition(
        key="admin_fte",
        dataset="workforce",
        description="Admin/Non-clinical staff FTE.",
        base_table="individual",
        expr="ROUND(SUM(fte), 1)",
        filter_sql="staff_group = 'Admin/Non-Clinical'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn"],
    ),
    # ── Workforce: GP sub-type breakdown ─────────────────────────
    "gp_partner_headcount": MetricDefinition(
        key="gp_partner_headcount",
        dataset="workforce",
        description="GP Partner/Provider headcount (includes senior partners).",
        base_table="practice_detailed",
        expr=(
            "ROUND(SUM(CAST(NULLIF(total_gp_ptnr_prov_hc, 'NA') AS DOUBLE))"
            " + SUM(CAST(NULLIF(total_gp_sen_ptnr_hc, 'NA') AS DOUBLE)), 0)"
        ),
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "gp_partner_fte": MetricDefinition(
        key="gp_partner_fte",
        dataset="workforce",
        description="GP Partner/Provider FTE (includes senior partners).",
        base_table="practice_detailed",
        expr=(
            "ROUND(SUM(CAST(NULLIF(total_gp_ptnr_prov_fte, 'NA') AS DOUBLE))"
            " + SUM(CAST(NULLIF(total_gp_sen_ptnr_fte, 'NA') AS DOUBLE)), 1)"
        ),
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "salaried_gp_headcount": MetricDefinition(
        key="salaried_gp_headcount",
        dataset="workforce",
        description="Salaried GP headcount (employed by practice or other organisation).",
        base_table="practice_detailed",
        expr=(
            "ROUND(SUM(CAST(NULLIF(total_gp_sal_by_prac_hc, 'NA') AS DOUBLE))"
            " + SUM(CAST(NULLIF(total_gp_sal_by_oth_hc, 'NA') AS DOUBLE)), 0)"
        ),
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "salaried_gp_fte": MetricDefinition(
        key="salaried_gp_fte",
        dataset="workforce",
        description="Salaried GP FTE (employed by practice or other organisation).",
        base_table="practice_detailed",
        expr=(
            "ROUND(SUM(CAST(NULLIF(total_gp_sal_by_prac_fte, 'NA') AS DOUBLE))"
            " + SUM(CAST(NULLIF(total_gp_sal_by_oth_fte, 'NA') AS DOUBLE)), 1)"
        ),
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "locum_gp_headcount": MetricDefinition(
        key="locum_gp_headcount",
        dataset="workforce",
        description="Locum GP headcount (covering vacancy, absence, or other).",
        base_table="practice_detailed",
        expr=(
            "ROUND(SUM(CAST(NULLIF(total_gp_locum_vac_hc, 'NA') AS DOUBLE))"
            " + SUM(CAST(NULLIF(total_gp_locum_abs_hc, 'NA') AS DOUBLE))"
            " + SUM(CAST(NULLIF(total_gp_locum_oth_hc, 'NA') AS DOUBLE)), 0)"
        ),
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "locum_gp_fte": MetricDefinition(
        key="locum_gp_fte",
        dataset="workforce",
        description="Locum GP FTE (covering vacancy, absence, or other).",
        base_table="practice_detailed",
        expr=(
            "ROUND(SUM(CAST(NULLIF(total_gp_locum_vac_fte, 'NA') AS DOUBLE))"
            " + SUM(CAST(NULLIF(total_gp_locum_abs_fte, 'NA') AS DOUBLE))"
            " + SUM(CAST(NULLIF(total_gp_locum_oth_fte, 'NA') AS DOUBLE)), 1)"
        ),
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "registrar_gp_headcount": MetricDefinition(
        key="registrar_gp_headcount",
        dataset="workforce",
        description="GP Registrar / trainee headcount (all specialty training years + foundation).",
        base_table="practice_detailed",
        expr=(
            "ROUND("
            "SUM(CAST(NULLIF(total_gp_trn_gr_st1_hc, 'NA') AS DOUBLE))"
            " + SUM(CAST(NULLIF(total_gp_trn_gr_st2_hc, 'NA') AS DOUBLE))"
            " + SUM(CAST(NULLIF(total_gp_trn_gr_st3_hc, 'NA') AS DOUBLE))"
            " + SUM(CAST(NULLIF(total_gp_trn_gr_st4_hc, 'NA') AS DOUBLE))"
            " + SUM(CAST(NULLIF(total_gp_trn_gr_oth_hc, 'NA') AS DOUBLE))"
            " + SUM(CAST(NULLIF(total_gp_trn_gr_f1_2_hc, 'NA') AS DOUBLE))"
            ", 0)"
        ),
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "gp_retainer_headcount": MetricDefinition(
        key="gp_retainer_headcount",
        dataset="workforce",
        description="GP Retainer headcount.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_gp_ret_hc, 'NA') AS DOUBLE)), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    # ── Workforce: nurse sub-types ───────────────────────────────
    "practice_nurse_headcount": MetricDefinition(
        key="practice_nurse_headcount",
        dataset="workforce",
        description="Practice Nurse headcount.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_n_prac_nurse_hc, 'NA') AS DOUBLE)), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "practice_nurse_fte": MetricDefinition(
        key="practice_nurse_fte",
        dataset="workforce",
        description="Practice Nurse FTE.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_n_prac_nurse_fte, 'NA') AS DOUBLE)), 1)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    # ── Workforce: DPC sub-roles ─────────────────────────────────
    "pharmacist_headcount": MetricDefinition(
        key="pharmacist_headcount",
        dataset="workforce",
        description="Clinical Pharmacist headcount in general practice.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_pharma_hc, 'NA') AS DOUBLE)), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "pharmacist_fte": MetricDefinition(
        key="pharmacist_fte",
        dataset="workforce",
        description="Clinical Pharmacist FTE in general practice.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_pharma_fte, 'NA') AS DOUBLE)), 1)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "physician_associate_headcount": MetricDefinition(
        key="physician_associate_headcount",
        dataset="workforce",
        description="Physician Associate headcount in general practice.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_physician_assoc_hc, 'NA') AS DOUBLE)), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "physician_associate_fte": MetricDefinition(
        key="physician_associate_fte",
        dataset="workforce",
        description="Physician Associate FTE in general practice.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_physician_assoc_fte, 'NA') AS DOUBLE)), 1)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "hca_headcount": MetricDefinition(
        key="hca_headcount",
        dataset="workforce",
        description="Healthcare Assistant (HCA) headcount in general practice.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_hca_hc, 'NA') AS DOUBLE)), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "hca_fte": MetricDefinition(
        key="hca_fte",
        dataset="workforce",
        description="Healthcare Assistant (HCA) FTE in general practice.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_hca_fte, 'NA') AS DOUBLE)), 1)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "paramedic_headcount": MetricDefinition(
        key="paramedic_headcount",
        dataset="workforce",
        description="Paramedic headcount in general practice.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_paramed_hc, 'NA') AS DOUBLE)), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "paramedic_fte": MetricDefinition(
        key="paramedic_fte",
        dataset="workforce",
        description="Paramedic FTE in general practice.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_paramed_fte, 'NA') AS DOUBLE)), 1)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "physiotherapist_headcount": MetricDefinition(
        key="physiotherapist_headcount",
        dataset="workforce",
        description="Physiotherapist headcount in general practice.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_physio_hc, 'NA') AS DOUBLE)), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "physiotherapist_fte": MetricDefinition(
        key="physiotherapist_fte",
        dataset="workforce",
        description="Physiotherapist FTE in general practice.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_physio_fte, 'NA') AS DOUBLE)), 1)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "splw_headcount": MetricDefinition(
        key="splw_headcount",
        dataset="workforce",
        description="Social Prescribing Link Worker headcount in general practice.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_splw_hc, 'NA') AS DOUBLE)), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "splw_fte": MetricDefinition(
        key="splw_fte",
        dataset="workforce",
        description="Social Prescribing Link Worker FTE in general practice.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_splw_fte, 'NA') AS DOUBLE)), 1)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "advanced_nurse_practitioner_headcount": MetricDefinition(
        key="advanced_nurse_practitioner_headcount",
        dataset="workforce",
        description="Advanced Nurse Practitioner (ANP) headcount from practice_detailed table.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_n_adv_nurse_prac_hc, 'NA') AS DOUBLE)), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "advanced_nurse_practitioner_fte": MetricDefinition(
        key="advanced_nurse_practitioner_fte",
        dataset="workforce",
        description="Advanced Nurse Practitioner (ANP) FTE from practice_detailed table.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_n_adv_nurse_prac_fte, 'NA') AS DOUBLE)), 1)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "nurse_specialist_headcount": MetricDefinition(
        key="nurse_specialist_headcount",
        dataset="workforce",
        description="Nurse Specialist headcount from practice_detailed table.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_n_nurse_spec_hc, 'NA') AS DOUBLE)), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "nurse_specialist_fte": MetricDefinition(
        key="nurse_specialist_fte",
        dataset="workforce",
        description="Nurse Specialist FTE from practice_detailed table.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_n_nurse_spec_fte, 'NA') AS DOUBLE)), 1)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "dietician_headcount": MetricDefinition(
        key="dietician_headcount",
        dataset="workforce",
        description="Dietician headcount from practice_detailed table.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_dietician_hc, 'NA') AS DOUBLE)), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "dietician_fte": MetricDefinition(
        key="dietician_fte",
        dataset="workforce",
        description="Dietician FTE from practice_detailed table.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_dietician_fte, 'NA') AS DOUBLE)), 1)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "counsellor_headcount": MetricDefinition(
        key="counsellor_headcount",
        dataset="workforce",
        description="Counsellor / therapist headcount from practice_detailed table.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_thera_cou_hc, 'NA') AS DOUBLE)), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "counsellor_fte": MetricDefinition(
        key="counsellor_fte",
        dataset="workforce",
        description="Counsellor / therapist FTE from practice_detailed table.",
        base_table="practice_detailed",
        expr="ROUND(SUM(CAST(NULLIF(total_dpc_thera_cou_fte, 'NA') AS DOUBLE)), 1)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_name", "pcn_name", "prac_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    # ── Appointments: attendance metrics ─────────────────────────
    "attended_count": MetricDefinition(
        key="attended_count",
        dataset="appointments",
        description="Appointment count where status is Attended.",
        base_table="practice",
        expr="ROUND(SUM(CASE WHEN appt_status = 'Attended' THEN CAST(count_of_appointments AS DOUBLE) ELSE 0 END), 0)",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
    ),
    "attended_rate": MetricDefinition(
        key="attended_rate",
        dataset="appointments",
        description="Attended appointments divided by total appointments.",
        derived=True,
        requires=["attended_count", "total_appointments"],
        formula="attended_count / NULLIF(total_appointments, 0)",
        format="percent",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    # ── Appointments: same-day access ────────────────────────────
    "same_day_appointments": MetricDefinition(
        key="same_day_appointments",
        dataset="appointments",
        description="Appointments booked and seen on the same day.",
        base_table="practice",
        expr="ROUND(SUM(CAST(count_of_appointments AS DOUBLE)), 0)",
        filter_sql="time_between_book_and_appt = 'Same Day'",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
        valid_benchmarks=["average_region", "average_icb", "average_sub_icb", "average_pcn", "average_practice"],
    ),
    "same_day_share": MetricDefinition(
        key="same_day_share",
        dataset="appointments",
        description="Same-day appointments divided by total appointments.",
        derived=True,
        requires=["same_day_appointments", "total_appointments"],
        formula="same_day_appointments / NULLIF(total_appointments, 0)",
        format="percent",
        valid_grains=["national", "region", "icb", "sub_icb", "pcn", "practice"],
        valid_dimensions=["region_name", "icb_name", "sub_icb_location_name", "pcn_name", "gp_code"],
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


def refresh_latest_periods(
    *,
    workforce_year: Optional[str] = None,
    workforce_month: Optional[str] = None,
    appointments_year: Optional[str] = None,
    appointments_month: Optional[str] = None,
) -> None:
    """Update the module-level LATEST dicts in place with live Athena values.

    Call this at application startup (or before any direct ``compile_request``
    call from a test harness or service split) so that compiled SQL uses real
    dates rather than the hardcoded fallback values baked into this file.

    Example — from the backend startup path::

        from v9_metric_registry import refresh_latest_periods
        appt = get_latest_year_month("practice", database=APPOINTMENTS_ATHENA_DATABASE)
        wf   = get_latest_year_month("practice_detailed")
        refresh_latest_periods(
            workforce_year=wf.get("year"), workforce_month=wf.get("month"),
            appointments_year=appt.get("year"), appointments_month=appt.get("month"),
        )
    """
    if workforce_year is not None:
        WORKFORCE_LATEST["year"] = workforce_year
    if workforce_month is not None:
        WORKFORCE_LATEST["month"] = workforce_month
    if appointments_year is not None:
        APPOINTMENTS_LATEST["year"] = appointments_year
    if appointments_month is not None:
        APPOINTMENTS_LATEST["month"] = appointments_month
