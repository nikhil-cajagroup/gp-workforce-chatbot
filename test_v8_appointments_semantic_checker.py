from __future__ import annotations

from v8_dataset_service_helpers import appointments_semantic_issue_checker


def _extract_practice_code(text: str):
    return None


def test_geo_sql_using_qualified_pcn_subicb_does_not_raise_geo_issue() -> None:
    state = {
        "sql": """
        WITH grouped AS (
          SELECT icb_name, SUM(count_of_appointments) AS total_appointments
          FROM "test-gp-appointments".pcn_subicb
          WHERE year = '2025' AND month = '11'
            AND LOWER(TRIM(icb_name)) = LOWER('NHS Greater Manchester ICB')
          GROUP BY 1
        )
        SELECT * FROM grouped
        """,
        "original_question": "What is the DNA rate in NHS Greater Manchester ICB?",
    }
    issues = appointments_semantic_issue_checker(
        state,
        [],
        extract_practice_code=_extract_practice_code,
    )
    assert "Appointments geography queries should usually use pcn_subicb." not in issues


if __name__ == "__main__":
    test_geo_sql_using_qualified_pcn_subicb_does_not_raise_geo_issue()
    print("[PASS] test_geo_sql_using_qualified_pcn_subicb_does_not_raise_geo_issue")
