"""
Focused tests for v8 dataset routing.

Covers:
- explicit appointments/workforce questions
- vague follow-ups that should inherit the prior dataset
- explicit domain switches that should override prior context
"""

from gp_workforce_chatbot_backend_agent_v8 import detect_dataset


def check(name: str, actual: str, expected: str) -> bool:
    ok = actual == expected
    label = "PASS" if ok else "FAIL"
    print(f"[{label}] {name}: expected={expected} actual={actual}")
    return ok


if __name__ == "__main__":
    passed = 0
    total = 0

    cases = [
        (
            "Explicit appointments question",
            detect_dataset("How many appointments were there nationally?"),
            "appointments",
        ),
        (
            "Explicit workforce question",
            detect_dataset("How many GPs are there nationally?"),
            "workforce",
        ),
        (
            "Vague appointments follow-up inherits prior dataset",
            detect_dataset(
                "What about DNA rate?",
                {
                    "dataset": "appointments",
                    "table": "practice",
                    "entity_name": "Queens Park Medical Centre",
                    "semantic_state": {"dataset": "appointments", "metric": "appointments_total"},
                },
            ),
            "appointments",
        ),
        (
            "Appointments wording without explicit keyword still stays appointments from context",
            detect_dataset(
                "How many does this practice have?",
                {
                    "dataset": "appointments",
                    "table": "practice",
                    "entity_name": "Queens Park Medical Centre",
                    "semantic_state": {"dataset": "appointments", "metric": "appointments_total"},
                },
            ),
            "appointments",
        ),
        (
            "Vague workforce follow-up inherits prior dataset",
            detect_dataset(
                "Break this down by gender",
                {
                    "dataset": "workforce",
                    "table": "individual",
                    "entity_name": "Leeds",
                    "semantic_state": {"dataset": "workforce", "metric": "headcount"},
                },
            ),
            "workforce",
        ),
        (
            "Explicit switch from appointments context to workforce",
            detect_dataset(
                "How many GPs are there nationally?",
                {
                    "dataset": "appointments",
                    "table": "practice",
                    "entity_name": "Queens Park Medical Centre",
                    "semantic_state": {"dataset": "appointments", "metric": "appointments_total"},
                },
            ),
            "workforce",
        ),
        (
            "Explicit switch from workforce context to appointments",
            detect_dataset(
                "Show total appointments nationally in the latest month",
                {
                    "dataset": "workforce",
                    "table": "individual",
                    "entity_name": "Leeds",
                    "semantic_state": {"dataset": "workforce", "metric": "headcount"},
                },
            ),
            "appointments",
        ),
    ]

    for name, actual, expected in cases:
        total += 1
        if check(name, actual, expected):
            passed += 1

    print(f"\nSummary: {passed}/{total} passed")
    raise SystemExit(0 if passed == total else 1)
