"""Retirement-window shadow simulation harness.

Production shadow logs take 1-2 weeks to accumulate before we can make a
data-driven call on retiring a legacy SQL branch. This package builds
that signal locally by running a representative corpus of real-world NHS
GP appointments / workforce questions through `_try_query_plan_v1_live`
and aggregating the resulting `legacy_retirement_report` payloads.

Use as:

    python -m tools.retirement_simulation

Output: per-metric admission rate, breakdown of rejection reasons, and a
go/no-go recommendation per retirement candidate.
"""
