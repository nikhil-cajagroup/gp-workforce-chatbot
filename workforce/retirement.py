"""Shadow-mode retirement telemetry for legacy deterministic-override branches.

Before any legacy SQL branch is deleted, we need production data proving it
is dead code — i.e. every turn that would have hit it was already handled
by planner-v1 live. The retirement report cross-references three per-turn
signals to detect which retirement-window metric a turn is asking about:

    1. The planner-v1 live outcome's `metric` field (when v9 compiled
       cleanly — the strongest signal).
    2. The shadow intent classifier's `intent` field (only when it
       matches a retirement-window metric directly; the production
       classifier mostly emits generic categories like `data_simple`,
       so this rarely fires).
    3. A lexical keyword scan on the original question (last-resort
       fallback for turns where v9 failed to compile).

For each metric marked for retirement, the report says:
    - "admitted"           — planner-v1 live handled it, legacy SQL not needed
    - "rejected_<reason>"  — legacy SQL is still load-bearing; `reason` pins
                             down which gap needs closing before retirement
                             (e.g. follow-up turns, unresolved practice names)

This is strictly observational. Behavior does not change.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, FrozenSet, Optional


# Metrics currently in the shadow-retirement window. Expand this set as
# each new legacy branch becomes a retirement candidate. Both metrics
# below are fully covered by `LIVE_QUERY_PLAN_V1_METRICS`, so the
# expectation is that production shadow logs show ~100%
# `status=admitted` for in-scope turns.
LEGACY_RETIREMENT_SHADOW_METRICS: FrozenSet[str] = frozenset({
    "total_appointments",
    "dna_rate",
})


# Env-driven kill-switch — comma-separated list of metric keys whose
# legacy override branches should short-circuit instead of running.
# Empty by default so deploying this code is a pure no-op until the
# operator opts in. Read fresh on every call so we don't need a process
# restart to flip the switch in an emergency.
_RETIREMENT_KILL_SWITCH_ENV = "RETIRE_LEGACY_METRICS"


def _retirement_kill_switch_metrics() -> FrozenSet[str]:
    raw = os.environ.get(_RETIREMENT_KILL_SWITCH_ENV, "") or ""
    return frozenset(
        item.strip().lower() for item in raw.split(",") if item.strip()
    )


def should_short_circuit_legacy_branch(*, shadow_intent: Optional[str]) -> bool:
    """Return True if the operator has flipped the kill switch for this
    metric AND the metric is still in the shadow retirement window.

    Defence in depth: the kill switch only fires for metrics we have
    explicitly enrolled in the retirement programme, so a typo in the
    env var can't accidentally disable an unrelated legacy branch.
    """
    intent_key = str(shadow_intent or "").strip().lower()
    if not intent_key or intent_key not in LEGACY_RETIREMENT_SHADOW_METRICS:
        return False
    return intent_key in _retirement_kill_switch_metrics()


# Lexical fallback patterns. Only used when v9 didn't compile and the
# shadow intent classifier emitted a generic category (the common case
# in production). Conservative on purpose: false positives here would
# inflate the "admitted" denominator, so we'd rather miss a borderline
# turn than misclassify it. Order matters — check `dna_rate` before
# `total_appointments` because "DNA rate" questions also mention
# "appointments".
_DNA_RATE_PATTERN = re.compile(
    r"\b(dna(\s+rate)?|did\s+not\s+attend(\s+rate)?)\b",
    re.IGNORECASE,
)
_APPOINTMENT_NOUN_PATTERN = re.compile(
    r"\bappointments?\b",
    re.IGNORECASE,
)


def _lexical_metric_scan(question: Optional[str]) -> Optional[str]:
    """Last-resort keyword scan for turns where v9 didn't run.

    Only emits a metric key that's actually in the retirement window —
    so this can't accidentally retire something we haven't enrolled.
    """
    text = str(question or "").strip()
    if not text:
        return None
    if "dna_rate" in LEGACY_RETIREMENT_SHADOW_METRICS and _DNA_RATE_PATTERN.search(text):
        return "dna_rate"
    if (
        "total_appointments" in LEGACY_RETIREMENT_SHADOW_METRICS
        and _APPOINTMENT_NOUN_PATTERN.search(text)
    ):
        return "total_appointments"
    return None


def detect_retirement_metric(
    *,
    shadow_intent: Optional[str],
    planner_v1_live_outcome: Optional[Dict[str, Any]],
    question: Optional[str],
) -> Optional[str]:
    """Tiered metric detection for retirement telemetry.

    The shadow classifier in production rarely emits metric-specific
    intents, so we read the planner-v1 outcome (which carries the
    v9-compiled metric on most paths) first, then fall back to lexical
    scanning when v9 failed entirely.
    """
    outcome_metric = str(
        (planner_v1_live_outcome or {}).get("metric") or ""
    ).strip().lower()
    if outcome_metric in LEGACY_RETIREMENT_SHADOW_METRICS:
        return outcome_metric

    intent_key = str(shadow_intent or "").strip().lower()
    if intent_key in LEGACY_RETIREMENT_SHADOW_METRICS:
        return intent_key

    return _lexical_metric_scan(question)


def build_legacy_retirement_report(
    *,
    shadow_intent: Optional[str],
    planner_v1_live_outcome: Optional[Dict[str, Any]],
    question: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Produce a structured retirement signal for this turn.

    Returns None when the turn is not asking about a retirement-window
    metric, so turns outside the window are cheap to filter in
    aggregate logs.
    """
    metric_key = detect_retirement_metric(
        shadow_intent=shadow_intent,
        planner_v1_live_outcome=planner_v1_live_outcome,
        question=question,
    )
    if metric_key is None:
        return None

    outcome_payload = dict(planner_v1_live_outcome or {})
    outcome = str(outcome_payload.get("outcome") or "").strip()

    if outcome in {"admitted", "admitted_hard_intent"}:
        status = "admitted"
    elif outcome.startswith("rejected_"):
        status = outcome
    else:
        # Supervisor never reached the admission check (cross-dataset
        # compound scope short-circuit, knowledge/greeting route, etc.)
        status = "not_reached"

    report: Dict[str, Any] = {
        "metric": metric_key,
        "status": status,
    }
    # `metric_source` is hugely useful for debugging the retirement
    # signal in prod: it tells us whether we got a high-confidence
    # outcome-payload match or a softer lexical match.
    if str((planner_v1_live_outcome or {}).get("metric") or "").strip().lower() == metric_key:
        report["metric_source"] = "outcome"
    elif str(shadow_intent or "").strip().lower() == metric_key:
        report["metric_source"] = "shadow_intent"
    else:
        report["metric_source"] = "lexical"
    for key in ("gate_reason", "hard_intent"):
        value = outcome_payload.get(key)
        if value:
            report[key] = value
    return report
