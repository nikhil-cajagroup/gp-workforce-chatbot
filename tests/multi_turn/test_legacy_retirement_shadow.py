"""Shadow-mode retirement telemetry coverage.

Before deleting any legacy SQL override branch we want production data
showing every turn that would have hit it was already handled by
planner-v1 live. These tests pin down the per-turn signal builder
(`build_legacy_retirement_report`) and the supervisor wiring that
stashes the report onto state for downstream serialization.
"""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import gp_workforce_chatbot_backend_agent_v8 as backend
from gp_workforce_chatbot_backend_agent_v8 import (
    _maybe_apply_legacy_retirement_kill_switch,
    _record_planner_v1_live_outcome,
)
from workforce.retirement import (
    LEGACY_RETIREMENT_SHADOW_METRICS,
    build_legacy_retirement_report,
    detect_retirement_metric,
    should_short_circuit_legacy_branch,
)


# ---------------------------------------------------------------------------
# Pure unit coverage — build_legacy_retirement_report
# ---------------------------------------------------------------------------


def test_retirement_report_is_none_for_non_retirement_intent():
    """Intents outside the retirement window must yield no report so
    aggregate logs stay cheap to filter."""
    report = build_legacy_retirement_report(
        shadow_intent="gp_fte",
        planner_v1_live_outcome={"outcome": "admitted", "metric": "gp_fte"},
    )
    assert report is None


def test_retirement_report_admitted_for_total_appointments():
    report = build_legacy_retirement_report(
        shadow_intent="total_appointments",
        planner_v1_live_outcome={"outcome": "admitted", "metric": "total_appointments"},
    )
    assert report == {
        "metric": "total_appointments",
        "status": "admitted",
        # Outcome payload carried the metric explicitly — strongest signal.
        "metric_source": "outcome",
    }


def test_retirement_report_admitted_via_hard_intent():
    """admitted_hard_intent collapses to status=admitted, but the
    hard_intent label is preserved so we can tell which gate fired."""
    report = build_legacy_retirement_report(
        shadow_intent="total_appointments",
        planner_v1_live_outcome={
            "outcome": "admitted_hard_intent",
            "metric": "total_appointments",
            "hard_intent": "total_appointments",
        },
    )
    assert report == {
        "metric": "total_appointments",
        "status": "admitted",
        "metric_source": "outcome",
        "hard_intent": "total_appointments",
    }


def test_retirement_report_rejected_preserves_reason():
    """Rejection statuses must propagate verbatim — that's what tells us
    which gap in planner-v1 live still keeps the legacy branch alive."""
    report = build_legacy_retirement_report(
        shadow_intent="total_appointments",
        planner_v1_live_outcome={
            "outcome": "rejected_gate",
            "gate_reason": "confidence_low",
        },
    )
    assert report == {
        "metric": "total_appointments",
        "status": "rejected_gate",
        # Metric came from the shadow intent (outcome had no metric field).
        "metric_source": "shadow_intent",
        "gate_reason": "confidence_low",
    }


def test_retirement_report_not_reached_when_supervisor_short_circuits():
    """Cross-dataset compound scope / knowledge / greeting routes never
    reach the admission check. We surface that as a distinct status so
    these turns don't get confused with planner-v1 rejections."""
    report = build_legacy_retirement_report(
        shadow_intent="total_appointments",
        planner_v1_live_outcome=None,
    )
    assert report == {
        "metric": "total_appointments",
        "status": "not_reached",
        "metric_source": "shadow_intent",
    }


def test_retirement_report_lexical_fallback_for_v9_compile_failure():
    """Production finding: shadow classifier emits `data_simple` for the
    canonical "Total GP appointments in England" question, and v9 may
    fail to compile, leaving the planner-v1 outcome with no metric. The
    lexical fallback recovers the retirement signal in that case."""
    report = build_legacy_retirement_report(
        shadow_intent="data_simple",
        planner_v1_live_outcome={"outcome": "rejected_v9_compile_failed"},
        question="Total GP appointments in England last month",
    )
    assert report == {
        "metric": "total_appointments",
        "status": "rejected_v9_compile_failed",
        "metric_source": "lexical",
    }


def test_retirement_report_dna_rate_lexical_fallback():
    report = build_legacy_retirement_report(
        shadow_intent="data_simple",
        planner_v1_live_outcome={"outcome": "rejected_v9_compile_failed"},
        question="Did not attend rate in England",
    )
    assert report == {
        "metric": "dna_rate",
        "status": "rejected_v9_compile_failed",
        "metric_source": "lexical",
    }


def test_retirement_metric_detector_uses_lexical_fallback_for_generic_shadow_intent():
    metric = detect_retirement_metric(
        shadow_intent="data_simple",
        planner_v1_live_outcome=None,
        question="Total GP appointments in England last month",
    )
    assert metric == "total_appointments"


def test_retirement_report_lexical_skipped_when_no_question():
    """No question text and no shadow intent match → no report."""
    report = build_legacy_retirement_report(
        shadow_intent="data_simple",
        planner_v1_live_outcome={"outcome": "rejected_v9_compile_failed"},
        question=None,
    )
    assert report is None


def test_total_appointments_is_in_retirement_window():
    """Lock the current retirement window so accidentally dropping
    `total_appointments` from the set fails loudly."""
    assert "total_appointments" in LEGACY_RETIREMENT_SHADOW_METRICS


def test_dna_rate_is_in_retirement_window():
    """Second retirement candidate — fully covered by planner-v1 live,
    so it should be gathering shadow data alongside total_appointments."""
    assert "dna_rate" in LEGACY_RETIREMENT_SHADOW_METRICS


# ---------------------------------------------------------------------------
# Kill-switch helper — opt-in, env-driven, default off
# ---------------------------------------------------------------------------


def test_kill_switch_default_off(monkeypatch):
    """No env var set → kill switch never fires, regardless of intent."""
    monkeypatch.delenv("RETIRE_LEGACY_METRICS", raising=False)
    assert should_short_circuit_legacy_branch(shadow_intent="total_appointments") is False
    assert should_short_circuit_legacy_branch(shadow_intent="dna_rate") is False


def test_kill_switch_fires_for_enrolled_metric(monkeypatch):
    monkeypatch.setenv("RETIRE_LEGACY_METRICS", "total_appointments")
    assert should_short_circuit_legacy_branch(shadow_intent="total_appointments") is True


def test_kill_switch_ignores_metrics_outside_retirement_window(monkeypatch):
    """Even if the env lists a metric, the kill switch refuses to fire
    unless that metric is also enrolled in the retirement window. This
    guards against typos disabling unrelated legacy branches."""
    monkeypatch.setenv("RETIRE_LEGACY_METRICS", "gp_fte")
    assert should_short_circuit_legacy_branch(shadow_intent="gp_fte") is False


def test_kill_switch_supports_multiple_metrics(monkeypatch):
    monkeypatch.setenv("RETIRE_LEGACY_METRICS", "total_appointments, dna_rate")
    assert should_short_circuit_legacy_branch(shadow_intent="total_appointments") is True
    assert should_short_circuit_legacy_branch(shadow_intent="dna_rate") is True
    # Unrelated intent stays off.
    assert should_short_circuit_legacy_branch(shadow_intent="gp_fte") is False


def test_kill_switch_handles_missing_intent(monkeypatch):
    monkeypatch.setenv("RETIRE_LEGACY_METRICS", "total_appointments")
    assert should_short_circuit_legacy_branch(shadow_intent=None) is False
    assert should_short_circuit_legacy_branch(shadow_intent="") is False


# ---------------------------------------------------------------------------
# Integration — _maybe_apply_legacy_retirement_kill_switch
# ---------------------------------------------------------------------------


def test_appointments_kill_switch_no_op_by_default(monkeypatch):
    """With env unset, the kill-switch shim is a pure no-op: returns
    False and leaves worker_plan untouched."""
    monkeypatch.delenv("RETIRE_LEGACY_METRICS", raising=False)
    state = {
        "worker_plan": {},
        "_intent_result_v1": {"intent": "total_appointments"},
    }
    fired = _maybe_apply_legacy_retirement_kill_switch(state)
    assert fired is False
    assert state["worker_plan"] == {}


def test_appointments_kill_switch_fires_and_marks_state(monkeypatch):
    monkeypatch.setenv("RETIRE_LEGACY_METRICS", "total_appointments")
    state = {
        "worker_plan": {},
        "_intent_result_v1": {"intent": "total_appointments"},
    }
    fired = _maybe_apply_legacy_retirement_kill_switch(state)
    assert fired is True
    assert state["worker_plan"]["legacy_retirement_kill_switch"] == {
        "metric": "total_appointments",
        "branch": "appointments",
    }


def test_appointments_kill_switch_ignores_unenrolled_metric(monkeypatch):
    """Even when the env lists `gp_fte`, the shim refuses to fire because
    `gp_fte` is not in the retirement window."""
    monkeypatch.setenv("RETIRE_LEGACY_METRICS", "gp_fte")
    state = {
        "worker_plan": {},
        "_intent_result_v1": {"intent": "gp_fte"},
    }
    fired = _maybe_apply_legacy_retirement_kill_switch(state)
    assert fired is False
    assert "legacy_retirement_kill_switch" not in state["worker_plan"]


def test_appointments_kill_switch_uses_lexical_metric_when_shadow_intent_is_generic(monkeypatch):
    monkeypatch.setenv("RETIRE_LEGACY_METRICS", "total_appointments")
    state = {
        "worker_plan": {},
        "_intent_result_v1": {"intent": "data_simple"},
        "original_question": "Total GP appointments in England last month",
    }
    fired = _maybe_apply_legacy_retirement_kill_switch(state)
    assert fired is True
    assert state["worker_plan"]["legacy_retirement_kill_switch"] == {
        "metric": "total_appointments",
        "branch": "appointments",
    }


# ---------------------------------------------------------------------------
# Integration — _record_planner_v1_live_outcome stashes the report
# ---------------------------------------------------------------------------


def test_record_outcome_stashes_retirement_report_for_total_appointments():
    state = {
        "worker_plan": {},
        "_intent_result_v1": {"intent": "total_appointments"},
    }

    _record_planner_v1_live_outcome(
        state,
        "rejected_gate",
        gate_reason="confidence_low",
    )

    worker_plan = state["worker_plan"]
    assert worker_plan["planner_v1_live_outcome"] == {
        "outcome": "rejected_gate",
        "gate_reason": "confidence_low",
    }
    assert worker_plan["legacy_retirement_report"] == {
        "metric": "total_appointments",
        "status": "rejected_gate",
        "metric_source": "shadow_intent",
        "gate_reason": "confidence_low",
    }


def test_record_outcome_lexical_fallback_when_shadow_is_generic():
    """Production scenario: shadow classifier emits `data_simple`, v9
    compile fails. The integration must still surface a retirement
    report by lexically scanning the question — otherwise the legacy
    SQL branch silently looks like dead code when it's actually still
    serving traffic."""
    state = {
        "worker_plan": {},
        "_intent_result_v1": {"intent": "data_simple"},
        "original_question": "Total GP appointments in England last month",
    }

    _record_planner_v1_live_outcome(state, "rejected_v9_compile_failed")

    report = state["worker_plan"]["legacy_retirement_report"]
    assert report["metric"] == "total_appointments"
    assert report["status"] == "rejected_v9_compile_failed"
    assert report["metric_source"] == "lexical"


def test_record_outcome_skips_retirement_report_for_other_intents():
    """When the shadow intent is outside the retirement window the
    supervisor must not stamp a report onto worker_plan — this keeps
    aggregate logs free of noise from non-candidate metrics."""
    state = {
        "worker_plan": {},
        "_intent_result_v1": {"intent": "gp_fte"},
    }

    _record_planner_v1_live_outcome(state, "admitted", metric="gp_fte")

    worker_plan = state["worker_plan"]
    assert worker_plan["planner_v1_live_outcome"]["outcome"] == "admitted"
    assert "legacy_retirement_report" not in worker_plan
