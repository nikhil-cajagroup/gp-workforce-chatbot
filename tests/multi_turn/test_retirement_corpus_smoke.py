"""Smoke test for the retirement-simulation harness.

Locks the contract that the harness can run a tiny representative
subset of the corpus end-to-end against the real backend code path —
so refactors that break the simulation surface in CI rather than at
analysis time. Runs a 6-question subset to stay under ~30s.
"""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import tools.retirement_simulation.harness as harness
from tools.retirement_simulation.corpus import SMOKE_CORPUS
from workforce.retirement import build_legacy_retirement_report
from workforce.retirement import LEGACY_RETIREMENT_SHADOW_METRICS


def _install_offline_admission_stub(monkeypatch):
    def _fake_try_query_plan_v1_live(state, question):
        q = question.lower()
        if "dna" in q:
            metric = "dna_rate"
        elif "appointment" in q:
            metric = "total_appointments"
        elif "gp fte" in q:
            metric = "gp_fte"
        else:
            metric = ""

        if "top 10" in q:
            outcome = {
                "outcome": "rejected_gate",
                "metric": metric,
                "gate_reason": "superlative_ranking_unsupported",
            }
            triggered = False
        else:
            outcome = {"outcome": "admitted", "metric": metric}
            triggered = True

        state["worker_plan"] = dict(state.get("worker_plan") or {})
        state["worker_plan"]["planner_v1_live_outcome"] = outcome
        report = build_legacy_retirement_report(
            shadow_intent=(state.get("_intent_result_v1") or {}).get("intent"),
            planner_v1_live_outcome=outcome,
            question=question,
        )
        if report:
            state["worker_plan"]["legacy_retirement_report"] = report
        return triggered

    monkeypatch.setattr(harness, "_try_query_plan_v1_live", _fake_try_query_plan_v1_live)


def test_smoke_corpus_runs_without_exception(monkeypatch):
    _install_offline_admission_stub(monkeypatch)
    results, summaries = harness.run(SMOKE_CORPUS)
    # Sanity: we got one result per corpus question.
    assert len(results) == len(SMOKE_CORPUS)
    # Sanity: every retirement-window metric has a summary slot.
    assert set(summaries.keys()) == set(LEGACY_RETIREMENT_SHADOW_METRICS)


def test_smoke_corpus_produces_in_window_signal(monkeypatch):
    _install_offline_admission_stub(monkeypatch)
    """At least one corpus turn must end up classified into the
    retirement window — otherwise the harness wiring is broken (this is
    exactly the bug the simulation surfaced when shadow intent didn't
    match metric names)."""
    results, summaries = harness.run(SMOKE_CORPUS)
    in_window_total = sum(s.total for s in summaries.values())
    assert in_window_total > 0, (
        "No corpus turns landed in any retirement window — the metric "
        "detection chain is producing zero signal. Check "
        "build_legacy_retirement_report and the harness bootstrap."
    )


def test_smoke_corpus_records_admit_for_canonical_dna_question(monkeypatch):
    _install_offline_admission_stub(monkeypatch)
    """The most canonical DNA-rate question must hit `admitted` — if
    this regresses, planner-v1 has lost coverage of a metric we already
    proved was retirement-ready."""
    results, _ = harness.run(SMOKE_CORPUS)
    canonical = next(
        (r for r in results if "DNA rate nationally" in r.question),
        None,
    )
    assert canonical is not None
    assert canonical.planner_v1_outcome.startswith("admitted")
    assert canonical.retirement_metric == "dna_rate"
    assert canonical.retirement_status == "admitted"
