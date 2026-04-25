"""Run the retirement-simulation corpus and aggregate the resulting
`legacy_retirement_report` payloads into a go/no-go decision.

The harness mirrors what production telemetry would surface after
1-2 weeks of live traffic, but compresses it into a single local run.
For each corpus question:

    1. Bootstrap state the way `node_init` would (dataset hint,
       `_hard_intent`, shadow `_intent_result_v1`).
    2. Call `_try_query_plan_v1_live(state, question)` — same code path
       production hits.
    3. Read `worker_plan["planner_v1_live_outcome"]` and
       `worker_plan["legacy_retirement_report"]`.
    4. Aggregate per metric: admission rate, rejection-reason histogram,
       and the count of "expectation matched" turns.

The harness does NOT execute SQL or hit the LLM planner — only the
admission decision. That's exactly the signal needed for the retirement
go/no-go call. Note however that v9 compilation may hit Athena via the
practice-name resolver; that's the same cost path production pays.
"""

from __future__ import annotations

import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

# Allow running as `python -m tools.retirement_simulation` AND as a
# direct script (`python tools/retirement_simulation/harness.py`).
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import gp_workforce_chatbot_backend_agent_v8 as backend  # noqa: E402
from gp_workforce_chatbot_backend_agent_v8 import (  # noqa: E402
    _classify_intent_shadow_fast,
    _infer_dataset_hint_from_question,
    _try_query_plan_v1_live,
    detect_hard_intent,
)
from tools.retirement_simulation.corpus import (  # noqa: E402
    FULL_CORPUS,
    SMOKE_CORPUS,
    CorpusQuestion,
)
from workforce.retirement import LEGACY_RETIREMENT_SHADOW_METRICS  # noqa: E402


# ---------------------------------------------------------------------------
# Per-question execution
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TurnResult:
    question: str
    category: str
    shadow_intent: str
    expected_outcome_class: str
    planner_v1_outcome: str
    gate_reason: str
    retirement_metric: str
    retirement_status: str
    expectation_met: bool


def _ensure_semantic_path_enabled() -> None:
    """Production has `USE_SEMANTIC_PATH=true` so v9 actually runs.
    Force it on for the simulation so we measure the same code path.
    The original env-driven default leaves it off in test environments.
    """
    if not getattr(backend, "USE_SEMANTIC_PATH", False):
        backend.USE_SEMANTIC_PATH = True


def _bootstrap_state(item: CorpusQuestion) -> Dict[str, Any]:
    """Build the minimum state shape `_try_query_plan_v1_live` reads.

    Mirrors what the dataset-router node does in production: infers
    workforce vs appointments vs cross-dataset from the question text
    so v9's lexical detection (which is dataset-aware) lines up with
    the same code path real traffic hits.
    """
    _ensure_semantic_path_enabled()
    inferred_dataset = (
        _infer_dataset_hint_from_question(item.question) or "workforce"
    )
    # Cross-dataset short-circuits before reaching `_try_query_plan_v1_live`
    # in production, but the harness only exercises the admission decision,
    # so collapse it down to the dominant dataset for consistency.
    if inferred_dataset == "cross_dataset":
        inferred_dataset = "appointments"
    state: Dict[str, Any] = {
        "dataset": inferred_dataset,
        "follow_up_context": dict(item.follow_up_context or {}),
        "semantic_state": {},
        "semantic_path": {},
        "worker_plan": {},
        "original_question": item.question,
        "question": item.question,
    }
    state["_hard_intent"] = detect_hard_intent(item.question) or ""
    state["_intent_result_v1"] = _classify_intent_shadow_fast(item.question, state)
    return state


def _classify_expectation(
    item: CorpusQuestion,
    outcome: str,
) -> bool:
    """Did planner-v1 do what the operator expected?"""
    if item.expected_outcome_class == "admit":
        return outcome.startswith("admitted")
    if item.expected_outcome_class == "reject_known":
        return outcome.startswith("rejected_")
    if item.expected_outcome_class == "not_reached":
        # Greeting / knowledge / follow-up turns: planner-v1 either
        # rejects them deterministically (rejected_follow_up,
        # rejected_v9_compile_failed) or never runs. Both are fine — the
        # point is the legacy branch isn't load-bearing here.
        return outcome != "admitted" and outcome != "admitted_hard_intent"
    return False


def run_one(item: CorpusQuestion) -> TurnResult:
    state = _bootstrap_state(item)

    # Skip v9 compilation for purely conversational turns to avoid an
    # Athena round-trip — the corpus already tags these as
    # "not_reached" and they're not in the retirement window anyway.
    skip_v9 = (
        item.expected_outcome_class == "not_reached"
        and item.category in {"greeting", "knowledge"}
    )
    if skip_v9:
        outcome = "not_reached_skipped"
    else:
        _try_query_plan_v1_live(state, item.question)
        outcome_payload = dict(
            (state.get("worker_plan") or {}).get("planner_v1_live_outcome") or {}
        )
        outcome = str(outcome_payload.get("outcome") or "no_outcome")

    worker_plan = dict(state.get("worker_plan") or {})
    outcome_payload = dict(worker_plan.get("planner_v1_live_outcome") or {})
    retirement_payload = dict(worker_plan.get("legacy_retirement_report") or {})

    return TurnResult(
        question=item.question,
        category=item.category,
        shadow_intent=str(
            (state.get("_intent_result_v1") or {}).get("intent") or ""
        ).strip().lower(),
        expected_outcome_class=item.expected_outcome_class,
        planner_v1_outcome=outcome,
        gate_reason=str(outcome_payload.get("gate_reason") or ""),
        retirement_metric=str(retirement_payload.get("metric") or ""),
        retirement_status=str(retirement_payload.get("status") or ""),
        expectation_met=_classify_expectation(item, outcome),
    )


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


@dataclass
class MetricSummary:
    metric: str
    total: int = 0
    admitted: int = 0
    rejection_breakdown: Counter = None  # type: ignore[assignment]
    not_reached: int = 0
    # Rejections that hand off to the LLM planner rather than the legacy
    # SQL branch — i.e. they don't keep legacy load-bearing.
    legacy_load_bearing_rejections: int = 0

    def __post_init__(self) -> None:
        if self.rejection_breakdown is None:
            self.rejection_breakdown = Counter()

    @property
    def admission_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.admitted / self.total

    @property
    def retirement_safe_rate(self) -> float:
        """Fraction of in-window turns that are NOT load-bearing on the
        legacy SQL branch. A gate-rejected turn is retirement-safe — it
        routes to the LLM planner, which the legacy branch would have
        done anyway. Only `rejected_v9_compile_failed` and
        `rejected_metric_not_live` keep legacy load-bearing."""
        if self.total == 0:
            return 0.0
        return (self.total - self.legacy_load_bearing_rejections) / self.total

    @property
    def go_signal(self) -> str:
        """Operator-facing recommendation, computed from
        retirement_safe_rate (not raw admission rate). Gate rejections
        and follow-ups don't block retirement — only v9-compile failures
        and metric_not_live do, since those are the cases where the
        legacy branch would actually be invoked today."""
        if self.total == 0:
            return "INSUFFICIENT_DATA"
        if self.retirement_safe_rate >= 0.95:
            return "GO"
        if self.retirement_safe_rate >= 0.80:
            return "PROBABLY_GO_CHECK_REJECTIONS"
        return "NO_GO_CLOSE_GAPS_FIRST"


def aggregate(results: List[TurnResult]) -> Dict[str, MetricSummary]:
    out: Dict[str, MetricSummary] = {}
    for metric in sorted(LEGACY_RETIREMENT_SHADOW_METRICS):
        out[metric] = MetricSummary(metric=metric)

    # Statuses that mean the legacy SQL branch is still actually being
    # invoked at runtime — the only signal that retirement is unsafe.
    _LEGACY_LOAD_BEARING_STATUSES = {
        "rejected_v9_compile_failed",
        "rejected_metric_not_live",
    }

    for r in results:
        if r.retirement_metric not in out:
            continue
        summary = out[r.retirement_metric]
        summary.total += 1
        if r.retirement_status == "admitted":
            summary.admitted += 1
        elif r.retirement_status == "not_reached":
            summary.not_reached += 1
        elif r.retirement_status.startswith("rejected_"):
            label = r.retirement_status
            if r.gate_reason:
                label = f"{label}({r.gate_reason})"
            summary.rejection_breakdown[label] += 1
            if r.retirement_status in _LEGACY_LOAD_BEARING_STATUSES:
                summary.legacy_load_bearing_rejections += 1

    return out


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _format_pct(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "  n/a"
    return f"{(numerator / denominator) * 100:5.1f}%"


def render_report(
    results: List[TurnResult],
    summaries: Dict[str, MetricSummary],
) -> str:
    lines: List[str] = []
    lines.append("=" * 78)
    lines.append("RETIREMENT-WINDOW SHADOW SIMULATION".center(78))
    lines.append("=" * 78)
    lines.append("")
    lines.append(f"Corpus size: {len(results)} turns")
    expectation_hits = sum(1 for r in results if r.expectation_met)
    lines.append(
        f"Operator expectation match: {expectation_hits}/{len(results)} "
        f"({_format_pct(expectation_hits, len(results)).strip()})"
    )
    lines.append("")
    lines.append("-" * 78)
    lines.append("PER-METRIC RETIREMENT SIGNAL")
    lines.append("-" * 78)
    for metric, summary in summaries.items():
        lines.append("")
        lines.append(f"  Metric: {metric}")
        lines.append(f"    Total in-window turns:  {summary.total}")
        lines.append(
            f"    Admitted by planner-v1: {summary.admitted}  "
            f"({_format_pct(summary.admitted, summary.total).strip()})"
        )
        lines.append(f"    Not reached:            {summary.not_reached}")
        lines.append(
            f"    Legacy load-bearing:    "
            f"{summary.legacy_load_bearing_rejections}  "
            f"(only v9_compile_failed / metric_not_live count)"
        )
        lines.append(
            f"    Retirement-safe rate:   "
            f"{_format_pct(summary.total - summary.legacy_load_bearing_rejections, summary.total).strip()}"
        )
        if summary.rejection_breakdown:
            lines.append(f"    Rejection breakdown:")
            for reason, count in sorted(
                summary.rejection_breakdown.items(),
                key=lambda kv: (-kv[1], kv[0]),
            ):
                lines.append(f"      - {reason:55s} x{count}")
        lines.append(f"    Recommendation: {summary.go_signal}")
    lines.append("")
    lines.append("-" * 78)
    lines.append("PER-TURN DETAIL")
    lines.append("-" * 78)
    by_category: Dict[str, List[TurnResult]] = defaultdict(list)
    for r in results:
        by_category[r.category].append(r)
    for category in sorted(by_category):
        lines.append("")
        lines.append(f"  [{category}]")
        for r in by_category[category]:
            mark = "OK " if r.expectation_met else "!! "
            lines.append(
                f"    {mark}{r.planner_v1_outcome:32s} "
                f"shadow={r.shadow_intent:22s} "
                f"q={r.question!r}"
            )
            if r.gate_reason:
                lines.append(f"        gate_reason={r.gate_reason}")
    lines.append("")
    lines.append("=" * 78)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------


def run(corpus: Tuple[CorpusQuestion, ...] = FULL_CORPUS) -> Tuple[
    List[TurnResult], Dict[str, MetricSummary]
]:
    results: List[TurnResult] = [run_one(item) for item in corpus]
    summaries = aggregate(results)
    return results, summaries


def main() -> int:
    # Default: full corpus. Pass `--smoke` for the fast subset.
    corpus = SMOKE_CORPUS if "--smoke" in sys.argv else FULL_CORPUS
    results, summaries = run(corpus)
    print(render_report(results, summaries))
    # Exit non-zero only if a candidate metric got NO_GO so this can
    # be a CI gate later if we want.
    if any(s.go_signal == "NO_GO_CLOSE_GAPS_FIRST" for s in summaries.values()):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
