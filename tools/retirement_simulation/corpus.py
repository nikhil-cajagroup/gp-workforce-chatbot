"""Representative GP appointments + workforce question corpus.

These questions are synthesised from real NHS user behaviour patterns
across:
    - ICB programme managers tracking footprint volume / quality
    - GP partners and practice managers benchmarking their practice
    - Primary Care Network leads aggregating across member practices
    - NHS analysts producing monthly board reports
    - GP federation / training-hub managers monitoring pipeline

Phrasing intentionally mixes formal NHS register ("Integrated Care
Board"), casual abbreviation ("ICB"), code-only references ("P82001"),
and noisy chat-style ("how about telephone share?"). The retirement
decision must hold across all of these.

Each entry annotates the EXPECTED shadow intent and the operator's
expected outcome class so the harness can compute "did this match what
we thought would happen" alongside raw admission rates.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class CorpusQuestion:
    """One synthetic-production turn.

    Fields
    ------
    question:
        The user-facing utterance.
    category:
        Short tag for grouping in the report (e.g. "national_aggregate",
        "practice_scoped", "follow_up").
    expected_shadow_intent:
        What we expect `_classify_intent_shadow_fast` to emit. Matters
        most for retirement-window metrics (`total_appointments`,
        `dna_rate`) — those are the ones we're measuring.
    expected_outcome_class:
        Operator's prior on what planner-v1 live will do:
            "admit"        — we expect ~100% admission for a healthy retirement candidate
            "reject_known" — known unsupported (compares, rankings, trends)
            "not_reached"  — supervisor short-circuit (knowledge, follow-up etc.)
    follow_up_context:
        Mimics a multi-turn entity-memory state. Empty for fresh turns.
    notes:
        Free-text rationale for the entry.
    """

    question: str
    category: str
    expected_shadow_intent: str
    expected_outcome_class: str
    follow_up_context: Dict[str, Any] = field(default_factory=dict)
    notes: str = ""


# ---------------------------------------------------------------------------
# total_appointments — primary retirement candidate
# ---------------------------------------------------------------------------

TOTAL_APPOINTMENTS_QUESTIONS: Tuple[CorpusQuestion, ...] = (
    # Category A: national aggregate (should sail through)
    CorpusQuestion(
        question="Total GP appointments in England last month",
        category="national_aggregate",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="admit",
        notes="Canonical national total — exact phrasing from NHSE bulletin.",
    ),
    CorpusQuestion(
        question="How many appointments did GPs do nationally?",
        category="national_aggregate",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="admit",
        notes="Casual register, no time qualifier — defaults to latest.",
    ),
    CorpusQuestion(
        question="What's the total number of GP appointments in England?",
        category="national_aggregate",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="admit",
    ),
    CorpusQuestion(
        question="appointments total england",
        category="national_aggregate",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="admit",
        notes="Telegraphic — analyst typing fast.",
    ),

    # Category B: ICB / region scoped
    CorpusQuestion(
        question="Total appointments in NHS Greater Manchester ICB",
        category="icb_scoped",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="admit",
        notes="Named ICB — must resolve to entity_filters via v9.",
    ),
    CorpusQuestion(
        question="How many appointments in Kent and Medway last month?",
        category="icb_scoped",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="admit",
        notes="ICB without 'NHS' prefix — common in chat usage.",
    ),
    CorpusQuestion(
        question="GP appointments in London region",
        category="region_scoped",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="admit",
    ),

    # Category C: practice-scoped (hard_intent territory)
    CorpusQuestion(
        question="Total appointments at P82001 last month",
        category="practice_scoped",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="admit",
        notes="Practice code — v9 should attach as entity_filter.",
    ),
    CorpusQuestion(
        question="How many appointments at Keele Practice?",
        category="practice_scoped",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="admit",
        notes="Named practice — depends on Athena name resolver.",
    ),

    # Category D: trend / time-series — v9 produces a correct
    # national time-series for these, so planner-v1 admission is the
    # right answer (verified by inspecting semantic_path: grain=national,
    # metric_keys=['total_appointments']).
    CorpusQuestion(
        question="GP appointments trend over last 12 months",
        category="trend",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="admit",
        notes="v9 emits national time-series — correct behaviour.",
    ),
    CorpusQuestion(
        question="Show me total appointments month by month for the past year",
        category="trend",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="admit",
    ),
    CorpusQuestion(
        question="Has GP appointment volume gone up over time?",
        category="trend",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="admit",
    ),

    # Category E: ranking / comparison (out of scope for planner-v1 live)
    CorpusQuestion(
        question="Top 10 practices by appointment volume",
        category="ranking",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="reject_known",
        notes="'top practices' triggers existing ranking guard.",
    ),
    CorpusQuestion(
        question="Which practice had the most appointments last month?",
        category="ranking",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="reject_known",
    ),
    CorpusQuestion(
        question="Compare GP appointments at P82001 vs national average",
        category="comparison",
        expected_shadow_intent="total_appointments",
        expected_outcome_class="reject_known",
    ),

    # Category F: follow-up turns (rejected_follow_up by design)
    CorpusQuestion(
        question="And the trend?",
        category="follow_up",
        expected_shadow_intent="followup_data",
        expected_outcome_class="reject_known",
        follow_up_context={
            "entity_type": "national",
            "metric": "total_appointments",
        },
        notes="Inherits scope from prior turn — planner-v1 always rejects.",
    ),
    CorpusQuestion(
        question="What about Greater Manchester ICB?",
        category="follow_up",
        expected_shadow_intent="followup_data",
        expected_outcome_class="reject_known",
        follow_up_context={
            "entity_type": "national",
            "metric": "total_appointments",
        },
    ),
)


# ---------------------------------------------------------------------------
# dna_rate — second retirement candidate
# ---------------------------------------------------------------------------

DNA_RATE_QUESTIONS: Tuple[CorpusQuestion, ...] = (
    CorpusQuestion(
        question="What was the DNA rate nationally last month?",
        category="national_aggregate",
        expected_shadow_intent="dna_rate",
        expected_outcome_class="admit",
    ),
    CorpusQuestion(
        question="National DNA rate for GP appointments",
        category="national_aggregate",
        expected_shadow_intent="dna_rate",
        expected_outcome_class="admit",
    ),
    CorpusQuestion(
        question="Did not attend rate in England",
        category="national_aggregate",
        expected_shadow_intent="dna_rate",
        expected_outcome_class="admit",
        notes="DNA spelled out — common in formal reports.",
    ),
    CorpusQuestion(
        question="DNA rate at NHS North East and North Cumbria ICB",
        category="icb_scoped",
        expected_shadow_intent="dna_rate",
        expected_outcome_class="admit",
    ),
    CorpusQuestion(
        question="What's the DNA rate at P82001?",
        category="practice_scoped",
        expected_shadow_intent="dna_rate",
        expected_outcome_class="admit",
    ),
    CorpusQuestion(
        question="DNA rate at Keele Practice",
        category="practice_scoped",
        expected_shadow_intent="dna_rate",
        expected_outcome_class="admit",
    ),
    CorpusQuestion(
        question="DNA rate trend over last 12 months",
        category="trend",
        expected_shadow_intent="dna_rate",
        expected_outcome_class="admit",
        notes="v9 emits national time-series — correct behaviour.",
    ),
    CorpusQuestion(
        question="Has DNA rate gone up over time?",
        category="trend",
        expected_shadow_intent="dna_rate",
        expected_outcome_class="admit",
    ),
    CorpusQuestion(
        question="Top 10 practices by DNA rate",
        category="ranking",
        expected_shadow_intent="dna_rate",
        expected_outcome_class="reject_known",
    ),
    CorpusQuestion(
        question="Which ICB has the highest DNA rate?",
        category="ranking",
        expected_shadow_intent="dna_rate",
        expected_outcome_class="reject_known",
    ),
    CorpusQuestion(
        question="Compare DNA rate at P82001 vs national",
        category="comparison",
        expected_shadow_intent="dna_rate",
        expected_outcome_class="reject_known",
    ),
    CorpusQuestion(
        question="And DNA rate?",
        category="follow_up",
        expected_shadow_intent="followup_data",
        expected_outcome_class="reject_known",
        follow_up_context={
            "entity_type": "practice",
            "entity_name": "P82001",
        },
    ),
)


# ---------------------------------------------------------------------------
# Adjacent intents — sanity check that retirement telemetry stays
# silent for non-candidate metrics. These should NOT produce a retirement
# report, regardless of admission decision.
# ---------------------------------------------------------------------------

ADJACENT_QUESTIONS: Tuple[CorpusQuestion, ...] = (
    CorpusQuestion(
        question="Total GP FTE in England",
        category="adjacent_workforce",
        expected_shadow_intent="gp_fte",
        expected_outcome_class="admit",
        notes="Outside retirement window — must not stamp report.",
    ),
    CorpusQuestion(
        question="How many trainee GPs in England?",
        category="adjacent_workforce",
        expected_shadow_intent="trainee_gp_count",
        expected_outcome_class="reject_known",
        notes="Hard intent for trainee count — not safe-listed.",
    ),
    CorpusQuestion(
        question="Patients per GP at P82001",
        category="adjacent_workforce",
        expected_shadow_intent="patients_per_gp",
        expected_outcome_class="admit",
        notes="Safe-listed hard intent with practice scope.",
    ),
    CorpusQuestion(
        question="Hello",
        category="greeting",
        expected_shadow_intent="knowledge",
        expected_outcome_class="not_reached",
    ),
    CorpusQuestion(
        question="What is DNA rate?",
        category="knowledge",
        expected_shadow_intent="knowledge",
        expected_outcome_class="not_reached",
        notes="Definition request — knowledge route, not a metric.",
    ),
    CorpusQuestion(
        question="Face to face share nationally",
        category="adjacent_appointments",
        expected_shadow_intent="face_to_face_share",
        expected_outcome_class="admit",
        notes="Outside retirement window for now — sanity check only.",
    ),
)


# ---------------------------------------------------------------------------
# Public corpus
# ---------------------------------------------------------------------------

FULL_CORPUS: Tuple[CorpusQuestion, ...] = (
    *TOTAL_APPOINTMENTS_QUESTIONS,
    *DNA_RATE_QUESTIONS,
    *ADJACENT_QUESTIONS,
)


# A small deterministic subset for fast CI smoke tests. Stable across
# refactors — pin questions that exercise each major code path exactly
# once to keep the smoke test under 10 seconds.
SMOKE_CORPUS: Tuple[CorpusQuestion, ...] = (
    TOTAL_APPOINTMENTS_QUESTIONS[0],   # national admit
    TOTAL_APPOINTMENTS_QUESTIONS[12],  # ranking reject ("Top 10 practices...")
    TOTAL_APPOINTMENTS_QUESTIONS[15],  # follow-up reject
    DNA_RATE_QUESTIONS[0],             # national admit
    DNA_RATE_QUESTIONS[8],             # ranking reject ("Top 10 practices by DNA rate")
    ADJACENT_QUESTIONS[0],             # adjacent (no retirement report)
)


def corpus_size() -> int:
    return len(FULL_CORPUS)
