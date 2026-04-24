from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Literal, Optional

try:
    from pydantic import BaseModel, Field
except ImportError:
    class BaseModel:  # pragma: no cover - lightweight test fallback
        def __init__(self, **data: Any) -> None:
            for name, value in self.__class__.__dict__.items():
                if name.startswith("_") or callable(value):
                    continue
                setattr(self, name, value)
            for key, value in data.items():
                setattr(self, key, value)

        def model_dump(self) -> Dict[str, Any]:
            return dict(self.__dict__)

    def Field(  # type: ignore[misc]
        default: Any = None,
        *,
        default_factory: Optional[Any] = None,
        **_: Any,
    ) -> Any:
        if default_factory is not None:
            return default_factory()
        return default

from v9_metric_registry import metric_catalog_for_prompt
from v9_semantic_types import CompareSpec, SemanticRequest, TimeScope, TransformSpec
from v9_entity_aliases import find_city_icb_in_text


SUPPORTED_SEMANTIC_METRICS = {
    # --- Workforce: core ---
    "gp_headcount",
    "gp_fte",
    "nurse_fte",
    "nurse_headcount",
    "patients_per_gp",
    "registered_patients",
    # --- Workforce: additional staff groups ---
    "dpc_headcount",
    "dpc_fte",
    "admin_headcount",
    "admin_fte",
    # --- Workforce: GP sub-types ---
    "gp_partner_headcount",
    "gp_partner_fte",
    "salaried_gp_headcount",
    "salaried_gp_fte",
    "locum_gp_headcount",
    "locum_gp_fte",
    "registrar_gp_headcount",
    "gp_retainer_headcount",
    # --- Workforce: nurse sub-types ---
    "practice_nurse_headcount",
    "practice_nurse_fte",
    # --- Workforce: DPC sub-roles ---
    "pharmacist_headcount",
    "pharmacist_fte",
    "physician_associate_headcount",
    "physician_associate_fte",
    "hca_headcount",
    "hca_fte",
    "paramedic_headcount",
    "paramedic_fte",
    "physiotherapist_headcount",
    "physiotherapist_fte",
    "splw_headcount",
    "splw_fte",
    # --- Appointments: core ---
    "total_appointments",
    "face_to_face_appointments",
    "face_to_face_share",
    "telephone_appointments",
    "telephone_share",
    "video_online_appointments",
    "video_online_share",
    "home_visit_appointments",
    "home_visit_share",
    "gp_hcp_appointments",
    "gp_hcp_share",
    "dna_count",
    "dna_rate",
    "within_2_weeks_appointments",
    "within_2_weeks_share",
    "over_2_weeks_appointments",
    "over_2_weeks_share",
    # --- Workforce: nurse sub-types (extended) ---
    "advanced_nurse_practitioner_headcount",
    "advanced_nurse_practitioner_fte",
    "nurse_specialist_headcount",
    "nurse_specialist_fte",
    "dietician_headcount",
    "dietician_fte",
    "counsellor_headcount",
    "counsellor_fte",
    # --- Appointments: attendance & access ---
    "attended_count",
    "attended_rate",
    "same_day_appointments",
    "same_day_share",
    # --- Cross-dataset ---
    "appointments_per_gp_fte",
    "appointments_per_gp_headcount",
    "appointments_per_nurse_fte",
    "appointments_per_patient",
}


class SemanticTimeModel(BaseModel):
    mode: Literal["latest", "explicit"] = "latest"
    year: Optional[str] = None
    month: Optional[str] = None


class SemanticTransformModel(BaseModel):
    type: Literal["topn", "benchmark", "trend"]
    n: Optional[int] = None
    order: Literal["asc", "desc"] = "desc"
    scope: Optional[str] = None


class SemanticCompareModel(BaseModel):
    dimension: str
    values: List[str] = Field(default_factory=list)


class SemanticParseDecision(BaseModel):
    metrics: List[str] = Field(default_factory=list)
    entity_filters: Dict[str, str] = Field(default_factory=dict)
    group_by: List[str] = Field(default_factory=list)
    time: SemanticTimeModel = Field(default_factory=SemanticTimeModel)
    transforms: List[SemanticTransformModel] = Field(default_factory=list)
    compare: Optional[SemanticCompareModel] = None
    clarification_needed: bool = False
    confidence: Literal["high", "medium", "low"] = "medium"
    notes: str = ""


def semantic_parser_system_prompt() -> str:
    return f"""
You parse NHS GP analytics questions into a structured semantic request.

Only choose metrics from this catalog:
{metric_catalog_for_prompt()}

Rules:
- Never write SQL.
- Prefer the smallest valid metric set.
- Use transforms for top/bottom rankings and benchmarks.
- Use compare only when the user explicitly names multiple values for the same dimension.
- If the question is outside the supported metrics, return metrics=[] and confidence=low.
""".strip()


def parse_semantic_request_deterministic(
    question: str,
    dataset_hint: str = "",
    practice_name_resolver: Optional[Callable[[str, str, str], Optional[str]]] = None,
) -> Optional[SemanticRequest]:
    q = str(question or "").strip()
    if not q:
        return None
    q_low = q.lower()

    # Skip fast path only for "more than 28 days" percentage queries — no dedicated metric exists
    # for that specific bucket alone.  Same-day percentage is handled by the same_day_share metric.
    # national_category-specific queries are handled via entity_filters in _detect_entity_filters.
    if dataset_hint == "appointments":
        _over28_specific = (
            re.search(r"\b(more than|over|greater than)\s+(28|4 weeks?)\s+days?\b", q_low)
            and any(w in q_low for w in ("percent", "%", "proportion", "share", "how many"))
        )
        if _over28_specific:
            return None  # No specific metric; let LLM build CASE WHEN SQL

    metric = _detect_metric(q_low, dataset_hint=dataset_hint)
    if not metric:
        return None

    group_by = _detect_group_by(q_low)
    transforms = _detect_transforms(q_low)
    compare = _detect_compare(q, q_low, group_by, dataset_hint=dataset_hint)
    entity_filters = _detect_entity_filters(
        q,
        q_low,
        dataset_hint=dataset_hint,
        metric=metric,
        practice_name_resolver=practice_name_resolver,
    )
    if compare is not None and compare.dimension in {"practice_code", "gp_code"}:
        resolved_compare_values: List[str] = []
        for value in compare.values:
            explicit_code = re.search(r"\b([A-Za-z]\d{5})\b", str(value or ""), flags=re.IGNORECASE)
            if explicit_code:
                resolved_compare_values.append(explicit_code.group(1).upper())
                continue
            if practice_name_resolver is None:
                resolved_compare_values = []
                break
            try:
                resolved_code = practice_name_resolver(str(value or ""), dataset_hint, metric)
            except Exception:
                resolved_code = None
            if not resolved_code:
                resolved_compare_values = []
                break
            resolved_compare_values.append(str(resolved_code).strip().upper())
        if len(resolved_compare_values) >= 2:
            compare = CompareSpec(dimension="practice_code", values=resolved_compare_values[:5])

    if compare is not None:
        compare_filter_keys = {
            "region_name": {"region_name"},
            "icb_name": {"icb_name"},
            "sub_icb_name": {"sub_icb_name"},
            "pcn_name": {"pcn_name"},
            "practice_code": {"practice_code"},
            "gp_code": {"practice_code"},
        }.get(compare.dimension, set())
        for key in compare_filter_keys:
            entity_filters.pop(key, None)
    clarification_needed = _detect_clarification_needed(
        question=q,
        q_low=q_low,
        dataset_hint=dataset_hint,
        entity_filters=entity_filters,
        group_by=group_by,
        compare=compare,
    )
    confidence = _detect_confidence(
        question=q,
        q_low=q_low,
        metric=metric,
        entity_filters=entity_filters,
        group_by=group_by,
        transforms=transforms,
        compare=compare,
        clarification_needed=clarification_needed,
    )

    return SemanticRequest(
        metrics=[metric],
        entity_filters=entity_filters,
        group_by=group_by,
        time=TimeScope(mode="latest"),
        transforms=transforms,
        compare=compare,
        clarification_needed=clarification_needed,
        confidence=confidence,
    )


_FOLLOWUP_TRIGGERS = (
    "what about",
    "how about",
    "and for",
    "same for",
    "same but",
    "break this down",
    "break it down",
    "split this",
    "split it",
    "show this",
    "show it",
    "show that",
    "show the same",
    "instead",
    "now show",
    "now give",
    # Anaphoric references to prior entity / metric — these reliably signal a
    # follow-up regardless of question length.
    "this practice",
    "that practice",
    "my practice",
    "our practice",
    "this icb",
    "that icb",
    "this region",
    "that region",
    "those appointments",
    "these appointments",
    "those gps",
    "these gps",
    "of those",
    "of these",
    # Distribution / breakdown follow-ups about appointments — the prior turn's
    # entity context (practice/icb) needs to be inherited.
    "distribution of",
    "breakdown of",
    "split of",
    "mix of",
    # Comparison follow-ups like "how many were X compare to other"
    "compare to other",
    "compared to other",
    "compared with other",
    "vs other",
    "versus other",
)


def _looks_like_followup_question(q_low: str) -> bool:
    if any(trigger in q_low for trigger in _FOLLOWUP_TRIGGERS):
        return True
    # Short subject-only follow-up: "nurses?" "gps" "dna rate" — <= 5 tokens
    token_count = len(re.findall(r"\b[\w-]+\b", q_low))
    if token_count <= 5 and any(
        term in q_low
        for term in (
            "nurse",
            "gps",
            "gp ",
            "doctor",
            "dna",
            "appointment",
            "hcp",
            "mode",
            "booking",
            "patient",
            "fte",
            "headcount",
        )
    ):
        return True
    return False


_FOLLOWUP_UNSUPPORTED_CONCEPTS = (
    "gender",
    "male",
    "female",
    "sex split",
    "age band",
    "by age",
    "age breakdown",
    "national category",
    "ethnicity",
    "part time",
    "part-time",
    "full time",
    "full-time",
    # "staff role" / "detailed staff" removed — specific role metrics now supported
)


def derive_followup_semantic_request(
    question: str,
    prior: SemanticRequest,
    dataset_hint: str = "",
) -> Optional[SemanticRequest]:
    """Merge a follow-up question with the previous high-confidence semantic request.

    Inherits entity_filters, time, group_by, compare and transforms from the prior
    request, and only rewrites the metric (plus optional group_by / benchmark /
    trend) when the follow-up explicitly mentions them.

    Returns None when the follow-up cannot be merged cleanly — callers should then
    fall back to the v8 path.
    """
    q = str(question or "").strip()
    if not q or prior is None or not prior.metrics:
        return None
    q_low = q.lower()
    # Strip any annotation the backend appends, e.g. "(context: icb = ...)"
    stripped = re.sub(r"\s*\(context:.*$", "", q_low).strip() or q_low

    if not _looks_like_followup_question(stripped):
        return None

    # Step aside for concepts the v9 metric registry does not cover so the v8
    # fallback can handle them (gender/age/staff-role style breakdowns etc.).
    if any(concept in stripped for concept in _FOLLOWUP_UNSUPPORTED_CONCEPTS):
        return None

    prior_metric = str(prior.metrics[0] or "").strip()
    new_metric = _detect_followup_metric(stripped, prior_metric=prior_metric, dataset_hint=dataset_hint)
    if not new_metric:
        new_metric = prior_metric
    if new_metric not in SUPPORTED_SEMANTIC_METRICS:
        return None

    inherited_group_by = list(prior.group_by)
    new_group_by = _detect_group_by(stripped)
    group_by = new_group_by or inherited_group_by

    inherited_transforms = list(prior.transforms)
    detected_transforms = _detect_transforms(stripped)
    transforms: List[TransformSpec] = []
    seen_types = set()
    # Follow-up overrides: topn/benchmark/trend mentioned in the new question replace
    # the prior transforms of the same type; anything else is inherited.
    for t in detected_transforms:
        transforms.append(t)
        seen_types.add(t.type)
    for t in inherited_transforms:
        if t.type not in seen_types:
            transforms.append(t)

    # Inherit filters and time wholesale. compare rarely carries into a follow-up;
    # keep it only if the new question did not introduce a group_by or transform.
    entity_filters = dict(prior.entity_filters)
    compare = prior.compare if (not new_group_by and not detected_transforms) else None

    return SemanticRequest(
        metrics=[new_metric],
        entity_filters=entity_filters,
        group_by=group_by,
        time=prior.time,
        transforms=transforms,
        compare=compare,
        clarification_needed=False,
        confidence="high",
    )


def _detect_followup_metric(q_low: str, *, prior_metric: str, dataset_hint: str) -> str:
    special_metric = _detect_appointments_special_metric(q_low)
    if special_metric:
        return special_metric

    if _detect_appointments_breakdown_group_by(q_low):
        return "total_appointments"

    _fte_synonyms = ("fte", "full-time equivalent", "full time equivalent")
    _has_fte = any(term in q_low for term in _fte_synonyms)

    # In appointments sessions, phrases like "gp appointment" usually mean
    # GP-practice appointments rather than workforce GP headcount.
    if "appointment" in q_low and dataset_hint == "appointments":
        if "attendance rate" in q_low or (("attended" in q_low or "attend" in q_low) and _has_share_language(q_low)):
            return "attended_rate"
        if ("attended" in q_low or "attend" in q_low):
            return "attended_count"
        if "same day" in q_low or "same-day" in q_low:
            return "same_day_share" if _has_share_language(q_low) else "same_day_appointments"
        if "appointments per patient" in q_low or (prior_metric == "total_appointments" and "per patient" in q_low):
            return "appointments_per_patient"
        if "appointments per nurse" in q_low:
            return "appointments_per_nurse_fte"
        if "appointments per gp headcount" in q_low:
            return "appointments_per_gp_headcount"
        if "appointments per gp" in q_low or "appointments per gp fte" in q_low:
            return "appointments_per_gp_fte"
        if "dna rate" in q_low or ("dna" in q_low and "rate" in q_low) or (prior_metric == "total_appointments" and "dna" in q_low):
            return "dna_rate"
        return "total_appointments"

    # GP sub-types (before generic GP)
    if re.search(r"\bgp\s+partners?\b", q_low) or "partner gp" in q_low:
        return "gp_partner_fte" if _has_fte else "gp_partner_headcount"
    if re.search(r"\bsalaried\s+gp", q_low) or re.search(r"\bgp\s+salaried\b", q_low):
        return "salaried_gp_fte" if _has_fte else "salaried_gp_headcount"
    if re.search(r"\blocum\s+gp", q_low) or re.search(r"\bgp\s+locum", q_low):
        return "locum_gp_fte" if _has_fte else "locum_gp_headcount"
    if re.search(r"\bgp\s+registrars?\b", q_low) or re.search(r"\bgp\s+trainees?\b", q_low):
        return "registrar_gp_headcount"

    # Practice nurse (before generic nurse)
    if "practice nurse" in q_low:
        return "practice_nurse_fte" if _has_fte else "practice_nurse_headcount"

    # DPC sub-roles
    if "pharmacist" in q_low:
        return "pharmacist_fte" if _has_fte else "pharmacist_headcount"
    if "physician associate" in q_low:
        return "physician_associate_fte" if _has_fte else "physician_associate_headcount"
    if re.search(r"\bhcas?\b", q_low) or "healthcare assistant" in q_low:
        return "hca_fte" if _has_fte else "hca_headcount"
    if "paramedic" in q_low:
        return "paramedic_fte" if _has_fte else "paramedic_headcount"
    if re.search(r"\bphysio(?:therapists?)?\b", q_low):
        return "physiotherapist_fte" if _has_fte else "physiotherapist_headcount"
    if "social prescribing" in q_low or "link worker" in q_low or re.search(r"\bsplw\b", q_low):
        return "splw_fte" if _has_fte else "splw_headcount"

    # DPC & admin totals
    if re.search(r"\bdpc\b", q_low) or "direct patient care" in q_low:
        return "dpc_fte" if _has_fte else "dpc_headcount"
    if re.search(r"\badmin\b", q_low) or "non-clinical staff" in q_low or "receptionist" in q_low:
        return "admin_fte" if _has_fte else "admin_headcount"

    # Generic nurse & GP staff-group swaps
    if "nurse" in q_low:
        return "nurse_fte" if _has_fte else "nurse_headcount"
    if re.search(r"\bgps?\b", q_low) or "gp headcount" in q_low or "gp fte" in q_low or "doctor" in q_low:
        if prior_metric.endswith("_fte") or _has_fte:
            return "gp_fte"
        return "gp_headcount"

    # Attendance / same-day without the "appointment" keyword
    if "attendance rate" in q_low:
        return "attended_rate"
    if "same day" in q_low or "same-day" in q_low:
        return "same_day_share" if _has_share_language(q_low) else "same_day_appointments"

    # Metric swaps
    if "dna rate" in q_low or ("dna" in q_low and "rate" in q_low) or (prior_metric == "total_appointments" and "dna" in q_low):
        return "dna_rate"
    if "appointments per patient" in q_low or (prior_metric == "total_appointments" and "per patient" in q_low):
        return "appointments_per_patient"
    if "appointments per nurse" in q_low:
        return "appointments_per_nurse_fte"
    if "appointments per gp" in q_low or "appointments per gp fte" in q_low:
        return "appointments_per_gp_fte"
    if "appointments per gp headcount" in q_low:
        return "appointments_per_gp_headcount"
    if "total appointments" in q_low or ("appointment" in q_low and dataset_hint == "appointments"):
        return "total_appointments"
    if "patients per gp" in q_low:
        return "patients_per_gp"
    if "registered patients" in q_low or "patient count" in q_low or "number of patients" in q_low or "list size" in q_low:
        return "registered_patients"
    return ""


def decision_to_semantic_request(decision: SemanticParseDecision) -> SemanticRequest:
    return SemanticRequest(
        metrics=list(decision.metrics),
        entity_filters=dict(decision.entity_filters),
        group_by=list(decision.group_by),
        time=TimeScope(
            mode=decision.time.mode,
            year=decision.time.year,
            month=decision.time.month,
        ),
        transforms=[
            TransformSpec(type=item.type, n=item.n, order=item.order, scope=item.scope)
            for item in decision.transforms
        ],
        compare=(
            CompareSpec(dimension=decision.compare.dimension, values=list(decision.compare.values))
            if decision.compare is not None
            else None
        ),
        clarification_needed=bool(decision.clarification_needed),
        confidence=decision.confidence,
    )


def semantic_request_to_dict(request: SemanticRequest) -> Dict[str, Any]:
    return {
        "metrics": list(request.metrics),
        "entity_filters": dict(request.entity_filters),
        "group_by": list(request.group_by),
        "time": {
            "mode": request.time.mode,
            "year": request.time.year,
            "month": request.time.month,
        },
        "transforms": [
            {
                "type": transform.type,
                "n": transform.n,
                "order": transform.order,
                "scope": transform.scope,
            }
            for transform in request.transforms
        ],
        "compare": (
            {
                "dimension": request.compare.dimension,
                "values": list(request.compare.values),
            }
            if request.compare is not None
            else None
        ),
        "clarification_needed": bool(request.clarification_needed),
        "confidence": request.confidence,
    }


def _detect_clarification_needed(
    *,
    question: str,
    q_low: str,
    dataset_hint: str,
    entity_filters: Dict[str, str],
    group_by: List[str],
    compare: Optional[CompareSpec],
) -> bool:
    if _has_unresolved_practice_reference(q_low, entity_filters):
        return True
    if _looks_like_named_practice_without_code(question, q_low, entity_filters):
        return True
    if _has_unsupported_appointments_shape(
        dataset_hint=dataset_hint,
        entity_filters=entity_filters,
        group_by=group_by,
        compare=compare,
    ):
        return True
    return False


def _detect_confidence(
    *,
    question: str,
    q_low: str,
    metric: str,
    entity_filters: Dict[str, str],
    group_by: List[str],
    transforms: List[TransformSpec],
    compare: Optional[CompareSpec],
    clarification_needed: bool,
) -> Literal["high", "medium", "low"]:
    if not metric:
        return "low"

    if clarification_needed:
        return "low"

    if _has_followup_language(q_low):
        return "low"

    if _looks_like_named_practice_without_code(question, q_low, entity_filters):
        return "low"

    if _has_unresolved_place_reference(q_low, entity_filters):
        return "low"

    score = 2
    if entity_filters:
        score += 1
    if group_by:
        score += 1
    if transforms:
        score += 1
    if compare is not None:
        score += 1
    # +1 if explicitly national OR implicitly national (no entity filter, no group_by →
    # there is only one sensible aggregate grain: the national total).
    _is_implicit_national = not entity_filters and not group_by
    if any(term in q_low for term in ("national", "nationally", "latest month", "latest")) or _is_implicit_national:
        score += 1

    token_count = len(re.findall(r"\b[\w-]+\b", question))
    # Questions up to 20 tokens can still be high-confidence if they have
    # strong signals (metric + entity_filter + national/latest).  The previous
    # 15-token ceiling rejected well-formed NHS questions like "What share of
    # appointments were face to face in NHS Greater Manchester ICB in the
    # latest month?" (17 tokens).  Beyond 20 tokens, cap at medium.
    if token_count > 20:
        return "medium" if score >= 4 else "low"

    if score >= 3:
        return "high"
    return "medium"


def _has_unsupported_appointments_shape(
    *,
    dataset_hint: str,
    entity_filters: Dict[str, str],
    group_by: List[str],
    compare: Optional[CompareSpec],
) -> bool:
    if dataset_hint != "appointments":
        return False
    uses_national_category = "national_category" in entity_filters or "national_category" in group_by
    if not uses_national_category:
        return False
    if any(dim in {"region_name", "icb_name"} for dim in group_by):
        return True
    if any(key in {"region_name", "icb_name"} for key in entity_filters):
        return True
    if compare is not None and compare.dimension in {"region_name", "icb_name"}:
        return True
    return False


def _has_followup_language(q_low: str) -> bool:
    return any(
        marker in q_low
        for marker in (
            "what about",
            "show this",
            "show that",
            "compare this",
            "same but",
            "same for",
            "instead of",
            "those",
            "these",
            "that one",
            "this one",
        )
    )


def _looks_like_named_practice_without_code(
    question: str,
    q_low: str,
    entity_filters: Dict[str, str],
) -> bool:
    if "practice_code" in entity_filters:
        return False
    if not any(token in q_low for token in ("medical centre", "health centre", "surgery", "clinic", "practice")):
        return False
    # Generic workforce phrases that mention "practices" but are NOT naming a specific practice
    _generic_practice_phrases = (
        "gp practice",
        "general practice",
        "primary care",
        "all practice",
    )
    if any(phrase in q_low for phrase in _generic_practice_phrases):
        return False
    return bool(re.search(r"\b(for|at|in)\s+[A-Z][A-Za-z0-9'& .-]{4,}", question))


def _has_unresolved_place_reference(q_low: str, entity_filters: Dict[str, str]) -> bool:
    if _has_unresolved_practice_reference(q_low, entity_filters):
        return True
    if not re.search(r"\bin\s+[a-z]", q_low):
        return False
    if any(key in entity_filters for key in ("region_name", "icb_name", "sub_icb_name", "pcn_name", "practice_code")):
        return False
    ignored_targets = (
        "the latest month",
        "latest month",
        "the latest",
        "latest",
        "england",
        # "in GP practices" / "in general practice" — employment location, not a
        # geographic place that needs resolving.
        "in gp practice",
        "in general practice",
        "in primary care",
        "in nhs",
    )
    # "nationally" / "across england" make the geographic scope explicit.
    if any(term in q_low for term in ("nationally", "across england", "in england")):
        return False
    return not any(target in q_low for target in ignored_targets)


def _has_unresolved_practice_reference(q_low: str, entity_filters: Dict[str, str]) -> bool:
    if "practice_code" in entity_filters:
        return False
    return any(
        token in q_low
        for token in (
            "my practice",
            "our practice",
            "this practice",
            "that practice",
        )
    )


def _has_share_language(q_low: str) -> bool:
    return any(
        term in q_low
        for term in ("share", "proportion", "percentage", "percent", "rate", "what fraction")
    )


def _detect_appointments_special_metric(q_low: str) -> str:
    share_metric = _has_share_language(q_low)

    if "face-to-face" in q_low or "face to face" in q_low:
        return "face_to_face_share" if share_metric else "face_to_face_appointments"
    if "telephone" in q_low:
        return "telephone_share" if share_metric else "telephone_appointments"
    if "home visit" in q_low and "care home visit" not in q_low:
        # "care home visit" is a national_category value, not an appt_mode home visit
        return "home_visit_share" if share_metric else "home_visit_appointments"
    if "video conference" in q_low or ("video" in q_low and "appointment" in q_low) or ("online" in q_low and "appointment" in q_low):
        return "video_online_share" if share_metric else "video_online_appointments"
    if "within 2 weeks" in q_low or "within two weeks" in q_low:
        return "within_2_weeks_share" if share_metric else "within_2_weeks_appointments"
    if any(term in q_low for term in ("more than 2 weeks", "more than two weeks", "over 2 weeks", "over two weeks")):
        return "over_2_weeks_share" if share_metric else "over_2_weeks_appointments"
    # HCP-type filtered: "appointments with a GP", "seen by a GP", "GP appointments"
    # Must come after mode checks so "face-to-face GP appointments" doesn't
    # short-circuit here.
    # "by GP FTE / by GP headcount / by GP count" are metric-name phrases, not
    # "appointments with a GP" — exclude them with a negative lookahead.
    if re.search(
        r"\b(?:with\s+(?:a\s+)?gp|seen\s+by\s+(?:a\s+)?gp"
        r"|by\s+(?:a\s+)?gp(?!\s*(?:fte|hc|headcount|count|partner|salaried|locum|registrar|retainer|trainees?)))\b",
        q_low,
    ):
        return "gp_hcp_share" if share_metric else "gp_hcp_appointments"
    # NOTE: bare "GP appointments" (without "with/by a GP") means "general
    # practice appointments" in NHS usage — do NOT map it to gp_hcp_*.
    return ""


def _detect_metric(q_low: str, dataset_hint: str = "") -> str:
    # Compute FTE-intent once; reused throughout this function.
    _fte_synonyms = ("fte", "full-time equivalent", "full time equivalent")
    _has_fte = any(term in q_low for term in _fte_synonyms)

    # Only run appointments-specific detection when the dataset hint is appointments
    # (or unset). Prevents "gp" in phrases like "employed by GP practices" from
    # being mis-detected as a GP HCP appointments metric in workforce queries.
    if dataset_hint != "workforce":
        special_metric = _detect_appointments_special_metric(q_low)
        if special_metric:
            return special_metric
        if _detect_appointments_breakdown_group_by(q_low):
            return "total_appointments"

    # ── Appointments: attendance & same-day ──────────────────────
    # "attendance rate" / "attended appointments" → attended_rate / attended_count
    if "attendance rate" in q_low or "attendance percentage" in q_low:
        return "attended_rate"
    if ("attended" in q_low or "attend" in q_low) and "appointment" in q_low:
        if _has_share_language(q_low) or "rate" in q_low:
            return "attended_rate"
        return "attended_count"
    # Same-day / walk-in / urgent same-day
    if "same day" in q_low or "same-day" in q_low or "sameday" in q_low:
        if _has_share_language(q_low):
            return "same_day_share"
        return "same_day_appointments"

    if "appointments per patient" in q_low or ("appointment" in q_low and "per patient" in q_low):
        return "appointments_per_patient"
    if "appointments per nurse" in q_low or ("appointment" in q_low and "per nurse" in q_low):
        return "appointments_per_nurse_fte"
    if "appointments per gp headcount" in q_low or "appointments per gp hc" in q_low:
        return "appointments_per_gp_headcount"
    if "appointments per gp" in q_low or ("appointment" in q_low and "per gp" in q_low):
        return "appointments_per_gp_fte"
    if "dna rate" in q_low or ("dna" in q_low and "rate" in q_low):
        return "dna_rate"
    if ("dna count" in q_low or "number of dna" in q_low or "how many dna" in q_low
            or (re.search(r"\bdna\b", q_low) and "appointment" in q_low
                and not re.search(r"\brate\b|\bpercent|\bshare\b|\bproportion\b", q_low))):
        return "dna_count"
    if "total appointments" in q_low or ("appointment" in q_low and dataset_hint == "appointments"):
        return "total_appointments"
    if "patients per gp" in q_low or "patients-per-gp" in q_low:
        return "patients_per_gp"
    if "registered patients" in q_low or "patient count" in q_low or "number of patients" in q_low or "list size" in q_low:
        return "registered_patients"

    # ── Appointments: consultation / national category breakdown ──
    if ("consultation category" in q_low or "national category" in q_low
            or "consultation type" in q_low or "category breakdown" in q_low
            or re.search(r"\bappointment\s+categor", q_low)
            or re.search(r"\bconsultation\s+categor", q_low)
            or re.search(r"\bcategor(?:ies|y)\s+(?:of\s+)?(?:appointment|consultation)", q_low)
            or (re.search(r"\bcategor(?:ies|y)\b", q_low) and "appointment" in q_low)):
        return "total_appointments"

    # ── Workforce: nurse sub-types (extended) ────────────────────
    if re.search(r"\badv(?:anced)?\s+nurse\s+prac", q_low) or re.search(r"\banps?\b", q_low):
        return "advanced_nurse_practitioner_fte" if _has_fte else "advanced_nurse_practitioner_headcount"
    if "nurse specialist" in q_low or re.search(r"\bspecialist\s+nurse\b", q_low):
        return "nurse_specialist_fte" if _has_fte else "nurse_specialist_headcount"
    if re.search(r"\bdietici[ae]ns?\b", q_low) or re.search(r"\bdieti[ct]ians?\b", q_low):
        return "dietician_fte" if _has_fte else "dietician_headcount"
    if re.search(r"\bcounsell?ors?\b", q_low) or ("therapist" in q_low and not "physio" in q_low and not "occupational" in q_low):
        return "counsellor_fte" if _has_fte else "counsellor_headcount"

    # ── Workforce: GP sub-types (before generic GP detection) ────
    if re.search(r"\bgp\s+partners?\b", q_low) or "partner gp" in q_low or re.search(r"\bgp\s+providers?\b", q_low):
        return "gp_partner_fte" if _has_fte else "gp_partner_headcount"
    if re.search(r"\bsalaried\s+gp", q_low) or re.search(r"\bgp\s+salaried\b", q_low):
        return "salaried_gp_fte" if _has_fte else "salaried_gp_headcount"
    if re.search(r"\blocum\s+gp", q_low) or re.search(r"\bgp\s+locum", q_low):
        return "locum_gp_fte" if _has_fte else "locum_gp_headcount"
    if re.search(r"\bgp\s+registrars?\b", q_low) or re.search(r"\bgp\s+trainees?\b", q_low) or "training grade" in q_low:
        return "registrar_gp_headcount"
    if re.search(r"\bgp\s+retainers?\b", q_low) or "retainer gp" in q_low:
        return "gp_retainer_headcount"

    # ── Workforce: nurse sub-types (before generic nurse detection) ──
    if "practice nurse" in q_low:
        return "practice_nurse_fte" if _has_fte else "practice_nurse_headcount"

    # ── Workforce: DPC sub-roles ─────────────────────────────────
    if "pharmacist" in q_low:
        return "pharmacist_fte" if _has_fte else "pharmacist_headcount"
    if "physician associate" in q_low or "physician's associate" in q_low:
        return "physician_associate_fte" if _has_fte else "physician_associate_headcount"
    if re.search(r"\bhcas?\b", q_low) or "healthcare assistant" in q_low or "health care assistant" in q_low:
        return "hca_fte" if _has_fte else "hca_headcount"
    if "paramedic" in q_low:
        return "paramedic_fte" if _has_fte else "paramedic_headcount"
    if re.search(r"\bphysio(?:therapists?)?\b", q_low):
        return "physiotherapist_fte" if _has_fte else "physiotherapist_headcount"
    if "social prescribing" in q_low or "link worker" in q_low or re.search(r"\bsplw\b", q_low):
        return "splw_fte" if _has_fte else "splw_headcount"

    # ── Workforce: DPC & admin totals ────────────────────────────
    if re.search(r"\bdpc\b", q_low) or "direct patient care" in q_low:
        return "dpc_fte" if _has_fte else "dpc_headcount"
    if (re.search(r"\badmin\b", q_low) and not re.search(r"\badminister\b|\badministration\b", q_low)):
        return "admin_fte" if _has_fte else "admin_headcount"
    if "non-clinical staff" in q_low or "non clinical staff" in q_low:
        return "admin_fte" if _has_fte else "admin_headcount"
    if "receptionist" in q_low:
        return "admin_headcount"

    # ── Workforce: nurse & GP (generic) ─────────────────────────
    if re.search(r"\bnurse\s+fte\b", q_low) or (_has_fte and "nurse" in q_low):
        return "nurse_fte"
    if re.search(r"\bhow\s+many\s+nurses?\b", q_low) or "nurse headcount" in q_low or "number of nurses" in q_low:
        return "nurse_headcount"
    if re.search(r"\bgp\s+fte\b", q_low) or (_has_fte and re.search(r"\bgp\b", q_low)):
        return "gp_fte"
    if re.search(r"\bhow\s+many\s+gps?\b", q_low) or "gp headcount" in q_low or "number of gps" in q_low:
        return "gp_headcount"
    return ""


def _detect_group_by(q_low: str) -> List[str]:
    appointment_breakdown_group = _detect_appointments_breakdown_group_by(q_low)
    if appointment_breakdown_group:
        # Multi-dimensional: national_category + geographic breakdown.
        # We preserve the user's requested grain here, even when a later compiler
        # validation may need to clarify unsupported ICB/region combinations.
        if appointment_breakdown_group == "national_category":
            if (
                " by sub-icb" in q_low or " by sub icb" in q_low
                or re.search(r"\b(?:top|bottom|lowest|highest|fewest|most)\s+\d+\s+sub[- ]icbs?\b", q_low)
                or re.search(r"\bacross\s+(?:all\s+)?sub[- ]icbs?\b", q_low)
            ):
                return ["sub_icb_location_name", "national_category"]
            if (
                " by icb" in q_low
                or re.search(r"\bicbs?\s+(?:by|ranked\s+by|with)\b", q_low)
                or re.search(r"\b(?:top|bottom|lowest|highest|fewest|most)\s+\d+\s+icbs?\b", q_low)
                or re.search(r"\bacross\s+(?:all\s+)?icbs?\b", q_low)
            ):
                return ["icb_name", "national_category"]
            if (
                " by region" in q_low
                or " by nhs region" in q_low
                or re.search(r"\b(?:top|bottom|lowest|highest|fewest|most)\s+\d+\s+regions?\b", q_low)
                or re.search(r"\bacross\s+(?:all\s+)?regions?\b", q_low)
            ):
                return ["region_name", "national_category"]
            if (
                " by pcn" in q_low
                or re.search(r"\bpcns?\s+(?:by|ranked\s+by|with)\b", q_low)
                or re.search(r"\bacross\s+(?:all\s+)?pcns?\b", q_low)
            ):
                return ["pcn_name", "national_category"]
        return [appointment_breakdown_group]
    # ICB-level group_by: "by ICB", "which ICBs", "top 5 ICBs", "ICBs by X",
    # "across all ICBs", "ICBs ranked by X".
    if (
        " by icb" in q_low
        or re.search(r"\bwhich\s+icbs?\b", q_low)
        or re.search(r"\bwhich\s+icb\s+has\b", q_low)
        or re.search(r"\b(?:top|bottom|lowest|highest|fewest|most|best|worst)\s+\d+\s+icbs?\b", q_low)
        or re.search(r"\bicbs?\s+(?:by|ranked\s+by|with|having)\b", q_low)
        or re.search(r"\bacross\s+(?:all\s+)?icbs?\b", q_low)
    ):
        return ["icb_name"]
    # Region-level group_by.
    if (
        " by region" in q_low
        or " by nhs region" in q_low
        or "which regions" in q_low
        or re.search(r"\bwhich\s+regions?\b", q_low)
        or re.search(r"\b(?:top|bottom|lowest|highest|fewest|most|best|worst)\s+\d+\s+regions?\b", q_low)
        or re.search(r"\bregions?\s+(?:by|ranked\s+by|with|having)\b", q_low)
        or re.search(r"\bacross\s+(?:all\s+)?regions?\b", q_low)
    ):
        return ["region_name"]
    # Sub-ICB-level group_by.
    if (
        " by sub-icb" in q_low
        or " by sub icb" in q_low
        or re.search(r"\bwhich\s+sub[- ]icbs?\b", q_low)
        or re.search(r"\b(?:top|bottom|lowest|highest|fewest|most|best|worst)\s+\d+\s+sub[- ]icbs?\b", q_low)
        or re.search(r"\bacross\s+(?:all\s+)?sub[- ]icbs?\b", q_low)
    ):
        return ["sub_icb_name"]
    # PCN-level group_by.
    if (
        " by pcn" in q_low
        or re.search(r"\bwhich\s+pcns?\b", q_low)
        or re.search(r"\bwhich\s+pcn\s+has\b", q_low)
        or re.search(r"\b(?:top|bottom|lowest|highest|fewest|most|best|worst)\s+\d+\s+pcns?\b", q_low)
        or re.search(r"\bacross\s+(?:all\s+)?pcns?\b", q_low)
    ):
        return ["pcn_name"]
    # Practice-level group_by.
    if (
        " by practice" in q_low
        or "which practices" in q_low
        or re.search(r"\b(?:top|bottom|lowest|highest|fewest|most|best|worst)\s+\d+\s+practices?\b", q_low)
        or re.search(r"\bpractices?\s+(?:by|ranked\s+by|with|having)\b", q_low)
    ):
        return ["practice_code"]
    return []


def _detect_appointments_breakdown_group_by(q_low: str) -> str:
    if any(term in q_low for term in (
        "hcp type", "hcp types", "by hcp",
        "health care professional type", "healthcare professional type",
        "healthcare professional", "health care professional",
        "by professional type", "professional type breakdown",
        "staff type breakdown",
    )):
        return "hcp_type"
    if any(
        term in q_low
        for term in (
            "appointment mode breakdown",
            "appointment mode",
            "appointments by mode",
            "appointment modes",
            "appt mode",
            "by mode",
        )
    ):
        return "appt_mode"
    # Generic "distribution / breakdown / mix / split" follow-ups about
    # appointments mean a mode breakdown unless the question explicitly mentions
    # another grouping (HCP type, lead time). Catches phrasings users actually
    # type: "how was the distribution of those appointments", "give me the
    # appointments breakdown", "what is the mix at this practice", etc.
    appt_breakdown_terms = (
        "distribution",
        "breakdown",
        "split",
        "mix",
        "make up",
        "make-up",
        "broken down",
        "break down",
    )
    if any(term in q_low for term in appt_breakdown_terms) and (
        "appointment" in q_low or "appt" in q_low
    ):
        return "appt_mode"
    if any(
        term in q_low
        for term in (
            "booking lead time",
            "booking window",
            "time between booking and appointment",
            "time between book and appt",
            "time from booking",
            "book and appointment",
        )
    ):
        return "time_between_book_and_appt"
    if ("consultation category" in q_low or "national category" in q_low
            or "consultation type" in q_low or "category breakdown" in q_low
            or re.search(r"\bby\s+categor", q_low)
            or re.search(r"\bappointment\s+categor", q_low)
            or re.search(r"\bconsultation\s+categor", q_low)
            or re.search(r"\bcategor(?:ies|y)\s+(?:of\s+)?(?:appointment|consultation)", q_low)
            or (re.search(r"\bcategor(?:ies|y)\b", q_low) and "appointment" in q_low)):
        return "national_category"
    return ""


def _detect_transforms(q_low: str) -> List[TransformSpec]:
    transforms: List[TransformSpec] = []
    top_match = re.search(r"\btop\s+(\d+)\b", q_low)
    if top_match:
        transforms.append(TransformSpec(type="topn", n=int(top_match.group(1)), order="desc"))
    elif "highest" in q_low or "most" in q_low:
        transforms.append(TransformSpec(type="topn", n=10, order="desc"))
    elif "lowest" in q_low or "fewest" in q_low or "bottom" in q_low:
        transforms.append(TransformSpec(type="topn", n=10, order="asc"))

    if any(term in q_low for term in ("national average", "compared to england", "vs national", "vs england", "nationally")):
        transforms.append(TransformSpec(type="benchmark", scope="national"))
    elif any(term in q_low for term in ("average icb", "icb average", "pcn peers", "within its icb")):
        transforms.append(TransformSpec(type="benchmark", scope="icb"))
    elif any(term in q_low for term in ("average region", "regional average")):
        transforms.append(TransformSpec(type="benchmark", scope="region"))
    elif any(term in q_low for term in ("sub-icb average", "sub icb average")):
        transforms.append(TransformSpec(type="benchmark", scope="sub_icb"))
    elif any(term in q_low for term in ("pcn average", "within its pcn")):
        transforms.append(TransformSpec(type="benchmark", scope="pcn"))

    trend_window = 12
    trend_match = re.search(r"last\s+(\d+)\s+months?", q_low)
    if trend_match:
        trend_window = int(trend_match.group(1))
    elif "last 4 quarters" in q_low:
        trend_window = 12
    if any(term in q_low for term in ("trend", "over time", "last 12 months", "over the past year", "past year", "last 4 quarters", "historical")):
        transforms.append(TransformSpec(type="trend", n=trend_window))

    return transforms


def _detect_compare(
    question: str,
    q_low: str,
    group_by: List[str],
    dataset_hint: str = "",
) -> Optional[CompareSpec]:
    if "compare " not in q_low and " vs " not in q_low:
        return None

    # ── Extract raw tokens from the comparison phrase ─────────────────────────
    raw_values: List[str] = []
    if " vs " in question:
        raw_values = [
            part.strip(" ?")
            for part in re.split(r"\bvs\b", question, flags=re.IGNORECASE)
            if part.strip()
        ]
    elif "compare" in q_low:
        parts = re.split(r"\bcompare\b", question, flags=re.IGNORECASE)
        if len(parts) > 1:
            tail = parts[-1]
            raw_values = [
                part.strip(" ?,")
                for part in re.split(r"\band\b|,", tail)
                if part.strip()
            ]

    # ── Strip sentence scaffolding that is not part of the entity name ─────────
    # Leading: "Compare ", "Show " etc. from sentence structure.
    _STRIP_LEADING = re.compile(
        r"^(?:compare\s+|show\s+(?:me\s+)?|what\s+(?:is|are)\s+(?:the\s+)?)",
        re.IGNORECASE,
    )
    # Trailing: metric/scope words appended by the user for clarity, not part of
    # the entity name ("Keele Practice appointments" → "Keele Practice").
    cleaned: List[str] = []
    for v in raw_values:
        v = _STRIP_LEADING.sub("", v).strip()
        for pattern in (
            r"\s+(?:total\s+)?appointments?$",
            r"\s+gp\s+(?:fte|headcount|hc|count)$",
            r"\s+gps?\s+(?:fte|headcount|hc|count)$",
            r"\s+(?:fte|headcount|hc|count)$",
            r"\s+(?:data|figures?|nationally|stats?)$",
        ):
            v = re.sub(pattern, "", v, flags=re.IGNORECASE).strip()
        if len(v) > 2:
            cleaned.append(v)
    values = cleaned

    if len(values) < 2:
        return None

    # ── Infer dimension from explicit group_by or entity type in values ────────
    if group_by:
        dimension = group_by[0]
    else:
        combined = " ".join(values)
        combined_low = combined.lower()
        if re.search(r"\bnhs\s+\S+.*\bicb\b|\bicb\b", combined_low):
            dimension = "icb_name"
        elif re.search(r"\bpcn\b", combined_low):
            dimension = "pcn_name"
        elif re.search(
            r"\b(?:north east and yorkshire|north west|midlands|east of england"
            r"|london|south east|south west)\b",
            combined_low,
        ):
            dimension = "region_name"
        elif re.search(r"\b[A-Z]\d{5}\b", combined):
            # GP practice codes like P82001
            dimension = "practice_code"
        elif re.search(
            r"\b(?:surgery|medical\s+centre|health\s+centre|practice|clinic)\b",
            combined_low,
        ):
            dimension = "practice_code"
        else:
            # Default: ICB comparisons are the most common free-text case
            dimension = "icb_name"

    return CompareSpec(dimension=dimension, values=values[:5])


def _detect_entity_filters(
    question: str,
    q_low: str,
    *,
    dataset_hint: str = "",
    metric: str = "",
    practice_name_resolver: Optional[Callable[[str, str, str], Optional[str]]] = None,
) -> Dict[str, str]:
    filters: Dict[str, str] = {}
    region_match = re.search(r"\bin\s+(london|midlands|south west|south east|east of england|north west|north east and yorkshire)\b", q_low)
    if region_match:
        filters["region_name"] = region_match.group(1).title()
    icb_match = re.search(r"\bin\s+(nhs\s+.+?\sicb)\b", question, flags=re.IGNORECASE)
    if icb_match:
        filters["icb_name"] = icb_match.group(1).strip()
    pcn_match = re.search(r"\b(?:in|for|at)\s+([A-Za-z0-9&,'()\/.\- ]+?\s+PCN)\b", question, flags=re.IGNORECASE)
    if pcn_match:
        filters["pcn_name"] = " ".join(pcn_match.group(1).split())
    sub_icb_match = re.search(r"\b(?:in|for|at)\s+(nhs\s+.+?\sicb\s*-\s*[a-z0-9]+)\b", question, flags=re.IGNORECASE)
    if sub_icb_match:
        filters["sub_icb_name"] = " ".join(sub_icb_match.group(1).split())
    practice_code_match = re.search(r"\b([a-z]\d{5})\b", question, flags=re.IGNORECASE)
    if practice_code_match:
        filters["practice_code"] = practice_code_match.group(1).upper()
    if not any(key in filters for key in ("icb_name", "region_name", "pcn_name", "sub_icb_name", "practice_code")):
        city_icb = find_city_icb_in_text(q_low)
        if city_icb:
            filters["icb_name"] = city_icb
    if "practice_code" not in filters and practice_name_resolver is not None:
        if _looks_like_practice_name_reference(q_low):
            try:
                resolved_practice_code = practice_name_resolver(question, dataset_hint, metric)
            except Exception:
                resolved_practice_code = None
            if resolved_practice_code:
                filters["practice_code"] = resolved_practice_code.upper()

    # Detect national_category filter for appointments (e.g. "Mental Health appointments")
    # Uses exact DB values from the practice table national_category column.
    if dataset_hint == "appointments" and "national_category" not in filters:
        _asks_breakdown = any(term in q_low for term in (
            "breakdown", "categories", "national category", "consultation category",
            "by category", "by type", "all categories",
        ))
        if not _asks_breakdown:
            _CATEGORY_PHRASE_MAP = [
                ("mental health", "Mental Health"),
                ("flu vaccination", "Flu Vaccination"),
                ("flu jab", "Flu Vaccination"),
                ("covid vaccination", "COVID Vaccination"),
                ("ante natal", "Ante Natal"),
                ("antenatal", "Ante Natal"),
                ("post natal", "Post Natal"),
                ("postnatal", "Post Natal"),
                ("structured medication review", "Structured Medication Review"),
                ("medication review", "Structured Medication Review"),
                ("care home visit", "Care Home Visit"),
                # Note: "home visit" is intentionally NOT mapped here — it is handled by
                # the home_visit_appointments metric (appt_mode = 'Home Visit') instead.
                ("planned clinical procedure", "Planned Clinical Procedure"),
                ("group consultation", "Group Consultation and Group Education"),
                ("group education", "Group Consultation and Group Education"),
                ("contraception", "Contraception"),
                ("care related letter", "Care Related Letter"),
                ("inconsistent mapping", "Inconsistent Mapping"),
                ("general consultation routine", "General Consultation Routine"),
                ("general consultation acute", "General Consultation Acute"),
                ("general consultation", "General Consultation Routine"),
            ]
            for phrase, category_value in _CATEGORY_PHRASE_MAP:
                if phrase in q_low:
                    filters["national_category"] = category_value
                    break

    return filters


def _looks_like_practice_name_reference(q_low: str) -> bool:
    return any(
        token in q_low
        for token in (
            "medical centre",
            "medical center",
            "health centre",
            "health center",
            "surgery",
            "clinic",
            " practice",
            "medical practice",
            "group practice",
            "partnership",
        )
    )
