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
    "gp_headcount",
    "gp_fte",
    "nurse_fte",
    "patients_per_gp",
    "registered_patients",
    "total_appointments",
    "face_to_face_appointments",
    "face_to_face_share",
    "telephone_appointments",
    "telephone_share",
    "video_online_appointments",
    "video_online_share",
    "home_visit_appointments",
    "home_visit_share",
    "dna_rate",
    "within_2_weeks_appointments",
    "within_2_weeks_share",
    "over_2_weeks_appointments",
    "over_2_weeks_share",
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

    metric = _detect_metric(q_low, dataset_hint=dataset_hint)
    if not metric:
        return None

    group_by = _detect_group_by(q_low)
    transforms = _detect_transforms(q_low)
    compare = _detect_compare(q, q_low, group_by)
    entity_filters = _detect_entity_filters(
        q,
        q_low,
        dataset_hint=dataset_hint,
        metric=metric,
        practice_name_resolver=practice_name_resolver,
    )
    clarification_needed = _detect_clarification_needed(
        question=q,
        q_low=q_low,
        entity_filters=entity_filters,
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
    "staff role",
    "detailed staff",
    "ethnicity",
    "part time",
    "part-time",
    "full time",
    "full-time",
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

    # In appointments sessions, phrases like "gp appointment" usually mean
    # GP-practice appointments rather than workforce GP headcount.
    if "appointment" in q_low and dataset_hint == "appointments":
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

    # Staff-group swaps
    if "nurse" in q_low:
        return "nurse_fte"
    if re.search(r"\bgps?\b", q_low) or "gp headcount" in q_low or "gp fte" in q_low or "doctor" in q_low:
        if prior_metric.endswith("_fte") or "fte" in q_low:
            return "gp_fte"
        return "gp_headcount"
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
    if "registered patients" in q_low or "patient count" in q_low or "number of patients" in q_low:
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
    entity_filters: Dict[str, str],
) -> bool:
    if _has_unresolved_practice_reference(q_low, entity_filters):
        return True
    if _looks_like_named_practice_without_code(question, q_low, entity_filters):
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
    if any(term in q_low for term in ("national", "nationally", "latest month", "latest")):
        score += 1

    token_count = len(re.findall(r"\b[\w-]+\b", question))
    if token_count > 15:
        return "medium" if score >= 4 else "low"

    if score >= 3:
        return "high"
    return "medium"


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
    )
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
    if "home visit" in q_low:
        return "home_visit_share" if share_metric else "home_visit_appointments"
    if "video conference" in q_low or ("video" in q_low and "appointment" in q_low) or ("online" in q_low and "appointment" in q_low):
        return "video_online_share" if share_metric else "video_online_appointments"
    if "within 2 weeks" in q_low or "within two weeks" in q_low:
        return "within_2_weeks_share" if share_metric else "within_2_weeks_appointments"
    if any(term in q_low for term in ("more than 2 weeks", "more than two weeks", "over 2 weeks", "over two weeks")):
        return "over_2_weeks_share" if share_metric else "over_2_weeks_appointments"
    return ""


def _detect_metric(q_low: str, dataset_hint: str = "") -> str:
    unsupported_breakdown_terms = (
        "care home visit",
        "national category",
    )
    if any(term in q_low for term in unsupported_breakdown_terms):
        return ""
    special_metric = _detect_appointments_special_metric(q_low)
    if special_metric:
        return special_metric
    if _detect_appointments_breakdown_group_by(q_low):
        return "total_appointments"
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
    if "total appointments" in q_low or ("appointment" in q_low and dataset_hint == "appointments"):
        return "total_appointments"
    if "patients per gp" in q_low or "patients-per-gp" in q_low:
        return "patients_per_gp"
    if "registered patients" in q_low or "patient count" in q_low or "number of patients" in q_low:
        return "registered_patients"
    if re.search(r"\bnurse\s+fte\b", q_low) or ("fte" in q_low and "nurse" in q_low):
        return "nurse_fte"
    if re.search(r"\bgp\s+fte\b", q_low) or ("fte" in q_low and "gp" in q_low):
        return "gp_fte"
    if re.search(r"\bhow\s+many\s+gps?\b", q_low) or "gp headcount" in q_low or "number of gps" in q_low:
        return "gp_headcount"
    return ""


def _detect_group_by(q_low: str) -> List[str]:
    appointment_breakdown_group = _detect_appointments_breakdown_group_by(q_low)
    if appointment_breakdown_group:
        return [appointment_breakdown_group]
    if " by icb" in q_low:
        return ["icb_name"]
    if re.search(r"\bwhich\s+icbs?\b", q_low) or re.search(r"\bwhich\s+icb\s+has\b", q_low):
        return ["icb_name"]
    if " by region" in q_low or "which regions" in q_low:
        return ["region_name"]
    if re.search(r"\bwhich\s+regions?\b", q_low):
        return ["region_name"]
    if " by pcn" in q_low:
        return ["pcn_name"]
    if re.search(r"\bwhich\s+pcns?\b", q_low) or re.search(r"\bwhich\s+pcn\s+has\b", q_low):
        return ["pcn_name"]
    if " by sub-icb" in q_low or " by sub icb" in q_low:
        return ["sub_icb_name"]
    if re.search(r"\bwhich\s+sub[- ]icbs?\b", q_low):
        return ["sub_icb_name"]
    if " by practice" in q_low or "which practices" in q_low:
        return ["practice_code"]
    return []


def _detect_appointments_breakdown_group_by(q_low: str) -> str:
    if any(term in q_low for term in ("hcp type", "hcp types", "by hcp", "health care professional type")):
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


def _detect_compare(question: str, q_low: str, group_by: List[str]) -> Optional[CompareSpec]:
    if "compare " not in q_low and " vs " not in q_low:
        return None
    dimension = group_by[0] if group_by else "icb_name"
    values: List[str] = []
    if " vs " in question:
        values = [part.strip(" ?") for part in re.split(r"\bvs\b", question, flags=re.IGNORECASE) if part.strip()]
    elif "compare" in q_low:
        parts = re.split(r"\bcompare\b", question, flags=re.IGNORECASE)
        if len(parts) > 1:
            tail = parts[-1]
            values = [part.strip(" ?,") for part in re.split(r"\band\b|,", tail) if part.strip()]
    values = [value for value in values if len(value) > 2]
    if len(values) < 2:
        return None
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
    practice_code_match = re.search(r"\b([a-z]\d{5})\b", question, flags=re.IGNORECASE)
    if practice_code_match:
        filters["practice_code"] = practice_code_match.group(1).upper()
    if "icb_name" not in filters and "region_name" not in filters and "practice_code" not in filters:
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
