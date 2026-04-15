from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional


DatasetName = Literal["workforce", "appointments", "cross"]
GrainName = Literal[
    "national",
    "region",
    "icb",
    "sub_icb",
    "pcn",
    "practice",
    "appt_mode",
    "hcp_type",
    "booking_window",
]
OrderName = Literal["asc", "desc"]
TransformType = Literal["topn", "benchmark", "trend"]


@dataclass(frozen=True)
class TimeScope:
    mode: Literal["latest", "explicit"] = "latest"
    year: Optional[str] = None
    month: Optional[str] = None


@dataclass(frozen=True)
class TransformSpec:
    type: TransformType
    n: Optional[int] = None
    order: OrderName = "desc"
    scope: Optional[str] = None


@dataclass(frozen=True)
class CompareSpec:
    dimension: str
    values: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class SemanticRequest:
    metrics: List[str]
    entity_filters: Dict[str, str] = field(default_factory=dict)
    group_by: List[str] = field(default_factory=list)
    time: TimeScope = field(default_factory=TimeScope)
    transforms: List[TransformSpec] = field(default_factory=list)
    compare: Optional[CompareSpec] = None
    clarification_needed: bool = False
    confidence: Literal["high", "medium", "low"] = "medium"
