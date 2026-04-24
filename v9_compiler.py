from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Literal, Sequence

from v9_metric_registry import (
    APPOINTMENTS_DATABASE,
    APPOINTMENTS_LATEST,
    WORKFORCE_DATABASE,
    WORKFORCE_LATEST,
    MetricDefinition,
    get_metric,
)
from v9_semantic_types import DatasetName, GrainName, SemanticRequest, TransformSpec


@dataclass(frozen=True)
class CompiledQuery:
    sql: str
    dataset: DatasetName
    grain: GrainName
    metric_keys: List[str] = field(default_factory=list)
    notes: Dict[str, str] = field(default_factory=dict)


_DIMENSION_TO_GRAIN: Dict[str, GrainName] = {
    "region_name": "region",
    "icb_name": "icb",
    "sub_icb_name": "sub_icb",
    "sub_icb_location_name": "sub_icb",
    "pcn_name": "pcn",
    "practice_code": "practice",
    "prac_code": "practice",
    "gp_code": "practice",
    "appt_mode": "appt_mode",
    "hcp_type": "hcp_type",
    "time_between_book_and_appt": "booking_window",
    "national_category": "national_category",
}

_STANDARD_DIMENSION_NAMES: Dict[str, str] = {
    "sub_icb_location_name": "sub_icb_name",
    "prac_code": "practice_code",
    "gp_code": "practice_code",
}

_SINGLE_DATASET_FILTER_COLUMNS: Dict[DatasetName, Dict[str, str]] = {
    "workforce": {
        "region_name": "region_name",
        "icb_name": "icb_name",
        "sub_icb_name": "sub_icb_name",
        "pcn_name": "pcn_name",
        "practice_code": "prac_code",
        "prac_code": "prac_code",
    },
    "appointments": {
        "region_name": "region_name",
        "icb_name": "icb_name",
        "sub_icb_name": "sub_icb_location_name",
        "sub_icb_location_name": "sub_icb_location_name",
        "pcn_name": "pcn_name",
        "practice_code": "gp_code",
        "gp_code": "gp_code",
        "appt_mode": "appt_mode",
        "hcp_type": "hcp_type",
        "time_between_book_and_appt": "time_between_book_and_appt",
        "national_category": "national_category",
    },
}

# Maps a benchmark scope name to the SQL column that should be used for
# PARTITION BY when computing a parent-scope average.
# "national" is intentionally absent — it means OVER () with no partition.
_SCOPE_TO_PARTITION_COL: Dict[str, str] = {
    "region": "region_name",
    "icb": "icb_name",
    "sub_icb": "sub_icb_name",
    "pcn": "pcn_name",
}

# Geographic grain names in hierarchy order (broadest → finest).
# Used to validate that a benchmark scope is a genuine parent of the query grain.
_GEO_HIERARCHY: List[str] = ["region", "icb", "sub_icb", "pcn", "practice"]

_CROSS_GROUP_COLUMNS: Dict[GrainName, List[tuple[str, str]]] = {
    "national": [],
    "region": [("region_name", "region_name")],
    "icb": [("icb_name", "icb_name")],
    "sub_icb": [("sub_icb_name", "sub_icb_name")],
    "pcn": [("pcn_name", "pcn_name")],
    "practice": [("practice_code", "practice_code")],
}


def compile_request(request: SemanticRequest) -> CompiledQuery:
    if not request.metrics:
        raise ValueError("SemanticRequest.metrics must not be empty")
    dataset = _infer_request_dataset(request.metrics)
    grain = _infer_request_grain(request)
    if dataset == "cross":
        return _compile_cross_request(request, grain)
    return _compile_single_dataset_request(request, dataset, grain)


def _infer_request_dataset(metric_keys: Sequence[str]) -> DatasetName:
    datasets = {get_metric(metric_key).dataset for metric_key in metric_keys}
    if "cross" in datasets or len(datasets) > 1:
        return "cross"
    return next(iter(datasets))


def _infer_request_grain(request: SemanticRequest) -> GrainName:
    if request.group_by:
        # When national_category appears alongside a geographic dim, use the
        # geographic dim to set the grain (national_category doesn't define
        # a geographic scope on its own).
        primary_dim = next(
            (d for d in request.group_by if d != "national_category"),
            request.group_by[0],
        )
        if primary_dim not in _DIMENSION_TO_GRAIN:
            raise ValueError(f"Unsupported group_by dimension: {primary_dim}")
        return _DIMENSION_TO_GRAIN[primary_dim]
    if request.compare is not None:
        dim = request.compare.dimension
        if dim not in _DIMENSION_TO_GRAIN:
            raise ValueError(f"Unsupported compare dimension: {dim}")
        return _DIMENSION_TO_GRAIN[dim]
    for key in request.entity_filters:
        if key in _DIMENSION_TO_GRAIN:
            return _DIMENSION_TO_GRAIN[key]
    return "national"


def _compile_single_dataset_request(
    request: SemanticRequest,
    dataset: Literal["workforce", "appointments"],
    grain: GrainName,
) -> CompiledQuery:
    metric = get_metric(request.metrics[0])
    if grain not in metric.valid_grains:
        raise ValueError(f"Metric {metric.key} does not support grain {grain}")
    if dataset == "appointments":
        _validate_appointments_request_shape(request)
    if metric.derived:
        sql = _compile_single_dataset_derived_metric(metric, request, dataset, grain)
    else:
        sql = _compile_single_dataset_base_metric(metric, request, dataset, grain)
    notes = _time_notes_for_dataset(dataset)
    return CompiledQuery(sql=sql, dataset=dataset, grain=grain, metric_keys=list(request.metrics), notes=notes)


def _compile_single_dataset_base_metric(
    metric: MetricDefinition,
    request: SemanticRequest,
    dataset: Literal["workforce", "appointments"],
    grain: GrainName,
) -> str:
    if metric.base_table is None or metric.expr is None:
        raise ValueError(f"Metric {metric.key} is missing a base table or expression")
    base_table = _effective_base_table(metric, dataset, grain, request)
    trend_transform = _trend_transform(request.transforms)
    time_filters = _single_dataset_time_filters(dataset, base_table, request)
    effective_group_dimensions = _effective_group_dimensions(request, dataset, grain)
    group_columns = _single_dataset_group_columns(dataset, effective_group_dimensions, base_table)
    where_clauses = list(time_filters)
    if metric.filter_sql:
        where_clauses.append(metric.filter_sql)
    where_clauses.extend(_single_dataset_entity_filters(dataset, request.entity_filters, base_table))
    if request.compare is not None:
        compare_column = _single_dataset_filter_column(dataset, request.compare.dimension, base_table)
        where_clauses.append(_in_clause(compare_column, request.compare.values))

    select_parts = list(group_columns)
    if trend_transform is not None:
        select_parts.extend(["year", "month"])
    select_parts.append(f"{metric.expr} AS {metric.key}")
    qualified_table = _qualified_single_dataset_table(dataset, base_table)
    sql_lines = [
        f'SELECT {", ".join(select_parts)}',
        f"FROM {qualified_table}",
        f"WHERE {' AND '.join(where_clauses)}",
    ]
    group_by_count = len(group_columns) + (2 if trend_transform is not None else 0)
    if group_by_count:
        sql_lines.append(f"GROUP BY {', '.join(str(index) for index in range(1, group_by_count + 1))}")
    sql_text = "\n".join(sql_lines)
    sql_text = _apply_grouped_benchmark_wrapper(sql_text, metric.key, request.transforms, effective_group_dimensions)
    sql_lines = [sql_text]
    sql_lines.extend(_transform_sql_lines(request.transforms, metric.key, effective_group_dimensions))
    return "\n".join(sql_lines)


def _compile_single_dataset_derived_metric(
    metric: MetricDefinition,
    request: SemanticRequest,
    dataset: Literal["workforce", "appointments"],
    grain: GrainName,
) -> str:
    required_metrics = [get_metric(metric_key) for metric_key in metric.requires]
    base_tables = {_effective_base_table(m, dataset, grain, request) for m in required_metrics}
    if len(base_tables) != 1:
        raise ValueError(f"Derived metric {metric.key} requires a single shared base table")
    base_table = next(iter(base_tables))
    if base_table is None:
        raise ValueError(f"Derived metric {metric.key} has no base table")
    trend_transform = _trend_transform(request.transforms)
    effective_group_dimensions = _effective_group_dimensions(request, dataset, grain)
    group_columns = _single_dataset_group_columns(dataset, effective_group_dimensions, base_table)
    time_filters = _single_dataset_time_filters(dataset, base_table, request)
    where_clauses = list(time_filters)
    where_clauses.extend(_single_dataset_entity_filters(dataset, request.entity_filters, base_table))
    if request.compare is not None:
        compare_column = _single_dataset_filter_column(dataset, request.compare.dimension, base_table)
        where_clauses.append(_in_clause(compare_column, request.compare.values))

    select_parts = list(group_columns)
    if trend_transform is not None:
        select_parts.extend(["year", "month"])
    for required in required_metrics:
        expr = required.expr or ""
        if required.filter_sql:
            expr = f"{_wrap_aggregate_expression(expr, required.filter_sql)}"
        select_parts.append(f"{expr} AS {required.key}")
    qualified_table = _qualified_single_dataset_table(dataset, base_table)
    grouped_sql = "\n".join(
        [
            f'SELECT {", ".join(select_parts)}',
            f"FROM {qualified_table}",
            f"WHERE {' AND '.join(where_clauses)}",
            (
                f"GROUP BY {', '.join(str(index) for index in range(1, len(group_columns) + (2 if trend_transform is not None else 0) + 1))}"
                if group_columns or trend_transform is not None
                else ""
            ),
        ]
    ).strip()
    outer_select = list(_standardized_group_aliases(effective_group_dimensions))
    if trend_transform is not None:
        outer_select.extend(["year", "month"])
    outer_select.append(f"{metric.formula} AS {metric.key}")
    sql_lines = [
        "WITH grouped AS (",
        _indent(grouped_sql),
        ")",
        f"SELECT {', '.join(outer_select)}",
        "FROM grouped",
    ]
    sql_text = "\n".join(sql_lines)
    sql_text = _apply_grouped_benchmark_wrapper(sql_text, metric.key, request.transforms, effective_group_dimensions)
    sql_lines = [sql_text]
    sql_lines.extend(_transform_sql_lines(request.transforms, metric.key, effective_group_dimensions))
    return "\n".join(sql_lines)


def _effective_base_table(
    metric: MetricDefinition,
    dataset: Literal["workforce", "appointments"],
    grain: GrainName,
    request: SemanticRequest,
) -> str:
    if metric.base_table is None:
        raise ValueError(f"Metric {metric.key} is missing a base table")
    if dataset != "appointments":
        return metric.base_table
    if metric.base_table != "practice":
        return metric.base_table
    # national_category only exists in the `practice` table; keep it there even
    # when a geographic secondary dimension is also requested, and when filtering
    # by a specific national_category value via entity_filters.
    if "national_category" in request.group_by or "national_category" in request.entity_filters:
        return "practice"
    if grain in {"region", "icb"}:
        return "pcn_subicb"
    if request.compare is not None and request.compare.dimension in {"region_name", "icb_name"}:
        return "pcn_subicb"
    if any(key in {"region_name", "icb_name"} for key in request.entity_filters):
        return "pcn_subicb"
    if any(dim in {"region_name", "icb_name"} for dim in request.group_by):
        return "pcn_subicb"
    return "practice"


def _compile_cross_request(request: SemanticRequest, grain: GrainName) -> CompiledQuery:
    metric = get_metric(request.metrics[0])
    if not metric.derived or len(metric.requires) != 2:
        raise ValueError(f"Unsupported cross-dataset metric: {metric.key}")
    sql = _compile_cross_ratio_metric(metric, request, grain)
    notes = {
        "appointments_year": APPOINTMENTS_LATEST["year"],
        "appointments_month": APPOINTMENTS_LATEST["month"],
        "workforce_year": WORKFORCE_LATEST["year"],
        "workforce_month": WORKFORCE_LATEST["month"],
    }
    return CompiledQuery(sql=sql, dataset="cross", grain=grain, metric_keys=list(request.metrics), notes=notes)


def _compile_cross_ratio_metric(metric: MetricDefinition, request: SemanticRequest, grain: GrainName) -> str:
    if grain not in metric.valid_grains:
        raise ValueError(f"{metric.key} does not support grain {grain}")
    appointment_metric = get_metric(metric.requires[0])
    workforce_metric = get_metric(metric.requires[1])
    if appointment_metric.dataset != "appointments" or workforce_metric.dataset != "workforce":
        raise ValueError(f"Cross metric {metric.key} requires one appointments metric and one workforce metric")
    appointments_table = _effective_cross_appointments_table(grain, request)
    workforce_base_table = workforce_metric.base_table or "individual"
    if grain == "icb":
        appt_selects = [
            f"{_normalized_icb_sql('icb_name')} AS icb_join_key",
            "MAX(icb_name) AS icb_name",
        ]
        wf_selects = [
            f"{_normalized_icb_sql('icb_name')} AS icb_join_key",
            "MAX(icb_name) AS icb_name",
        ]
        appt_group_by = f"GROUP BY {_normalized_icb_sql('icb_name')}"
        wf_group_by = f"GROUP BY {_normalized_icb_sql('icb_name')}"
    else:
        appt_selects = _cross_group_selects(
            grain,
            aliases={"sub_icb_name": "sub_icb_location_name", "practice_code": "gp_code"},
        )
        wf_selects = _cross_group_selects(
            grain,
            aliases=_cross_workforce_aliases(workforce_base_table),
        )
        appt_group_by = _group_by_clause(appt_selects)
        wf_group_by = _group_by_clause(wf_selects)
    appt_filters = [
        f"year = '{APPOINTMENTS_LATEST['year']}'",
        f"month = '{APPOINTMENTS_LATEST['month']}'",
    ]
    wf_filters = [
        f"year = '{WORKFORCE_LATEST['year']}'",
        f"month = '{WORKFORCE_LATEST['month']}'",
    ]
    appt_filters.extend(_cross_entity_filters(request.entity_filters, source="appointments"))
    wf_filters.extend(_cross_entity_filters(request.entity_filters, source="workforce", workforce_table=workforce_base_table))
    if request.compare is not None:
        compare_filters = _cross_compare_filters(request.compare.dimension, request.compare.values, workforce_table=workforce_base_table)
        appt_filters.extend(compare_filters["appointments"])
        wf_filters.extend(compare_filters["workforce"])

    appt_select_parts = list(appt_selects)
    appointment_expr = appointment_metric.expr or ""
    if appointment_metric.filter_sql:
        appointment_expr = _wrap_aggregate_expression(appointment_expr, appointment_metric.filter_sql)
    appt_select_parts.append(f"{appointment_expr} AS {appointment_metric.key}")
    wf_select_parts = list(wf_selects)
    workforce_expr = workforce_metric.expr or ""
    if workforce_metric.filter_sql:
        workforce_expr = _wrap_aggregate_expression(workforce_expr, workforce_metric.filter_sql)
    wf_select_parts.append(f"{workforce_expr} AS {workforce_metric.key}")

    if grain == "icb":
        join_clause = "JOIN wf ON appt.icb_join_key = wf.icb_join_key"
        select_parts = ["COALESCE(appt.icb_name, wf.icb_name) AS icb_name"]
    else:
        join_keys = ", ".join(alias for alias, _ in _CROSS_GROUP_COLUMNS[grain])
        if join_keys:
            join_clause = f"JOIN wf USING ({join_keys})"
        else:
            join_clause = "CROSS JOIN wf"
        select_parts = list(alias for alias, _ in _CROSS_GROUP_COLUMNS[grain])
    select_parts.extend(
        [
            f"appt.{appointment_metric.key}",
            f"wf.{workforce_metric.key}",
            f"ROUND(appt.{appointment_metric.key} / NULLIF(wf.{workforce_metric.key}, 0), 1) AS {metric.key}",
        ]
    )

    sql_lines = [
        "WITH appt AS (",
        _indent(
            "\n".join(
                [
                    f'SELECT {", ".join(appt_select_parts)}',
                    f'FROM "{APPOINTMENTS_DATABASE}".{appointments_table}',
                    f"WHERE {' AND '.join(appt_filters)}",
                    appt_group_by,
                ]
            ).strip()
        ),
        "),",
        "wf AS (",
        _indent(
            "\n".join(
                [
                    f'SELECT {", ".join(wf_select_parts)}',
                    f'FROM "{WORKFORCE_DATABASE}".{workforce_base_table}',
                    f"WHERE {' AND '.join(wf_filters)}",
                    wf_group_by,
                ]
            ).strip()
        ),
        "),",
        "joined AS (",
        _indent(
            "\n".join(
                [
                    f"SELECT {', '.join(select_parts)}",
                    "FROM appt",
                    join_clause,
                    f"WHERE appt.{appointment_metric.key} IS NOT NULL",
                    f"  AND wf.{workforce_metric.key} IS NOT NULL",
                    f"  AND wf.{workforce_metric.key} > 0",
                ]
            )
        ),
        ")",
    ]
    final_select = ["icb_name"] if grain == "icb" else list(alias for alias, _ in _CROSS_GROUP_COLUMNS[grain])
    final_select.extend([appointment_metric.key, workforce_metric.key, metric.key])
    sql_lines.extend(
        [
            f"SELECT {', '.join(final_select)}",
            "FROM joined",
        ]
    )
    effective_group_dimensions = ["icb_name"] if grain == "icb" else list(alias for alias, _ in _CROSS_GROUP_COLUMNS[grain])
    sql_text = "\n".join(line for line in sql_lines if line)
    sql_text = _apply_grouped_benchmark_wrapper(sql_text, metric.key, request.transforms, effective_group_dimensions)
    sql_lines = [sql_text]
    sql_lines.extend(_transform_sql_lines(request.transforms, metric.key, effective_group_dimensions))
    return "\n".join(sql_lines)


def _effective_cross_appointments_table(request_grain: GrainName, request: SemanticRequest) -> str:
    if request_grain in {"region", "icb"}:
        return "pcn_subicb"
    if request.compare is not None and request.compare.dimension in {"region_name", "icb_name"}:
        return "pcn_subicb"
    if any(key in {"region_name", "icb_name"} for key in request.entity_filters):
        return "pcn_subicb"
    if any(dim in {"region_name", "icb_name"} for dim in request.group_by):
        return "pcn_subicb"
    return "practice"


def _single_dataset_time_filters(
    dataset: Literal["workforce", "appointments"],
    base_table: str,
    request: SemanticRequest,
) -> List[str]:
    trend_transform = _trend_transform(request.transforms)
    time = request.time
    if time.mode == "explicit":
        year = time.year
        month = time.month
    elif dataset == "workforce":
        year, month = WORKFORCE_LATEST["year"], WORKFORCE_LATEST["month"]
    else:
        year, month = APPOINTMENTS_LATEST["year"], APPOINTMENTS_LATEST["month"]

    if year is None or month is None:
        raise ValueError("Both year and month are required for compilation")

    if trend_transform is not None:
        window = int(trend_transform.n or 12)
        start_year, start_month = _rolling_month_window_start(str(year), str(month), window)
        start_index = int(start_year) * 12 + int(start_month)
        end_index = int(year) * 12 + int(month)
        return [
            f"(CAST(year AS INTEGER) * 12 + CAST(month AS INTEGER)) BETWEEN {start_index} AND {end_index}"
        ]

    return [f"year = '{year}'", f"month = '{month}'"]


def _single_dataset_group_columns(dataset: DatasetName, group_by: Sequence[str], base_table: str | None = None) -> List[str]:
    columns: List[str] = []
    for dimension in group_by:
        column = _single_dataset_filter_column(dataset, dimension, base_table)
        alias = _STANDARD_DIMENSION_NAMES.get(dimension, dimension)
        if column == alias:
            columns.append(column)
        else:
            columns.append(f"{column} AS {alias}")
    return columns


def _effective_group_dimensions(
    request: SemanticRequest,
    dataset: Literal["workforce", "appointments"],
    grain: GrainName,
) -> List[str]:
    # ── Base group-by columns ────────────────────────────────────────────────
    if request.group_by:
        base_dims = list(request.group_by)
    elif request.compare is not None:
        base_dims = [request.compare.dimension]
    elif any(transform.type == "benchmark" for transform in request.transforms) and grain != "national":
        dimension = _default_dimension_for_grain(dataset, grain)
        base_dims = [dimension] if dimension else []
    else:
        return []

    # ── Scope column injection for non-national benchmark PARTITION BY ────────
    # When the user requests "each ICB vs its regional average" (scope="region"),
    # inject the parent scope column so _apply_grouped_benchmark_wrapper can emit
    # OVER (PARTITION BY region_name) instead of the incorrect OVER () that was
    # producing a global average labelled as e.g. "region_average".
    benchmark_transform = next(
        (t for t in request.transforms if t.type == "benchmark"), None
    )
    if benchmark_transform is not None and base_dims:
        scope = str(benchmark_transform.scope or "national").strip().lower()
        scope_col = _SCOPE_TO_PARTITION_COL.get(scope)
        if scope_col is not None and scope_col not in base_dims:
            scope_grain = next(
                (k for k, v in _SCOPE_TO_PARTITION_COL.items() if v == scope_col), None
            )
            # Only inject when the scope is a strict geographic parent of the grain.
            if (
                scope_grain in _GEO_HIERARCHY
                and grain in _GEO_HIERARCHY
                and _GEO_HIERARCHY.index(scope_grain) < _GEO_HIERARCHY.index(grain)
            ):
                # For appointments, region/ICB columns only exist in the pcn_subicb
                # table (used when grain is region or icb). Don't inject them for
                # finer grains that would route to the practice table instead.
                skip = (
                    dataset == "appointments"
                    and scope_col in {"region_name", "icb_name"}
                    and grain not in {"region", "icb"}
                )
                if not skip:
                    base_dims = [scope_col] + base_dims

    return base_dims


def _default_dimension_for_grain(
    dataset: Literal["workforce", "appointments"],
    grain: GrainName,
) -> str:
    if grain == "region":
        return "region_name"
    if grain == "icb":
        return "icb_name"
    if grain == "sub_icb":
        return "sub_icb_name"
    if grain == "pcn":
        return "pcn_name"
    if grain == "practice":
        return "practice_code"
    if dataset == "appointments" and grain == "appt_mode":
        return "appt_mode"
    if dataset == "appointments" and grain == "hcp_type":
        return "hcp_type"
    if dataset == "appointments" and grain == "booking_window":
        return "time_between_book_and_appt"
    if dataset == "appointments" and grain == "national_category":
        return "national_category"
    return ""


def _single_dataset_entity_filters(dataset: DatasetName, entity_filters: Dict[str, str], base_table: str | None = None) -> List[str]:
    filters: List[str] = []
    for key, value in entity_filters.items():
        column = _single_dataset_filter_column(dataset, key, base_table)
        if key == "icb_name":
            filters.append(_normalized_icb_equality(column, value))
        else:
            filters.append(f"LOWER(TRIM({column})) = LOWER('{_escape_literal(value)}')")
    return filters


def _single_dataset_filter_column(dataset: DatasetName, dimension: str, base_table: str | None = None) -> str:
    if dataset == "workforce" and dimension == "region_name":
        if str(base_table or "").strip().lower() == "individual":
            return "comm_region_name"
        return "region_name"
    if dataset == "appointments":
        table = str(base_table or "").strip().lower()
        if table == "practice" and dimension in {"region_name", "icb_name"}:
            raise ValueError(f"Appointments {table} does not contain {dimension}")
        if table == "pcn_subicb" and dimension in {"pcn_name", "national_category"}:
            raise ValueError(f"Appointments {table} does not contain {dimension}")
    if dataset not in _SINGLE_DATASET_FILTER_COLUMNS:
        raise ValueError(f"Unsupported dataset for filter mapping: {dataset}")
    mapping = _SINGLE_DATASET_FILTER_COLUMNS[dataset]
    if dimension not in mapping:
        raise ValueError(f"Unsupported dimension for {dataset}: {dimension}")
    return mapping[dimension]


def _qualified_single_dataset_table(dataset: DatasetName, base_table: str) -> str:
    if dataset == "workforce":
        return f'"{WORKFORCE_DATABASE}".{base_table}'
    if dataset == "appointments":
        return f'"{APPOINTMENTS_DATABASE}".{base_table}'
    raise ValueError(f"Unsupported dataset for qualified table resolution: {dataset}")


def _cross_group_selects(grain: GrainName, aliases: Dict[str, str]) -> List[str]:
    selects: List[str] = []
    for alias, _ in _CROSS_GROUP_COLUMNS[grain]:
        source_column = aliases.get(alias, alias)
        if source_column == alias:
            selects.append(alias)
        else:
            selects.append(f"{source_column} AS {alias}")
    return selects


def _cross_entity_filters(
    entity_filters: Dict[str, str],
    source: Literal["appointments", "workforce"],
    workforce_table: str | None = None,
) -> List[str]:
    mapping = {
        "appointments": {
            "region_name": "region_name",
            "icb_name": "icb_name",
            "sub_icb_name": "sub_icb_location_name",
            "pcn_name": "pcn_name",
            "practice_code": "gp_code",
        },
        "workforce": {
            "region_name": "region_name",
            "icb_name": "icb_name",
            "sub_icb_name": "sub_icb_name",
            "pcn_name": "pcn_name",
            "practice_code": "prac_code",
        },
    }
    if source == "workforce" and str(workforce_table or "").strip().lower() == "individual":
        mapping["workforce"]["region_name"] = "comm_region_name"
    filters: List[str] = []
    for key, value in entity_filters.items():
        if key not in mapping[source]:
            continue
        if key == "icb_name":
            filters.append(_normalized_icb_equality(mapping[source][key], value))
        else:
            filters.append(f"LOWER(TRIM({mapping[source][key]})) = LOWER('{_escape_literal(value)}')")
    return filters


def _cross_compare_filters(
    dimension: str,
    values: Sequence[str],
    workforce_table: str | None = None,
) -> Dict[str, List[str]]:
    appointments_map = {
        "region_name": "region_name",
        "icb_name": "icb_name",
        "sub_icb_name": "sub_icb_location_name",
        "pcn_name": "pcn_name",
        "practice_code": "gp_code",
    }
    workforce_map = {
        "region_name": "region_name",
        "icb_name": "icb_name",
        "sub_icb_name": "sub_icb_name",
        "pcn_name": "pcn_name",
        "practice_code": "prac_code",
    }
    if str(workforce_table or "").strip().lower() == "individual":
        workforce_map["region_name"] = "comm_region_name"
    if dimension not in appointments_map or dimension not in workforce_map:
        raise ValueError(f"Unsupported cross-dataset compare dimension: {dimension}")
    return {
        "appointments": [_in_clause(appointments_map[dimension], values)],
        "workforce": [_in_clause(workforce_map[dimension], values)],
    }


def _validate_appointments_request_shape(request: SemanticRequest) -> None:
    uses_national_category = "national_category" in request.group_by or "national_category" in request.entity_filters
    if not uses_national_category:
        return
    if any(dim in {"region_name", "icb_name"} for dim in request.group_by):
        raise ValueError("Appointments national_category queries do not support region or ICB groupings directly")
    if any(key in {"region_name", "icb_name"} for key in request.entity_filters):
        raise ValueError("Appointments national_category queries do not support region or ICB filters directly")
    if request.compare is not None and request.compare.dimension in {"region_name", "icb_name"}:
        raise ValueError("Appointments national_category queries do not support region or ICB comparisons directly")


def _group_by_clause(select_parts: Sequence[str]) -> str:
    if not select_parts:
        return ""
    group_columns: List[str] = []
    for select_part in select_parts:
        part = str(select_part or "").strip()
        alias_split = __import__("re").split(r"\s+AS\s+", part, flags=__import__("re").IGNORECASE)
        group_columns.append(alias_split[0].strip())
    return f"GROUP BY {', '.join(group_columns)}"


def _transform_sql_lines(
    transforms: Sequence[TransformSpec],
    metric_alias: str,
    group_by: Sequence[str],
) -> List[str]:
    lines: List[str] = []
    for transform in transforms:
        if transform.type == "topn":
            lines.append(f"ORDER BY {metric_alias} {transform.order.upper()}")
            if transform.n is not None:
                lines.append(f"LIMIT {int(transform.n)}")
        elif transform.type == "benchmark":
            continue
        elif transform.type == "trend":
            lines.append("ORDER BY CAST(year AS INTEGER) ASC, CAST(month AS INTEGER) ASC")
    return lines


def _trend_transform(transforms: Sequence[TransformSpec]) -> TransformSpec | None:
    return next((transform for transform in transforms if transform.type == "trend"), None)


def _rolling_month_window_start(year: str, month: str, window: int) -> tuple[str, str]:
    latest_index = int(year) * 12 + int(month) - 1
    start_index = max(0, latest_index - max(window - 1, 0))
    start_year = start_index // 12
    start_month = (start_index % 12) + 1
    return str(start_year), f"{start_month:02d}"


def _apply_grouped_benchmark_wrapper(
    sql: str,
    metric_alias: str,
    transforms: Sequence[TransformSpec],
    group_by: Sequence[str],
) -> str:
    benchmark = next((transform for transform in transforms if transform.type == "benchmark"), None)
    if benchmark is None or not group_by:
        return sql
    scope = str(benchmark.scope or "national").strip().lower()

    # Use PARTITION BY when the scope column is present in the result set.
    # _effective_group_dimensions injects it as a prefix when the scope is a
    # valid geographic parent of the query grain, so by the time we arrive here
    # the column is available in the result CTE for partitioning.
    scope_col = _SCOPE_TO_PARTITION_COL.get(scope)
    if scope_col and scope_col in group_by:
        over_clause = f"OVER (PARTITION BY {scope_col})"
        benchmark_alias = f"{scope}_average"
    else:
        # Scope column not in result — fall back to a global OVER ().
        # Use "national_average" as the label so it honestly reflects the
        # calculation rather than mislabelling a global mean as e.g. "pcn_average".
        over_clause = "OVER ()"
        benchmark_alias = "national_average"

    return "\n".join(
        [
            "WITH result AS (",
            _indent(sql),
            ")",
            f"SELECT *, ROUND(AVG({metric_alias}) {over_clause}, 1) AS {benchmark_alias}",
            "FROM result",
        ]
    )


def _wrap_aggregate_expression(expr: str, filter_sql: str) -> str:
    if expr.startswith("COUNT(DISTINCT "):
        inner = expr[len("COUNT(DISTINCT ") : -1]
        return f"COUNT(DISTINCT CASE WHEN {filter_sql} THEN {inner} END)"
    round_sum_match = __import__("re").match(r"^ROUND\(SUM\((.+)\),\s*(\d+)\)$", expr)
    if round_sum_match:
        inner, scale = round_sum_match.group(1), round_sum_match.group(2)
        return f"ROUND(SUM(CASE WHEN {filter_sql} THEN {inner} ELSE 0 END), {scale})"
    if expr.startswith("SUM("):
        inner = expr[len("SUM(") : -1]
        return f"SUM(CASE WHEN {filter_sql} THEN {inner} END)"
    raise ValueError(f"Unsupported aggregate expression for filter wrapping: {expr}")


def _cross_workforce_aliases(base_table: str) -> Dict[str, str]:
    aliases: Dict[str, str] = {"practice_code": "prac_code"}
    if str(base_table or "").strip().lower() == "individual":
        aliases["region_name"] = "comm_region_name"
    return aliases


def _standardized_group_aliases(group_by: Sequence[str]) -> Iterable[str]:
    for dimension in group_by:
        yield _STANDARD_DIMENSION_NAMES.get(dimension, dimension)


def _in_clause(column: str, values: Sequence[str]) -> str:
    if not values:
        raise ValueError(f"IN filter requires at least one value for {column}")
    joined = ", ".join(f"LOWER(TRIM('{_escape_literal(value)}'))" for value in values)
    return f"LOWER(TRIM({column})) IN ({joined})"


def _normalized_icb_equality(column: str, value: str) -> str:
    return f"{_normalized_icb_sql(column)} = {_normalized_icb_literal(value)}"


def _normalized_icb_sql(expr: str) -> str:
    return (
        f"REPLACE(REPLACE(LOWER(TRIM({expr})), ' integrated care board', ''), ' icb', '')"
    )


def _normalized_icb_literal(value: str) -> str:
    escaped = _escape_literal(value)
    return (
        f"REPLACE(REPLACE(LOWER(TRIM('{escaped}')), ' integrated care board', ''), ' icb', '')"
    )


def _time_notes_for_dataset(dataset: Literal["workforce", "appointments"]) -> Dict[str, str]:
    if dataset == "workforce":
        return {"year": WORKFORCE_LATEST["year"], "month": WORKFORCE_LATEST["month"]}
    return {"year": APPOINTMENTS_LATEST["year"], "month": APPOINTMENTS_LATEST["month"]}


def _escape_literal(value: str) -> str:
    return str(value).replace("'", "''")


def _indent(text: str) -> str:
    return "\n".join(f"  {line}" if line else line for line in text.splitlines())
