# gp_workforce_chatbot_backend.py
"""
GP Workforce Athena Chatbot (Improved Planner + Normalization + Semantic Checks + Auto SQL Fix)

Key upgrades:
- Planner JSON output is robust: prompt braces escaped, and planner output is normalized (so "null" won't break geo routing)
- Better "latest data we have" detection (not just "latest month")
- Stronger semantic coverage checks so SQL fulfills the full request
- Safer geo aggregation: GROUP BY code, name = max(name) to avoid duplicates
- Still optimized for Athena cost: always filters to latest month when asked

Environment:
  export AWS_PROFILE=chatbot
  export AWS_REGION=eu-west-2
  export ATHENA_DATABASE="test-gp-workforce"
  export ATHENA_OUTPUT_S3="s3://test-athena-results-fingertips/"
  export BEDROCK_CHAT_MODEL_ID="amazon.nova-pro-v1:0"
Optional:
  export ATHENA_WORKGROUP="primary"
"""

from __future__ import annotations

import os
import re
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from langchain_aws import ChatBedrockConverse
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser


# -----------------------------
# Config
# -----------------------------
AWS_PROFILE = os.getenv("AWS_PROFILE", "default")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-2")

ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "test-gp-workforce")
ATHENA_OUTPUT_S3 = os.getenv("ATHENA_OUTPUT_S3", "s3://test-athena-results-fingertips/")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "")  # optional

BEDROCK_CHAT_MODEL_ID = os.getenv("BEDROCK_CHAT_MODEL_ID", "amazon.nova-pro-v1:0")

V_PRACTICE_HIGH = f'"{ATHENA_DATABASE}"."v_practice_high"'
V_PRACTICE_DETAILED = f'"{ATHENA_DATABASE}"."v_practice_detailed_core"'
T_INDIVIDUAL = f'"{ATHENA_DATABASE}"."individual"'  # optional


# -----------------------------
# Simple chat memory
# -----------------------------
@dataclass
class Turn:
    user: str
    assistant: str


class SimpleChatMemory:
    def __init__(self, max_turns: int = 10):
        self.max_turns = max_turns
        self.turns: List[Turn] = []

    def add(self, user: str, assistant: str) -> None:
        self.turns.append(Turn(user=user, assistant=assistant))
        if len(self.turns) > self.max_turns:
            self.turns = self.turns[-self.max_turns :]

    def format_history(self) -> str:
        if not self.turns:
            return "No prior conversation."
        out = []
        for i, t in enumerate(self.turns, 1):
            out.append(f"Turn {i} - User: {t.user}")
            out.append(f"Turn {i} - Assistant: {t.assistant}")
        return "\n".join(out)


# -----------------------------
# AWS clients
# -----------------------------
def _session() -> boto3.Session:
    return boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)


def bedrock_runtime_client():
    return _session().client("bedrock-runtime", region_name=AWS_REGION)


def athena_client():
    return _session().client("athena", region_name=AWS_REGION)


def chat_llm() -> ChatBedrockConverse:
    return ChatBedrockConverse(
        client=bedrock_runtime_client(),
        model=BEDROCK_CHAT_MODEL_ID,
        temperature=0.1,
        max_tokens=1200,
    )


# -----------------------------
# SQL helpers
# -----------------------------
_SQL_START_RE = re.compile(r"^\s*(WITH|SELECT)\b", re.IGNORECASE)

def _strip_code_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip().rstrip(";")

def validate_sql(sql: str) -> str:
    sql = _strip_code_fences(sql)
    if not _SQL_START_RE.match(sql):
        raise ValueError("Generated SQL is not a SELECT/WITH query.")
    banned = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "MSCK", "REPAIR"]
    up = sql.upper()
    if any(b in up for b in banned):
        raise ValueError("Generated SQL contains a non-read statement.")
    return sql

def month_date_expr(year_col: str, month_col: str) -> str:
    return (
        f"date_parse(concat(cast({year_col} as varchar), '-', lpad(cast({month_col} as varchar), 2, '0'), '-01'), '%Y-%m-%d')"
    )

def latest_month_cte_for_view(view_sql: str) -> str:
    return f"""
WITH latest AS (
  SELECT year_int, month_int, {month_date_expr("year_int","month_int")} AS period_date
  FROM {view_sql}
  GROUP BY 1,2,3
  ORDER BY year_int DESC, month_int DESC
  LIMIT 1
)
""".strip()


# -----------------------------
# Deterministic helpers (fallback inference)
# -----------------------------
def _parse_last_n_months(q: str) -> Optional[int]:
    m = re.search(r"last\s+(\d+)\s+months", q.lower())
    if m:
        try:
            return int(m.group(1))
        except:
            return None
    return None

def _wants_trend(q: str) -> bool:
    ql = q.lower()
    return ("trend" in ql) or ("over time" in ql) or (_parse_last_n_months(q) is not None)

def _infer_staff_group(q: str) -> Optional[str]:
    ql = q.lower()
    if "doctor" in ql or "doctors" in ql or re.search(r"\bgp\b", ql):
        return "GP"
    if "nurse" in ql:
        return "Nurses"
    if "admin" in ql or "reception" in ql:
        return "Admin/Non-Clinical"
    if "direct patient care" in ql or "dpc" in ql:
        return "Direct Patient Care"
    return None

def _infer_measure(q: str) -> str:
    ql = q.lower()
    if "headcount" in ql or "how many" in ql or "count" in ql or "number of" in ql:
        return "Headcount"
    return "FTE"

def _infer_geo(q: str) -> Optional[str]:
    ql = q.lower()
    if "icb" in ql:
        return "icb"
    if "region" in ql:
        return "region"
    if "sub icb" in ql or "sub-icb" in ql:
        return "sub_icb"
    if "pcn" in ql:
        return "pcn"
    return None

def _infer_top_n(q: str) -> Optional[int]:
    m = re.search(r"top\s+(\d+)", q.lower())
    if m:
        return int(m.group(1))
    if "top" in q.lower():
        return 10
    return None

def _is_latest_data_question(q: str) -> bool:
    ql = q.lower().strip()
    # catches “latest data we have”, “most recent data”, “latest snapshot”, etc.
    return any(
        phrase in ql
        for phrase in [
            "latest data",
            "most recent data",
            "latest available data",
            "latest snapshot",
            "what's the latest data",
            "whats the latest data",
            "what is the latest data",
            "latest month available",
            "latest month",
        ]
    )


# -----------------------------
# Deterministic SQL builders
# -----------------------------
def sql_latest_month_available() -> str:
    cte = latest_month_cte_for_view(V_PRACTICE_HIGH)
    return validate_sql(f"""{cte}
SELECT year_int, month_int, period_date
FROM latest
""")

def sql_totals_by_staff_group_latest(measure: str) -> str:
    cte = latest_month_cte_for_view(V_PRACTICE_HIGH)
    metric_alias = "total_fte" if measure == "FTE" else "total_headcount"
    return validate_sql(f"""{cte}
SELECT
  ph.staff_group,
  SUM(ph.value) AS {metric_alias}
FROM {V_PRACTICE_HIGH} ph
JOIN latest l
  ON ph.year_int = l.year_int AND ph.month_int = l.month_int
WHERE ph.measure = '{measure}'
  AND ph.detailed_staff_role = 'Total'
GROUP BY 1
ORDER BY {metric_alias} DESC
""")

def sql_top_roles_latest(n: int, staff_group: Optional[str], measure: str) -> str:
    cte = latest_month_cte_for_view(V_PRACTICE_HIGH)
    staff_filter = f"AND ph.staff_group = '{staff_group}'" if staff_group else ""
    return validate_sql(f"""{cte}
SELECT
  ph.staff_group,
  ph.detailed_staff_role,
  SUM(ph.value) AS metric_value
FROM {V_PRACTICE_HIGH} ph
JOIN latest l
  ON ph.year_int = l.year_int AND ph.month_int = l.month_int
WHERE ph.measure = '{measure}'
{staff_filter}
GROUP BY 1,2
ORDER BY metric_value DESC
LIMIT {n}
""")

def sql_geo_metric_latest(geo: str, staff_group: Optional[str], measure: str, force_total_role: bool = True) -> str:
    cte = latest_month_cte_for_view(V_PRACTICE_HIGH)

    if geo == "icb":
        geo_code, geo_name = "pd.icb_code", "pd.icb_name"
    elif geo == "region":
        geo_code, geo_name = "pd.region_code", "pd.region_name"
    elif geo == "sub_icb":
        geo_code, geo_name = "pd.sub_icb_code", "pd.sub_icb_name"
    elif geo == "pcn":
        geo_code, geo_name = "pd.pcn_code", "pd.pcn_name"
    else:
        raise ValueError(f"Unsupported geo: {geo}")

    staff_filter = f"AND ph.staff_group = '{staff_group}'" if staff_group else ""
    role_filter = "AND ph.detailed_staff_role = 'Total'" if force_total_role else ""

    # De-dupe names: group by code only, name = max(name)
    return validate_sql(f"""{cte}
SELECT
  {geo_code} AS geo_code,
  max({geo_name}) AS geo_name,
  SUM(ph.value) AS metric_value
FROM {V_PRACTICE_HIGH} ph
JOIN latest l
  ON ph.year_int = l.year_int AND ph.month_int = l.month_int
JOIN {V_PRACTICE_DETAILED} pd
  ON ph.prac_code = pd.prac_code
 AND ph.year_int = pd.year_int AND ph.month_int = pd.month_int
WHERE ph.measure = '{measure}'
{staff_filter}
{role_filter}
GROUP BY 1
ORDER BY metric_value DESC
""")

def sql_trend_last_n_months(months: int, staff_group: Optional[str], measure: str, detailed_role: str = "Total") -> str:
    months = max(1, min(months, 36))

    staff_filter = f"AND staff_group = '{staff_group}'" if staff_group else ""
    role_filter = f"AND detailed_staff_role = '{detailed_role}'" if detailed_role else ""

    sql = f"""
WITH all_months AS (
  SELECT
    year_int, month_int,
    {month_date_expr("year_int","month_int")} AS period_date
  FROM {V_PRACTICE_HIGH}
  GROUP BY 1,2,3
),
latest AS (
  SELECT period_date AS latest_date
  FROM all_months
  ORDER BY year_int DESC, month_int DESC
  LIMIT 1
),
series AS (
  SELECT
    year_int,
    month_int,
    {month_date_expr("year_int","month_int")} AS period_date,
    SUM(value) AS metric_value
  FROM {V_PRACTICE_HIGH}
  WHERE measure = '{measure}'
  {staff_filter}
  {role_filter}
  GROUP BY 1,2,3
)
SELECT year_int, month_int, metric_value
FROM series
CROSS JOIN latest
WHERE period_date BETWEEN date_add('month', -{months-1}, latest.latest_date) AND latest.latest_date
ORDER BY year_int, month_int
"""
    return validate_sql(sql)


# -----------------------------
# Planner (Nova returns JSON) + Normalization
# -----------------------------
PLANNER_PROMPT = ChatPromptTemplate.from_messages(
    [
        ("system",
         "You are an intent planner for an Athena workforce chatbot.\n"
         "Return ONLY valid JSON (no markdown, no commentary).\n\n"
         "Available objects:\n"
         f"- v_practice_high: workforce metrics by month (year_int, month_int), columns: staff_group, detailed_staff_role, measure(FTE/Headcount), value.\n"
         f"- v_practice_detailed_core: practice->geo bridge by month, columns include icb_code/name, region_code/name, sub_icb_code/name, pcn_code/name.\n\n"
         "Output JSON with keys exactly:\n"
         "{{"
         "\"intent\": \"latest_month|totals_by_staff_group_latest|geo_metric_latest|top_roles_latest|trend_last_n_months|other\", "
         "\"measure\": \"FTE|Headcount|null\", "
         "\"staff_group\": \"GP|Nurses|Admin/Non-Clinical|Direct Patient Care|null\", "
         "\"geo\": \"icb|region|sub_icb|pcn|null\", "
         "\"top_n\": 10, "
         "\"last_n_months\": 12, "
         "\"force_total_role\": true"
         "}}\n\n"
         "Rules:\n"
         "- If user says doctors => staff_group=GP and measure=Headcount unless they explicitly ask FTE.\n"
         "- If question asks totals by staff_group => intent=totals_by_staff_group_latest and force_total_role=true.\n"
         "- If question asks trend/last N months => intent=trend_last_n_months and last_n_months default 12.\n"
         "- If question is 'latest data we have' or similar => intent=latest_month.\n"
        ),
        ("system", "Conversation history:\n{history}"),
        ("human", "{question}"),
    ]
)

def _to_none(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("null", "none", "", "n/a", "na", "unknown"):
            return None
    return v

def normalize_plan(plan: Dict[str, Any], question: str) -> Dict[str, Any]:
    # Normalize null-like strings to None
    for k in list(plan.keys()):
        plan[k] = _to_none(plan[k])

    # Normalize intent
    intent = (plan.get("intent") or "").strip()
    if not intent:
        intent = "other"

    # If user asked "latest data" but model didn't set it, force it
    if _is_latest_data_question(question) and not _wants_trend(question):
        intent = "latest_month"

    # Normalize measure
    measure = plan.get("measure")
    if measure not in ("FTE", "Headcount", None):
        measure = None

    # Doctors rule
    ql = question.lower()
    if ("doctor" in ql or "doctors" in ql) and "fte" not in ql:
        plan["staff_group"] = "GP"
        measure = "Headcount"

    if measure is None:
        measure = _infer_measure(question)
    plan["measure"] = measure

    # Normalize geo
    geo = plan.get("geo")
    if geo not in ("icb", "region", "sub_icb", "pcn", None):
        geo = None
    plan["geo"] = geo

    # Normalize numbers
    def _to_int(x: Any, default: Optional[int]) -> Optional[int]:
        x = _to_none(x)
        if x is None:
            return default
        try:
            return int(x)
        except:
            return default

    plan["top_n"] = _to_int(plan.get("top_n"), None)
    plan["last_n_months"] = _to_int(plan.get("last_n_months"), None)

    # Force trend months default
    if _wants_trend(question) and not plan["last_n_months"]:
        plan["last_n_months"] = 12

    # Normalize boolean
    ftr = plan.get("force_total_role")
    if isinstance(ftr, str):
        plan["force_total_role"] = ftr.strip().lower() in ("true", "1", "yes")
    elif isinstance(ftr, bool):
        pass
    else:
        plan["force_total_role"] = True

    plan["intent"] = intent
    return plan

def plan_question(llm: ChatBedrockConverse, history: str, question: str) -> Dict[str, Any]:
    raw = (PLANNER_PROMPT | llm | StrOutputParser()).invoke({"history": history, "question": question}).strip()
    try:
        plan = json.loads(raw)
        if not isinstance(plan, dict):
            raise ValueError("Planner returned non-dict JSON.")
    except Exception:
        # Fallback deterministic plan
        plan = {
            "intent": "other",
            "measure": _infer_measure(question),
            "staff_group": _infer_staff_group(question),
            "geo": _infer_geo(question),
            "top_n": _infer_top_n(question),
            "last_n_months": _parse_last_n_months(question) or (12 if _wants_trend(question) else None),
            "force_total_role": True,
        }
    return normalize_plan(plan, question)


# -----------------------------
# Semantic coverage checks
# -----------------------------
def semantic_check(question: str, sql: str, plan: Dict[str, Any]) -> Optional[str]:
    q = question.lower()
    s = sql.lower()

    # Must compute metric if user asks for totals/counts/trend/top
    if any(x in q for x in ["total", "how many", "count", "number of", "trend", "top"]):
        if not any(x in s for x in ["sum(", "count("]):
            return "Semantic: question requires aggregation (SUM/COUNT) but SQL has none."

    # Latest (non-trend) should use latest CTE or equivalent
    if _is_latest_data_question(question) and not _wants_trend(question):
        if "with latest" not in s:
            return "Semantic: latest requested but SQL does not use latest month selection."

    # Trend must not pin to a single month
    if _wants_trend(question):
        if re.search(r"month_int\s*=\s*\d+", s) or re.search(r"year_int\s*=\s*\d+", s):
            return "Semantic: trend requested but SQL pins a single year/month."

    # Doctors should map to GP + Headcount unless FTE explicitly asked
    if ("doctor" in q or "doctors" in q) and "fte" not in q:
        if "staff_group = 'gp'" not in s:
            return "Semantic: doctors requested but SQL not constrained to staff_group='GP'."
        if "measure = 'headcount'" not in s:
            return "Semantic: doctors requested but SQL not constrained to measure='Headcount'."

    # Geo intent should group by geo_code
    if plan.get("intent") == "geo_metric_latest":
        if "geo_code" not in s and "group by 1" not in s:
            return "Semantic: geo query should group by geo code."

    return None


# -----------------------------
# LLM SQL generation + repair (fallback + fixing)
# -----------------------------
SCHEMA_CONTEXT = f"""
You generate SQL for Amazon Athena (Trino/Presto). Output ONLY SQL.

Objects:
1) {V_PRACTICE_HIGH}
  prac_code, staff_group, detailed_staff_role, measure ('FTE','Headcount'), value, year_int, month_int
2) {V_PRACTICE_DETAILED}
  prac_code, icb_code/name, region_code/name, sub_icb_code/name, pcn_code/name, year_int, month_int
3) {T_INDIVIDUAL} (optional)

Hard rules:
- Must start with SELECT or WITH. Never SELECT *.
- If latest requested (and not trend): choose latest month by ORDER BY year_int DESC, month_int DESC LIMIT 1 and filter/join to that month.
- If trend/last N months requested: output monthly series; do NOT pin single month.
- Doctors => staff_group='GP' and measure='Headcount' unless user explicitly asks FTE.
- Use LIMIT 50 for any non-fully-aggregated listing.
""".strip()

def generate_sql_llm(llm: ChatBedrockConverse, history: str, question: str) -> str:
    prompt = ChatPromptTemplate.from_messages(
        [("system", SCHEMA_CONTEXT),
         ("system", "Conversation history:\n{history}"),
         ("human", "{question}")]
    )
    raw = (prompt | llm | StrOutputParser()).invoke({"history": history, "question": question}).strip()
    return validate_sql(raw)

def fix_sql_llm(llm: ChatBedrockConverse, history: str, question: str, bad_sql: str, error: str) -> str:
    repair = """
Return ONLY corrected SQL. Must start with SELECT or WITH. No explanations. No SELECT *.
Fix Athena errors AND fix semantic coverage if mentioned.
""".strip()

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", SCHEMA_CONTEXT),
            ("system", repair),
            ("system", "Conversation history:\n{history}"),
            ("system", "User question:\n{question}"),
            ("system", "Bad SQL:\n{bad_sql}"),
            ("system", "Error / semantic issue:\n{error}"),
            ("human", "Fix the SQL."),
        ]
    )
    raw = (prompt | llm | StrOutputParser()).invoke(
        {"history": history, "question": question, "bad_sql": bad_sql, "error": error}
    ).strip()
    return validate_sql(raw)


# -----------------------------
# SQL selection from plan
# -----------------------------
def choose_sql_from_plan(plan: Dict[str, Any], question: str) -> Optional[str]:
    ql = question.lower()
    intent = plan.get("intent") or "other"
    measure = plan.get("measure") or _infer_measure(question)
    staff_group = plan.get("staff_group")
    geo = plan.get("geo")
    top_n = plan.get("top_n")
    last_n = plan.get("last_n_months")
    force_total_role = bool(plan.get("force_total_role", True))

    # Strong override: “latest data we have”
    if _is_latest_data_question(question) and not _wants_trend(question):
        return sql_latest_month_available()

    if intent == "latest_month":
        return sql_latest_month_available()

    if intent == "totals_by_staff_group_latest":
        return sql_totals_by_staff_group_latest(measure)

    if intent == "top_roles_latest":
        n = top_n or 10
        return sql_top_roles_latest(n, staff_group, measure)

    if intent == "geo_metric_latest":
        if geo is None:
            # If planner couldn't infer geo but question contains geo keyword, infer again
            geo = _infer_geo(question)
        if geo is None:
            return None
        # Doctors rule already normalized, so just use
        return sql_geo_metric_latest(geo, staff_group, measure, force_total_role=force_total_role)

    if intent == "trend_last_n_months" or _wants_trend(question):
        months = last_n or 12
        return sql_trend_last_n_months(months, staff_group, measure, detailed_role="Total")

    # Deterministic keyword fallback (even if planner said "other")
    if "by staff_group" in ql or "by staff group" in ql:
        return sql_totals_by_staff_group_latest(measure)

    if (geo := _infer_geo(question)) and ("latest" in ql or "latest month" in ql):
        return sql_geo_metric_latest(geo, staff_group, measure, force_total_role=True)

    if _wants_trend(question):
        return sql_trend_last_n_months(last_n or 12, staff_group, measure, detailed_role="Total")

    return None


# -----------------------------
# Athena execution
# -----------------------------
def run_athena_query(sql: str, max_rows: int = 200, poll_seconds: float = 0.8) -> Tuple[str, List[str], List[List[Any]]]:
    ath = athena_client()

    req: Dict[str, Any] = dict(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_S3},
    )
    if ATHENA_WORKGROUP:
        req["WorkGroup"] = ATHENA_WORKGROUP

    try:
        start = ath.start_query_execution(**req)
    except ClientError as e:
        raise RuntimeError(f"Failed to start Athena query: {e}")

    qid = start["QueryExecutionId"]

    while True:
        qe = ath.get_query_execution(QueryExecutionId=qid)
        state = qe["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(poll_seconds)

    if state != "SUCCEEDED":
        reason = qe["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        raise RuntimeError(f"Athena query {state}: {reason}")

    paginator = ath.get_paginator("get_query_results")
    rows: List[List[Any]] = []
    columns: List[str] = []
    first_page = True

    for page in paginator.paginate(QueryExecutionId=qid, PaginationConfig={"PageSize": 1000}):
        rs = page["ResultSet"]
        meta_cols = rs["ResultSetMetadata"]["ColumnInfo"]
        if first_page:
            columns = [c["Name"] for c in meta_cols]
            first_page = False

        for r in rs["Rows"]:
            rows.append([d.get("VarCharValue", None) for d in r["Data"]])

        if len(rows) >= max_rows + 1:
            break

    if rows and rows[0] == columns:
        rows = rows[1:]

    return qid, columns, rows[:max_rows]


# -----------------------------
# Output formatting (no hallucination)
# -----------------------------
def to_markdown_table(columns: List[str], rows: List[List[Any]], max_rows: int = 30) -> str:
    show = rows[:max_rows]
    if not columns:
        return "No columns returned."
    if not show:
        return "No rows returned."

    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join(["---"] * len(columns)) + " |"
    body = "\n".join("| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in show)

    extra = ""
    if len(rows) > max_rows:
        extra = f"\n\n_Showing first {max_rows} rows of {len(rows)} returned._"
    return "\n".join([header, sep, body]) + extra


# -----------------------------
# Main ask()
# -----------------------------
def ask(
    memory: SimpleChatMemory,
    question: str,
    max_retries: int = 2,
) -> Dict[str, Any]:
    llm = chat_llm()
    history = memory.format_history()

    # 1) Planner
    plan = plan_question(llm, history, question)

    # 2) Deterministic SQL from plan; else LLM SQL
    sql = choose_sql_from_plan(plan, question)
    used_llm_for_sql = False
    if not sql:
        sql = generate_sql_llm(llm, history, question)
        used_llm_for_sql = True

    # 3) Semantic coverage check before running Athena
    sem = semantic_check(question, sql, plan)
    if sem:
        sql = fix_sql_llm(llm, history, question, sql, sem)

    attempts = 0
    seen_sql = set()
    last_error = ""

    while True:
        attempts += 1

        if sql in seen_sql:
            raise RuntimeError(f"Retry loop detected (same SQL repeated). Last error: {last_error}\nSQL:\n{sql}")
        seen_sql.add(sql)

        try:
            qid, cols, rows = run_athena_query(sql)
            answer = to_markdown_table(cols, rows, max_rows=30)

            memory.add(question, answer)

            return {
                "answer": answer,
                "sql": sql,
                "query_execution_id": qid,
                "attempts": attempts,
                "used_llm_for_sql": used_llm_for_sql,
                "plan": plan,
                "columns": cols,
                "rows": rows,
            }

        except Exception as e:
            last_error = str(e)
            if attempts > max_retries:
                raise RuntimeError(f"Failed after {attempts} attempt(s). Last error: {last_error}\nSQL:\n{sql}")

            sql = fix_sql_llm(llm, history, question, sql, last_error)

            # re-check semantics after repair
            sem2 = semantic_check(question, sql, plan)
            if sem2:
                sql = fix_sql_llm(llm, history, question, sql, sem2)


# -----------------------------
# Local test
# -----------------------------
if __name__ == "__main__":
    mem = SimpleChatMemory(max_turns=10)

    tests = [
        "What's the latest data we have?",
        "What is the latest month available in the data?",
        "Show me the latest month available and total FTE by staff_group",
        "Show me the latest month available and total Headcount by staff_group",
        "Trend of GP total FTE last 12 months",
        "How many doctors are there in each ICB latest month",
        "How many nurses are there in each region latest month",
        "Top 10 detailed_staff_role by total FTE in the latest month",
        "Top 5 detailed_staff_role by Headcount for GP latest month",
    ]

    for q in tests:
        print("\n" + "=" * 90)
        print("Q:", q)
        out = ask(mem, q, max_retries=2)
        print("\nPlan:\n", json.dumps(out["plan"], indent=2))
        print("\nSQL:\n", out["sql"])
        print("\nAnswer:\n", out["answer"])
