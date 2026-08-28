from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Literal
from urllib import error as urlerror
from urllib import request as urlrequest

import duckdb
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field


router = APIRouter()

PROJECT_ROOT = Path(r"G:\TTE_Local_Intelligence")
PRODUCTION_DB = PROJECT_ROOT / "production_data" / "tte_production.duckdb"
ADVANCED_CHAT_HTML = Path(__file__).resolve().parent / "static" / "advanced_chat.html"

OLLAMA_BASE_URL = os.environ.get(
    "OLLAMA_BASE_URL",
    "http://127.0.0.1:11434",
).rstrip("/")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3:4b")

RoleName = Literal[
    "management",
    "finance",
    "agriculture",
    "sustainability",
    "estate",
    "viewer",
]

ROLE_TABLE_ACCESS: dict[str, set[str]] = {
    "management": {
        "g_report_metrics",
        "plucking_plan_fields",
        "non_wage_plucking_fields",
        "agricultural_activity_records",
        "contract_records",
        "crop_field_performance",
        "crop_division_summary",
        "plucker_performance",
        "cop_line_items",
        "revenue_grade_sales",
        "invoice_sales",
        "break_even_metrics",
        "m_format_monthly_yph",
        "uploaded_files",
        "source_sheet_inventory",
        "data_quality_issues",
        "metric_approvals",
    },
    "finance": {
        "g_report_metrics",
        "crop_division_summary",
        "cop_line_items",
        "revenue_grade_sales",
        "invoice_sales",
        "break_even_metrics",
        "data_quality_issues",
        "metric_approvals",
    },
    "agriculture": {
        "g_report_metrics",
        "plucking_plan_fields",
        "non_wage_plucking_fields",
        "agricultural_activity_records",
        "contract_records",
        "crop_field_performance",
        "crop_division_summary",
        "m_format_monthly_yph",
        "data_quality_issues",
    },
    "sustainability": {
        "g_report_metrics",
        "agricultural_activity_records",
        "crop_field_performance",
        "crop_division_summary",
        "m_format_monthly_yph",
        "data_quality_issues",
    },
    "estate": {
        "g_report_metrics",
        "plucking_plan_fields",
        "non_wage_plucking_fields",
        "agricultural_activity_records",
        "contract_records",
        "crop_field_performance",
        "crop_division_summary",
        "plucker_performance",
        "m_format_monthly_yph",
        "data_quality_issues",
    },
    "viewer": {
        "g_report_metrics",
        "crop_division_summary",
        "data_quality_issues",
    },
}

PERSONAL_COLUMNS = {
    "employee_name",
    "employee_code",
    "contractor_name",
    "field_officer",
    "assistant_manager",
    "manager",
}

FINANCIAL_COLUMNS = {
    "price_rs_per_kg",
    "total_proceeds_rs",
    "net_proceeds_rs",
    "tea_profit_rs",
    "total_fixed_cost_rs",
    "variable_cost_rs_per_kg",
    "contribution_rs_per_kg",
    "contract_value",
    "labour_total_cost_rs",
}

TABLE_DESCRIPTIONS: dict[str, str] = {
    "g_report_metrics": "Division green leaf, average plucking and hectares.",
    "plucking_plan_fields": "Field anticipated and actual plucking performance.",
    "non_wage_plucking_fields": "Field non-wage plucking estimates and actuals.",
    "agricultural_activity_records": "Fertilizer, weeding and spraying records.",
    "contract_records": "Contract work, labour, costs and responsible staff.",
    "crop_field_performance": "Field made-tea crop and yield per hectare.",
    "crop_division_summary": "Division extent, made-tea crop and yield.",
    "plucker_performance": "Employee attendance and plucking performance.",
    "cop_line_items": "Cost-of-production line items.",
    "revenue_grade_sales": "Grade sales, price, proceeds, broker and buyer.",
    "invoice_sales": "Invoice quantity, net proceeds and despatch details.",
    "break_even_metrics": "Break-even, budget crop and tea profit.",
    "m_format_monthly_yph": "Field monthly YPH and nitrogen history.",
    "uploaded_files": "Approved source-file inventory.",
    "source_sheet_inventory": "Workbook sheet inventory.",
    "data_quality_issues": "Detected management and data-quality issues.",
    "metric_approvals": "Approved official metric decisions.",
}


class HistoryItem(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(max_length=4000)


class AdvancedChatRequest(BaseModel):
    question: str = Field(min_length=2, max_length=1200)
    estate_name: str | None = None
    reporting_month: str | None = None
    division_name: str | None = None
    role: RoleName = "management"
    history: list[HistoryItem] = Field(default_factory=list, max_length=12)
    previous_intent: str | None = None


class AdvancedChatResponse(BaseModel):
    answer: str
    estate_name: str
    reporting_month: str
    division_name: str | None
    role: str
    data_mode: str
    data_status: str
    intent: str
    source_tables: list[str]
    supporting_rows: list[dict[str, Any]]
    chart: dict[str, Any] | None = None
    warnings: list[str] = []
    follow_up_suggestions: list[str] = []


def open_db() -> duckdb.DuckDBPyConnection:
    if not PRODUCTION_DB.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Production database not found: {PRODUCTION_DB}",
        )
    try:
        return duckdb.connect(str(PRODUCTION_DB), read_only=True)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Unable to open production database: {exc}",
        ) from exc


def serialisable(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if isinstance(value, bytes):
        return value.hex()
    return value


def fetch_dicts(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    parameters: list[Any] | None = None,
) -> list[dict[str, Any]]:
    result = connection.execute(sql, parameters or [])
    columns = [item[0] for item in result.description]
    return [
        {
            column: serialisable(value)
            for column, value in zip(columns, row)
        }
        for row in result.fetchall()
    ]


def table_exists(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
) -> bool:
    return bool(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_type = 'BASE TABLE'
              AND table_name = ?
            """,
            [table_name],
        ).fetchone()[0]
    )


def has_table_access(role: str, table_name: str) -> bool:
    return table_name in ROLE_TABLE_ACCESS.get(role, set())


def require_table_access(role: str, table_name: str) -> None:
    if not has_table_access(role, table_name):
        raise HTTPException(
            status_code=403,
            detail=f"The selected role cannot access {table_name}.",
        )


def mask_rows(
    rows: list[dict[str, Any]],
    role: str,
) -> list[dict[str, Any]]:
    masked: list[dict[str, Any]] = []
    for row in rows:
        output: dict[str, Any] = {}
        for key, value in row.items():
            if key in PERSONAL_COLUMNS and role not in {"management", "estate"}:
                output[key] = "[RESTRICTED]"
            elif key in FINANCIAL_COLUMNS and role not in {"management", "finance"}:
                output[key] = "[RESTRICTED]"
            else:
                output[key] = value
        masked.append(output)
    return masked


def format_number(value: Any, decimals: int = 2) -> str:
    if value is None:
        return "not available"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:,.{decimals}f}"


def get_batches(
    connection: duckdb.DuckDBPyConnection,
) -> list[dict[str, Any]]:
    if not table_exists(connection, "batch_analytics"):
        return []
    return fetch_dicts(
        connection,
        """
        SELECT
            batch_id,
            estate_name,
            reporting_month,
            analytics_status,
            parser_version,
            parsed_at,
            metrics_approved_at
        FROM batch_analytics
        ORDER BY
            estate_name,
            reporting_month DESC
        """,
    )


def resolve_batch(
    connection: duckdb.DuckDBPyConnection,
    estate_name: str | None,
    reporting_month: str | None,
) -> dict[str, Any]:
    batches = get_batches(connection)
    if not batches:
        raise HTTPException(
            status_code=404,
            detail="No parsed production batches are available.",
        )

    candidates = batches
    if estate_name:
        candidates = [
            item
            for item in candidates
            if str(item["estate_name"]).casefold()
            == estate_name.strip().casefold()
        ]
    if reporting_month:
        candidates = [
            item
            for item in candidates
            if str(item["reporting_month"]) == reporting_month.strip()
        ]

    if not candidates:
        raise HTTPException(
            status_code=404,
            detail="No parsed batch matches the selected estate and month.",
        )
    return candidates[0]


def get_divisions(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
) -> list[str]:
    divisions: set[str] = set()
    for table_name in (
        "g_report_metrics",
        "plucking_plan_fields",
        "crop_division_summary",
        "crop_field_performance",
        "agricultural_activity_records",
        "contract_records",
    ):
        if not table_exists(connection, table_name):
            continue
        columns = {
            row[0]
            for row in connection.execute(
                f"DESCRIBE {table_name}"
            ).fetchall()
        }
        if "division_name" not in columns:
            continue
        rows = fetch_dicts(
            connection,
            f"""
            SELECT DISTINCT division_name
            FROM {table_name}
            WHERE batch_id = ?
              AND division_name IS NOT NULL
              AND TRIM(division_name) <> ''
            """,
            [batch_id],
        )
        divisions.update(str(row["division_name"]) for row in rows)
    return sorted(divisions)


def resolve_division(
    question: str,
    selected_division: str | None,
    estate_name: str,
    divisions: list[str],
) -> str | None:
    if selected_division:
        for division in divisions:
            if division.casefold() == selected_division.strip().casefold():
                return division

    normalised = re.sub(r"[^a-z0-9]+", " ", question.lower()).strip()
    estate_token = re.sub(r"[^a-z0-9]+", " ", estate_name.lower()).strip()

    for division in divisions:
        token = re.sub(r"[^a-z0-9]+", " ", division.lower()).strip()
        if not token:
            continue
        if token == estate_token:
            explicit = (
                re.search(rf"\b{re.escape(token)}\s+division\b", normalised)
                or re.search(rf"\bdivision\s+{re.escape(token)}\b", normalised)
            )
            if explicit:
                return division
            continue
        if re.search(rf"\b{re.escape(token)}\b", normalised):
            return division
    return None


def status_mode(analytics_status: str) -> tuple[str, str | None]:
    status = str(analytics_status).upper()
    if "APPROVED" in status and "PENDING" not in status:
        return "APPROVED", None
    return (
        "PREVIEW",
        "Parsed metrics are visible for review but are not final approved metrics.",
    )


def governance_note(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
) -> tuple[str | None, list[str]]:
    if not table_exists(connection, "data_quality_issues"):
        return None, []

    issues = fetch_dicts(
        connection,
        """
        SELECT
            issue_type,
            severity,
            metric_name,
            status,
            source_a_name,
            source_a_value,
            source_b_name,
            source_b_value,
            difference_value,
            difference_pct,
            manager_message,
            official_source,
            official_value,
            resolved_by,
            resolution_note
        FROM data_quality_issues
        WHERE batch_id = ?
        ORDER BY
            CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
            status,
            detected_at DESC
        """,
        [batch_id],
    )

    warnings = [
        str(item["manager_message"])
        for item in issues
        if item.get("status") == "OPEN"
    ]

    reconciliation = next(
        (
            item
            for item in issues
            if item.get("issue_type") == "SOURCE_RECONCILIATION"
            and item.get("metric_name") == "green_leaf_kg"
        ),
        None,
    )

    if not reconciliation:
        return None, warnings

    if reconciliation.get("status") == "RESOLVED":
        return (
            "[OFFICIAL METRIC APPROVED] "
            f"Official green leaf is "
            f"{format_number(reconciliation.get('official_value'), 0)} kg, "
            f"based on {reconciliation.get('official_source')}. "
            f"The source difference is "
            f"{format_number(reconciliation.get('difference_value'), 0)} kg "
            f"({format_number(reconciliation.get('difference_pct'), 2)}%). "
            f"Approved by {reconciliation.get('resolved_by')}.",
            warnings,
        )

    return (
        "[MANAGEMENT ALERT - RECONCILIATION PENDING] "
        f"The G Report records "
        f"{format_number(reconciliation.get('source_a_value'), 0)} kg and "
        f"the Plucking Plan records "
        f"{format_number(reconciliation.get('source_b_value'), 0)} kg. "
        f"The difference is "
        f"{format_number(reconciliation.get('difference_value'), 0)} kg "
        f"({format_number(reconciliation.get('difference_pct'), 2)}%). "
        "No official green-leaf figure has been approved.",
        warnings,
    )


def infer_intent(
    question: str,
    previous_intent: str | None,
) -> str:
    lower = question.lower()

    patterns: list[tuple[str, str]] = [
        (r"\b(data quality|issue|problem|warning|anomal|outlier|reconciliation)\b", "data_quality"),
        (r"\b(compare month|month comparison|trend|previous month|last month)\b", "compare_months"),
        (r"\b(compare|comparison|versus|vs)\b.*\bdivision", "compare_divisions"),
        (r"\b(compare all divisions|which division|highest division|lowest division)\b", "compare_divisions"),
        (r"\b(break even|break-even|tea profit|fixed cost|contribution)\b", "break_even"),
        (r"\b(cop|cost of production|cost per hectare|cost share)\b", "cop"),
        (r"\b(revenue|grade sales|buyer|broker|invoice|proceeds|despatch|dispatch)\b", "revenue"),
        (r"\b(contract|contractor|labour cost|work type|hectares awarded)\b", "contracts"),
        (r"\b(fertilizer|fertiliser|weeding|spraying|agricultural activ)\b", "agriculture"),
        (r"\b(plucker|employee|worker|attendance|top worker)\b", "plucker"),
        (r"\b(m format|monthly yph|nitrogen|yield history)\b", "m_format"),
        (r"\b(field|fields)\b.*\b(yield|crop|performance)\b", "field_crop"),
        (r"\b(made tea|crop|yield)\b", "crop"),
        (r"\b(source file|uploaded file|workbook|sheet inventory|show all files)\b", "sources"),
        (r"\b(what data|available data|categories|what can i ask)\b", "catalog"),
        (r"\b(summary|overview|management|performance|green leaf|g report)\b", "summary"),
    ]
    for pattern, intent in patterns:
        if re.search(pattern, lower):
            return intent

    if re.search(r"\b(it|that|those|same|more detail|show details)\b", lower):
        return previous_intent or "summary"
    return previous_intent or "summary"


def with_division_filter(
    batch_id: str,
    division: str | None,
) -> tuple[str, list[Any]]:
    conditions = ["batch_id = ?"]
    params: list[Any] = [batch_id]
    if division:
        conditions.append("division_name = ?")
        params.append(division)
    return " AND ".join(conditions), params


def query_summary(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    division: str | None,
    role: str,
) -> tuple[str, list[str], list[dict[str, Any]], dict[str, Any] | None]:
    for table_name in (
        "g_report_metrics",
        "plucking_plan_fields",
        "crop_division_summary",
        "cop_line_items",
        "break_even_metrics",
    ):
        if table_exists(connection, table_name):
            require_table_access(role, table_name)

    where_clause, params = with_division_filter(batch["batch_id"], division)
    g = fetch_dicts(
        connection,
        f"""
        SELECT
            ROUND(SUM(total_green_leaf_kg), 2) AS g_report_green_leaf_kg,
            ROUND(AVG(average_plucking_kg), 2) AS average_plucking_kg,
            ROUND(SUM(total_hectare_plucked), 2) AS hectare_plucked
        FROM g_report_metrics
        WHERE {where_clause}
        """,
        params,
    )[0]
    p = fetch_dicts(
        connection,
        f"""
        SELECT
            COUNT(*) AS field_count,
            ROUND(SUM(actual_green_leaf_kg), 2) AS actual_green_leaf_kg,
            ROUND(SUM(anticipated_green_leaf_kg), 2) AS anticipated_green_leaf_kg
        FROM plucking_plan_fields
        WHERE {where_clause}
        """,
        params,
    )[0]
    c = fetch_dicts(
        connection,
        f"""
        SELECT
            ROUND(SUM(made_tea_crop_kg), 2) AS made_tea_crop_kg,
            ROUND(SUM(extent_ha), 2) AS extent_ha,
            ROUND(
                SUM(made_tea_crop_kg) / NULLIF(SUM(extent_ha), 0),
                2
            ) AS weighted_yield_kg_per_ha
        FROM crop_division_summary
        WHERE {where_clause}
        """,
        params,
    )[0]

    cop = fetch_dicts(
        connection,
        """
        SELECT description, rupees_per_kg, cost_per_hectare
        FROM cop_line_items
        WHERE batch_id = ?
          AND UPPER(description) LIKE '%TOTAL%C.O.P%'
        ORDER BY line_number DESC
        LIMIT 1
        """,
        [batch["batch_id"]],
    )
    cop_row = cop[0] if cop else {}

    difference = (
        float(p["actual_green_leaf_kg"]) - float(g["g_report_green_leaf_kg"])
        if p.get("actual_green_leaf_kg") is not None
        and g.get("g_report_green_leaf_kg") is not None
        else None
    )

    scope = division or batch["estate_name"]
    answer = (
        f"{scope} summary for {batch['reporting_month']}: "
        f"G Report green leaf was "
        f"{format_number(g.get('g_report_green_leaf_kg'), 0)} kg; "
        f"Plucking Plan actual green leaf was "
        f"{format_number(p.get('actual_green_leaf_kg'), 0)} kg; "
        f"made-tea crop was "
        f"{format_number(c.get('made_tea_crop_kg'), 0)} kg; "
        f"weighted made-tea yield was "
        f"{format_number(c.get('weighted_yield_kg_per_ha'), 2)} kg/ha; "
        f"and total COP was Rs. "
        f"{format_number(cop_row.get('rupees_per_kg'), 2)} per kg."
    )
    if difference is not None:
        answer += (
            f" The green-leaf source difference was "
            f"{format_number(difference, 0)} kg."
        )

    supporting = [
        {"metric": "G Report green leaf kg", "value": g.get("g_report_green_leaf_kg")},
        {"metric": "Plucking Plan actual green leaf kg", "value": p.get("actual_green_leaf_kg")},
        {"metric": "Green-leaf difference kg", "value": difference},
        {"metric": "Made-tea crop kg", "value": c.get("made_tea_crop_kg")},
        {"metric": "Weighted yield kg/ha", "value": c.get("weighted_yield_kg_per_ha")},
        {"metric": "COP Rs/kg", "value": cop_row.get("rupees_per_kg")},
    ]
    chart = {
        "type": "bar",
        "title": "Green Leaf Source Comparison",
        "labels": ["G Report", "Plucking Plan"],
        "values": [
            g.get("g_report_green_leaf_kg"),
            p.get("actual_green_leaf_kg"),
        ],
        "unit": "kg",
    }
    return (
        answer,
        [
            "g_report_metrics",
            "plucking_plan_fields",
            "crop_division_summary",
            "cop_line_items",
        ],
        supporting,
        chart,
    )


def query_compare_divisions(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    role: str,
) -> tuple[str, list[str], list[dict[str, Any]], dict[str, Any] | None]:
    for table_name in (
        "g_report_metrics",
        "plucking_plan_fields",
        "crop_division_summary",
    ):
        require_table_access(role, table_name)

    rows = fetch_dicts(
        connection,
        """
        WITH g AS (
            SELECT
                division_name,
                SUM(total_green_leaf_kg) AS g_report_green_leaf_kg
            FROM g_report_metrics
            WHERE batch_id = ?
            GROUP BY division_name
        ),
        p AS (
            SELECT
                division_name,
                SUM(actual_green_leaf_kg) AS plucking_plan_green_leaf_kg
            FROM plucking_plan_fields
            WHERE batch_id = ?
            GROUP BY division_name
        ),
        c AS (
            SELECT
                division_name,
                SUM(extent_ha) AS extent_ha,
                SUM(made_tea_crop_kg) AS made_tea_crop_kg,
                SUM(made_tea_crop_kg) / NULLIF(SUM(extent_ha), 0)
                    AS yield_kg_per_ha
            FROM crop_division_summary
            WHERE batch_id = ?
            GROUP BY division_name
        )
        SELECT
            COALESCE(g.division_name, p.division_name, c.division_name)
                AS division_name,
            ROUND(g.g_report_green_leaf_kg, 2) AS g_report_green_leaf_kg,
            ROUND(p.plucking_plan_green_leaf_kg, 2) AS plucking_plan_green_leaf_kg,
            ROUND(
                p.plucking_plan_green_leaf_kg - g.g_report_green_leaf_kg,
                2
            ) AS difference_kg,
            ROUND(c.made_tea_crop_kg, 2) AS made_tea_crop_kg,
            ROUND(c.yield_kg_per_ha, 2) AS yield_kg_per_ha
        FROM g
        FULL OUTER JOIN p
          ON p.division_name = g.division_name
        FULL OUTER JOIN c
          ON c.division_name = COALESCE(g.division_name, p.division_name)
        ORDER BY division_name
        """,
        [batch["batch_id"], batch["batch_id"], batch["batch_id"]],
    )
    valid = [row for row in rows if row.get("yield_kg_per_ha") is not None]
    if not valid:
        return (
            "No division comparison data is available.",
            ["g_report_metrics", "plucking_plan_fields", "crop_division_summary"],
            rows,
            None,
        )
    highest = max(valid, key=lambda row: float(row["yield_kg_per_ha"]))
    lowest = min(valid, key=lambda row: float(row["yield_kg_per_ha"]))
    answer = (
        f"Division comparison for {batch['estate_name']} in "
        f"{batch['reporting_month']}: "
        f"{highest['division_name']} had the highest made-tea yield at "
        f"{format_number(highest['yield_kg_per_ha'], 2)} kg/ha, while "
        f"{lowest['division_name']} had the lowest at "
        f"{format_number(lowest['yield_kg_per_ha'], 2)} kg/ha."
    )
    chart = {
        "type": "bar",
        "title": "Made-Tea Yield by Division",
        "labels": [row["division_name"] for row in rows],
        "values": [row.get("yield_kg_per_ha") for row in rows],
        "unit": "kg/ha",
    }
    return (
        answer,
        ["g_report_metrics", "plucking_plan_fields", "crop_division_summary"],
        rows,
        chart,
    )


def query_compare_months(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    role: str,
) -> tuple[str, list[str], list[dict[str, Any]], dict[str, Any] | None]:
    require_table_access(role, "g_report_metrics")
    require_table_access(role, "crop_division_summary")

    rows = fetch_dicts(
        connection,
        """
        WITH b AS (
            SELECT batch_id, reporting_month
            FROM batch_analytics
            WHERE LOWER(estate_name) = LOWER(?)
        ),
        g AS (
            SELECT
                b.reporting_month,
                SUM(g.total_green_leaf_kg) AS green_leaf_kg
            FROM b
            JOIN g_report_metrics g
              ON g.batch_id = b.batch_id
            GROUP BY b.reporting_month
        ),
        c AS (
            SELECT
                b.reporting_month,
                SUM(c.made_tea_crop_kg) AS made_tea_crop_kg,
                SUM(c.made_tea_crop_kg) / NULLIF(SUM(c.extent_ha), 0)
                    AS yield_kg_per_ha
            FROM b
            JOIN crop_division_summary c
              ON c.batch_id = b.batch_id
            GROUP BY b.reporting_month
        )
        SELECT
            COALESCE(g.reporting_month, c.reporting_month) AS reporting_month,
            ROUND(g.green_leaf_kg, 2) AS green_leaf_kg,
            ROUND(c.made_tea_crop_kg, 2) AS made_tea_crop_kg,
            ROUND(c.yield_kg_per_ha, 2) AS yield_kg_per_ha
        FROM g
        FULL OUTER JOIN c
          ON c.reporting_month = g.reporting_month
        ORDER BY reporting_month
        """,
        [batch["estate_name"]],
    )

    if len(rows) < 2:
        return (
            f"Only one parsed month is available for {batch['estate_name']}. "
            "Upload and approve another month before running a month comparison.",
            ["g_report_metrics", "crop_division_summary"],
            rows,
            None,
        )
    first, last = rows[0], rows[-1]
    change = (
        float(last["made_tea_crop_kg"]) - float(first["made_tea_crop_kg"])
        if last.get("made_tea_crop_kg") is not None
        and first.get("made_tea_crop_kg") is not None
        else None
    )
    answer = (
        f"{batch['estate_name']} month comparison from "
        f"{first['reporting_month']} to {last['reporting_month']}: "
        f"made-tea crop changed by {format_number(change, 0)} kg."
    )
    chart = {
        "type": "line",
        "title": "Monthly Made-Tea Crop",
        "labels": [row["reporting_month"] for row in rows],
        "values": [row.get("made_tea_crop_kg") for row in rows],
        "unit": "kg",
    }
    return (
        answer,
        ["g_report_metrics", "crop_division_summary"],
        rows,
        chart,
    )


def query_table(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    batch: dict[str, Any],
    division: str | None,
    role: str,
    order_by: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    require_table_access(role, table_name)
    if not table_exists(connection, table_name):
        return []

    columns = {
        row[0]
        for row in connection.execute(f"DESCRIBE {table_name}").fetchall()
    }
    conditions = ["batch_id = ?"]
    params: list[Any] = [batch["batch_id"]]
    if division and "division_name" in columns:
        conditions.append("division_name = ?")
        params.append(division)

    sql = f"SELECT * FROM {table_name} WHERE {' AND '.join(conditions)}"
    if order_by and order_by in columns:
        sql += f" ORDER BY {order_by}"
    sql += " LIMIT ?"
    params.append(min(limit, 100))
    return mask_rows(fetch_dicts(connection, sql, params), role)


def query_cop(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    role: str,
) -> tuple[str, list[str], list[dict[str, Any]], dict[str, Any] | None]:
    require_table_access(role, "cop_line_items")
    rows = fetch_dicts(
        connection,
        """
        SELECT
            line_number,
            description,
            ROUND(rupees_per_kg, 2) AS rupees_per_kg,
            ROUND(cost_share * 100, 2) AS cost_share_pct,
            ROUND(cost_per_hectare, 2) AS cost_per_hectare
        FROM cop_line_items
        WHERE batch_id = ?
        ORDER BY rupees_per_kg DESC NULLS LAST
        LIMIT 100
        """,
        [batch["batch_id"]],
    )
    rows = mask_rows(rows, role)
    total = next(
        (
            row
            for row in rows
            if "TOTAL C.O.P" in str(row.get("description", "")).upper()
        ),
        None,
    )
    answer = (
        f"Total COP for {batch['estate_name']} in "
        f"{batch['reporting_month']} was Rs. "
        f"{format_number(total.get('rupees_per_kg') if total else None, 2)} "
        "per kg."
    )
    top = [
        row for row in rows
        if isinstance(row.get("rupees_per_kg"), (int, float))
        and "TOTAL" not in str(row.get("description", "")).upper()
    ][:10]
    chart = {
        "type": "bar",
        "title": "Top COP Components",
        "labels": [row.get("description") for row in top],
        "values": [row.get("rupees_per_kg") for row in top],
        "unit": "Rs/kg",
    }
    return answer, ["cop_line_items"], rows, chart


def query_break_even(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    role: str,
) -> tuple[str, list[str], list[dict[str, Any]], dict[str, Any] | None]:
    require_table_access(role, "break_even_metrics")
    rows = fetch_dicts(
        connection,
        """
        SELECT
            metric_period,
            ROUND(total_fixed_cost_rs, 2) AS total_fixed_cost_rs,
            ROUND(variable_cost_rs_per_kg, 2) AS variable_cost_rs_per_kg,
            ROUND(contribution_rs_per_kg, 2) AS contribution_rs_per_kg,
            ROUND(break_even_quantity_kg, 2) AS break_even_quantity_kg,
            ROUND(budget_crop_kg, 2) AS budget_crop_kg,
            ROUND(budget_cop_rs_per_kg, 2) AS budget_cop_rs_per_kg,
            ROUND(tea_profit_rs, 2) AS tea_profit_rs,
            ROUND(break_even_pct_of_budget * 100, 2)
                AS break_even_pct_of_budget
        FROM break_even_metrics
        WHERE batch_id = ?
        ORDER BY metric_period
        """,
        [batch["batch_id"]],
    )
    rows = mask_rows(rows, role)
    parts = [
        (
            f"{row.get('metric_period')}: break-even "
            f"{format_number(row.get('break_even_quantity_kg'), 0)} kg, "
            f"budget crop {format_number(row.get('budget_crop_kg'), 0)} kg, "
            f"break-even percentage "
            f"{format_number(row.get('break_even_pct_of_budget'), 2)}%"
        )
        for row in rows
    ]
    answer = (
        f"Break-even position for {batch['estate_name']} in "
        f"{batch['reporting_month']}: " + "; ".join(parts) + "."
    )
    chart = {
        "type": "bar",
        "title": "Break-Even Quantity vs Budget Crop",
        "labels": [row.get("metric_period") for row in rows],
        "series": [
            {
                "name": "Break-even quantity",
                "values": [row.get("break_even_quantity_kg") for row in rows],
            },
            {
                "name": "Budget crop",
                "values": [row.get("budget_crop_kg") for row in rows],
            },
        ],
        "unit": "kg",
    }
    return answer, ["break_even_metrics"], rows, chart


def query_data_quality(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    role: str,
) -> tuple[str, list[str], list[dict[str, Any]], dict[str, Any] | None]:
    require_table_access(role, "data_quality_issues")
    if not table_exists(connection, "data_quality_issues"):
        return (
            "The governance scan has not been installed yet.",
            [],
            [],
            None,
        )
    rows = fetch_dicts(
        connection,
        """
        SELECT
            issue_type,
            severity,
            metric_name,
            status,
            manager_message,
            official_source,
            official_value,
            resolved_by,
            resolution_note
        FROM data_quality_issues
        WHERE batch_id = ?
        ORDER BY
            CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
            status
        """,
        [batch["batch_id"]],
    )
    open_rows = [row for row in rows if row.get("status") == "OPEN"]
    answer = (
        f"{batch['estate_name']} {batch['reporting_month']} has "
        f"{len(open_rows)} open management/data-quality issue(s)."
    )
    return answer, ["data_quality_issues"], rows, None


def generic_table_intent(
    connection: duckdb.DuckDBPyConnection,
    intent: str,
    batch: dict[str, Any],
    division: str | None,
    role: str,
) -> tuple[str, list[str], list[dict[str, Any]], dict[str, Any] | None]:
    mapping = {
        "agriculture": ("agricultural_activity_records", "activity_type"),
        "contracts": ("contract_records", "work_type"),
        "plucker": ("plucker_performance", "green_leaf_kg"),
        "m_format": ("m_format_monthly_yph", "metric_year"),
        "field_crop": ("crop_field_performance", "yield_kg_per_ha"),
        "crop": ("crop_division_summary", "division_name"),
        "revenue": ("revenue_grade_sales", "total_proceeds_rs"),
        "sources": ("uploaded_files", "category"),
    }
    table_name, order_by = mapping[intent]
    rows = query_table(
        connection,
        table_name,
        batch,
        division,
        role,
        order_by=order_by,
        limit=100,
    )
    answer = (
        f"{len(rows)} {TABLE_DESCRIPTIONS.get(table_name, table_name)} "
        f"row(s) are displayed for {division or batch['estate_name']} "
        f"in {batch['reporting_month']}."
    )
    return answer, [table_name], rows, None


def catalog_response(
    connection: duckdb.DuckDBPyConnection,
    role: str,
) -> tuple[str, list[str], list[dict[str, Any]], dict[str, Any] | None]:
    rows = []
    for table_name in sorted(ROLE_TABLE_ACCESS.get(role, set())):
        if table_exists(connection, table_name):
            rows.append(
                {
                    "table_name": table_name,
                    "description": TABLE_DESCRIPTIONS.get(table_name, ""),
                }
            )
    answer = (
        f"The {role} role can ask questions across "
        f"{len(rows)} available production data categories."
    )
    return answer, [row["table_name"] for row in rows], rows, None


def try_ollama_summary(
    question: str,
    answer: str,
    rows: list[dict[str, Any]],
    data_mode: str,
) -> str:
    if not rows:
        return answer

    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Rewrite the supplied deterministic answer into clear "
                    "management language. Use only the supplied answer and rows. "
                    "Do not add causes, recommendations or figures. "
                    "Return only the final answer. /no_think"
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "question": question,
                        "deterministic_answer": answer,
                        "data_mode": data_mode,
                        "rows": rows[:20],
                    },
                    default=str,
                    ensure_ascii=False,
                ),
            },
        ],
        "stream": False,
        "think": False,
        "options": {
            "temperature": 0.0,
            "num_predict": 250,
            "num_ctx": 8192,
        },
    }
    req = urlrequest.Request(
        f"{OLLAMA_BASE_URL}/api/chat",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=120) as response:
            result = json.loads(response.read().decode("utf-8"))
    except (urlerror.URLError, TimeoutError, json.JSONDecodeError):
        return answer

    text = str(result.get("message", {}).get("content", "")).strip()
    if not text:
        return answer
    if re.search(
        r"\b(we are given|rules to follow|the user asks|from the context)\b",
        text,
        flags=re.IGNORECASE,
    ):
        return answer
    text = re.sub(
        r"<think>.*?</think>",
        "",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    ).strip()
    return text or answer


@router.get("/assistant", include_in_schema=False)
def advanced_chat_page() -> FileResponse:
    if not ADVANCED_CHAT_HTML.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Advanced chatbot page not found: {ADVANCED_CHAT_HTML}",
        )
    return FileResponse(ADVANCED_CHAT_HTML)


@router.get("/api/assistant/options")
def advanced_options() -> dict[str, Any]:
    connection = open_db()
    try:
        batches = get_batches(connection)
        enriched = []
        for batch in batches:
            enriched.append(
                {
                    **batch,
                    "divisions": get_divisions(connection, batch["batch_id"]),
                    "data_mode": status_mode(batch["analytics_status"])[0],
                }
            )
        return {
            "batches": enriched,
            "roles": sorted(ROLE_TABLE_ACCESS),
            "examples": [
                "Give me the management summary.",
                "Show all management problems and data-quality issues.",
                "Compare all divisions.",
                "Compare this estate across months.",
                "What was the total COP per kilogram?",
                "Explain the break-even position.",
                "Show fertilizer, weeding and spraying records.",
                "Show contract information.",
                "Show the top field yields.",
                "Show revenue and grade sales.",
                "What data can my role access?",
            ],
        }
    finally:
        connection.close()


@router.post(
    "/api/assistant",
    response_model=AdvancedChatResponse,
)
def advanced_chat(
    payload: AdvancedChatRequest,
) -> AdvancedChatResponse:
    connection = open_db()
    try:
        batch = resolve_batch(
            connection,
            payload.estate_name,
            payload.reporting_month,
        )
        divisions = get_divisions(connection, batch["batch_id"])
        division = resolve_division(
            payload.question,
            payload.division_name,
            batch["estate_name"],
            divisions,
        )
        data_mode, mode_warning = status_mode(batch["analytics_status"])
        intent = infer_intent(payload.question, payload.previous_intent)

        if intent == "summary":
            answer, tables, rows, chart = query_summary(
                connection, batch, division, payload.role
            )
        elif intent == "compare_divisions":
            answer, tables, rows, chart = query_compare_divisions(
                connection, batch, payload.role
            )
        elif intent == "compare_months":
            answer, tables, rows, chart = query_compare_months(
                connection, batch, payload.role
            )
        elif intent == "cop":
            answer, tables, rows, chart = query_cop(
                connection, batch, payload.role
            )
        elif intent == "break_even":
            answer, tables, rows, chart = query_break_even(
                connection, batch, payload.role
            )
        elif intent == "data_quality":
            answer, tables, rows, chart = query_data_quality(
                connection, batch, payload.role
            )
        elif intent == "catalog":
            answer, tables, rows, chart = catalog_response(
                connection, payload.role
            )
        elif intent in {
            "agriculture",
            "contracts",
            "plucker",
            "m_format",
            "field_crop",
            "crop",
            "revenue",
            "sources",
        }:
            answer, tables, rows, chart = generic_table_intent(
                connection,
                intent,
                batch,
                division,
                payload.role,
            )
        else:
            answer, tables, rows, chart = catalog_response(
                connection, payload.role
            )

        note, governance_warnings = governance_note(
            connection,
            batch["batch_id"],
        )

        warnings = []
        if mode_warning:
            warnings.append(mode_warning)
        warnings.extend(governance_warnings)

        if note and intent in {
            "summary",
            "compare_divisions",
            "compare_months",
            "data_quality",
        }:
            answer = note + "\n\n" + answer

        prefix = (
            "[PREVIEW - NOT FINAL APPROVED] "
            if data_mode == "PREVIEW"
            else ""
        )
        if prefix and not answer.startswith(prefix):
            answer = prefix + answer

        answer = try_ollama_summary(
            payload.question,
            answer,
            rows,
            data_mode,
        )

        suggestions = {
            "summary": [
                "Compare all divisions.",
                "Show all management problems.",
                "Explain the break-even position.",
            ],
            "compare_divisions": [
                "Show the field yields.",
                "Compare this estate across months.",
            ],
            "data_quality": [
                "Open the governance page to resolve official metrics.",
            ],
        }.get(intent, ["Give me the management summary."])

        return AdvancedChatResponse(
            answer=answer,
            estate_name=batch["estate_name"],
            reporting_month=batch["reporting_month"],
            division_name=division,
            role=payload.role,
            data_mode=data_mode,
            data_status=batch["analytics_status"],
            intent=intent,
            source_tables=[
                table
                for table in tables
                if table_exists(connection, table)
            ],
            supporting_rows=mask_rows(rows[:100], payload.role),
            chart=chart,
            warnings=warnings,
            follow_up_suggestions=suggestions,
        )
    finally:
        connection.close()
