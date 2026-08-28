from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest

import duckdb
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field


router = APIRouter()

PROJECT_ROOT = Path(r"G:\TTE_Local_Intelligence")
PRODUCTION_DB = (
    PROJECT_ROOT
    / "production_data"
    / "tte_production.duckdb"
)
CHAT_HTML = (
    Path(__file__).resolve().parent
    / "static"
    / "chat.html"
)

OLLAMA_BASE_URL = os.environ.get(
    "OLLAMA_BASE_URL",
    "http://127.0.0.1:11434",
).rstrip("/")

OLLAMA_MODEL = os.environ.get(
    "OLLAMA_MODEL",
    "qwen3:4b",
)

MAX_SUPPORTING_ROWS = 100

TABLE_CATALOG: dict[str, dict[str, Any]] = {
    "g_report_metrics": {
        "title": "G Report metrics",
        "description": (
            "Division green leaf, plucking average, "
            "checkroll measure and hectare information."
        ),
        "keywords": [
            "g report", "green leaf", "checkroll",
            "plucking average", "hectare plucked",
        ],
    },
    "plucking_plan_fields": {
        "title": "Plucking plan",
        "description": (
            "Field-level anticipated and actual rounds, "
            "pluckers and green leaf."
        ),
        "keywords": [
            "plucking plan", "anticipated", "actual plucking",
            "rounds", "lph", "pph",
        ],
    },
    "non_wage_plucking_fields": {
        "title": "Non-wage plucking",
        "description": (
            "Field-level round estimates and actual kilograms "
            "for non-wage plucking."
        ),
        "keywords": [
            "non wage", "non-wage", "round estimate",
            "round actual",
        ],
    },
    "agricultural_activity_records": {
        "title": "Agricultural activities",
        "description": (
            "Fertilizer, weeding, spraying and other "
            "field activity records."
        ),
        "keywords": [
            "fertilizer", "fertiliser", "weeding",
            "spraying", "agricultural activity", "activity",
        ],
    },
    "contract_records": {
        "title": "Contract monitoring",
        "description": (
            "Contract work, contractor, labour allocation, "
            "cost and responsible officers."
        ),
        "keywords": [
            "contract", "contractor", "labour cost",
            "work type", "hectares awarded",
        ],
    },
    "crop_field_performance": {
        "title": "Field crop performance",
        "description": (
            "Field-level made-tea crop and yield per hectare."
        ),
        "keywords": [
            "field crop", "field yield", "lowest field",
            "highest field", "field performance",
        ],
    },
    "crop_division_summary": {
        "title": "Division crop summary",
        "description": (
            "Division-level extent, made-tea crop and "
            "yield per hectare."
        ),
        "keywords": [
            "made tea", "division crop", "division yield",
            "crop summary", "yield per hectare",
        ],
    },
    "plucker_performance": {
        "title": "Plucker performance",
        "description": (
            "Employee attendance, green leaf and "
            "average plucking kilograms."
        ),
        "keywords": [
            "plucker", "employee", "attendance",
            "days attended", "average kg", "worker",
        ],
    },
    "cop_line_items": {
        "title": "Cost of production",
        "description": (
            "COP line items, rupees per kilogram, "
            "cost share and cost per hectare."
        ),
        "keywords": [
            "cop", "cost of production", "rupees per kg",
            "cost share", "cost per hectare",
        ],
    },
    "revenue_grade_sales": {
        "title": "Revenue and grade sales",
        "description": (
            "Broker, grade, quantity, price, proceeds "
            "and buyer information."
        ),
        "keywords": [
            "revenue", "grade", "broker", "buyer",
            "proceeds", "selling price", "sales",
        ],
    },
    "invoice_sales": {
        "title": "Invoice sales",
        "description": (
            "Invoice dates, grades, sold quantity, "
            "net proceeds and despatch details."
        ),
        "keywords": [
            "invoice", "despatch", "dispatch",
            "net proceeds", "sold quantity",
        ],
    },
    "break_even_metrics": {
        "title": "Break-even",
        "description": (
            "Fixed cost, contribution, break-even quantity, "
            "budget crop, COP and tea profit."
        ),
        "keywords": [
            "break even", "break-even", "fixed cost",
            "contribution", "tea profit", "budget crop",
        ],
    },
    "m_format_monthly_yph": {
        "title": "M Format monthly YPH",
        "description": (
            "Field monthly YPH and nitrogen values "
            "by metric year and month."
        ),
        "keywords": [
            "m format", "monthly yph", "nitrogen",
            "yph history", "yield history",
        ],
    },
    "source_sheet_inventory": {
        "title": "Source sheet inventory",
        "description": (
            "Workbook sheet names, non-empty row counts "
            "and dimensions."
        ),
        "keywords": [
            "source sheet", "workbook", "sheet count",
            "inventory", "source data",
        ],
    },
    "uploaded_files": {
        "title": "Uploaded source files",
        "description": (
            "Approved upload file names, categories, "
            "divisions and validation status."
        ),
        "keywords": [
            "source file", "uploaded file", "file name",
            "file category", "validation status",
        ],
    },
}


class DatasetChatRequest(BaseModel):
    question: str = Field(
        min_length=2,
        max_length=1000,
    )
    estate_name: str | None = None
    reporting_month: str | None = None
    division_name: str | None = None


class DatasetChatResponse(BaseModel):
    answer: str
    model: str
    data_mode: str
    data_status: str
    estate_name: str
    reporting_month: str
    division_name: str | None = None
    source_tables: list[str]
    supporting_rows: list[dict[str, Any]]
    supporting_row_count: int
    warning: str | None = None


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


def open_db() -> duckdb.DuckDBPyConnection:
    if not PRODUCTION_DB.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Production database not found: {PRODUCTION_DB}",
        )

    try:
        return duckdb.connect(
            str(PRODUCTION_DB),
            read_only=True,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Unable to open production database: {exc}",
        ) from exc


def fetch_dicts(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    parameters: list[Any] | None = None,
) -> list[dict[str, Any]]:
    result = connection.execute(
        sql,
        parameters or [],
    )

    columns = [
        item[0]
        for item in result.description
    ]

    output: list[dict[str, Any]] = []

    for row in result.fetchall():
        output.append(
            {
                column: serialisable(value)
                for column, value in zip(columns, row)
            }
        )

    return output


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


def get_batches(
    connection: duckdb.DuckDBPyConnection,
) -> list[dict[str, Any]]:
    if not table_exists(connection, "batch_analytics"):
        return []

    upload_join = ""

    if table_exists(connection, "upload_batches"):
        upload_join = """
        LEFT JOIN upload_batches u
          ON u.batch_id = a.batch_id
        """

    return fetch_dicts(
        connection,
        f"""
        SELECT
            a.batch_id,
            a.estate_name,
            a.reporting_month,
            a.analytics_status,
            a.parser_version,
            a.parsed_at,
            a.metrics_approved_at,
            a.source_file_count,
            a.parsed_record_count,
            a.error_count,
            a.warning_count
            {
                ", u.status AS source_status, u.approved_at"
                if upload_join
                else
                ", NULL AS source_status, NULL AS approved_at"
            }
        FROM batch_analytics a
        {upload_join}
        ORDER BY
            COALESCE(a.metrics_approved_at, a.parsed_at) DESC,
            a.estate_name,
            a.reporting_month DESC
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

    filtered = batches

    if estate_name:
        filtered = [
            item
            for item in filtered
            if str(item["estate_name"]).casefold()
            == estate_name.strip().casefold()
        ]

    if reporting_month:
        filtered = [
            item
            for item in filtered
            if str(item["reporting_month"])
            == reporting_month.strip()
        ]

    if not filtered:
        raise HTTPException(
            status_code=404,
            detail=(
                "No parsed production batch matches the "
                "selected estate and reporting month."
            ),
        )

    return filtered[0]


def get_divisions(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
) -> list[str]:
    divisions: set[str] = set()

    for table in (
        "g_report_metrics",
        "plucking_plan_fields",
        "crop_division_summary",
        "crop_field_performance",
        "agricultural_activity_records",
        "contract_records",
    ):
        if not table_exists(connection, table):
            continue

        rows = fetch_dicts(
            connection,
            f"""
            SELECT DISTINCT division_name
            FROM {table}
            WHERE batch_id = ?
              AND division_name IS NOT NULL
              AND TRIM(division_name) <> ''
            """,
            [batch_id],
        )

        for row in rows:
            divisions.add(str(row["division_name"]))

    return sorted(divisions)


def infer_division(
    question: str,
    divisions: list[str],
    selected_division: str | None,
    estate_name: str | None = None,
) -> str | None:
    # The dropdown selection always has priority.
    if selected_division:
        for division in divisions:
            if division.casefold() == selected_division.strip().casefold():
                return division

    normalised = re.sub(
        r"[^a-z0-9]+",
        " ",
        question.lower(),
    ).strip()

    estate_token = re.sub(
        r"[^a-z0-9]+",
        " ",
        (estate_name or "").lower(),
    ).strip()

    for division in divisions:
        token = re.sub(
            r"[^a-z0-9]+",
            " ",
            division.lower(),
        ).strip()

        if not token:
            continue

        same_as_estate = bool(
            estate_token
            and token == estate_token
        )

        if same_as_estate:
            explicit_patterns = [
                rf"\b{re.escape(token)}\s+division\b",
                rf"\bdivision\s+{re.escape(token)}\b",
            ]

            if any(
                re.search(pattern, normalised)
                for pattern in explicit_patterns
            ):
                return division

            continue

        if re.search(
            rf"\b{re.escape(token)}\b",
            normalised,
        ):
            return division

    return None


def status_mode(analytics_status: str) -> tuple[str, str | None]:
    status = analytics_status.upper()

    approved = (
        "METRIC" in status
        and "APPROVED" in status
        and "PENDING" not in status
    )

    if approved:
        return "APPROVED", None

    return (
        "PREVIEW",
        (
            "Preview data only. Parsed metrics have not received "
            "final management approval."
        ),
    )


def format_number(
    value: Any,
    decimals: int = 2,
) -> str:
    if value is None:
        return "not available"

    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)

    if decimals == 0:
        return f"{number:,.0f}"

    return f"{number:,.{decimals}f}"


def title_case(value: str) -> str:
    return value.replace("_", " ").title()


def preview_prefix(
    data_mode: str,
) -> str:
    if data_mode == "PREVIEW":
        return "[PREVIEW - NOT FINAL APPROVED] "

    return ""


def with_filters(
    base_sql: str,
    batch_id: str,
    division: str | None = None,
) -> tuple[str, list[Any]]:
    conditions = ["batch_id = ?"]
    parameters: list[Any] = [batch_id]

    if division:
        conditions.append("division_name = ?")
        parameters.append(division)

    return (
        base_sql.replace(
            "/*WHERE*/",
            "WHERE " + " AND ".join(conditions),
        ),
        parameters,
    )


def table_rows(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    batch_id: str,
    division: str | None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    if table_name not in TABLE_CATALOG:
        return []

    if not table_exists(connection, table_name):
        return []

    columns = {
        row[0]
        for row in connection.execute(
            f"DESCRIBE {table_name}"
        ).fetchall()
    }

    conditions = ["batch_id = ?"]
    parameters: list[Any] = [batch_id]

    if division and "division_name" in columns:
        conditions.append("division_name = ?")
        parameters.append(division)

    order_candidates = [
        "division_name",
        "field_no",
        "line_number",
        "invoice_date",
        "metric_period",
        "activity_type",
        "work_type",
        "employee_name",
        "sheet_name",
        "file_name",
    ]

    order_by = next(
        (
            column
            for column in order_candidates
            if column in columns
        ),
        None,
    )

    sql = (
        f"SELECT * FROM {table_name} "
        f"WHERE {' AND '.join(conditions)}"
    )

    if order_by:
        sql += f" ORDER BY {order_by}"

    sql += " LIMIT ?"
    parameters.append(min(limit, MAX_SUPPORTING_ROWS))

    return fetch_dicts(
        connection,
        sql,
        parameters,
    )


def query_company_summary(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    division: str | None,
    data_mode: str,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    batch_id = batch["batch_id"]

    g_rows: list[dict[str, Any]] = []
    p_rows: list[dict[str, Any]] = []
    c_rows: list[dict[str, Any]] = []
    plucker_rows: list[dict[str, Any]] = []
    cop_rows: list[dict[str, Any]] = []
    be_rows: list[dict[str, Any]] = []

    if table_exists(connection, "g_report_metrics"):
        sql, params = with_filters(
            """
            SELECT
                ROUND(SUM(total_green_leaf_kg), 2)
                    AS g_report_green_leaf_kg,
                ROUND(SUM(total_checkroll_names), 2)
                    AS checkroll_measure,
                ROUND(AVG(average_plucking_kg), 2)
                    AS average_division_plucking_kg,
                ROUND(SUM(total_hectare_plucked), 2)
                    AS total_hectare_plucked
            FROM g_report_metrics
            /*WHERE*/
            """,
            batch_id,
            division,
        )
        g_rows = fetch_dicts(connection, sql, params)

    if table_exists(connection, "plucking_plan_fields"):
        sql, params = with_filters(
            """
            SELECT
                COUNT(*) AS field_count,
                ROUND(SUM(anticipated_green_leaf_kg), 2)
                    AS anticipated_green_leaf_kg,
                ROUND(SUM(actual_green_leaf_kg), 2)
                    AS actual_green_leaf_kg,
                ROUND(SUM(actual_total_pluckers), 2)
                    AS actual_total_plucker_attendance,
                ROUND(AVG(actual_plucking_average), 2)
                    AS average_actual_plucking_kg
            FROM plucking_plan_fields
            /*WHERE*/
            """,
            batch_id,
            division,
        )
        p_rows = fetch_dicts(connection, sql, params)

    if table_exists(connection, "crop_division_summary"):
        sql, params = with_filters(
            """
            SELECT
                ROUND(SUM(extent_ha), 2) AS extent_ha,
                ROUND(SUM(made_tea_crop_kg), 2)
                    AS made_tea_crop_kg,
                ROUND(
                    SUM(made_tea_crop_kg)
                    / NULLIF(SUM(extent_ha), 0),
                    2
                ) AS weighted_yield_kg_per_ha
            FROM crop_division_summary
            /*WHERE*/
            """,
            batch_id,
            division,
        )
        c_rows = fetch_dicts(connection, sql, params)

    if table_exists(connection, "plucker_performance"):
        sql, params = with_filters(
            """
            SELECT
                COUNT(*) AS plucker_record_count,
                COUNT(DISTINCT COALESCE(
                    NULLIF(employee_code, ''),
                    employee_name
                )) AS distinct_pluckers,
                ROUND(SUM(green_leaf_kg), 2)
                    AS plucker_green_leaf_kg,
                ROUND(AVG(average_kg), 2)
                    AS average_plucker_kg
            FROM plucker_performance
            /*WHERE*/
            """,
            batch_id,
            division,
        )
        plucker_rows = fetch_dicts(connection, sql, params)

    if table_exists(connection, "cop_line_items"):
        cop_rows = fetch_dicts(
            connection,
            """
            SELECT
                description,
                rupees_per_kg,
                cost_per_hectare
            FROM cop_line_items
            WHERE batch_id = ?
              AND UPPER(description) LIKE '%TOTAL%C.O.P%'
            ORDER BY line_number DESC
            LIMIT 1
            """,
            [batch_id],
        )

    if table_exists(connection, "break_even_metrics"):
        be_rows = fetch_dicts(
            connection,
            """
            SELECT
                metric_period,
                break_even_quantity_kg,
                budget_crop_kg,
                budget_cop_rs_per_kg,
                tea_profit_rs,
                break_even_pct_of_budget
            FROM break_even_metrics
            WHERE batch_id = ?
            ORDER BY metric_period
            """,
            [batch_id],
        )

    g = g_rows[0] if g_rows else {}
    p = p_rows[0] if p_rows else {}
    c = c_rows[0] if c_rows else {}
    pl = plucker_rows[0] if plucker_rows else {}
    cop = cop_rows[0] if cop_rows else {}

    g_leaf = g.get("g_report_green_leaf_kg")
    plan_leaf = p.get("actual_green_leaf_kg")

    difference = None
    difference_pct = None

    if g_leaf is not None and plan_leaf is not None:
        difference = float(plan_leaf) - float(g_leaf)

        if float(g_leaf) != 0:
            difference_pct = (
                difference
                / float(g_leaf)
                * 100
            )

    scope_text = (
        f"{division} Division"
        if division
        else str(batch["estate_name"])
    )

    answer = (
        preview_prefix(data_mode)
        + f"{scope_text} summary for "
        + f"{batch['reporting_month']}: "
        + f"G Report green leaf was "
        + f"{format_number(g_leaf, 0)} kg; "
        + f"Plucking Plan actual green leaf was "
        + f"{format_number(plan_leaf, 0)} kg; "
        + f"made-tea crop was "
        + f"{format_number(c.get('made_tea_crop_kg'), 0)} kg; "
        + f"weighted made-tea yield was "
        + f"{format_number(c.get('weighted_yield_kg_per_ha'), 2)} "
        + "kg/ha; "
        + f"the plucker file contains "
        + f"{format_number(pl.get('distinct_pluckers'), 0)} "
        + "distinct pluckers; and total COP was "
        + f"Rs. {format_number(cop.get('rupees_per_kg'), 2)} "
        + "per kg."
    )

    if difference is not None:
        answer += (
            " The Plucking Plan green-leaf total differs "
            "from the G Report by "
            f"{format_number(difference, 0)} kg"
        )

        if difference_pct is not None:
            answer += (
                f" ({format_number(difference_pct, 2)}%)."
            )
        else:
            answer += "."

    if be_rows:
        period_text = "; ".join(
            (
                f"{row.get('metric_period')}: break-even "
                f"{format_number(row.get('break_even_quantity_kg'), 0)} kg, "
                f"tea profit Rs. "
                f"{format_number(row.get('tea_profit_rs'), 2)}"
            )
            for row in be_rows
        )

        answer += " Break-even records: " + period_text + "."

    supporting = [
        {
            "metric": "G Report green leaf kg",
            "value": g_leaf,
        },
        {
            "metric": "Plucking Plan actual green leaf kg",
            "value": plan_leaf,
        },
        {
            "metric": "Green-leaf difference kg",
            "value": difference,
        },
        {
            "metric": "Made-tea crop kg",
            "value": c.get("made_tea_crop_kg"),
        },
        {
            "metric": "Weighted yield kg/ha",
            "value": c.get("weighted_yield_kg_per_ha"),
        },
        {
            "metric": "Distinct pluckers",
            "value": pl.get("distinct_pluckers"),
        },
        {
            "metric": "Total COP Rs/kg",
            "value": cop.get("rupees_per_kg"),
        },
    ]

    supporting.extend(
        {
            "metric": (
                f"{row.get('metric_period')} "
                "break-even quantity kg"
            ),
            "value": row.get("break_even_quantity_kg"),
        }
        for row in be_rows
    )

    return (
        answer,
        [
            "g_report_metrics",
            "plucking_plan_fields",
            "crop_division_summary",
            "plucker_performance",
            "cop_line_items",
            "break_even_metrics",
        ],
        supporting,
    )


def query_division_comparison(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    data_mode: str,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    batch_id = batch["batch_id"]

    rows = fetch_dicts(
        connection,
        """
        WITH g AS (
            SELECT
                division_name,
                SUM(total_green_leaf_kg)
                    AS g_report_green_leaf_kg,
                AVG(average_plucking_kg)
                    AS average_plucking_kg
            FROM g_report_metrics
            WHERE batch_id = ?
            GROUP BY division_name
        ),
        p AS (
            SELECT
                division_name,
                SUM(actual_green_leaf_kg)
                    AS plucking_plan_actual_green_leaf_kg,
                SUM(actual_total_pluckers)
                    AS actual_total_plucker_attendance
            FROM plucking_plan_fields
            WHERE batch_id = ?
            GROUP BY division_name
        ),
        c AS (
            SELECT
                division_name,
                SUM(extent_ha) AS extent_ha,
                SUM(made_tea_crop_kg)
                    AS made_tea_crop_kg,
                SUM(made_tea_crop_kg)
                    / NULLIF(SUM(extent_ha), 0)
                    AS yield_kg_per_ha
            FROM crop_division_summary
            WHERE batch_id = ?
            GROUP BY division_name
        )
        SELECT
            COALESCE(g.division_name, p.division_name, c.division_name)
                AS division_name,
            ROUND(g.g_report_green_leaf_kg, 2)
                AS g_report_green_leaf_kg,
            ROUND(p.plucking_plan_actual_green_leaf_kg, 2)
                AS plucking_plan_actual_green_leaf_kg,
            ROUND(
                p.plucking_plan_actual_green_leaf_kg
                - g.g_report_green_leaf_kg,
                2
            ) AS green_leaf_difference_kg,
            ROUND(g.average_plucking_kg, 2)
                AS average_plucking_kg,
            ROUND(c.extent_ha, 2)
                AS extent_ha,
            ROUND(c.made_tea_crop_kg, 2)
                AS made_tea_crop_kg,
            ROUND(c.yield_kg_per_ha, 2)
                AS yield_kg_per_ha
        FROM g
        FULL OUTER JOIN p
          ON p.division_name = g.division_name
        FULL OUTER JOIN c
          ON c.division_name = COALESCE(
              g.division_name,
              p.division_name
          )
        ORDER BY division_name
        """,
        [batch_id, batch_id, batch_id],
    )

    if not rows:
        return (
            preview_prefix(data_mode)
            + "No division-comparison data is available.",
            [
                "g_report_metrics",
                "plucking_plan_fields",
                "crop_division_summary",
            ],
            [],
        )

    highest_green = max(
        rows,
        key=lambda row: row.get("g_report_green_leaf_kg")
        or float("-inf"),
    )
    highest_crop = max(
        rows,
        key=lambda row: row.get("made_tea_crop_kg")
        or float("-inf"),
    )
    highest_yield = max(
        rows,
        key=lambda row: row.get("yield_kg_per_ha")
        or float("-inf"),
    )

    answer = (
        preview_prefix(data_mode)
        + f"Division comparison for {batch['estate_name']} "
        + f"in {batch['reporting_month']}: "
        + f"{highest_green['division_name']} had the highest "
        + "G Report green leaf at "
        + f"{format_number(highest_green['g_report_green_leaf_kg'], 0)} kg; "
        + f"{highest_crop['division_name']} had the highest "
        + "made-tea crop at "
        + f"{format_number(highest_crop['made_tea_crop_kg'], 0)} kg; "
        + f"and {highest_yield['division_name']} had the highest "
        + "made-tea yield at "
        + f"{format_number(highest_yield['yield_kg_per_ha'], 2)} kg/ha."
    )

    return (
        answer,
        [
            "g_report_metrics",
            "plucking_plan_fields",
            "crop_division_summary",
        ],
        rows,
    )


def query_plucking_plan(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    division: str | None,
    data_mode: str,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    sql, params = with_filters(
        """
        SELECT
            division_name,
            field_no,
            extent_ha,
            anticipated_rounds,
            anticipated_total_pluckers,
            anticipated_green_leaf_kg,
            actual_rounds,
            actual_lph,
            actual_plucking_average,
            actual_total_pluckers,
            actual_green_leaf_kg,
            ROUND(
                actual_green_leaf_kg
                - anticipated_green_leaf_kg,
                2
            ) AS variance_green_leaf_kg
        FROM plucking_plan_fields
        /*WHERE*/
        ORDER BY
            division_name,
            field_no
        LIMIT 100
        """,
        batch["batch_id"],
        division,
    )

    rows = fetch_dicts(connection, sql, params)

    totals = fetch_dicts(
        connection,
        (
            """
            SELECT
                COUNT(*) AS field_count,
                SUM(anticipated_green_leaf_kg)
                    AS anticipated_green_leaf_kg,
                SUM(actual_green_leaf_kg)
                    AS actual_green_leaf_kg,
                SUM(actual_green_leaf_kg)
                    - SUM(anticipated_green_leaf_kg)
                    AS variance_green_leaf_kg,
                SUM(actual_total_pluckers)
                    AS actual_total_plucker_attendance
            FROM plucking_plan_fields
            """
            + (
                " WHERE batch_id = ? AND division_name = ?"
                if division
                else
                " WHERE batch_id = ?"
            )
        ),
        (
            [batch["batch_id"], division]
            if division
            else
            [batch["batch_id"]]
        ),
    )[0]

    scope_text = division or batch["estate_name"]

    answer = (
        preview_prefix(data_mode)
        + f"{scope_text} Plucking Plan for "
        + f"{batch['reporting_month']} contains "
        + f"{format_number(totals.get('field_count'), 0)} fields. "
        + "Anticipated green leaf was "
        + f"{format_number(totals.get('anticipated_green_leaf_kg'), 0)} kg, "
        + "actual green leaf was "
        + f"{format_number(totals.get('actual_green_leaf_kg'), 0)} kg, "
        + "and the variance was "
        + f"{format_number(totals.get('variance_green_leaf_kg'), 0)} kg."
    )

    return answer, ["plucking_plan_fields"], rows


def query_crop(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    division: str | None,
    question: str,
    data_mode: str,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    field_request = bool(
        re.search(
            r"\b(field|fields|lowest|highest|top|bottom)\b",
            question,
            flags=re.IGNORECASE,
        )
    )

    if field_request:
        sql, params = with_filters(
            """
            SELECT
                division_name,
                field_no,
                vp_sd,
                extent_ha,
                made_tea_crop_kg,
                ROUND(yield_kg_per_ha, 2)
                    AS yield_kg_per_ha
            FROM crop_field_performance
            /*WHERE*/
            ORDER BY yield_kg_per_ha DESC NULLS LAST
            LIMIT 100
            """,
            batch["batch_id"],
            division,
        )

        rows = fetch_dicts(connection, sql, params)

        valid = [
            row
            for row in rows
            if row.get("yield_kg_per_ha") is not None
        ]

        if not valid:
            return (
                preview_prefix(data_mode)
                + "No field crop values are available.",
                ["crop_field_performance"],
                rows,
            )

        highest = max(
            valid,
            key=lambda row: row["yield_kg_per_ha"],
        )
        lowest = min(
            valid,
            key=lambda row: row["yield_kg_per_ha"],
        )

        answer = (
            preview_prefix(data_mode)
            + f"Field crop performance for "
            + f"{division or batch['estate_name']} in "
            + f"{batch['reporting_month']}: "
            + f"the highest recorded yield was field "
            + f"{highest.get('field_no')} in "
            + f"{highest.get('division_name')} at "
            + f"{format_number(highest.get('yield_kg_per_ha'), 2)} kg/ha; "
            + f"the lowest recorded yield was field "
            + f"{lowest.get('field_no')} in "
            + f"{lowest.get('division_name')} at "
            + f"{format_number(lowest.get('yield_kg_per_ha'), 2)} kg/ha."
        )

        return answer, ["crop_field_performance"], rows

    sql, params = with_filters(
        """
        SELECT
            division_name,
            extent_ha,
            made_tea_crop_kg,
            ROUND(yield_kg_per_ha, 2)
                AS yield_kg_per_ha
        FROM crop_division_summary
        /*WHERE*/
        ORDER BY division_name
        """,
        batch["batch_id"],
        division,
    )

    rows = fetch_dicts(connection, sql, params)

    total_crop = sum(
        float(row.get("made_tea_crop_kg") or 0)
        for row in rows
    )
    total_extent = sum(
        float(row.get("extent_ha") or 0)
        for row in rows
    )
    weighted_yield = (
        total_crop / total_extent
        if total_extent
        else None
    )

    answer = (
        preview_prefix(data_mode)
        + f"Made-tea crop for "
        + f"{division or batch['estate_name']} in "
        + f"{batch['reporting_month']} was "
        + f"{format_number(total_crop, 0)} kg from "
        + f"{format_number(total_extent, 2)} ha, "
        + "giving a weighted yield of "
        + f"{format_number(weighted_yield, 2)} kg/ha."
    )

    return answer, ["crop_division_summary"], rows


def query_cop(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    data_mode: str,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    rows = fetch_dicts(
        connection,
        """
        SELECT
            line_number,
            description,
            ROUND(rupees_per_kg, 2)
                AS rupees_per_kg,
            ROUND(cost_share * 100, 2)
                AS cost_share_pct,
            ROUND(cost_per_hectare, 2)
                AS cost_per_hectare
        FROM cop_line_items
        WHERE batch_id = ?
        ORDER BY line_number
        LIMIT 100
        """,
        [batch["batch_id"]],
    )

    total = next(
        (
            row
            for row in reversed(rows)
            if "TOTAL" in str(row.get("description", "")).upper()
            and "C.O.P" in str(row.get("description", "")).upper()
        ),
        None,
    )

    if total:
        answer = (
            preview_prefix(data_mode)
            + f"Total COP for {batch['estate_name']} in "
            + f"{batch['reporting_month']} was "
            + f"Rs. {format_number(total.get('rupees_per_kg'), 2)} "
            + "per kg, with cost per hectare of "
            + f"Rs. {format_number(total.get('cost_per_hectare'), 2)}."
        )
    else:
        answer = (
            preview_prefix(data_mode)
            + f"{len(rows)} COP line items are available for "
            + f"{batch['estate_name']} in {batch['reporting_month']}."
        )

    return answer, ["cop_line_items"], rows


def query_break_even(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    data_mode: str,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    rows = fetch_dicts(
        connection,
        """
        SELECT
            metric_period,
            ROUND(total_fixed_cost_rs, 2)
                AS total_fixed_cost_rs,
            ROUND(variable_cost_rs_per_kg, 2)
                AS variable_cost_rs_per_kg,
            ROUND(contribution_rs_per_kg, 2)
                AS contribution_rs_per_kg,
            ROUND(break_even_quantity_kg, 2)
                AS break_even_quantity_kg,
            ROUND(budget_crop_kg, 2)
                AS budget_crop_kg,
            ROUND(budget_cop_rs_per_kg, 2)
                AS budget_cop_rs_per_kg,
            ROUND(tea_profit_rs, 2)
                AS tea_profit_rs,
            ROUND(break_even_pct_of_budget * 100, 2)
                AS break_even_pct_of_budget
        FROM break_even_metrics
        WHERE batch_id = ?
        ORDER BY metric_period
        """,
        [batch["batch_id"]],
    )

    parts = []

    for row in rows:
        parts.append(
            f"{row.get('metric_period')}: break-even quantity "
            f"{format_number(row.get('break_even_quantity_kg'), 0)} kg, "
            f"break-even at "
            f"{format_number(row.get('break_even_pct_of_budget'), 2)}% "
            f"of budget crop, tea profit Rs. "
            f"{format_number(row.get('tea_profit_rs'), 2)}"
        )

    answer = (
        preview_prefix(data_mode)
        + f"Break-even position for {batch['estate_name']} "
        + f"in {batch['reporting_month']}: "
        + ("; ".join(parts) if parts else "no values found")
        + "."
    )

    return answer, ["break_even_metrics"], rows


def query_contracts(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    division: str | None,
    data_mode: str,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    sql, params = with_filters(
        """
        SELECT
            division_name,
            work_type,
            contractor_name,
            field_no,
            labour_allocated,
            labour_total_cost_rs,
            converted_lph,
            hectares_awarded,
            contract_value,
            field_officer,
            assistant_manager,
            manager,
            last_date
        FROM contract_records
        /*WHERE*/
        ORDER BY
            division_name,
            work_type,
            contractor_name
        LIMIT 100
        """,
        batch["batch_id"],
        division,
    )

    rows = fetch_dicts(connection, sql, params)

    total_cost = sum(
        float(row.get("labour_total_cost_rs") or 0)
        for row in rows
    )
    contract_value = sum(
        float(row.get("contract_value") or 0)
        for row in rows
    )

    work_counts: dict[str, int] = {}

    for row in rows:
        work_type = str(row.get("work_type") or "Unspecified")
        work_counts[work_type] = work_counts.get(work_type, 0) + 1

    top_work = (
        max(work_counts.items(), key=lambda item: item[1])
        if work_counts
        else ("not available", 0)
    )

    answer = (
        preview_prefix(data_mode)
        + f"{division or batch['estate_name']} has "
        + f"{format_number(len(rows), 0)} displayed contract records "
        + f"for {batch['reporting_month']}. "
        + f"Displayed labour cost totals Rs. "
        + f"{format_number(total_cost, 2)}, displayed contract value "
        + f"totals Rs. {format_number(contract_value, 2)}, and the "
        + f"most frequent displayed work type is "
        + f"{top_work[0]} ({top_work[1]} records)."
    )

    return answer, ["contract_records"], rows


def query_agricultural_activities(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    division: str | None,
    question: str,
    data_mode: str,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    activity_filter = None
    lower = question.lower()

    if "fertili" in lower:
        activity_filter = "FERTIL"
    elif "weed" in lower:
        activity_filter = "WEED"
    elif "spray" in lower:
        activity_filter = "SPRAY"

    conditions = ["batch_id = ?"]
    parameters: list[Any] = [batch["batch_id"]]

    if division:
        conditions.append("division_name = ?")
        parameters.append(division)

    if activity_filter:
        conditions.append("UPPER(activity_type) LIKE ?")
        parameters.append(f"%{activity_filter}%")

    rows = fetch_dicts(
        connection,
        f"""
        SELECT
            division_name,
            field_no,
            extent_ha,
            vp_sd,
            field_age,
            budget_yph,
            actual_yph,
            activity_type,
            activity_month,
            activity_value_text,
            activity_value_number
        FROM agricultural_activity_records
        WHERE {' AND '.join(conditions)}
        ORDER BY
            division_name,
            field_no,
            activity_type
        LIMIT 100
        """,
        parameters,
    )

    type_counts: dict[str, int] = {}

    for row in rows:
        activity = str(row.get("activity_type") or "Unspecified")
        type_counts[activity] = type_counts.get(activity, 0) + 1

    summary = ", ".join(
        f"{name}: {count}"
        for name, count in sorted(type_counts.items())
    )

    answer = (
        preview_prefix(data_mode)
        + f"{division or batch['estate_name']} has "
        + f"{format_number(len(rows), 0)} displayed agricultural "
        + f"activity records for {batch['reporting_month']}"
        + (f" matching {activity_filter.lower()}" if activity_filter else "")
        + ". "
        + (f"Activity counts: {summary}." if summary else "No records found.")
    )

    return answer, ["agricultural_activity_records"], rows


def query_pluckers(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    division: str | None,
    question: str,
    data_mode: str,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    order = "green_leaf_kg DESC NULLS LAST"

    if re.search(
        r"\b(lowest|bottom|least)\b",
        question,
        flags=re.IGNORECASE,
    ):
        order = "green_leaf_kg ASC NULLS LAST"
    elif "attendance" in question.lower():
        order = "days_attended DESC NULLS LAST"
    elif "average" in question.lower():
        order = "average_kg DESC NULLS LAST"

    conditions = ["batch_id = ?"]
    parameters: list[Any] = [batch["batch_id"]]

    if division:
        conditions.append("division_name = ?")
        parameters.append(division)

    rows = fetch_dicts(
        connection,
        f"""
        SELECT
            division_name,
            employee_name,
            employee_code,
            days_offered,
            days_attended,
            green_leaf_kg,
            average_kg
        FROM plucker_performance
        WHERE {' AND '.join(conditions)}
        ORDER BY {order}
        LIMIT 100
        """,
        parameters,
    )

    top = rows[0] if rows else None

    if top:
        answer = (
            preview_prefix(data_mode)
            + f"Plucker results for "
            + f"{division or batch['estate_name']} in "
            + f"{batch['reporting_month']} include "
            + f"{format_number(len(rows), 0)} displayed records. "
            + f"The first ranked record is "
            + f"{top.get('employee_name') or top.get('employee_code')} "
            + f"from {top.get('division_name')}, with "
            + f"{format_number(top.get('green_leaf_kg'), 2)} kg "
            + "green leaf and average "
            + f"{format_number(top.get('average_kg'), 2)} kg."
        )
    else:
        answer = (
            preview_prefix(data_mode)
            + "No plucker-performance records were found."
        )

    return answer, ["plucker_performance"], rows


def query_revenue(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    question: str,
    data_mode: str,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    if re.search(
        r"\b(invoice|despatch|dispatch)\b",
        question,
        flags=re.IGNORECASE,
    ):
        rows = fetch_dicts(
            connection,
            """
            SELECT
                invoice_date,
                invoice_no,
                grade,
                sold_quantity_kg,
                net_proceeds_rs,
                despatch_document_no,
                despatch_date
            FROM invoice_sales
            WHERE batch_id = ?
            ORDER BY invoice_date, invoice_no
            LIMIT 100
            """,
            [batch["batch_id"]],
        )

        quantity = sum(
            float(row.get("sold_quantity_kg") or 0)
            for row in rows
        )
        proceeds = sum(
            float(row.get("net_proceeds_rs") or 0)
            for row in rows
        )

        answer = (
            preview_prefix(data_mode)
            + f"The first {len(rows)} displayed invoice records for "
            + f"{batch['estate_name']} in {batch['reporting_month']} "
            + f"total {format_number(quantity, 2)} kg and "
            + f"Rs. {format_number(proceeds, 2)} in net proceeds."
        )

        return answer, ["invoice_sales"], rows

    rows = fetch_dicts(
        connection,
        """
        SELECT
            broker,
            invoice_no,
            grade,
            quantity_kg,
            month_manufactured,
            month_sold,
            price_rs_per_kg,
            total_proceeds_rs,
            buyer
        FROM revenue_grade_sales
        WHERE batch_id = ?
        ORDER BY
            total_proceeds_rs DESC NULLS LAST,
            quantity_kg DESC NULLS LAST
        LIMIT 100
        """,
        [batch["batch_id"]],
    )

    quantity = sum(
        float(row.get("quantity_kg") or 0)
        for row in rows
    )
    proceeds = sum(
        float(row.get("total_proceeds_rs") or 0)
        for row in rows
    )

    top = rows[0] if rows else None

    answer = (
        preview_prefix(data_mode)
        + f"The first {len(rows)} displayed grade-sale records for "
        + f"{batch['estate_name']} in {batch['reporting_month']} "
        + f"total {format_number(quantity, 2)} kg and "
        + f"Rs. {format_number(proceeds, 2)} in proceeds."
    )

    if top:
        answer += (
            " The highest displayed proceeds row is grade "
            f"{top.get('grade')} sold to "
            f"{top.get('buyer') or 'buyer not recorded'} at "
            f"Rs. {format_number(top.get('price_rs_per_kg'), 2)} "
            "per kg."
        )

    return answer, ["revenue_grade_sales"], rows


def query_m_format(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    division: str | None,
    data_mode: str,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    conditions = ["batch_id = ?"]
    parameters: list[Any] = [batch["batch_id"]]

    if division:
        conditions.append("division_name = ?")
        parameters.append(division)

    rows = fetch_dicts(
        connection,
        f"""
        SELECT
            division_name,
            field_no,
            extent_ha,
            metric_year,
            metric_month,
            yph,
            nitrogen
        FROM m_format_monthly_yph
        WHERE {' AND '.join(conditions)}
        ORDER BY
            division_name,
            field_no,
            metric_year,
            metric_month
        LIMIT 100
        """,
        parameters,
    )

    yph_values = [
        float(row["yph"])
        for row in rows
        if row.get("yph") is not None
    ]
    nitrogen_values = [
        float(row["nitrogen"])
        for row in rows
        if row.get("nitrogen") is not None
    ]

    avg_yph = (
        sum(yph_values) / len(yph_values)
        if yph_values
        else None
    )
    avg_nitrogen = (
        sum(nitrogen_values) / len(nitrogen_values)
        if nitrogen_values
        else None
    )

    answer = (
        preview_prefix(data_mode)
        + f"The first {len(rows)} displayed M Format monthly records "
        + f"for {division or batch['estate_name']} have average YPH "
        + f"{format_number(avg_yph, 2)} and average nitrogen "
        + f"{format_number(avg_nitrogen, 2)} across non-null values."
    )

    return answer, ["m_format_monthly_yph"], rows


def query_source_information(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    data_mode: str,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    source_tables: list[str] = []

    if table_exists(connection, "uploaded_files"):
        rows = fetch_dicts(
            connection,
            """
            SELECT
                archive_name,
                relative_path,
                file_name,
                extension,
                size_bytes,
                category,
                division_name,
                validation_status,
                validation_message
            FROM uploaded_files
            WHERE batch_id = ?
            ORDER BY category, division_name, file_name
            LIMIT 100
            """,
            [batch["batch_id"]],
        )
        source_tables.append("uploaded_files")

    if not rows and table_exists(connection, "source_sheet_inventory"):
        rows = fetch_dicts(
            connection,
            """
            SELECT
                category,
                file_name,
                sheet_name,
                non_empty_row_count,
                non_empty_cell_count,
                max_row,
                max_column
            FROM source_sheet_inventory
            WHERE batch_id = ?
            ORDER BY category, file_name, sheet_name
            LIMIT 100
            """,
            [batch["batch_id"]],
        )
        source_tables.append("source_sheet_inventory")

    categories = sorted(
        {
            str(row.get("category"))
            for row in rows
            if row.get("category")
        }
    )

    answer = (
        preview_prefix(data_mode)
        + f"{batch['estate_name']} {batch['reporting_month']} has "
        + f"{len(rows)} displayed source-file or sheet records"
        + (
            ". Categories: " + ", ".join(categories) + "."
            if categories
            else "."
        )
    )

    return answer, source_tables, rows


def query_data_catalog(
    connection: duckdb.DuckDBPyConnection,
    batch: dict[str, Any],
    data_mode: str,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    rows = []

    for table_name, item in TABLE_CATALOG.items():
        if not table_exists(connection, table_name):
            continue

        count = None

        try:
            count = connection.execute(
                f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE batch_id = ?
                """,
                [batch["batch_id"]],
            ).fetchone()[0]
        except Exception:
            count = connection.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]

        rows.append(
            {
                "table_name": table_name,
                "title": item["title"],
                "description": item["description"],
                "batch_row_count": count,
            }
        )

    answer = (
        preview_prefix(data_mode)
        + f"{len(rows)} dataset categories are available for "
        + f"{batch['estate_name']} in {batch['reporting_month']}. "
        + "They include G Reports, plucking plans, non-wage "
        + "plucking, agricultural activities, contracts, field "
        + "and division crop, plucker performance, COP, revenue, "
        + "invoice sales, break-even and M Format monthly YPH."
    )

    return (
        answer,
        [row["table_name"] for row in rows],
        rows,
    )


def choose_table(question: str) -> str | None:
    lower = question.lower()
    scores: list[tuple[int, str]] = []

    for table_name, item in TABLE_CATALOG.items():
        score = 0

        for keyword in item["keywords"]:
            if keyword in lower:
                score += max(1, len(keyword.split()))

        if table_name.replace("_", " ") in lower:
            score += 4

        if score:
            scores.append((score, table_name))

    if not scores:
        return None

    scores.sort(reverse=True)
    return scores[0][1]


def clean_model_answer(answer: str) -> str:
    text = answer.strip()
    lower = text.lower()

    if "</think>" in lower:
        position = lower.rfind("</think>")
        text = text[position + len("</think>"):].strip()

    text = re.sub(
        r"<think>.*?</think>",
        "",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    ).strip()

    replacements = {
        "\u2013": "-",
        "\u2014": "-",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u00a0": " ",
    }

    for source, replacement in replacements.items():
        text = text.replace(source, replacement)

    return text.strip()


def contains_planning(answer: str) -> bool:
    patterns = [
        r"\bwe are given\b",
        r"\brules to follow\b",
        r"\bkey points\b",
        r"\bthe user asks\b",
        r"\blet me\b",
        r"\bfrom the context\b",
        r"\bimportant:\s*rule\b",
        r"\bsteps\b.*\banswer\b",
    ]

    return any(
        re.search(
            pattern,
            answer,
            flags=re.IGNORECASE,
        )
        for pattern in patterns
    )


def call_ollama(
    question: str,
    batch: dict[str, Any],
    division: str | None,
    data_mode: str,
    table_name: str,
    rows: list[dict[str, Any]],
) -> str | None:
    context = {
        "estate_name": batch["estate_name"],
        "reporting_month": batch["reporting_month"],
        "division_name": division,
        "data_mode": data_mode,
        "source_table": table_name,
        "table_description": TABLE_CATALOG[table_name]["description"],
        "rows": rows[:30],
    }

    prompt = """
You are the internal TTE production-data assistant.

Answer only from the supplied verified database rows.
Return only the final user-facing answer.
Do not show analysis, reasoning, rules, planning or internal context.
Do not invent values.
Clearly state when the rows do not contain the answer.
Use the correct units from the column names.
Keep the answer concise but complete.
If data_mode is PREVIEW, begin with:
[PREVIEW - NOT FINAL APPROVED]
""".strip()

    user_message = (
        json.dumps(
            context,
            default=str,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + "\n\nQuestion: "
        + question
        + "\nReturn only the answer. /no_think"
    )

    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {
                "role": "system",
                "content": prompt,
            },
            {
                "role": "user",
                "content": user_message,
            },
        ],
        "stream": False,
        "think": False,
        "options": {
            "temperature": 0.0,
            "num_predict": 350,
            "num_ctx": 8192,
        },
    }

    request = urlrequest.Request(
        f"{OLLAMA_BASE_URL}/api/chat",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlrequest.urlopen(
            request,
            timeout=180,
        ) as response:
            result = json.loads(
                response.read().decode("utf-8")
            )
    except (urlerror.URLError, TimeoutError):
        return None

    answer = clean_model_answer(
        result.get("message", {}).get("content", "")
    )

    if not answer or contains_planning(answer):
        return None

    if (
        data_mode == "PREVIEW"
        and not answer.startswith(
            "[PREVIEW - NOT FINAL APPROVED]"
        )
    ):
        answer = preview_prefix(data_mode) + answer

    return answer


def generic_fallback_answer(
    batch: dict[str, Any],
    division: str | None,
    data_mode: str,
    table_name: str,
    rows: list[dict[str, Any]],
) -> str:
    title = TABLE_CATALOG[table_name]["title"]

    if not rows:
        return (
            preview_prefix(data_mode)
            + f"No {title.lower()} rows were found for "
            + f"{division or batch['estate_name']} in "
            + f"{batch['reporting_month']}."
        )

    first = rows[0]
    compact = ", ".join(
        f"{title_case(key)}: {value}"
        for key, value in list(first.items())[:6]
    )

    return (
        preview_prefix(data_mode)
        + f"{len(rows)} {title.lower()} rows are available for "
        + f"{division or batch['estate_name']} in "
        + f"{batch['reporting_month']}. "
        + f"First displayed row - {compact}. "
        + "Open Supporting Data in the chatbot to review the rows."
    )



def get_governance_note(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    question: str,
) -> str | None:
    relevant = bool(
        re.search(
            r"\b(summary|management|overview|green leaf|g report|"
            r"plucking plan|reconciliation|difference|official)\b",
            question,
            flags=re.IGNORECASE,
        )
    )

    if not relevant:
        return None

    if not table_exists(connection, "data_quality_issues"):
        return None

    sql = (
        "SELECT estate_name, reporting_month, status, "
        "source_a_value, source_b_value, difference_value, "
        "difference_pct, official_source, official_value, "
        "resolved_by, resolution_note "
        "FROM data_quality_issues "
        "WHERE batch_id = ? "
        "AND issue_type = 'SOURCE_RECONCILIATION' "
        "AND metric_name = 'green_leaf_kg' "
        "ORDER BY CASE status WHEN 'OPEN' THEN 1 ELSE 2 END, "
        "detected_at DESC LIMIT 1"
    )

    rows = fetch_dicts(connection, sql, [batch_id])

    if not rows:
        return None

    issue = rows[0]

    if issue["status"] == "OPEN":
        return (
            "[MANAGEMENT ALERT - RECONCILIATION PENDING] "
            f"The G Report records "
            f"{format_number(issue['source_a_value'], 0)} kg "
            f"and the Plucking Plan records "
            f"{format_number(issue['source_b_value'], 0)} kg. "
            f"The difference is "
            f"{format_number(issue['difference_value'], 0)} kg "
            f"({format_number(issue['difference_pct'], 2)}%). "
            "No official green-leaf figure has been approved."
        )

    return (
        "[OFFICIAL METRIC APPROVED] "
        f"Official {issue['reporting_month']} green leaf is "
        f"{format_number(issue['official_value'], 0)} kg, "
        f"based on {issue['official_source']}. "
        f"The alternative source differs by "
        f"{format_number(issue['difference_value'], 0)} kg "
        f"({format_number(issue['difference_pct'], 2)}%). "
        f"Approved by {issue['resolved_by']}. "
        f"Reconciliation note: {issue['resolution_note']}"
    )


@router.get("/chat", include_in_schema=False)
def dataset_chat_page() -> FileResponse:
    if not CHAT_HTML.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Chat page not found: {CHAT_HTML}",
        )

    return FileResponse(CHAT_HTML)


@router.get("/api/dataset-chat/health")
def dataset_chat_health() -> dict[str, Any]:
    connection = open_db()

    try:
        batches = get_batches(connection)

        return {
            "status": "healthy",
            "production_database": str(PRODUCTION_DB),
            "available_batches": len(batches),
            "ollama_url": OLLAMA_BASE_URL,
            "ollama_model": OLLAMA_MODEL,
            "chat_page": "/chat",
        }
    finally:
        connection.close()


@router.get("/api/dataset-chat/options")
def dataset_chat_options() -> dict[str, Any]:
    connection = open_db()

    try:
        batches = get_batches(connection)
        output_batches = []

        for batch in batches:
            output_batches.append(
                {
                    **batch,
                    "divisions": get_divisions(
                        connection,
                        batch["batch_id"],
                    ),
                    "data_mode": status_mode(
                        batch["analytics_status"]
                    )[0],
                }
            )

        return {
            "batches": output_batches,
            "question_examples": [
                "Give me the Bearwell June 2026 management summary.",
                "Compare all divisions.",
                "What is the difference between G Report and Plucking Plan green leaf?",
                "Which division had the highest made-tea yield?",
                "Show the field crop performance.",
                "What was the total COP per kilogram?",
                "Explain the June break-even position.",
                "Show contract monitoring information.",
                "Show fertilizer, weeding and spraying activities.",
                "Who are the top pluckers by green leaf?",
                "Show revenue and grade sales.",
                "Show invoice sales and net proceeds.",
                "What data categories are available?",
                "Show all source files.",
            ],
            "available_categories": [
                {
                    "table_name": name,
                    "title": item["title"],
                    "description": item["description"],
                }
                for name, item in TABLE_CATALOG.items()
            ],
        }

    finally:
        connection.close()


@router.post(
    "/api/dataset-chat",
    response_model=DatasetChatResponse,
)
def dataset_chat(
    payload: DatasetChatRequest,
) -> DatasetChatResponse:
    question = payload.question.strip()
    connection = open_db()

    try:
        batch = resolve_batch(
            connection,
            payload.estate_name,
            payload.reporting_month,
        )

        divisions = get_divisions(
            connection,
            batch["batch_id"],
        )

        division = infer_division(
            question,
            divisions,
            payload.division_name,
            batch["estate_name"],
        )

        data_mode, warning = status_mode(
            batch["analytics_status"]
        )

        lower = question.lower()

        if re.search(
            r"\b(what data|data available|available data|categories|dataset contains|what can i ask)\b",
            lower,
        ):
            answer, tables, rows = query_data_catalog(
                connection,
                batch,
                data_mode,
            )

        elif re.search(
            r"\b(source file|uploaded file|file category|workbook|sheet inventory|show all files)\b",
            lower,
        ):
            answer, tables, rows = query_source_information(
                connection,
                batch,
                data_mode,
            )

        elif (
            re.search(
                r"\b(compare|comparison|versus|vs)\b",
                lower,
            )
            and not division
        ):
            answer, tables, rows = query_division_comparison(
                connection,
                batch,
                data_mode,
            )

        elif re.search(
            r"\b(break even|break-even|tea profit|fixed cost|contribution)\b",
            lower,
        ):
            answer, tables, rows = query_break_even(
                connection,
                batch,
                data_mode,
            )

        elif re.search(
            r"\b(cop|cost of production|cost per hectare|cost share)\b",
            lower,
        ):
            answer, tables, rows = query_cop(
                connection,
                batch,
                data_mode,
            )

        elif re.search(
            r"\b(contract|contractor|work type|labour cost|hectares awarded)\b",
            lower,
        ):
            answer, tables, rows = query_contracts(
                connection,
                batch,
                division,
                data_mode,
            )

        elif re.search(
            r"\b(fertilizer|fertiliser|weeding|spraying|agricultural activity)\b",
            lower,
        ):
            answer, tables, rows = query_agricultural_activities(
                connection,
                batch,
                division,
                question,
                data_mode,
            )

        elif re.search(
            r"\b(plucker|employee|attendance|worker performance|top worker)\b",
            lower,
        ):
            answer, tables, rows = query_pluckers(
                connection,
                batch,
                division,
                question,
                data_mode,
            )

        elif re.search(
            r"\b(revenue|grade sales|buyer|broker|invoice|despatch|dispatch|net proceeds)\b",
            lower,
        ):
            answer, tables, rows = query_revenue(
                connection,
                batch,
                question,
                data_mode,
            )

        elif re.search(
            r"\b(m format|monthly yph|nitrogen|yield history)\b",
            lower,
        ):
            answer, tables, rows = query_m_format(
                connection,
                batch,
                division,
                data_mode,
            )

        elif re.search(
            r"\b(plucking plan|anticipated|actual plucking|lph|pph)\b",
            lower,
        ):
            answer, tables, rows = query_plucking_plan(
                connection,
                batch,
                division,
                data_mode,
            )

        elif re.search(
            r"\b(made tea|crop|yield|field performance|highest field|lowest field)\b",
            lower,
        ):
            answer, tables, rows = query_crop(
                connection,
                batch,
                division,
                question,
                data_mode,
            )

        elif re.search(
            r"\b(summary|overview|performance|management|green leaf|g report|checkroll)\b",
            lower,
        ):
            answer, tables, rows = query_company_summary(
                connection,
                batch,
                division,
                data_mode,
            )

        else:
            selected_table = choose_table(question)

            if selected_table is None:
                answer, tables, rows = query_data_catalog(
                    connection,
                    batch,
                    data_mode,
                )
            else:
                rows = table_rows(
                    connection,
                    selected_table,
                    batch["batch_id"],
                    division,
                    limit=50,
                )

                model_answer = call_ollama(
                    question,
                    batch,
                    division,
                    data_mode,
                    selected_table,
                    rows,
                )

                answer = (
                    model_answer
                    or generic_fallback_answer(
                        batch,
                        division,
                        data_mode,
                        selected_table,
                        rows,
                    )
                )
                tables = [selected_table]

        rows = rows[:MAX_SUPPORTING_ROWS]

        governance_note = get_governance_note(
            connection,
            batch["batch_id"],
            question,
        )

        if governance_note:
            answer = governance_note + "\n\n" + answer

        return DatasetChatResponse(
            answer=answer,
            model=(
                "deterministic-read-only"
                if not answer.startswith(
                    preview_prefix(data_mode)
                    + "No "
                )
                else
                "deterministic-read-only"
            ),
            data_mode=data_mode,
            data_status=batch["analytics_status"],
            estate_name=batch["estate_name"],
            reporting_month=batch["reporting_month"],
            division_name=division,
            source_tables=[
                table
                for table in tables
                if table_exists(connection, table)
            ],
            supporting_rows=rows,
            supporting_row_count=len(rows),
            warning=warning,
        )

    finally:
        connection.close()
