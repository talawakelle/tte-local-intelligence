from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field


router = APIRouter()

PRODUCTION_DB = Path(
    r"G:\TTE_Local_Intelligence\production_data\tte_production.duckdb"
)
GOVERNANCE_HTML = (
    Path(__file__).resolve().parent
    / "static"
    / "governance_v2.html"
)


class ResolveRequest(BaseModel):
    action: str
    approved_by: str = Field(min_length=2, max_length=200)
    note: str = Field(min_length=3, max_length=1200)
    confirm_text: str
    official_source: str | None = None
    manual_value: float | None = None


def open_db(read_only: bool) -> duckdb.DuckDBPyConnection:
    if not PRODUCTION_DB.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Production database not found: {PRODUCTION_DB}",
        )
    try:
        return duckdb.connect(
            str(PRODUCTION_DB),
            read_only=read_only,
        )
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


def ensure_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS data_quality_issues (
            issue_id VARCHAR PRIMARY KEY,
            batch_id VARCHAR NOT NULL,
            estate_name VARCHAR NOT NULL,
            reporting_month VARCHAR NOT NULL,
            issue_type VARCHAR NOT NULL,
            severity VARCHAR NOT NULL,
            metric_name VARCHAR NOT NULL,
            source_a_name VARCHAR,
            source_a_value DOUBLE,
            source_b_name VARCHAR,
            source_b_value DOUBLE,
            difference_value DOUBLE,
            difference_pct DOUBLE,
            status VARCHAR NOT NULL,
            manager_message VARCHAR NOT NULL,
            detected_at TIMESTAMP NOT NULL,
            resolved_at TIMESTAMP,
            resolved_by VARCHAR,
            resolution_note VARCHAR,
            official_source VARCHAR,
            official_value DOUBLE
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS metric_approvals (
            approval_id VARCHAR PRIMARY KEY,
            issue_id VARCHAR NOT NULL,
            batch_id VARCHAR NOT NULL,
            estate_name VARCHAR NOT NULL,
            reporting_month VARCHAR NOT NULL,
            metric_name VARCHAR NOT NULL,
            official_source VARCHAR NOT NULL,
            official_value DOUBLE NOT NULL,
            approved_by VARCHAR NOT NULL,
            approval_note VARCHAR,
            approved_at TIMESTAMP NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS governance_audit_log (
            audit_id VARCHAR PRIMARY KEY,
            action_type VARCHAR NOT NULL,
            issue_id VARCHAR,
            batch_id VARCHAR,
            performed_by VARCHAR,
            action_note VARCHAR,
            action_at TIMESTAMP NOT NULL
        )
        """
    )


def issue_exists(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    issue_type: str,
    metric_name: str,
) -> bool:
    return bool(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM data_quality_issues
            WHERE batch_id = ?
              AND issue_type = ?
              AND metric_name = ?
            """,
            [batch_id, issue_type, metric_name],
        ).fetchone()[0]
    )


def scan_batch(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    estate_name: str,
    reporting_month: str,
) -> int:
    created = 0

    g_report = connection.execute(
        """
        SELECT SUM(total_green_leaf_kg)
        FROM g_report_metrics
        WHERE batch_id = ?
        """,
        [batch_id],
    ).fetchone()[0]
    plan = connection.execute(
        """
        SELECT SUM(actual_green_leaf_kg)
        FROM plucking_plan_fields
        WHERE batch_id = ?
        """,
        [batch_id],
    ).fetchone()[0]

    if g_report is not None and plan is not None:
        difference = float(plan) - float(g_report)
        difference_pct = (
            difference / float(g_report) * 100
            if float(g_report) != 0
            else None
        )
        if (
            abs(difference) > 0.01
            and not issue_exists(
                connection,
                batch_id,
                "SOURCE_RECONCILIATION",
                "green_leaf_kg",
            )
        ):
            message = (
                f"{estate_name} {reporting_month} green-leaf reconciliation "
                f"is pending. The G Report records {float(g_report):,.0f} kg "
                f"and the Plucking Plan records {float(plan):,.0f} kg, "
                f"a difference of {difference:,.0f} kg"
                + (
                    f" ({difference_pct:,.2f}%)."
                    if difference_pct is not None
                    else "."
                )
                + " No official green-leaf figure has been approved."
            )
            connection.execute(
                """
                INSERT INTO data_quality_issues (
                    issue_id,
                    batch_id,
                    estate_name,
                    reporting_month,
                    issue_type,
                    severity,
                    metric_name,
                    source_a_name,
                    source_a_value,
                    source_b_name,
                    source_b_value,
                    difference_value,
                    difference_pct,
                    status,
                    manager_message,
                    detected_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    str(uuid.uuid4()),
                    batch_id,
                    estate_name,
                    reporting_month,
                    "SOURCE_RECONCILIATION",
                    "HIGH",
                    "green_leaf_kg",
                    "G_REPORT",
                    float(g_report),
                    "PLUCKING_PLAN",
                    float(plan),
                    difference,
                    difference_pct,
                    "OPEN",
                    message,
                    datetime.now(),
                ],
            )
            created += 1

    outlier = connection.execute(
        """
        SELECT COUNT(*), MAX(budget_yph)
        FROM agricultural_activity_records
        WHERE batch_id = ?
          AND budget_yph > 10000
        """,
        [batch_id],
    ).fetchone()
    if (
        outlier
        and outlier[0]
        and not issue_exists(
            connection,
            batch_id,
            "POTENTIAL_OUTLIER",
            "budget_yph_outlier",
        )
    ):
        connection.execute(
            """
            INSERT INTO data_quality_issues (
                issue_id,
                batch_id,
                estate_name,
                reporting_month,
                issue_type,
                severity,
                metric_name,
                source_a_name,
                source_a_value,
                difference_value,
                status,
                manager_message,
                detected_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(uuid.uuid4()),
                batch_id,
                estate_name,
                reporting_month,
                "POTENTIAL_OUTLIER",
                "MEDIUM",
                "budget_yph_outlier",
                "AGRICULTURAL_ACTIVITY",
                float(outlier[1]),
                float(outlier[0]),
                "OPEN",
                (
                    f"{estate_name} {reporting_month} contains "
                    f"{int(outlier[0])} agricultural records with budget YPH "
                    f"above 10,000. The maximum is {float(outlier[1]):,.0f}. "
                    "Verify whether these are valid values or data-entry errors."
                ),
                datetime.now(),
            ],
        )
        created += 1

    negative_cop = connection.execute(
        """
        SELECT COUNT(*), MIN(rupees_per_kg)
        FROM cop_line_items
        WHERE batch_id = ?
          AND rupees_per_kg < 0
        """,
        [batch_id],
    ).fetchone()
    if (
        negative_cop
        and negative_cop[0]
        and not issue_exists(
            connection,
            batch_id,
            "NEGATIVE_VALUE",
            "negative_cop_lines",
        )
    ):
        connection.execute(
            """
            INSERT INTO data_quality_issues (
                issue_id,
                batch_id,
                estate_name,
                reporting_month,
                issue_type,
                severity,
                metric_name,
                source_a_name,
                source_a_value,
                difference_value,
                status,
                manager_message,
                detected_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(uuid.uuid4()),
                batch_id,
                estate_name,
                reporting_month,
                "NEGATIVE_VALUE",
                "MEDIUM",
                "negative_cop_lines",
                "COP_LINE_ITEMS",
                float(negative_cop[1]),
                float(negative_cop[0]),
                "OPEN",
                (
                    f"{estate_name} {reporting_month} contains "
                    f"{int(negative_cop[0])} negative COP line item(s). "
                    f"The minimum is Rs. {float(negative_cop[1]):,.2f}/kg. "
                    "Confirm whether these are valid credits/apportionments."
                ),
                datetime.now(),
            ],
        )
        created += 1

    missing_buyer = connection.execute(
        """
        SELECT COUNT(*)
        FROM revenue_grade_sales
        WHERE batch_id = ?
          AND (buyer IS NULL OR TRIM(buyer) = '')
        """,
        [batch_id],
    ).fetchone()[0]
    if (
        missing_buyer
        and not issue_exists(
            connection,
            batch_id,
            "MISSING_VALUE",
            "missing_buyer",
        )
    ):
        connection.execute(
            """
            INSERT INTO data_quality_issues (
                issue_id,
                batch_id,
                estate_name,
                reporting_month,
                issue_type,
                severity,
                metric_name,
                source_a_name,
                source_a_value,
                difference_value,
                status,
                manager_message,
                detected_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(uuid.uuid4()),
                batch_id,
                estate_name,
                reporting_month,
                "MISSING_VALUE",
                "LOW",
                "missing_buyer",
                "REVENUE_GRADE_SALES",
                float(missing_buyer),
                float(missing_buyer),
                "OPEN",
                (
                    f"{estate_name} {reporting_month} contains "
                    f"{int(missing_buyer)} revenue-grade sale row(s) "
                    "without a buyer name."
                ),
                datetime.now(),
            ],
        )
        created += 1

    return created


@router.get("/governance-v2", include_in_schema=False)
def governance_page() -> FileResponse:
    if not GOVERNANCE_HTML.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Governance page not found: {GOVERNANCE_HTML}",
        )
    return FileResponse(GOVERNANCE_HTML)


@router.post("/api/governance-v2/scan")
def run_scan() -> dict[str, Any]:
    connection = open_db(read_only=False)
    try:
        ensure_tables(connection)
        batches = fetch_dicts(
            connection,
            """
            SELECT batch_id, estate_name, reporting_month
            FROM batch_analytics
            ORDER BY estate_name, reporting_month
            """,
        )
        created = 0
        for batch in batches:
            created += scan_batch(
                connection,
                batch["batch_id"],
                batch["estate_name"],
                batch["reporting_month"],
            )
        return {
            "status": "completed",
            "batches_scanned": len(batches),
            "issues_created": created,
        }
    finally:
        connection.close()


@router.get("/api/governance-v2/issues")
def list_issues() -> list[dict[str, Any]]:
    connection = open_db(read_only=False)
    try:
        ensure_tables(connection)
        return fetch_dicts(
            connection,
            """
            SELECT *
            FROM data_quality_issues
            ORDER BY
                CASE severity
                    WHEN 'HIGH' THEN 1
                    WHEN 'MEDIUM' THEN 2
                    ELSE 3
                END,
                status,
                detected_at DESC
            """,
        )
    finally:
        connection.close()


@router.post("/api/governance-v2/issues/{issue_id}/resolve")
def resolve_issue(
    issue_id: str,
    payload: ResolveRequest,
) -> dict[str, Any]:
    if payload.confirm_text.strip().upper() != "CONFIRM MANAGEMENT ACTION":
        raise HTTPException(
            status_code=400,
            detail=(
                'Confirmation text must be exactly '
                '"CONFIRM MANAGEMENT ACTION".'
            ),
        )

    action = payload.action.strip().upper()
    if action not in {
        "APPROVE_OFFICIAL_METRIC",
        "MARK_REVIEWED",
        "DISMISS_WITH_REASON",
    }:
        raise HTTPException(
            status_code=400,
            detail="Unsupported governance action.",
        )

    connection = open_db(read_only=False)
    try:
        ensure_tables(connection)
        issues = fetch_dicts(
            connection,
            """
            SELECT *
            FROM data_quality_issues
            WHERE issue_id = ?
            LIMIT 1
            """,
            [issue_id],
        )
        if not issues:
            raise HTTPException(status_code=404, detail="Issue not found.")
        issue = issues[0]
        if issue["status"] == "RESOLVED":
            raise HTTPException(
                status_code=409,
                detail="This issue is already resolved.",
            )

        official_source = None
        official_value = None

        if action == "APPROVE_OFFICIAL_METRIC":
            if issue["issue_type"] != "SOURCE_RECONCILIATION":
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Only source-reconciliation issues support "
                        "official metric approval."
                    ),
                )
            official_source = (payload.official_source or "").strip().upper()
            if official_source == "G_REPORT":
                official_value = issue["source_a_value"]
            elif official_source == "PLUCKING_PLAN":
                official_value = issue["source_b_value"]
            elif official_source == "MANUAL":
                if payload.manual_value is None:
                    raise HTTPException(
                        status_code=400,
                        detail="manual_value is required.",
                    )
                official_value = payload.manual_value
            else:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "official_source must be G_REPORT, "
                        "PLUCKING_PLAN or MANUAL."
                    ),
                )

        now = datetime.now()
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute(
                """
                UPDATE data_quality_issues
                SET
                    status = 'RESOLVED',
                    resolved_at = ?,
                    resolved_by = ?,
                    resolution_note = ?,
                    official_source = ?,
                    official_value = ?
                WHERE issue_id = ?
                """,
                [
                    now,
                    payload.approved_by.strip(),
                    payload.note.strip(),
                    official_source,
                    official_value,
                    issue_id,
                ],
            )

            if action == "APPROVE_OFFICIAL_METRIC":
                connection.execute(
                    """
                    INSERT INTO metric_approvals (
                        approval_id,
                        issue_id,
                        batch_id,
                        estate_name,
                        reporting_month,
                        metric_name,
                        official_source,
                        official_value,
                        approved_by,
                        approval_note,
                        approved_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        str(uuid.uuid4()),
                        issue_id,
                        issue["batch_id"],
                        issue["estate_name"],
                        issue["reporting_month"],
                        issue["metric_name"],
                        official_source,
                        official_value,
                        payload.approved_by.strip(),
                        payload.note.strip(),
                        now,
                    ],
                )

            connection.execute(
                """
                INSERT INTO governance_audit_log (
                    audit_id,
                    action_type,
                    issue_id,
                    batch_id,
                    performed_by,
                    action_note,
                    action_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    str(uuid.uuid4()),
                    action,
                    issue_id,
                    issue["batch_id"],
                    payload.approved_by.strip(),
                    payload.note.strip(),
                    now,
                ],
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise

        return {
            "status": "RESOLVED",
            "action": action,
            "official_source": official_source,
            "official_value": official_value,
            "resolved_by": payload.approved_by.strip(),
            "resolved_at": now.isoformat(),
        }
    finally:
        connection.close()
