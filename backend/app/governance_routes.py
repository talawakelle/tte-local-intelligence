from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field


router = APIRouter()

PRODUCTION_DB = Path(
    r"G:\TTE_Local_Intelligence\production_data\tte_production.duckdb"
)
GOVERNANCE_HTML = (
    Path(__file__).resolve().parent
    / "static"
    / "governance.html"
)


class ResolveIssueRequest(BaseModel):
    official_source: str
    approved_by: str = Field(min_length=2, max_length=200)
    approval_note: str = Field(min_length=3, max_length=1000)
    confirm_text: str
    manual_value: float | None = None


def serialisable(value: Any) -> Any:
    if value is None:
        return None

    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass

    return value


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


@router.get("/governance", include_in_schema=False)
def governance_page() -> FileResponse:
    if not GOVERNANCE_HTML.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Governance page not found: {GOVERNANCE_HTML}",
        )

    return FileResponse(GOVERNANCE_HTML)


@router.get("/api/data-governance/status")
def governance_status() -> dict[str, Any]:
    connection = open_db(read_only=True)

    try:
        return {
            "status": "online",
            "issue_summary": fetch_dicts(
                connection,
                """
                SELECT status, severity, COUNT(*) AS issue_count
                FROM data_quality_issues
                GROUP BY status, severity
                ORDER BY status, severity
                """,
            ),
            "metric_approval_count": connection.execute(
                "SELECT COUNT(*) FROM metric_approvals"
            ).fetchone()[0],
            "page": "/governance",
        }

    finally:
        connection.close()


@router.get("/api/data-governance/issues")
def list_issues(
    status: str | None = Query(default=None),
    batch_id: str | None = Query(default=None),
) -> list[dict[str, Any]]:
    connection = open_db(read_only=True)

    try:
        conditions = ["1 = 1"]
        parameters: list[Any] = []

        if status:
            conditions.append("status = ?")
            parameters.append(status.upper())

        if batch_id:
            conditions.append("batch_id = ?")
            parameters.append(batch_id)

        return fetch_dicts(
            connection,
            f"""
            SELECT *
            FROM data_quality_issues
            WHERE {' AND '.join(conditions)}
            ORDER BY
                CASE severity
                    WHEN 'HIGH' THEN 1
                    WHEN 'MEDIUM' THEN 2
                    ELSE 3
                END,
                status,
                detected_at DESC
            """,
            parameters,
        )

    finally:
        connection.close()


@router.get("/api/data-governance/approvals")
def list_approvals() -> list[dict[str, Any]]:
    connection = open_db(read_only=True)

    try:
        return fetch_dicts(
            connection,
            """
            SELECT *
            FROM metric_approvals
            ORDER BY approved_at DESC
            """,
        )

    finally:
        connection.close()


@router.post("/api/data-governance/issues/{issue_id}/resolve")
def resolve_issue(
    issue_id: str,
    payload: ResolveIssueRequest,
) -> dict[str, Any]:
    if payload.confirm_text.strip().upper() != "APPROVE OFFICIAL METRIC":
        raise HTTPException(
            status_code=400,
            detail=(
                'Confirmation text must be exactly '
                '"APPROVE OFFICIAL METRIC".'
            ),
        )

    source = payload.official_source.strip().upper()

    if source not in {"G_REPORT", "PLUCKING_PLAN", "MANUAL"}:
        raise HTTPException(
            status_code=400,
            detail=(
                "official_source must be G_REPORT, "
                "PLUCKING_PLAN or MANUAL."
            ),
        )

    connection = open_db(read_only=False)

    try:
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

        if issue["issue_type"] != "SOURCE_RECONCILIATION":
            raise HTTPException(
                status_code=400,
                detail=(
                    "Only source-reconciliation issues can approve "
                    "an official metric."
                ),
            )

        if source == "G_REPORT":
            official_value = issue["source_a_value"]
        elif source == "PLUCKING_PLAN":
            official_value = issue["source_b_value"]
        else:
            if payload.manual_value is None:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "manual_value is required when "
                        "official_source is MANUAL."
                    ),
                )
            official_value = payload.manual_value

        approved_at = datetime.now()
        approval_id = str(uuid.uuid4())

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
                    approved_at,
                    payload.approved_by.strip(),
                    payload.approval_note.strip(),
                    source,
                    official_value,
                    issue_id,
                ],
            )

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
                    approval_id,
                    issue_id,
                    issue["batch_id"],
                    issue["estate_name"],
                    issue["reporting_month"],
                    issue["metric_name"],
                    source,
                    official_value,
                    payload.approved_by.strip(),
                    payload.approval_note.strip(),
                    approved_at,
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
                    "APPROVE_OFFICIAL_METRIC",
                    issue_id,
                    issue["batch_id"],
                    payload.approved_by.strip(),
                    payload.approval_note.strip(),
                    approved_at,
                ],
            )

            connection.execute("COMMIT")

        except Exception:
            connection.execute("ROLLBACK")
            raise

        return {
            "status": "RESOLVED",
            "issue_id": issue_id,
            "estate_name": issue["estate_name"],
            "reporting_month": issue["reporting_month"],
            "metric_name": issue["metric_name"],
            "official_source": source,
            "official_value": official_value,
            "approved_by": payload.approved_by.strip(),
            "approved_at": approved_at.isoformat(),
            "message": (
                "Official metric approved. The chatbot will show "
                "the approved value and reconciliation details."
            ),
        }

    finally:
        connection.close()
