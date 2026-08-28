from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware


DEFAULT_DATABASE = Path(
    r"G:\TTE_Local_Intelligence\data\tte_intelligence.duckdb"
)

DATABASE_PATH = Path(
    os.environ.get("TTE_DB_PATH", str(DEFAULT_DATABASE))
)

app = FastAPI(
    title="TTE Local Intelligence API",
    description=(
        "Read-only analytical API for "
        "Talawakelle Tea Estates PLC."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5173",
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


def get_connection() -> duckdb.DuckDBPyConnection:
    """Open the approved TTE database in read-only mode."""

    if not DATABASE_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Database not found: {DATABASE_PATH}",
        )

    try:
        return duckdb.connect(
            str(DATABASE_PATH),
            read_only=True,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Unable to open database: {exc}",
        ) from exc


def fetch_dicts(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    parameters: list[Any] | None = None,
) -> list[dict[str, Any]]:
    """Execute an approved query and return dictionaries."""

    result = connection.execute(
        sql,
        parameters or [],
    )

    columns = [
        description[0]
        for description in result.description
    ]

    return [
        dict(zip(columns, row))
        for row in result.fetchall()
    ]


def estate_exists(
    connection: duckdb.DuckDBPyConnection,
    estate_id: str,
) -> dict[str, Any]:
    records = fetch_dicts(
        connection,
        """
        SELECT
            estate_id,
            estate_name,
            region_code,
            data_status,
            weighing_records,
            first_date,
            last_date,
            status_note
        FROM estate_registry
        WHERE estate_id = ?
        """,
        [estate_id],
    )

    if not records:
        raise HTTPException(
            status_code=404,
            detail=f"Estate ID {estate_id} was not found.",
        )

    return records[0]


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "TTE Local Intelligence API",
        "status": "online",
        "scope": "Talawakelle Tea Estates PLC",
        "documentation": "/docs",
    }


@app.get("/health")
def health() -> dict[str, Any]:
    connection = get_connection()

    try:
        estate_count = connection.execute(
            "SELECT COUNT(*) FROM estate_registry"
        ).fetchone()[0]

        loaded_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM estate_registry
            WHERE data_status = 'LOADED'
            """
        ).fetchone()[0]

        record_count = connection.execute(
            "SELECT COUNT(*) FROM estate_records"
        ).fetchone()[0]

        return {
            "status": "healthy",
            "database": str(DATABASE_PATH),
            "estate_registry_count": estate_count,
            "loaded_estates": loaded_count,
            "records": record_count,
        }
    finally:
        connection.close()


@app.get("/api/summary")
def company_summary() -> dict[str, Any]:
    connection = get_connection()

    try:
        summary = fetch_dicts(
            connection,
            """
            SELECT
                COUNT(DISTINCT estate_id)
                    AS estates_with_records,
                COUNT(DISTINCT employee_key)
                    AS workers,
                COUNT(*)
                    AS weighing_records,
                ROUND(SUM(net_weight), 2)
                    AS total_net_kg,
                ROUND(AVG(net_weight), 2)
                    AS average_net_kg,
                MIN(work_date)
                    AS first_date,
                MAX(work_date)
                    AS last_date
            FROM estate_records
            """,
        )[0]

        registry = fetch_dicts(
            connection,
            """
            SELECT
                data_status,
                COUNT(*) AS estates
            FROM estate_registry
            GROUP BY data_status
            ORDER BY data_status
            """,
        )

        return {
            "company_code": "TTE",
            "company_name": "Talawakelle Tea Estates PLC",
            "summary": summary,
            "estate_status": registry,
        }
    finally:
        connection.close()


@app.get("/api/estates")
def list_estates() -> list[dict[str, Any]]:
    connection = get_connection()

    try:
        return fetch_dicts(
            connection,
            """
            SELECT
                estate_id,
                estate_name,
                region_code,
                data_status,
                weighing_records,
                first_date,
                last_date,
                status_note
            FROM estate_registry
            ORDER BY TRY_CAST(estate_id AS INTEGER)
            """,
        )
    finally:
        connection.close()


@app.get("/api/estates/{estate_id}")
def get_estate(estate_id: str) -> dict[str, Any]:
    connection = get_connection()

    try:
        estate = estate_exists(
            connection,
            estate_id,
        )

        if estate["data_status"] != "LOADED":
            return {
                "estate": estate,
                "summary": None,
                "message": (
                    f"{estate['estate_name']} has no loaded "
                    "source data."
                ),
            }

        summary = fetch_dicts(
            connection,
            """
            SELECT
                COUNT(*) AS weighing_records,
                COUNT(DISTINCT employee_key) AS workers,
                COUNT(DISTINCT division_id) AS divisions,
                COUNT(DISTINCT field_id) AS fields,
                ROUND(SUM(net_weight), 2) AS total_net_kg,
                ROUND(AVG(net_weight), 2) AS average_net_kg,
                MIN(work_date) AS first_date,
                MAX(work_date) AS last_date
            FROM estate_records
            WHERE estate_id = ?
            """,
            [estate_id],
        )[0]

        return {
            "estate": estate,
            "summary": summary,
        }
    finally:
        connection.close()


@app.get("/api/estates/{estate_id}/daily")
def estate_daily(
    estate_id: str,
    start_date: str | None = Query(
        default=None,
        description="Start date in YYYY-MM-DD format",
    ),
    end_date: str | None = Query(
        default=None,
        description="End date in YYYY-MM-DD format",
    ),
) -> dict[str, Any]:
    connection = get_connection()

    try:
        estate = estate_exists(
            connection,
            estate_id,
        )

        if estate["data_status"] != "LOADED":
            return {
                "estate": estate,
                "results": [],
                "message": "No estate data is currently loaded.",
            }

        conditions = ["estate_id = ?"]
        parameters: list[Any] = [estate_id]

        if start_date:
            conditions.append("work_date >= CAST(? AS DATE)")
            parameters.append(start_date)

        if end_date:
            conditions.append("work_date <= CAST(? AS DATE)")
            parameters.append(end_date)

        where_clause = " AND ".join(conditions)

        results = fetch_dicts(
            connection,
            f"""
            SELECT
                work_date,
                weighing_records,
                active_workers,
                ROUND(total_net_kg, 2) AS total_net_kg,
                ROUND(average_net_kg, 2) AS average_net_kg
            FROM daily_estate_summary
            WHERE {where_clause}
            ORDER BY work_date
            """,
            parameters,
        )

        return {
            "estate": estate,
            "filters": {
                "start_date": start_date,
                "end_date": end_date,
            },
            "results": results,
        }
    except duckdb.ConversionException as exc:
        raise HTTPException(
            status_code=400,
            detail="Dates must use YYYY-MM-DD format.",
        ) from exc
    finally:
        connection.close()


@app.get("/api/estates/{estate_id}/monthly")
def estate_monthly(
    estate_id: str,
) -> dict[str, Any]:
    connection = get_connection()

    try:
        estate = estate_exists(
            connection,
            estate_id,
        )

        if estate["data_status"] != "LOADED":
            return {
                "estate": estate,
                "results": [],
                "message": "No estate data is currently loaded.",
            }

        results = fetch_dicts(
            connection,
            """
            SELECT
                month,
                weighing_records,
                active_workers,
                ROUND(total_net_kg, 2) AS total_net_kg,
                ROUND(average_net_kg, 2) AS average_net_kg,
                ROUND(total_gross_kg, 2) AS total_gross_kg,
                ROUND(tare_total, 2) AS tare_total,
                ROUND(moisture_total, 2) AS moisture_total,
                ROUND(coarse_leaf_total, 2)
                    AS coarse_leaf_total
            FROM monthly_estate_summary
            WHERE estate_id = ?
            ORDER BY month
            """,
            [estate_id],
        )

        return {
            "estate": estate,
            "results": results,
        }
    finally:
        connection.close()

from app.chat_routes import router as chat_router

app.include_router(chat_router)

from app.upload_routes import router as upload_router

app.include_router(upload_router)

from app.monitor_routes import router as monitor_router

app.include_router(monitor_router)

from app.dataset_chat_routes import router as dataset_chat_router

app.include_router(dataset_chat_router)

from app.governance_routes import router as governance_router

app.include_router(governance_router)

from app.advanced_chat_routes import router as advanced_chat_router

app.include_router(advanced_chat_router)

from app.governance_v2_routes import router as governance_v2_router

app.include_router(governance_v2_router)

from app.full_suite_routes import router as full_suite_router

app.include_router(full_suite_router)
