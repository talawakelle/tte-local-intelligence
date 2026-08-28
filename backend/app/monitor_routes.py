from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import duckdb
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse


router = APIRouter()

PROJECT_ROOT = Path(r"G:\TTE_Local_Intelligence")
PRODUCTION_ROOT = PROJECT_ROOT / "production_data"
PRODUCTION_DB = PRODUCTION_ROOT / "tte_production.duckdb"
PREVIEW_ROOT = PRODUCTION_ROOT / "previews"
APPROVED_ROOT = PRODUCTION_ROOT / "approved_sources"
MONITOR_HTML = Path(__file__).resolve().parent / "static" / "monitor.html"

BATCH_ID_PATTERN = re.compile(r"^[A-Za-z0-9-]+$")
TABLE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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

    rows: list[dict[str, Any]] = []

    for row in result.fetchall():
        rows.append(
            {
                column: serialisable(value)
                for column, value in zip(columns, row)
            }
        )

    return rows


def open_production_db() -> duckdb.DuckDBPyConnection:
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


def validate_batch_id(batch_id: str) -> str:
    if not BATCH_ID_PATTERN.fullmatch(batch_id):
        raise HTTPException(status_code=400, detail="Invalid batch ID.")

    return batch_id


def preview_path(batch_id: str) -> Path:
    return PREVIEW_ROOT / f"{validate_batch_id(batch_id)}.json"


def read_preview(batch_id: str) -> dict[str, Any]:
    path = preview_path(batch_id)

    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Preview not found for batch {batch_id}.",
        )

    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Preview JSON is invalid: {exc}",
        ) from exc


def find_approved_batch_folder(
    batch_id: str,
    preview: dict[str, Any],
) -> Path | None:
    estate = str(preview.get("estate_name", "")).strip().lower()
    month = str(preview.get("reporting_month", "")).strip()

    direct = APPROVED_ROOT / estate / month / batch_id

    if direct.exists():
        return direct

    if APPROVED_ROOT.exists():
        for candidate in APPROVED_ROOT.rglob(batch_id):
            if candidate.is_dir():
                return candidate

    return None


def list_source_files(folder: Path | None) -> list[dict[str, Any]]:
    if folder is None or not folder.exists():
        return []

    output: list[dict[str, Any]] = []

    for path in sorted(folder.rglob("*")):
        if not path.is_file():
            continue

        output.append(
            {
                "name": path.name,
                "relative_path": str(path.relative_to(folder)),
                "extension": path.suffix.lower(),
                "size_bytes": path.stat().st_size,
                "size_mb": round(path.stat().st_size / (1024 * 1024), 3),
                "modified_at": path.stat().st_mtime,
            }
        )

    return output


@router.get("/monitor", include_in_schema=False)
def production_monitor() -> FileResponse:
    if not MONITOR_HTML.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Monitor page not found: {MONITOR_HTML}",
        )

    return FileResponse(MONITOR_HTML)


@router.get("/api/monitor/health")
def monitor_health() -> dict[str, Any]:
    return {
        "status": "healthy",
        "production_root": str(PRODUCTION_ROOT),
        "production_database": str(PRODUCTION_DB),
        "production_database_exists": PRODUCTION_DB.exists(),
        "preview_root": str(PREVIEW_ROOT),
        "approved_root": str(APPROVED_ROOT),
        "monitor_page": "/monitor",
    }


@router.get("/api/monitor/previews")
def list_previews() -> list[dict[str, Any]]:
    if not PREVIEW_ROOT.exists():
        return []

    previews: list[dict[str, Any]] = []

    for path in sorted(
        PREVIEW_ROOT.glob("*.json"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    ):
        try:
            data = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception as exc:
            previews.append(
                {
                    "batch_id": path.stem,
                    "status": "INVALID_PREVIEW",
                    "error": str(exc),
                    "modified_at": path.stat().st_mtime,
                }
            )
            continue

        previews.append(
            {
                "batch_id": data.get("batch_id", path.stem),
                "estate_name": data.get("estate_name"),
                "reporting_month": data.get("reporting_month"),
                "analytics_status": data.get("analytics_status"),
                "parser_version": data.get("parser_version"),
                "errors_count": len(data.get("errors") or []),
                "warnings_count": len(data.get("warnings") or []),
                "modified_at": path.stat().st_mtime,
            }
        )

    return previews


@router.get("/api/monitor/previews/{batch_id}")
def get_preview(batch_id: str) -> dict[str, Any]:
    preview = read_preview(batch_id)
    approved_folder = find_approved_batch_folder(batch_id, preview)

    return {
        "preview": preview,
        "approved_folder": (
            str(approved_folder)
            if approved_folder is not None
            else None
        ),
        "source_files": list_source_files(approved_folder),
    }


@router.get("/api/monitor/tables")
def list_production_tables() -> list[dict[str, Any]]:
    connection = open_production_db()

    try:
        tables = fetch_dicts(
            connection,
            """
            SELECT
                table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
        )

        output: list[dict[str, Any]] = []

        for table in tables:
            table_name = table["table_name"]

            if not TABLE_PATTERN.fullmatch(table_name):
                continue

            quoted = '"' + table_name.replace('"', '""') + '"'

            try:
                row_count = connection.execute(
                    f"SELECT COUNT(*) FROM {quoted}"
                ).fetchone()[0]
            except Exception:
                row_count = None

            columns = fetch_dicts(
                connection,
                """
                SELECT
                    column_name,
                    data_type,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_schema = 'main'
                  AND table_name = ?
                ORDER BY ordinal_position
                """,
                [table_name],
            )

            output.append(
                {
                    "table_name": table_name,
                    "row_count": row_count,
                    "columns": columns,
                }
            )

        return output

    finally:
        connection.close()


@router.get("/api/monitor/tables/{table_name}")
def read_production_table(
    table_name: str,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    if not TABLE_PATTERN.fullmatch(table_name):
        raise HTTPException(status_code=400, detail="Invalid table name.")

    connection = open_production_db()

    try:
        exists = connection.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_type = 'BASE TABLE'
              AND table_name = ?
            """,
            [table_name],
        ).fetchone()[0]

        if not exists:
            raise HTTPException(
                status_code=404,
                detail=f"Table not found: {table_name}",
            )

        quoted = '"' + table_name.replace('"', '""') + '"'

        total_rows = connection.execute(
            f"SELECT COUNT(*) FROM {quoted}"
        ).fetchone()[0]

        rows = fetch_dicts(
            connection,
            f"SELECT * FROM {quoted} LIMIT ? OFFSET ?",
            [limit, offset],
        )

        columns = fetch_dicts(
            connection,
            """
            SELECT
                column_name,
                data_type,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'main'
              AND table_name = ?
            ORDER BY ordinal_position
            """,
            [table_name],
        )

        return {
            "table_name": table_name,
            "total_rows": total_rows,
            "limit": limit,
            "offset": offset,
            "columns": columns,
            "rows": rows,
        }

    finally:
        connection.close()
