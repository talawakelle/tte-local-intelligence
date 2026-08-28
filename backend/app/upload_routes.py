from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field


router = APIRouter(
    prefix="/api/uploads",
    tags=["Production Data Uploads"],
)

DATA_ROOT = Path(
    os.environ.get(
        "TTE_PRODUCTION_DATA_ROOT",
        r"G:\TTE_Local_Intelligence\production_data",
    )
)

PRODUCTION_DB = Path(
    os.environ.get(
        "TTE_PRODUCTION_DB",
        str(DATA_ROOT / "tte_production.duckdb"),
    )
)

INCOMING_ROOT = DATA_ROOT / "incoming"
STAGING_ROOT = DATA_ROOT / "staging"
APPROVED_ROOT = DATA_ROOT / "approved_sources"
REJECTED_ROOT = DATA_ROOT / "rejected"

MAX_ZIP_BYTES = 150 * 1024 * 1024
MAX_EXTRACTED_BYTES = 1_000 * 1024 * 1024
MAX_EXTRACTED_FILES = 1_000

SUPPORTED_SOURCE_EXTENSIONS = {
    ".pdf",
    ".xlsx",
    ".xls",
    ".xlsm",
    ".csv",
}

REQUIRED_CATEGORIES = {
    "G_REPORT_PDF",
    "M_FORMAT",
    "PLUCKING_PLAN",
    "NON_WAGE_PLUCKING",
    "AGRICULTURAL_ACTIVITY",
    "CONTRACT_MONITORING",
    "BREAK_EVEN",
    "CROP_COP_REVENUE",
}

KNOWN_ESTATES = {
    "bearwell",
    "calsay",
    "clarendon",
    "deniyaya",
    "dessford",
    "great western",
    "greatwestern",
    "pitiyagoda",
    "holyrood",
    "indola",
    "kiruwanaganga",
    "logie",
    "mattakelle",
    "moragalla",
    "palmerston",
    "radella",
    "somerset",
}

MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


class ApprovalRequest(BaseModel):
    confirm_text: str = Field(
        description="Must be exactly APPROVE",
    )
    notes: str | None = Field(
        default=None,
        max_length=1_000,
    )


class RejectionRequest(BaseModel):
    reason: str = Field(
        min_length=3,
        max_length=1_000,
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_storage() -> None:
    for path in (
        DATA_ROOT,
        INCOMING_ROOT,
        STAGING_ROOT,
        APPROVED_ROOT,
        REJECTED_ROOT,
    ):
        path.mkdir(parents=True, exist_ok=True)


def open_production_db() -> duckdb.DuckDBPyConnection:
    ensure_storage()
    connection = duckdb.connect(str(PRODUCTION_DB))

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS upload_batches (
            batch_id VARCHAR PRIMARY KEY,
            estate_name VARCHAR NOT NULL,
            reporting_month VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL,
            validated_at TIMESTAMP,
            approved_at TIMESTAMP,
            rejected_at TIMESTAMP,
            uploaded_zip_count INTEGER NOT NULL,
            extracted_file_count INTEGER NOT NULL,
            error_count INTEGER NOT NULL,
            warning_count INTEGER NOT NULL,
            package_sha256 VARCHAR NOT NULL,
            manifest_json VARCHAR NOT NULL,
            validation_json VARCHAR NOT NULL,
            approved_path VARCHAR,
            notes VARCHAR
        )
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS uploaded_files (
            file_id VARCHAR PRIMARY KEY,
            batch_id VARCHAR NOT NULL,
            archive_name VARCHAR NOT NULL,
            relative_path VARCHAR NOT NULL,
            file_name VARCHAR NOT NULL,
            extension VARCHAR NOT NULL,
            size_bytes BIGINT NOT NULL,
            sha256 VARCHAR NOT NULL,
            category VARCHAR NOT NULL,
            division_name VARCHAR,
            validation_status VARCHAR NOT NULL,
            validation_message VARCHAR
        )
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS production_metadata (
            metadata_key VARCHAR PRIMARY KEY,
            metadata_value VARCHAR NOT NULL
        )
        """
    )

    connection.execute(
        """
        INSERT OR REPLACE INTO production_metadata VALUES
        ('data_mode', 'PRODUCTION'),
        ('dummy_data_enabled', 'FALSE'),
        ('analytics_state', 'SOURCE_UPLOAD_ONLY'),
        ('updated_at_utc', ?)
        """,
        [utc_now().isoformat()],
    )

    return connection


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "estate"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def classify_source(path: Path) -> str:
    name = path.name.upper()
    extension = path.suffix.lower()

    if extension == ".pdf":
        return "G_REPORT_PDF"

    if "M FORMAT" in name or "M FORMET" in name:
        if "NON WAGE" in name:
            return "NON_WAGE_PLUCKING"
        return "M_FORMAT"

    if "PLUCKING PLAN" in name or "PLACKING PLAN" in name:
        return "PLUCKING_PLAN"

    if "NON WAGE" in name:
        return "NON_WAGE_PLUCKING"

    if any(token in name for token in ("FERTIL", "WEED", "SPRAY")):
        return "AGRICULTURAL_ACTIVITY"

    if "CONTRACT" in name:
        return "CONTRACT_MONITORING"

    if "BREAK EVEN" in name or "BREAK-EVEN" in name:
        return "BREAK_EVEN"

    if any(token in name for token in ("80% 20%", "REVENUE", "COP")):
        return "CROP_COP_REVENUE"

    if extension in {".xlsx", ".xls", ".xlsm", ".csv"}:
        return "OTHER_TABULAR_SOURCE"

    return "UNSUPPORTED"


def detect_division(path: Path) -> str | None:
    upper = path.name.upper()

    token_map = (
        (r"(^|[^A-Z])BG([^A-Z]|$)", "BELGRAVIA"),
        (r"(^|[^A-Z])BW([^A-Z]|$)", "BEARWELL"),
        (r"(^|[^A-Z])FF([^A-Z]|$)", "FAIRFIELD"),
        (r"(^|[^A-Z])(WA|WH)([^A-Z]|$)", "WALAHA"),
    )

    for pattern, division in token_map:
        if re.search(pattern, upper):
            return division

    for division in ("BELGRAVIA", "BEARWELL", "FAIRFIELD", "WALAHA"):
        if division in upper:
            return division

    return None


def safe_extract_archive(
    archive_path: Path,
    destination: Path,
) -> list[Path]:
    destination.mkdir(parents=True, exist_ok=True)
    destination_resolved = destination.resolve()
    extracted: list[Path] = []
    total_bytes = 0

    try:
        archive = zipfile.ZipFile(archive_path)
    except zipfile.BadZipFile as exc:
        raise ValueError(f"Invalid ZIP file: {archive_path.name}") from exc

    with archive:
        members = archive.infolist()

        if len(members) > MAX_EXTRACTED_FILES:
            raise ValueError(
                f"ZIP contains too many entries: {len(members)}"
            )

        for member in members:
            member_path = Path(member.filename)

            if member_path.is_absolute() or ".." in member_path.parts:
                raise ValueError(
                    f"Unsafe ZIP path detected: {member.filename}"
                )

            total_bytes += member.file_size

            if total_bytes > MAX_EXTRACTED_BYTES:
                raise ValueError(
                    "Uncompressed ZIP content exceeds the 1 GB safety limit."
                )

            target = (destination / member_path).resolve()

            if (
                target != destination_resolved
                and destination_resolved not in target.parents
            ):
                raise ValueError(
                    f"Unsafe extraction target: {member.filename}"
                )

            if member.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue

            target.parent.mkdir(parents=True, exist_ok=True)

            with archive.open(member) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output)

            extracted.append(target)

    return extracted


def batch_record(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
) -> dict[str, Any]:
    result = connection.execute(
        """
        SELECT *
        FROM upload_batches
        WHERE batch_id = ?
        """,
        [batch_id],
    )

    row = result.fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Upload batch not found: {batch_id}",
        )

    columns = [column[0] for column in result.description]
    return dict(zip(columns, row))


def json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


@router.get("/status")
def production_upload_status() -> dict[str, Any]:
    connection = open_production_db()

    try:
        status_rows = connection.execute(
            """
            SELECT status, COUNT(*)
            FROM upload_batches
            GROUP BY status
            ORDER BY status
            """
        ).fetchall()

        approved_months = connection.execute(
            """
            SELECT estate_name, reporting_month, batch_id, approved_at
            FROM upload_batches
            WHERE status = 'APPROVED_SOURCE'
            ORDER BY estate_name, reporting_month
            """
        ).fetchall()

        return {
            "data_mode": "PRODUCTION",
            "dummy_data_enabled": False,
            "analytics_state": "SOURCE_UPLOAD_ONLY",
            "batch_status_counts": {
                status: count for status, count in status_rows
            },
            "approved_sources": [
                {
                    "estate_name": row[0],
                    "reporting_month": row[1],
                    "batch_id": row[2],
                    "approved_at": json_value(row[3]),
                }
                for row in approved_months
            ],
        }
    finally:
        connection.close()


@router.post("/validate")
async def validate_monthly_upload(
    estate_name: str = Form(...),
    reporting_month: str = Form(...),
    files: list[UploadFile] = File(...),
) -> dict[str, Any]:
    estate_name = estate_name.strip()
    reporting_month = reporting_month.strip()

    if len(estate_name) < 2 or len(estate_name) > 100:
        raise HTTPException(
            status_code=400,
            detail="Estate name must contain 2 to 100 characters.",
        )

    if not MONTH_PATTERN.fullmatch(reporting_month):
        raise HTTPException(
            status_code=400,
            detail="Reporting month must use YYYY-MM format.",
        )

    if not files:
        raise HTTPException(
            status_code=400,
            detail="At least one ZIP file is required.",
        )

    ensure_storage()

    batch_id = str(uuid.uuid4())
    batch_root = STAGING_ROOT / batch_id
    original_root = batch_root / "original"
    extracted_root = batch_root / "extracted"
    original_root.mkdir(parents=True, exist_ok=False)
    extracted_root.mkdir(parents=True, exist_ok=False)

    errors: list[str] = []
    warnings: list[str] = []
    archive_records: list[dict[str, Any]] = []
    extracted_records: list[dict[str, Any]] = []
    archive_hashes: list[str] = []

    if estate_name.lower() not in KNOWN_ESTATES:
        warnings.append(
            "Estate name is not yet in the configured estate registry."
        )

    for upload in files:
        original_name = Path(upload.filename or "upload.zip").name
        extension = Path(original_name).suffix.lower()

        if extension != ".zip":
            errors.append(f"Only ZIP uploads are accepted: {original_name}")
            continue

        destination = original_root / original_name
        bytes_written = 0

        with destination.open("wb") as output:
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                bytes_written += len(chunk)
                if bytes_written > MAX_ZIP_BYTES:
                    errors.append(
                        f"ZIP exceeds 150 MB limit: {original_name}"
                    )
                    break
                output.write(chunk)

        await upload.close()

        if bytes_written == 0:
            errors.append(f"ZIP is empty: {original_name}")
            destination.unlink(missing_ok=True)
            continue

        if bytes_written > MAX_ZIP_BYTES:
            destination.unlink(missing_ok=True)
            continue

        archive_hash = sha256_file(destination)
        archive_hashes.append(archive_hash)

        archive_records.append(
            {
                "archive_name": original_name,
                "size_bytes": bytes_written,
                "sha256": archive_hash,
            }
        )

        archive_extract_root = extracted_root / slugify(
            Path(original_name).stem
        )

        try:
            extracted_paths = safe_extract_archive(
                destination,
                archive_extract_root,
            )
        except ValueError as exc:
            errors.append(str(exc))
            continue

        for extracted_path in extracted_paths:
            extension = extracted_path.suffix.lower()
            category = classify_source(extracted_path)
            validation_status = "VALID"
            validation_message = None

            if extension not in SUPPORTED_SOURCE_EXTENSIONS:
                validation_status = "ERROR"
                validation_message = (
                    f"Unsupported extracted file type: {extension or 'none'}"
                )
                errors.append(
                    f"Unsupported source file: {extracted_path.name}"
                )
            elif category == "OTHER_TABULAR_SOURCE":
                validation_status = "WARNING"
                validation_message = (
                    "Tabular source was not automatically classified."
                )
                warnings.append(
                    f"Unclassified tabular source: {extracted_path.name}"
                )

            extracted_records.append(
                {
                    "file_id": str(uuid.uuid4()),
                    "archive_name": original_name,
                    "relative_path": str(
                        extracted_path.relative_to(batch_root)
                    ),
                    "file_name": extracted_path.name,
                    "extension": extension,
                    "size_bytes": extracted_path.stat().st_size,
                    "sha256": sha256_file(extracted_path),
                    "category": category,
                    "division_name": detect_division(extracted_path),
                    "validation_status": validation_status,
                    "validation_message": validation_message,
                }
            )

    category_counts: dict[str, int] = {}
    for record in extracted_records:
        category = record["category"]
        category_counts[category] = category_counts.get(category, 0) + 1

    for category in sorted(REQUIRED_CATEGORIES):
        if category_counts.get(category, 0) == 0:
            errors.append(f"Required source category is missing: {category}")

    g_divisions = {
        record["division_name"]
        for record in extracted_records
        if record["category"] == "G_REPORT_PDF"
        and record["division_name"]
    }

    m_divisions = {
        record["division_name"]
        for record in extracted_records
        if record["category"] == "M_FORMAT"
        and record["division_name"]
    }

    if g_divisions and m_divisions and g_divisions != m_divisions:
        warnings.append(
            "G-report and M-format division coverage do not match."
        )

    if not extracted_records:
        errors.append("No supported source files were extracted.")

    package_digest = hashlib.sha256(
        "|".join(sorted(archive_hashes)).encode("utf-8")
    ).hexdigest()

    connection = open_production_db()

    try:
        duplicate = connection.execute(
            """
            SELECT batch_id, status
            FROM upload_batches
            WHERE lower(estate_name) = lower(?)
              AND reporting_month = ?
              AND status IN ('VALIDATED', 'APPROVED_SOURCE')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [estate_name, reporting_month],
        ).fetchone()

        if duplicate:
            errors.append(
                "A validated or approved source batch already exists for "
                f"{estate_name} {reporting_month}: {duplicate[0]}"
            )

        errors = list(dict.fromkeys(errors))
        warnings = list(dict.fromkeys(warnings))

        status = "VALIDATED" if not errors else "VALIDATION_FAILED"

        manifest = {
            "batch_id": batch_id,
            "estate_name": estate_name,
            "reporting_month": reporting_month,
            "archives": archive_records,
            "category_counts": category_counts,
            "g_report_divisions": sorted(g_divisions),
            "m_format_divisions": sorted(m_divisions),
            "files": extracted_records,
        }

        validation = {
            "status": status,
            "errors": errors,
            "warnings": warnings,
        }

        connection.execute(
            """
            INSERT INTO upload_batches VALUES (
                ?, ?, ?, ?, ?, ?, NULL, NULL,
                ?, ?, ?, ?, ?, ?, ?, NULL, NULL
            )
            """,
            [
                batch_id,
                estate_name,
                reporting_month,
                status,
                utc_now(),
                utc_now(),
                len(archive_records),
                len(extracted_records),
                len(errors),
                len(warnings),
                package_digest,
                json.dumps(manifest, ensure_ascii=False),
                json.dumps(validation, ensure_ascii=False),
            ],
        )

        for record in extracted_records:
            connection.execute(
                """
                INSERT INTO uploaded_files VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                [
                    record["file_id"],
                    batch_id,
                    record["archive_name"],
                    record["relative_path"],
                    record["file_name"],
                    record["extension"],
                    record["size_bytes"],
                    record["sha256"],
                    record["category"],
                    record["division_name"],
                    record["validation_status"],
                    record["validation_message"],
                ],
            )

        return {
            "batch_id": batch_id,
            "estate_name": estate_name,
            "reporting_month": reporting_month,
            "status": status,
            "can_approve": status == "VALIDATED",
            "archive_count": len(archive_records),
            "extracted_file_count": len(extracted_records),
            "category_counts": category_counts,
            "g_report_divisions": sorted(g_divisions),
            "m_format_divisions": sorted(m_divisions),
            "errors": errors,
            "warnings": warnings,
            "next_step": (
                f"POST /api/uploads/{batch_id}/approve"
                if status == "VALIDATED"
                else "Correct the errors and upload a new batch."
            ),
        }
    finally:
        connection.close()


@router.get("/{batch_id}")
def get_upload_batch(batch_id: str) -> dict[str, Any]:
    connection = open_production_db()

    try:
        batch = batch_record(connection, batch_id)

        files = connection.execute(
            """
            SELECT
                archive_name,
                relative_path,
                file_name,
                extension,
                size_bytes,
                sha256,
                category,
                division_name,
                validation_status,
                validation_message
            FROM uploaded_files
            WHERE batch_id = ?
            ORDER BY category, file_name
            """,
            [batch_id],
        ).fetchall()

        columns = [
            "archive_name",
            "relative_path",
            "file_name",
            "extension",
            "size_bytes",
            "sha256",
            "category",
            "division_name",
            "validation_status",
            "validation_message",
        ]

        batch["manifest"] = json.loads(batch.pop("manifest_json"))
        batch["validation"] = json.loads(batch.pop("validation_json"))
        batch["files"] = [dict(zip(columns, row)) for row in files]

        return {
            key: json_value(value)
            for key, value in batch.items()
        }
    finally:
        connection.close()


@router.post("/{batch_id}/approve")
def approve_upload_batch(
    batch_id: str,
    payload: ApprovalRequest,
) -> dict[str, Any]:
    if payload.confirm_text.strip().upper() != "APPROVE":
        raise HTTPException(
            status_code=400,
            detail="confirm_text must be exactly APPROVE.",
        )

    connection = open_production_db()

    try:
        batch = batch_record(connection, batch_id)

        if batch["status"] != "VALIDATED":
            raise HTTPException(
                status_code=409,
                detail=(
                    "Only a VALIDATED batch can be approved. "
                    f"Current status: {batch['status']}"
                ),
            )

        existing = connection.execute(
            """
            SELECT batch_id
            FROM upload_batches
            WHERE lower(estate_name) = lower(?)
              AND reporting_month = ?
              AND status = 'APPROVED_SOURCE'
            LIMIT 1
            """,
            [batch["estate_name"], batch["reporting_month"]],
        ).fetchone()

        if existing:
            raise HTTPException(
                status_code=409,
                detail=(
                    "An approved source already exists for this estate and "
                    f"month: {existing[0]}"
                ),
            )

        source = STAGING_ROOT / batch_id
        destination = (
            APPROVED_ROOT
            / slugify(batch["estate_name"])
            / batch["reporting_month"]
            / batch_id
        )

        if not source.exists():
            raise HTTPException(
                status_code=500,
                detail=f"Staging folder is missing: {source}",
            )

        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(destination))

        connection.execute(
            """
            UPDATE upload_batches
            SET status = 'APPROVED_SOURCE',
                approved_at = ?,
                approved_path = ?,
                notes = ?
            WHERE batch_id = ?
            """,
            [
                utc_now(),
                str(destination),
                payload.notes,
                batch_id,
            ],
        )

        return {
            "batch_id": batch_id,
            "status": "APPROVED_SOURCE",
            "estate_name": batch["estate_name"],
            "reporting_month": batch["reporting_month"],
            "approved_path": str(destination),
            "analytics_state": "SOURCE_APPROVED_NOT_PARSED",
            "message": (
                "Source package approved and archived. Business metrics "
                "remain unavailable until the category parsers are enabled."
            ),
        }
    finally:
        connection.close()


@router.post("/{batch_id}/reject")
def reject_upload_batch(
    batch_id: str,
    payload: RejectionRequest,
) -> dict[str, Any]:
    connection = open_production_db()

    try:
        batch = batch_record(connection, batch_id)

        if batch["status"] == "APPROVED_SOURCE":
            raise HTTPException(
                status_code=409,
                detail="An approved source cannot be rejected by this endpoint.",
            )

        source = STAGING_ROOT / batch_id
        destination = REJECTED_ROOT / batch_id

        if source.exists():
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(destination))

        connection.execute(
            """
            UPDATE upload_batches
            SET status = 'REJECTED',
                rejected_at = ?,
                notes = ?
            WHERE batch_id = ?
            """,
            [utc_now(), payload.reason, batch_id],
        )

        return {
            "batch_id": batch_id,
            "status": "REJECTED",
            "reason": payload.reason,
        }
    finally:
        connection.close()

