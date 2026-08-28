from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest

import duckdb
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field


router = APIRouter()

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

OLLAMA_BASE_URL = os.environ.get(
    "OLLAMA_BASE_URL",
    "http://127.0.0.1:11434",
).rstrip("/")

OLLAMA_MODEL = os.environ.get(
    "OLLAMA_MODEL",
    "qwen3:4b",
)


class ChatRequest(BaseModel):
    question: str = Field(
        min_length=2,
        max_length=500,
    )


class ChatResponse(BaseModel):
    answer: str
    model: str
    scope: str
    estate_id: str | None = None
    estate_name: str | None = None
    data_status: str | None = None


def approved_source_count() -> int:
    if not PRODUCTION_DB.exists():
        return 0

    connection = duckdb.connect(
        str(PRODUCTION_DB),
        read_only=True,
    )

    try:
        table_exists = connection.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = 'upload_batches'
            """
        ).fetchone()[0]

        if not table_exists:
            return 0

        return connection.execute(
            """
            SELECT COUNT(*)
            FROM upload_batches
            WHERE status = 'APPROVED_SOURCE'
            """
        ).fetchone()[0]
    finally:
        connection.close()


@router.get("/api/llm/health")
def llm_health() -> dict[str, Any]:
    request = urlrequest.Request(
        f"{OLLAMA_BASE_URL}/api/tags",
        method="GET",
    )

    try:
        with urlrequest.urlopen(request, timeout=15) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urlerror.URLError as exc:
        raise HTTPException(
            status_code=503,
            detail="Ollama is not reachable.",
        ) from exc

    models = [
        model.get("name")
        for model in result.get("models", [])
        if model.get("name")
    ]

    return {
        "status": "healthy",
        "ollama_url": OLLAMA_BASE_URL,
        "configured_model": OLLAMA_MODEL,
        "model_available": OLLAMA_MODEL in models,
        "installed_models": models,
        "data_mode": "PRODUCTION",
        "dummy_data_enabled": False,
    }


@router.post(
    "/api/chat",
    response_model=ChatResponse,
)
def chat(payload: ChatRequest) -> ChatResponse:
    source_count = approved_source_count()

    if source_count == 0:
        answer = (
            "Production mode is active. The previous dummy database is "
            "disabled, and no approved production source package has been "
            "loaded yet."
        )
        status = "NO_APPROVED_SOURCE"
    else:
        answer = (
            f"Production mode is active with {source_count} approved source "
            "package(s). These source files are safely archived, but business "
            "metrics are not available until the category parsers and final "
            "data approval stage are completed."
        )
        status = "SOURCE_APPROVED_NOT_PARSED"

    return ChatResponse(
        answer=answer,
        model="production-gate",
        scope="production",
        data_status=status,
    )

