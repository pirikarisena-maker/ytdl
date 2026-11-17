from __future__ import annotations

import asyncio
import json
import time
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import FileResponse, StreamingResponse
from redis.asyncio import Redis
from starlette.concurrency import run_in_threadpool

from . import jobs as jobstore
from . import ratelimit
from .config import Settings, get_settings
from .deps import get_redis, get_storage, ip_hash
from .schemas import JobCreate, JobOut, StatsOut
from .security import URLValidationError, ensure_public_host, validate_url_structure
from .services import (
    CATEGORY_LABELS,
    detect_service,
    enforce_service_policy,
    public_services,
)
from .storage import Storage, StorageNotConfigured

router = APIRouter(prefix="/api/v1", tags=["public"])


def _public_job(raw: dict[str, str], storage: Storage) -> dict:
    download_url = None
    if raw.get("status") == "done" and raw.get("storage_key"):
        try:
            download_url = storage.download_url(
                raw["storage_key"], raw.get("filename") or "download"
            )
        except StorageNotConfigured:
            download_url = None
    return jobstore.serialize_job(raw, download_url)


@router.post("/jobs", response_model=JobOut, status_code=status.HTTP_202_ACCEPTED)
async def submit_job(
    payload: JobCreate,
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
    redis: Annotated[Redis, Depends(get_redis)],
    storage: Annotated[Storage, Depends(get_storage)],
) -> dict:
    try:
        url, host, port = validate_url_structure(
            payload.url,
            max_length=settings.max_url_length,
            allowed_hosts=None,
            blocked_hosts=settings.blocked_host_set or None,
        )
    except URLValidationError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

    service = detect_service(host)
    try:
        enforce_service_policy(
            host,
            service,
            allowed_hosts=settings.allowed_host_set or None,
            strict=settings.strict_services,
            disabled=settings.disabled_service_set,
        )
    except URLValidationError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

    if await jobstore.queue_depth(redis) >= settings.max_queue_size:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "現在混雑しています。少し時間をおいて再試行してください。",
            headers={"Retry-After": "30"},
        )

    service_slug = service.slug if service else ""
    limit = settings.service_limits.get(service_slug) if service_slug else None
    if limit and await jobstore.service_active(redis, service_slug) >= limit:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            f"{service.name}のジョブが混み合っています。少し時間をおいて再試行してください。",
            headers={"Retry-After": "60"},
        )

    client_hash = ip_hash(request, settings)

    allowed, _count = await ratelimit.hit(
        redis,
        f"sokudl:ratelimit:{client_hash}",
        settings.max_jobs_per_ip_per_hour,
        3600,
    )
    if not allowed:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            f"1時間あたり{settings.max_jobs_per_ip_per_hour}件までです。時間をおいて再試行してください。",
            headers={"Retry-After": "600"},
        )

    active = await ratelimit.count_active_jobs(redis, client_hash)
    if active >= settings.max_active_jobs_per_ip:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "同時に処理できる件数の上限に達しました。完了を待ってから再試行してください。",
            headers={"Retry-After": "30"},
        )

    try:
        await run_in_threadpool(ensure_public_host, host, port)
    except URLValidationError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

    raw = await jobstore.create_job(
        redis,
        url=url,
        mode=payload.mode,
        quality=payload.quality,
        ip_hash=client_hash,
        ttl_seconds=settings.job_ttl_seconds,
        service=service_slug,
    )
    return _public_job(raw, storage)


@router.get("/jobs/{job_id}", response_model=JobOut)
async def get_job(
    job_id: str,
    redis: Annotated[Redis, Depends(get_redis)],
    storage: Annotated[Storage, Depends(get_storage)],
) -> dict:
    raw = await jobstore.get_raw(redis, job_id)
    if raw is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "ジョブが見つかりません")
    return _public_job(raw, storage)


@router.get("/jobs/{job_id}/download")
async def download_job(
    job_id: str,
    redis: Annotated[Redis, Depends(get_redis)],
    storage: Annotated[Storage, Depends(get_storage)],
) -> FileResponse:
    raw = await jobstore.get_raw(redis, job_id)
    if raw is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "ジョブが見つかりません")
    if raw.get("status") != "done" or not raw.get("storage_key"):
        raise HTTPException(status.HTTP_409_CONFLICT, "ダウンロードの準備ができていません")
    try:
        path = storage.download_path(raw["storage_key"])
    except StorageNotConfigured as exc:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            "ファイルが見つかりません",
        ) from exc
    return FileResponse(
        path,
        filename=raw.get("filename") or "download",
        content_disposition_type="attachment",
        headers={"Cache-Control": "private, max-age=3600"},
    )


@router.delete("/jobs/{job_id}", response_model=JobOut)
async def cancel_job(
    job_id: str,
    redis: Annotated[Redis, Depends(get_redis)],
    storage: Annotated[Storage, Depends(get_storage)],
) -> dict:
    raw = await jobstore.request_cancel(redis, job_id)
    if raw is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "ジョブが見つかりません")
    return _public_job(raw, storage)


@router.get("/jobs/{job_id}/events")
async def job_events(
    job_id: str,
    request: Request,
    redis: Annotated[Redis, Depends(get_redis)],
    storage: Annotated[Storage, Depends(get_storage)],
) -> StreamingResponse:
    raw = await jobstore.get_raw(redis, job_id)
    if raw is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "ジョブが見つかりません")

    async def event_stream():
        last_payload: dict | None = None
        idle_ticks = 0
        yield "retry: 3000\n\n"
        while True:
            if await request.is_disconnected():
                break

            current = await jobstore.get_raw(redis, job_id)
            if current is None:
                yield "event: gone\ndata: {}\n\n"
                break

            payload = _public_job(current, storage)
            if payload != last_payload:
                last_payload = payload
                yield (
                    "event: update\n"
                    f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                )
                idle_ticks = 0
                if current.get("status") in jobstore.TERMINAL:
                    break
            else:
                idle_ticks += 1
                if idle_ticks >= 15:
                    idle_ticks = 0
                    yield ": ping\n\n"

            # Queued jobs change less often, so poll them less aggressively.
            await asyncio.sleep(0.75 if current.get("status") == "running" else 2.0)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/stats", response_model=StatsOut)
async def get_stats(
    settings: Annotated[Settings, Depends(get_settings)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> dict:
    return await jobstore.stats_snapshot(
        redis, offline_seconds=settings.worker_offline_seconds
    )


@router.get("/services")
async def list_services(
    settings: Annotated[Settings, Depends(get_settings)],
) -> dict:
    return {
        "services": public_services(settings.disabled_service_set),
        "strict": settings.strict_services,
        "categories": CATEGORY_LABELS,
    }


@router.get("/config")
async def get_config(
    settings: Annotated[Settings, Depends(get_settings)],
) -> dict:
    return {
        "max_url_length": settings.max_url_length,
        "max_active_jobs_per_ip": settings.max_active_jobs_per_ip,
        "max_jobs_per_ip_per_hour": settings.max_jobs_per_ip_per_hour,
        "qualities": ["best", "2160", "1440", "1080", "720", "480", "360"],
        "allowed_hosts": sorted(settings.allowed_host_set),
        "strict_services": settings.strict_services,
        "services": public_services(settings.disabled_service_set),
        "server_time": time.time(),
    }
