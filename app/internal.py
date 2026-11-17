from __future__ import annotations

import re
import time
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from redis.asyncio import Redis

from . import jobs as jobstore
from .config import Settings, get_settings
from .deps import get_redis, get_storage, require_internal_token
from .filestore import FileStoreError
from .storage import Storage
from .schemas import (
    ClaimRequest,
    ClaimResponse,
    CompleteRequest,
    FailRequest,
    ProgressRequest,
    WorkerHeartbeat,
    WorkerIdRequest,
)

router = APIRouter(
    prefix="/internal",
    tags=["internal"],
    dependencies=[Depends(require_internal_token)],
)

@router.post("/workers/heartbeat")
async def heartbeat(
    payload: WorkerHeartbeat,
    settings: Annotated[Settings, Depends(get_settings)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> dict:
    now = time.time()
    key = jobstore.worker_key(payload.worker_id)

    pipe = redis.pipeline()
    pipe.hset(
        key,
        mapping={
            "worker_id": payload.worker_id,
            "load": payload.load,
            "jobs_done": payload.jobs_done,
            "jobs_failed": payload.jobs_failed,
            "version": payload.version,
            "last_seen": f"{now:.3f}",
        },
    )
    pipe.expire(key, max(120, settings.worker_offline_seconds * 4))
    pipe.zadd(jobstore.WORKERS_KEY, {payload.worker_id: now})
    await pipe.execute()

    if payload.active_jobs:
        pipe = redis.pipeline()
        for job_id in payload.active_jobs:
            pipe.expire(jobstore.job_key(job_id), settings.job_ttl_seconds)
        await pipe.execute()

        # Refresh only the leases this worker still owns. This is what lets a
        # dead worker's job be reclaimed after `claim_idle_ms`.
        for job_id in payload.active_jobs:
            owner = await redis.get(jobstore.lease_key(job_id))
            if owner == payload.worker_id:
                await redis.expire(
                    jobstore.lease_key(job_id), settings.lease_ttl_seconds
                )

    return {"ok": True, "server_time": now}


@router.post("/queue/claim", response_model=ClaimResponse)
async def claim(
    payload: ClaimRequest,
    settings: Annotated[Settings, Depends(get_settings)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> dict:
    job = await jobstore.claim_job(
        redis,
        payload.worker_id,
        block_ms=settings.claim_block_ms,
        idle_ms=settings.claim_idle_ms,
        lease_ttl=settings.lease_ttl_seconds,
    )
    return {"job": job}


@router.post("/jobs/{job_id}/progress")
async def progress(
    job_id: str,
    payload: ProgressRequest,
    settings: Annotated[Settings, Depends(get_settings)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> dict:
    data = payload.model_dump(exclude={"worker_id"}, exclude_none=True)
    try:
        result = await jobstore.update_progress(
            redis,
            job_id,
            payload.worker_id,
            data,
            ttl_seconds=settings.job_ttl_seconds,
            lease_ttl=settings.lease_ttl_seconds,
        )
    except jobstore.StaleWorkerError as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "job was reassigned to another worker"
        ) from exc
    if result is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return {"ok": True, **result}


@router.post("/jobs/{job_id}/complete")
async def complete(
    job_id: str,
    payload: CompleteRequest,
    settings: Annotated[Settings, Depends(get_settings)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> dict:
    try:
        raw = await jobstore.complete_job(
            redis,
            job_id,
            payload.worker_id,
            storage_key=payload.storage_key,
            filename=payload.filename,
            filesize=payload.filesize,
            title=payload.title,
            duration=payload.duration,
            thumbnail=payload.thumbnail,
        )
    except jobstore.StaleWorkerError as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "job was reassigned to another worker"
        ) from exc
    if raw is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return {"ok": True, "status": raw.get("status")}


@router.post("/jobs/{job_id}/fail")
async def fail(
    job_id: str,
    payload: FailRequest,
    settings: Annotated[Settings, Depends(get_settings)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> dict:
    try:
        raw = await jobstore.fail_job(
            redis, job_id, payload.worker_id, payload.error
        )
    except jobstore.StaleWorkerError as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "job was reassigned to another worker"
        ) from exc
    if raw is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return {"ok": True, "status": raw.get("status")}


@router.post("/jobs/{job_id}/canceled")
async def canceled(
    job_id: str,
    payload: WorkerIdRequest,
    settings: Annotated[Settings, Depends(get_settings)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> dict:
    try:
        raw = await jobstore.mark_canceled(redis, job_id, payload.worker_id)
    except jobstore.StaleWorkerError as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "job was reassigned to another worker"
        ) from exc
    if raw is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return {"ok": True, "status": raw.get("status")}


@router.get("/queues/health")
async def queue_health(
    settings: Annotated[Settings, Depends(get_settings)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> dict:
    stats = await jobstore.stats_snapshot(
        redis, offline_seconds=settings.worker_offline_seconds
    )
    return {
        "redis": "ok",
        **stats,
        "claim_block_ms": settings.claim_block_ms,
        "claim_idle_ms": settings.claim_idle_ms,
    }


@router.post("/jobs/{job_id}/upload")
async def upload_result(
    job_id: str,
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
    storage: Annotated[Storage, Depends(get_storage)],
    filename: str = Query(min_length=1, max_length=300),
) -> dict:
    """Worker sends the finished file here; the API stores it on this host's disk."""
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", job_id):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid job id")

    safe_name = storage.files.safe_filename(filename)
    try:
        path = storage.files.path_for(job_id, safe_name)
    except FileStoreError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

    declared = request.headers.get("content-length")
    if declared is not None and int(declared) > settings.upload_max_bytes:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "file too large")
    if declared is not None and not storage.files.has_room_for(int(declared)):
        raise HTTPException(status.HTTP_507_INSUFFICIENT_STORAGE, "storage is full")

    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    try:
        with path.open("wb") as handle:
            async for chunk in request.stream():
                written += len(chunk)
                if written > settings.upload_max_bytes:
                    raise HTTPException(
                        status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "file too large"
                    )
                handle.write(chunk)
    except HTTPException:
        path.unlink(missing_ok=True)
        storage.files.delete_job(job_id)
        raise

    return {"ok": True, "storage_key": f"{job_id}/{safe_name}", "size": written}

