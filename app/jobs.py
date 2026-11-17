"""Redis-backed job store and worker queue (Redis Streams consumer group)."""

from __future__ import annotations

import secrets
import time
from typing import Any, Mapping

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from .services import service_options

STREAM = "sokudl:jobs"
GROUP = "workers"

JOB_PREFIX = "sokudl:job:"
LEASE_PREFIX = "sokudl:lease:"
IP_PREFIX = "sokudl:ip:"
WORKERS_KEY = "sokudl:workers"
WORKER_PREFIX = "sokudl:worker:"
STATS_KEY = "sokudl:stats"
SERVICE_PREFIX = "sokudl:svc:"

TERMINAL = {"done", "failed", "canceled"}
ACTIVE = {"queued", "running"}

MAX_ATTEMPTS = 3

# Only these fields can be written by a worker through the progress endpoint.
_PROGRESS_FIELDS = {
    "progress",
    "phase",
    "speed",
    "eta",
    "downloaded_bytes",
    "total_bytes",
    "filesize",
    "title",
    "duration",
    "thumbnail",
}


class StaleWorkerError(RuntimeError):
    """The job was reassigned to another worker."""


def job_key(job_id: str) -> str:
    return f"{JOB_PREFIX}{job_id}"


def lease_key(job_id: str) -> str:
    return f"{LEASE_PREFIX}{job_id}"


def active_jobs_key(ip_hash: str) -> str:
    return f"{IP_PREFIX}{ip_hash}:jobs"


def service_jobs_key(slug: str) -> str:
    return f"{SERVICE_PREFIX}{slug}:active"


def worker_key(worker_id: str) -> str:
    return f"{WORKER_PREFIX}{worker_id}"


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    number = _to_float(value)
    return int(number) if number is not None else None


def serialize_job(raw: Mapping[str, Any], download_url: str | None = None) -> dict:
    status = str(raw.get("status") or "queued")
    return {
        "id": str(raw.get("id") or ""),
        "status": status,
        "url": str(raw.get("url") or ""),
        "mode": str(raw.get("mode") or "video"),
        "quality": str(raw.get("quality") or "1080"),
        "service": raw.get("service") or None,
        "progress": round(_to_float(raw.get("progress")) or 0.0, 1),
        "phase": raw.get("phase") or None,
        "title": raw.get("title") or None,
        "filename": raw.get("filename") or None,
        "filesize": _to_int(raw.get("filesize")),
        "duration": _to_float(raw.get("duration")),
        "thumbnail": raw.get("thumbnail") or None,
        "speed": _to_float(raw.get("speed")),
        "eta": _to_int(raw.get("eta")),
        "error": raw.get("error") or None,
        "download_url": download_url if status == "done" else None,
        "created_at": _to_float(raw.get("created_at")) or 0.0,
        "started_at": _to_float(raw.get("started_at")),
        "finished_at": _to_float(raw.get("finished_at")),
    }


async def get_raw(redis: Redis, job_id: str) -> dict[str, str] | None:
    data = await redis.hgetall(job_key(job_id))
    return data or None


async def create_job(
    redis: Redis,
    *,
    url: str,
    mode: str,
    quality: str,
    ip_hash: str,
    ttl_seconds: int,
    service: str = "",
) -> dict[str, str]:
    job_id = secrets.token_urlsafe(12)
    now = time.time()
    fields = {
        "id": job_id,
        "status": "queued",
        "phase": "queued",
        "url": url,
        "mode": mode,
        "quality": quality,
        "service": service or "",
        "progress": "0",
        "attempts": "0",
        "cancel_requested": "0",
        "ip_hash": ip_hash,
        "created_at": f"{now:.3f}",
        "updated_at": f"{now:.3f}",
    }
    key = job_key(job_id)

    await redis.hset(key, mapping=fields)
    await redis.expire(key, ttl_seconds)
    msg_id = await redis.xadd(
        STREAM,
        {"job_id": job_id},
        maxlen=100_000,
        approximate=True,
    )
    await redis.hset(key, mapping={"stream_id": msg_id})

    pipe = redis.pipeline()
    pipe.sadd(active_jobs_key(ip_hash), job_id)
    pipe.expire(active_jobs_key(ip_hash), max(ttl_seconds, 21_600))
    if service:
        pipe.sadd(service_jobs_key(service), job_id)
        pipe.expire(service_jobs_key(service), max(ttl_seconds, 21_600))
    pipe.hincrby(STATS_KEY, "created", 1)
    await pipe.execute()

    raw = await get_raw(redis, job_id)
    assert raw is not None
    return raw


async def service_active(redis: Redis, service: str) -> int:
    if not service:
        return 0
    return int(await redis.scard(service_jobs_key(service)))


async def queue_depth(redis: Redis) -> int:
    try:
        groups = await redis.xinfo_groups(STREAM)
    except ResponseError:
        return int(await redis.xlen(STREAM))

    for group in groups:
        if group.get("name") == GROUP:
            lag = group.get("lag")
            if lag is not None:
                return max(0, int(lag))
            return int(await redis.xlen(STREAM))
    return int(await redis.xlen(STREAM))


async def _ack(redis: Redis, raw: Mapping[str, Any]) -> None:
    stream_id = raw.get("stream_id")
    if not stream_id:
        return
    try:
        await redis.xack(STREAM, GROUP, stream_id)
        await redis.xdel(STREAM, stream_id)
    except ResponseError:
        pass


async def _try_start(
    redis: Redis,
    msg_id: str,
    fields: Mapping[str, Any] | None,
    worker_id: str,
    lease_ttl: int,
) -> dict | None:
    job_id = (fields or {}).get("job_id")
    if not job_id:
        await redis.xack(STREAM, GROUP, msg_id)
        return None

    raw = await get_raw(redis, job_id)
    if raw is None:
        await redis.xack(STREAM, GROUP, msg_id)
        return None

    status = raw.get("status")
    if status in TERMINAL:
        await redis.xack(STREAM, GROUP, msg_id)
        return None

    if raw.get("cancel_requested") == "1" and status == "queued":
        await _finish(redis, raw, status="canceled", phase="canceled", stat="canceled")
        await redis.xack(STREAM, GROUP, msg_id)
        return None

    lease_owner = await redis.get(lease_key(job_id))
    if lease_owner and lease_owner != worker_id:
        # A live worker owns it. Leave the message pending for a later reclaim.
        return None

    attempts = _to_int(raw.get("attempts")) or 0
    if attempts >= MAX_ATTEMPTS:
        await _finish(
            redis,
            raw,
            status="failed",
            phase="failed",
            stat="failed",
            error="ワーカーが繰り返し停止したため処理を中止しました。時間をおいて再試行してください。",
        )
        await redis.xack(STREAM, GROUP, msg_id)
        return None

    now = time.time()
    started_at = raw.get("started_at") or f"{now:.3f}"
    phase = "retrying" if status == "running" else "starting"
    await redis.hset(
        job_key(job_id),
        mapping={
            "status": "running",
            "worker_id": worker_id,
            "phase": phase,
            "started_at": started_at,
            "updated_at": f"{now:.3f}",
            "attempts": str(attempts + 1),
            "speed": "",
            "eta": "",
        },
    )
    await redis.set(lease_key(job_id), worker_id, ex=lease_ttl)

    return {
        "id": job_id,
        "url": raw.get("url"),
        "mode": raw.get("mode") or "video",
        "quality": raw.get("quality") or "1080",
        "service": raw.get("service") or None,
        "options": service_options(raw.get("service")),
        "attempt": attempts + 1,
    }


async def claim_job(
    redis: Redis,
    worker_id: str,
    *,
    block_ms: int,
    idle_ms: int,
    lease_ttl: int,
) -> dict | None:
    """Long-poll the queue. Returns a job payload or None."""
    try:
        reclaimed = await redis.xautoclaim(
            STREAM,
            GROUP,
            worker_id,
            idle_ms,
            start_id="0-0",
            count=10,
        )
        if reclaimed:
            _next_id, entries, deleted = (list(reclaimed) + [None, None, None])[:3]
            for deleted_id in deleted or []:
                await redis.xack(STREAM, GROUP, deleted_id)
            for msg_id, fields in entries or []:
                if not fields:
                    await redis.xack(STREAM, GROUP, msg_id)
                    continue
                job = await _try_start(redis, msg_id, fields, worker_id, lease_ttl)
                if job:
                    return job
    except ResponseError:
        # NOGROUP etc. Re-created by the app lifespan; fall through to new reads.
        pass

    response = await redis.xreadgroup(
        GROUP,
        worker_id,
        {STREAM: ">"},
        count=10,
        block=block_ms,
    )
    if not response:
        return None

    for _stream, messages in response:
        for msg_id, fields in messages:
            job = await _try_start(redis, msg_id, fields, worker_id, lease_ttl)
            if job:
                return job

    return None


async def update_progress(
    redis: Redis,
    job_id: str,
    worker_id: str,
    data: Mapping[str, Any],
    *,
    ttl_seconds: int,
    lease_ttl: int,
) -> dict | None:
    """Store progress and return {"canceled": bool, "status": str}.

    Returns None when the job no longer exists.
    Raises StaleWorkerError when another worker owns the job.
    """
    raw = await get_raw(redis, job_id)
    if raw is None:
        return None
    if raw.get("worker_id") != worker_id:
        raise StaleWorkerError(job_id)
    if raw.get("status") in TERMINAL:
        return {"canceled": raw.get("status") == "canceled", "status": raw.get("status")}

    mapping: dict[str, Any] = {}
    for field, value in data.items():
        if field not in _PROGRESS_FIELDS or value is None:
            continue
        if field == "phase":
            mapping[field] = str(value)[:32]
        elif field == "title":
            mapping[field] = str(value)[:400]
        elif field == "thumbnail":
            mapping[field] = str(value)[:1200]
        elif field == "progress":
            mapping[field] = f"{max(0.0, min(100.0, float(value))):.1f}"
        elif field in {"downloaded_bytes", "total_bytes", "filesize", "eta"}:
            mapping[field] = str(max(0, int(value)))
        else:
            mapping[field] = str(value)

    now = time.time()
    mapping["updated_at"] = f"{now:.3f}"

    pipe = redis.pipeline()
    pipe.hset(job_key(job_id), mapping=mapping)
    pipe.expire(job_key(job_id), ttl_seconds)
    pipe.expire(lease_key(job_id), lease_ttl)
    await pipe.execute()

    return {
        "canceled": raw.get("cancel_requested") == "1",
        "status": raw.get("status"),
    }


async def _finish(
    redis: Redis,
    raw: Mapping[str, Any],
    *,
    status: str,
    phase: str,
    stat: str,
    extra: Mapping[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, str] | None:
    job_id = raw["id"]
    now = time.time()
    mapping: dict[str, Any] = {
        "status": status,
        "phase": phase,
        "finished_at": f"{now:.3f}",
        "updated_at": f"{now:.3f}",
        "speed": "",
        "eta": "",
    }
    if extra:
        mapping.update({k: v for k, v in extra.items() if v is not None})
    if error is not None:
        mapping["error"] = error[:2000]

    pipe = redis.pipeline()
    pipe.hset(job_key(job_id), mapping=mapping)
    pipe.hincrby(STATS_KEY, stat, 1)
    if raw.get("ip_hash"):
        pipe.srem(active_jobs_key(raw["ip_hash"]), job_id)
    if raw.get("service"):
        pipe.srem(service_jobs_key(raw["service"]), job_id)
    pipe.delete(lease_key(job_id))
    await pipe.execute()

    await _ack(redis, raw)
    return await get_raw(redis, job_id)


async def complete_job(
    redis: Redis,
    job_id: str,
    worker_id: str,
    *,
    storage_key: str,
    filename: str,
    filesize: int,
    title: str | None,
    duration: float | None,
    thumbnail: str | None,
) -> dict[str, str] | None:
    raw = await get_raw(redis, job_id)
    if raw is None:
        return None
    if raw.get("worker_id") != worker_id:
        raise StaleWorkerError(job_id)

    return await _finish(
        redis,
        raw,
        status="done",
        phase="done",
        stat="done",
        extra={
            "progress": "100",
            "storage_key": storage_key,
            "filename": filename,
            "filesize": filesize,
            "title": title,
            "duration": duration,
            "thumbnail": thumbnail,
        },
    )


async def fail_job(
    redis: Redis,
    job_id: str,
    worker_id: str,
    error: str,
) -> dict[str, str] | None:
    raw = await get_raw(redis, job_id)
    if raw is None:
        return None
    if raw.get("worker_id") != worker_id:
        raise StaleWorkerError(job_id)

    return await _finish(
        redis,
        raw,
        status="failed",
        phase="failed",
        stat="failed",
        error=error,
    )


async def mark_canceled(
    redis: Redis,
    job_id: str,
    worker_id: str,
) -> dict[str, str] | None:
    raw = await get_raw(redis, job_id)
    if raw is None:
        return None
    if raw.get("worker_id") != worker_id:
        raise StaleWorkerError(job_id)

    return await _finish(
        redis,
        raw,
        status="canceled",
        phase="canceled",
        stat="canceled",
        extra={"cancel_requested": "1"},
    )


async def request_cancel(redis: Redis, job_id: str) -> dict[str, str] | None:
    raw = await get_raw(redis, job_id)
    if raw is None:
        return None
    if raw.get("status") in TERMINAL:
        return raw

    now = f"{time.time():.3f}"
    if raw.get("status") == "queued":
        await _finish(
            redis,
            raw,
            status="canceled",
            phase="canceled",
            stat="canceled",
            extra={"cancel_requested": "1"},
        )
    else:
        await redis.hset(
            job_key(job_id),
            mapping={"cancel_requested": "1", "updated_at": now},
        )
    return await get_raw(redis, job_id)


async def stats_snapshot(redis: Redis, *, offline_seconds: int) -> dict[str, int]:
    now = time.time()
    cutoff = now - offline_seconds
    pipe = redis.pipeline()
    pipe.zremrangebyscore(WORKERS_KEY, "-inf", cutoff)
    pipe.zrange(WORKERS_KEY, 0, -1)
    pipe.hgetall(STATS_KEY)
    _removed, worker_ids, counters = await pipe.execute()

    worker_load = 0
    if worker_ids:
        pipe = redis.pipeline()
        for worker_id in worker_ids:
            pipe.hmget(worker_key(worker_id), ["load"])
        rows = await pipe.execute()
        for row in rows:
            try:
                worker_load += int((row or [0])[0] or 0)
            except (TypeError, ValueError):
                pass

    return {
        "queue_depth": await queue_depth(redis),
        "workers_online": len(worker_ids or []),
        "worker_load": worker_load,
        "jobs_created": int(counters.get("created", 0)),
        "jobs_done": int(counters.get("done", 0)),
        "jobs_failed": int(counters.get("failed", 0)),
        "jobs_canceled": int(counters.get("canceled", 0)),
    }
