from __future__ import annotations

from redis.asyncio import Redis

from .jobs import TERMINAL, active_jobs_key, job_key


async def hit(redis: Redis, key: str, limit: int, window_seconds: int) -> tuple[bool, int]:
    """Atomic fixed-window counter: INCR + EXPIRE only when the key is new."""
    pipe = redis.pipeline(transaction=True)
    pipe.incr(key)
    pipe.expire(key, int(window_seconds), nx=True)
    count, _ttl = await pipe.execute()
    current = int(count)
    return current <= limit, current


async def count_active_jobs(redis: Redis, ip_hash: str) -> int:
    """Number of non-terminal jobs for an IP, pruning finished ones."""
    key = active_jobs_key(ip_hash)
    ids = await redis.smembers(key)
    if not ids:
        return 0

    ids = list(ids)
    pipe = redis.pipeline()
    for job_id in ids:
        pipe.hget(job_key(job_id), "status")
    statuses = await pipe.execute()

    finished = [
        job_id
        for job_id, status in zip(ids, statuses)
        if status is None or status in TERMINAL
    ]
    if finished:
        await redis.srem(key, *finished)

    return len(ids) - len(finished)
