from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from . import jobs as jobstore
from .config import get_settings
from .internal import router as internal_router
from .routes import router as public_router
from .security import URLValidationError
from .storage import Storage

logger = logging.getLogger("sokudl.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    redis = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        health_check_interval=30,
        retry_on_timeout=True,
        max_connections=64,
    )
    await redis.ping()
    try:
        await redis.xgroup_create(jobstore.STREAM, jobstore.GROUP, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise

    app.state.redis = redis
    app.state.storage = Storage(settings)
    logger.info(
        "started env=%s storage_dir=%s",
        settings.environment,
        str(app.state.storage.files.root),

    )

    try:
        yield
    finally:
        await redis.aclose()


_settings = get_settings()
_settings.validate_secrets()

app = FastAPI(
    title="SokuDL API",
    version="1.0.0",
    lifespan=lifespan,
    # Public docs are disabled in production so the API surface is not advertised.
    docs_url=None if _settings.is_production else "/docs",
    redoc_url=None,
    openapi_url=None if _settings.is_production else "/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_settings.cors_origins or ["http://localhost:3000"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
    max_age=86400,
)


@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault(
        "Permissions-Policy", "camera=(), microphone=(), geolocation=()"
    )
    return response


@app.middleware("http")
async def kill_switch(request: Request, call_next):
    # Emergency stop: when KILL_SWITCH=true, refuse everything except the health probe.
    if get_settings().kill_switch and request.url.path != "/healthz":
        return JSONResponse(
            status_code=503,
            content={"detail": "service disabled"},
            headers={"Retry-After": "3600"},
        )
    return await call_next(request)


@app.middleware("http")
async def access_log(request: Request, call_next):
    started = time.perf_counter()
    response = await call_next(request)
    if request.url.path not in {"/healthz", "/readyz", "/metrics"}:
        elapsed = (time.perf_counter() - started) * 1000
        logger.info(
            "%s %s -> %s (%.0fms)",
            request.method,
            request.url.path,
            response.status_code,
            elapsed,
        )
    return response


@app.exception_handler(URLValidationError)
async def url_validation_handler(_request: Request, exc: URLValidationError):
    return JSONResponse(status_code=400, content={"detail": str(exc)})


app.include_router(public_router)
app.include_router(internal_router)


@app.get("/", include_in_schema=False)
async def root() -> dict:
    return {
        "service": "SokuDL",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/healthz",
    }


@app.get("/healthz", include_in_schema=False)
async def healthz(request: Request):
    try:
        await request.app.state.redis.ping()
    except Exception:
        return JSONResponse(status_code=503, content={"status": "degraded"})
    return {"status": "ok"}


@app.get("/readyz", include_in_schema=False)
async def readyz(request: Request):
    try:
        await request.app.state.redis.ping()
    except Exception:
        return JSONResponse(status_code=503, content={"status": "degraded"})
    return {"status": "ready"}


@app.get("/metrics", include_in_schema=False)
async def metrics(request: Request) -> PlainTextResponse:
    from .deps import get_settings as _get_settings

    settings = _get_settings()
    stats = await jobstore.stats_snapshot(
        request.app.state.redis,
        offline_seconds=settings.worker_offline_seconds,
    )
    lines = [
        "# HELP sokudl_queue_depth Jobs waiting for a worker.",
        "# TYPE sokudl_queue_depth gauge",
        f"sokudl_queue_depth {stats['queue_depth']}",
        "# HELP sokudl_workers_online Workers with a recent heartbeat.",
        "# TYPE sokudl_workers_online gauge",
        f"sokudl_workers_online {stats['workers_online']}",
        "# HELP sokudl_worker_load Jobs currently running across workers.",
        "# TYPE sokudl_worker_load gauge",
        f"sokudl_worker_load {stats['worker_load']}",
        "# HELP sokudl_jobs_created_total Jobs accepted since startup.",
        "# TYPE sokudl_jobs_created_total counter",
        f"sokudl_jobs_created_total {stats['jobs_created']}",
        "# HELP sokudl_jobs_done_total Jobs completed successfully.",
        "# TYPE sokudl_jobs_done_total counter",
        f"sokudl_jobs_done_total {stats['jobs_done']}",
        "# HELP sokudl_jobs_failed_total Jobs that failed.",
        "# TYPE sokudl_jobs_failed_total counter",
        f"sokudl_jobs_failed_total {stats['jobs_failed']}",
        "# HELP sokudl_jobs_canceled_total Jobs canceled.",
        "# TYPE sokudl_jobs_canceled_total counter",
        f"sokudl_jobs_canceled_total {stats['jobs_canceled']}",
    ]
    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")
