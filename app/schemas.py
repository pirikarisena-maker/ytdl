from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator

JobMode = Literal["video", "audio"]
JobQuality = Literal["best", "2160", "1440", "1080", "720", "480", "360"]


class JobCreate(BaseModel):
    url: str = Field(min_length=8, max_length=4096)
    mode: JobMode = "video"
    quality: JobQuality = "1080"

    @field_validator("url")
    @classmethod
    def _strip_url(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("URLを入力してください")
        return value


class JobOut(BaseModel):
    id: str
    status: Literal["queued", "running", "done", "failed", "canceled"]
    url: str
    mode: JobMode
    quality: str
    service: str | None = None
    progress: float = 0
    phase: str | None = None
    title: str | None = None
    filename: str | None = None
    filesize: int | None = None
    duration: float | None = None
    thumbnail: str | None = None
    speed: float | None = None
    eta: int | None = None
    error: str | None = None
    download_url: str | None = None
    created_at: float
    started_at: float | None = None
    finished_at: float | None = None


class StatsOut(BaseModel):
    queue_depth: int
    workers_online: int
    worker_load: int
    jobs_created: int
    jobs_done: int
    jobs_failed: int
    jobs_canceled: int


class WorkerHeartbeat(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    load: int = Field(default=0, ge=0, le=10_000)
    active_jobs: list[str] = Field(default_factory=list, max_length=64)
    jobs_done: int = Field(default=0, ge=0)
    jobs_failed: int = Field(default=0, ge=0)
    version: str = Field(default="", max_length=40)

    @field_validator("active_jobs")
    @classmethod
    def _limit_ids(cls, value: list[str]) -> list[str]:
        return [v[:64] for v in value[:64]]


class ClaimRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)


class ClaimResponse(BaseModel):
    job: dict | None = None


class ProgressRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    progress: float | None = Field(default=None, ge=0, le=100)
    phase: str | None = Field(default=None, max_length=32)
    speed: float | None = Field(default=None, ge=0)
    eta: int | None = Field(default=None, ge=0, le=10**9)
    downloaded_bytes: int | None = Field(default=None, ge=0)
    total_bytes: int | None = Field(default=None, ge=0)
    filesize: int | None = Field(default=None, ge=0)
    title: str | None = Field(default=None, max_length=400)
    duration: float | None = Field(default=None, ge=0)
    thumbnail: str | None = Field(default=None, max_length=1200)


class CompleteRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    storage_key: str = Field(min_length=1, max_length=1024)
    filename: str = Field(min_length=1, max_length=300)
    filesize: int = Field(ge=0)
    title: str | None = Field(default=None, max_length=400)
    duration: float | None = Field(default=None, ge=0)
    thumbnail: str | None = Field(default=None, max_length=1200)


class FailRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    error: str = Field(min_length=1, max_length=2000)


class WorkerIdRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
