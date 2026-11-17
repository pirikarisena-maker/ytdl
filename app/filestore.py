"""Local file storage on the API host.

Workers upload finished files over HTTP; the API keeps them on disk and serves
them to browsers. No third-party object storage is required.
"""

from __future__ import annotations

import re
import time
from pathlib import Path

from .config import Settings

_JOB_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]{1,200}$")


class FileStoreError(RuntimeError):
    pass


class FileStore:
    def __init__(self, settings: Settings) -> None:
        self.root = Path(settings.storage_dir).resolve()
        self.max_bytes = settings.storage_max_bytes
        self.retention_seconds = settings.storage_retention_seconds
        self.root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def safe_filename(filename: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9._-]", "_", filename)[:200].strip("._")
        return cleaned or "download"

    def path_for(self, job_id: str, filename: str) -> Path:
        if not _JOB_ID.match(job_id) or not _SAFE_NAME.match(filename):
            raise FileStoreError("invalid file path")
        path = (self.root / job_id / filename).resolve()
        if self.root not in path.parents:
            raise FileStoreError("invalid file path")
        return path

    def used_bytes(self) -> int:
        return sum(p.stat().st_size for p in self.root.rglob("*") if p.is_file())

    def has_room_for(self, size: int) -> bool:
        return self.used_bytes() + size <= self.max_bytes

    def delete_job(self, job_id: str) -> None:
        if not _JOB_ID.match(job_id):
            return
        folder = self.root / job_id
        if folder.is_dir():
            for child in folder.iterdir():
                child.unlink(missing_ok=True)
            folder.rmdir()

    def purge_expired(self) -> int:
        """Delete job folders older than the retention window. Returns the count."""
        cutoff = time.time() - self.retention_seconds
        removed = 0
        for folder in self.root.iterdir():
            if folder.is_dir() and folder.stat().st_mtime < cutoff:
                self.delete_job(folder.name)
                removed += 1
        return removed
