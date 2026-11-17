from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

from .config import Settings
from .filestore import FileStore, FileStoreError


class StorageNotConfigured(RuntimeError):
    pass


def content_disposition(filename: str) -> str:
    """RFC 6266 / 5987 content-disposition for unicode filenames."""
    cleaned = "".join(c for c in filename if ord(c) >= 0x20 and c not in '"\r\n')
    ascii_fallback = cleaned.encode("ascii", "ignore").decode("ascii").strip() or "download"
    return (
        f'attachment; filename="{ascii_fallback}"; '
        f"filename*=UTF-8''{quote(cleaned, safe='')}"
    )


class Storage:
    """Host-local file storage. Files live on this host's disk under STORAGE_DIR."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.files = FileStore(settings)
        self.enabled = True

    def download_path(self, key: str) -> Path:
        """Resolve a stored key ('<job_id>/<filename>') to a file on disk."""
        try:
            job_id, _, filename = key.partition("/")
            path = self.files.path_for(job_id, filename)
        except FileStoreError as exc:
            raise StorageNotConfigured(str(exc)) from exc
        if not path.is_file():
            raise StorageNotConfigured("file is missing")
        return path

    def download_url(self, key: str, filename: str) -> str:
        """Public URL served by the website's same-origin API prefix."""
        self.download_path(key)
        return f"/api/v1/files/{quote(key, safe='/')}"
