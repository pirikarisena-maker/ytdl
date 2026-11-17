from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_name: str = "SokuDL API"
    environment: str = "development"
    log_level: str = "INFO"

    # infrastructure
    redis_url: str = "redis://127.0.0.1:6379/0"
    internal_token: str = "dev-internal-token-change-me"
    ip_hash_salt: str = "dev-salt-change-me"
    trust_proxy: bool = True

    # browser access
    web_origins: str = "http://localhost:3000"

    # local file storage on the API host (the host's own disk)
    storage_dir: str = "/var/lib/sokudl/files"
    storage_max_bytes: int = 2 * 1024**3
    storage_retention_seconds: int = 86400
    # largest single upload a worker may send
    upload_max_bytes: int = 2 * 1024**3

    # limits
    max_jobs_per_ip_per_hour: int = 20
    max_active_jobs_per_ip: int = 2
    max_queue_size: int = 2000
    max_url_length: int = 2048
    allowed_hosts: str = ""
    blocked_hosts: str = ""
    # true = only hosts from the service registry (or ALLOWED_HOSTS) are accepted
    strict_services: bool = False
    # comma separated service slugs hidden from users, e.g. "instagram,facebook"
    disabled_services: str = ""
    # per-service concurrency caps, e.g. "youtube=8,instagram=2"
    service_max_active: str = ""

    # emergency stop: true = every endpoint except /healthz returns 503
    kill_switch: bool = False

    # job lifecycle
    job_ttl_seconds: int = 86400
    worker_offline_seconds: int = 30
    claim_idle_ms: int = 120_000
    claim_block_ms: int = 20_000
    lease_ttl_seconds: int = 120

    @property
    def is_production(self) -> bool:
        return self.environment.lower() == "production"

    def validate_secrets(self) -> None:
        """Refuse to start in production with the placeholder secrets."""
        if not self.is_production:
            return
        weak = {"dev-internal-token-change-me", "dev-salt-change-me", "", "dev-token"}
        if self.internal_token in weak or len(self.internal_token) < 32:
            raise RuntimeError(
                "INTERNAL_TOKEN must be a random value of at least 32 characters in production"
            )
        if self.ip_hash_salt in weak or len(self.ip_hash_salt) < 16:
            raise RuntimeError(
                "IP_HASH_SALT must be a random value of at least 16 characters in production"
            )

    @property
    def cors_origins(self) -> list[str]:
        return [o.strip() for o in self.web_origins.split(",") if o.strip()]

    @property
    def allowed_host_set(self) -> set[str]:
        return {
            h.strip().lower().lstrip(".")
            for h in self.allowed_hosts.split(",")
            if h.strip()
        }

    @property
    def blocked_host_set(self) -> set[str]:
        return {
            h.strip().lower().lstrip(".")
            for h in self.blocked_hosts.split(",")
            if h.strip()
        }

    @property
    def disabled_service_set(self) -> set[str]:
        return {
            s.strip().lower() for s in self.disabled_services.split(",") if s.strip()
        }

    @property
    def service_limits(self) -> dict[str, int]:
        from .services import parse_service_limits

        return parse_service_limits(self.service_max_active)


@lru_cache
def get_settings() -> Settings:
    return Settings()

