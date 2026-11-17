from __future__ import annotations

import secrets
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status
from redis.asyncio import Redis

from .config import Settings, get_settings
from .security import hash_client_ip
from .storage import Storage


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


def get_storage(request: Request) -> Storage:
    return request.app.state.storage


def client_ip(request: Request, settings: Settings) -> str:
    if settings.trust_proxy:
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        real_ip = request.headers.get("cf-connecting-ip") or request.headers.get(
            "x-real-ip"
        )
        if real_ip:
            return real_ip.strip()
    if request.client:
        return request.client.host
    return "0.0.0.0"


def ip_hash(request: Request, settings: Settings) -> str:
    return hash_client_ip(client_ip(request, settings), settings.ip_hash_salt)


def require_internal_token(
    settings: Annotated[Settings, Depends(get_settings)],
    x_internal_token: Annotated[str | None, Header(alias="X-Internal-Token")] = None,
) -> None:
    if not x_internal_token or not secrets.compare_digest(
        x_internal_token, settings.internal_token
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid internal token",
        )
