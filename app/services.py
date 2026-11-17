"""Supported service registry.

The registry is the single source of truth for:

* which hosts are recognised as a known service (used to annotate jobs),
* per-service hints sent to workers (TLS impersonation, extractor tweaks),
* the public ``/api/v1/services`` catalogue rendered by the web UI,
* optional policy enforcement (``STRICT_SERVICES``, ``DISABLED_SERVICES``).

It is stdlib-only so it can be unit tested without FastAPI or Redis.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .security import URLValidationError, host_allowed

CATEGORY_LABELS = {
    "video": "動画",
    "short": "ショート",
    "live": "ライブ",
    "audio": "音声",
    "social": "SNS",
    "jp": "国内",
}


@dataclass(frozen=True)
class Service:
    slug: str
    name: str
    domains: tuple[str, ...]
    category: str
    auth: bool = False
    """Content often requires a cookie file (login) to download."""
    note: str = ""
    options: Mapping[str, Any] = field(default_factory=dict)
    """Hints merged into the yt-dlp options by the worker."""


# Order matters only for the UI listing.
SERVICES: tuple[Service, ...] = (
    # --- general video -------------------------------------------------
    Service("youtube", "YouTube", ("youtube.com", "youtu.be", "youtube-nocookie.com"), "video",
            note="年齢制限・メンバー限定はCookieが必要です"),
    Service("nicovideo", "ニコニコ動画", ("nicovideo.jp", "nico.ms"), "jp"),
    Service("bilibili", "bilibili", ("bilibili.com", "b23.tv"), "video", auth=True,
            note="地域制限・ログインが必要な場合があります"),
    Service("vimeo", "Vimeo", ("vimeo.com",), "video"),
    Service("dailymotion", "Dailymotion", ("dailymotion.com", "dai.ly"), "video"),
    Service("twitch", "Twitch", ("twitch.tv",), "live"),
    Service("rumble", "Rumble", ("rumble.com",), "video"),
    Service("odysee", "Odysee", ("odysee.com",), "video"),
    Service("bitchute", "BitChute", ("bitchute.com",), "video"),
    Service("streamable", "Streamable", ("streamable.com",), "video"),
    Service("vk", "VK", ("vk.com", "vkvideo.ru"), "video"),
    Service("okru", "OK.ru", ("ok.ru",), "video"),
    Service("archive", "Internet Archive", ("archive.org",), "video"),

    # --- shorts / social video -----------------------------------------
    Service("tiktok", "TikTok", ("tiktok.com",), "short",
            options={"impersonate": "chrome"}),
    Service("instagram", "Instagram", ("instagram.com",), "social", auth=True,
            note="ログインCookieがないと取得できないことがあります",
            options={"impersonate": "chrome"}),
    Service("threads", "Threads", ("threads.net", "threads.com"), "social", auth=True,
            options={"impersonate": "chrome"}),
    Service("x", "X (Twitter)", ("x.com", "twitter.com", "t.co"), "social", auth=True,
            note="年齢制限・非公開アカウントはCookieが必要です",
            options={"impersonate": "chrome"}),
    Service("facebook", "Facebook", ("facebook.com", "fb.watch"), "social", auth=True,
            options={"impersonate": "chrome"}),
    Service("reddit", "Reddit", ("reddit.com", "v.redd.it"), "social",
            options={"impersonate": "chrome"}),
    Service("pinterest", "Pinterest", ("pinterest.com", "pin.it"), "social",
            options={"impersonate": "chrome"}),
    Service("imgur", "Imgur", ("imgur.com",), "short"),
    Service("9gag", "9GAG", ("9gag.com",), "short"),
    Service("coub", "Coub", ("coub.com",), "short"),
    Service("tumblr", "Tumblr", ("tumblr.com",), "social"),
    Service("douyin", "Douyin", ("douyin.com", "iesdouyin.com"), "short",
            options={"impersonate": "chrome"}),
    Service("kuaishou", "Kuaishou", ("kuaishou.com",), "short"),
    Service("weibo", "Weibo", ("weibo.com", "weibo.cn"), "social"),

    # --- audio ---------------------------------------------------------
    Service("soundcloud", "SoundCloud", ("soundcloud.com", "snd.sc"), "audio"),
    Service("bandcamp", "Bandcamp", ("bandcamp.com",), "audio"),
    Service("mixcloud", "Mixcloud", ("mixcloud.com",), "audio"),

    # --- Japanese broadcasters -----------------------------------------
    Service("tver", "TVer", ("tver.jp",), "jp", auth=True,
            note="配信期限・ログインが必要な場合があります"),
    Service("abema", "ABEMA", ("abema.tv",), "jp", auth=True),
    Service("nhk", "NHK", ("nhk.or.jp",), "jp"),
    Service("tvasahi", "テレビ朝日", ("tv-asahi.co.jp",), "jp"),
)

SERVICE_BY_SLUG: dict[str, Service] = {s.slug: s for s in SERVICES}

# Domains sorted longest-first so "music.youtube.com" wins over "youtube.com".
_DOMAIN_INDEX: tuple[tuple[str, Service], ...] = tuple(
    sorted(
        ((domain, service) for service in SERVICES for domain in service.domains),
        key=lambda item: len(item[0]),
        reverse=True,
    )
)

REGISTRY_DOMAINS: frozenset[str] = frozenset(
    domain for service in SERVICES for domain in service.domains
)


def detect_service(host: str) -> Service | None:
    """Return the service matching *host*, or ``None`` for generic sites."""
    host = (host or "").lower().rstrip(".")
    if not host:
        return None
    for domain, service in _DOMAIN_INDEX:
        if host == domain or host.endswith("." + domain):
            return service
    return None


def get_service(slug: str | None) -> Service | None:
    return SERVICE_BY_SLUG.get((slug or "").strip().lower())


def service_options(slug: str | None) -> dict[str, Any]:
    """Worker hints for a service (empty dict when unknown)."""
    service = get_service(slug)
    return dict(service.options) if service else {}


def parse_service_limits(raw: str) -> dict[str, int]:
    """Parse ``youtube=6,instagram=2`` into ``{"youtube": 6, ...}``."""
    limits: dict[str, int] = {}
    for chunk in (raw or "").split(","):
        name, _, value = chunk.partition("=")
        name = name.strip().lower()
        if not name:
            continue
        try:
            number = int(value.strip())
        except ValueError:
            continue
        if number > 0:
            limits[name] = min(number, 10_000)
    return limits


def parse_service_list(raw: str) -> set[str]:
    return {part.strip().lower() for part in (raw or "").split(",") if part.strip()}


def public_services(disabled: set[str] | None = None) -> list[dict[str, Any]]:
    disabled = disabled or set()
    return [
        {
            "slug": service.slug,
            "name": service.name,
            "domains": list(service.domains),
            "category": service.category,
            "category_label": CATEGORY_LABELS.get(service.category, service.category),
            "auth": service.auth,
            "note": service.note,
        }
        for service in SERVICES
        if service.slug not in disabled
    ]


def enforce_service_policy(
    host: str,
    service: Service | None,
    *,
    allowed_hosts: set[str] | None = None,
    strict: bool = False,
    disabled: set[str] | None = None,
) -> None:
    """Raise :class:`URLValidationError` when the host is not permitted."""
    if service and disabled and service.slug in disabled:
        raise URLValidationError("このサービスは現在ご利用いただけません")

    if allowed_hosts and not host_allowed(host, allowed_hosts):
        raise URLValidationError("このサイトは現在サポートされていません")

    if strict and service is None:
        raise URLValidationError(
            "対応サイト外のURLです。対応サービス一覧をご確認ください"
        )
