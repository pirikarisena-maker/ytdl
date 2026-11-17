"""URL validation and SSRF protection.

Everything here is stdlib-only so it can be unit tested without FastAPI,
Redis, or network access (tests monkeypatch ``socket.getaddrinfo``).

Note: validating DNS at submit time does not fully protect against DNS
rebinding. The real defence is an egress firewall on the worker network that
blocks RFC1918 / link-local / metadata endpoints. See README.
"""

from __future__ import annotations

import hashlib
import ipaddress
import socket
from urllib.parse import urlsplit, urlunsplit


class URLValidationError(ValueError):
    """Raised when a submitted URL is not allowed."""


_BLOCKED_HOSTNAMES = {
    "localhost",
    "localhost.localdomain",
    "ip6-localhost",
    "ip6-loopback",
    "metadata",
    "metadata.google.internal",
    "metadata.goog",
    "instance-data",
}

_BLOCKED_SUFFIXES = (".localhost", ".local", ".internal", ".home.arpa")


def host_allowed(host: str, allowed_hosts: set[str]) -> bool:
    host = host.lower().rstrip(".")
    for allowed in allowed_hosts:
        allowed = allowed.lower().lstrip(".").rstrip(".")
        if not allowed:
            continue
        if host == allowed or host.endswith("." + allowed):
            return True
    return False


def validate_url_structure(
    raw: str,
    *,
    max_length: int = 2048,
    allowed_hosts: set[str] | None = None,
    blocked_hosts: set[str] | None = None,
) -> tuple[str, str, int]:
    """Validate scheme/userinfo/host syntax.

    Returns ``(normalized_url, ascii_hostname, port)``.
    Does NOT resolve DNS (call :func:`ensure_public_host` for that).
    """
    url = (raw or "").strip()
    if not url:
        raise URLValidationError("URLを入力してください")
    if len(url) > max_length:
        raise URLValidationError("URLが長すぎます")
    if any(ord(c) < 0x20 or ord(c) == 0x7F for c in url):
        raise URLValidationError("URLに使用できない文字が含まれています")

    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        raise URLValidationError("http:// または https:// のURLのみ対応しています")
    if not parts.netloc:
        raise URLValidationError("URLの形式が正しくありません")
    if parts.username or parts.password:
        raise URLValidationError("URLに認証情報を含めることはできません")

    host = parts.hostname
    if not host:
        raise URLValidationError("URLの形式が正しくありません")
    if any(c.isspace() for c in host) or "%" in host:
        raise URLValidationError("URLの形式が正しくありません")

    host = host.rstrip(".").lower()
    try:
        ascii_host = host.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise URLValidationError("ホスト名が正しくありません") from exc

    if ascii_host in _BLOCKED_HOSTNAMES:
        raise URLValidationError("このホストにはアクセスできません")
    if ascii_host.endswith(_BLOCKED_SUFFIXES):
        raise URLValidationError("このホストにはアクセスできません")

    if blocked_hosts and host_allowed(ascii_host, blocked_hosts):
        raise URLValidationError("このサイトは現在ご利用いただけません")

    if allowed_hosts and not host_allowed(ascii_host, allowed_hosts):
        raise URLValidationError("このサイトは現在サポートされていません")

    try:
        port = parts.port
    except ValueError as exc:
        raise URLValidationError("ポート番号が正しくありません") from exc
    if port is None:
        port = 443 if scheme == "https" else 80
    if not (1 <= port <= 65535):
        raise URLValidationError("ポート番号が正しくありません")

    normalized = urlunsplit((scheme, parts.netloc, parts.path or "/", parts.query, ""))
    return normalized, ascii_host, port


def is_public_ip(value: str) -> bool:
    ip = ipaddress.ip_address(value)
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    if (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    ):
        return False
    return ip.is_global


def ensure_public_host(host: str, port: int) -> list[str]:
    """Resolve *host* and reject if any address is not public."""
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise URLValidationError("ホスト名を解決できませんでした") from exc

    if not infos:
        raise URLValidationError("ホスト名を解決できませんでした")

    addresses: list[str] = []
    for info in infos:
        address = info[4][0]
        addresses.append(address)
        try:
            public = is_public_ip(address)
        except ValueError:
            public = False
        if not public:
            raise URLValidationError("内部ネットワークやローカルアドレスは指定できません")

    return addresses


def hash_client_ip(ip: str, salt: str) -> str:
    digest = hashlib.sha256(f"{salt}:{ip}".encode("utf-8")).hexdigest()
    return digest[:24]
