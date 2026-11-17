"""Render entry point for the SokuDL API.

Render passes the port in the PORT environment variable and expects the app
to listen on 0.0.0.0 at that port. This file:

  * starts uvicorn on $PORT (default 8000 when run locally);
  * if REDIS_URL points at a real Redis (Render Key Value, etc.), uses it;
    otherwise starts the built-in Redis-compatible store on 127.0.0.1:16379.

Set the start command on Render to:  python start.py
"""

import os
import socket
import sys
import threading
import time
from urllib.parse import urlparse

BASE = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE)
sys.path.insert(0, BASE)

HOST = "0.0.0.0"
PORT = os.getenv("PORT", "8000")
INTERNAL_REDIS_PORT = int(os.getenv("REDIS_PORT", "16379"))


def log(message: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}", flush=True)


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def ensure_redis() -> None:
    """Use REDIS_URL if it is a real external Redis; otherwise start the built-in one."""
    url = os.getenv("REDIS_URL", "").strip()
    if url:
        host = urlparse(url).hostname or ""
        if host not in ("", "127.0.0.1", "localhost"):
            log("using external Redis from REDIS_URL")
            return
    if _port_open("127.0.0.1", INTERNAL_REDIS_PORT):
        log(f"Redis already on 127.0.0.1:{INTERNAL_REDIS_PORT}")
    else:
        import fakeredis

        server = fakeredis.TcpFakeServer(("127.0.0.1", INTERNAL_REDIS_PORT))
        threading.Thread(target=server.serve_forever, name="builtin-redis", daemon=True).start()
        for _ in range(50):
            if _port_open("127.0.0.1", INTERNAL_REDIS_PORT):
                break
            time.sleep(0.1)
        log(f"built-in Redis-compatible store on 127.0.0.1:{INTERNAL_REDIS_PORT}")
    os.environ["REDIS_URL"] = f"redis://127.0.0.1:{INTERNAL_REDIS_PORT}/0"


def main() -> None:
    ensure_redis()
    log(f"starting API on {HOST}:{PORT}")
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=HOST,
        port=int(PORT),
        proxy_headers=True,
        forwarded_allow_ips=os.getenv("FORWARDED_ALLOW_IPS", "*"),
    )


if __name__ == "__main__":
    main()