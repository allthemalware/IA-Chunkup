"""Shared transfer helpers for ia_upload and ia_download.

Copyright (C) 2026 Whyte. Licensed under GPL-3.0-or-later; see LICENSE.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import requests


NETWORK_ERRORS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)
RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504, 507}
_SECRET_RE = re.compile(r"(LOW\s+)[A-Za-z0-9]+:[A-Za-z0-9]+", re.IGNORECASE)


def safe(text: object) -> str:
    """Strip IA S3 authorization credentials before logging."""
    return _SECRET_RE.sub(r"\1<redacted>", str(text))


def _md5():
    """MD5 is a content checksum here, including on FIPS-enforcing builds."""
    try:
        return hashlib.md5(usedforsecurity=False)
    except TypeError:
        return hashlib.md5()


def md5_of(path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    digest = _md5()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{int(n)}B" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"


def clock(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"
