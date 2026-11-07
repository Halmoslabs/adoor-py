"""The server's error envelope, raised as one exception type."""

from __future__ import annotations

import json
from typing import Any, Optional

AUTHORITY_CODES = frozenset({
    "PERMISSIONS_CHANGED", "CONNECTION_CHANGED", "CONNECTION_REVOKED", "WORKSPACE_CHANGED", "FIELD_ACCESS_DENIED",
})


class AdoorError(Exception):
    """A refused or failed call. ``code`` and ``status`` come from the server; ``status`` 0 is a network failure."""

    def __init__(self, code: str, message: str, status: int, details: Any = None,
                 retryable: bool = False, retry_after: Optional[float] = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details = details
        self._retryable = retryable
        self.retry_after = retry_after

    @property
    def retryable(self) -> bool:
        """A retry with the same idempotency key is safe and may succeed later."""
        return self._retryable or self.status == 0 or self.status >= 500 or self.status == 429

    @property
    def authority(self) -> bool:
        """The caller's access ended or changed; a retry needs a new credential or a fresh read."""
        return self.status in (401, 403) or self.code in AUTHORITY_CODES

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


def error_from_body(status: int, raw: bytes) -> AdoorError:
    """Build the typed error from a response body. A body that is not the JSON envelope keeps the HTTP status."""
    try:
        body = json.loads(raw.decode("utf-8")) if raw else None
    except (UnicodeDecodeError, ValueError):
        body = None
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict) and isinstance(error.get("code"), str):
        retry_after = error.get("retryAfter")
        return AdoorError(
            error["code"], str(error.get("message") or error["code"]), status, error.get("details"),
            error.get("retryable") is True,
            float(retry_after) if isinstance(retry_after, (int, float)) else None,
        )
    return AdoorError(f"HTTP_{status}", f"The request failed with HTTP {status}.", status)
