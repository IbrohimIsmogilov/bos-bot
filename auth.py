"""Validation of Telegram WebApp `initData` (Mini Apps auth).

Reference: https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app
"""

import hashlib
import hmac
import json
import time
from typing import Optional
from urllib.parse import parse_qsl

from config import INIT_DATA_MAX_AGE


class InitDataError(Exception):
    """Raised when `initData` is missing, malformed, or fails verification."""


def parse_init_data(init_data: str, bot_token: str, max_age: int = INIT_DATA_MAX_AGE) -> dict:
    """Validate `initData` and return its parsed fields.

    Raises InitDataError if the signature is invalid, expired, or malformed.
    Returns a dict with at least `user` (parsed JSON dict) and `auth_date` (int).
    """
    if not init_data:
        raise InitDataError("empty initData")

    try:
        pairs = dict(parse_qsl(init_data, strict_parsing=True))
    except ValueError as exc:
        raise InitDataError("malformed initData") from exc

    received_hash = pairs.pop("hash", None)
    if not received_hash:
        raise InitDataError("missing hash")

    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))

    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    computed_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(computed_hash, received_hash):
        raise InitDataError("signature mismatch")

    auth_date = int(pairs.get("auth_date", "0"))
    if max_age > 0 and (time.time() - auth_date) > max_age:
        raise InitDataError("initData expired")

    user: Optional[dict] = None
    if "user" in pairs:
        try:
            user = json.loads(pairs["user"])
        except (ValueError, TypeError) as exc:
            raise InitDataError("invalid user payload") from exc

    if not user or "id" not in user:
        raise InitDataError("missing user id")

    pairs["user"] = user
    pairs["auth_date"] = auth_date
    return pairs


def extract_init_data(authorization_header: Optional[str]) -> str:
    """Extract the raw initData string from an `Authorization: tma <initData>` header."""
    if not authorization_header:
        raise InitDataError("missing Authorization header")
    prefix = "tma "
    if not authorization_header.startswith(prefix):
        raise InitDataError("invalid Authorization scheme, expected 'tma'")
    return authorization_header[len(prefix):]
