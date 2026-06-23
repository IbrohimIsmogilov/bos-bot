"""Async PostgreSQL (Supabase) access layer.

Replaces the old `/data/users.json` file, which blocked the asyncio event
loop on every read/write and was wiped entirely whenever `json.load()` raised
an exception. All operations here are atomic, race-condition-free upserts
performed by asyncpg against a connection pool.
"""

import logging
import secrets
from typing import Optional

import asyncpg

from config import DATABASE_URL

logger = logging.getLogger(__name__)

_pool: Optional[asyncpg.Pool] = None

# Mirrors schema.sql — executed on startup so the app works against a fresh
# Supabase database without a manual migration step.
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    telegram_id  BIGINT PRIMARY KEY,
    phone_number TEXT,
    is_admin     BOOLEAN NOT NULL DEFAULT FALSE,
    is_allowed   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_users_phone_number ON users (phone_number);

CREATE TABLE IF NOT EXISTS allowed_phones (
    phone_number TEXT PRIMARY KEY,
    is_admin     BOOLEAN NOT NULL DEFAULT FALSE,
    added_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stats (
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT NOT NULL REFERENCES users (telegram_id) ON DELETE CASCADE,
    day        TEXT NOT NULL,
    topic      TEXT NOT NULL,
    progress   INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, day, topic)
);

CREATE INDEX IF NOT EXISTS idx_stats_user_id ON stats (user_id);

CREATE TABLE IF NOT EXISTS browser_tokens (
    token       TEXT PRIMARY KEY,
    telegram_id BIGINT NOT NULL REFERENCES users (telegram_id) ON DELETE CASCADE,
    day         TEXT,
    topic       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# How long a "Открыть в браузере" token remains valid after creation. Long
# enough to cover a full viewing session (loadCourseData + periodic /api/stats
# reports), since the token is not deleted on use.
BROWSER_TOKEN_TTL = "2 hours"


async def init_pool() -> None:
    global _pool
    ssl_mode = None if any(h in DATABASE_URL for h in ("localhost", "127.0.0.1")) else "require"
    _pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        ssl=ssl_mode,
        min_size=1,
        max_size=5,
        # Supabase's PgBouncer (transaction pooling mode) doesn't keep
        # server-side prepared statements across requests; disabling
        # asyncpg's statement cache makes the pool work with both the
        # pooler and a direct connection.
        statement_cache_size=0,
    )
    async with _pool.acquire() as conn:
        await conn.execute(_SCHEMA_SQL)
    logger.info("Database pool initialized")


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def _get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool is not initialized; call init_pool() first")
    return _pool


# ─── Users ──────────────────────────────────────────────────────────────


async def get_user(telegram_id: int) -> Optional[asyncpg.Record]:
    return await _get_pool().fetchrow("SELECT * FROM users WHERE telegram_id = $1", telegram_id)


async def upsert_user(
    telegram_id: int,
    *,
    phone_number: Optional[str] = None,
    is_admin: Optional[bool] = None,
    is_allowed: Optional[bool] = None,
) -> asyncpg.Record:
    """Create or update a user row.

    Any argument left as `None` is not modified on an existing row (and
    defaults to FALSE/NULL on insert). The INSERT ... ON CONFLICT statement
    is atomic, so concurrent calls for the same telegram_id cannot race.
    """
    return await _get_pool().fetchrow(
        """
        INSERT INTO users (telegram_id, phone_number, is_admin, is_allowed)
        VALUES ($1, $2, COALESCE($3, FALSE), COALESCE($4, FALSE))
        ON CONFLICT (telegram_id) DO UPDATE SET
            phone_number = COALESCE($2, users.phone_number),
            is_admin     = COALESCE($3, users.is_admin),
            is_allowed   = COALESCE($4, users.is_allowed)
        RETURNING *
        """,
        telegram_id,
        phone_number,
        is_admin,
        is_allowed,
    )


async def is_user_admin(telegram_id: int) -> bool:
    row = await get_user(telegram_id)
    return bool(row and row["is_admin"])


async def is_user_allowed(telegram_id: int) -> bool:
    row = await get_user(telegram_id)
    return bool(row and row["is_allowed"])


async def revoke_user(telegram_id: int) -> bool:
    """Remove course access and admin rights from a registered user."""
    result = await _get_pool().execute(
        "UPDATE users SET is_allowed = FALSE, is_admin = FALSE WHERE telegram_id = $1",
        telegram_id,
    )
    return result != "UPDATE 0"


async def set_admin_by_phone(phone_number: str, is_admin: bool) -> bool:
    """Grant/revoke admin rights for an already-registered user by phone."""
    result = await _get_pool().execute(
        "UPDATE users SET is_admin = $2, is_allowed = (is_allowed OR $2) WHERE phone_number = $1",
        phone_number,
        is_admin,
    )
    return result != "UPDATE 0"


async def set_allowed_by_phone(phone_number: str, is_allowed: bool) -> bool:
    """Grant/revoke course access for an already-registered user by phone."""
    result = await _get_pool().execute(
        "UPDATE users SET is_allowed = $2 WHERE phone_number = $1",
        phone_number,
        is_allowed,
    )
    return result != "UPDATE 0"


async def list_users() -> list[asyncpg.Record]:
    return await _get_pool().fetch(
        "SELECT * FROM users WHERE is_allowed OR is_admin ORDER BY created_at"
    )


# ─── Allowed phones (pre-approval before first contact) ───────────────────


async def add_allowed_phone(phone_number: str, is_admin: bool = False) -> asyncpg.Record:
    return await _get_pool().fetchrow(
        """
        INSERT INTO allowed_phones (phone_number, is_admin)
        VALUES ($1, $2)
        ON CONFLICT (phone_number) DO UPDATE SET is_admin = $2
        RETURNING *
        """,
        phone_number,
        is_admin,
    )


async def remove_allowed_phone(phone_number: str) -> bool:
    result = await _get_pool().execute("DELETE FROM allowed_phones WHERE phone_number = $1", phone_number)
    return result != "DELETE 0"


async def get_allowed_phone(phone_number: str) -> Optional[asyncpg.Record]:
    return await _get_pool().fetchrow("SELECT * FROM allowed_phones WHERE phone_number = $1", phone_number)


async def list_allowed_phones() -> list[asyncpg.Record]:
    return await _get_pool().fetch("SELECT * FROM allowed_phones ORDER BY added_at")


# ─── Stats ──────────────────────────────────────────────────────────────


async def record_stat(user_id: int, day: str, topic: str, progress: int) -> asyncpg.Record:
    """Upsert watch progress for (user, day, topic).

    Progress only ever moves forward (GREATEST), and the upsert is a single
    atomic statement, so two concurrent requests for the same topic cannot
    overwrite each other with a stale value.
    """
    return await _get_pool().fetchrow(
        """
        INSERT INTO stats (user_id, day, topic, progress, updated_at)
        VALUES ($1, $2, $3, $4, now())
        ON CONFLICT (user_id, day, topic) DO UPDATE SET
            progress   = GREATEST(stats.progress, EXCLUDED.progress),
            updated_at = now()
        RETURNING *
        """,
        user_id,
        day,
        topic,
        progress,
    )


async def get_user_stats(user_id: int) -> list[asyncpg.Record]:
    return await _get_pool().fetch(
        "SELECT day, topic, progress, updated_at FROM stats WHERE user_id = $1 ORDER BY updated_at",
        user_id,
    )


async def get_all_stats() -> list[asyncpg.Record]:
    return await _get_pool().fetch(
        """
        SELECT u.telegram_id, u.phone_number, s.day, s.topic, s.progress, s.updated_at
        FROM stats s
        JOIN users u ON u.telegram_id = s.user_id
        ORDER BY u.telegram_id, s.updated_at
        """
    )


# ─── Browser tokens ("Открыть в браузере") ─────────────────────────────


async def create_browser_token(telegram_id: int, day: Optional[str], topic: Optional[str]) -> str:
    """Issue a one-time token and opportunistically purge expired ones."""
    token = secrets.token_urlsafe(32)
    pool = _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                f"DELETE FROM browser_tokens WHERE created_at <= now() - interval '{BROWSER_TOKEN_TTL}'"
            )
            await conn.execute(
                "INSERT INTO browser_tokens (token, telegram_id, day, topic) VALUES ($1, $2, $3, $4)",
                token,
                telegram_id,
                day,
                topic,
            )
    return token


async def get_browser_token(token: str) -> Optional[asyncpg.Record]:
    """Validate a browser token without consuming it.

    Returns the row if the token exists and hasn't expired, or None
    otherwise. Tokens are read-only checks (not deleted on use) so the same
    token can authenticate both /api/course and repeated /api/stats calls
    for the rest of the viewing session, until BROWSER_TOKEN_TTL elapses.
    """
    return await _get_pool().fetchrow(
        f"""
        SELECT * FROM browser_tokens
        WHERE token = $1 AND created_at > now() - interval '{BROWSER_TOKEN_TTL}'
        """,
        token,
    )
