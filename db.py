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
    username     TEXT,
    is_admin     BOOLEAN NOT NULL DEFAULT FALSE,
    is_allowed   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT;

CREATE INDEX IF NOT EXISTS idx_users_phone_number ON users (phone_number);

CREATE TABLE IF NOT EXISTS courses (
    id         TEXT PRIMARY KEY,
    title      TEXT NOT NULL,
    subtitle   TEXT,
    icon       TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO courses (id, title, subtitle, icon) VALUES
    ('bos', 'Бизнес Операционная Система', 'Александр Высоцкий', '📚')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS user_course_access (
    user_id    BIGINT NOT NULL REFERENCES users (telegram_id) ON DELETE CASCADE,
    course_id  TEXT NOT NULL REFERENCES courses (id) ON DELETE CASCADE,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    granted_by BIGINT,
    PRIMARY KEY (user_id, course_id)
);

CREATE INDEX IF NOT EXISTS idx_user_course_access_user_id ON user_course_access (user_id);

CREATE TABLE IF NOT EXISTS allowed_phones (
    phone_number TEXT PRIMARY KEY,
    is_admin     BOOLEAN NOT NULL DEFAULT FALSE,
    added_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS allowed_phone_course_access (
    phone_number TEXT NOT NULL REFERENCES allowed_phones (phone_number) ON DELETE CASCADE,
    course_id    TEXT NOT NULL REFERENCES courses (id) ON DELETE CASCADE,
    PRIMARY KEY (phone_number, course_id)
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


async def get_user_by_phone(phone_number: str) -> Optional[asyncpg.Record]:
    return await _get_pool().fetchrow("SELECT * FROM users WHERE phone_number = $1", phone_number)


async def upsert_user(
    telegram_id: int,
    *,
    phone_number: Optional[str] = None,
    username: Optional[str] = None,
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
        INSERT INTO users (telegram_id, phone_number, username, is_admin, is_allowed)
        VALUES ($1, $2, $3, COALESCE($4, FALSE), COALESCE($5, FALSE))
        ON CONFLICT (telegram_id) DO UPDATE SET
            phone_number = COALESCE($2, users.phone_number),
            username     = COALESCE($3, users.username),
            is_admin     = COALESCE($4, users.is_admin),
            is_allowed   = COALESCE($5, users.is_allowed)
        RETURNING *
        """,
        telegram_id,
        phone_number,
        username,
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


async def delete_user(telegram_id: int) -> dict:
    """Permanently delete a user and all their data.

    Deleting the `users` row alone is enough: `user_course_access.user_id`,
    `stats.user_id` and `browser_tokens.telegram_id` all have ON DELETE
    CASCADE back to `users.telegram_id` (see schema.sql), and Postgres
    applies those cascades atomically as part of this single DELETE.
    Wrapped in an explicit transaction anyway so the pre-delete counts
    (used to report what was removed) are consistent with what actually
    gets deleted, even under concurrent access.
    """
    pool = _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            stats_count = await conn.fetchval("SELECT count(*) FROM stats WHERE user_id = $1", telegram_id)
            course_count = await conn.fetchval(
                "SELECT count(*) FROM user_course_access WHERE user_id = $1", telegram_id
            )
            token_count = await conn.fetchval(
                "SELECT count(*) FROM browser_tokens WHERE telegram_id = $1", telegram_id
            )
            result = await conn.execute("DELETE FROM users WHERE telegram_id = $1", telegram_id)
            deleted = result != "DELETE 0"
            return {
                "deleted": deleted,
                "stats_deleted": stats_count if deleted else 0,
                "course_access_deleted": course_count if deleted else 0,
                "browser_tokens_deleted": token_count if deleted else 0,
            }


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


async def list_all_users_with_access() -> list[asyncpg.Record]:
    """All registered users with the list of course_ids each has access to
    (for the admin panel — includes users with no course access yet)."""
    return await _get_pool().fetch(
        """
        SELECT
            u.telegram_id AS user_id,
            u.username,
            u.phone_number,
            u.is_admin,
            u.is_allowed,
            u.created_at,
            COALESCE(
                array_agg(uca.course_id) FILTER (WHERE uca.course_id IS NOT NULL),
                '{}'
            ) AS course_ids
        FROM users u
        LEFT JOIN user_course_access uca ON uca.user_id = u.telegram_id
        GROUP BY u.telegram_id
        ORDER BY u.created_at
        """
    )


# ─── Courses ────────────────────────────────────────────────────────────


async def list_courses() -> list[asyncpg.Record]:
    return await _get_pool().fetch("SELECT * FROM courses ORDER BY id")


async def get_course(course_id: str) -> Optional[asyncpg.Record]:
    return await _get_pool().fetchrow("SELECT * FROM courses WHERE id = $1", course_id)


# ─── Per-user course access ─────────────────────────────────────────────


async def has_course_access(user_id: int, course_id: str) -> bool:
    row = await _get_pool().fetchrow(
        "SELECT 1 FROM user_course_access WHERE user_id = $1 AND course_id = $2",
        user_id,
        course_id,
    )
    return row is not None


async def get_user_courses(user_id: int) -> list[asyncpg.Record]:
    """Courses a user has access to, joined with their display metadata."""
    return await _get_pool().fetch(
        """
        SELECT c.id, c.title, c.subtitle, c.icon
        FROM user_course_access uca
        JOIN courses c ON c.id = uca.course_id
        WHERE uca.user_id = $1
        ORDER BY c.id
        """,
        user_id,
    )


async def grant_course_access(
    user_id: int, course_id: str, granted_by: Optional[int] = None
) -> asyncpg.Record:
    """Grant course access, creating a placeholder user row first if needed
    so the foreign key is always satisfied (mirrors record_stat's pattern)."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO users (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id
            )
            return await conn.fetchrow(
                """
                INSERT INTO user_course_access (user_id, course_id, granted_by)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, course_id) DO UPDATE SET
                    granted_by = COALESCE($3, user_course_access.granted_by)
                RETURNING *
                """,
                user_id,
                course_id,
                granted_by,
            )


async def revoke_course_access(user_id: int, course_id: str) -> bool:
    result = await _get_pool().execute(
        "DELETE FROM user_course_access WHERE user_id = $1 AND course_id = $2",
        user_id,
        course_id,
    )
    return result != "DELETE 0"


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


async def add_allowed_phone_course_access(phone_number: str, course_id: str) -> None:
    """Remember that `course_id` should be granted once this (not-yet-
    registered) phone number's owner first messages the bot — see
    contact_handler, which reads this back via get_allowed_phone_course_ids."""
    await _get_pool().execute(
        """
        INSERT INTO allowed_phone_course_access (phone_number, course_id)
        VALUES ($1, $2)
        ON CONFLICT (phone_number, course_id) DO NOTHING
        """,
        phone_number,
        course_id,
    )


async def get_allowed_phone_course_ids(phone_number: str) -> list[str]:
    rows = await _get_pool().fetch(
        "SELECT course_id FROM allowed_phone_course_access WHERE phone_number = $1",
        phone_number,
    )
    return [r["course_id"] for r in rows]


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
