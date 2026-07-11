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

INSERT INTO courses (id, title, subtitle, icon) VALUES
    ('roadmap', 'Дорожная карта: 12 шагов (live)', '12 шагов системного бизнеса', 'roadmap_icon.png')
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

-- Automated lesson-ingestion pipeline (Этап 1): admin posts a YouTube link,
-- the bot downloads/transcribes/groups it into a draft topic outline here
-- for review, before anything is written to courses/course_data.py.
CREATE TABLE IF NOT EXISTS pending_lessons (
    id                  BIGSERIAL PRIMARY KEY,
    source_youtube_url  TEXT NOT NULL,
    video_id            TEXT NOT NULL,
    video_title         TEXT,
    status              TEXT NOT NULL DEFAULT 'processing'
                         CHECK (status IN (
                             'processing', 'transcribing', 'grouping',
                             'ready_for_review', 'published', 'failed'
                         )),
    created_by          BIGINT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    error_message       TEXT
);

-- Draft topic outline the LLM grouping step produces from the transcript —
-- editable by an admin in the (future) WebApp review screen before publish.
CREATE TABLE IF NOT EXISTS pending_lesson_topics (
    id                 BIGSERIAL PRIMARY KEY,
    pending_lesson_id  BIGINT NOT NULL REFERENCES pending_lessons (id) ON DELETE CASCADE,
    position           INTEGER NOT NULL,
    title              TEXT NOT NULL,
    start_seconds      INTEGER NOT NULL,
    UNIQUE (pending_lesson_id, position)
);

CREATE INDEX IF NOT EXISTS idx_pending_lesson_topics_lesson_id ON pending_lesson_topics (pending_lesson_id);

CREATE TABLE IF NOT EXISTS pending_lesson_transcript (
    id                 BIGSERIAL PRIMARY KEY,
    pending_lesson_id  BIGINT NOT NULL REFERENCES pending_lessons (id) ON DELETE CASCADE,
    position           INTEGER NOT NULL,
    start_seconds      NUMERIC NOT NULL,
    text               TEXT NOT NULL,
    UNIQUE (pending_lesson_id, position)
);

CREATE INDEX IF NOT EXISTS idx_pending_lesson_transcript_lesson_id ON pending_lesson_transcript (pending_lesson_id);

-- Модуль — самостоятельная сущность, объединяющая видео и материалы одного
-- курса под общим заголовком и порядком. Курсы без модулей (bos, roadmap)
-- не затрагиваются: у них db_course_videos.module_id остаётся NULL.
CREATE TABLE IF NOT EXISTS modules (
    id         BIGSERIAL PRIMARY KEY,
    course_id  TEXT NOT NULL REFERENCES courses (id) ON DELETE CASCADE,
    position   INTEGER NOT NULL,
    title      TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (course_id, position)
);

CREATE INDEX IF NOT EXISTS idx_modules_course_id ON modules (course_id);

CREATE TABLE IF NOT EXISTS db_course_videos (
    id         BIGSERIAL PRIMARY KEY,
    course_id  TEXT NOT NULL REFERENCES courses (id) ON DELETE CASCADE,
    position   INTEGER NOT NULL,
    title      TEXT,
    video_id   TEXT NOT NULL,
    module_id  BIGINT REFERENCES modules (id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_db_course_videos_course_id ON db_course_videos (course_id);
CREATE INDEX IF NOT EXISTS idx_db_course_videos_module_id ON db_course_videos (module_id);

-- Old course_id+position uniqueness applies only to non-modular rows;
-- modular courses get their own module-scoped uniqueness so each module can
-- number its items 0,1,2... independently (see schema.sql for the
-- ALTER/DROP CONSTRAINT dance that got an already-deployed DB here).
CREATE UNIQUE INDEX IF NOT EXISTS uq_db_course_videos_course_position
    ON db_course_videos (course_id, position) WHERE module_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_db_course_videos_module_position
    ON db_course_videos (module_id, position) WHERE module_id IS NOT NULL;

-- Не-видео материалы модуля (PDF и т.п.). Пока только 'pdf'.
CREATE TABLE IF NOT EXISTS course_materials (
    id          BIGSERIAL PRIMARY KEY,
    module_id   BIGINT NOT NULL REFERENCES modules (id) ON DELETE CASCADE,
    type        TEXT NOT NULL CHECK (type IN ('pdf')),
    title       TEXT NOT NULL,
    storage_url TEXT NOT NULL,
    position    INTEGER NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (module_id, position)
);

CREATE INDEX IF NOT EXISTS idx_course_materials_module_id ON course_materials (module_id);

CREATE TABLE IF NOT EXISTS db_course_topics (
    id                 BIGSERIAL PRIMARY KEY,
    db_course_video_id BIGINT NOT NULL REFERENCES db_course_videos (id) ON DELETE CASCADE,
    position           INTEGER NOT NULL,
    title              TEXT NOT NULL,
    start_seconds      INTEGER NOT NULL,
    UNIQUE (db_course_video_id, position)
);

CREATE INDEX IF NOT EXISTS idx_db_course_topics_video_id ON db_course_topics (db_course_video_id);

CREATE TABLE IF NOT EXISTS lesson_edit_sessions (
    admin_id           BIGINT PRIMARY KEY,
    pending_lesson_id  BIGINT NOT NULL REFERENCES pending_lessons (id) ON DELETE CASCADE,
    started_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-video resume position ("Продолжить просмотр"), keyed at topic
-- granularity so a finished topic (completed = TRUE) drops out of the
-- continue-watching query while the viewer's next topic becomes the new
-- candidate. Distinct from `stats`, which is topic-segment-relative and
-- feeds only the admin analytics screen — position_seconds here is always
-- absolute within the underlying video, so it can be handed straight to
-- the player's seekTo().
CREATE TABLE IF NOT EXISTS watch_progress (
    user_id          BIGINT NOT NULL REFERENCES users (telegram_id) ON DELETE CASCADE,
    course_id        TEXT NOT NULL REFERENCES courses (id) ON DELETE CASCADE,
    section_key      TEXT NOT NULL DEFAULT '',
    section_label    TEXT NOT NULL,
    topic_idx        INTEGER NOT NULL,
    topic_title      TEXT NOT NULL,
    position_seconds INTEGER NOT NULL DEFAULT 0,
    duration_seconds INTEGER,
    completed        BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, course_id, section_key, topic_idx)
);

CREATE INDEX IF NOT EXISTS idx_watch_progress_user_updated ON watch_progress (user_id, updated_at DESC);
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


async def create_course(
    course_id: str, title: str, subtitle: Optional[str] = None, icon: Optional[str] = None
) -> asyncpg.Record:
    """Create a brand-new, entirely DB-backed course (Этап 2 publish flow,
    mode="new_course"). Caller must have already checked course_id is free —
    no ON CONFLICT here, since silently overwriting an existing course would
    be a bug, not a race worth tolerating for this admin-only, low-volume path."""
    return await _get_pool().fetchrow(
        "INSERT INTO courses (id, title, subtitle, icon) VALUES ($1, $2, $3, $4) RETURNING *",
        course_id,
        title,
        subtitle,
        icon,
    )


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
        SELECT u.telegram_id, u.username, u.phone_number, s.day, s.topic, s.progress, s.updated_at
        FROM stats s
        JOIN users u ON u.telegram_id = s.user_id
        ORDER BY u.telegram_id, s.updated_at
        """
    )


# ─── Watch progress ("Продолжить просмотр") ────────────────────────────


async def record_watch_progress(
    user_id: int,
    course_id: str,
    section_key: str,
    section_label: str,
    topic_idx: int,
    topic_title: str,
    position_seconds: int,
    duration_seconds: Optional[int],
    completed: bool,
) -> asyncpg.Record:
    """Upsert the resume position for (user, course, section, topic).

    Unlike stats.progress (GREATEST-only), position_seconds is overwritten
    on every call — a user who deliberately rewinds should resume from
    where they actually left off, not the furthest point they ever reached.
    """
    return await _get_pool().fetchrow(
        """
        INSERT INTO watch_progress (
            user_id, course_id, section_key, section_label,
            topic_idx, topic_title, position_seconds, duration_seconds,
            completed, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
        ON CONFLICT (user_id, course_id, section_key, topic_idx) DO UPDATE SET
            section_label    = EXCLUDED.section_label,
            topic_title      = EXCLUDED.topic_title,
            position_seconds = EXCLUDED.position_seconds,
            duration_seconds = EXCLUDED.duration_seconds,
            completed        = EXCLUDED.completed,
            updated_at       = now()
        RETURNING *
        """,
        user_id,
        course_id,
        section_key,
        section_label,
        topic_idx,
        topic_title,
        position_seconds,
        duration_seconds,
        completed,
    )


async def get_continue_watching(user_id: int) -> Optional[asyncpg.Record]:
    """The most recently updated not-yet-completed video for `user_id`,
    across all their courses — powers the "Продолжить просмотр" card on the
    "Мои курсы" screen. Restricted to courses the user still has access to,
    so a revoked course never surfaces here."""
    return await _get_pool().fetchrow(
        """
        SELECT wp.course_id, wp.section_key, wp.section_label, wp.topic_idx,
               wp.topic_title, wp.position_seconds, wp.duration_seconds, wp.updated_at,
               c.title AS course_title, c.subtitle AS course_subtitle, c.icon AS course_icon
        FROM watch_progress wp
        JOIN courses c ON c.id = wp.course_id
        WHERE wp.user_id = $1
          AND wp.completed = FALSE
          AND EXISTS (
              SELECT 1 FROM user_course_access uca
              WHERE uca.user_id = wp.user_id AND uca.course_id = wp.course_id
          )
        ORDER BY wp.updated_at DESC
        LIMIT 1
        """,
        user_id,
    )


async def get_admin_stats_summary() -> dict:
    """Raw data behind the admin panel's Статистика screen.

    Reuses get_all_stats() for both the recent-activity feed and the
    per-course engagement classification, rather than re-querying
    stats+users with a near-duplicate join. Classifying which course each
    row's `day` badge belongs to (and each topic's duration, for a
    percent-watched estimate) needs course_data.py's COURSES registry,
    which this module doesn't import — that part happens in bot.py.
    """
    pool = _get_pool()
    total_users = await pool.fetchval("SELECT count(*) FROM users")
    users_with_progress = await pool.fetchval("SELECT count(DISTINCT user_id) FROM stats")
    access_counts = await pool.fetch(
        """
        SELECT c.id AS course_id, c.title, count(uca.user_id) AS access_count
        FROM courses c
        LEFT JOIN user_course_access uca ON uca.course_id = c.id
        GROUP BY c.id, c.title
        ORDER BY c.id
        """
    )
    course_access = await pool.fetch("SELECT user_id, course_id FROM user_course_access")
    all_stats = await get_all_stats()
    return {
        "total_users": total_users,
        "users_with_progress": users_with_progress,
        "access_counts": access_counts,
        "course_access": course_access,
        "all_stats": all_stats,
    }


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


# ─── Pending lessons (automated lesson-ingestion pipeline) ────────────────


async def create_pending_lesson(
    source_youtube_url: str, video_id: str, video_title: Optional[str], created_by: int
) -> asyncpg.Record:
    return await _get_pool().fetchrow(
        """
        INSERT INTO pending_lessons (source_youtube_url, video_id, video_title, created_by)
        VALUES ($1, $2, $3, $4)
        RETURNING *
        """,
        source_youtube_url,
        video_id,
        video_title,
        created_by,
    )


async def update_pending_lesson_status(
    pending_lesson_id: int, status: str, error_message: Optional[str] = None
) -> None:
    await _get_pool().execute(
        "UPDATE pending_lessons SET status = $2, error_message = $3 WHERE id = $1",
        pending_lesson_id,
        status,
        error_message,
    )


async def get_pending_lesson(pending_lesson_id: int) -> Optional[asyncpg.Record]:
    return await _get_pool().fetchrow("SELECT * FROM pending_lessons WHERE id = $1", pending_lesson_id)


async def list_pending_lessons() -> list[asyncpg.Record]:
    return await _get_pool().fetch("SELECT * FROM pending_lessons ORDER BY created_at DESC")


async def add_pending_lesson_topics(pending_lesson_id: int, topics: list[dict]) -> None:
    """Bulk-insert the LLM-grouped draft topic outline, ordered by position."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO pending_lesson_topics (pending_lesson_id, position, title, start_seconds)
                VALUES ($1, $2, $3, $4)
                """,
                [(pending_lesson_id, i, t["title"], t["start_seconds"]) for i, t in enumerate(topics)],
            )


async def get_pending_lesson_topics(pending_lesson_id: int) -> list[asyncpg.Record]:
    return await _get_pool().fetch(
        "SELECT * FROM pending_lesson_topics WHERE pending_lesson_id = $1 ORDER BY position",
        pending_lesson_id,
    )


async def save_pending_lesson_transcript(pending_lesson_id: int, segments: list[dict]) -> None:
    """Persist the raw Whisper transcript (see lesson_pipeline.download_and_transcribe)
    so it outlives the one-shot grouping pass that originally consumed it —
    called once, right after transcription succeeds (see process_pending_lesson
    in bot.py). Assumes no transcript is already stored for this lesson."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO pending_lesson_transcript (pending_lesson_id, position, start_seconds, text)
                VALUES ($1, $2, $3, $4)
                """,
                [(pending_lesson_id, i, seg["start"], seg["text"]) for i, seg in enumerate(segments)],
            )


async def get_pending_lesson_transcript(pending_lesson_id: int) -> list[asyncpg.Record]:
    return await _get_pool().fetch(
        "SELECT * FROM pending_lesson_transcript WHERE pending_lesson_id = $1 ORDER BY position",
        pending_lesson_id,
    )


async def list_pending_lessons_summary() -> list[asyncpg.Record]:
    """Powers the admin panel's "Черновики уроков" list — one row per
    pending lesson with its current topic count, newest first."""
    return await _get_pool().fetch(
        """
        SELECT pl.id, pl.video_title, pl.status, pl.created_at, count(plt.id) AS topic_count
        FROM pending_lessons pl
        LEFT JOIN pending_lesson_topics plt ON plt.pending_lesson_id = pl.id
        GROUP BY pl.id
        ORDER BY pl.created_at DESC
        """
    )


async def delete_pending_lesson(pending_lesson_id: int) -> bool:
    """Permanently delete a pending lesson draft and everything hanging off
    it — pending_lesson_topics, pending_lesson_transcript and
    lesson_edit_sessions all have ON DELETE CASCADE back to pending_lessons
    (see schema.sql), so this single DELETE is enough."""
    result = await _get_pool().execute("DELETE FROM pending_lessons WHERE id = $1", pending_lesson_id)
    return result != "DELETE 0"


async def replace_pending_lesson_topics(pending_lesson_id: int, topics: list[dict]) -> None:
    """Overwrite the full draft topic outline (admin's WebApp editor always
    sends the complete list back, never a partial patch — see PATCH
    /api/admin/pending-lessons/{id}/topics)."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM pending_lesson_topics WHERE pending_lesson_id = $1", pending_lesson_id
            )
            await conn.executemany(
                """
                INSERT INTO pending_lesson_topics (pending_lesson_id, position, title, start_seconds)
                VALUES ($1, $2, $3, $4)
                """,
                [(pending_lesson_id, i, t["title"], t["start_seconds"]) for i, t in enumerate(topics)],
            )


# ─── Published course videos (Этап 2: publish flow) ────────────────────────


async def get_course_videos(course_id: str) -> list[asyncpg.Record]:
    return await _get_pool().fetch(
        "SELECT * FROM db_course_videos WHERE course_id = $1 ORDER BY position", course_id
    )


async def next_course_video_position(course_id: str) -> int:
    return await _get_pool().fetchval(
        "SELECT COALESCE(MAX(position) + 1, 0) FROM db_course_videos WHERE course_id = $1", course_id
    )


async def get_course_video_topics(db_course_video_id: int) -> list[asyncpg.Record]:
    return await _get_pool().fetch(
        "SELECT * FROM db_course_topics WHERE db_course_video_id = $1 ORDER BY position",
        db_course_video_id,
    )


async def add_course_video_with_topics(
    course_id: str,
    position: int,
    title: Optional[str],
    video_id: str,
    topics: list[dict],
    module_id: Optional[int] = None,
) -> asyncpg.Record:
    """Publish a reviewed lesson: one db_course_videos row plus its topic
    outline, inserted together so a course video is never left without any
    topics if the process dies mid-way. `module_id` is optional — omitted
    (NULL) for the existing flat, non-modular courses (bos, roadmap)."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            video = await conn.fetchrow(
                """
                INSERT INTO db_course_videos (course_id, position, title, video_id, module_id)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *
                """,
                course_id,
                position,
                title,
                video_id,
                module_id,
            )
            await conn.executemany(
                """
                INSERT INTO db_course_topics (db_course_video_id, position, title, start_seconds)
                VALUES ($1, $2, $3, $4)
                """,
                [(video["id"], i, t["title"], t["start_seconds"]) for i, t in enumerate(topics)],
            )
            return video


# ─── Course modules (Этап 3: modular courses like "atm") ───────────────────


async def create_module(course_id: str, position: int, title: str) -> asyncpg.Record:
    return await _get_pool().fetchrow(
        "INSERT INTO modules (course_id, position, title) VALUES ($1, $2, $3) RETURNING *",
        course_id,
        position,
        title,
    )


async def list_modules(course_id: str) -> list[asyncpg.Record]:
    return await _get_pool().fetch(
        "SELECT * FROM modules WHERE course_id = $1 ORDER BY position", course_id
    )


async def next_module_item_position(module_id: int) -> int:
    """Next position for a new item (video OR material) in this module —
    a single shared sequence across both tables so the two lists interleave
    into one gap-free, collision-free order when merged at read time."""
    return await _get_pool().fetchval(
        """
        SELECT COALESCE(MAX(pos) + 1, 0) FROM (
            SELECT position AS pos FROM db_course_videos WHERE module_id = $1
            UNION ALL
            SELECT position AS pos FROM course_materials WHERE module_id = $1
        ) combined
        """,
        module_id,
    )


async def add_course_material(
    module_id: int, type_: str, title: str, storage_url: str, position: int
) -> asyncpg.Record:
    return await _get_pool().fetchrow(
        """
        INSERT INTO course_materials (module_id, type, title, storage_url, position)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
        """,
        module_id,
        type_,
        title,
        storage_url,
        position,
    )


async def get_module_contents(module_id: int) -> list[dict]:
    """Videos and materials belonging to one module, merged into a single
    list ordered by their shared `position` sequence (see
    next_module_item_position) and tagged with `type` so the caller doesn't
    need to know which table each item came from."""
    pool = _get_pool()
    videos = await pool.fetch(
        "SELECT * FROM db_course_videos WHERE module_id = $1", module_id
    )
    materials = await pool.fetch(
        "SELECT * FROM course_materials WHERE module_id = $1", module_id
    )
    items = [{"type": "video", **dict(v)} for v in videos]
    items += [dict(row) for row in materials]  # already has its own "type" column
    items.sort(key=lambda it: it["position"])
    return items


# ─── Lesson edit sessions ("edit via chat", Этап 2.1) ──────────────────────


async def start_edit_session(admin_id: int, pending_lesson_id: int) -> None:
    """Begin (or replace) an admin's active edit-via-chat session."""
    await _get_pool().execute(
        """
        INSERT INTO lesson_edit_sessions (admin_id, pending_lesson_id, started_at)
        VALUES ($1, $2, now())
        ON CONFLICT (admin_id) DO UPDATE SET
            pending_lesson_id = $2,
            started_at = now()
        """,
        admin_id,
        pending_lesson_id,
    )


async def get_edit_session(admin_id: int) -> Optional[asyncpg.Record]:
    return await _get_pool().fetchrow("SELECT * FROM lesson_edit_sessions WHERE admin_id = $1", admin_id)


async def end_edit_session(admin_id: int) -> None:
    await _get_pool().execute("DELETE FROM lesson_edit_sessions WHERE admin_id = $1", admin_id)
