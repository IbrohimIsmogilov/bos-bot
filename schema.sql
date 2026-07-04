-- БОС Курс — схема базы данных (Supabase / PostgreSQL)
-- Выполнить один раз в SQL Editor проекта Supabase.

-- Пользователи бота / WebApp.
-- telegram_id — настоящий Telegram ID, полученный из проверенного initData
-- (никогда из URL), является первичным ключом.
CREATE TABLE IF NOT EXISTS users (
    telegram_id  BIGINT PRIMARY KEY,
    phone_number TEXT,
    username     TEXT,
    is_admin     BOOLEAN NOT NULL DEFAULT FALSE,
    is_allowed   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Safe to re-run against an already-deployed database: adds the column
-- backing per-user Telegram @username display in the admin panel.
ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT;

CREATE INDEX IF NOT EXISTS idx_users_phone_number ON users (phone_number);

-- Registry of courses on the BilimBook platform. Course *content* (video
-- IDs, timecodes) lives in course_data.py; this table only holds the
-- metadata needed to list courses in the WebApp's course picker.
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

-- `icon` here is a relative path (served alongside index.html on GitHub
-- Pages) rather than an emoji — the frontend's course-card renderer treats
-- any icon ending in an image extension as an <img>, emoji otherwise.
INSERT INTO courses (id, title, subtitle, icon) VALUES
    ('roadmap', 'Дорожная карта: 12 шагов (live)', '12 шагов системного бизнеса', 'roadmap_icon.png')
ON CONFLICT (id) DO NOTHING;

-- Per-user, per-course entitlements. A row here means the user can fetch
-- that course's content via GET /api/course?course_id=...
CREATE TABLE IF NOT EXISTS user_course_access (
    user_id    BIGINT NOT NULL REFERENCES users (telegram_id) ON DELETE CASCADE,
    course_id  TEXT NOT NULL REFERENCES courses (id) ON DELETE CASCADE,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    granted_by BIGINT,
    PRIMARY KEY (user_id, course_id)
);

CREATE INDEX IF NOT EXISTS idx_user_course_access_user_id ON user_course_access (user_id);

-- Номера телефонов, заранее одобренные администратором до того, как
-- пользователь впервые написал боту и поделился контактом.
CREATE TABLE IF NOT EXISTS allowed_phones (
    phone_number TEXT PRIMARY KEY,
    is_admin     BOOLEAN NOT NULL DEFAULT FALSE,
    added_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Courses an admin pre-selected for a not-yet-registered phone number (see
-- POST /api/admin/add-user-by-phone). Applied to user_course_access once the
-- phone's owner actually messages the bot and shares their contact
-- (contact_handler); falls back to DEFAULT_COURSE_ID if empty, matching the
-- pre-multi-course behavior of a plain /add <phone>.
CREATE TABLE IF NOT EXISTS allowed_phone_course_access (
    phone_number TEXT NOT NULL REFERENCES allowed_phones (phone_number) ON DELETE CASCADE,
    course_id    TEXT NOT NULL REFERENCES courses (id) ON DELETE CASCADE,
    PRIMARY KEY (phone_number, course_id)
);

-- Статистика просмотров. Прогресс по теме сохраняется как максимум
-- (GREATEST) между текущим и новым значением — апсерт атомарен и
-- не подвержен Race Condition при параллельных запросах.
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

-- Short-lived tokens that power "Открыть в браузере" — they let the WebApp
-- page be loaded outside Telegram (where the real Fullscreen API isn't
-- restricted), and let it report stats without initData. Issued by
-- POST /api/browser-token (requires a valid initData). Validated (without
-- being deleted) by GET /api/course?token=... and POST /api/stats?token=...
-- for up to BROWSER_TOKEN_TTL (2 hours) after creation.
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

-- Raw Whisper transcript segments (see lesson_pipeline.download_and_transcribe),
-- saved right after transcription succeeds (status -> 'grouping') so the full
-- text of what was actually said survives past the one-shot LLM grouping pass
-- that originally consumed it — edit_topics_via_instruction can then ground
-- an "edit via chat" instruction in real speech, not just the current topic
-- titles/timecodes.
CREATE TABLE IF NOT EXISTS pending_lesson_transcript (
    id                 BIGSERIAL PRIMARY KEY,
    pending_lesson_id  BIGINT NOT NULL REFERENCES pending_lessons (id) ON DELETE CASCADE,
    position           INTEGER NOT NULL,
    start_seconds      NUMERIC NOT NULL,
    text               TEXT NOT NULL,
    UNIQUE (pending_lesson_id, position)
);

CREATE INDEX IF NOT EXISTS idx_pending_lesson_transcript_lesson_id ON pending_lesson_transcript (pending_lesson_id);

-- Published lesson videos for a course (Этап 2). A course can be entirely
-- DB-backed (no entry in course_data.py's COURSES at all) or a hardcoded
-- multi-day course (e.g. "bos") that gets extra days appended from here —
-- see _build_course_payload in bot.py for how the two are merged.
CREATE TABLE IF NOT EXISTS db_course_videos (
    id         BIGSERIAL PRIMARY KEY,
    course_id  TEXT NOT NULL REFERENCES courses (id) ON DELETE CASCADE,
    position   INTEGER NOT NULL,
    title      TEXT,
    video_id   TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (course_id, position)
);

CREATE INDEX IF NOT EXISTS idx_db_course_videos_course_id ON db_course_videos (course_id);

-- The published (post-review) topic outline for a db_course_videos row —
-- copied from pending_lesson_topics at publish time.
CREATE TABLE IF NOT EXISTS db_course_topics (
    id                 BIGSERIAL PRIMARY KEY,
    db_course_video_id BIGINT NOT NULL REFERENCES db_course_videos (id) ON DELETE CASCADE,
    position           INTEGER NOT NULL,
    title              TEXT NOT NULL,
    start_seconds      INTEGER NOT NULL,
    UNIQUE (db_course_video_id, position)
);

CREATE INDEX IF NOT EXISTS idx_db_course_topics_video_id ON db_course_topics (db_course_video_id);

-- Active "edit via chat" session (Этап 2.1): while a row exists for an
-- admin, their next non-YouTube-link text message is sent to the LLM as an
-- instruction to edit pending_lesson_id's topic list, instead of being
-- silently ignored (see text_message_router in bot.py). One row per admin —
-- starting a new session (even for a different lesson) replaces the old
-- one. Stored in the DB, not in-process state, so it survives a bot restart
-- mid-conversation.
CREATE TABLE IF NOT EXISTS lesson_edit_sessions (
    admin_id           BIGINT PRIMARY KEY,
    pending_lesson_id  BIGINT NOT NULL REFERENCES pending_lessons (id) ON DELETE CASCADE,
    started_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
