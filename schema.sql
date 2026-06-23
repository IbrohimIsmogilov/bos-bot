-- БОС Курс — схема базы данных (Supabase / PostgreSQL)
-- Выполнить один раз в SQL Editor проекта Supabase.

-- Пользователи бота / WebApp.
-- telegram_id — настоящий Telegram ID, полученный из проверенного initData
-- (никогда из URL), является первичным ключом.
CREATE TABLE IF NOT EXISTS users (
    telegram_id  BIGINT PRIMARY KEY,
    phone_number TEXT,
    is_admin     BOOLEAN NOT NULL DEFAULT FALSE,
    is_allowed   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_users_phone_number ON users (phone_number);

-- Номера телефонов, заранее одобренные администратором до того, как
-- пользователь впервые написал боту и поделился контактом.
CREATE TABLE IF NOT EXISTS allowed_phones (
    phone_number TEXT PRIMARY KEY,
    is_admin     BOOLEAN NOT NULL DEFAULT FALSE,
    added_at     TIMESTAMPTZ NOT NULL DEFAULT now()
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
