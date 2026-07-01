"""One-off migration: grant every existing user access to the "bos" course.

Run once against Supabase after deploying the multi-course schema (courses,
user_course_access — see schema.sql / db.py's _SCHEMA_SQL, which the bot
also applies automatically on startup). Safe to re-run: ON CONFLICT DO
NOTHING means already-migrated users are left untouched.

    python migrate_course_access.py
"""

import asyncio
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]
COURSE_ID = "bos"


async def main() -> None:
    ssl_mode = None if any(h in DATABASE_URL for h in ("localhost", "127.0.0.1")) else "require"
    conn = await asyncpg.connect(dsn=DATABASE_URL, ssl=ssl_mode)
    try:
        await conn.execute(
            """
            INSERT INTO courses (id, title, subtitle, icon)
            VALUES ($1, 'Бизнес Операционная Система', 'Александр Высоцкий', '📚')
            ON CONFLICT (id) DO NOTHING
            """,
            COURSE_ID,
        )

        result = await conn.execute(
            """
            INSERT INTO user_course_access (user_id, course_id)
            SELECT telegram_id, $1 FROM users
            ON CONFLICT (user_id, course_id) DO NOTHING
            """,
            COURSE_ID,
        )
        print(f"user_course_access insert: {result}")

        total_users = await conn.fetchval("SELECT count(*) FROM users")
        total_access = await conn.fetchval(
            "SELECT count(*) FROM user_course_access WHERE course_id = $1", COURSE_ID
        )
        print(f"users: {total_users}, granted '{COURSE_ID}' access: {total_access}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
