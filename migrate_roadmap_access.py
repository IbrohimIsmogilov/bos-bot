"""One-off migration: grant "roadmap" course access to everyone who already
has "bos" access.

The "Дорожная карта: 12 шагов" video used to be a section nested inside the
BOS course before being split into its own top-level course (see
course_data.py's COURSES registry). Anyone who already had "bos" access
could already watch it, so this backfills "roadmap" access for them too
rather than silently taking it away. Safe to re-run: ON CONFLICT DO NOTHING
means already-migrated users are left untouched.

    python migrate_roadmap_access.py
"""

import asyncio
import os

import asyncpg

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # not needed when env vars are already injected (e.g. `railway run`)

DATABASE_URL = os.environ["DATABASE_URL"]
SOURCE_COURSE_ID = "bos"
TARGET_COURSE_ID = "roadmap"


async def main() -> None:
    ssl_mode = None if any(h in DATABASE_URL for h in ("localhost", "127.0.0.1")) else "require"
    conn = await asyncpg.connect(dsn=DATABASE_URL, ssl=ssl_mode, statement_cache_size=0)
    try:
        await conn.execute(
            """
            INSERT INTO courses (id, title, subtitle, icon)
            VALUES ($1, 'Дорожная карта: 12 шагов (live)', '12 шагов системного бизнеса', 'roadmap_icon.png')
            ON CONFLICT (id) DO NOTHING
            """,
            TARGET_COURSE_ID,
        )

        result = await conn.execute(
            """
            INSERT INTO user_course_access (user_id, course_id)
            SELECT user_id, $2 FROM user_course_access WHERE course_id = $1
            ON CONFLICT (user_id, course_id) DO NOTHING
            """,
            SOURCE_COURSE_ID,
            TARGET_COURSE_ID,
        )
        print(f"user_course_access insert: {result}")

        total_bos = await conn.fetchval(
            "SELECT count(*) FROM user_course_access WHERE course_id = $1", SOURCE_COURSE_ID
        )
        total_roadmap = await conn.fetchval(
            "SELECT count(*) FROM user_course_access WHERE course_id = $1", TARGET_COURSE_ID
        )
        print(f"'{SOURCE_COURSE_ID}' access: {total_bos}, '{TARGET_COURSE_ID}' access: {total_roadmap}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
