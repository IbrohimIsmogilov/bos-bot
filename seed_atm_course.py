"""One-off seed script: creates course "atm" with "Модуль 1" (3 confirmed
videos + 2 PDF materials), using the new modules/course_materials tables.

Not run by the deployed bot — a manual admin script, same style as
migrate_course_access.py / migrate_roadmap_access.py. Safe to re-run only
in the sense that it checks for an existing course_id first; it does NOT
dedupe modules/videos/materials on a second run against the same course.

Uses pg8000 (pure-Python Postgres driver) instead of db.py/asyncpg —
asyncpg's compiled `protocol` extension fails to load locally under this
machine's WDAC policy (same class of failure as google-auth/cryptography
earlier), while pg8000 has no compiled extensions at all.

Usage:  python seed_atm_course.py
"""
import os
import ssl
from urllib.parse import unquote, urlparse

import pg8000
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

COURSE_ID = "atm"
COURSE_TITLE = "АТМ"
COURSE_SUBTITLE = None
COURSE_ICON = None  # emoji or relative image path, see courses.icon in schema.sql

MODULE_TITLE = "Модуль 1"

# Confirmed order from the playlist/DB-diagnosis conversation.
VIDEOS = [
    {"video_id": "SpJm8wTHBF0", "title": "Финмодель от 13.05.2026", "topics": []},
    {"video_id": "gE_IvGMokLU", "title": "1 урок. ЗП от результата. 7.07.2026", "topics": []},
    {
        "video_id": "_MH9vGdxwSo",
        "title": "Дивиденды рост Х2. Метрики бизнеса рост 35-40%. Отзыв о работе с АТМ",
        "topics": [],
    },
]

MATERIALS = [
    {
        "type": "pdf",
        "title": "Интенсив по ЗП - 1 занятие",
        "storage_url": "https://pub-633ad4e98b3c43a1a84f5168e7d6b219.r2.dev/course-materials/atm/pdf/Интенсив по ЗП - 1 занятие.pdf",
    },
    {
        "type": "pdf",
        "title": "Основные KPI",
        # Filename on R2 has a stray "(1)" from a download artifact — title
        # above is what the UI shows, independent of the raw file name.
        "storage_url": "https://pub-633ad4e98b3c43a1a84f5168e7d6b219.r2.dev/course-materials/atm/pdf/Основные KPI (1).pdf",
    },
]


def connect():
    parsed = urlparse(DATABASE_URL)
    # Match db.py's asyncpg ssl="require" behavior for this same pooler
    # host: encrypt the connection but don't verify the certificate chain
    # (Supabase's PgBouncer pooler cert isn't in Python's default trust
    # store — full verification fails with "self-signed certificate in
    # certificate chain" even though the connection itself is legitimate).
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return pg8000.connect(
        user=unquote(parsed.username),
        password=unquote(parsed.password) if parsed.password else None,
        host=parsed.hostname,
        port=parsed.port or 5432,
        database=parsed.path.lstrip("/"),
        ssl_context=ctx,
    )


def next_module_item_position(cur, module_id):
    cur.execute(
        """
        SELECT COALESCE(MAX(pos) + 1, 0) FROM (
            SELECT position AS pos FROM db_course_videos WHERE module_id = %s
            UNION ALL
            SELECT position AS pos FROM course_materials WHERE module_id = %s
        ) combined
        """,
        (module_id, module_id),
    )
    return cur.fetchone()[0]


def main():
    conn = connect()
    cur = conn.cursor()

    cur.execute("SELECT title FROM courses WHERE id = %s", (COURSE_ID,))
    existing = cur.fetchone()
    if existing:
        print(f"Course '{COURSE_ID}' already exists (title={existing[0]!r}) — aborting, nothing written.")
        conn.close()
        return

    cur.execute(
        "INSERT INTO courses (id, title, subtitle, icon) VALUES (%s, %s, %s, %s) RETURNING id, title",
        (COURSE_ID, COURSE_TITLE, COURSE_SUBTITLE, COURSE_ICON),
    )
    course_id, course_title = cur.fetchone()
    print(f"Created course: {course_id} ({course_title})")

    cur.execute(
        "INSERT INTO modules (course_id, position, title) VALUES (%s, %s, %s) RETURNING id, title",
        (COURSE_ID, 0, MODULE_TITLE),
    )
    module_id, module_title = cur.fetchone()
    print(f"Created module: {module_id} ({module_title})")

    for v in VIDEOS:
        pos = next_module_item_position(cur, module_id)
        cur.execute(
            """
            INSERT INTO db_course_videos (course_id, position, title, video_id, module_id)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, video_id, title
            """,
            (COURSE_ID, pos, v["title"], v["video_id"], module_id),
        )
        video_row_id, video_id, video_title = cur.fetchone()
        print(f"  + video [{pos}] {video_id} — {video_title}")

        for i, t in enumerate(v["topics"]):
            cur.execute(
                """
                INSERT INTO db_course_topics (db_course_video_id, position, title, start_seconds)
                VALUES (%s, %s, %s, %s)
                """,
                (video_row_id, i, t["title"], t["start_seconds"]),
            )

    for m in MATERIALS:
        pos = next_module_item_position(cur, module_id)
        cur.execute(
            """
            INSERT INTO course_materials (module_id, type, title, storage_url, position)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, type, title
            """,
            (module_id, m["type"], m["title"], m["storage_url"], pos),
        )
        _material_id, material_type, material_title = cur.fetchone()
        print(f"  + material [{pos}] {material_type} — {material_title}")

    conn.commit()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
