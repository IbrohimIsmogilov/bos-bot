import asyncio
import datetime
import json
import logging
import re

import asyncpg
from aiohttp import web
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

import auth
import db
import lesson_pipeline
from config import ADMIN_ID, ADMIN_USER_IDS, BOT_TOKEN, CEREBRAS_API_KEY, GROQ_API_KEY, PORT, WEBAPP_ORIGIN, WEBAPP_URL
from course_data import COURSES

# Matches a YouTube watch/shorts/short-link URL anywhere in an admin's
# message — see lesson_link_handler (Этап 1 of automated lesson ingestion).
YOUTUBE_URL_RE = re.compile(r"https?://(?:www\.)?(?:youtube\.com/(?:watch\?v=|shorts/)\S+|youtu\.be/\S+)")

# Background pipeline tasks (asyncio.create_task) must be kept referenced
# somewhere, or the event loop is free to garbage-collect them mid-run.
_background_tasks: set[asyncio.Task] = set()

# Course granted automatically to every approved bot member until the
# frontend/bot flows are updated (Этап 2) to let admins pick per-course
# access explicitly. Keeps /add, /remove and the contact flow working
# unchanged for the only course that exists today.
DEFAULT_COURSE_ID = "bos"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# httpx logs the full request URL at INFO level, which includes BOT_TOKEN for
# every call to the Telegram API (.../bot<TOKEN>/getMe etc.) — keep it at WARNING
# so the token never ends up in plaintext logs.
logging.getLogger("httpx").setLevel(logging.WARNING)


# ─── Helpers ────────────────────────────────────────────────────────────


def clean_phone(value) -> str:
    return "".join(c for c in str(value) if c.isdigit())


def is_phone(arg: str) -> bool:
    return len(clean_phone(arg)) >= 9


def parse_arg(args) -> str:
    return "".join(args).strip()


async def is_admin(telegram_id: int) -> bool:
    if telegram_id == ADMIN_ID:
        return True
    return await db.is_user_admin(telegram_id)


async def is_allowed(telegram_id: int) -> bool:
    if await is_admin(telegram_id):
        return True
    return await db.is_user_allowed(telegram_id)


# ─── Telegram handlers ──────────────────────────────────────────────────


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if await is_allowed(user_id):
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("📚 Мои курсы", web_app={"url": WEBAPP_URL})]])
        prefix = "администратор!" if await is_admin(user_id) else "участник!"
        await update.message.reply_text(
            f"✅ Добро пожаловать в BilimBook, {prefix}\n\nНажмите кнопку ниже, чтобы открыть ваши курсы.",
            reply_markup=kb,
        )
        return
    kb = ReplyKeyboardMarkup(
        [[KeyboardButton("📱 Поделиться номером", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await update.message.reply_text(
        "🎓 *Добро пожаловать в BilimBook!*\n\n"
        "BilimBook — образовательная платформа с несколькими курсами для владельцев бизнеса.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "📚 Курс «Бизнес Операционная Система»\n"
        "👤 Автор: Александр Высоцкий\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Этот курс поможет вам:\n"
        "✅ Выстроить систему управления бизнесом\n"
        "✅ Освободиться от операционки\n"
        "✅ Масштабировать компанию без хаоса\n\n"
        "Для получения доступа нажмите кнопку ниже 👇",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def contact_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    contact = update.message.contact
    user_id = update.effective_user.id
    phone = f"+{clean_phone(contact.phone_number)}"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("📚 Мои курсы", web_app={"url": WEBAPP_URL})]])

    allowed_phone = await db.get_allowed_phone(phone)
    if allowed_phone:
        await db.upsert_user(
            user_id,
            phone_number=phone,
            username=update.effective_user.username,
            is_admin=allowed_phone["is_admin"],
            is_allowed=True,
        )
        pending_courses = await db.get_allowed_phone_course_ids(phone)
        for course_id in pending_courses or [DEFAULT_COURSE_ID]:
            await db.grant_course_access(user_id, course_id, granted_by=None)
        if allowed_phone["is_admin"]:
            await update.message.reply_text("✅ Вы вошли как администратор!", reply_markup=ReplyKeyboardRemove())
        else:
            await update.message.reply_text("✅ Доступ открыт!", reply_markup=ReplyKeyboardRemove())
        await update.message.reply_text("Нажмите чтобы начать:", reply_markup=kb)
        return

    existing = await db.get_user(user_id)
    if existing and existing["is_allowed"]:
        await db.upsert_user(user_id, phone_number=phone, username=update.effective_user.username)
        await db.grant_course_access(user_id, DEFAULT_COURSE_ID, granted_by=None)
        await update.message.reply_text("✅ Доступ открыт!", reply_markup=ReplyKeyboardRemove())
        await update.message.reply_text("Нажмите чтобы начать:", reply_markup=kb)
        return

    await update.message.reply_text(
        "🔒 Доступ закрыт.\n\nВаш номер не найден в списке участников.", reply_markup=ReplyKeyboardRemove()
    )


async def add_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        await update.message.reply_text("❌ У вас нет прав администратора.")
        return
    if not ctx.args:
        await update.message.reply_text("Использование: /add +998XXXXXXXXX или /add 123456789")
        return
    arg = parse_arg(ctx.args)
    if is_phone(arg):
        phone = f"+{clean_phone(arg)}"
        if await db.get_allowed_phone(phone):
            await update.message.reply_text(f"⚠️ Номер {phone} уже в списке.")
            return
        await db.add_allowed_phone(phone, is_admin=False)
        await db.set_allowed_by_phone(phone, True)
        await update.message.reply_text(f"✅ Добавлен номер {phone}")
        return
    try:
        tid = int(arg)
    except ValueError:
        await update.message.reply_text("❌ Неверный формат.")
        return
    existing = await db.get_user(tid)
    if existing and existing["is_allowed"]:
        await update.message.reply_text(f"⚠️ ID {tid} уже в списке.")
        return
    await db.upsert_user(tid, is_allowed=True)
    await db.grant_course_access(tid, DEFAULT_COURSE_ID, granted_by=user_id)
    await update.message.reply_text(f"✅ Добавлен Telegram ID {tid}")


async def remove_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        await update.message.reply_text("❌ У вас нет прав администратора.")
        return
    if not ctx.args:
        await update.message.reply_text("Использование: /remove +998XXXXXXXXX или /remove 123456789")
        return
    arg = parse_arg(ctx.args)
    if is_phone(arg):
        phone = f"+{clean_phone(arg)}"
        registered = await db.get_user_by_phone(phone)
        removed_pre = await db.remove_allowed_phone(phone)
        revoked = await db.set_allowed_by_phone(phone, False)
        if registered:
            await db.revoke_course_access(registered["telegram_id"], DEFAULT_COURSE_ID)
        if removed_pre or revoked:
            await update.message.reply_text(f"✅ Удалён номер {phone}")
        else:
            await update.message.reply_text("❌ Номер не найден.")
        return
    try:
        tid = int(arg)
    except ValueError:
        await update.message.reply_text("❌ Неверный формат.")
        return
    existing = await db.get_user(tid)
    if not existing or not existing["is_allowed"]:
        await update.message.reply_text(f"❌ ID {tid} не найден.")
        return
    await db.upsert_user(tid, is_allowed=False)
    await db.revoke_course_access(tid, DEFAULT_COURSE_ID)
    await update.message.reply_text(f"✅ Удалён ID {tid}")


async def list_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        await update.message.reply_text("❌ У вас нет прав.")
        return

    users = await db.list_users()
    allowed_phones = await db.list_allowed_phones()
    registered_phones = {u["phone_number"] for u in users if u["phone_number"]}
    pending_phones = [p for p in allowed_phones if p["phone_number"] not in registered_phones]

    admins = [u for u in users if u["is_admin"]]
    members = [u for u in users if u["is_allowed"] and not u["is_admin"]]

    msg = f"👥 Участников: {len(members) + len(admins)}\n"
    if members or admins:
        msg += "\n🆔 Зарегистрированы:\n"
        for u in members + admins:
            label = u["phone_number"] or str(u["telegram_id"])
            msg += f"  • {label} (ID {u['telegram_id']})\n"
    if pending_phones:
        msg += "\n📱 Ожидают регистрации:\n"
        msg += "\n".join(f"  • {p['phone_number']}" for p in pending_phones) + "\n"
    msg += f"\n🛠 Администраторов: {len(admins)}"
    if admins:
        msg += "\n" + "\n".join(
            f"  • {a['phone_number'] or a['telegram_id']} (ID {a['telegram_id']})" for a in admins
        )
    await update.message.reply_text(msg)


async def add_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Только супер-администратор может назначать администраторов.")
        return
    if not ctx.args:
        await update.message.reply_text("Использование: /addadmin +998XXXXXXXXX или /addadmin 123456789")
        return
    arg = parse_arg(ctx.args)
    if is_phone(arg):
        phone = f"+{clean_phone(arg)}"
        existing = await db.get_allowed_phone(phone)
        if existing and existing["is_admin"]:
            await update.message.reply_text(f"⚠️ {phone} уже администратор.")
            return
        await db.add_allowed_phone(phone, is_admin=True)
        await db.set_admin_by_phone(phone, True)
        await update.message.reply_text(f"✅ Администратор добавлен: {phone}")
        return
    try:
        tid = int(arg)
    except ValueError:
        await update.message.reply_text("❌ Неверный формат.")
        return
    existing = await db.get_user(tid)
    if existing and existing["is_admin"]:
        await update.message.reply_text(f"⚠️ ID {tid} уже администратор.")
        return
    await db.upsert_user(tid, is_admin=True, is_allowed=True)
    await update.message.reply_text(f"✅ Администратор добавлен: ID {tid}")


async def remove_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Только супер-администратор.")
        return
    if not ctx.args:
        await update.message.reply_text("Использование: /removeadmin +998XXXXXXXXX или /removeadmin 123456789")
        return
    arg = parse_arg(ctx.args)
    if is_phone(arg):
        phone = f"+{clean_phone(arg)}"
        existing = await db.get_allowed_phone(phone)
        downgraded_pre = False
        if existing and existing["is_admin"]:
            await db.add_allowed_phone(phone, is_admin=False)
            downgraded_pre = True
        downgraded_user = await db.set_admin_by_phone(phone, False)
        if downgraded_pre or downgraded_user:
            await update.message.reply_text(f"✅ Снят: {phone}")
        else:
            await update.message.reply_text("❌ Не найден.")
        return
    try:
        tid = int(arg)
    except ValueError:
        await update.message.reply_text("❌ Неверный формат.")
        return
    existing = await db.get_user(tid)
    if not existing or not existing["is_admin"]:
        await update.message.reply_text("❌ Не найден.")
        return
    await db.upsert_user(tid, is_admin=False)
    await update.message.reply_text(f"✅ Снят: ID {tid}")


async def stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        await update.message.reply_text("❌ У вас нет прав.")
        return

    rows = await db.get_all_stats()
    if not rows:
        await update.message.reply_text(
            "📊 Статистика пока пуста.\n\nДанные появятся после того как участники откроют видео."
        )
        return

    by_user: dict[int, dict] = {}
    for r in rows:
        u = by_user.setdefault(r["telegram_id"], {"phone": r["phone_number"], "topics": [], "last": None})
        u["topics"].append(r)
        if u["last"] is None or r["updated_at"] > u["last"]["updated_at"]:
            u["last"] = r

    if ctx.args:
        arg = parse_arg(ctx.args)
        match = None
        if is_phone(arg):
            target_phone = f"+{clean_phone(arg)}"
            for tid, u in by_user.items():
                if u["phone"] == target_phone:
                    match = (tid, u)
                    break
        if match is None and arg.lstrip("-").isdigit():
            target_id = int(arg)
            if target_id in by_user:
                match = (target_id, by_user[target_id])
        if not match:
            await update.message.reply_text(f"❌ Нет данных для {arg}")
            return

        tid, u = match
        label = u["phone"] or str(tid)
        msg = (
            f"📊 {label}\n\n"
            f"📚 Просмотрено: {len(u['topics'])} тем\n"
            f"🕐 {u['last']['updated_at'].strftime('%d.%m.%Y %H:%M')}\n"
            f"Последнее: {u['last']['day']} — {u['last']['topic']}\n\nТемы:\n"
        )
        for t in u["topics"][-20:]:
            msg += f"  ✅ {t['day']} — {t['topic']} ({t['progress']}с)\n"
        await update.message.reply_text(msg)
        return

    msg = f"📊 Общая статистика\n👥 Пользователей: {len(by_user)}\n"
    for tid, u in sorted(by_user.items(), key=lambda kv: len(kv[1]["topics"]), reverse=True):
        label = u["phone"] or str(tid)
        msg += "\n━━━━━━━━━━━━━━━━━\n"
        msg += f"👤 {label}\n📚 Просмотрено: {len(u['topics'])} тем\n"
        msg += f"🕐 Последний просмотр: {u['last']['updated_at'].strftime('%d.%m.%Y %H:%M')}\n\nТемы:\n"
        for i, t in enumerate(u["topics"], 1):
            msg += f"  {i}. {t['day']} — {t['topic']}\n"
    if len(msg) > 4000:
        msg = msg[:3900] + "\n..."
    await update.message.reply_text(msg)


async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if user_id == ADMIN_ID:
        text = (
            "🤖 Команды супер-администратора:\n\n"
            "/add +998XXXXXXXXX — добавить участника\n"
            "/remove +998XXXXXXXXX — удалить участника\n"
            "/list — список всех участников\n"
            "/addadmin +998XXXXXXXXX — назначить администратора\n"
            "/removeadmin +998XXXXXXXXX — снять администратора\n"
            "/stats — статистика просмотров\n"
            "/stats +998XXXXXXXXX — статистика конкретного\n"
            "/start — мои курсы"
        )
    elif await is_admin(user_id):
        text = (
            "🤖 Команды администратора:\n\n"
            "/add +998XXXXXXXXX — добавить участника\n"
            "/remove +998XXXXXXXXX — удалить участника\n"
            "/list — список всех участников\n"
            "/stats — статистика просмотров\n"
            "/start — мои курсы"
        )
    else:
        text = "/start — мои курсы"
    await update.message.reply_text(text)


# ─── Automated lesson ingestion (Этап 1: YouTube → transcript → draft topics) ──


def _spawn_background(coro) -> None:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def lesson_link_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """An admin posts a YouTube link -> validate it, create a pending_lessons
    row, and kick off the download/transcribe/group pipeline in the
    background. Silently ignored for non-admins and for messages without a
    YouTube link, so it never interferes with any other text flow."""
    text = update.message.text or ""
    match = YOUTUBE_URL_RE.search(text)
    if not match:
        return
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        return

    url = match.group(0)
    try:
        info = await asyncio.to_thread(lesson_pipeline.probe_video, url)
    except lesson_pipeline.PipelineError as exc:
        await update.message.reply_text(f"❌ Не удалось обработать ссылку: {exc}")
        return

    lesson = await db.create_pending_lesson(url, info["video_id"], info["title"], created_by=user_id)
    await update.message.reply_text(f"⏳ Начал обработку видео «{info['title']}»...")

    _spawn_background(
        process_pending_lesson(lesson["id"], update.effective_chat.id, ctx.bot, url, info["title"])
    )


async def process_pending_lesson(lesson_id: int, chat_id: int, bot, url: str, title: str) -> None:
    """Runs the heavy download/transcribe/group pipeline off the event loop
    (via asyncio.to_thread) and reports progress/errors back to the admin
    who requested it. Any failure updates status=failed with error_message
    instead of leaving the lesson stuck in an earlier in-progress status."""
    try:
        await db.update_pending_lesson_status(lesson_id, "transcribing")
        segments = await asyncio.to_thread(lesson_pipeline.download_and_transcribe, url, GROQ_API_KEY)

        await db.update_pending_lesson_status(lesson_id, "grouping")
        topics = await asyncio.to_thread(lesson_pipeline.group_into_topics, title, segments, CEREBRAS_API_KEY)

        await db.add_pending_lesson_topics(lesson_id, topics)
        await db.update_pending_lesson_status(lesson_id, "ready_for_review")
        await bot.send_message(chat_id, f"✅ Транскрипт готов, {len(topics)} тем найдено.")
    except lesson_pipeline.PipelineError as exc:
        logger.warning("Lesson %s pipeline failed: %s", lesson_id, exc)
        await db.update_pending_lesson_status(lesson_id, "failed", error_message=str(exc)[:500])
        await bot.send_message(chat_id, f"❌ Ошибка обработки видео «{title}»: {exc}")
    except Exception as exc:
        logger.exception("Lesson %s pipeline crashed", lesson_id)
        await db.update_pending_lesson_status(lesson_id, "failed", error_message=str(exc)[:500])
        await bot.send_message(chat_id, f"❌ Непредвиденная ошибка при обработке видео «{title}».")


async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Log unhandled exceptions (e.g. a transient DB outage) and let the user
    know, instead of silently dropping their message."""
    logger.error("Unhandled exception while processing update %s", update, exc_info=ctx.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("⚠️ Временная ошибка сервера. Попробуйте ещё раз позже.")
        except Exception:
            pass


# ─── HTTP API (aiohttp) ─────────────────────────────────────────────────


@web.middleware
async def cors_middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        response: web.StreamResponse = web.Response(status=204)
    else:
        try:
            response = await handler(request)
        except web.HTTPException as exc:
            response = exc
    response.headers["Access-Control-Allow-Origin"] = WEBAPP_ORIGIN
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


async def _authenticate(request: web.Request) -> dict:
    """Validate the `Authorization: tma <initData>` header, return the Telegram user dict."""
    try:
        raw_init_data = auth.extract_init_data(request.headers.get("Authorization"))
        data = auth.parse_init_data(raw_init_data, BOT_TOKEN)
    except auth.InitDataError as exc:
        raise web.HTTPUnauthorized(reason=str(exc))
    return data["user"]


async def _resolve_user_id(request: web.Request) -> int:
    """Resolve the acting Telegram user ID via initData or a `?token=`
    "Открыть в браузере" token (see handle_browser_token)."""
    token = request.query.get("token")
    if token:
        row = await db.get_browser_token(token)
        if not row:
            raise web.HTTPUnauthorized(reason="invalid or expired token")
        return row["telegram_id"]
    user = await _authenticate(request)
    return user["id"]


async def _has_course_access(user_id: int, course_id: str) -> bool:
    """Admins (bot-wide, via is_admin) can open any course; everyone else
    needs an explicit user_course_access row for that course_id."""
    if await is_admin(user_id):
        return True
    return await db.has_course_access(user_id, course_id)


async def _require_admin(user_id: int) -> None:
    if user_id in ADMIN_USER_IDS:
        return
    if await is_admin(user_id):
        return
    raise web.HTTPForbidden(reason="admin access required")


def _row_to_dict(row: asyncpg.Record) -> dict:
    return {k: (v.isoformat() if isinstance(v, datetime.datetime) else v) for k, v in dict(row).items()}


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def handle_course(request: web.Request) -> web.Response:
    # Defaults to the only course that exists today so the not-yet-updated
    # frontend (Этап 2) keeps working unchanged during the transition.
    user_id = await _resolve_user_id(request)

    course_id = request.query.get("course_id", DEFAULT_COURSE_ID)
    if course_id not in COURSES:
        raise web.HTTPNotFound(reason="unknown course_id")
    if not await _has_course_access(user_id, course_id):
        raise web.HTTPForbidden(reason="access denied")
    return web.json_response(COURSES[course_id])


async def handle_my_courses(request: web.Request) -> web.Response:
    user = await _authenticate(request)
    await db.upsert_user(user["id"], username=user.get("username"))
    courses = await db.get_user_courses(user["id"])
    return web.json_response([_row_to_dict(c) for c in courses])


async def handle_browser_token(request: web.Request) -> web.Response:
    """Issue a one-time token for the "Открыть в браузере" link (see handle_course)."""
    user = await _authenticate(request)
    if not await is_allowed(user["id"]):
        raise web.HTTPForbidden(reason="access denied")

    try:
        payload = await request.json()
    except (json.JSONDecodeError, ValueError):
        payload = {}

    day = payload.get("day")
    topic = payload.get("topic")
    day = day.strip()[:200] if isinstance(day, str) and day.strip() else None
    topic = topic.strip()[:300] if isinstance(topic, str) and topic.strip() else None

    token = await db.create_browser_token(user["id"], day, topic)
    return web.json_response({"token": token})


async def handle_stats(request: web.Request) -> web.Response:
    user_id = await _resolve_user_id(request)
    if not await is_allowed(user_id):
        raise web.HTTPForbidden(reason="access denied")

    try:
        payload = await request.json()
    except (json.JSONDecodeError, ValueError):
        raise web.HTTPBadRequest(reason="invalid JSON body")

    day = payload.get("day")
    topic = payload.get("topic")
    progress = payload.get("progress")
    if not isinstance(day, str) or not isinstance(topic, str) or not isinstance(progress, (int, float)):
        raise web.HTTPBadRequest(reason="day, topic and progress are required")
    if not day.strip() or not topic.strip():
        raise web.HTTPBadRequest(reason="day and topic must not be empty")

    progress_seconds = max(0, min(int(progress), 100_000))

    # Ensure a `users` row exists so the FK on `stats.user_id` is satisfied
    # even for the super-admin, who may never have triggered an upsert before.
    await db.upsert_user(user_id)
    await db.record_stat(user_id, day.strip()[:200], topic.strip()[:300], progress_seconds)
    return web.json_response({"ok": True})


# ─── Admin API (BilimBook admin panel) ──────────────────────────────────


async def handle_admin_users(request: web.Request) -> web.Response:
    user = await _authenticate(request)
    await _require_admin(user["id"])
    rows = await db.list_all_users_with_access()
    return web.json_response([_row_to_dict(r) for r in rows])


async def handle_admin_grant_access(request: web.Request) -> web.Response:
    admin_user = await _authenticate(request)
    await _require_admin(admin_user["id"])

    try:
        payload = await request.json()
    except (json.JSONDecodeError, ValueError):
        raise web.HTTPBadRequest(reason="invalid JSON body")

    target_user_id = payload.get("user_id")
    course_id = payload.get("course_id")
    grant = payload.get("grant")
    if not isinstance(target_user_id, int) or not isinstance(course_id, str) or not isinstance(grant, bool):
        raise web.HTTPBadRequest(reason="user_id (int), course_id (str) and grant (bool) are required")

    if not await db.get_course(course_id):
        raise web.HTTPNotFound(reason="unknown course_id")

    if grant:
        await db.grant_course_access(target_user_id, course_id, granted_by=admin_user["id"])
    else:
        await db.revoke_course_access(target_user_id, course_id)
    return web.json_response({"ok": True})


async def _validate_course_ids(course_ids) -> list:
    """Validate an optional `course_ids` request field: must be a list of
    strings, each naming a real course. Returns [] if omitted entirely."""
    if course_ids is None:
        return []
    if not isinstance(course_ids, list) or not all(isinstance(c, str) for c in course_ids):
        raise web.HTTPBadRequest(reason="course_ids must be a list of strings")
    for course_id in course_ids:
        if not await db.get_course(course_id):
            raise web.HTTPNotFound(reason=f"unknown course_id: {course_id}")
    return course_ids


async def handle_admin_add_user_by_id(request: web.Request) -> web.Response:
    admin_user = await _authenticate(request)
    await _require_admin(admin_user["id"])

    try:
        payload = await request.json()
    except (json.JSONDecodeError, ValueError):
        raise web.HTTPBadRequest(reason="invalid JSON body")

    target_user_id = payload.get("user_id")
    if not isinstance(target_user_id, int):
        raise web.HTTPBadRequest(reason="user_id (int) is required")
    course_ids = await _validate_course_ids(payload.get("course_ids"))

    await db.upsert_user(target_user_id, is_allowed=True)
    for course_id in course_ids:
        await db.grant_course_access(target_user_id, course_id, granted_by=admin_user["id"])
    return web.json_response({"ok": True})


async def handle_admin_add_user_by_phone(request: web.Request) -> web.Response:
    admin_user = await _authenticate(request)
    await _require_admin(admin_user["id"])

    try:
        payload = await request.json()
    except (json.JSONDecodeError, ValueError):
        raise web.HTTPBadRequest(reason="invalid JSON body")

    raw_phone = payload.get("phone_number")
    if not isinstance(raw_phone, str) or not is_phone(raw_phone):
        raise web.HTTPBadRequest(reason="a valid phone_number is required")
    course_ids = await _validate_course_ids(payload.get("course_ids"))

    phone = f"+{clean_phone(raw_phone)}"
    await db.add_allowed_phone(phone, is_admin=False)
    await db.set_allowed_by_phone(phone, True)
    for course_id in course_ids:
        await db.add_allowed_phone_course_access(phone, course_id)

    # If this phone already belongs to a registered user, grant immediately
    # instead of only waiting on a future contact share (which may never
    # come again for someone who's already been through that flow once).
    existing = await db.get_user_by_phone(phone)
    if existing:
        for course_id in course_ids:
            await db.grant_course_access(existing["telegram_id"], course_id, granted_by=admin_user["id"])
    return web.json_response({"ok": True})


async def handle_admin_delete_user(request: web.Request) -> web.Response:
    admin_user = await _authenticate(request)
    await _require_admin(admin_user["id"])

    try:
        payload = await request.json()
    except (json.JSONDecodeError, ValueError):
        raise web.HTTPBadRequest(reason="invalid JSON body")

    target_user_id = payload.get("user_id")
    if not isinstance(target_user_id, int):
        raise web.HTTPBadRequest(reason="user_id (int) is required")

    result = await db.delete_user(target_user_id)
    if not result["deleted"]:
        raise web.HTTPNotFound(reason="user not found")
    return web.json_response(result)


# `stats.day`/`stats.topic` are free-text badges (see reportStats() in
# main.js), not a course_id — this maps them back to a course + topic list
# so the admin stats screen can classify historical rows and estimate a
# percent-watched from each topic's known duration in course_data.py.
def _build_badge_lookup() -> dict:
    lookup = {}
    bos = COURSES.get("bos", {})
    for day in bos.get("days", []):
        lookup["ДЕНЬ " + str(day["id"])] = ("bos", day["topics"])
    for i, bonus in enumerate(bos.get("bonuses", [])):
        lookup["БОНУС " + str(i + 1)] = ("bos", bonus["topics"])
    for tool in bos.get("tools", []):
        if tool.get("topics"):
            lookup[tool["title"].upper()] = ("bos", tool["topics"])
    for course_id, data in COURSES.items():
        if isinstance(data, dict) and "topics" in data and "videoId" in data:
            lookup[data["title"].upper()] = (course_id, data["topics"])
    # Backward-compat: stats recorded before "roadmap" became its own course
    # used this fixed badge (see the old openRoadmap() in main.js) instead of
    # the course's actual title.
    if "roadmap" in COURSES:
        lookup.setdefault("ДОРОЖНАЯ КАРТА", ("roadmap", COURSES["roadmap"]["topics"]))
    return lookup


_BADGE_LOOKUP = _build_badge_lookup()
_TOPIC_IDX_RE = re.compile(r"^Тема (\d+):")


def _classify_stat(day: str, topic: str, progress: int):
    """Returns (course_id, percent) for a stats row. `course_id` is None if
    the badge doesn't match any known course/section; `percent` is None if
    the topic's total duration isn't known (e.g. its endSeconds is null)."""
    entry = _BADGE_LOOKUP.get(day)
    if not entry:
        return None, None
    course_id, topics = entry
    m = _TOPIC_IDX_RE.match(topic or "")
    if not m:
        return course_id, None
    idx = int(m.group(1)) - 1
    if not (0 <= idx < len(topics)):
        return course_id, None
    tp = topics[idx]
    start = tp.get("startSeconds") or 0
    end = tp.get("endSeconds")
    if end is None or end <= start:
        return course_id, None
    percent = max(0, min(100, round(progress / (end - start) * 100)))
    return course_id, percent


async def handle_admin_stats(request: web.Request) -> web.Response:
    user = await _authenticate(request)
    await _require_admin(user["id"])

    raw = await db.get_admin_stats_summary()

    access_counts = [_row_to_dict(r) for r in raw["access_counts"]]
    access_users_by_course: dict = {}
    for r in raw["course_access"]:
        access_users_by_course.setdefault(r["course_id"], set()).add(r["user_id"])

    watched_users_by_course: dict = {}
    decorated = []
    for r in raw["all_stats"]:
        course_id, percent = _classify_stat(r["day"], r["topic"], r["progress"])
        if course_id:
            watched_users_by_course.setdefault(course_id, set()).add(r["telegram_id"])
        decorated.append(
            {
                "user_id": r["telegram_id"],
                "username": r["username"],
                "phone_number": r["phone_number"],
                "course_id": course_id,
                "day": r["day"],
                "topic": r["topic"],
                "progress_seconds": r["progress"],
                "percent": percent,
                "updated_at": r["updated_at"].isoformat(),
                "_updated_at_sort": r["updated_at"],
            }
        )

    course_engagement = []
    for c in access_counts:
        cid = c["course_id"]
        total = c["access_count"]
        watched = len(watched_users_by_course.get(cid, set()) & access_users_by_course.get(cid, set()))
        course_engagement.append(
            {
                "course_id": cid,
                "title": c["title"],
                "watched": watched,
                "total": total,
                "percent": round(watched / total * 100) if total else 0,
            }
        )

    decorated.sort(key=lambda r: r["_updated_at_sort"], reverse=True)
    recent_activity = [{k: v for k, v in r.items() if k != "_updated_at_sort"} for r in decorated[:20]]

    return web.json_response(
        {
            "overview": {
                "total_users": raw["total_users"],
                "users_with_progress": raw["users_with_progress"],
                "access_counts": access_counts,
            },
            "course_engagement": course_engagement,
            "recent_activity": recent_activity,
        }
    )


def build_web_app() -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get("/health", handle_health)
    app.router.add_get("/api/course", handle_course)
    app.router.add_get("/api/my-courses", handle_my_courses)
    app.router.add_post("/api/stats", handle_stats)
    app.router.add_post("/api/browser-token", handle_browser_token)
    app.router.add_get("/api/admin/users", handle_admin_users)
    app.router.add_post("/api/admin/grant-access", handle_admin_grant_access)
    app.router.add_post("/api/admin/add-user-by-id", handle_admin_add_user_by_id)
    app.router.add_post("/api/admin/add-user-by-phone", handle_admin_add_user_by_phone)
    app.router.add_post("/api/admin/delete-user", handle_admin_delete_user)
    app.router.add_get("/api/admin/stats", handle_admin_stats)
    app.router.add_route("OPTIONS", "/{tail:.*}", lambda request: web.Response(status=204))
    return app


# ─── Main ─────────────────────────────────────────────────────────────


async def main() -> None:
    await db.init_pool()

    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add", add_user))
    application.add_handler(CommandHandler("remove", remove_user))
    application.add_handler(CommandHandler("list", list_users))
    application.add_handler(CommandHandler("addadmin", add_admin))
    application.add_handler(CommandHandler("removeadmin", remove_admin))
    application.add_handler(CommandHandler("stats", stats_cmd))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(MessageHandler(filters.CONTACT, contact_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, lesson_link_handler))
    application.add_error_handler(error_handler)

    runner = web.AppRunner(build_web_app())
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)

    async with application:
        await application.start()
        await application.updater.start_polling(drop_pending_updates=True)
        await site.start()
        logger.info(f"Бот запущен, API сервер слушает порт {PORT}")
        try:
            await asyncio.Event().wait()
        finally:
            await application.updater.stop()
            await application.stop()
            await runner.cleanup()
            await db.close_pool()


if __name__ == "__main__":
    asyncio.run(main())
