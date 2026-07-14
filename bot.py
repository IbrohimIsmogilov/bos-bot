import asyncio
import base64
import binascii
import copy
import datetime
import json
import logging
import re
from typing import Optional

import aiohttp
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
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

import auth
import db
import lesson_pipeline
from config import ADMIN_ID, ADMIN_USER_IDS, BOT_TOKEN, GROQ_API_KEY, MISTRAL_API_KEY, PORT, R2_PUBLIC_URL, WEBAPP_ORIGIN, WEBAPP_URL
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

    # Deep-link from the WebApp's "Черновики уроков" screen (see admin.js's
    # openEditViaChat): tg.openTelegramLink('https://t.me/<bot>?start=edit_<id>')
    # sends "/start edit_<id>" the same way any Telegram deep link does.
    if ctx.args and ctx.args[0].startswith("edit_"):
        if not await is_admin(user_id):
            await update.message.reply_text("❌ У вас нет прав администратора.")
            return
        try:
            lesson_id = int(ctx.args[0][len("edit_"):])
        except ValueError:
            await update.message.reply_text("❌ Некорректная ссылка редактирования.")
            return
        await _begin_edit_session(update.message, user_id, lesson_id)
        return

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
    """/add <phone или id> — validates and de-dupes the target up front (for
    fast feedback), then asks which role to grant via inline buttons before
    actually writing anything. See handle_add_role_callback for the write."""
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
    else:
        try:
            tid = int(arg)
        except ValueError:
            await update.message.reply_text("❌ Неверный формат.")
            return
        existing = await db.get_user(tid)
        if existing and existing["is_allowed"]:
            await update.message.reply_text(f"⚠️ ID {tid} уже в списке.")
            return

    buttons = [[InlineKeyboardButton("👤 Обычный участник", callback_data=f"addrole:participant:{arg}")]]
    # Only the super-admin can hand out admin rights (mirrors /addadmin's
    # own ADMIN_ID-only gate) — a regular admin doesn't even see the option.
    if user_id in ADMIN_USER_IDS:
        buttons.append([InlineKeyboardButton("🛠 Админ", callback_data=f"addrole:admin:{arg}")])
    buttons.append([InlineKeyboardButton("Отмена", callback_data="addrole:cancel:")])
    await update.message.reply_text(
        "Выберите роль для добавляемого участника:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def handle_add_role_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Second half of /add: applies the role picked via handle_add_role_callback's
    inline buttons. Re-validates admin/super-admin rights server-side rather
    than trusting the button that was rendered for this Telegram user."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        return
    try:
        _, role, arg = query.data.split(":", 2)
    except ValueError:
        return
    if role == "cancel":
        await query.edit_message_text("Отменено.")
        return

    make_admin = role == "admin"
    if make_admin and user_id not in ADMIN_USER_IDS:
        await query.edit_message_text("❌ Только супер-администратор может назначать администраторов.")
        return

    if is_phone(arg):
        phone = f"+{clean_phone(arg)}"
        if await db.get_allowed_phone(phone):
            await query.edit_message_text(f"⚠️ Номер {phone} уже в списке.")
            return
        await db.add_allowed_phone(phone, is_admin=make_admin)
        await db.set_allowed_by_phone(phone, True)
        if make_admin:
            await db.set_admin_by_phone(phone, True)
        label = f"номер {phone}"
    else:
        try:
            tid = int(arg)
        except ValueError:
            await query.edit_message_text("❌ Неверный формат.")
            return
        existing = await db.get_user(tid)
        if existing and existing["is_allowed"]:
            await query.edit_message_text(f"⚠️ ID {tid} уже в списке.")
            return
        await db.upsert_user(tid, is_allowed=True, is_admin=make_admin or None)
        await db.grant_course_access(tid, DEFAULT_COURSE_ID, granted_by=user_id)
        label = f"Telegram ID {tid}"

    role_label = "администратор" if make_admin else "участник"
    await query.edit_message_text(f"✅ Добавлен {label} ({role_label})")


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

        # Saved before grouping so the raw transcript survives past this one-shot
        # LLM pass — see edit_topics_via_instruction, which uses it to ground
        # later "edit via chat" instructions in the actual speech.
        await db.save_pending_lesson_transcript(lesson_id, segments)

        await db.update_pending_lesson_status(lesson_id, "grouping")
        topics = await asyncio.to_thread(lesson_pipeline.group_into_topics, title, segments, MISTRAL_API_KEY)

        await db.add_pending_lesson_topics(lesson_id, topics)
        await db.update_pending_lesson_status(lesson_id, "ready_for_review")
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("✏️ Редактировать текстом", callback_data=f"edit_lesson:{lesson_id}")]]
        )
        await bot.send_message(chat_id, f"✅ Транскрипт готов, {len(topics)} тем найдено.", reply_markup=kb)
    except lesson_pipeline.PipelineError as exc:
        logger.warning("Lesson %s pipeline failed: %s", lesson_id, exc)
        await db.update_pending_lesson_status(lesson_id, "failed", error_message=str(exc)[:500])
        await bot.send_message(chat_id, f"❌ Ошибка обработки видео «{title}»: {exc}")
    except Exception as exc:
        logger.exception("Lesson %s pipeline crashed", lesson_id)
        await db.update_pending_lesson_status(lesson_id, "failed", error_message=str(exc)[:500])
        await bot.send_message(chat_id, f"❌ Непредвиденная ошибка при обработке видео «{title}».")


# ─── Edit via chat (Этап 2.1: text-message alternative to the WebApp editor) ──

# Recognized as "end the session" regardless of trailing punctuation/case —
# see text_message_router.
EDIT_SESSION_STOP_WORDS = {"готово", "стоп", "хватит"}

# An instruction starting with this (case-insensitive) routes to
# lesson_pipeline.edit_topics_via_deep_analysis instead of the default
# edit_topics_via_instruction — see _apply_edit_instruction. Multi-pass and
# several minutes slower, but rebuilds topics window-by-window from the full
# transcript instead of a single downsampled-transcript call, for
# instructions that need the whole video rethought rather than a light edit.
DEEP_EDIT_PREFIX = "глубоко:"

# A long draft (e.g. a 100+-topic live-webinar transcript) can't have its
# full topic list echoed into one Telegram message (4096-char limit) without
# risking truncation — cap what _begin_edit_session shows, while still
# giving the LLM (in lesson_pipeline.edit_topics_via_instruction) the
# complete list to work from.
MAX_LISTED_TOPICS_IN_CHAT = 40


def _format_mmss(seconds: int) -> str:
    m, s = divmod(max(0, int(seconds)), 60)
    return f"{m}:{s:02d}"


def _format_topics_listing(topics) -> str:
    shown = topics[:MAX_LISTED_TOPICS_IN_CHAT]
    lines = [f"{i + 1}. [{_format_mmss(t['start_seconds'])}] {t['title']}" for i, t in enumerate(shown)]
    if len(topics) > MAX_LISTED_TOPICS_IN_CHAT:
        lines.append(f"… и ещё {len(topics) - MAX_LISTED_TOPICS_IN_CHAT} тем (полный список — в WebApp-редакторе).")
    return "\n".join(lines)


async def _begin_edit_session(message, user_id: int, lesson_id: int) -> None:
    """Shared by the inline-button callback and the /start deep link: starts
    (or replaces) `user_id`'s edit-via-chat session for `lesson_id`, or
    explains why it can't."""
    lesson = await db.get_pending_lesson(lesson_id)
    if not lesson:
        await message.reply_text("⚠️ Черновик не найден.")
        return
    if lesson["status"] != "ready_for_review":
        await message.reply_text(
            f"⚠️ Черновик сейчас в статусе «{lesson['status']}» — редактирование через чат доступно "
            "только для черновиков, ждущих проверки (ready_for_review)."
        )
        return

    await db.start_edit_session(user_id, lesson_id)
    topics = await db.get_pending_lesson_topics(lesson_id)
    listing = _format_topics_listing(topics)
    await message.reply_text(
        f"✏️ Режим редактирования: «{lesson['video_title'] or lesson_id}»\n\n"
        f"Текущие темы:\n{listing}\n\n"
        "Опишите, что изменить, например:\n"
        "• «объедини темы 3 и 4»\n"
        "• «удали тему 7»\n"
        "• «переименуй тему 2 в ...»\n"
        "• «раздели тему 5 на две»\n\n"
        "Для сложных правок, требующих анализа всего видео, начните инструкцию со слова "
        "«глубоко:» — это займёт больше времени (несколько минут), но даст более точный результат.\n\n"
        "Когда закончите — напишите «готово»."
    )


async def handle_edit_lesson_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        return
    try:
        lesson_id = int(query.data.split(":", 1)[1])
    except (IndexError, ValueError):
        return
    await _begin_edit_session(update.effective_message, user_id, lesson_id)


async def _apply_edit_instruction(message, lesson_id: int, instruction: str) -> None:
    """One turn of the edit-via-chat conversation: send `instruction` to the
    LLM against lesson_id's current topic list, save the result if it looks
    sane, and report back — success or failure — without ever ending the
    session (see text_message_router for how the session itself ends).

    An instruction starting with DEEP_EDIT_PREFIX routes to the multi-pass
    edit_topics_via_deep_analysis instead of the default single-call
    edit_topics_via_instruction — see that function's docstring for why."""
    lesson = await db.get_pending_lesson(lesson_id)
    if not lesson:
        await message.reply_text("⚠️ Черновик не найден — сессия редактирования завершена.")
        return

    topic_rows = await db.get_pending_lesson_topics(lesson_id)
    topics = [{"title": t["title"], "start_seconds": t["start_seconds"]} for t in topic_rows]

    transcript_rows = await db.get_pending_lesson_transcript(lesson_id)
    transcript = [{"start_seconds": r["start_seconds"], "text": r["text"]} for r in transcript_rows]

    deep_mode = instruction.strip().lower().startswith(DEEP_EDIT_PREFIX)
    if deep_mode:
        instruction_body = instruction.strip()[len(DEEP_EDIT_PREFIX):].strip()
        if not transcript:
            await message.reply_text(
                "❌ Для глубокого анализа нужен сохранённый транскрипт видео, а для этого черновика "
                "его нет (создан до появления этой функции). Опишите правку без «глубоко:» — "
                "обычный режим по-прежнему доступен."
            )
            return
        status_msg = await message.reply_text(
            "⏳ Провожу глубокий анализ всего видео, это займёт несколько минут..."
        )
    else:
        instruction_body = instruction
        status_msg = await message.reply_text("⏳ Применяю правку...")

    try:
        if deep_mode:
            result = await asyncio.to_thread(
                lesson_pipeline.edit_topics_via_deep_analysis,
                lesson["video_title"] or "",
                topics,
                instruction_body,
                transcript,
                MISTRAL_API_KEY,
            )
        else:
            result = await asyncio.to_thread(
                lesson_pipeline.edit_topics_via_instruction,
                lesson["video_title"] or "",
                topics,
                instruction_body,
                MISTRAL_API_KEY,
                transcript,
            )
    except lesson_pipeline.PipelineError as exc:
        await status_msg.edit_text(
            f"❌ Не удалось применить правку: {exc}\n\nПопробуйте переформулировать инструкцию."
        )
        return
    except Exception:
        logger.exception("Edit instruction crashed for lesson %s", lesson_id)
        await status_msg.edit_text("❌ Непредвиденная ошибка при применении правки. Попробуйте ещё раз.")
        return

    await db.replace_pending_lesson_topics(lesson_id, result["topics"])
    await status_msg.edit_text(
        f"✅ {result['summary']}\n\n"
        f"Всего тем: {len(result['topics'])}. Опишите следующую правку или напишите «готово», если закончили."
    )


async def text_message_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Dispatches a plain text message to exactly one of: new lesson
    ingestion (a YouTube link), an active edit-via-chat session, or nothing
    (silently ignored — e.g. a non-admin's random message, or an admin with
    no active session). A YouTube link always wins over an active session,
    so starting a new lesson never requires first typing "готово"."""
    text = update.message.text or ""
    if YOUTUBE_URL_RE.search(text):
        await lesson_link_handler(update, ctx)
        return

    user_id = update.effective_user.id
    if not await is_admin(user_id):
        return

    session = await db.get_edit_session(user_id)
    if not session:
        return

    normalized = text.strip().lower().rstrip(".!")
    if normalized in EDIT_SESSION_STOP_WORDS:
        await db.end_edit_session(user_id)
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("📚 Админ-панель", web_app={"url": WEBAPP_URL.rstrip("/") + "/admin.html"})]]
        )
        await update.message.reply_text(
            "✅ Изменения сохранены. Откройте админ-панель, чтобы опубликовать.", reply_markup=kb
        )
        return

    await _apply_edit_instruction(update.message, session["pending_lesson_id"], text)


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
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type, Range"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PATCH, OPTIONS"
    response.headers["Access-Control-Expose-Headers"] = "Content-Range, Content-Length, Accept-Ranges"
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


def _topics_from_db_rows(rows: list[asyncpg.Record]) -> list[dict]:
    """Same {title, startSeconds, endSeconds} shape as course_data.py's
    _build_topics — endSeconds is the next topic's start, None for the last."""
    topics = []
    for i, row in enumerate(rows):
        end = rows[i + 1]["start_seconds"] if i + 1 < len(rows) else None
        topics.append({"title": row["title"], "startSeconds": row["start_seconds"], "endSeconds": end})
    return topics


async def _db_video_to_day(video: asyncpg.Record, day_id: int) -> dict:
    """One db_course_videos row -> a "day" entry in the {days: [...]} shape.
    `videoHlsUrl` is a slight misnomer here (it's a bare YouTube video ID,
    not an HLS URL) — main.js's loadVideo() picks the playback backend by
    checking whether the value looks like an http(s) URL, so a bare ID in
    this field is routed to the YouTube player exactly as intended."""
    topics = _topics_from_db_rows(await db.get_course_video_topics(video["id"]))
    return {
        "id": day_id,
        "title": video["title"] or f"День {day_id}",
        "videoHlsUrl": video["video_id"],
        "topics": topics,
    }


def _module_item_to_frontend(item: dict) -> dict:
    """One db.get_module_contents() row -> the shape main.js expects inside
    a module's "items" list. Videos keep the same {videoHlsUrl, topics}
    fields _db_video_to_day already uses (topics filled in separately,
    since get_module_contents doesn't join db_course_topics); materials are
    just their own columns renamed to a stable public shape."""
    if item["type"] == "video":
        return {
            "type": "video",
            "id": item["id"],
            "title": item["title"] or "Видео",
            "videoHlsUrl": item["video_id"],
            "topics": [],  # filled in by _build_module_payload
        }
    return {
        "type": item["type"],
        "id": item["id"],
        "title": item["title"],
        "url": item["storage_url"],
    }


async def _build_module_payload(module) -> dict:
    items = await db.get_module_contents(module["id"])
    result = []
    for item in items:
        frontend_item = _module_item_to_frontend(item)
        if frontend_item["type"] == "video":
            topics = _topics_from_db_rows(await db.get_course_video_topics(item["id"]))
            frontend_item["topics"] = topics
        result.append(frontend_item)
    return {"id": module["id"], "title": module["title"], "items": result}


async def _build_course_payload(course_id: str, hardcoded: Optional[dict], course_row) -> dict:
    """Assemble the GET /api/course response for one course_id, per the
    Этап 2 spec's three cases (plus the Этап 3 modules case):

    0. course_id has modules (db.list_modules) -> {modules: [...]}, each
       module's videos and materials merged into one position-ordered
       "items" list. Takes priority over the flat cases below — a modular
       course is never also hardcoded or flat-DB.
    1. course_id is a hardcoded multi-day course (has "days") -> DB videos
       for it are appended as extra days at the end.
    2. course_id isn't hardcoded and has exactly one DB video -> flat
       {videoId, topics} shape (same as "roadmap" today; the frontend
       already treats this structurally via isSingleVideoCourse()).
    3. course_id isn't hardcoded and has 2+ DB videos -> {days: [...]},
       same shape as case 1 minus bonuses/tools.

    A hardcoded course without "days" (e.g. "roadmap", a single fixed
    video) is returned unchanged — Этап 2 doesn't support layering DB
    lessons onto that shape, and handle_admin_publish_pending_lesson
    rejects "existing_course" targets that would need it to.
    """
    modules = await db.list_modules(course_id)
    if modules:
        return {
            "title": course_row["title"] if course_row else None,
            "modules": [await _build_module_payload(m) for m in modules],
        }

    if hardcoded is not None and "days" in hardcoded:
        payload = copy.deepcopy(hardcoded)
        next_day_id = max((d["id"] for d in payload["days"]), default=0) + 1
        for video in await db.get_course_videos(course_id):
            payload["days"].append(await _db_video_to_day(video, next_day_id))
            next_day_id += 1
        return payload

    if hardcoded is not None:
        return hardcoded

    videos = await db.get_course_videos(course_id)
    if len(videos) == 1:
        video = videos[0]
        topics = _topics_from_db_rows(await db.get_course_video_topics(video["id"]))
        return {"id": course_id, "title": course_row["title"], "videoId": video["video_id"], "topics": topics}

    days = []
    for i, video in enumerate(videos, start=1):
        days.append(await _db_video_to_day(video, i))
    return {"days": days}


async def handle_course(request: web.Request) -> web.Response:
    # Defaults to the only course that exists today so the not-yet-updated
    # frontend (Этап 2) keeps working unchanged during the transition.
    user_id = await _resolve_user_id(request)

    course_id = request.query.get("course_id", DEFAULT_COURSE_ID)
    hardcoded = COURSES.get(course_id)
    course_row = None if hardcoded is not None else await db.get_course(course_id)
    if hardcoded is None and course_row is None:
        raise web.HTTPNotFound(reason="unknown course_id")
    if not await _has_course_access(user_id, course_id):
        raise web.HTTPForbidden(reason="access denied")

    payload = await _build_course_payload(course_id, hardcoded, course_row)
    return web.json_response(payload)


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


async def handle_watch_progress(request: web.Request) -> web.Response:
    """Records the exact resume point (video-absolute seconds) for one
    (course, section, topic) — see handle_continue_watching for how this
    powers the "Продолжить просмотр" card. Separate from /api/stats: stats
    is topic-segment-relative and feeds only the admin analytics screen."""
    user_id = await _resolve_user_id(request)
    if not await is_allowed(user_id):
        raise web.HTTPForbidden(reason="access denied")

    try:
        payload = await request.json()
    except (json.JSONDecodeError, ValueError):
        raise web.HTTPBadRequest(reason="invalid JSON body")

    course_id = payload.get("course_id")
    section_label = payload.get("section_label")
    topic_idx = payload.get("topic_idx")
    topic_title = payload.get("topic_title")
    position_seconds = payload.get("position_seconds")
    section_key = payload.get("section_key") or ""
    duration_seconds = payload.get("duration_seconds")
    completed = payload.get("completed", False)

    if (
        not isinstance(course_id, str)
        or not isinstance(section_label, str)
        or not isinstance(topic_idx, int)
        or not isinstance(topic_title, str)
        or not isinstance(position_seconds, (int, float))
        or not isinstance(section_key, str)
        or not isinstance(completed, bool)
    ):
        raise web.HTTPBadRequest(reason="course_id, section_label, topic_idx, topic_title and position_seconds are required")
    if duration_seconds is not None and not isinstance(duration_seconds, (int, float)):
        raise web.HTTPBadRequest(reason="duration_seconds must be a number if present")
    if not course_id.strip() or not await db.get_course(course_id):
        raise web.HTTPNotFound(reason="unknown course_id")

    await db.upsert_user(user_id)
    await db.record_watch_progress(
        user_id,
        course_id,
        section_key.strip()[:200],
        section_label.strip()[:200],
        max(0, int(topic_idx)),
        topic_title.strip()[:300],
        max(0, min(int(position_seconds), 1_000_000)),
        max(0, min(int(duration_seconds), 1_000_000)) if duration_seconds is not None else None,
        completed,
    )
    return web.json_response({"ok": True})


async def handle_continue_watching(request: web.Request) -> web.Response:
    user_id = await _resolve_user_id(request)
    if not await is_allowed(user_id):
        raise web.HTTPForbidden(reason="access denied")
    row = await db.get_continue_watching(user_id)
    return web.json_response(_row_to_dict(row) if row else None)


async def handle_pdf_proxy(request: web.Request) -> web.StreamResponse:
    """Streams a course-materials PDF from R2 through our own origin instead
    of the browser fetching pub-*.r2.dev directly — that public dev domain
    doesn't support CORS preflight at all, so pdf.js's Range-based fetches
    were blocked by the browser before ever reaching R2. Range headers are
    forwarded both ways so pdf.js's lazy per-page loading still only pulls
    the bytes it needs, instead of the whole file on every page turn."""
    key = request.query.get("key", "")
    if not key.startswith("course-materials/") or ".." in key:
        raise web.HTTPBadRequest(text="invalid key")

    upstream_headers = {}
    range_header = request.headers.get("Range")
    if range_header:
        upstream_headers["Range"] = range_header

    session = aiohttp.ClientSession()
    try:
        upstream = await session.get(f"{R2_PUBLIC_URL}/{key}", headers=upstream_headers)
    except aiohttp.ClientError:
        await session.close()
        raise web.HTTPBadGateway(text="upstream fetch failed")

    if upstream.status >= 400:
        await upstream.release()
        await session.close()
        raise web.HTTPNotFound()

    resp = web.StreamResponse(status=upstream.status)
    resp.content_type = upstream.headers.get("Content-Type", "application/pdf")
    for h in ("Content-Range", "Content-Length", "Accept-Ranges", "ETag", "Last-Modified"):
        if h in upstream.headers:
            resp.headers[h] = upstream.headers[h]
    # cors_middleware sets these on the response object after the handler
    # returns, which works for a plain web.Response (built in memory, sent
    # once) but not here — prepare() flushes the header block to the client
    # immediately, before the middleware ever runs. Set them ourselves.
    resp.headers["Access-Control-Allow-Origin"] = WEBAPP_ORIGIN
    resp.headers["Access-Control-Expose-Headers"] = "Content-Range, Content-Length, Accept-Ranges"

    await resp.prepare(request)
    try:
        async for chunk in upstream.content.iter_chunked(65536):
            await resp.write(chunk)
    finally:
        await upstream.release()
        await session.close()
    return resp


# ─── Admin API (BilimBook admin panel) ──────────────────────────────────


async def handle_admin_users(request: web.Request) -> web.Response:
    user = await _authenticate(request)
    await _require_admin(user["id"])
    rows = await db.list_all_users_with_access()
    return web.json_response([_row_to_dict(r) for r in rows])


async def handle_admin_whoami(request: web.Request) -> web.Response:
    """Tells the admin panel whether the caller is the super-admin (the only
    role allowed to grant admin rights to others — see handle_admin_add_user_by_id
    and handle_admin_add_user_by_phone) so it knows whether to show the
    "Админ" role option in the add-user form."""
    user = await _authenticate(request)
    await _require_admin(user["id"])
    return web.json_response({"is_super_admin": user["id"] in ADMIN_USER_IDS})


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


def _validate_make_admin(payload: dict, admin_user_id: int) -> bool:
    """Validates the optional `is_admin` field for the add-user endpoints.

    Only the super-admin (ADMIN_USER_IDS) may set it True — mirrors the
    bot's own /addadmin, which is restricted the same way — so a regular
    admin can't hand out admin rights just because the field is client-supplied.
    """
    make_admin = payload.get("is_admin", False)
    if not isinstance(make_admin, bool):
        raise web.HTTPBadRequest(reason="is_admin must be a bool")
    if make_admin and admin_user_id not in ADMIN_USER_IDS:
        raise web.HTTPForbidden(reason="only the super-admin can grant admin rights")
    return make_admin


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
    make_admin = _validate_make_admin(payload, admin_user["id"])

    # Only touch is_admin when actually granting it — passing False here
    # (instead of None) would silently demote an already-existing admin.
    await db.upsert_user(target_user_id, is_allowed=True, is_admin=make_admin or None)
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
    make_admin = _validate_make_admin(payload, admin_user["id"])

    phone = f"+{clean_phone(raw_phone)}"
    await db.add_allowed_phone(phone, is_admin=make_admin)
    await db.set_allowed_by_phone(phone, True)
    if make_admin:
        await db.set_admin_by_phone(phone, True)
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


# ─── Admin API: publish flow (Этап 2) ───────────────────────────────────


async def handle_admin_courses(request: web.Request) -> web.Response:
    """Courses eligible as an "existing_course" publish target — i.e.
    anything that isn't a hardcoded single-video course like "roadmap",
    which _build_course_payload doesn't know how to layer a DB lesson onto."""
    user = await _authenticate(request)
    await _require_admin(user["id"])
    rows = await db.list_courses()
    result = []
    for r in rows:
        hardcoded = COURSES.get(r["id"])
        if hardcoded is not None and "days" not in hardcoded:
            continue
        result.append({"id": r["id"], "title": r["title"], "subtitle": r["subtitle"], "icon": r["icon"]})
    return web.json_response(result)


async def handle_admin_pending_lessons(request: web.Request) -> web.Response:
    user = await _authenticate(request)
    await _require_admin(user["id"])
    rows = await db.list_pending_lessons_summary()
    return web.json_response([_row_to_dict(r) for r in rows])


def _parse_lesson_id(request: web.Request) -> int:
    try:
        return int(request.match_info["id"])
    except ValueError:
        raise web.HTTPBadRequest(reason="invalid lesson id")


async def handle_admin_pending_lesson_detail(request: web.Request) -> web.Response:
    user = await _authenticate(request)
    await _require_admin(user["id"])
    lesson_id = _parse_lesson_id(request)

    lesson = await db.get_pending_lesson(lesson_id)
    if not lesson:
        raise web.HTTPNotFound(reason="unknown pending lesson")

    topics = await db.get_pending_lesson_topics(lesson_id)
    result = _row_to_dict(lesson)
    result["topics"] = [{"title": t["title"], "start_seconds": t["start_seconds"]} for t in topics]
    return web.json_response(result)


def _format_transcript_timecode(seconds) -> str:
    """"M:SS" formatting for the downloadable transcript — same convention
    as admin.js's formatTimecode (minutes not zero-padded past 9, e.g.
    "3:07", "198:43" for a long video)."""
    total = max(0, int(seconds))
    m, s = divmod(total, 60)
    return f"{m}:{s:02d}"


def _interleave_transcript_with_topic_markers(
    transcript_rows: list[asyncpg.Record], topic_rows: list[asyncpg.Record]
) -> list[str]:
    """Build the downloadable transcript's lines, inserting a
    "=== ТЕМА N: {title} [{M:SS}] ===" marker right before the first segment
    whose timecode is >= that topic's start_seconds (i.e. the segment the
    topic's start_seconds was grounded in — see group_into_topics/
    edit_topics_via_instruction, which always copy an actual segment start
    rather than inventing one). Topics are numbered 1-based in start_seconds
    order, matching how the admin sees them numbered in the WebApp editor
    and in Telegram's "edit via chat" flow.

    A topic whose start_seconds falls after every transcript segment (e.g.
    manually retimed past the end) still gets its marker — appended at the
    very end, after the last segment line — so no topic is silently dropped.
    Returns [] lines unchanged (no markers) when topic_rows is empty.
    """
    topics_sorted = sorted(topic_rows, key=lambda t: t["start_seconds"])
    topic_idx = 0
    n_topics = len(topics_sorted)

    lines = []
    for row in transcript_rows:
        seg_start = row["start_seconds"]
        while topic_idx < n_topics and topics_sorted[topic_idx]["start_seconds"] <= seg_start:
            t = topics_sorted[topic_idx]
            lines.append(
                f"=== ТЕМА {topic_idx + 1}: {t['title']} [{_format_transcript_timecode(t['start_seconds'])}] ==="
            )
            topic_idx += 1
        lines.append(f"[{_format_transcript_timecode(seg_start)}] {row['text']}")

    while topic_idx < n_topics:
        t = topics_sorted[topic_idx]
        lines.append(
            f"=== ТЕМА {topic_idx + 1}: {t['title']} [{_format_transcript_timecode(t['start_seconds'])}] ==="
        )
        topic_idx += 1

    return lines


async def handle_admin_pending_lesson_transcript(request: web.Request) -> web.Response:
    user = await _authenticate(request)
    await _require_admin(user["id"])
    lesson_id = _parse_lesson_id(request)

    if not await db.get_pending_lesson(lesson_id):
        raise web.HTTPNotFound(reason="unknown pending lesson")

    rows = await db.get_pending_lesson_transcript(lesson_id)
    if not rows:
        raise web.HTTPNotFound(
            reason="transcript not available for this lesson (it was created before transcript saving was added)"
        )

    topic_rows = await db.get_pending_lesson_topics(lesson_id)
    lines = _interleave_transcript_with_topic_markers(rows, topic_rows)
    return web.Response(
        text="\n".join(lines) + "\n",
        content_type="text/plain",
        charset="utf-8",
        headers={"Content-Disposition": f'attachment; filename="transcript_{lesson_id}.txt"'},
    )


def _validate_topics_payload(payload) -> list[dict]:
    if not isinstance(payload, list) or not payload:
        raise web.HTTPBadRequest(reason="body must be a non-empty JSON array of {title, start_seconds}")
    topics = []
    for item in payload:
        if not isinstance(item, dict):
            raise web.HTTPBadRequest(reason="each topic must be an object")
        title = item.get("title")
        start_seconds = item.get("start_seconds")
        if not isinstance(title, str) or not title.strip():
            raise web.HTTPBadRequest(reason="each topic needs a non-empty title")
        if isinstance(start_seconds, bool) or not isinstance(start_seconds, int) or start_seconds < 0:
            raise web.HTTPBadRequest(reason="each topic needs a non-negative integer start_seconds")
        topics.append({"title": title.strip()[:300], "start_seconds": start_seconds})
    # The editor lets an admin add/delete/reorder rows freely, so the array
    # it sends isn't guaranteed to already be chronological — but every
    # consumer (endSeconds derivation in _topics_from_db_rows, position as
    # stored order) assumes ascending start_seconds. Sorting here (not
    # rejecting) keeps that invariant without pushing manual reordering onto
    # the admin.
    topics.sort(key=lambda t: t["start_seconds"])
    return topics


async def handle_admin_pending_lesson_topics(request: web.Request) -> web.Response:
    user = await _authenticate(request)
    await _require_admin(user["id"])
    lesson_id = _parse_lesson_id(request)

    if not await db.get_pending_lesson(lesson_id):
        raise web.HTTPNotFound(reason="unknown pending lesson")

    try:
        payload = await request.json()
    except (json.JSONDecodeError, ValueError):
        raise web.HTTPBadRequest(reason="invalid JSON body")

    topics = _validate_topics_payload(payload)
    await db.replace_pending_lesson_topics(lesson_id, topics)
    return web.json_response({"ok": True})


# Statuses where the pipeline (process_pending_lesson) is still actively
# writing to this row — deleting out from under it would race the worker
# and could resurrect a half-deleted draft on its next status update.
_PENDING_LESSON_ACTIVE_STATUSES = {"processing", "transcribing", "grouping"}


async def handle_admin_delete_pending_lesson(request: web.Request) -> web.Response:
    admin_user = await _authenticate(request)
    await _require_admin(admin_user["id"])
    lesson_id = _parse_lesson_id(request)

    lesson = await db.get_pending_lesson(lesson_id)
    if not lesson:
        raise web.HTTPNotFound(reason="unknown pending lesson")
    if lesson["status"] in _PENDING_LESSON_ACTIVE_STATUSES:
        raise web.HTTPBadRequest(
            reason=f"lesson is still {lesson['status']}, wait for it to finish before deleting"
        )

    await db.delete_pending_lesson(lesson_id)
    return web.json_response({"ok": True})


# Cyrillic -> Latin transliteration for auto-generating a course_id slug
# from a Russian course title (see handle_admin_publish_pending_lesson,
# mode="new_course" without an explicit course_id).
_CYRILLIC_TRANSLIT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e", "ж": "zh", "з": "z",
    "и": "i", "й": "y", "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r",
    "с": "s", "т": "t", "у": "u", "ф": "f", "х": "h", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}
_SLUG_STRIP_RE = re.compile(r"[^a-z0-9]+")


def _slugify(title: str) -> str:
    translit = "".join(_CYRILLIC_TRANSLIT.get(ch, ch) for ch in title.lower())
    return _SLUG_STRIP_RE.sub("-", translit).strip("-")[:60]


# Cap on an admin-uploaded course icon (see POST .../publish's icon_data_url,
# mode="new_course") after base64-decoding — keeps a single icon from
# ballooning courses.icon while comfortably fitting a small square PNG/JPG.
ICON_DATA_URL_MAX_DECODED_BYTES = 500 * 1024
_ICON_DATA_URL_RE = re.compile(r"^data:image/[a-zA-Z0-9.+-]+;base64,")


def _validate_icon_data_url(icon_data_url: object) -> Optional[str]:
    """Validate an admin-uploaded course icon before it's stored as-is in
    courses.icon. Returns None if icon_data_url wasn't provided at all (the
    caller then falls back to the default emoji icon, same as before this
    field existed — see courseIconHtml in main.js/admin.js). Raises
    HTTPBadRequest if it was provided but isn't a plausible data:image/...
    URL or decodes to more than ICON_DATA_URL_MAX_DECODED_BYTES."""
    if icon_data_url is None:
        return None
    if not isinstance(icon_data_url, str) or not _ICON_DATA_URL_RE.match(icon_data_url):
        raise web.HTTPBadRequest(reason="icon_data_url must be a data:image/...;base64,... URL")

    b64_payload = icon_data_url.split(",", 1)[1]
    try:
        decoded_size = len(base64.b64decode(b64_payload, validate=True))
    except (ValueError, binascii.Error) as exc:
        raise web.HTTPBadRequest(reason="icon_data_url is not valid base64") from exc
    if decoded_size > ICON_DATA_URL_MAX_DECODED_BYTES:
        raise web.HTTPBadRequest(
            reason=f"icon too large ({decoded_size} bytes decoded, max {ICON_DATA_URL_MAX_DECODED_BYTES})"
        )
    return icon_data_url


async def handle_admin_publish_pending_lesson(request: web.Request) -> web.Response:
    admin_user = await _authenticate(request)
    await _require_admin(admin_user["id"])
    lesson_id = _parse_lesson_id(request)

    lesson = await db.get_pending_lesson(lesson_id)
    if not lesson:
        raise web.HTTPNotFound(reason="unknown pending lesson")
    if lesson["status"] != "ready_for_review":
        raise web.HTTPBadRequest(reason=f"lesson status is {lesson['status']!r}, expected ready_for_review")

    topic_rows = await db.get_pending_lesson_topics(lesson_id)
    if not topic_rows:
        raise web.HTTPBadRequest(reason="lesson has no topics to publish")
    topics = [{"title": t["title"], "start_seconds": t["start_seconds"]} for t in topic_rows]

    try:
        payload = await request.json()
    except (json.JSONDecodeError, ValueError):
        raise web.HTTPBadRequest(reason="invalid JSON body")
    if not isinstance(payload, dict):
        raise web.HTTPBadRequest(reason="invalid JSON body")

    mode = payload.get("mode")
    if mode == "new_course":
        title = payload.get("title")
        if not isinstance(title, str) or not title.strip():
            raise web.HTTPBadRequest(reason="title is required")
        title = title.strip()
        subtitle = payload.get("subtitle")
        subtitle = subtitle.strip() if isinstance(subtitle, str) and subtitle.strip() else None
        icon = _validate_icon_data_url(payload.get("icon_data_url"))

        explicit_course_id = payload.get("course_id")
        if isinstance(explicit_course_id, str) and explicit_course_id.strip():
            course_id = explicit_course_id.strip()
            if await db.get_course(course_id):
                raise web.HTTPConflict(reason=f"course_id already exists: {course_id}")
        else:
            base = _slugify(title) or "course"
            course_id = base
            suffix = 2
            while await db.get_course(course_id):
                course_id = f"{base}-{suffix}"
                suffix += 1

        await db.create_course(course_id, title, subtitle, icon)
        await db.add_course_video_with_topics(course_id, 0, None, lesson["video_id"], topics)

    elif mode == "existing_course":
        course_id = payload.get("course_id")
        if not isinstance(course_id, str) or not course_id.strip():
            raise web.HTTPBadRequest(reason="course_id is required")
        course_id = course_id.strip()
        day_title = payload.get("day_title")
        if not isinstance(day_title, str) or not day_title.strip():
            raise web.HTTPBadRequest(reason="day_title is required")

        if not await db.get_course(course_id):
            raise web.HTTPNotFound(reason="unknown course_id")
        hardcoded = COURSES.get(course_id)
        if hardcoded is not None and "days" not in hardcoded:
            raise web.HTTPBadRequest(reason="this course does not support adding lessons")

        position = await db.next_course_video_position(course_id)
        await db.add_course_video_with_topics(course_id, position, day_title.strip(), lesson["video_id"], topics)

    else:
        raise web.HTTPBadRequest(reason="mode must be 'new_course' or 'existing_course'")

    await db.update_pending_lesson_status(lesson_id, "published")
    await db.grant_course_access(admin_user["id"], course_id, granted_by=admin_user["id"])
    return web.json_response({"ok": True, "course_id": course_id})


def build_web_app() -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get("/health", handle_health)
    app.router.add_get("/api/course", handle_course)
    app.router.add_get("/api/my-courses", handle_my_courses)
    app.router.add_post("/api/stats", handle_stats)
    app.router.add_post("/api/watch-progress", handle_watch_progress)
    app.router.add_get("/api/continue-watching", handle_continue_watching)
    app.router.add_get("/api/pdf-proxy", handle_pdf_proxy)
    app.router.add_post("/api/browser-token", handle_browser_token)
    app.router.add_get("/api/admin/users", handle_admin_users)
    app.router.add_get("/api/admin/whoami", handle_admin_whoami)
    app.router.add_post("/api/admin/grant-access", handle_admin_grant_access)
    app.router.add_post("/api/admin/add-user-by-id", handle_admin_add_user_by_id)
    app.router.add_post("/api/admin/add-user-by-phone", handle_admin_add_user_by_phone)
    app.router.add_post("/api/admin/delete-user", handle_admin_delete_user)
    app.router.add_get("/api/admin/stats", handle_admin_stats)
    app.router.add_get("/api/admin/courses", handle_admin_courses)
    app.router.add_get("/api/admin/pending-lessons", handle_admin_pending_lessons)
    app.router.add_get("/api/admin/pending-lessons/{id}", handle_admin_pending_lesson_detail)
    app.router.add_get("/api/admin/pending-lessons/{id}/transcript", handle_admin_pending_lesson_transcript)
    app.router.add_patch("/api/admin/pending-lessons/{id}/topics", handle_admin_pending_lesson_topics)
    app.router.add_post("/api/admin/pending-lessons/{id}/delete", handle_admin_delete_pending_lesson)
    app.router.add_post("/api/admin/pending-lessons/{id}/publish", handle_admin_publish_pending_lesson)
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
    application.add_handler(CallbackQueryHandler(handle_edit_lesson_callback, pattern=r"^edit_lesson:\d+$"))
    application.add_handler(CallbackQueryHandler(handle_add_role_callback, pattern=r"^addrole:"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_router))
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
