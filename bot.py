import asyncio
import json
import logging

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
from config import ADMIN_ID, BOT_TOKEN, PORT, WEBAPP_ORIGIN, WEBAPP_URL
from course_data import COURSE_DATA

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
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("📚 Открыть курс", web_app={"url": WEBAPP_URL})]])
        prefix = "администратор!" if await is_admin(user_id) else "участник!"
        await update.message.reply_text(
            f"✅ Добро пожаловать, {prefix}\n\nВаш доступ к курсу открыт.", reply_markup=kb
        )
        return
    kb = ReplyKeyboardMarkup(
        [[KeyboardButton("📱 Поделиться номером", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await update.message.reply_text(
        "🎓 *Добро пожаловать!*\n\n"
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
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("📚 Открыть курс", web_app={"url": WEBAPP_URL})]])

    allowed_phone = await db.get_allowed_phone(phone)
    if allowed_phone:
        await db.upsert_user(user_id, phone_number=phone, is_admin=allowed_phone["is_admin"], is_allowed=True)
        if allowed_phone["is_admin"]:
            await update.message.reply_text("✅ Вы вошли как администратор!", reply_markup=ReplyKeyboardRemove())
        else:
            await update.message.reply_text("✅ Доступ открыт!", reply_markup=ReplyKeyboardRemove())
        await update.message.reply_text("Нажмите чтобы начать:", reply_markup=kb)
        return

    existing = await db.get_user(user_id)
    if existing and existing["is_allowed"]:
        await db.upsert_user(user_id, phone_number=phone)
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
        removed_pre = await db.remove_allowed_phone(phone)
        revoked = await db.set_allowed_by_phone(phone, False)
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
            "/start — открыть курс"
        )
    elif await is_admin(user_id):
        text = (
            "🤖 Команды администратора:\n\n"
            "/add +998XXXXXXXXX — добавить участника\n"
            "/remove +998XXXXXXXXX — удалить участника\n"
            "/list — список всех участников\n"
            "/stats — статистика просмотров\n"
            "/start — открыть курс"
        )
    else:
        text = "/start — открыть курс"
    await update.message.reply_text(text)


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


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def handle_course(request: web.Request) -> web.Response:
    user_id = await _resolve_user_id(request)
    if not await is_allowed(user_id):
        raise web.HTTPForbidden(reason="access denied")
    return web.json_response(COURSE_DATA)


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


def build_web_app() -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get("/health", handle_health)
    app.router.add_get("/api/course", handle_course)
    app.router.add_post("/api/stats", handle_stats)
    app.router.add_post("/api/browser-token", handle_browser_token)
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
