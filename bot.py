import os
import json
import logging
import asyncio
from datetime import datetime
from aiohttp import web
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
WEBAPP_URL = os.environ.get("WEBAPP_URL", "https://kslmvv.github.io/bos-course/")
SUPER_ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
PORT = int(os.environ.get("PORT", "8080"))
USERS_FILE = "/data/allowed_users.json"

def load_data():
    try:
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Ошибка загрузки: {e}")
    return {"phones": [], "telegram_ids": [], "admins": [], "admin_phones": [], "stats": {}}

def save_data(data):
    try:
        with open(USERS_FILE, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка сохранения: {e}")

def clean_phone(raw):
    return raw.replace(" ", "").replace("-", "").lstrip("+")

def is_phone(raw):
    c = clean_phone(raw)
    return c.isdigit() and len(c) >= 7 and (raw.strip().startswith("+") or len(c) > 10)

def is_super_admin(uid): return SUPER_ADMIN_ID != 0 and uid == SUPER_ADMIN_ID
def is_admin(uid):
    if is_super_admin(uid): return True
    return uid in load_data().get("admins", [])
def is_admin_phone(phone):
    clean = clean_phone(phone)
    return clean in [clean_phone(p) for p in load_data().get("admin_phones", [])]
def is_allowed(uid, phone=None):
    if is_admin(uid): return True
    data = load_data()
    if uid in data.get("telegram_ids", []): return True
    if phone:
        c = clean_phone(phone)
        if c in [clean_phone(p) for p in data.get("phones", [])]: return True
        if c in [clean_phone(p) for p in data.get("admin_phones", [])]: return True
    return False

def parse_arg(context):
    if not context.args: return None
    return "".join(context.args).strip()

# ── HTTP API для статистики ───────────────────────────
async def handle_track(request):
    """WebApp отправляет просмотр сюда"""
    try:
        # CORS headers
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
        }
        if request.method == 'OPTIONS':
            return web.Response(status=200, headers=headers)

        body = await request.json()
        uid = str(body.get("uid", ""))
        entry = body.get("entry", "")
        phone = body.get("phone", uid)
        date = body.get("date", datetime.now().strftime("%d.%m.%Y"))

        if not uid or not entry:
            return web.json_response({"ok": False, "error": "missing uid or entry"}, headers=headers)

        data = load_data()
        if "stats" not in data: data["stats"] = {}
        if uid not in data["stats"]:
            data["stats"][uid] = {"watched": [], "phone": phone, "last_title": "", "last_date": ""}

        user_stat = data["stats"][uid]
        user_stat["phone"] = phone
        if entry not in user_stat["watched"]:
            user_stat["watched"].append(entry)
        user_stat["last_title"] = entry
        user_stat["last_date"] = date
        data["stats"][uid] = user_stat
        save_data(data)
        logger.info(f"Tracked: uid={uid}, entry={entry}")
        return web.json_response({"ok": True, "total": len(user_stat["watched"])}, headers=headers)
    except Exception as e:
        logger.error(f"handle_track error: {e}")
        return web.json_response({"ok": False, "error": str(e)}, headers={'Access-Control-Allow-Origin': '*'})

async def handle_health(request):
    return web.json_response({"ok": True, "status": "running"})

# ── ТЕКСТЫ ────────────────────────────────────────────
WELCOME_TEXT = """\
🎓 *Добро пожаловать!*

━━━━━━━━━━━━━━━━━━━━━━
📚 *Курс «Бизнес Операционная Система»*
👤 *Автор:* Александр Высоцкий
━━━━━━━━━━━━━━━━━━━━━━

Этот курс поможет вам:
✅ Выстроить систему управления бизнесом
✅ Освободиться от операционки
✅ Масштабировать компанию без хаоса

Для получения доступа нажмите кнопку ниже 👇"""

GRANTED_TEXT = """\
✅ *Доступ открыт!*

━━━━━━━━━━━━━━━━━━━━━━
🎓 *Курс «Бизнес Операционная Система»*
👤 *Александр Высоцкий*
━━━━━━━━━━━━━━━━━━━━━━

Нажмите кнопку ниже чтобы начать обучение 👇"""

DENIED_TEXT = """\
🔒 *Доступ закрыт*

━━━━━━━━━━━━━━━━━━━━━━

Ваш номер не найден в списке участников курса.

Если это ошибка — обратитесь к организатору."""

# ── ПОЛЬЗОВАТЕЛЬ ─────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if is_allowed(uid):
        await send_course_button(update)
        return
    keyboard = [[KeyboardButton("📱 Поделиться номером", request_contact=True)]]
    await update.message.reply_text(
        WELCOME_TEXT, parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
    )

async def contact_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    contact = update.message.contact
    if not contact: return
    phone = contact.phone_number
    uid = update.effective_user.id
    await update.message.reply_text("🔍 Проверяю доступ...", reply_markup=ReplyKeyboardRemove())
    if is_allowed(uid, phone):
        data = load_data()
        if is_admin_phone(phone) and uid not in data.get("admins", []):
            if "admins" not in data: data["admins"] = []
            data["admins"].append(uid)
        if uid not in data["telegram_ids"]:
            data["telegram_ids"].append(uid)
        save_data(data)
        await send_course_button(update)
    else:
        await update.message.reply_text(DENIED_TEXT, parse_mode="Markdown")

async def send_course_button(update: Update):
    keyboard = [[InlineKeyboardButton("📚 Открыть курс", web_app=WebAppInfo(url=WEBAPP_URL))]]
    await update.message.reply_text(
        GRANTED_TEXT, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ── СТАТИСТИКА ────────────────────────────────────────
async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ У вас нет прав администратора.")
        return
    data = load_data()
    stats = data.get("stats", {})
    if not stats:
        await update.message.reply_text("📊 *Статистика пока пуста*\n\nУчастники ещё не смотрели видео.", parse_mode="Markdown")
        return
    if context.args:
        uid_str = "".join(context.args).strip().replace("+","")
        user_stat = None
        for key, val in stats.items():
            if key == uid_str or clean_phone(val.get("phone","")) == uid_str:
                user_stat = val; break
        if not user_stat:
            await update.message.reply_text("ℹ️ Пользователь не найден.")
            return
        watched = user_stat.get("watched", [])
        text = f"📊 *Статистика*\n\n📱 {user_stat.get('phone','—')}\n📚 Просмотрено: *{len(watched)} видео*\n📅 _{user_stat.get('last_title','—')}_\n🕐 {user_stat.get('last_date','—')}\n"
        if watched:
            text += "\n*Темы:*\n"
            for w in watched[-15:]: text += f"  • {w}\n"
            if len(watched) > 15: text += f"  _...и ещё {len(watched)-15}_\n"
        await update.message.reply_text(text, parse_mode="Markdown")
        return
    total_views = sum(len(v.get("watched",[])) for v in stats.values())
    text = f"📊 *Статистика просмотров*\n\n👥 Участников: *{len(stats)}*\n👁 Всего просмотров: *{total_views}*\n\n"
    for uid, val in sorted(stats.items(), key=lambda x: len(x[1].get("watched",[])), reverse=True):
        w = len(val.get("watched",[]))
        bar = "█"*min(w,10) + "░"*max(0,10-w)
        text += f"👤 *{val.get('phone',uid)}*\n   {bar} {w} видео | {val.get('last_date','—')}\n   _{val.get('last_title','—')}_\n\n"
    await update.message.reply_text(text, parse_mode="Markdown")

async def mystats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    data = load_data()
    user_stat = data.get("stats", {}).get(uid)
    if not user_stat or not user_stat.get("watched"):
        await update.message.reply_text("📊 *Ваша статистика*\n\nПока пусто — откройте курс и посмотрите видео!", parse_mode="Markdown")
        return
    watched = user_stat.get("watched", [])
    pct = int(len(watched)/77*100)
    bar = "█"*int(pct/10) + "░"*(10-int(pct/10))
    text = f"📊 *Ваша статистика*\n\n{bar} {pct}%\n📚 Просмотрено: *{len(watched)} из 77 видео*\n📅 Последнее: _{user_stat.get('last_title','—')}_\n🕐 {user_stat.get('last_date','—')}\n"
    await update.message.reply_text(text, parse_mode="Markdown")

# ── КОМАНДЫ АДМИНИСТРАТОРА ────────────────────────────
async def add_user(update, context):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ У вас нет прав администратора."); return
    arg = parse_arg(context)
    if not arg:
        await update.message.reply_text("Использование:\n/add +998901234567\n/add 123456789"); return
    data = load_data()
    if is_phone(arg):
        c = clean_phone(arg)
        if c not in data["phones"]:
            data["phones"].append(c); save_data(data)
            await update.message.reply_text(f"✅ Номер +{c} добавлен.")
        else: await update.message.reply_text(f"ℹ️ Уже в списке.")
    else:
        c = clean_phone(arg)
        if c.isdigit():
            tid = int(c)
            if tid not in data["telegram_ids"]:
                data["telegram_ids"].append(tid); save_data(data)
                await update.message.reply_text(f"✅ ID {tid} добавлен.")
            else: await update.message.reply_text(f"ℹ️ Уже в списке.")
        else: await update.message.reply_text("❌ Неверный формат.")

async def remove_user(update, context):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ У вас нет прав."); return
    arg = parse_arg(context)
    if not arg:
        await update.message.reply_text("Использование: /remove +998... или /remove 123..."); return
    data = load_data()
    if is_phone(arg):
        c = clean_phone(arg)
        if c in data["phones"]: data["phones"].remove(c); save_data(data); await update.message.reply_text(f"✅ +{c} удалён.")
        else: await update.message.reply_text(f"ℹ️ Не найден.")
    else:
        c = clean_phone(arg)
        if c.isdigit():
            tid = int(c)
            if tid in data["telegram_ids"]: data["telegram_ids"].remove(tid); save_data(data); await update.message.reply_text(f"✅ ID {tid} удалён.")
            else: await update.message.reply_text(f"ℹ️ Не найден.")

async def list_users(update, context):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ У вас нет прав."); return
    data = load_data()
    phones = data.get("phones", []); tids = data.get("telegram_ids", [])
    admins = data.get("admins", []); admin_phones = data.get("admin_phones", [])
    text = "📋 *Список доступа*\n\n"
    text += f"📱 *Участники по номеру* ({len(phones)}):\n"
    for p in phones: text += f"  +{p}\n"
    text += f"\n🆔 *Участники по ID* ({len(tids)}):\n"
    for t in tids: text += f"  {t}\n"
    text += f"\n👑 *Администраторы* ({len(admins)+len(admin_phones)+1}):\n"
    text += f"  {SUPER_ADMIN_ID} (главный)\n"
    for a in admins: text += f"  {a}\n"
    for p in admin_phones: text += f"  +{p} (по номеру)\n"
    if not phones and not tids: text += "\n_Участников нет_"
    await update.message.reply_text(text, parse_mode="Markdown")

async def add_admin(update, context):
    if not is_super_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только главный администратор."); return
    arg = parse_arg(context)
    if not arg:
        await update.message.reply_text("Использование:\n/addadmin +998...\n/addadmin 123..."); return
    data = load_data()
    if "admin_phones" not in data: data["admin_phones"] = []
    if "admins" not in data: data["admins"] = []
    if is_phone(arg):
        c = clean_phone(arg)
        if c not in data["admin_phones"]:
            data["admin_phones"].append(c); save_data(data)
            await update.message.reply_text(f"✅ Номер +{c} назначен администратором.")
        else: await update.message.reply_text(f"ℹ️ Уже администратор.")
    else:
        c = clean_phone(arg)
        if c.isdigit():
            tid = int(c)
            if tid not in data["admins"]:
                data["admins"].append(tid); save_data(data)
                await update.message.reply_text(f"✅ ID {tid} назначен администратором.")
            else: await update.message.reply_text(f"ℹ️ Уже администратор.")
        else: await update.message.reply_text("❌ Неверный формат.")

async def remove_admin(update, context):
    if not is_super_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только главный администратор."); return
    arg = parse_arg(context)
    if not arg:
        await update.message.reply_text("Использование: /removeadmin +998... или /removeadmin 123..."); return
    data = load_data()
    if is_phone(arg):
        c = clean_phone(arg)
        ap = data.get("admin_phones", [])
        if c in ap: ap.remove(c); data["admin_phones"] = ap; save_data(data); await update.message.reply_text(f"✅ +{c} удалён из администраторов.")
        else: await update.message.reply_text(f"ℹ️ Не найден.")
    else:
        c = clean_phone(arg)
        if c.isdigit():
            tid = int(c)
            if tid in data.get("admins",[]): data["admins"].remove(tid); save_data(data); await update.message.reply_text(f"✅ ID {tid} удалён.")
            else: await update.message.reply_text(f"ℹ️ Не найден.")

async def help_cmd(update, context):
    uid = update.effective_user.id
    if is_super_admin(uid):
        text = ("👑 *Команды главного администратора:*\n\n"
            "/add +998XXXXXXXXX — добавить участника\n/add 123456789 — по ID\n"
            "/remove +998XXXXXXXXX — удалить\n/list — список\n"
            "/addadmin +998XXXXXXXXX — назначить админа\n/removeadmin — снять\n"
            "/stats — статистика всех\n/stats 123... — конкретного\n/start — курс\n")
    elif is_admin(uid):
        text = ("🛠 *Команды администратора:*\n\n"
            "/add +998XXXXXXXXX — добавить\n/remove — удалить\n"
            "/list — список\n/stats — статистика\n/start — курс\n")
    else:
        text = "📚 Напишите /start для доступа к курсу.\n/mystats — ваша статистика"
    await update.message.reply_text(text, parse_mode="Markdown")

# ── ЗАПУСК ────────────────────────────────────────────
def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN не задан!")

    # Создаём приложение бота
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("add", add_user))
    app.add_handler(CommandHandler("remove", remove_user))
    app.add_handler(CommandHandler("list", list_users))
    app.add_handler(CommandHandler("addadmin", add_admin))
    app.add_handler(CommandHandler("removeadmin", remove_admin))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("mystats", mystats_cmd))
    app.add_handler(MessageHandler(filters.CONTACT, contact_handler))

    # Создаём HTTP сервер для приёма статистики от WebApp
    http_app = web.Application()
    http_app.router.add_post('/track', handle_track)
    http_app.router.add_options('/track', handle_track)
    http_app.router.add_get('/health', handle_health)

    async def run_all():
        # Запускаем HTTP сервер
        runner = web.AppRunner(http_app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        logger.info(f"HTTP API запущен на порту {PORT}")

        # Запускаем бота
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        logger.info("Бот запущен!")

        # Держим запущенным
        try:
            await asyncio.Event().wait()
        finally:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
            await runner.cleanup()

    asyncio.run(run_all())

if __name__ == "__main__":
    main()
