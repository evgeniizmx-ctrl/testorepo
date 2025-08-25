# bot.py
import os
import re
import json
import sqlite3
import logging
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger

from dateutil import parser as dparser

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("planner-bot")

# -------- OpenAI ------------
from openai import OpenAI
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

# ---------- ENV -------------
BOT_TOKEN = os.environ.get("BOT_TOKEN") or os.environ.get("TELEGRAM_TOKEN")
PROMPTS_PATH = os.environ.get("PROMPTS_PATH", "prompts.yaml")
DB_PATH = os.environ.get("DB_PATH", "reminders.db")

if not BOT_TOKEN:
    log.error("BOT_TOKEN не задан. Установи BOT_TOKEN (или TELEGRAM_TOKEN) в переменных окружения.")
    raise SystemExit(1)
if not os.environ.get("OPENAI_API_KEY"):
    log.warning("OPENAI_API_KEY не задан — парсер времени упадёт при первом обращении к LLM.")

# ---------- DB --------------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def db_init():
    with db() as conn:
        conn.execute("""
            create table if not exists users (
                user_id integer primary key,
                tz text
            )
        """)
        conn.execute("""
            create table if not exists reminders (
                id integer primary key autoincrement,
                user_id integer not null,
                title text not null,
                body text,
                when_iso text,
                status text default 'scheduled',
                kind text default 'oneoff',         -- 'oneoff' | 'recurring'
                recurrence_json text                -- JSON с {type,weekday,day,time,tz}
            )
        """)
        # Мягкие ALTER для обратной совместимости
        try: conn.execute("alter table reminders add column kind text default 'oneoff'")
        except Exception: pass
        try: conn.execute("alter table reminders add column recurrence_json text")
        except Exception: pass
        conn.commit()

def db_get_user_tz(user_id: int) -> str | None:
    with db() as conn:
        row = conn.execute("select tz from users where user_id=?", (user_id,)).fetchone()
        return row["tz"] if row and row["tz"] else None

def db_set_user_tz(user_id: int, tz: str):
    with db() as conn:
        conn.execute(
            "insert into users(user_id, tz) values(?, ?) "
            "on conflict(user_id) do update set tz=excluded.tz",
            (user_id, tz)
        )
        conn.commit()

def db_add_reminder_oneoff(user_id: int, title: str, body: str | None, when_iso: str) -> int:
    with db() as conn:
        cur = conn.execute(
            "insert into reminders(user_id,title,body,when_iso,kind) values(?,?,?,?,?)",
            (user_id, title, body, when_iso, 'oneoff')
        )
        conn.commit()
        return cur.lastrowid

def db_add_reminder_recurring(user_id: int, title: str, body: str | None, recurrence: dict, tz: str) -> int:
    rec = dict(recurrence or {})
    rec["tz"] = tz
    with db() as conn:
        cur = conn.execute(
            "insert into reminders(user_id,title,body,when_iso,kind,recurrence_json) values(?,?,?,?,?,?)",
            (user_id, title, body, None, 'recurring', json.dumps(rec, ensure_ascii=False))
        )
        conn.commit()
        return cur.lastrowid

def db_mark_done(rem_id: int):
    with db() as conn:
        conn.execute("update reminders set status='done' where id=?", (rem_id,))
        conn.commit()

def db_snooze(rem_id: int, minutes: int):
    with db() as conn:
        row = conn.execute("select when_iso, kind from reminders where id=?", (rem_id,)).fetchone()
        if not row:
            return None, None
        if row["kind"] == "recurring":
            return "recurring", None
        dt = parse_iso_flexible(row["when_iso"]) + timedelta(minutes=minutes)
        new_iso = iso_no_seconds(dt)
        conn.execute("update reminders set when_iso=?, status='scheduled' where id=?", (new_iso, rem_id))
        conn.commit()
        return "oneoff", dt

def db_delete(rem_id: int):
    with db() as conn:
        conn.execute("delete from reminders where id=?", (rem_id,))
        conn.commit()

def db_future(user_id: int):
    with db() as conn:
        rows = conn.execute(
            "select * from reminders where user_id=? and status='scheduled' order by id desc",
            (user_id,)
        ).fetchall()
        return rows

def db_get_reminder(rem_id: int):
    with db() as conn:
        return conn.execute("select * from reminders where id=?", (rem_id,)).fetchone()

# ---------- TZ utils --------
def tzinfo_from_user(tz_str: str) -> timezone | ZoneInfo:
    """Конвертация СТРОГО валидной строки TZ в tzinfo. Предполагается, что валидация уже прошла."""
    if not tz_str:
        return timezone(timedelta(hours=3))
    tz_str = tz_str.strip()
    if tz_str[0] in "+-":
        m = re.fullmatch(r"([+-])(\d{1,2})(?::?(\d{2}))?$", tz_str)
        if not m:
            raise ValueError("invalid offset")
        sign, hh, mm = m.group(1), int(m.group(2)), int(m.group(3) or 0)
        delta = timedelta(hours=hh, minutes=mm)
        if sign == "-":
            delta = -delta
        return timezone(delta)
    return ZoneInfo(tz_str)

def now_in_user_tz(tz_str: str) -> datetime:
    return datetime.now(tzinfo_from_user(tz_str))

def iso_no_seconds(dt: datetime) -> str:
    dt = dt.replace(microsecond=0)
    s = dt.isoformat()
    s = re.sub(r":\d{2}([+-Z])", r"\1", s) if re.search(r"T\d{2}:\d{2}:\d{2}", s) else s
    return s

def parse_iso_flexible(s: str) -> datetime:
    return dparser.isoparse(s)

# ---------- UI: Keyboards ---
MAIN_MENU_KB = ReplyKeyboardMarkup(
    [[KeyboardButton("📝 Список напоминаний"), KeyboardButton("⚙️ Настройки")]],
    resize_keyboard=True, one_time_keyboard=False
)

_TZ_ROWS = [
    ["Калининград (+2)", "Москва (+3)"],
    ["Самара (+4)", "Екатеринбург (+5)"],
    ["Омск (+6)", "Новосибирск (+7)"],
    ["Иркутск (+8)", "Якутск (+9)"],
    ["Хабаровск (+10)", "Другой…"],
]
CITY_TO_OFFSET = {
    "Калининград (+2)": "+02:00",
    "Москва (+3)": "+03:00",
    "Самара (+4)": "+04:00",
    "Екатеринбург (+5)": "+05:00",
    "Омск (+6)": "+06:00",
    "Новосибирск (+7)": "+07:00",
    "Иркутск (+8)": "+08:00",
    "Якутск (+9)": "+09:00",
    "Хабаровск (+10)": "+10:00",
}

def build_tz_inline_kb() -> InlineKeyboardMarkup:
    rows = []
    for row in _TZ_ROWS:
        btns = []
        for label in row:
            if label == "Другой…":
                btns.append(InlineKeyboardButton(label, callback_data="tz:other"))
            else:
                off = CITY_TO_OFFSET[label]
                btns.append(InlineKeyboardButton(label, callback_data=f"tz:{off}"))
        rows.append(btns)
    return InlineKeyboardMarkup(rows)

# ---------- TZ parsing (строгая валидация) ----------
def normalize_offset(sign: str, hh: str, mm: str | None) -> str:
    h = int(hh); m = int(mm or 0)
    return f"{sign}{h:02d}:{m:02d}"

def parse_tz_input(text: str) -> str | None:
    """Вернёт нормализованный TZ ('+03:00' или 'Europe/Moscow') или None если это НЕ TZ."""
    if not text:
        return None
    t = text.strip()
    # кнопка-город — тут не ловим (ее обрабатывает cb_tz), но если вдруг пришла строкой:
    if t in CITY_TO_OFFSET:
        return CITY_TO_OFFSET[t]
    # смещение
    m = re.fullmatch(r"([+-])(\d{1,2})(?::?(\d{2}))?$", t)
    if m:
        return normalize_offset(m.group(1), m.group(2), m.group(3))
    # IANA
    if "/" in t and " " not in t:
        try:
            _ = ZoneInfo(t)
            return t
        except Exception:
            return None
    return None

# ---------- Prompts ---------
import yaml
def load_prompts():
    with open(PROMPTS_PATH, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return raw

PROMPTS = load_prompts()

# ---------- LLM ------------
async def call_llm(user_text: str, user_tz: str) -> dict:
    now_local = now_in_user_tz(user_tz)
    now_iso = iso_no_seconds(now_local)
    header = f"NOW_ISO={now_iso}\nTZ_DEFAULT={user_tz or '+03:00'}"

    messages = [
        {"role": "system", "content": PROMPTS["system"]},
        {"role": "system", "content": header},
        {"role": "system", "content": PROMPTS["parse"]["system"]},
    ]

    few = PROMPTS.get("fewshot") or []
    messages.extend(few)

    messages.append({"role": "user", "content": user_text})

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0.2
    )
    txt = resp.choices[0].message.content.strip()
    try:
        return json.loads(txt)
    except Exception:
        m = re.search(r"\{[\s\S]+\}", txt)
        if m:
            return json.loads(m.group(0))
        raise

# ---------- Scheduler -------
scheduler = AsyncIOScheduler()

async def fire_reminder(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    data = job.data
    chat_id = data["chat_id"]
    rem_id = data["rem_id"]
    title = data["title"]
    kind = data.get("kind", "oneoff")

    kb_rows = [[
        InlineKeyboardButton("Через 10 мин", callback_data=f"snooze:10:{rem_id}"),
        InlineKeyboardButton("Через 1 час", callback_data=f"snooze:60:{rem_id}")
    ]]
    if kind == "oneoff":
        kb_rows.append([InlineKeyboardButton("✅", callback_data=f"done:{rem_id}")])

    await context.bot.send_message(chat_id, f"🔔 «{title}»", reply_markup=InlineKeyboardMarkup(kb_rows))

def schedule_oneoff(rem_id: int, user_id: int, when_iso: str, title: str, kind: str = "oneoff"):
    dt = parse_iso_flexible(when_iso)
    scheduler.add_job(
        fire_reminder,
        trigger=DateTrigger(run_date=dt),
        id=f"rem-{rem_id}",
        replace_existing=True,
        misfire_grace_time=60,
        coalesce=True,
        name=f"rem {rem_id}",
    )
    job = scheduler.get_job(f"rem-{rem_id}")
    if job:
        job.data = {"chat_id": user_id, "rem_id": rem_id, "title": title, "kind": kind}

def schedule_recurring(rem_id: int, user_id: int, title: str, recurrence: dict, tz_str: str):
    tzinfo = tzinfo_from_user(tz_str)
    rtype = recurrence.get("type")
    time_str = recurrence.get("time")
    hh, mm = map(int, time_str.split(":"))
    if rtype == "daily":
        trigger = CronTrigger(hour=hh, minute=mm, timezone=tzinfo)
    elif rtype == "weekly":
        trigger = CronTrigger(day_of_week=recurrence.get("weekday"), hour=hh, minute=mm, timezone=tzinfo)
    elif rtype == "monthly":
        trigger = CronTrigger(day=int(recurrence.get("day")), hour=hh, minute=mm, timezone=tzinfo)
    else:
        return
    scheduler.add_job(
        fire_reminder,
        trigger=trigger,
        id=f"rem-{rem_id}",
        replace_existing=True,
        misfire_grace_time=300,
        coalesce=True,
        name=f"rem {rem_id}",
    )
    job = scheduler.get_job(f"rem-{rem_id}")
    if job:
        job.data = {"chat_id": user_id, "rem_id": rem_id, "title": title, "kind": "recurring"}

# ---------- Handlers --------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    tz = db_get_user_tz(user_id)
    if not tz:
        await update.message.reply_text(
            "Для начала укажи свой часовой пояс.\n"
            "Выбери город или пришли вручную смещение (+03:00) или IANA (Europe/Moscow).",
            reply_markup=MAIN_MENU_KB
        )
        await update.message.reply_text("Выбери из списка:", reply_markup=build_tz_inline_kb())
        return
    await update.message.reply_text(
        f"Часовой пояс установлен: {tz}\nТеперь напиши что и когда напомнить.",
        reply_markup=MAIN_MENU_KB
    )

async def try_handle_tz_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Обрабатывает ВВОД ТЕКСТОМ (не кнопками). Возвращает True, если это был TZ."""
    if not update.message or not update.message.text:
        return False
    text = update.message.text.strip()
    user_id = update.effective_user.id

    tz = parse_tz_input(text)
    if tz is None:
        return False

    db_set_user_tz(user_id, tz)
    log.info("TZ set via text: user=%s tz=%s", user_id, tz)
    await update.message.reply_text(
        f"Часовой пояс установлен: {tz}\nТеперь напиши что и когда напомнить.",
        reply_markup=MAIN_MENU_KB
    )
    return True

async def cb_tz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор из inline-клавиатуры tz:*"""
    q = update.callback_query
    await q.answer()
    data = q.data  # tz:<value>|tz:other
    if not data.startswith("tz:"):
        return
    value = data.split(":", 1)[1]
    chat_id = q.message.chat.id

    if value == "other":
        await q.edit_message_text("Пришли смещение вида +03:00 или IANA-зону (Europe/Moscow).")
        return

    db_set_user_tz(chat_id, value)
    log.info("TZ set via inline: user=%s tz=%s", chat_id, value)
    await q.edit_message_text(f"Часовой пояс установлен: {value}\nТеперь напиши что и когда напомнить.")

async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    rows = db_future(user_id)
    if not rows:
        await update.message.reply_text("Будущих напоминаний нет.", reply_markup=MAIN_MENU_KB)
        return

    lines = ["🗓 Ближайшие напоминания —"]
    kb_rows = []
    tz = db_get_user_tz(user_id) or "+03:00"
    for r in rows:
        title = r["title"]
        kind = r["kind"] or "oneoff"
        if kind == "oneoff" and r["when_iso"]:
            dt_local = parse_iso_flexible(r["when_iso"]).astimezone(tzinfo_from_user(tz))
            line = f"• {dt_local.strftime('%d.%m в %H:%M')} — «{title}»"
        else:
            rec = json.loads(r["recurrence_json"]) if r["recurrence_json"] else {}
            rtype = rec.get("type")
            if rtype == "daily":
                line = f"• Каждый день в {rec.get('time')} — «{title}»"
            elif rtype == "weekly":
                line = f"• Каждую {rec.get('weekday')} в {rec.get('time')} — «{title}»"
            else:
                line = f"• Каждое {rec.get('day')}-е в {rec.get('time')} — «{title}»"
        lines.append(line)
        kb_rows.append([InlineKeyboardButton("🗑 Удалить", callback_data=f"del:{r['id']}")])

    await update.message.reply_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(kb_rows))

async def cb_inline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""

    if data.startswith("del:"):
        rem_id = int(data.split(":")[1])
        db_delete(rem_id)
        job = scheduler.get_job(f"rem-{rem_id}")
        if job: job.remove()
        await q.edit_message_text("Удалено ✅")
        return

    if data.startswith("snooze:"):
        _, mins, rem_id = data.split(":")
        rem_id = int(rem_id); mins = int(mins)
        kind, _ = db_snooze(rem_id, mins)
        row = db_get_reminder(rem_id)
        if not row:
            await q.edit_message_text("Ошибка: напоминание не найдено.")
            return
        if kind == "oneoff":
            schedule_oneoff(rem_id, row["user_id"], row["when_iso"], row["title"], kind="oneoff")
            await q.edit_message_text(f"⏲ Отложено на {mins} мин.")
        else:
            when = iso_no_seconds(datetime.now(timezone.utc) + timedelta(minutes=mins))
            tmp_job_id = f"snooze-{rem_id}"
            scheduler.add_job(
                fire_reminder,
                trigger=DateTrigger(run_date=parse_iso_flexible(when)),
                id=tmp_job_id,
                replace_existing=True,
                misfire_grace_time=60,
                coalesce=True,
                name=f"snooze {rem_id}",
            )
            job = scheduler.get_job(tmp_job_id)
            if job:
                job.data = {"chat_id": row["user_id"], "rem_id": rem_id, "title": row["title"], "kind": "oneoff"}
            await q.edit_message_text(f"⏲ Отложено на {mins} мин. (одноразово)")
        return

    if data.startswith("done:"):
        rem_id = int(data.split(":")[1])
        db_mark_done(rem_id)
        job = scheduler.get_job(f"rem-{rem_id}")
        if job: job.remove()
        await q.edit_message_text("✅ Выполнено")
        return

async def cb_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if data.startswith("pick:"):
        iso = data.split("pick:")[1]
        user_id = q.message.chat.id
        title = "Напоминание"
        rem_id = db_add_reminder_oneoff(user_id, title, None, iso)
        schedule_oneoff(rem_id, user_id, iso, title, kind="oneoff")
        tz = db_get_user_tz(user_id) or "+03:00"
        dt_local = parse_iso_flexible(iso).astimezone(tzinfo_from_user(tz))
        await q.edit_message_text(f"📅 Окей, напомню «{title}» {dt_local.strftime('%d.%m в %H:%M')}")

async def cb_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("answer:"):
        return
    choice = data.split("answer:", 1)[1]
    context.user_data["__auto_answer"] = choice
    await handle_text(update, context)

# ---------- Clarification memory ----------
def get_clarify_state(context: ContextTypes.DEFAULT_TYPE):
    return context.user_data.get("clarify_state")

def set_clarify_state(context: ContextTypes.DEFAULT_TYPE, state: dict | None):
    if state is None:
        context.user_data.pop("clarify_state", None)
    else:
        context.user_data["clarify_state"] = state

# ---------- main text handler ----------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await try_handle_tz_input(update, context):
        return

    user_id = update.effective_user.id
    incoming_text = (context.user_data.pop("__auto_answer", None)
                     or (update.message.text.strip() if update.message and update.message.text else ""))

    if incoming_text == "📝 Список напоминаний" or incoming_text.lower() == "/list":
        return await cmd_list(update, context)
    if incoming_text == "⚙️ Настройки" or incoming_text.lower() == "/settings":
        return await update.message.reply_text("Раздел «Настройки» в разработке.", reply_markup=MAIN_MENU_KB)

    user_tz = db_get_user_tz(user_id)
    if not user_tz:
        await update.message.reply_text(
            "Сначала укажи часовой пояс. Выбери из списка ниже или пришли вручную:",
            reply_markup=MAIN_MENU_KB
        )
        await update.message.reply_text("Выбери из списка:", reply_markup=build_tz_inline_kb())
        return

    cstate = get_clarify_state(context)
    user_text_for_llm = (f"Исходная заявка: {cstate['original']}\nОтвет на уточнение: {incoming_text}"
                         if cstate else incoming_text)

    try:
        result = await call_llm(user_text_for_llm, user_tz)
    except Exception:
        return await update.message.reply_text("Что-то не понял. Скажи, например: «завтра в 15 позвонить маме».")
    intent = result.get("intent")

    if intent == "ask_clarification":
        question = result.get("question") or "Уточни, пожалуйста."
        variants = result.get("variants") or []
        original = cstate['original'] if cstate else (result.get("text_original") or incoming_text)
        set_clarify_state(context, {"original": original})

        kb_rows = []
        for v in variants[:6]:
            if isinstance(v, dict):
                label = v.get("label") or v.get("text") or v.get("iso_datetime") or "Выбрать"
                iso = v.get("iso_datetime")
                kb_rows.append([InlineKeyboardButton(label, callback_data=(f"pick:{iso}" if iso else f"answer:{label}"))])
            else:
                kb_rows.append([InlineKeyboardButton(str(v), callback_data=f"answer:{v}")])

        return await update.message.reply_text(question, reply_markup=InlineKeyboardMarkup(kb_rows) if kb_rows else None)

    if intent == "create_reminder":
        set_clarify_state(context, None)
        title = result.get("title") or "Напоминание"
        body = result.get("description")
        dt_iso = result.get("fixed_datetime")
        recurrence = result.get("recurrence")

        if recurrence:
            rem_id = db_add_reminder_recurring(user_id, title, body, recurrence, user_tz)
            schedule_recurring(rem_id, user_id, title, recurrence, user_tz)
            rtype = recurrence.get("type")
            if rtype == "daily":
                text = f"📅 Окей, буду напоминать «{title}» каждый день в {recurrence.get('time')}"
            elif rtype == "weekly":
                text = f"📅 Окей, буду напоминать «{title}» каждую {recurrence.get('weekday')} в {recurrence.get('time')}"
            else:
                text = f"📅 Окей, буду напоминать «{title}» каждое {recurrence.get('day')}-е число в {recurrence.get('time')}"
            return await update.message.reply_text(text, reply_markup=MAIN_MENU_KB)

        if not dt_iso:
            return await update.message.reply_text("Не понял время. Напиши, например: «сегодня 18:30».")
        dt = parse_iso_flexible(dt_iso)
        dt_iso_clean = iso_no_seconds(dt)
        rem_id = db_add_reminder_oneoff(user_id, title, body, dt_iso_clean)
        schedule_oneoff(rem_id, user_id, dt_iso_clean, title, kind="oneoff")

        tz = db_get_user_tz(user_id) or "+03:00"
        dt_local = parse_iso_flexible(dt_iso_clean).astimezone(tzinfo_from_user(tz))
        return await update.message.reply_text(
            f"📅 Окей, напомню «{title}» {dt_local.strftime('%d.%m в %H:%M')}",
            reply_markup=MAIN_MENU_KB
        )

    set_clarify_state(context, None)
    await update.message.reply_text("Я не понял, попробуй ещё раз.", reply_markup=MAIN_MENU_KB)

# ---------- main -----------
def main():
    log.info("Starting PlannerBot...")
    db_init()
    log.info("DB init done")
    try:
        app = Application.builder().token(BOT_TOKEN).build()
    except Exception as e:
        log.exception("Ошибка инициализации Telegram Application: %s", e)
        raise

    scheduler.start()
    log.info("Scheduler started")

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CommandHandler("settings", lambda u,c: u.message.reply_text(
        "Раздел «Настройки» в разработке.", reply_markup=MAIN_MENU_KB)))

    app.add_handler(CallbackQueryHandler(cb_tz, pattern=r"^tz:"))
    app.add_handler(CallbackQueryHandler(cb_inline, pattern=r"^(del:|done:|snooze:)"))
    app.add_handler(CallbackQueryHandler(cb_pick, pattern=r"^pick:"))
    app.add_handler(CallbackQueryHandler(cb_answer, pattern=r"^answer:"))

    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))

    try:
        log.info("Run polling...")
        app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        log.exception("run_polling упал: %s", e)
        raise

if __name__ == "__main__":
    main()
