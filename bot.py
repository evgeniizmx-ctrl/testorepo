import os
import json
import re
import asyncio
from datetime import datetime, timedelta, date, time as dtime
import pytz

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command
from aiogram.enums import ChatAction

import httpx
from dateutil import parser as dateparser
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ===================== ОКРУЖЕНИЕ =====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OCR_SPACE_API_KEY = os.getenv("OCR_SPACE_API_KEY")

TZ = os.getenv("APP_TZ", "Europe/Moscow")
tz = pytz.timezone(TZ)

# ===================== ИНИЦИАЛИЗАЦИЯ =====================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TZ)

# PENDING[user_id] = {
#   "description": str,
#   "repeat": "none|daily|weekly",
#   "variants": [datetime, ...],   # если ждём выбор времени с кнопок
#   "base_date": date              # если известен день, но ждём только время
# }
PENDING: dict[int, dict] = {}
REMINDERS: list[dict] = []

# ===================== УТИЛИТЫ =====================
async def send_reminder(user_id: int, text: str):
    try:
        await bot.send_message(user_id, f"🔔 Напоминание: {text}")
    except Exception as e:
        print("Send reminder error:", e)

def schedule_one(reminder: dict):
    scheduler.add_job(send_reminder, "date",
                      run_date=reminder["remind_dt"],
                      args=[reminder["user_id"], reminder["text"]])

def as_local_iso(dt_like: str | None) -> datetime | None:
    if not dt_like:
        return None
    try:
        dt = dateparser.parse(dt_like)
        if not dt:
            return None
        dt = tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)
        return dt.replace(second=0, microsecond=0)
    except Exception:
        return None

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()

def clean_description(desc: str) -> str:
    d = desc.strip()
    d = re.sub(r"^(напомни(те)?|пожалуйста)\b[\s,:-]*", "", d, flags=re.IGNORECASE)
    d = re.sub(r"^(о|про|насч[её]т)\s+", "", d, flags=re.IGNORECASE)
    d = re.sub(r"^(сегодня|завтра|послезавтра)\b", "", d, flags=re.IGNORECASE).strip()
    return d or "Напоминание"

def _variants_keyboard(variants: list[datetime]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=dt.strftime("%d.%m %H:%M"),
                              callback_data=f"time|{dt.isoformat()}")]
        for dt in variants
    ])

def _mk_dt(base_d: date, hh: int, mm: int) -> datetime:
    return tz.localize(datetime(base_d.year, base_d.month, base_d.day, hh % 24, mm % 60))

# ===================== ПАРСЕРЫ =====================
# --- «через … / спустя … / полчаса / минуту/час/день» ---
REL_NUM_PATTERNS = [
    (r"(через|спустя)\s+(\d+)\s*(секунд(?:у|ы)?|сек\.?)\b", "seconds"),
    (r"(через|спустя)\s+(\d+)\s*(минут(?:у|ы)?|мин\.?)\b", "minutes"),
    (r"(через|спустя)\s+(\d+)\s*(час(?:а|ов)?|ч\.?)\b",     "hours"),
    (r"(через|спустя)\s+(\d+)\s*(дн(?:я|ей)?|день|дн\.?)\b","days"),
]
REL_NUM_REGEXES = [re.compile(p, re.IGNORECASE | re.UNICODE | re.DOTALL) for p, _ in REL_NUM_PATTERNS]
REL_SINGULAR = [
    (re.compile(r"(через|спустя)\s+секунд(?:у)\b", re.I), "seconds", 1),
    (re.compile(r"(через|спустя)\s+минут(?:у|ку)\b", re.I), "minutes", 1),
    (re.compile(r"(через|спустя)\s+час\b", re.I), "hours", 1),
    (re.compile(r"(через|спустя)\s+день\b", re.I), "days", 1),
]
REL_HALF_HOUR_RX = re.compile(r"(через)\s+пол\s*часа\b", re.IGNORECASE | re.UNICODE)

def parse_relative_phrase(raw_text: str):
    s = normalize_spaces(raw_text)
    now = datetime.now(tz).replace(second=0, microsecond=0)

    m = REL_HALF_HOUR_RX.search(s)
    if m:
        dt = now + timedelta(minutes=30)
        remainder = (s[:m.start()] + s[m.end():]).strip(" ,.-")
        return dt, remainder

    for rx, kind, val in REL_SINGULAR:
        m = rx.search(s)
        if m:
            if kind == "seconds": dt = now + timedelta(seconds=val)
            elif kind == "minutes": dt = now + timedelta(minutes=val)
            elif kind == "hours":   dt = now + timedelta(hours=val)
            elif kind == "days":    dt = now + timedelta(days=val)
            remainder = (s[:m.start()] + s[m.end():]).strip(" ,.-")
            return dt, remainder

    for rx, (_, kind) in zip(REL_NUM_REGEXES, REL_NUM_PATTERNS):
        m = rx.search(s)
        if not m:
            continue
        amount = int(m.group(2))
        if kind == "seconds": dt = now + timedelta(seconds=amount)
        elif kind == "minutes": dt = now + timedelta(minutes=amount)
        elif kind == "hours":   dt = now + timedelta(hours=amount)
        elif kind == "days":    dt = now + timedelta(days=amount)
        remainder = (s[:m.start()] + s[m.end():]).strip(" ,.-")
        return dt, remainder

    return None

# --- «в это же время» (завтра/послезавтра/через N дней) ---
SAME_TIME_RX = re.compile(r"\bв это же время\b", re.I | re.UNICODE)
TOMORROW_RX = re.compile(r"\bзавтра\b", re.I | re.UNICODE)
AFTER_TOMORROW_RX = re.compile(r"\bпослезавтра\b", re.I | re.UNICODE)
IN_N_DAYS_RX = re.compile(r"(через|спустя)\s+(\d+)\s*(дн(?:я|ей)?|день|дн\.?)\b", re.I | re.UNICODE)

def parse_same_time_phrase(raw_text: str):
    s = normalize_spaces(raw_text)
    if not SAME_TIME_RX.search(s):
        return None
    now = datetime.now(tz).replace(second=0, microsecond=0)
    days = None
    if AFTER_TOMORROW_RX.search(s): days = 2
    elif TOMORROW_RX.search(s):     days = 1
    else:
        m = IN_N_DAYS_RX.search(s)
        if m:
            try: days = int(m.group(2))
            except: days = None
    if days is None:
        return None
    target = (now + timedelta(days=days)).replace(hour=now.hour, minute=now.minute)
    remainder = s
    for rx in (SAME_TIME_RX, TOMORROW_RX, AFTER_TOMORROW_RX, IN_N_DAYS_RX):
        remainder = rx.sub("", remainder)
    remainder = remainder.strip(" ,.-")
    return target, remainder

# --- «сегодня/завтра/послезавтра … в HH[:MM] (утра/дня/вечера/ночи)» ---
DAYTIME_RX = re.compile(
    r"\b(сегодня|завтра|послезавтра)\b.*?\bв\s*(\d{1,2})(?::(\d{2}))?(?:\s*(утра|дня|вечера|ночи))?\b",
    re.IGNORECASE | re.UNICODE | re.DOTALL
)

def parse_daytime_phrase(raw_text: str):
    s = normalize_spaces(raw_text)
    m = DAYTIME_RX.search(s)
    if not m:
        return None

    day_word = m.group(1).lower()
    hour_raw = int(m.group(2))
    minute = int(m.group(3) or 0)
    mer = (m.group(4) or "").lower()

    now = datetime.now(tz).replace(second=0, microsecond=0)
    base = now if day_word == "сегодня" else (now + timedelta(days=1 if day_word == "завтра" else 2))
    remainder = (s[:m.start()] + s[m.end():]).strip(" ,.-")

    if mer in ("утра", "дня", "вечера", "ночи"):
        h = hour_raw
        if mer in ("дня", "вечера") and h < 12: h += 12
        if mer == "ночи" and h == 12: h = 0
        target = base.replace(hour=h % 24, minute=minute % 60)
        return ("ok", target, remainder)

    # без «утра/вечера» → два варианта
    dt1 = base.replace(hour=hour_raw % 24, minute=minute % 60)
    h2 = 0 if hour_raw == 12 else (hour_raw + 12) % 24
    dt2 = base.replace(hour=h2, minute=minute % 60)
    return ("amb", remainder, [dt1, dt2])

# --- «просто в HH[:MM]» (без дня) → кнопки/однозначно ---
ONLYTIME_RX = re.compile(r"\bв\s*(\d{1,2})(?::(\d{2}))?\b", re.I | re.UNICODE)

def parse_onlytime_phrase(raw_text: str):
    s = normalize_spaces(raw_text)
    m = ONLYTIME_RX.search(s)
    if not m:
        return None
    hour_raw = int(m.group(1))
    minute = int(m.group(2) or 0)
    now = datetime.now(tz).replace(second=0, microsecond=0)

    mer_m = re.search(r"(утра|вечера|дня|ночи)", s, re.IGNORECASE)
    if mer_m:
        mer = mer_m.group(1).lower()
        h = hour_raw
        if mer in ("дня", "вечера") and h < 12: h += 12
        if mer == "ночи" and h == 12: h = 0
        target = now.replace(hour=h % 24, minute=minute % 60)
        if target <= now: target += timedelta(days=1)
        remainder = (s[:m.start()] + s[m.end():]).strip(" ,.-")
        return ("ok", target, remainder)

    # двусмысленно → варианты (сегодня): HH и HH+12, если прошло — на завтра
    cand = []
    for h in [hour_raw % 24, (hour_raw + 12) % 24]:
        dt = now.replace(hour=h, minute=minute % 60)
        if dt <= now: dt += timedelta(days=1)
        cand.append(dt)
    remainder = (s[:m.start()] + s[m.end():]).strip(" ,.-")
    return ("amb", remainder, cand)

# --- НОВОЕ: «только дата без времени» (числом) ---
# ловим: 25, 25 числа, на 25; 25.09; 25/9; 25-09-2025; и т.д., если НЕТ времени «в \d»
DATE_NUM_RX = re.compile(
    r"(?:\bна\s+)?\b(\d{1,2})\b(?:\s*числа)?(?!\s*[:.]\d)\b(?!.*\bв\s*\d)", re.IGNORECASE | re.UNICODE
)
DATE_DOT_RX = re.compile(
    r"\b(\d{1,2})[.\-\/](\d{1,2})(?:[.\-\/](\d{2,4}))?\b(?!.*\bв\s*\d)", re.IGNORECASE | re.UNICODE
)

def nearest_future_day(day: int, now: datetime) -> date:
    """Ближайшее будущее число (day) в этом или следующем месяце."""
    y, m = now.year, now.month
    try:
        candidate = date(y, m, day)
    except ValueError:
        # если дня нет в этом месяце — берём след. месяц
        m2 = m + 1 if m < 12 else 1
        y2 = y if m < 12 else y + 1
        return date(y2, m2, min(day, 28))
    if candidate <= now.date():
        m2 = m + 1 if m < 12 else 1
        y2 = y if m < 12 else y + 1
        # защищаемся от 31/30 февраля и т.п.
        for dcap in (31, 30, 29, 28):
            try:
                return date(y2, m2, min(day, dcap))
            except ValueError:
                continue
    return candidate

def parse_numeric_date_only(raw_text: str):
    s = normalize_spaces(raw_text)
    # DD.MM(.YYYY)
    m = DATE_DOT_RX.search(s)
    if m:
        dd = int(m.group(1)); mm = int(m.group(2))
        yy = m.group(3)
        now = datetime.now(tz)
        if yy:
            yyyy = int(yy)
            if yyyy < 100: yyyy += 2000
        else:
            yyyy = now.year
        try:
            base = date(yyyy, mm, dd)
        except ValueError:
            return None
        desc = clean_description(DATE_DOT_RX.sub("", s))
        return ("day", base, desc)

    # «25», «25 числа», «на 25» — ближайшее будущее
    m2 = DATE_NUM_RX.search(s)
    if m2:
        dd = int(m2.group(1))
        now = datetime.now(tz)
        base = nearest_future_day(dd, now)
        desc = clean_description(DATE_NUM_RX.sub("", s))
        return ("day", base, desc)

    return None

# ===================== OpenAI (fallback при полной неразберихе) =====================
OPENAI_BASE = "https://api.openai.com/v1"

async def gpt_parse(text: str) -> dict:
    system = (
        "Ты — ассистент-напоминалка. Верни СТРОГО JSON с ключами: "
        "description, event_time, remind_time, repeat(daily|weekly|none), "
        "needs_clarification, clarification_question. "
        "Даты/время в 'YYYY-MM-DD HH:MM' (24h). Язык — русский."
    )
    payload = {
        "model": "gpt-4o-mini",
        "messages": [{"role": "system", "content": system},
                     {"role": "user", "content": text}],
        "temperature": 0
    }
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(f"{OPENAI_BASE}/chat/completions", headers=headers, json=payload)
            r.raise_for_status()
            answer = r.json()["choices"][0]["message"]["content"]
        return json.loads(answer)
    except Exception as e:
        print("GPT parse fail:", e)
        return {"description": text, "event_time": "", "remind_time": "",
                "repeat": "none", "needs_clarification": True,
                "clarification_question": "Уточните дату и время (например, 25.08 14:25)."}

# ===================== КОМАНДЫ =====================
@dp.message(Command("start"))
async def start(message: Message):
    await message.answer(
        "Привет! Я бот-напоминалка.\n"
        "Понимаю: «послезавтра свадьба», «Свадьба 25», «25.09», «в 10», «в 17 часов», «через 3 минуты», «завтра в 5».\n"
        "Если указал только день — спрошу лишь время (ответ «18» — сразу 18:00; «6» — предложу 06:00/18:00).\n"
        "Голос/скрин тоже можно. /list — список, /ping — проверка."
    )

@dp.message(Command("ping"))
async def ping(message: Message):
    await message.answer("pong ✅")

@dp.message(Command("list"))
async def list_cmd(message: Message):
    uid = message.from_user.id
    items = [r for r in REMINDERS if r["user_id"] == uid]
    if not items:
        await message.answer("Пока нет напоминаний (в этой сессии).")
        return
    lines = [
        f"• {r['text']} — {r['remind_dt'].strftime('%d.%m %H:%M')} ({TZ})"
        + (f" [{r['repeat']}]" if r['repeat'] != "none" else "")
        for r in items
    ]
    await message.answer("\n".join(lines))

# ===================== ОСНОВНАЯ ЛОГИКА =====================
@dp.message(F.text)
async def on_any_text(message: Message):
    uid = message.from_user.id
    text_raw = message.text or ""
    text = normalize_spaces(text_raw)

    # 0) если ждём уточнение
    if uid in PENDING:
        st = PENDING[uid]

        # ждём выбор по кнопкам
        if st.get("variants"):
            await message.reply("Нажмите одну из кнопок ниже, чтобы выбрать время ⬇️")
            return

        # известен день — ждём только время
        if st.get("base_date"):
            m = re.search(r"(?:^|\bв\s*)(\d{1,2})(?::(\d{2}))?\s*(утра|дня|вечера|ночи)?\b", text, re.IGNORECASE)
            if not m:
                await message.reply("Во сколько? Например: 6, 18, 6:30, «6 утра» или «6 вечера».")
                return
            hour = int(m.group(1))
            minute = int(m.group(2) or 0)
            mer = (m.group(3) or "").lower()

            base_d: date = st["base_date"]

            # метка «утра/вечера» — однозначно
            if mer:
                h = hour
                if mer in ("дня", "вечера") and h < 12: h += 12
                if mer == "ночи" and h == 12: h = 0
                dt = _mk_dt(base_d, h, minute)
                desc = st.get("description", "Напоминание")
                PENDING.pop(uid, None)
                REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": "none"})
                schedule_one(REMINDERS[-1])
                await message.reply(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({TZ})")
                return

            # «18» — сразу 18:00
            if hour == 18 and minute == 0:
                dt = _mk_dt(base_d, 18, 0)
                desc = st.get("description", "Напоминание")
                PENDING.pop(uid, None)
                REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": "none"})
                schedule_one(REMINDERS[-1])
                await message.reply(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({TZ})")
                return

            # иначе двусмысленно: HH и HH+12
            v1 = _mk_dt(base_d, hour, minute)
            v2 = _mk_dt(base_d, (hour + 12) % 24, minute)
            PENDING[uid]["variants"] = [v1, v2]
            await message.reply("Уточните время:", reply_markup=_variants_keyboard([v1, v2]))
            return

        # обычная ветка уточнения (без base_date)
        for parser in (parse_daytime_phrase, parse_onlytime_phrase):
            pack = parser(text)
            if pack:
                tag = pack[0]
                if tag == "amb":
                    _, remainder, variants = pack
                    desc = clean_description(remainder or st.get("description", text))
                    PENDING[uid] = {"description": desc, "variants": variants, "repeat": "none"}
                    await message.reply(f"Уточните, во сколько напомнить «{desc}»?", reply_markup=_variants_keyboard(variants))
                    return
                else:
                    _, dt, remainder = pack
                    desc = clean_description(remainder or st.get("description", text))
                    PENDING.pop(uid, None)
                    REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": st.get("repeat","none")})
                    schedule_one(REMINDERS[-1])
                    await message.reply(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({TZ})")
                    return

        rel = parse_relative_phrase(text)
        if rel:
            dt, remainder = rel
            desc = clean_description(remainder or st.get("description", text))
            PENDING.pop(uid, None)
            REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": st.get("repeat","none")})
            schedule_one(REMINDERS[-1])
            await message.reply(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({TZ})")
            return

        same = parse_same_time_phrase(text)
        if same:
            dt, remainder = same
            desc = clean_description(remainder or st.get("description", text))
            PENDING.pop(uid, None)
            REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": st.get("repeat","none")})
            schedule_one(REMINDERS[-1])
            await message.reply(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({TZ})")
            return

        # свободный парс
        dt = as_local_iso(text)
        if dt:
            desc = clean_description(st.get("description", text))
            PENDING.pop(uid, None)
            REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": st.get("repeat","none")})
            schedule_one(REMINDERS[-1])
            await message.reply(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({TZ})")
            return

        await message.reply("Не понял время. Например: 6, 18, 6:30, «6 утра» или «6 вечера».")
        return

    # 1) Сначала — «только дата без времени» (словесная и числовая)
    day_only_words = re.search(r"\b(сегодня|завтра|послезавтра)\b(?!.*\bв\s*\d)", text, re.IGNORECASE)
    if day_only_words:
        word = day_only_words.group(1).lower()
        now = datetime.now(tz)
        base = (now.date() if word == "сегодня"
                else (now + timedelta(days=1)).date() if word == "завтра"
                else (now + timedelta(days=2)).date())
        desc = clean_description(re.sub(r"\b(сегодня|завтра|послезавтра)\b", "", text, flags=re.IGNORECASE))
        PENDING[uid] = {"description": desc, "base_date": base, "repeat": "none"}
        await message.reply(f"Ок, {base.strftime('%d.%m')}. Во сколько напомнить? (например: 6, 18, 6:30)")
        return

    day_only_num = parse_numeric_date_only(text)
    if day_only_num:
        _, base, desc = day_only_num
        PENDING[uid] = {"description": desc, "base_date": base, "repeat": "none"}
        await message.reply(f"Ок, {base.strftime('%d.%m')}. Во сколько напомнить? (например: 6, 18, 6:30)")
        return

    # 2) затем «день + время», «только время», «через…», «в это же время»
    for parser in (parse_daytime_phrase, parse_onlytime_phrase):
        pack = parser(text)
        if pack:
            tag = pack[0]
            if tag == "amb":
                _, remainder, variants = pack
                desc = clean_description(remainder or text)
                PENDING[uid] = {"description": desc, "variants": variants, "repeat": "none"}
                await message.reply(f"Уточните, во сколько напомнить «{desc}»?", reply_markup=_variants_keyboard(variants))
                return
            else:
                _, dt, remainder = pack
                desc = clean_description(remainder or text)
                REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": "none"})
                schedule_one(REMINDERS[-1])
                await message.reply(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({TZ})")
                return

    rel = parse_relative_phrase(text)
    if rel:
        dt, remainder = rel
        desc = clean_description(remainder or text)
        REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": "none"})
        schedule_one(REMINDERS[-1])
        await message.reply(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({TZ})")
        return

    same = parse_same_time_phrase(text)
    if same:
        dt, remainder = same
        desc = clean_description(remainder or text)
        REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": "none"})
        schedule_one(REMINDERS[-1])
        await message.reply(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({TZ})")
        return

    # 3) GPT fallback
    await bot.send_chat_action(message.chat.id, ChatAction.TYPING)
    plan = await gpt_parse(text)
    desc = clean_description(plan.get("description") or "Напоминание")
    repeat = (plan.get("repeat") or "none").lower()
    remind_iso = plan.get("remind_time") or plan.get("event_time")
    remind_dt = as_local_iso(remind_iso)

    if plan.get("needs_clarification") or not remind_dt:
        PENDING[uid] = {"description": desc, "repeat": "none"}
        await message.reply(plan.get("clarification_question") or
                            "Уточните дату/время (например, 25.08 14:25).")
        return

    REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": remind_dt,
                      "repeat": "none" if repeat not in ("daily","weekly") else repeat})
    schedule_one(REMINDERS[-1])
    await message.reply(f"Готово. Напомню: «{desc}» в {remind_dt.strftime('%d.%m %H:%M')} ({TZ})")

# ===================== КНОПКИ ВЫБОРА ВРЕМЕНИ =====================
@dp.callback_query(F.data.startswith("time|"))
async def on_time_choice(cb: CallbackQuery):
    uid = cb.from_user.id
    if uid not in PENDING or not PENDING[uid].get("variants"):
        await cb.answer("Нет активного уточнения")
        return
    try:
        iso = cb.data.split("|", 1)[1]
        dt = datetime.fromisoformat(iso)
        dt = tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)
    except Exception as e:
        print("time| parse error:", e)
        await cb.answer("Ошибка выбора времени")
        return

    desc = PENDING[uid].get("description", "Напоминание")
    PENDING.pop(uid, None)

    REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": "none"})
    schedule_one(REMINDERS[-1])

    try:
        await cb.message.edit_text(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({TZ})")
    except Exception:
        await cb.message.answer(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({TZ})")
    await cb.answer("Установлено ✅")

# ===================== ЗАПУСК =====================
async def main():
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        import traceback, time
        print("FATAL:", e)
        traceback.print_exc()
        time.sleep(10)
        raise
