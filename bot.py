import os
import re
import asyncio
from datetime import datetime, timedelta, date
import pytz

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ================== ОКРУЖЕНИЕ ==================
BOT_TOKEN = os.getenv("BOT_TOKEN")
APP_TZ = os.getenv("APP_TZ", "Europe/Moscow")
tz = pytz.timezone(APP_TZ)

# ================== ИНИЦ ==================
bot = Bot(BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=APP_TZ)

# PENDING[user_id] = {
#   "description": str,
#   "repeat": "none",
#   "variants": [datetime],        # если нужно выбрать из двух
#   "base_date": date              # если ждём только время
# }
PENDING: dict[int, dict] = {}
REMINDERS: list[dict] = []

# ================== УТИЛИТЫ ==================
def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s or "", flags=re.UNICODE).strip()

def clean_desc(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"^(напомни(те)?|пожалуйста)\b[\s,:-]*", "", s, flags=re.I)
    s = re.sub(r"^(о|про|насч[её]т)\s+", "", s, flags=re.I)
    return s.strip() or "Напоминание"

async def send_reminder(uid: int, text: str):
    try:
        await bot.send_message(uid, f"🔔 Напоминание: {text}")
    except Exception as e:
        print("send_reminder error:", e)

def plan(reminder: dict):
    scheduler.add_job(send_reminder, "date",
                      run_date=reminder["remind_dt"],
                      args=[reminder["user_id"], reminder["text"]])

def mk_dt(d: date, h: int, m: int) -> datetime:
    return tz.localize(datetime(d.year, d.month, d.day, h % 24, m % 60, 0, 0))

def soonest(dts: list[datetime]) -> list[datetime]:
    return sorted(dts, key=lambda x: x)

# Человекочитаемая подпись кнопки
def human_label(dt: datetime) -> str:
    now = datetime.now(tz)
    if dt.date() == now.date():
        dword = "Сегодня"
    elif dt.date() == (now + timedelta(days=1)).date():
        dword = "Завтра"
    else:
        dword = dt.strftime("%d.%m")

    h = dt.hour
    m = dt.minute
    if 0 <= h <= 4:
        mer = "ночи"
    elif 5 <= h <= 11:
        mer = "утра"
    elif 12 <= h <= 16:
        mer = "дня"
    else:
        mer = "вечера"

    h12 = h % 12
    if h12 == 0:
        h12 = 12
    t = f"{h12}:{m:02d}" if m else f"{h12}"
    return f"{dword} в {t} {mer}"

def kb_variants(dts: list[datetime]) -> InlineKeyboardMarkup:
    rows = []
    for dt in soonest(dts):
        rows.append([InlineKeyboardButton(text=human_label(dt),
                                          callback_data=f"time|{dt.isoformat()}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ================== РУССКИЕ МЕСЯЦЫ ==================
MONTHS = {
    # родительный
    "января":1, "февраля":2, "марта":3, "апреля":4, "мая":5, "июня":6,
    "июля":7, "августа":8, "сентября":9, "октября":10, "ноября":11, "декабря":12,
    # часто пишут в именительном
    "январь":1,"февраль":2,"март":3,"апрель":4,"май":5,"июнь":6,"июль":7,
    "август":8,"сентябрь":9,"октябрь":10,"ноябрь":11,"декабрь":12,
}

def nearest_future_day(day: int, now: datetime) -> date:
    y, m = now.year, now.month
    try:
        cand = date(y, m, day)
        if cand > now.date():
            return cand
    except ValueError:
        pass
    if m == 12:
        y2, m2 = y + 1, 1
    else:
        y2, m2 = y, m + 1
    for dcap in (31, 30, 29, 28):
        try:
            return date(y2, m2, min(day, dcap))
        except ValueError:
            continue
    return date(y2, m2, 28)

# ================== ПАРСЕРЫ ==================
# 1) через полчаса
RX_HALF_HOUR = re.compile(r"\bчерез\s+пол\s*часа\b", re.I)
# 2) через N единиц
RX_REL = [
    (re.compile(r"\bчерез\s+(\d+)\s*(секунд(?:у|ы)?|сек\.?)\b", re.I), "seconds"),
    (re.compile(r"\bчерез\s+(\d+)\s*(минут(?:у|ы)?|мин\.?)\b", re.I), "minutes"),
    (re.compile(r"\bчерез\s+(\d+)\s*(час(?:а|ов)?|ч\.?)\b", re.I), "hours"),
    (re.compile(r"\bчерез\s+(\d+)\s*(дн(?:я|ей)?|день|дн\.?)\b", re.I), "days"),
]
# 2a) ЕДИН. число: «через минуту/секунду/час/день»
RX_REL_SINGULAR = [
    (re.compile(r"\bчерез\s+секунд[ую]\b", re.I), "seconds", 1),
    (re.compile(r"\bчерез\s+минут[ую]\b", re.I), "minutes", 1),
    (re.compile(r"\bчерез\s+час\b", re.I), "hours", 1),
    (re.compile(r"\bчерез\s+день\b", re.I), "days", 1),
]
# 3) в это же время завтра/послезавтра/через N дней
RX_SAME_TIME = re.compile(r"\bв это же время\b", re.I)
RX_TMR = re.compile(r"\bзавтра\b", re.I)
RX_ATMR = re.compile(r"\bпослезавтра\b", re.I)
RX_IN_N_DAYS = re.compile(r"\bчерез\s+(\d+)\s*(дн(?:я|ей)?|день|дн\.?)\b", re.I)
# 4) слова дня + время (сегодня/завтра/послезавтра в 7[:30] (утра/вечера/…))
RX_DAY_WORD_TIME = re.compile(
    r"\b(сегодня|завтра|послезавтра)\b.*?\bв\s*(\d{1,2})(?::(\d{2}))?(?:\s*(утра|дня|вечера|ночи))?\b",
    re.I | re.DOTALL
)
# 5) только время «в 7[:30] [утра/вечера]»
RX_ONLY_TIME = re.compile(r"\bв\s*(\d{1,2})(?::(\d{2}))?\b", re.I)
# 6) «DD.MM[.YYYY]» (+ опц. время)
RX_DOT_DATE = re.compile(
    r"\b(\d{1,2})[.\-/](\d{1,2})(?:[.\-/](\d{2,4}))?"
    r"(?:\s*в\s*(\d{1,2})(?::(\d{2}))?(?:\s*(утра|дня|вечера|ночи))?)?",
    re.I
)
# 7) «DD месяца» (+ опц. время), «24 мая», «24 мая в 7»
RX_MONTH_DATE = re.compile(
    r"\b(\d{1,2})\s+([А-Яа-яёЁ]+)\b"
    r"(?:\s*в\s*(\d{1,2})(?::(\d{2}))?(?:\s*(утра|дня|вечера|ночи))?)?",
    re.I
)
# 8) «N числа» (+ опц. время) — без месяца
RX_DAY_OF_MONTH = re.compile(
    r"\b(\d{1,2})\s*числ[ао]\b"
    r"(?:\s*в\s*(\d{1,2})(?::(\d{2}))?(?:\s*(утра|дня|вечера|ночи))?)?",
    re.I
)

def parse_relative(text: str):
    s = norm(text)
    now = datetime.now(tz).replace(second=0, microsecond=0)

    if RX_HALF_HOUR.search(s):
        dt = now + timedelta(minutes=30)
        s2 = RX_HALF_HOUR.sub("", s).strip(" ,.-")
        return dt, s2

    # единичные формы
    for rx, kind, val in RX_REL_SINGULAR:
        m = rx.search(s)
        if m:
            if kind == "seconds":
                dt = now + timedelta(seconds=val)
            elif kind == "minutes":
                dt = now + timedelta(minutes=val)
            elif kind == "hours":
                dt = now + timedelta(hours=val)
            else:
                dt = now + timedelta(days=val)
            s2 = (s[:m.start()] + s[m.end():]).strip(" ,.-")
            return dt, s2

    # с количеством
    for rx, kind in RX_REL:
        m = rx.search(s)
        if m:
            n = int(m.group(1))
            if kind == "seconds":
                dt = now + timedelta(seconds=n)
            elif kind == "minutes":
                dt = now + timedelta(minutes=n)
            elif kind == "hours":
                dt = now + timedelta(hours=n)
            else:
                dt = now + timedelta(days=n)
            s2 = (s[:m.start()] + s[m.end():]).strip(" ,.-")
            return dt, s2
    return None

def parse_same_time(text: str):
    s = norm(text)
    if not RX_SAME_TIME.search(s):
        return None
    now = datetime.now(tz).replace(second=0, microsecond=0)
    days = None
    if RX_ATMR.search(s): days = 2
    elif RX_TMR.search(s): days = 1
    else:
        m = RX_IN_N_DAYS.search(s)
        if m:
            days = int(m.group(1))
    if days is None:
        return None
    dt = (now + timedelta(days=days)).replace(second=0, microsecond=0)
    s2 = RX_SAME_TIME.sub("", s)
    s2 = RX_TMR.sub("", s2)
    s2 = RX_ATMR.sub("", s2)
    s2 = RX_IN_N_DAYS.sub("", s2)
    return dt, s2.strip(" ,.-")

def apply_meridian(h: int, mer: str | None) -> int:
    if not mer:
        return h
    mer = mer.lower()
    if mer in ("дня","вечера") and h < 12: return h + 12
    if mer == "ночи" and h == 12: return 0
    return h

def parse_dayword_time(text: str):
    s = norm(text)
    m = RX_DAY_WORD_TIME.search(s)
    if not m:
        return None
    word = m.group(1).lower()
    h = int(m.group(2))
    mm = int(m.group(3) or 0)
    mer = (m.group(4) or "").lower()

    now = datetime.now(tz).replace(second=0, microsecond=0)
    base = now.date()
    if word == "завтра":
        base = (now + timedelta(days=1)).date()
    elif word == "послезавтра":
        base = (now + timedelta(days=2)).date()

    if mer in ("утра","дня","вечера","ночи"):
        hh = apply_meridian(h, mer)
        dt = mk_dt(base, hh, mm)
        rest = (s[:m.start()] + s[m.end():]).strip(" ,.-")
        return ("ok", dt, rest)

    # двусмысленно: 7 -> 07:00 или 19:00
    v1 = mk_dt(base, h % 24, mm)
    v2 = mk_dt(base, (h + 12) % 24, mm)
    if base == now.date():
        if v1 <= now: v1 = v1 + timedelta(days=1)
        if v2 <= now: v2 = v2 + timedelta(days=1)
    rest = (s[:m.start()] + s[m.end():]).strip(" ,.-")
    return ("amb", rest, soonest([v1, v2]))

def parse_only_time(text: str):
    s = norm(text)
    m = RX_ONLY_TIME.search(s)
    if not m:
        return None
    now = datetime.now(tz).replace(second=0, microsecond=0)
    h = int(m.group(1))
    mm = int(m.group(2) or 0)

    mer_m = re.search(r"(утра|дня|вечера|ночи)", s, re.I)
    if mer_m:
        hh = apply_meridian(h, mer_m.group(1))
        dt = now.replace(hour=hh % 24, minute=mm)
        if dt <= now: dt += timedelta(days=1)
        rest = (s[:m.start()] + s[m.end():]).strip(" ,.-")
        return ("ok", dt, rest)

    v1 = now.replace(hour=h % 24, minute=mm)
    v2 = now.replace(hour=(h + 12) % 24, minute=mm)
    if v1 <= now: v1 += timedelta(days=1)
    if v2 <= now: v2 += timedelta(days=1)
    rest = (s[:m.start()] + s[m.end():]).strip(" ,.-")
    return ("amb", rest, soonest([v1, v2]))

def parse_dot_date(text: str):
    s = norm(text)
    m = RX_DOT_DATE.search(s)
    if not m:
        return None
    dd = int(m.group(1)); mm = int(m.group(2))
    yy = m.group(3)
    hh = m.group(4); minu = m.group(5); mer = m.group(6)
    now = datetime.now(tz)
    yyyy = now.year if not yy else (int(yy) + 2000 if len(yy) == 2 else int(yy))
    try:
        base = date(yyyy, mm, dd)
    except ValueError:
        return None

    rest = RX_DOT_DATE.sub("", s, count=1).strip(" ,.-")

    if hh:
        h = int(hh)
        minute = int(minu or 0)
        if mer:
            # однозначно
            h = apply_meridian(h, mer)
            dt = mk_dt(base, h, minute)
            return ("ok", dt, rest)
        else:
            # двусмысленно -> две кнопки на ЭТУ дату
            dt1 = mk_dt(base, h % 24, minute)
            dt2 = mk_dt(base, (h + 12) % 24, minute)
            return ("amb", rest, soonest([dt1, dt2]))
    else:
        return ("day", base, rest)

def parse_month_date(text: str):
    s = norm(text)
    m = RX_MONTH_DATE.search(s)
    if not m:
        return None

    dd = int(m.group(1))
    mon = m.group(2).lower()
    if mon not in MONTHS:
        return None
    mm = MONTHS[mon]

    hh = m.group(3)
    minu = m.group(4)
    mer = (m.group(5) or "").lower() if m.group(5) else None

    now = datetime.now(tz)
    yyyy = now.year
    try:
        base = date(yyyy, mm, dd)
    except ValueError:
        return None
    if base < now.date():
        try:
            base = date(yyyy + 1, mm, dd)
        except ValueError:
            return None

    rest = RX_MONTH_DATE.sub("", s, count=1).strip(" ,.-")

    if hh:
        h = int(hh)
        minute = int(minu or 0)
        if mer:
            h = apply_meridian(h, mer)
            dt = mk_dt(base, h, minute)
            return ("ok", dt, rest)
        else:
            dt1 = mk_dt(base, h % 24, minute)
            dt2 = mk_dt(base, (h + 12) % 24, minute)
            return ("amb", rest, soonest([dt1, dt2]))
    else:
        return ("day", base, rest)

def parse_day_of_month(text: str):
    s = norm(text)
    m = RX_DAY_OF_MONTH.search(s)
    if not m:
        return None

    dd = int(m.group(1))
    hh = m.group(2)
    minu = m.group(3)
    mer = (m.group(4) or "").lower() if m.group(4) else None

    now = datetime.now(tz)
    base = nearest_future_day(dd, now)

    rest = RX_DAY_OF_MONTH.sub("", s, count=1).strip(" ,.-")

    if hh:
        h = int(hh)
        minute = int(minu or 0)
        if mer:
            h = apply_meridian(h, mer)
            dt = mk_dt(base, h, minute)
            return ("ok", dt, rest)
        else:
            dt1 = mk_dt(base, h % 24, minute)
            dt2 = mk_dt(base, (h + 12) % 24, minute)
            return ("amb", rest, soonest([dt1, dt2]))
    else:
        return ("day", base, rest)

# ================== КОМАНДЫ ==================
@dp.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(
        "Привет! Я бот-напоминалка.\n"
        "Понимаю: «24 мая в 19», «1 числа в 7», «через 30 минут/час/минуту», "
        "«завтра в 6», «в это же время завтра».\n"
        "/list — список, /ping — проверка."
    )

@dp.message(Command("ping"))
async def cmd_ping(m: Message):
    await m.answer("pong ✅")

@dp.message(Command("list"))
async def cmd_list(m: Message):
    uid = m.from_user.id
    items = [r for r in REMINDERS if r["user_id"] == uid]
    if not items:
        await m.answer("Пока нет напоминаний (в этой сессии).")
        return
    items = sorted(items, key=lambda r: r["remind_dt"])
    lines = [f"• {r['text']} — {r['remind_dt'].strftime('%d.%m %H:%M')} ({APP_TZ})" for r in items]
    await m.answer("\n".join(lines))

# ================== ОСНОВНАЯ ЛОГИКА ==================
@dp.message(F.text)
async def on_text(m: Message):
    uid = m.from_user.id
    text = norm(m.text)

    # если ждём уточнение
    if uid in PENDING:
        st = PENDING[uid]
        if st.get("variants"):
            await m.reply("Нажмите кнопку ниже ⬇️")
            return
        if st.get("base_date"):
            mt = re.search(r"(?:^|\bв\s*)(\d{1,2})(?::(\d{2}))?\s*(утра|дня|вечера|ночи)?\b", text, re.I)
            if not mt:
                await m.reply("Во сколько?")
                return
            h = int(mt.group(1)); minute = int(mt.group(2) or 0); mer = mt.group(3)
            hh = apply_meridian(h, mer)
            dt = mk_dt(st["base_date"], hh, minute)
            desc = st.get("description", "Напоминание")
            PENDING.pop(uid, None)
            REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": "none"})
            plan(REMINDERS[-1])
            await m.reply(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({APP_TZ})")
            return
        PENDING.pop(uid, None)

    # ВАЖНО: порядок! сначала относительное/конкретные даты, потом «просто время»
    # «через …»
    r = parse_relative(text)
    if r:
        dt, rest = r
        desc = clean_desc(rest or text)
        REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": "none"})
        plan(REMINDERS[-1])
        await m.reply(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({APP_TZ})")
        return

    # «в это же время …»
    r = parse_same_time(text)
    if r:
        dt, rest = r
        desc = clean_desc(rest or text)
        REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": "none"})
        plan(REMINDERS[-1])
        await m.reply(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({APP_TZ})")
        return

    # «сегодня/завтра/послезавтра в …»
    r = parse_dayword_time(text)
    if r:
        tag = r[0]
        if tag == "ok":
            _, dt, rest = r
            desc = clean_desc(rest or text)
            REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": "none"})
            plan(REMINDERS[-1])
            await m.reply(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({APP_TZ})")
            return
        else:
            _, rest, variants = r
            desc = clean_desc(rest or text)
            PENDING[uid] = {"description": desc, "variants": variants, "repeat": "none"}
            await m.reply(f"Уточните, во сколько напомнить «{desc}»?",
                          reply_markup=kb_variants(variants))
            return

    # КОНКРЕТНЫЕ ДАТЫ (раньше, чем «просто время»!)
    # дата через точки «DD.MM[.YYYY] [в HH[:MM] …]»
    r = parse_dot_date(text)
    if r:
        tag = r[0]
        if tag == "ok":
            _, dt, rest = r
            desc = clean_desc(rest or text)
            REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": "none"})
            plan(REMINDERS[-1])
            await m.reply(f"Готово. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({APP_TZ})")
            return
        elif tag == "amb":
            _, rest, variants = r
            desc = clean_desc(rest or text)
            PENDING[uid] = {"description": desc, "variants": variants, "repeat":"none"}
            await m.reply(f"Уточните, во сколько напомнить «{desc}»?",
                          reply_markup=kb_variants(variants))
            return
        else:
            _, base, rest = r
            desc = clean_desc(rest or text)
            PENDING[uid] = {"description": desc, "base_date": base, "repeat":"none"}
            await m.reply(f"Окей, {base.strftime('%d.%m')}. В какое время?")
            return

    # «DD месяца [в HH[:MM] …]»
    r = parse_month_date(text)
    if r:
        tag = r[0]
        if tag == "ok":
            _, dt, rest = r
            desc = clean_desc(rest or text)
            REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat":"none"})
            plan(REMINDERS[-1])
            await m.reply(f"Готово. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({APP_TZ})")
            return
        elif tag == "amb":
            _, rest, variants = r
            desc = clean_desc(rest or text)
            PENDING[uid] = {"description": desc, "variants": variants, "repeat":"none"}
            await m.reply(f"Уточните, во сколько напомнить «{desc}»?",
                          reply_markup=kb_variants(variants))
            return
        else:
            _, base, rest = r
            desc = clean_desc(rest or text)
            PENDING[uid] = {"description": desc, "base_date": base, "repeat":"none"}
            await m.reply(f"Окей, {base.strftime('%d.%m')}. В какое время?")
            return

    # «N числа [в HH[:MM] …]»
    r = parse_day_of_month(text)
    if r:
        tag = r[0]
        if tag == "ok":
            _, dt, rest = r
            desc = clean_desc(rest or text)
            REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat":"none"})
            plan(REMINDERS[-1])
            await m.reply(f"Готово. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({APP_TZ})")
            return
        elif tag == "amb":
            _, rest, variants = r
            desc = clean_desc(rest or text)
            PENDING[uid] = {"description": desc, "variants": variants, "repeat":"none"}
            await m.reply(f"Уточните, во сколько напомнить «{desc}»?",
                          reply_markup=kb_variants(variants))
            return
        else:
            _, base, rest = r
            desc = clean_desc(rest or text)
            PENDING[uid] = {"description": desc, "base_date": base, "repeat":"none"}
            await m.reply(f"Окей, {base.strftime('%d.%m')}. В какое время?")
            return

    # И ТОЛЬКО ТЕПЕРЬ — «просто время»
    r = parse_only_time(text)
    if r:
        tag = r[0]
        if tag == "ok":
            _, dt, rest = r
            desc = clean_desc(rest or text)
            REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat": "none"})
            plan(REMINDERS[-1])
            await m.reply(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({APP_TZ})")
            return
        else:
            _, rest, variants = r
            desc = clean_desc(rest or text)
            PENDING[uid] = {"description": desc, "variants": variants, "repeat": "none"}
            await m.reply(f"Уточните, во сколько напомнить «{desc}»?",
                          reply_markup=kb_variants(variants))
            return

    await m.reply("Не понял дату/время. Примеры: «24.05 19:00», «24 мая в 19», «1 числа в 7», «через 30 минут», «завтра в 6».")
# ================== КНОПКИ ==================
@dp.callback_query(F.data.startswith("time|"))
async def choose_time(cb: CallbackQuery):
    uid = cb.from_user.id
    if uid not in PENDING or not PENDING[uid].get("variants"):
        await cb.answer("Нет активного уточнения")
        return
    try:
        iso = cb.data.split("|", 1)[1]
        dt = datetime.fromisoformat(iso)
        dt = tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)
    except Exception as e:
        print("parse cb time error:", e)
        await cb.answer("Ошибка выбора времени")
        return

    desc = PENDING[uid].get("description", "Напоминание")
    PENDING.pop(uid, None)
    REMINDERS.append({"user_id": uid, "text": desc, "remind_dt": dt, "repeat":"none"})
    plan(REMINDERS[-1])

    try:
        await cb.message.edit_text(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({APP_TZ})")
    except Exception:
        await cb.message.answer(f"Принял. Напомню: «{desc}» в {dt.strftime('%d.%m %H:%M')} ({APP_TZ})")
    await cb.answer("Установлено ✅")

# ================== ЗАПУСК ==================
async def main():
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
