"""
Скенер за интрадей сигнали - самостоятелна версия за GitHub Actions.

За разлика от Colab версията, тук няма безкраен while цикъл с time.sleep() -
GitHub Actions сам "буди" този скрипт на всеки 5 минути (виж
.github/workflows/scanner.yml) и всяко пускане прави ЕДНА проверка, после
приключва. Състоянието между пусканията (предпазарен watchlist, CSV лог на
сигналите) се пази в обикновени файлове (state.json, signals_log.csv), които
workflow-ът автоматично commit-ва обратно в repo-то след всяко пускане.

Логиката на самия сигнал (EMA9/EMA20 + обем + VWAP) е same-same като
проверената Colab версия - нищо не е променяно тук.
"""

import csv
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import yfinance as yf

# yfinance пише свои ERROR/CRITICAL съобщения директно в конзолата (напр. при
# моментен глитч на Yahoo за даден тикър - "possibly delisted"). Заглушаваме
# ги - грешките пак се хващат от нашите try/except блокове по-долу.
logging.getLogger("yfinance").setLevel(logging.CRITICAL)


# ========================= НАСТРОЙКИ =========================

SCREENER_CONFIG = {
    "most_actives": 15,
    "day_gainers": 25,   # "топ 25 най-качили-се за деня", като в Revolut
    # "day_losers" премахнат умишлено - стратегията е BUY-only (bullish
    # пресичане), няма смисъл да теглим падащи акции в пула за анализ.
}

FALLBACK_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META",
    "NFLX", "AMD", "JPM", "V", "DIS", "BA", "KO", "PFE",
]

# Тикъри, изключени напълно от скенера - ръчно попълвай тук при нужда
# (напр. проблем с изпълнение в Revolut, или друга специфична причина).
EXCLUDED_TICKERS = [
    # "ПРИМЕР", "ДРУГ_ТИКЪР",
]

# Гоним само акции, които В МОМЕНТА растат за деня - няма смисъл да следим
# нещо, което вече пада. 0.0 = само положителна дневна промяна.
MIN_DAILY_CHANGE_PCT = 0.0

# Таван на общия пул тикъри, които скенерът следи едновременно между
# отделните 5-минутни пускания (пази от неограничено растене през деня).
MAX_TRACKED_TICKERS = 30

EMA_FAST = 9
EMA_SLOW = 20
VOLUME_MULTIPLIER = 1.5
TAKE_PROFIT_PCT = 0.05
STOP_LOSS_PCT = 0.02
REQUEST_DELAY_SEC = 1
STALE_DATA_WARNING_MIN = 10

ENABLE_PREMARKET_PREP = True
PREMARKET_START_HOUR = 4
PREMARKET_TOP_N = 10
PREMARKET_MIN_PCT_MOVE = 2.0

# Файлове, пазени в самия repo (committed от workflow-а след всяко пускане)
STATE_PATH = Path("state.json")
CSV_PATH = Path("signals_log.csv")
CSV_COLUMNS = ["id", "ticker", "signal_time", "entry_price", "take_profit", "stop_loss",
               "status", "close_time", "close_price", "pnl_pct"]

# Telegram - идват от GitHub Secrets (виж workflow YAML)
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")


# ========================= TELEGRAM =========================

def send_telegram(text: str):
    """Праща съобщение в Telegram. Ако токенът/chat_id липсват, само printва
    в конзолата (полезно при локално тестване без Telegram)."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[Telegram изключен - липсват TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID]")
        print(text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=10,
        )
        if resp.status_code != 200:
            print(f"Telegram грешка ({resp.status_code}): {resp.text}")
    except Exception as e:
        print(f"Telegram изпращането се провали: {e}")


# ========================= TELEGRAM СЪОБЩЕНИЯ - ФОРМАТИРАНЕ =========================
#
# Общ визуален стил на всички съобщения:
#   - тънка разделителна линия ("┈┈┈") под заглавието
#   - цените подравнени в <code> (моноширинен шрифт, изглежда като таблица)
#   - един ред "мета" информация (час/риск) в дъно, по-дребен смислово акцент
# Telegram HTML поддържа само: b, i, u, s, code, pre, a - никакви таблици или
# markdown ```, затова подравняването е чрез фиксирана ширина в <code>.

DIVIDER = "┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈"


def fmt_price(value):
    return f"${value:,.2f}"


def build_buy_signal_message(signal, stale, duplicate):
    risk_reward = TAKE_PROFIT_PCT / STOP_LOSS_PCT
    lines = [
        f"🟢 <b>BUY СИГНАЛ · {signal['ticker']}</b>",
        DIVIDER,
        f"Вход:  <code>{fmt_price(signal['price']):<10}</code>",
        f"TP:    <code>{fmt_price(signal['take_profit']):<10}</code>  <i>(+{TAKE_PROFIT_PCT*100:.0f}%)</i>",
        f"SL:    <code>{fmt_price(signal['stop_loss']):<10}</code>  <i>(-{STOP_LOSS_PCT*100:.0f}%)</i>",
        f"R:R:   <code>1 : {risk_reward:.1f}</code>",
        DIVIDER,
        f"🕐 {signal['time']} NY  ·  бар отпреди {signal['data_age_min']} мин",
    ]
    if stale:
        lines.append(f"⚠️ <i>Данните са над {STALE_DATA_WARNING_MIN} мин стари - провери реалната цена преди да действаш.</i>")
    if duplicate:
        lines.append("ℹ️ <i>Вече има отворена позиция за този тикър - не се дублира в лога.</i>")
    return "\n".join(lines)


def build_position_closed_message(row):
    is_win = row["status"] == "tp_hit"
    emoji = "✅" if is_win else "🛑"
    label = "TAKE PROFIT" if is_win else "STOP LOSS"
    pnl = float(row["pnl_pct"])
    lines = [
        f"{emoji} <b>{label} · {row['ticker']}</b>",
        DIVIDER,
        f"Вход:   <code>{fmt_price(float(row['entry_price'])):<10}</code>",
        f"Изход:  <code>{fmt_price(float(row['close_price'])):<10}</code>",
        f"P/L:    <b>{pnl:+.2f}%</b>",
        DIVIDER,
        f"🕐 затворена {row['close_time']} NY",
    ]
    return "\n".join(lines)


def build_premarket_digest_message(movers, ny_now):
    lines = [f"🌅 <b>ПРЕДПАЗАРНИ ЛИДЕРИ</b>  ·  {ny_now.strftime('%H:%M')} NY", DIVIDER]
    for m in movers:
        arrow = "🔺" if m["pct_move"] > 0 else "🔻"
        lines.append(f"{arrow} <b>{m['ticker']}</b>   <code>{m['pct_move']:+.2f}%</code>   @ {fmt_price(m['last_price'])}")
    return "\n".join(lines)


def build_premarket_change_message(movers, added, removed, ny_now):
    """Показва само какво се е ПРОМЕНИЛО спрямо предишната проверка - нови
    имена, влезли в топ движещите се, и такива, които вече не отговарят на
    критериите (паднали под прага или изместени от по-силни движения)."""
    lines = [f"🌅 <b>ПРОМЯНА В ПРЕДПАЗАРНИТЕ ЛИДЕРИ</b>  ·  {ny_now.strftime('%H:%M')} NY", DIVIDER]

    if added:
        lines.append("🆕 <b>Нови:</b>")
        movers_by_ticker = {m["ticker"]: m for m in movers}
        for t in added:
            m = movers_by_ticker.get(t)
            if m:
                arrow = "🔺" if m["pct_move"] > 0 else "🔻"
                lines.append(f"  {arrow} <b>{t}</b>   <code>{m['pct_move']:+.2f}%</code>   @ {fmt_price(m['last_price'])}")

    if removed:
        if added:
            lines.append("")
        lines.append("❌ <b>Отпаднали:</b>")
        for t in removed:
            lines.append(f"  • {t}")

    lines.append(DIVIDER)
    lines.append("📋 <b>Пълен списък сега:</b>")
    for m in sorted(movers, key=lambda x: abs(x["pct_move"]), reverse=True):
        arrow = "🔺" if m["pct_move"] > 0 else "🔻"
        lines.append(f"  {arrow} {m['ticker']}   <code>{m['pct_move']:+.2f}%</code>   @ {fmt_price(m['last_price'])}")
    return "\n".join(lines)


def build_session_summary_message():
    rows = read_signals_log()
    closed = [r for r in rows if r["status"] in ("tp_hit", "sl_hit")]
    if not closed:
        return None
    wins = sum(1 for r in closed if r["status"] == "tp_hit")
    total = len(closed)
    win_rate = wins / total * 100
    avg_pnl = sum(float(r["pnl_pct"]) for r in closed) / total
    open_count = sum(1 for r in rows if r["status"] == "open")
    lines = [
        "📊 <b>ОБОБЩЕНИЕ ЗА СЕСИЯТА</b>",
        DIVIDER,
        f"Затворени:  <code>{total}</code>",
        f"Печеливши:  <code>{wins}</code>  <i>({win_rate:.1f}%)</i>",
        f"Среден P/L: <b>{avg_pnl:+.2f}%</b>",
        f"Отворени:   <code>{open_count}</code>",
    ]
    return "\n".join(lines)


# ========================= СЪСТОЯНИЕ (state.json) =========================

def load_state():
    if STATE_PATH.exists():
        try:
            state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            state.setdefault("tracked_tickers", [])
            state.setdefault("tracked_day", None)
            return state
        except Exception:
            pass
    return {"premarket_watchlist": [], "last_session": None, "tracked_tickers": [], "tracked_day": None}


def save_state(state):
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


# ========================= ПАЗАРНА СЕСИЯ =========================

def get_market_session():
    """Връща ("closed" | "premarket" | "open", ny_now)."""
    ny_now = datetime.now(ZoneInfo("America/New_York"))
    if ny_now.weekday() >= 5:
        return "closed", ny_now

    premarket_start = ny_now.replace(hour=PREMARKET_START_HOUR, minute=0, second=0, microsecond=0)
    market_open = ny_now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = ny_now.replace(hour=16, minute=0, second=0, microsecond=0)

    if market_open <= ny_now <= market_close:
        return "open", ny_now
    if premarket_start <= ny_now < market_open:
        return "premarket", ny_now
    return "closed", ny_now


# ========================= ОТКРИВАНЕ НА ТИКЪРИ =========================

def get_trending_tickers(screener_config=None):
    """Тегли комбиниран списък от screener-ите, маха изключените тикъри и
    тези, които в момента ПАДАТ за деня (под MIN_DAILY_CHANGE_PCT)."""
    screener_config = screener_config or SCREENER_CONFIG
    combined, seen, any_success = [], set(), False
    for name, count in screener_config.items():
        try:
            results = yf.screen(name, count=count)
            quotes = results.get("quotes", [])
            if quotes:
                any_success = True
            for q in quotes:
                symbol = q.get("symbol")
                change_pct = q.get("regularMarketChangePercent")
                if not symbol or symbol in seen or symbol in EXCLUDED_TICKERS:
                    continue
                if change_pct is None or change_pct < MIN_DAILY_CHANGE_PCT:
                    continue  # пада или няма данни за промяната - прескачаме
                seen.add(symbol)
                combined.append(symbol)
        except Exception as e:
            print(f"Не успях да изтегля screener '{name}' ({e}) - пропускам го.")

    if not any_success:
        print("Всички screener-и се провалиха - ползвам резервния списък.")
        return [t for t in FALLBACK_TICKERS if t not in EXCLUDED_TICKERS]
    return combined


def update_tracked_tickers(tracked, new_trending, max_size=MAX_TRACKED_TICKERS):
    """Добавя нови тикъри към вече следения пул, без да маха стари, освен
    ако не се наложи заради таван на размера - тогава маха първо тези,
    които вече не са в текущия топ списък. Тикъри от EXCLUDED_TICKERS
    никога не влизат и се махат, ако вече присъстват."""
    tracked = [t for t in tracked if t not in EXCLUDED_TICKERS]

    for t in new_trending:
        if t not in tracked and t not in EXCLUDED_TICKERS:
            tracked.append(t)

    if len(tracked) > max_size:
        excess = len(tracked) - max_size
        removable = [t for t in tracked if t not in new_trending]
        to_remove = removable[:excess]
        for t in to_remove:
            tracked.remove(t)
        if len(tracked) > max_size:
            tracked = tracked[-max_size:]

    return tracked


# ========================= ПРЕДПАЗАРНО ОПИПВАНЕ =========================

def scan_premarket_movers(tickers, top_n=None, min_pct_move=None):
    top_n = top_n or PREMARKET_TOP_N
    min_pct_move = min_pct_move if min_pct_move is not None else PREMARKET_MIN_PCT_MOVE

    candidates = []
    for ticker in tickers:
        try:
            data = yf.download(ticker, period="2d", interval="5m", prepost=True, progress=False)
            if data.empty:
                continue
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            today = data.index[-1].date()
            prev_days = sorted(set(data.index.date) - {today})
            if not prev_days:
                continue
            prev_close = data[data.index.date == prev_days[-1]]["Close"].iloc[-1]

            today_data = data[data.index.date == today]
            if today_data.empty:
                continue
            last_price = today_data["Close"].iloc[-1]
            pct_move = (last_price - prev_close) / prev_close * 100

            if abs(pct_move) >= min_pct_move:
                candidates.append({
                    "ticker": ticker,
                    "pct_move": round(float(pct_move), 2),
                    "last_price": round(float(last_price), 2),
                })
        except Exception as e:
            print(f"Грешка при премаркет проверка на {ticker}: {e}")
        time.sleep(REQUEST_DELAY_SEC)

    candidates.sort(key=lambda m: abs(m["pct_move"]), reverse=True)
    top_candidates = candidates[:top_n]

    for m in top_candidates:
        m["premarket_volume"] = None
        try:
            info = yf.Ticker(m["ticker"]).info
            vol = info.get("preMarketVolume") or info.get("regularMarketVolume")
            if vol:
                m["premarket_volume"] = int(vol)
        except Exception:
            pass

    return top_candidates


# ========================= АНАЛИЗ НА ТИКЪР =========================

def analyze_ticker(ticker):
    try:
        data = yf.download(ticker, period="2d", interval="5m", progress=False)
        if data.empty or len(data) < EMA_SLOW + 1:
            return None
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        data["EMA_fast"] = data["Close"].ewm(span=EMA_FAST).mean()
        data["EMA_slow"] = data["Close"].ewm(span=EMA_SLOW).mean()
        data["VolAvg"] = data["Volume"].rolling(20).mean().shift(1)

        trading_day = data.index.date
        pv = data["Close"] * data["Volume"]
        data["VWAP"] = pv.groupby(trading_day).cumsum() / data["Volume"].groupby(trading_day).cumsum()

        last = data.iloc[-1]
        prev = data.iloc[-2]

        crossed_up = prev["EMA_fast"] <= prev["EMA_slow"] and last["EMA_fast"] > last["EMA_slow"]
        volume_ok = pd.notna(last["VolAvg"]) and last["Volume"] > VOLUME_MULTIPLIER * last["VolAvg"]
        above_vwap = last["Close"] > last["VWAP"]

        if crossed_up and volume_ok and above_vwap:
            entry = float(last["Close"])
            take_profit = entry * (1 + TAKE_PROFIT_PCT)
            stop_loss = entry * (1 - STOP_LOSS_PCT)

            bar_time = data.index[-1]
            now = pd.Timestamp.now(tz=bar_time.tzinfo) if bar_time.tzinfo is not None else pd.Timestamp.now()
            data_age_min = round((now - bar_time).total_seconds() / 60, 1)

            return {
                "ticker": ticker,
                "price": round(entry, 2),
                "take_profit": round(take_profit, 2),
                "stop_loss": round(stop_loss, 2),
                # Ню Йорк час, за консистентност с close_time в update_open_positions()
                # (преди беше наивен сървърен datetime.now(), обикновено UTC на GitHub Actions).
                "time": datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S"),
                "bar_time": bar_time.strftime("%H:%M:%S"),
                "data_age_min": data_age_min,
            }
    except Exception as e:
        print(f"Грешка при обработка на {ticker}: {e}")
    return None


# ========================= CSV ЛОГ =========================

def init_signals_log():
    if not CSV_PATH.exists():
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()


def read_signals_log():
    if not CSV_PATH.exists():
        return []
    with open(CSV_PATH, "r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_signals_log(rows):
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def log_new_signal(signal):
    rows = read_signals_log()
    if any(r["ticker"] == signal["ticker"] and r["status"] == "open" for r in rows):
        return None
    new_id = max([int(r["id"]) for r in rows], default=0) + 1
    rows.append({
        "id": new_id,
        "ticker": signal["ticker"],
        "signal_time": signal["time"],
        "entry_price": signal["price"],
        "take_profit": signal["take_profit"],
        "stop_loss": signal["stop_loss"],
        "status": "open",
        "close_time": "",
        "close_price": "",
        "pnl_pct": "",
    })
    write_signals_log(rows)
    return new_id


def update_open_positions():
    """Проверява отворените позиции и връща списък с ТОЗИ РЪН новозатворените
    (за да им пратим Telegram известие), плюс обновява CSV-то."""
    rows = read_signals_log()
    open_rows = [r for r in rows if r["status"] == "open"]
    newly_closed = []
    if not open_rows:
        return newly_closed

    for r in open_rows:
        try:
            data = yf.download(r["ticker"], period="1d", interval="5m", progress=False)
            if data.empty:
                continue
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            current_price = float(data["Close"].iloc[-1])
        except Exception:
            continue

        entry = float(r["entry_price"])
        tp = float(r["take_profit"])
        sl = float(r["stop_loss"])
        now_str = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")

        if current_price >= tp:
            r.update(status="tp_hit", close_price=round(current_price, 2), close_time=now_str,
                      pnl_pct=round((current_price - entry) / entry * 100, 2))
            newly_closed.append(r)
        elif current_price <= sl:
            r.update(status="sl_hit", close_price=round(current_price, 2), close_time=now_str,
                      pnl_pct=round((current_price - entry) / entry * 100, 2))
            newly_closed.append(r)
        time.sleep(REQUEST_DELAY_SEC)

    write_signals_log(rows)
    return newly_closed


def win_rate_summary_text():
    rows = read_signals_log()
    closed = [r for r in rows if r["status"] in ("tp_hit", "sl_hit")]
    if not closed:
        return ""
    wins = sum(1 for r in closed if r["status"] == "tp_hit")
    total = len(closed)
    win_rate = wins / total * 100
    avg_pnl = sum(float(r["pnl_pct"]) for r in closed) / total
    open_count = sum(1 for r in rows if r["status"] == "open")
    return (f"📊 {wins}/{total} печеливши ({win_rate:.1f}%), "
            f"среден P/L: {avg_pnl:+.2f}%, отворени: {open_count}")


# ========================= ГЛАВНА ЛОГИКА (един run) =========================

def main():
    init_signals_log()
    state = load_state()
    session, ny_now = get_market_session()

    print(f"=== Скенер - {ny_now.strftime('%Y-%m-%d %H:%M:%S %Z')} (сесия: {session}) ===")

    if session == "closed":
        print("🔒 Пазарът е затворен - нищо за правене.")
        state["premarket_watchlist"] = []
        state["tracked_tickers"] = []
        state["tracked_day"] = None
        state["last_session"] = "closed"
        save_state(state)
        return

    if session == "premarket":
        if not ENABLE_PREMARKET_PREP:
            state["last_session"] = "premarket"
            save_state(state)
            return

        previous_watchlist = set(state.get("premarket_watchlist", []))
        candidates = get_trending_tickers()
        movers = scan_premarket_movers(candidates)
        current_watchlist = set(m["ticker"] for m in movers)

        added = [t for t in (m["ticker"] for m in movers) if t not in previous_watchlist]
        removed = [t for t in previous_watchlist if t not in current_watchlist]

        state["premarket_watchlist"] = [m["ticker"] for m in movers]
        state["last_session"] = "premarket"
        save_state(state)

        # Пращаме Telegram само когато списъкът РЕАЛНО се е променил (нов
        # лидер или отпаднал) - не по часовник. Първото пускане за деня
        # (previous_watchlist е празен) винаги праща пълния списък.
        if movers and (added or removed):
            if not previous_watchlist:
                send_telegram(build_premarket_digest_message(movers, ny_now))
            else:
                send_telegram(build_premarket_change_message(movers, added, removed, ny_now))
        print(f"Предпазарни лидери: {state['premarket_watchlist']}")
        if added or removed:
            print(f"  Промяна: +{added} -{removed}")
        return

    # --- session == "open" ---
    newly_closed = update_open_positions()
    for r in newly_closed:
        send_telegram(build_position_closed_message(r))

    # Растящ пул от тикъри, пазен в state.json между отделните пускания -
    # не сменяме списъка всеки run, а добавяме нови трендящи към вече
    # следените (до MAX_TRACKED_TICKERS), за да не губим тикър, който е
    # изпаднал от топ списъка точно преди да довърши пресичането си.
    today_str = ny_now.strftime("%Y-%m-%d")
    tracked_tickers = state.get("tracked_tickers", [])
    if state.get("tracked_day") != today_str:
        tracked_tickers = []  # нов ден - чист старт на пула

    new_trending = get_trending_tickers()
    tracked_tickers = update_tracked_tickers(tracked_tickers, new_trending)

    premarket_watchlist = state.get("premarket_watchlist", [])
    if state.get("last_session") in ("premarket", None) and premarket_watchlist:
        tracked_tickers = update_tracked_tickers(tracked_tickers, premarket_watchlist)
        print(f"🚀 Пазарът отвори - приоритет на предпазарните лидери: {premarket_watchlist}")

    state["tracked_tickers"] = tracked_tickers
    state["tracked_day"] = today_str
    current_tickers = tracked_tickers

    print(f"Проверявам {len(current_tickers)} тикъра (следени общо, таван {MAX_TRACKED_TICKERS}): {current_tickers}")

    any_signal = False
    for ticker in current_tickers:
        signal = analyze_ticker(ticker)
        if signal:
            any_signal = True
            stale = signal["data_age_min"] > STALE_DATA_WARNING_MIN
            logged_id = log_new_signal(signal)
            duplicate = logged_id is None

            msg = build_buy_signal_message(signal, stale, duplicate)
            print(msg.replace("<b>", "").replace("</b>", "").replace("<i>", "").replace("</i>", "").replace("<code>", "").replace("</code>", ""))
            if not duplicate:
                # Пращаме Telegram само за НОВИ сигнали, не за дубликати.
                send_telegram(msg)
        time.sleep(REQUEST_DELAY_SEC)

    if not any_signal:
        print("Няма сигнали в момента.")

    summary = win_rate_summary_text()
    if summary:
        print(summary)

    # Обобщение за деня, изпратено веднъж - на последното пускане преди
    # затваряне на пазара (16:00 NY), не на всеки 5 мин.
    if ny_now.hour == 15 and ny_now.minute >= 55:
        summary_msg = build_session_summary_message()
        if summary_msg:
            send_telegram(summary_msg)

    state["last_session"] = "open"
    save_state(state)


if __name__ == "__main__":
    main()
