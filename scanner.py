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

ОПТИМИЗАЦИЯ (за пестене на GitHub Actions минути): данните за много тикъри
се теглят с ЕДНА групова заявка (fetch_batch), вместо тикър по тикър с пауза
между всеки.

НОВО: филтър за общ пазарен тренд - преди да прати нови BUY сигнали, скриптът
поглежда дали SPY (S&P 500 ETF) е над своя VWAP за деня. Ако не е (пазарът
като цяло пада), пропуска изпращането на нови сигнали този цикъл - пробиви на
отделни акции статистически "успяват" по-рядко, когато целият пазар е слаб.
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
    "most_actives": 30,
    "day_gainers": 40,   # "топ 40 най-качили-се за деня", като в Revolut
    # По-агресивна настройка (repo вече е публично -> Actions минутите не са
    # проблем): добавени още 2 категории, за да не пропускаме реални движения
    # извън топ 30/40-те по обем/ръст (тези често са доминирани от megacap
    # имена). И двете все пак минават през MIN_DAILY_CHANGE_PCT филтъра по-
    # долу, така че тикъри, които не растат днес, пак отпадат автоматично.
    "small_cap_gainers": 40,     # по-малки компании (пазарна кап. <2 млрд), не само megacap
    "aggressive_small_caps": 30,  # допълнителен пул от по-малки/по-волатилни имена
    "most_shorted_stocks": 30,   # висок % "къси" продажби -- склонни към резки short-squeeze пробиви нагоре, точно каквото гони тази стратегия
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
# Вдигнат допълнително до 250 (от първоначалните 60) - repo вече е публично,
# GitHub Actions минутите не са ограничение. ВНИМАНИЕ: това вече е близо до
# практически таван -- Yahoo е НЕофициален endpoint, не мисли за огромни
# групови заявки. Над ~250-300 тикъра расте рискът от throttling/грешки
# (429, прекъснати заявки), не просто по-бавен run. Ако видиш зачестили
# "Групово теглене се провали" съобщения в лога, свали обратно това число.
MAX_TRACKED_TICKERS = 250
EMA_FAST = 9
EMA_SLOW = 20
VOLUME_MULTIPLIER = 1.5
TAKE_PROFIT_PCT = 0.05
STOP_LOSS_PCT = 0.02
# Обемно "изчакване" след пресичане (както в crypto бота) -- по-старата логика
# изискваше EMA пресичане И обем И VWAP на ЕДНА и съща свещ, което пропускаше
# реални пробиви, когато обемът потвърждава 1-2 свещи по-късно. Сега: свещ 1
# пресича -> "pending"; следващите свещи (до този таван) продължават да се
# проверяват за обем; ако трендът се обърне обратно бичи->мечи преди да
# потвърди, чакането отпада и трябва ново пресичане.
BULL_PENDING_MAX_CANDLES = 3
# След затворен сигнал (tp_hit/sl_hit) за даден тикър, изчакваме толкова
# минути, преди да позволим НОВ сигнал за същия тикър -- пази от "разклащане"
# (бърза поредица сигнали на един и същ тикър около една и съща цена).
SIGNAL_COOLDOWN_MINUTES = 30
# Използва се само за малкото индивидуални заявки, останали (yf.Ticker().info
# за предпазарния обем на топ движещите се - максимум PREMARKET_TOP_N на ден).
REQUEST_DELAY_SEC = 1
STALE_DATA_WARNING_MIN = 10
ENABLE_PREMARKET_PREP = True
# Прозорецът за предпазарна подготовка тръгва от този час (NY време) до
# отварянето на пазара (9:30). 9 => само последните 30 минути преди отваряне.
PREMARKET_START_HOUR = 9
PREMARKET_TOP_N = 15  # вдигнато от 10, в синхрон с по-широкия следобеден пул
PREMARKET_MIN_PCT_MOVE = 2.0
# ---- Филтър за общ пазарен тренд ----
ENABLE_MARKET_TREND_FILTER = True
MARKET_TREND_TICKER = "SPY"  # S&P 500 ETF - използва се като "барометър" на целия пазар
# Файлове, пазени в самия repo (committed от workflow-а след всяко пускане)
STATE_PATH = Path("state.json")
CSV_PATH = Path("signals_log.csv")
CSV_COLUMNS = ["id", "ticker", "source", "signal_time", "entry_price", "take_profit", "stop_loss",
               "status", "close_time", "close_price", "pnl_pct"]
# Telegram - идват от GitHub Secrets (виж workflow YAML)
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
# ========================= ГРУПОВО ТЕГЛЕНЕ НА ДАННИ =========================
def fetch_batch(tickers, **kwargs):
    """Тегли данни за МНОГО тикъри с ЕДНА заявка към Yahoo, вместо тикър по
    тикър с пауза между всеки - много по-бързо и много по-малко отделни
    заявки (по-малко минути на GitHub Actions, по-малък риск от лимитиране).
    Връща dict {тикър: DataFrame}; тикъри без данни просто липсват в резултата."""
    if not tickers:
        return {}
    try:
        data = yf.download(tickers, group_by="ticker", threads=True, progress=False, **kwargs)
    except Exception as e:
        print(f"Групово теглене се провали ({e}) - пропускам тази партида тикъри.")
        return {}
    if data is None or data.empty:
        return {}
    result = {}
    if isinstance(data.columns, pd.MultiIndex):
        top_level = set(data.columns.get_level_values(0))
        for t in tickers:
            if t in top_level:
                sub = data[t].dropna(how="all")
                if not sub.empty:
                    result[t] = sub
    elif len(tickers) == 1:
        # Само 1 тикър в заявката - yfinance връща плосък DataFrame директно.
        result[tickers[0]] = data
    return result
# ========================= ОБЩ ПАЗАРЕН ТРЕНД (SPY) =========================
def compute_market_trend(data):
    """Приема вече изтеглени данни (за MARKET_TREND_TICKER) и връща True
    (бичи пазар - позволяваме нови BUY сигнали) или False (мечи - пропускаме
    ги този цикъл). При липса на данни/грешка връща True (fail-open - не
    искаме временен проблем с извличането да блокира всички сигнали)."""
    if not ENABLE_MARKET_TREND_FILTER:
        return True
    if data is None or data.empty:
        print(f"Не успях да проверя пазарния тренд ({MARKET_TREND_TICKER}) - пропускам филтъра този път.")
        return True
    try:
        trading_day = data.index.date
        pv = data["Close"] * data["Volume"]
        vwap = pv.groupby(trading_day).cumsum() / data["Volume"].groupby(trading_day).cumsum()
        last_close = float(data["Close"].iloc[-1])
        last_vwap = float(vwap.iloc[-1])
        return last_close >= last_vwap
    except Exception as e:
        print(f"Грешка при проверка на пазарния тренд ({MARKET_TREND_TICKER}): {e}")
        return True
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
            state.setdefault("pending_bull", {})    # (#1) памет за чакащо потвърждение на обема, по тикър
            state.setdefault("ticker_sources", {})  # (#3) последно известен screener източник, по тикър
            return state
        except Exception:
            pass
    return {"premarket_watchlist": [], "last_session": None, "tracked_tickers": [], "tracked_day": None,
            "pending_bull": {}, "ticker_sources": {}}
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
    """Връща (combined, sources) -- combined е списък с тикъри (без дублажи),
    sources е dict {тикър: "screener1+screener2"} -- от кои категории идва
    всеки тикър (може да е повече от една), използва се само за CSV лога
    (колона 'source'), за да видим по-късно кои категории реално дават добри
    сигнали."""
    screener_config = screener_config or SCREENER_CONFIG
    combined, sources, any_success = [], {}, False
    for name, count in screener_config.items():
        try:
            results = yf.screen(name, count=count)
            quotes = results.get("quotes", [])
            if quotes:
                any_success = True
            for q in quotes:
                symbol = q.get("symbol")
                change_pct = q.get("regularMarketChangePercent")
                if not symbol or symbol in EXCLUDED_TICKERS:
                    continue
                if change_pct is None or change_pct < MIN_DAILY_CHANGE_PCT:
                    continue
                if symbol not in sources:
                    sources[symbol] = []
                    combined.append(symbol)
                if name not in sources[symbol]:
                    sources[symbol].append(name)
        except Exception as e:
            print(f"Не успях да изтегля screener '{name}' ({e}) - пропускам го.")
    if not any_success:
        print("Всички screener-и се провалиха - ползвам резервния списък.")
        fallback = [t for t in FALLBACK_TICKERS if t not in EXCLUDED_TICKERS]
        return fallback, {t: "fallback" for t in fallback}
    return combined, {t: "+".join(v) for t, v in sources.items()}
def update_tracked_tickers(tracked, new_trending, max_size=MAX_TRACKED_TICKERS):
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
    data_by_ticker = fetch_batch(tickers, period="2d", interval="5m", prepost=True)
    for ticker, data in data_by_ticker.items():
        try:
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
        time.sleep(REQUEST_DELAY_SEC)
    return top_candidates
# ========================= АНАЛИЗ НА ТИКЪР =========================
def _build_signal_dict(ticker, last, bar_time):
    entry = float(last["Close"])
    take_profit = entry * (1 + TAKE_PROFIT_PCT)
    stop_loss = entry * (1 - STOP_LOSS_PCT)
    now = pd.Timestamp.now(tz=bar_time.tzinfo) if bar_time.tzinfo is not None else pd.Timestamp.now()
    data_age_min = round((now - bar_time).total_seconds() / 60, 1)
    return {
        "ticker": ticker,
        "price": round(entry, 2),
        "take_profit": round(take_profit, 2),
        "stop_loss": round(stop_loss, 2),
        "time": datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S"),
        "bar_time": bar_time.strftime("%H:%M:%S"),
        "data_age_min": data_age_min,
    }
def evaluate_ticker(ticker, data, pending=None):
    """Оценява ЕДИН тикър от вече изтеглени данни (без мрежова заявка), като
    поддържа паметта за "чакащо потвърждение на обема" между отделните
    пускания (виж BULL_PENDING_MAX_CANDLES по-горе):

        Свещ 1: EMA9 пресича EMA20 нагоре, но обемът НЕ е достатъчен -> pending
        Свещ 2: пак няма обем -> продължаваме да чакаме
        Свещ 3: обем > 1.5x среден И над VWAP -> BUY сигнал
        (ако измине BULL_PENDING_MAX_CANDLES свещи без потвърждение, или
        трендът се обърне обратно мечи преди да потвърди -> отказваме се,
        трябва свежо пресичане, за да пробваме пак)

    pending: None, или {"start_bar": ISO timestamp на свещта на пресичането}
    (както е бил запазен в state.json от предишното пускане за този тикър).

    Връща (new_pending, signal): new_pending е какво да се запази за
    следващия цикъл (None ако вече не чакаме нищо за този тикър); signal е
    dict за нов BUY сигнал, ако условията са изпълнени точно сега (иначе
    None).
    """
    try:
        if data.empty or len(data) < EMA_SLOW + 1:
            return pending, None
        data = data.copy()
        data["EMA_fast"] = data["Close"].ewm(span=EMA_FAST).mean()
        data["EMA_slow"] = data["Close"].ewm(span=EMA_SLOW).mean()
        data["VolAvg"] = data["Volume"].rolling(20).mean().shift(1)
        trading_day = data.index.date
        pv = data["Close"] * data["Volume"]
        data["VWAP"] = pv.groupby(trading_day).cumsum() / data["Volume"].groupby(trading_day).cumsum()
        last = data.iloc[-1]
        prev = data.iloc[-2]
        bar_time = data.index[-1]

        is_bullish = last["EMA_fast"] > last["EMA_slow"]
        crossed_up_now = prev["EMA_fast"] <= prev["EMA_slow"] and is_bullish
        volume_ok = pd.notna(last["VolAvg"]) and last["Volume"] > VOLUME_MULTIPLIER * last["VolAvg"]
        above_vwap = last["Close"] > last["VWAP"]

        if not is_bullish:
            # Трендът вече е мечи -- всяко чакане отпада, трябва ново пресичане.
            return None, None

        if pending is None:
            if crossed_up_now:
                if volume_ok and above_vwap:
                    return None, _build_signal_dict(ticker, last, bar_time)  # веднага, на същата свещ
                return {"start_bar": bar_time.isoformat()}, None  # чакаме обем
            return None, None

        # Вече чакахме от предишен цикъл, и трендът е все още бичи.
        try:
            start_bar = pd.Timestamp(pending["start_bar"])
            if start_bar.tzinfo is None and bar_time.tzinfo is not None:
                start_bar = start_bar.tz_localize(bar_time.tzinfo)
        except Exception:
            return None, None  # повредено/непонятно pending -- зачистваме
        candles_waited = round((bar_time - start_bar).total_seconds() / 60 / 5)  # 5-минутни свещи
        if volume_ok and above_vwap:
            return None, _build_signal_dict(ticker, last, bar_time)
        if candles_waited >= BULL_PENDING_MAX_CANDLES:
            return None, None  # изчакахме достатъчно -- отказваме се
        return pending, None  # продължаваме да чакаме
    except Exception as e:
        print(f"Грешка при обработка на {ticker}: {e}")
        return pending, None
def analyze_ticker(ticker):
    """Единичен тикър - тегли и анализира. Пазено за съвместимост/тестове;
    в главния цикъл вече се ползва груповото теглене (fetch_batch) вместо
    това. Без памет за обема (pending винаги None) -- само за бърза ръчна
    проверка на един тикър."""
    data = fetch_batch([ticker], period="2d", interval="5m").get(ticker)
    if data is None:
        return None
    _, signal = evaluate_ticker(ticker, data, pending=None)
    return signal
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
def ticker_in_cooldown(ticker, now_ny, rows=None):
    """True ако този тикър е имал ЗАТВОРЕН сигнал (tp_hit/sl_hit) през
    последните SIGNAL_COOLDOWN_MINUTES минути -- пази от бърза поредица
    сигнали на един и същ тикър около една и съща цена (whipsaw). Приема
    вече прочетени rows по избор (за да не чете CSV-то по два пъти на
    цикъл)."""
    if SIGNAL_COOLDOWN_MINUTES <= 0:
        return False
    rows = rows if rows is not None else read_signals_log()
    closed = [r for r in rows if r["ticker"] == ticker and r["status"] in ("tp_hit", "sl_hit") and r["close_time"]]
    if not closed:
        return False
    try:
        last_close = max(datetime.strptime(r["close_time"], "%Y-%m-%d %H:%M:%S") for r in closed)
    except Exception:
        return False
    last_close = last_close.replace(tzinfo=ZoneInfo("America/New_York"))
    elapsed_min = (now_ny - last_close).total_seconds() / 60
    return 0 <= elapsed_min < SIGNAL_COOLDOWN_MINUTES
def log_new_signal(signal, source=""):
    rows = read_signals_log()
    if any(r["ticker"] == signal["ticker"] and r["status"] == "open" for r in rows):
        return None
    new_id = max([int(r["id"]) for r in rows], default=0) + 1
    rows.append({
        "id": new_id,
        "ticker": signal["ticker"],
        "source": source,
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
    """Проверява отворените позиции (с ЕДНА групова заявка за всички
    отворени тикъри едновременно) и връща новозатворените ТОЗИ РЪН."""
    rows = read_signals_log()
    open_rows = [r for r in rows if r["status"] == "open"]
    newly_closed = []
    if not open_rows:
        return newly_closed
    tickers = sorted({r["ticker"] for r in open_rows})
    price_data = fetch_batch(tickers, period="1d", interval="5m")
    now_str = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")
    for r in open_rows:
        data = price_data.get(r["ticker"])
        if data is None or data.empty:
            continue
        try:
            current_price = float(data["Close"].iloc[-1])
        except Exception:
            continue
        entry = float(r["entry_price"])
        tp = float(r["take_profit"])
        sl = float(r["stop_loss"])
        if current_price >= tp:
            r.update(status="tp_hit", close_price=round(current_price, 2), close_time=now_str,
                      pnl_pct=round((current_price - entry) / entry * 100, 2))
            newly_closed.append(r)
        elif current_price <= sl:
            r.update(status="sl_hit", close_price=round(current_price, 2), close_time=now_str,
                      pnl_pct=round((current_price - entry) / entry * 100, 2))
            newly_closed.append(r)
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
        # ВАЖНО: tracked_tickers вече НЕ се изчиства тук -- пренася се между
        # дните, за да не губим тикър, който вчера е бил "почти пресичащ", но
        # днес временно е изпаднал от топ движещите се. update_tracked_tickers()
        # вече сама маха първо тези, които не трендират, когато трябва да
        # направи място под MAX_TRACKED_TICKERS -- това си остава достатъчно.
        state["premarket_watchlist"] = []  # тази е по дефиниция дневна, нея нулираме
        state["last_session"] = "closed"
        save_state(state)
        return
    if session == "premarket":
        if not ENABLE_PREMARKET_PREP:
            state["last_session"] = "premarket"
            save_state(state)
            return
        previous_watchlist = set(state.get("premarket_watchlist", []))
        candidates, _candidate_sources = get_trending_tickers()
        movers = scan_premarket_movers(candidates)
        current_watchlist = set(m["ticker"] for m in movers)
        added = [t for t in (m["ticker"] for m in movers) if t not in previous_watchlist]
        removed = [t for t in previous_watchlist if t not in current_watchlist]
        state["premarket_watchlist"] = [m["ticker"] for m in movers]
        state["last_session"] = "premarket"
        save_state(state)
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
    today_str = ny_now.strftime("%Y-%m-%d")
    # ВАЖНО (#4): tracked_tickers вече се ПРЕНАСЯ от предишния цикъл/ден
    # директно от state -- вече не се нулира на нов ден. update_tracked_tickers()
    # маха първо тези, които не трендират в момента, когато трябва да
    # направи място под MAX_TRACKED_TICKERS -- пази старите still-valid
    # кандидати по-дълго, вместо да ги губим само защото денят се е сменил.
    tracked_tickers = state.get("tracked_tickers", [])
    new_trending, trending_sources = get_trending_tickers()
    tracked_tickers = update_tracked_tickers(tracked_tickers, new_trending)
    premarket_watchlist = state.get("premarket_watchlist", [])
    if state.get("last_session") in ("premarket", None) and premarket_watchlist:
        tracked_tickers = update_tracked_tickers(tracked_tickers, premarket_watchlist)
        print(f"🚀 Пазарът отвори - приоритет на предпазарните лидери: {premarket_watchlist}")
    state["tracked_tickers"] = tracked_tickers
    state["tracked_day"] = today_str  # само информативно вече, не се ползва за нулиране
    current_tickers = tracked_tickers
    # (#3) Пазим последно известния "източник" (screener категория) за всеки
    # тикър, за да можем да го запишем в CSV-то дори ако сигналът дойде след
    # BULL_PENDING изчакване, през което тикърът вече може да не е сред
    # днешните "свежи" трендиращи резултати.
    ticker_sources = state.get("ticker_sources", {})
    ticker_sources.update(trending_sources)
    ticker_sources = {t: s for t, s in ticker_sources.items() if t in current_tickers}
    state["ticker_sources"] = ticker_sources
    # (#1) Памет за "чакащо потвърждение на обема" по тикър -- пазим само за
    # тикъри, които все още реално следим.
    pending_bull = {t: p for t, p in state.get("pending_bull", {}).items() if t in current_tickers}
    print(f"Проверявам {len(current_tickers)} тикъра (следени общо, таван {MAX_TRACKED_TICKERS}): {current_tickers}")
    # Тегли данните за следените тикъри И за пазарния "барометър" (SPY) с ЕДНА
    # обща заявка (по-евтино), после ги разделя.
    fetch_list = list(current_tickers)
    if ENABLE_MARKET_TREND_FILTER and MARKET_TREND_TICKER not in fetch_list:
        fetch_list.append(MARKET_TREND_TICKER)
    ticker_data = fetch_batch(fetch_list, period="2d", interval="5m")
    market_bullish = compute_market_trend(ticker_data.get(MARKET_TREND_TICKER))
    if not market_bullish:
        print(f"⚠️ Пазарният тренд ({MARKET_TREND_TICKER}) е мечи (под VWAP) - пропускам нови BUY сигнали този цикъл.")
    cooldown_rows = read_signals_log()  # четем веднъж, не за всеки тикър поотделно
    any_signal = False
    for ticker in current_tickers:
        data = ticker_data.get(ticker)
        if data is None:
            continue
        new_pending, signal = evaluate_ticker(ticker, data, pending_bull.get(ticker))
        if new_pending is None:
            pending_bull.pop(ticker, None)
        else:
            pending_bull[ticker] = new_pending
        if signal:
            if not market_bullish:
                print(f"[{ticker}] BUY сигнал засечен, но пазарният тренд е мечи - пропускам известието.")
                continue
            # (#2) Cooldown -- пази от бърза поредица сигнали на един и същ
            # тикър скоро след предишен затворен сигнал (whipsaw).
            if ticker_in_cooldown(ticker, ny_now, rows=cooldown_rows):
                print(f"[{ticker}] BUY сигнал засечен, но е в cooldown ({SIGNAL_COOLDOWN_MINUTES} мин "
                      f"след последен затворен сигнал) - пропускам.")
                continue
            any_signal = True
            stale = signal["data_age_min"] > STALE_DATA_WARNING_MIN
            source = ticker_sources.get(ticker, "")
            logged_id = log_new_signal(signal, source=source)
            duplicate = logged_id is None
            msg = build_buy_signal_message(signal, stale, duplicate)
            print(msg.replace("<b>", "").replace("</b>", "").replace("<i>", "").replace("</i>", "").replace("<code>", "").replace("</code>", ""))
            if not duplicate:
                send_telegram(msg)
    state["pending_bull"] = pending_bull
    if not any_signal:
        print("Няма сигнали в момента.")
    summary = win_rate_summary_text()
    if summary:
        print(summary)
    if ny_now.hour == 15 and ny_now.minute >= 55:
        summary_msg = build_session_summary_message()
        if summary_msg:
            send_telegram(summary_msg)
    state["last_session"] = "open"
    save_state(state)
if __name__ == "__main__":
    main()
