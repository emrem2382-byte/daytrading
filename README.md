# Intraday Stock Signal Scanner

Runs for free on GitHub Actions every 5 minutes (04:00-16:00 New York time,
weekdays), scans up to 250 US stocks across 5 different "trending" categories,
sends BUY signals and closed-position (TP/SL) alerts to Telegram — with a
candlestick chart attached — and keeps a permanent history in
`signals_log.csv` inside the repo itself.

## What it does

- **Signal logic:** EMA9/EMA20 bullish crossover, confirmed by volume (>1.5x
  the 20-candle average) and price above VWAP, on 5-minute candles.
- **Volume-confirmation memory:** if the crossover happens but volume hasn't
  confirmed yet, the scanner keeps watching for up to 3 more candles instead
  of giving up immediately — catches real breakouts where volume follows the
  price by a candle or two.
- **Market-trend filter:** skips new BUY signals while SPY (S&P 500 ETF) is
  below its own VWAP for the day (broad market weak).
- **Ticker coverage:** up to 250 tickers tracked at once, sourced from 5
  Yahoo Finance screeners (most active, day gainers, small-cap gainers,
  aggressive small caps, most shorted) — refreshed every cycle, and
  **carried over between trading days** so a ticker that was building
  momentum yesterday isn't dropped just because it briefly falls out of
  today's top movers.
- **Cooldown:** after a signal closes (TP or SL hit) for a ticker, waits 30
  minutes before allowing a new signal on the same ticker — avoids rapid
  back-to-back whipsaw alerts.
- **Telegram charts:** every BUY signal and every closed position comes with
  a candlestick chart (EMA9/EMA20, VWAP, volume, and entry/TP/SL/exit lines)
  attached, not just plain text.
- **Premarket prep:** in the last 30 minutes before the open, builds a
  watchlist of the biggest premarket movers and prioritizes them once the
  market opens.
- This is an **alert-only** system — it never places any real trade. You act
  on the Telegram signal manually in your own brokerage app.

## One-time setup (~5 minutes)

### 1. Upload these files to a new GitHub repo
- Go to [github.com](https://github.com) → New repository.
- Upload every file from this folder, including the hidden `.github/` folder.
- **Recommendation: make the repo public.** GitHub Actions minutes are
  unlimited on public repos; on a private repo, this schedule (5-minute
  interval, market hours only) can use several thousand minutes a month,
  well past the free private-repo quota (2000 min/month). Nothing sensitive
  is exposed by going public — there's no brokerage API key here, only a
  Telegram token (kept as a GitHub Secret either way, never in the code),
  and the CSV log only shows hypothetical percentage outcomes for publicly
  known tickers, not real account balances or trade sizes.

### 2. Add the two secrets
In the repo: **Settings → Secrets and variables → Actions → New repository secret**

| Name | Value |
|---|---|
| `TELEGRAM_BOT_TOKEN` | your bot's token from @BotFather |
| `TELEGRAM_CHAT_ID` | your Telegram chat ID |

### 3. Test it manually
- In the repo: **Actions** tab → select "Intraday Stock Scanner" → **Run workflow** button (top right).
- Wait about a minute and check for a Telegram message (or at least that the Actions log finished without errors).

### 4. Done
From here on, the schedule runs it automatically every 5 minutes on
weekdays. Nothing else to do — `signals_log.csv` and `state.json` are
updated and committed back to the repo automatically after every run.

## Important notes

- **Cost:** free on a public repo (unlimited Actions minutes). On a private
  repo, budget carefully — see the setup recommendation above.
- **Timing isn't second-precise.** GitHub's scheduler can delay a run by a
  few minutes under load. Fine for an informational signal, not for
  split-second execution.
- **60-day inactivity auto-pause:** GitHub disables a scheduled workflow if
  no commit happens in the repo for 60 days. Since the scanner commits its
  own state file on every weekday run, this shouldn't normally trigger —
  but if the repo goes fully quiet (e.g. paused over an extended market
  closure), check that the workflow is still enabled under the Actions tab.
- **This is signal generation, not financial advice.** The scanner tells you
  when its rules fired; whether and how to act on that is entirely your own
  decision.
