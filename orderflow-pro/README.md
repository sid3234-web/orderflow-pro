# OrderFlow Pro — local crypto order-flow terminal

A local-first, alert-only crypto analytics prototype for Windows. It connects to Binance Spot public market-data endpoints; it does **not** connect to an account, request API keys, or place orders.

## What is live vs. historical

- **Live:** trade stream, best-effort live candles, synchronized local order book (snapshot + depth diffs), current depth visualization, footprint bars computed from received trades, and rule-based alerts.
- **Historical candles:** up to 30 days of 1-minute OHLCV fetched from Binance REST when the app starts (subject to API availability and exchange limits).
- **Not historical:** Binance public streams do not supply a ready-made 30-day historical order-book heatmap or tick-level historical footprint. Heatmap history begins when this app starts; footprint aggregation starts when trades are received. The chart marks historical candles separately from live-updated candles.
- **Signals:** transparent heuristics, not institutional intelligence or a promise of profitability. A displayed score is not a probability of profit. Alerts are informational only.

## Requirements

- Windows 10/11
- Python 3.11+ recommended
- Internet access to Binance public REST and WebSocket endpoints
- No exchange account or API keys needed

## Run on Windows

Open PowerShell in the project directory:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

If PowerShell blocks activation, run:
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

Then open http://127.0.0.1:8000

## Run tests

```powershell
python -m unittest discover -s tests -v
```

## Controls

- Select BTCUSDT, ETHUSDT or SOLUSDT.
- Toggle footprint, heatmap, indicators, trendline, and signal markers independently.
- Enable/disable alert popups.
- The alert engine only emits a new alert when its simple confluence rules change into a qualifying state. This avoids repeatedly alerting on every update.
- Data status and book validity are visible in the terminal.

## Architecture

- `app/main.py`: FastAPI HTTP + WebSocket app, historical candle fetch, stream lifecycle.
- `app/orderbook.py`: snapshot/diff synchronization with update-ID checks and resync.
- `app/analytics.py`: trade aggregation, footprint bars, transparent heuristic signals.
- `app/static/index.html`: chart, footprint/heatmap canvas overlays and feature toggles.
- `tests/`: unit tests for order-book sequencing, footprint aggregation, and signal safety.

## Notes / limitations

1. Binance endpoints and stream availability can change; connection status is surfaced in the UI.
2. The app is configured for Binance Spot. It does not merge books from different exchanges.
3. Depth heatmap is a runtime history of sampled resting liquidity, not 30-day archived depth. Historical depth requires a separately sourced dataset or paid provider.
4. The footprint is trade-derived. Aggressor side is inferred from Binance's `m` field (`m=true` means buyer is maker, therefore seller-initiated trade). It is not an exchange-provided "institutional" classification.
5. The starter signal rules are intentionally conservative and require valid, fresh book data. They must be backtested before any reliance.
6. Browser notifications require permission and may require HTTPS in some deployment contexts; in localhost, the browser usually treats it as a secure context.
7. Do not expose this app to the public internet without authentication, TLS, rate limiting, and operational hardening.
