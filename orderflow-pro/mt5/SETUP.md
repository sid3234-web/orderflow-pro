# MT5 bridge setup (demo-only first)

This bridge reads OrderFlow Pro's public, read-only signal endpoint. It does not send orders automatically. The EA shows an approval button on the MT5 chart. Order placement is locked by default (`InpEnableDemoOrders = false`) and the EA refuses to place orders unless the account is reported by MT5 as a DEMO account.

## Files

- `OrderFlowBridgeEA.mq5` — MT5 Expert Advisor source.
- Endpoint: `https://orderflow-pro-4m31.onrender.com/api/mt5-signal/BTCUSDT`

## First verify the endpoint

Open the endpoint URL in a browser after the Render deployment finishes. It should show plain text fields including `symbol=BTCUSDT`, `side=WAIT` or `side=BUY/SELL`, `eligible=true/false`, and `execution=disabled`. A WAIT signal is normal when the order book is invalid or the signal rules are not met.

## Install in MT5

1. In the main Exness MT5 desktop app, choose **File → Open Data Folder**.
2. Open `MQL5\Experts\Advisors` (or another subfolder under `MQL5\Experts` that you use).
3. Copy `OrderFlowBridgeEA.mq5` into that folder. If you use the file from GitHub, open the file and copy its entire contents into MetaEditor, then save it under that path.
4. In MT5, open **Tools → Options → Expert Advisors**.
5. Enable **Allow WebRequest for listed URL** and add exactly `https://orderflow-pro-4m31.onrender.com` to the list.
6. Open the EA in MetaEditor and compile it. Check the Errors tab. The code must compile with zero errors before use.
7. In MT5 Navigator, right-click **Expert Advisors → Refresh**, then attach the EA to the BTCUSD chart.
8. Keep **Algo Trading OFF** during the read-only connection test. The EA should show the current signal on the chart/log. The approval button will say execution is locked.

## Trading safety

- `InpEnableDemoOrders` is false by default. Do not enable it until the endpoint, broker symbol, minimum volume, and stop distances have been checked.
- If later enabled, the EA has an additional DEMO-account check and requires a manual click on the chart button. It uses a 14-period M1 ATR for the initial stop (2 ATR) and target (1.5R), and rejects orders when the spread is over 20% of ATR or the broker's minimum stop distance is not met.
- `InpTradeSymbol` defaults to `BTCUSD`; confirm the exact symbol spelling in your Exness account.
- This is a prototype, not a verified production trading system. MQL5 compilation and demo-account forward testing are required. The dashboard's heuristic signals are not a promise of profitability.
- Never enable this EA on a live account. Do not add broker passwords or account credentials to the dashboard or GitHub.
