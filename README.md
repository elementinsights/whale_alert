# Whale Alerts Runner

A Python bot that streams **crypto whale alerts** to Telegram and logs them into Google Sheets.  
It combines two distinct data sources from the CoinGlass API:

- **Hyperliquid Whale Alerts** (wallet-level opens/closes above thresholds)
- **Large Orderbook Fills** (large filled orders across major exchanges)

---

## Features

- Per-coin USD notional thresholds (global fallback + coin overrides via `.env`)
- Exchange filtering for Large Orderbook endpoint
- Dedupe mechanism to avoid duplicate alerts
- Telegram notifications with rich formatting
- Google Sheets logging with automatic header setup

---

## Installation

1. Clone or move this project into your desired folder:

   ```bash
   cd /path/to/your/project
   ```

2. Create and activate a virtual environment:

   ```bash
   python3 -m venv venv
   source venv/bin/activate   # On macOS/Linux
   venv\Scripts\activate    # On Windows
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

---

## Configuration

All configuration is handled via your `.env` file in the same folder as the script.

### Required

```env
COINGLASS_API_KEY=your_api_key_here
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=@your_channel_or_chat_id
GOOGLE_SHEETS_ID=your_google_sheet_id
GOOGLE_SA_JSON=/absolute/path/to/service_account.json
```

### Optional

```env
ALERT_TAG=[Whale]
GOOGLE_SHEETS_TAB=Alerts
GSHEET_WEBHOOK_URL=https://your-fallback-webhook
WATCH_COINS=BTC,ETH,SOL,XRP,DOGE,LINK
INTERVAL_S=30
HL_ENABLED=true
LOB_ENABLED=true
MIN_NOTIONAL_USD=1000000
MIN_NOTIONAL_BTC=5000000
MIN_NOTIONAL_ETH=2000000
PER_REQUEST_PAUSE=0.25
DEDUPE_TTL_MIN=180
```

### Exchanges

- `EXCHANGES` only applies to the **Large Orderbook** feed.  
- Example:

  ```env
  EXCHANGES=Binance,Bybit
  ```

- Supported exchanges include: Binance, OKX, Bybit, Bitget, Coinbase, Kraken, Deribit, Huobi, KuCoin.

⚠️ **Note:** Hyperliquid alerts are separate and always run when `HL_ENABLED=true`, regardless of what you set in `EXCHANGES`.

---

## Usage

Run one iteration (testing mode):

```bash
python whale_alert_runner.py --once
```

Run continuously:

```bash
python whale_alert_runner.py
```

Stop with `Ctrl-C`.

---

## Server Commands

### Login
```bash
ssh -i ~/.ssh/heatmap_do heatmap@206.189.79.36
```


### Upload Files
```bash
rsync -avz --progress \
  --exclude '.venv/' --exclude 'venv/' --exclude '__pycache__/' \
  -e "ssh -i ~/.ssh/heatmap_do" \
  whale_alert_runner.py .env service_account.json \
  heatmap@206.189.79.36:/home/heatmap/whale-alert/
```

### Reload and Start
```bash
systemctl --user daemon-reload
systemctl --user enable --now whale-alert.service
journalctl --user -u whale-alert -f
```

### Logs
```bash
journalctl --user -u whale-alert -f
```

### Start on Server
```bash
sudo systemctl start whale-alert
```

### Modify System File
```bash
sudo nano /etc/systemd/system/whale-alert.service
```

## Google Sheets Setup

- Share your target Google Sheet with the **Service Account email** from your JSON key.  
- Ensure `GOOGLE_SHEETS_ID` is set in `.env`.

---

## Example Output

Telegram notification:

```
Whale Alert SOL • Hyperliquid
Action: Open Long
Notional: $2,000,000 • Size: 12,000
Price: $205.87
UTC: 2025-08-28 12:55:03
```

---

## License

MIT
