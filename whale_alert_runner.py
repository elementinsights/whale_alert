#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Whale Alerts Runner (Telegram + Google Sheets) — HL-only, entry-time & no-backfill (+lag +debug)
- Hyperliquid whale alerts only (executed opens/closes).
- Shows the trade's execution time (Hyperliquid create_time) in Telegram + Sheets.
- Ignores historical alerts prior to script start, but allows a small server-lag window.
- Read-quota friendly: single startup read; write-only thereafter.
- Tracks %Δ vs LIVE market price over time.
- Debug prints show how many events were fetched each cycle (and a few samples).

ENV:
  COINGLASS_API_KEY (required)

  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID (or TELEGRAM_CHANNEL)
  ALERT_TAG (optional)

  GOOGLE_SHEETS_ID (required for Sheets logging)
  GOOGLE_SHEETS_TAB (default 'Alerts')
  GOOGLE_SA_JSON (path to service account JSON; file must be shared with the SA email)
  GSHEET_WEBHOOK_URL (optional fallback for appends)

  WATCH_COINS            default: "BTC,ETH,SOL,XRP,DOGE,LINK"
  INTERVAL_S             default: 60
  MIN_NOTIONAL_USD       default: 1000000
  DEDUP_TTL_MIN          default: 180
  ALLOWED_LAG_S          default: 120   # accept events up to this many seconds before script start

  PRICE_POLL_TIMEOUT_S   default: 8
  PRICE_COOLDOWN_S       default: 2

  COINGLASS_BASE          default: https://open-api-v4.coinglass.com
  COINGLASS_FALLBACK_BASE default: https://open-api.coinglass.com
"""

import os, json, time, uuid, argparse, requests, datetime as dt
from collections import deque
from pathlib import Path
from dotenv import load_dotenv

# ---------- Startup ----------
SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=SCRIPT_DIR / ".env", override=True)

TRACK_FILE = str(SCRIPT_DIR / "price_track_state.json")
if os.path.exists(TRACK_FILE):
    try:
        os.remove(TRACK_FILE)
        print(f"[info] Deleted old {Path(TRACK_FILE).name}")
    except Exception as e:
        print(f"[warn] Could not delete old {Path(TRACK_FILE).name}: {e}")

# Capture script start time (seconds since epoch, UTC); used to ignore historical alerts
SCRIPT_START_TS = time.time()

# ---------- Config ----------
API_HOSTS = [
    os.getenv("COINGLASS_BASE", "https://open-api-v4.coinglass.com").rstrip("/"),
    os.getenv("COINGLASS_FALLBACK_BASE", "https://open-api.coinglass.com").rstrip("/"),
]
ENDPOINT_HL_ALERT = "/api/hyperliquid/whale-alert"

DEFAULT_WATCH = [
    c.strip().upper()
    for c in os.getenv("WATCH_COINS", "BTC,ETH,SOL,XRP,DOGE,LINK").split(",")
    if c.strip()
]
INTERVAL_S = int(os.getenv("INTERVAL_S", "20"))
DEDUP_TTL_MIN = int(os.getenv("DEDUP_TTL_MIN", "180"))
ALLOWED_LAG_S = int(os.getenv("ALLOWED_LAG_S", "120"))

PRICE_POLL_TIMEOUT_S = int(os.getenv("PRICE_POLL_TIMEOUT_S", "8"))
PRICE_COOLDOWN_S     = float(os.getenv("PRICE_COOLDOWN_S", "2"))

# ---------- Thresholds ----------
DEFAULT_MIN_NOTIONAL_USD = float(os.getenv("MIN_NOTIONAL_USD", "1000000"))
MIN_NOTIONAL_BY_SYMBOL = {
    "BTC": float(os.getenv("MIN_NOTIONAL_BTC", "1000")),
    "ETH": float(os.getenv("MIN_NOTIONAL_ETH", "1000")),
    "SOL": float(os.getenv("MIN_NOTIONAL_SOL", "1000")),
    "XRP": float(os.getenv("MIN_NOTIONAL_XRP", "1000")),
    "DOGE": float(os.getenv("MIN_NOTIONAL_DOGE", "1000")),
    "LINK": float(os.getenv("MIN_NOTIONAL_LINK", "1000")),
    "HYPE": float(os.getenv("MIN_NOTIONAL_HYPE", "1000")),
}

def min_notional_for(symbol: str) -> float:
    sym = (symbol or "").upper()
    v = os.getenv(f"MIN_NOTIONAL_{sym}")
    if v:
        try:
            return float(v)
        except Exception:
            pass
    return float(MIN_NOTIONAL_BY_SYMBOL.get(sym, DEFAULT_MIN_NOTIONAL_USD))

def effective_thresholds_for_watchlist():
    return {sym: min_notional_for(sym) for sym in DEFAULT_WATCH}

# ---------- Utils ----------
def fmt_usd(x):
    try:
        x = float(x)
        return f"${x:,.0f}" if abs(x) >= 1000 else f"${x:,.4f}" if abs(x) < 1 else f"${x:,.2f}"
    except Exception:
        return str(x)

def uid() -> str:
    return uuid.uuid4().hex[:12]

def ts_to_utc_dt(ts_seconds: float) -> dt.datetime:
    return dt.datetime.fromtimestamp(ts_seconds, tz=dt.timezone.utc)

def fmt_ts(ts_seconds: float):
    d = ts_to_utc_dt(ts_seconds)
    return d.strftime("%Y-%m-%d"), d.strftime("%H:%M:%S")

def get_api_key() -> str:
    k = os.getenv("COINGLASS_API_KEY")
    if not k:
        raise RuntimeError("Missing COINGLASS_API_KEY in .env")
    return k

def shorten_hl_url(url: str) -> str:
    # Display https://www.coinglass.com/hyperliquid/0x9...fc4
    try:
        base, addr = url.rsplit("/", 1)
        if len(addr) > 8:
            disp = f"{addr[:3]}...{addr[-3:]}"
        else:
            disp = addr
        return f"{base}/{disp}"
    except Exception:
        return url

# ---------- HTTP session ----------
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_SESSION = requests.Session()
_RETRY = Retry(
    total=2, connect=2, read=2,
    backoff_factor=0.4,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=False
)
_SESSION.mount("https://", HTTPAdapter(max_retries=_RETRY))

def http_get(path, params=None, timeout=20):
    headers = {"CG-API-KEY": get_api_key(), "accept": "application/json"}
    last_err = None
    for host in API_HOSTS:
        url = f"{host}{path}"
        try:
            r = _SESSION.get(url, headers=headers, params=params or {}, timeout=timeout)
            try:
                j = r.json()
            except Exception:
                j = {}
            if r.status_code == 200 and str(j.get("code")) == "0":
                return j.get("data", [])
            if r.status_code in (401, 403):
                raise RuntimeError(f"Auth/plan error {r.status_code}: {j.get('msg') or r.text}")
            last_err = RuntimeError(f"HTTP {r.status_code} / code={j.get('code')} / msg={j.get('msg')}")
        except requests.exceptions.RequestException as e:
            last_err = e
            continue
    raise RuntimeError(f"All CoinGlass hosts failed for {path}: {last_err}")

# ---------- Telegram ----------
def send_telegram(text: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("TELEGRAM_CHANNEL")
    tag = os.getenv("ALERT_TAG", "").strip()
    if tag:
        text = f"{tag} {text}"
    if not (token and chat_id):
        print("[warn] Telegram not sent: missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID", flush=True)
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        r = requests.post(url, json=payload, timeout=15)
        ok = (r.status_code == 200 and r.json().get("ok") is True)
        if not ok:
            print("[error] Telegram send failed:", r.text, flush=True)
        return ok
    except Exception as e:
        print("[error] Telegram exception:", e, flush=True)
        return False

def telegram_lines(evt):
    # Use execution timestamp (evt['ts'])
    entry_date, entry_time = fmt_ts(evt["ts"])
    lines = ["🐳🐳🐳 <b>¡ALERTA BALLENA!</b> 🐳🐳🐳"]
    lines.append(f"Coin: {evt['symbol']}")
    lines.append(f"Action: {evt['action']}")
    lines.append(f"Notional: {fmt_usd(evt['notional'])} | Size: {evt['size']}")
    entry = evt.get("entry_price")
    mark  = evt.get("price")  # Market Price at alert time
    if entry is not None or mark is not None:
        if entry is not None and mark is not None:
            lines.append(f"Entry: {fmt_usd(entry)} | Market Price: {fmt_usd(mark)}")
        elif mark is not None:
            lines.append(f"Market Price: {fmt_usd(mark)}")
        else:
            lines.append(f"Entry: {fmt_usd(entry)}")
    lines.append(f"UTC: {entry_date} at {entry_time}")  # execution time
    if evt.get("url"):
        disp = shorten_hl_url(evt["url"])
        lines.append(f'Transaction: <a href="{evt["url"]}">{disp}</a>')
    return "\n".join(lines)

# ---------- Google Sheets (read-quota friendly) ----------
HEADER_ROW = 1
BASE_HEADERS = [
    "Date","Time","Exchange","Symbol","Action",
    "Size","Market Price","Entry","Liq/Side","NotionalUSD","URL"
]

def build_tracker_minutes():
    mins = [1,2,3,4,5]
    mins += list(range(10, 31, 5))
    mins += list(range(45, 181, 15))
    mins += list(range(210, 721, 30))
    mins += list(range(780, 1441, 60))
    return mins

TRACK_MINUTES = build_tracker_minutes()
TRACK_HEADERS = [
    f"%Δ {m}m" if m < 60 else (f"%Δ {m//60}h" if m % 60 == 0 else f"%Δ {m}m")
    for m in TRACK_MINUTES
]
SHEET_HEADERS = BASE_HEADERS + TRACK_HEADERS + ["UID"]
TRACK_COL_OFFSET = len(BASE_HEADERS)

def col_number_to_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

class SheetsClient:
    def __init__(self):
        self.ws = None
        self.next_row = None
        self._init()

    def _init(self):
        sheet_id = os.getenv("GOOGLE_SHEETS_ID")
        tab_name = os.getenv("GOOGLE_SHEETS_TAB", "Alerts")
        if not sheet_id:
            print("[warn] Sheets not configured: missing GOOGLE_SHEETS_ID", flush=True)
            return
        try:
            from google.oauth2.service_account import Credentials
            import gspread
            creds_path = os.getenv("GOOGLE_SA_JSON")
            if not creds_path or not os.path.exists(creds_path):
                raise RuntimeError("Missing/invalid GOOGLE_SA_JSON")
            scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(sheet_id)
            try:
                ws = sh.worksheet(tab_name)
            except Exception:
                ws = sh.add_worksheet(title=tab_name, rows=8000, cols=max(60, len(SHEET_HEADERS)+4))
            vals = ws.get_values(f"A{HEADER_ROW}:ZZ{HEADER_ROW}")
            headers = (vals[0] if vals else [])
            if headers[:len(SHEET_HEADERS)] != SHEET_HEADERS:
                self._update_range(ws, f"A{HEADER_ROW}", [SHEET_HEADERS])
                self.next_row = HEADER_ROW + 1
            else:
                colA = ws.col_values(1)
                self.next_row = len(colA) + 1 if colA else HEADER_ROW + 1
            self.ws = ws
            print(f"[info] Sheets ready. Next row: {self.next_row}", flush=True)
        except Exception as e:
            print("[error] Sheets open/init failed:", e, flush=True)
            self.ws = None

    def _with_backoff(self, func, *args, **kwargs):
        max_attempts = 5
        delay = 1.0
        for attempt in range(1, max_attempts + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                msg = str(e)
                if "429" in msg or "Rate Limit" in msg or "quota" in msg.lower():
                    if attempt == max_attempts:
                        raise
                    time.sleep(delay)
                    delay = min(delay * 2, 16)
                else:
                    raise

    def _update_range(self, ws, start_a1, values_2d):
        return self._with_backoff(ws.update, range_name=start_a1, values=values_2d, value_input_option="RAW")

    def append_rows(self, rows_2d):
        if not self.ws or not rows_2d:
            return []
        start_row = self.next_row or (HEADER_ROW + 1)
        a1 = f"A{start_row}"
        self._update_range(self.ws, a1, rows_2d)
        written_rows = list(range(start_row, start_row + len(rows_2d)))
        self.next_row = start_row + len(rows_2d)
        return written_rows

    def update_pct_cell(self, row: int, track_minute: int, pct_value: float) -> bool:
        if not self.ws:
            return False
        try:
            idx = TRACK_MINUTES.index(track_minute)
        except ValueError:
            return False
        sheet_col = TRACK_COL_OFFSET + idx + 1
        col_letter = col_number_to_letter(sheet_col)
        a1 = f"{col_letter}{row}"
        try:
            self._update_range(self.ws, a1, [[f"{pct_value:.2f}%"]])
            return True
        except Exception as e:
            print(f"[warn] update pct cell failed @ row {row}, {track_minute}m: {e}", flush=True)
            return False

_SHEETS = SheetsClient()

def append_rows(rows):
    if _SHEETS.ws:
        try:
            written_rows = _SHEETS.append_rows(rows)
            return True, written_rows
        except Exception as e:
            print("[error] Sheets append failed:", e, flush=True)
    url = os.getenv("GSHEET_WEBHOOK_URL")
    if not url:
        return False, []
    try:
        r = requests.post(url, json={"rows": rows}, timeout=20)
        ok = (r.status_code == 200 and isinstance(r.json(), dict) and r.json().get("ok") is True)
        if not ok:
            print("[error] Webhook append failed:", r.text, flush=True)
        return ok, []
    except Exception as ee:
        print("[error] Webhook exception:", ee, flush=True)
        return False, []

def update_pct_cell(row_index, minutes_from_start, pct_value):
    if row_index is None:
        return False
    return _SHEETS.update_pct_cell(row_index, minutes_from_start, pct_value)

# ---------- Dedupe ----------
class SeenCache:
    def __init__(self, ttl_min=180, maxlen=6000):
        self.ttl = ttl_min * 60
        self.buf = deque(maxlen=maxlen)
        self.set = {}

    def add(self, key):
        ts = time.time()
        self.buf.append((key, ts))
        self.set[key] = ts
        self._gc()

    def seen(self, key):
        self._gc()
        return key in self.set

    def _gc(self):
        cutoff = time.time() - self.ttl
        while self.buf and self.buf[0][1] < cutoff:
            k,_ = self.buf.popleft()
            self.set.pop(k, None)

# ---------- Hyperliquid fetch ----------
def fetch_hl_whale_alerts():
    data = http_get(ENDPOINT_HL_ALERT)
    out = []
    for e in data or []:
        sym = str(e.get("symbol","")).upper()
        if DEFAULT_WATCH and sym not in DEFAULT_WATCH:
            continue
        notional = float(e.get("position_value_usd", 0) or 0)
        if notional < min_notional_for(sym):
            continue
        size = float(e.get("position_size", 0) or 0)
        act_code = int(e.get("position_action", 0) or 0)  # 1=open, 2=close
        side = "Long" if size > 0 else ("Short" if size < 0 else "Flat")
        action = "Open" if act_code == 1 else ("Close" if act_code == 2 else f"Act{act_code}")
        ts_ms = int(e.get("create_time", 0) or 0)  # execution time (ms)
        ts_seconds = ts_ms/1000 if ts_ms else time.time()
        url = f"https://www.coinglass.com/hyperliquid/{e.get('user')}"

        out.append({
            "exchange": "Hyperliquid",
            "address": e.get("user",""),  # internal for dedupe only
            "symbol": sym,
            "action": f"{action} {side}",
            "size": size,
            "price": None,                       # Market Price filled later
            "entry_price": float(e.get("entry_price", 0) or 0),
            "liq_or_side": f"Liq {float(e.get('liq_price', 0) or 0):,.2f}",
            "notional": notional,
            "ts": ts_seconds,                    # execution time (seconds)
            "url": url
        })
    return out

# ---------- Price polling ----------
def fetch_price_now(symbol: str):
    s = (symbol or "").upper()
    # 1) Binance USDT
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": f"{s}USDT"},
            timeout=PRICE_POLL_TIMEOUT_S
        )
        if r.status_code == 200:
            j = r.json()
            p = float(j.get("price"))
            if p > 0: return p
    except Exception:
        pass
    time.sleep(PRICE_COOLDOWN_S)
    # 2) Coinbase USD
    try:
        r = requests.get(
            f"https://api.exchange.coinbase.com/products/{s}-USD/ticker",
            timeout=PRICE_POLL_TIMEOUT_S
        )
        if r.status_code == 200:
            j = r.json()
            p = float(j.get("price") or j.get("last") or 0)
            if p > 0: return p
    except Exception:
        pass
    return None

# ---------- Tracking state ----------
def load_track_state():
    try:
        with open(TRACK_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"items": []}

def save_track_state(state):
    try:
        with open(TRACK_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        print("[warn] save_track_state failed:", e, flush=True)

def build_checkpoint_minutes():
    return TRACK_MINUTES[:]

def schedule_tracker(uid_val, symbol, base_price, start_ts, row_index):
    return {
        "uid": uid_val,
        "row": row_index,
        "symbol": symbol.upper(),
        "p0": float(base_price),
        "t0": float(start_ts),   # baselined to execution time
        "due": build_checkpoint_minutes(),
        "done": []
    }

def process_trackers():
    state = load_track_state()
    if not state["items"]: return
    now_ts = time.time()
    changed = False
    for tr in list(state["items"]):
        still_due = []
        for m in tr["due"]:
            if now_ts - tr["t0"] >= m * 60 - 1:
                p = fetch_price_now(tr["symbol"])
                if p:
                    pct = (p - tr["p0"]) / tr["p0"] * 100.0
                    ok = update_pct_cell(tr["row"], m, pct)
                    if ok:
                        tr["done"].append(m)
                        changed = True
                    time.sleep(PRICE_COOLDOWN_S)
                else:
                    still_due.append(m)
            else:
                still_due.append(m)
        tr["due"] = still_due
        if not tr["due"]:
            state["items"].remove(tr)
            changed = True
    if changed:
        save_track_state(state)

# ---------- Row assembly (uses execution time) ----------
def to_sheet_row(evt, uid_val):
    date_str, time_str = fmt_ts(evt["ts"])
    return [
        date_str,
        time_str,
        evt["exchange"],
        evt["symbol"],
        evt["action"],
        evt["size"],
        evt.get("price"),
        evt.get("entry_price"),
        evt["liq_or_side"],
        evt["notional"],
        evt["url"],
        *[""] * len(TRACK_HEADERS),
        uid_val
    ]

# ---------- Main loop ----------
def run_loop():
    seen = SeenCache(ttl_min=DEDUP_TTL_MIN)
    print("=== Whale Alerts Runner (HL-only, entry-time, no-backfill +lag +debug) ===", flush=True)
    thr = effective_thresholds_for_watchlist()
    thr_str = ", ".join(f"{k}:{fmt_usd(v)}" for k,v in thr.items())
    print(f"API hosts: {', '.join(API_HOSTS)}", flush=True)
    print(f"Watchlist: {', '.join(DEFAULT_WATCH)}", flush=True)
    print(f"Per-coin thresholds: {thr_str} | fallback(default)={fmt_usd(DEFAULT_MIN_NOTIONAL_USD)}", flush=True)
    print(f"Interval={INTERVAL_S}s | AllowedLag={ALLOWED_LAG_S}s", flush=True)
    print("Price tracking enabled: will fill %Δ columns up to 24h.\n", flush=True)

    while True:
        staged = []

        try:
            events = fetch_hl_whale_alerts()
            print(f"[debug] fetched {len(events)} HL events")
            for e in events[:3]:
                print("[debug] sample:", e.get("symbol"), e.get("notional"), e.get("ts"))

            for evt in events:
                # Ignore events older than script start, but allow small lag tolerance
                if evt["ts"] < SCRIPT_START_TS - ALLOWED_LAG_S:
                    continue

                key = ("HL", evt["address"], evt["symbol"], evt["action"], round(evt["notional"]), int(evt["ts"] // 60))
                if seen.seen(key):
                    continue

                mark = fetch_price_now(evt["symbol"])
                if mark is not None:
                    evt["price"] = mark

                send_telegram(telegram_lines(evt))
                u = uid()
                staged.append((evt, u))
                seen.add(key)
        except Exception as e:
            print(f"HL fetch error: {e}", flush=True)

        if staged:
            rows = [to_sheet_row(evt, u) for (evt, u) in staged]
            ok, written_rows = append_rows(rows)
            if ok:
                state = load_track_state()
                for (evt, u), row_index in zip(staged, (written_rows or [None]*len(staged))):
                    if evt.get("price") is not None and row_index is not None:
                        state["items"].append(
                            schedule_tracker(u, evt["symbol"], evt["price"], evt["ts"], row_index)
                        )
                save_track_state(state)
                print(f"appended {len(rows)} row(s) + staged trackers", flush=True)
            else:
                print("sheet append failed (trackers not saved)", flush=True)

        try:
            process_trackers()
        except Exception as e:
            print("[warn] process_trackers error:", e, flush=True)

        time.sleep(INTERVAL_S)

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Whale Alerts Runner — HL-only, entry-time, no-backfill +lag +debug")
    ap.add_argument("--once", action="store_true", help="Run one iteration then exit (for testing)")
    args = ap.parse_args()

    if args.once:
        try:
            events = fetch_hl_whale_alerts()
            print(f"[debug] fetched {len(events)} HL events (--once)")
            for e in events[:5]:
                print("[debug] sample:", e.get("symbol"), e.get("notional"), e.get("ts"))
            staged = []
            for evt in events:
                if evt["ts"] < SCRIPT_START_TS - ALLOWED_LAG_S:
                    continue
                mark = fetch_price_now(evt["symbol"])
                if mark is not None:
                    evt["price"] = mark
                print(evt)
                u = uid()
                staged.append((evt, u))

            if staged:
                rows = [to_sheet_row(evt, u) for (evt,u) in staged]
                ok, written_rows = append_rows(rows)
                if ok and written_rows:
                    state = load_track_state()
                    for (evt,u), row_index in zip(staged, written_rows):
                        if evt.get("price") is not None:
                            state["items"].append(
                                schedule_tracker(u, evt["symbol"], evt["price"], evt["ts"], row_index)
                            )
                    save_track_state(state)
        except Exception as e:
            print("HL fetch error (--once):", e, flush=True)
        print("Done.")
    else:
        run_loop()

if __name__ == "__main__":
    main()
