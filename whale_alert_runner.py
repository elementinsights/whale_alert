#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Whale Alerts Runner (Telegram + Google Sheets) — resilient version
- Multi-host fallback for CoinGlass (v4 -> legacy) and a retrying session.
- Per-coin thresholds, exchange+symbol LOB params, and post-alert price tracking.
- %Δ tracking is baselined off the LIVE mark price at alert time (not the whale's entry).
- Telegram and Sheets keep BOTH values: Entry vs Mark@.

Live price sources (in order): Binance (USDT), Coinbase (USD).

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
  HL_ENABLED             default: true
  LOB_ENABLED            default: true
  MIN_NOTIONAL_USD       default: 1000000
  EXCHANGES              default: "" (blank = all)
  DEDUPE_TTL_MIN         default: 180
  PER_REQUEST_PAUSE      default: 0.25

  # Optional base overrides:
  COINGLASS_BASE          default: https://open-api-v4.coinglass.com
  COINGLASS_FALLBACK_BASE default: https://open-api.coinglass.com

Optional per-coin threshold overrides:
  MIN_NOTIONAL_BTC, MIN_NOTIONAL_ETH, MIN_NOTIONAL_SOL, MIN_NOTIONAL_XRP, ...

Optional price-tracker tuning:
  PRICE_POLL_TIMEOUT_S   default: 8   (HTTP timeout for price calls)
  PRICE_COOLDOWN_S       default: 2   (sleep between successive price HTTP calls)
"""

import os, json, time, uuid, argparse, requests, datetime as dt
from collections import deque
from pathlib import Path
from dotenv import load_dotenv

# ---------- Startup housekeeping ----------
SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=SCRIPT_DIR / ".env", override=True)

# Delete price tracking state file at startup (fresh run)
TRACK_FILE = str(SCRIPT_DIR / "price_track_state.json")
if os.path.exists(TRACK_FILE):
    try:
        os.remove(TRACK_FILE)
        print(f"[info] Deleted old {Path(TRACK_FILE).name}")
    except Exception as e:
        print(f"[warn] Could not delete old {Path(TRACK_FILE).name}: {e}")

STATE_FILE = str(SCRIPT_DIR / "whale_alert_state.json")  # reserved for future use

# ---------- Config ----------
API_HOSTS = [
    os.getenv("COINGLASS_BASE", "https://open-api-v4.coinglass.com").rstrip("/"),
    os.getenv("COINGLASS_FALLBACK_BASE", "https://open-api.coinglass.com").rstrip("/"),
]

ENDPOINT_HL_ALERT = "/api/hyperliquid/whale-alert"
ENDPOINT_LOB_HIST = "/api/futures/orderbook/large-limit-order-history"

DEFAULT_WATCH = [
    c.strip().upper()
    for c in os.getenv("WATCH_COINS", "BTC,ETH,SOL,XRP,DOGE,LINK").split(",")
    if c.strip()
]
INTERVAL_S = int(os.getenv("INTERVAL_S", "60"))
HL_ENABLED = os.getenv("HL_ENABLED", "true").strip().lower() in ("1","true","yes","on")
LOB_ENABLED = os.getenv("LOB_ENABLED", "true").strip().lower() in ("1","true","yes","on")
EXCH_FILTER = {e.strip() for e in os.getenv("EXCHANGES", "").split(",") if e.strip()}
DEDUP_TTL_MIN = int(os.getenv("DEDUPE_TTL_MIN", "180"))
PER_REQUEST_PAUSE = float(os.getenv("PER_REQUEST_PAUSE", "0.25"))

PRICE_POLL_TIMEOUT_S = int(os.getenv("PRICE_POLL_TIMEOUT_S", "8"))
PRICE_COOLDOWN_S     = float(os.getenv("PRICE_COOLDOWN_S", "2"))

# ---------- Per-coin thresholds ----------
DEFAULT_MIN_NOTIONAL_USD = float(os.getenv("MIN_NOTIONAL_USD", "1000000"))
MIN_NOTIONAL_BY_SYMBOL = {
    "BTC": float(os.getenv("MIN_NOTIONAL_BTC", "100000000")),
    "ETH": float(os.getenv("MIN_NOTIONAL_ETH", "50000000")),
    "SOL": float(os.getenv("MIN_NOTIONAL_SOL", "50000000")),
    "XRP": float(os.getenv("MIN_NOTIONAL_XRP", "50000000")),
    "DOGE": float(os.getenv("MIN_NOTIONAL_DOGE", "20000000")),
    "LINK": float(os.getenv("MIN_NOTIONAL_LINK", "20000000")),
    "HYPE": float(os.getenv("MIN_NOTIONAL_HYPE", "20000000")),
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
def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def fmt_usd(x):
    try:
        x = float(x)
        return f"${x:,.0f}" if abs(x) >= 1000 else f"${x:,.4f}" if abs(x) < 1 else f"${x:,.2f}"
    except Exception:
        return str(x)

def uid() -> str:
    return uuid.uuid4().hex[:12]

def get_api_key() -> str:
    k = os.getenv("COINGLASS_API_KEY")
    if not k:
        raise RuntimeError("Missing COINGLASS_API_KEY in .env")
    return k

# ---------- HTTP session with retries + host fallback ----------
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
    """
    Try each host in API_HOSTS until one succeeds.
    Handles CoinGlass JSON envelope: {"code": "0", "data": ...}
    """
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
            last_err = RuntimeError(f"HTTP {r.status_code} / code={j.get('code')} / msg={j.get('msg')} / params={params}")
        except requests.exceptions.RequestException as e:
            last_err = e
            continue
    raise RuntimeError(f"All CoinGlass hosts failed for {path}: {last_err}")

# ---------- Telegram ----------
def send_telegram(text: str) -> bool:
    load_dotenv(dotenv_path=SCRIPT_DIR / ".env", override=False)
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

def telegram_lines(evt, utc_now):
    lines = ["🐳🐳🐳 <b>¡¡¡ALERTA BALLENA!!!</b> 🐳🐳🐳"]
    if evt.get("symbol"):
        lines.append(f"Coin: {evt['symbol']}")
    if evt.get("exchange"):
        lines.append(f"Exchange: {evt['exchange']}")
    if evt.get("address"):
        addr = evt["address"]
        short = addr[:6] + "..." + addr[-4:]
        lines.append(f"Address: <code>{short}</code>")

    lines.append(f"Action: {evt['action']}")
    lines.append(f"Notional: {fmt_usd(evt['notional'])} • Size: {evt['size']}")

    entry = evt.get("entry_price")
    mark  = evt.get("price")  # 'price' is Mark@ alert time
    if entry is not None or mark is not None:
        if entry is not None and mark is not None:
            lines.append(f"Entry: {fmt_usd(entry)} • Mark@: {fmt_usd(mark)}")
        elif mark is not None:
            lines.append(f"Mark@: {fmt_usd(mark)}")
        else:
            lines.append(f"Entry: {fmt_usd(entry)}")

    lines.append(f"UTC: {utc_now}")
    if evt.get("url"):
        lines.append(f"Transaction: {evt['url']}")
    if evt.get("note"):
        lines.append(f"Note: {evt['note']}")
    return "\n".join(lines)

# ---------- Google Sheets ----------
HEADER_ROW = 1

def build_tracker_minutes():
    mins = [1,2,3,4,5]
    mins += list(range(10, 31, 5))             # 10,15,20,25,30
    mins += list(range(45, 181, 15))           # 45..180 step 15
    mins += list(range(210, 721, 30))          # 210..720 step 30 (to 12h)
    mins += list(range(780, 1441, 60))         # 780..1440 step 60 (to 24h)
    return mins

TRACK_MINUTES = build_tracker_minutes()
TRACK_HEADERS = [
    f"%Δ {m}m" if m < 60 else (f"%Δ {m//60}h" if m % 60 == 0 else f"%Δ {m}m")
    for m in TRACK_MINUTES
]

# IMPORTANT: Price column is the Mark@ alert time; Entry is the whale's cost basis/fill.
BASE_HEADERS = ["Date","Time","Source","Exchange","Address","Symbol","Action",
                "Size","Price","Entry","Liq/Side","NotionalUSD","Note","URL"]
SHEET_HEADERS = BASE_HEADERS + TRACK_HEADERS + ["UID"]

TRACK_COL_OFFSET = len(BASE_HEADERS)

def _get_gspread_client():
    load_dotenv(dotenv_path=SCRIPT_DIR / ".env", override=False)
    creds_path = os.getenv("GOOGLE_SA_JSON")
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("Missing/invalid GOOGLE_SA_JSON")
    from google.oauth2.service_account import Credentials
    import gspread
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return gspread.authorize(creds)

def _get_sheet_handles():
    sheet_id = os.getenv("GOOGLE_SHEETS_ID")
    tab_name = os.getenv("GOOGLE_SHEETS_TAB", "Alerts")
    if not sheet_id:
        print("[warn] Sheets not configured: missing GOOGLE_SHEETS_ID", flush=True)
        return None, None, None
    try:
        gc = _get_gspread_client()
        sh = gc.open_by_key(sheet_id)
        try:
            ws = sh.worksheet(tab_name)
        except Exception:
            ws = sh.add_worksheet(title=tab_name, rows=8000, cols=max(60, len(SHEET_HEADERS)+4))
        return gc, sh, ws
    except Exception as e:
        print("[error] Sheets open failed:", e, flush=True)
        return None, None, None

def ensure_headers(ws):
    values = ws.get_values(f"A{HEADER_ROW}:ZZ{HEADER_ROW}")
    headers = (values[0] if values else [])
    if headers[:len(SHEET_HEADERS)] != SHEET_HEADERS:
        ws.update(range_name=f"A{HEADER_ROW}", values=[SHEET_HEADERS])

def append_rows(rows):
    gc, sh, ws = _get_sheet_handles()
    if not ws: return False
    try:
        ensure_headers(ws)
        if rows:
            ws.append_rows(rows, value_input_option="RAW")
        return True
    except Exception as e:
        print("[error] Sheets append failed:", e, flush=True)
        url = os.getenv("GSHEET_WEBHOOK_URL")
        if not url: return False
        try:
            r = requests.post(url, json={"rows": rows}, timeout=20)
            ok = (r.status_code == 200 and isinstance(r.json(), dict) and r.json().get("ok") is True)
            if not ok: print("[error] Webhook append failed:", r.text, flush=True)
            return ok
        except Exception as ee:
            print("[error] Webhook exception:", ee, flush=True)
            return False

def find_row_by_uid(uid_val):
    gc, sh, ws = _get_sheet_handles()
    if not ws: return None
    try:
        cell = ws.find(uid_val)
        return cell.row if cell else None
    except Exception:
        return None

def update_pct_cell(uid_val, minutes_from_start, pct_value):
    gc, sh, ws = _get_sheet_handles()
    if not ws: return False
    try:
        try:
            idx = TRACK_MINUTES.index(minutes_from_start)
        except ValueError:
            return False
        col_index = TRACK_COL_OFFSET + idx
        sheet_col = col_index + 1
        row = find_row_by_uid(uid_val)
        if not row: return False
        ws.update_cell(row, sheet_col, f"{pct_value:.2f}%")
        return True
    except Exception as e:
        print(f"[warn] update pct cell failed for UID={uid_val} @ {minutes_from_start}m: {e}", flush=True)
        return False

def to_sheet_row(evt, utc_dt, uid_val):
    return [
        utc_dt.strftime("%Y-%m-%d"),
        utc_dt.strftime("%H:%M:%S"),
        evt["source"],
        evt["exchange"],
        evt["address"],
        evt["symbol"],
        evt["action"],
        evt["size"],
        evt.get("price"),           # Mark@ at alert time
        evt.get("entry_price"),     # Whale's entry/fill
        evt["liq_or_side"],
        evt["notional"],
        evt["note"],
        evt["url"],
        *[""] * len(TRACK_HEADERS),
        uid_val
    ]

# ---------- Dedupe (in-memory TTL) ----------
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

# ---------- Helpers for LOB endpoint ----------
def default_futures_symbol(base: str, exchange: str) -> str:
    base = (base or "").upper()
    ex = (exchange or "").lower()
    if ex in ("binance","okx","bybit","bitget","kucoin","huobi"):
        return f"{base}USDT"
    if ex in ("coinbase","kraken","deribit"):
        return f"{base}USD"
    return f"{base}USDT"

# ---------- Core fetchers ----------
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
        ts_ms = int(e.get("create_time", 0) or 0)
        url = f"https://www.coinglass.com/hyperliquid/{e.get('user')}"

        out.append({
            "source": "Hyperliquid Whale Alert",
            "exchange": "Hyperliquid",
            "address": e.get("user",""),
            "symbol": sym,
            "action": f"{action} {side}",
            "size": size,
            "price": None,  # Mark@ will be fetched live
            "entry_price": float(e.get("entry_price", 0) or 0),
            "liq_or_side": f"Liq {float(e.get('liq_price', 0) or 0):,.2f}",
            "notional": notional,
            "note": "",
            "ts": ts_ms/1000 if ts_ms else time.time(),
            "url": url
        })
    return out

def fetch_large_orderbook_fills():
    exchanges = sorted(EXCH_FILTER) if EXCH_FILTER else [
        "Binance", "OKX", "Bybit", "Bitget", "Coinbase", "Kraken", "Deribit", "Huobi", "KuCoin"
    ]
    out_all = []
    for exch in exchanges:
        for base in DEFAULT_WATCH:
            symbol = default_futures_symbol(base, exch)
            params = {"exchange": exch, "symbol": symbol}
            try:
                data = http_get(ENDPOINT_LOB_HIST, params=params)
            except Exception as e:
                print(f"[warn] large-orderbook fetch failed for {exch} {symbol}: {e}", flush=True)
                continue

            for e in data or []:
                exch_name = e.get("exchange_name") or exch
                base_asset = str(e.get("base_asset", "")).upper()
                if DEFAULT_WATCH and base_asset not in DEFAULT_WATCH:
                    continue
                state = int(e.get("order_state", 0) or 0)
                if state != 2:  # 2 = filled
                    continue
                notional = float(e.get("start_usd_value", 0) or 0)
                if notional < min_notional_for(base_asset):
                    continue
                side = int(e.get("order_side", 0) or 0)
                side_lab = "Buy (bid filled)" if side == 2 else "Sell (ask filled)" if side == 1 else f"Side{side}"
                ts_ms = int(e.get("order_end_time") or e.get("current_time") or e.get("start_time") or 0)
                price = float(e.get("price", 0) or 0)
                size = float(e.get("start_quantity", 0) or 0)
                if price <= 0 and size:
                    price = notional / abs(size)

                out_all.append({
                    "source": "Large Orderbook (filled)",
                    "exchange": exch_name,
                    "address": "",
                    "symbol": base_asset,
                    "action": side_lab,
                    "size": size,
                    "price": None,                 # Mark@ fetched live
                    "entry_price": price,          # Treat fill price as entry
                    "liq_or_side": side_lab.split()[0],  # 'Buy' or 'Sell'
                    "notional": notional,
                    "note": e.get("symbol", ""),
                    "ts": ts_ms/1000 if ts_ms else time.time(),
                    "url": ""
                })
            time.sleep(PER_REQUEST_PAUSE)
    return out_all

# ---------- Price polling (Binance → Coinbase fallback) ----------
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
    return TRACK_MINUTES[:]  # already built

def schedule_tracker(uid_val, symbol, base_price, start_ts):
    return {
        "uid": uid_val,
        "symbol": symbol.upper(),
        "p0": float(base_price),
        "t0": float(start_ts),
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
            if now_ts - tr["t0"] >= m * 60 - 1:  # small tolerance
                p = fetch_price_now(tr["symbol"])
                if p:
                    pct = (p - tr["p0"]) / tr["p0"] * 100.0
                    ok = update_pct_cell(tr["uid"], m, pct)
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

# ---------- Main loop ----------
def run_loop():
    seen = SeenCache(ttl_min=DEDUP_TTL_MIN)
    print("=== Whale Alerts Runner (resilient) ===", flush=True)
    thr = effective_thresholds_for_watchlist()
    thr_str = ", ".join(f"{k}:{fmt_usd(v)}" for k,v in thr.items())
    print(f"API hosts: {', '.join(API_HOSTS)}", flush=True)
    print(f"Watchlist: {', '.join(DEFAULT_WATCH)}", flush=True)
    print(f"Per-coin thresholds: {thr_str} | fallback(default)={fmt_usd(DEFAULT_MIN_NOTIONAL_USD)}", flush=True)
    print(f"Interval={INTERVAL_S}s | enabled: HL={HL_ENABLED} LOB={LOB_ENABLED}", flush=True)
    print(f"Exchange filter: {', '.join(sorted(EXCH_FILTER)) if EXCH_FILTER else '(all defaults)'}", flush=True)
    print("Price tracking enabled: will fill %Δ columns up to 24h.\n", flush=True)

    # ensure header exists at startup
    _ = _get_sheet_handles()
    if _[2]:
        ensure_headers(_[2])

    while True:
        utc_dt = now_utc()
        utc_now = utc_dt.strftime("%Y-%m-%d %H:%M:%S")
        staged = []
        trackers_to_stage = []

        # Hyperliquid whale alerts
        try:
            if HL_ENABLED:
                for evt in fetch_hl_whale_alerts():
                    key = ("HL", evt["address"], evt["symbol"], evt["action"], round(evt["notional"]))
                    if not seen.seen(key):
                        # Fetch live mark price; store to event; build note
                        mark = fetch_price_now(evt["symbol"])
                        if mark is not None:
                            evt["price"] = mark  # Price column = Mark@
                        if evt.get("entry_price") is not None and mark is not None:
                            note = evt.get("note") or ""
                            if note: note += " | "
                            evt["note"] = note + f"Entry {fmt_usd(evt['entry_price'])} vs Mark {fmt_usd(mark)}"

                        send_telegram(telegram_lines(evt, utc_now))
                        u = uid()
                        staged.append(to_sheet_row(evt, utc_dt, u))

                        if mark is not None:
                            trackers_to_stage.append(schedule_tracker(u, evt["symbol"], mark, evt["ts"]))
                        seen.add(key)
        except Exception as e:
            print(f"{utc_now} | HL fetch error: {e}", flush=True)

        # Large orderbook filled orders
        try:
            if LOB_ENABLED:
                for evt in fetch_large_orderbook_fills():
                    key = ("LOB", evt["exchange"], evt["symbol"], evt["action"], round(evt["notional"]), int(evt["ts"]//60))
                    if not seen.seen(key):
                        mark = fetch_price_now(evt["symbol"])
                        if mark is not None:
                            evt["price"] = mark
                        if evt.get("entry_price") is not None and mark is not None:
                            note = evt.get("note") or ""
                            if note: note += " | "
                            evt["note"] = note + f"Entry {fmt_usd(evt['entry_price'])} vs Mark {fmt_usd(mark)}"

                        send_telegram(telegram_lines(evt, utc_now))
                        u = uid()
                        staged.append(to_sheet_row(evt, utc_dt, u))

                        if mark is not None:
                            trackers_to_stage.append(schedule_tracker(u, evt["symbol"], mark, evt["ts"]))
                        seen.add(key)
        except Exception as e:
            print(f"{utc_now} | LOB fetch error: {e}", flush=True)

        # Append to Sheets + persist trackers
        if staged:
            ok = append_rows(staged)
            if ok:
                st = load_track_state()
                st["items"].extend(trackers_to_stage)
                save_track_state(st)
                print(f"{utc_now} | appended {len(staged)} row(s) + staged {len(trackers_to_stage)} tracker(s)", flush=True)
            else:
                print(f"{utc_now} | sheet append failed (trackers not saved)", flush=True)

        # Process due trackers (fill %Δ columns)
        try:
            process_trackers()
        except Exception as e:
            print("[warn] process_trackers error:", e, flush=True)

        time.sleep(INTERVAL_S)

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Whale Alerts Runner (Telegram + Sheets + Price Tracking) — resilient")
    ap.add_argument("--once", action="store_true", help="Run one iteration then exit (for testing)")
    args = ap.parse_args()

    if args.once:
        utc_dt = now_utc()
        print("Effective thresholds:", effective_thresholds_for_watchlist())
        staged = []
        trackers_to_stage = []

        if HL_ENABLED:
            for evt in fetch_hl_whale_alerts():
                # Fetch live mark for baseline and logging
                mark = fetch_price_now(evt["symbol"])
                if mark is not None:
                    evt["price"] = mark
                    evt["note"] = (evt.get("note") or "")
                    if evt["note"]: evt["note"] += " | "
                    if evt.get("entry_price") is not None:
                        evt["note"] += f"Entry {fmt_usd(evt['entry_price'])} vs Mark {fmt_usd(mark)}"
                print(evt)
                u = uid()
                staged.append(to_sheet_row(evt, utc_dt, u))
                if mark is not None:
                    trackers_to_stage.append(schedule_tracker(u, evt["symbol"], mark, evt["ts"]))

        if LOB_ENABLED:
            for evt in fetch_large_orderbook_fills():
                mark = fetch_price_now(evt["symbol"])
                if mark is not None:
                    evt["price"] = mark
                    evt["note"] = (evt.get("note") or "")
                    if evt["note"]: evt["note"] += " | "
                    if evt.get("entry_price") is not None:
                        evt["note"] += f"Entry {fmt_usd(evt['entry_price'])} vs Mark {fmt_usd(mark)}"
                print(evt)
                u = uid()
                staged.append(to_sheet_row(evt, utc_dt, u))
                if mark is not None:
                    trackers_to_stage.append(schedule_tracker(u, evt["symbol"], mark, evt["ts"]))

        if staged:
            append_rows(staged)
            st = load_track_state()
            st["items"].extend(trackers_to_stage)
            save_track_state(st)
        print("Done.")
    else:
        run_loop()

if __name__ == "__main__":
    main()
