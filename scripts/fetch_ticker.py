#!/usr/bin/env python3
"""
fetch_ticker.py  –  Leichtgewichtiger Ticker-Updater
Schreibt nur data/ticker.json mit aktuellen Kursdaten.
Laufzeit: ~3–8 Sekunden. Wird alle 15 Minuten von GitHub Actions ausgeführt.
"""
from __future__ import annotations
import csv, io, json, os, urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT  = ROOT / "data" / "ticker.json"
NOW  = datetime.now(timezone.utc)


def http_get(url: str, timeout: int = 12) -> bytes:
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 (compatible; MarktrisikoKompassTicker/1.0)"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def stooq_quote(symbol: str) -> dict | None:
    """Holt Tages-OHLCV via Stooq CSV und liefert {close, open, chgPct}."""
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
    try:
        raw = http_get(url).decode("utf-8", errors="ignore")
        rows = list(csv.DictReader(io.StringIO(raw)))
        if len(rows) < 2:
            return None
        last = rows[-1]
        prev = rows[-2]
        close = float(last.get("Close") or last.get("close", 0))
        prev_close = float(prev.get("Close") or prev.get("close", 0))
        if close <= 0:
            return None
        chg_pct = ((close / prev_close) - 1) * 100 if prev_close else 0
        return {"close": close, "prev": prev_close, "chgPct": round(chg_pct, 2), "date": last.get("Date", "")}
    except Exception:
        return None


SYMBOLS = [
    # symbol,        label,            icon, unit, round_digits
    ("spy.us",       "S&P 500",        "🇺🇸", "$",  2),
    ("qqq.us",       "Nasdaq 100",     "💻", "$",  2),
    ("^dax",         "DAX",            "🇩🇪", "Pkt", 0),
    ("^stoxx50e",    "EuroStoxx 50",   "🇪🇺", "Pkt", 0),
    ("^n225",        "Nikkei 225",     "🇯🇵", "Pkt", 0),
    ("eurusd.fx",    "EUR/USD",        "💱", "",   4),
    ("usdjpy.fx",    "USD/JPY",        "💱", "",   2),
    ("cl.f",         "Öl (Brent WTI)", "🛢️", "$",  2),
    ("gc.f",         "Gold",           "🥇", "$",  1),
    ("btc.v",        "Bitcoin",        "₿",  "$",  0),
]


def main():
    ticker = []
    for sym, label, icon, unit, digits in SYMBOLS:
        q = stooq_quote(sym)
        if q:
            ticker.append({
                "label": label,
                "icon":  icon,
                "val":   round(q["close"], digits) if digits else int(q["close"]),
                "unit":  unit,
                "chgPct": q["chgPct"],
                "date":  q["date"],
            })

    payload = {
        "fetchedAt": NOW.isoformat(),
        "source":    "Stooq",
        "cadence":   "15min",
        "ticker":    ticker,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {OUT}  ({len(ticker)} items)")


if __name__ == "__main__":
    main()
