#!/usr/bin/env python3
"""fetch_facts_figures.py – Generates data/facts_figures.json for the Facts & Figures dashboard.

Data sources:
  data/latest.json   Already-computed by fetch_data.py (stress, VIX components, Fed, recProb …)
  FRED API           All market/macro data. No Stooq, no external scrapers for primary paths.
  multpl.com         Shiller CAPE (scrape, optional – graceful fallback if blocked)

Run order: always AFTER fetch_data.py so latest.json is fresh.

QQQ proxy note:
  Stooq is unreliable from GitHub Actions IP ranges.
  We use FRED NASDAQCOM (NASDAQ Composite, daily) as a proxy for QQQ/Growth trend.
  The chart label is updated accordingly. MA50/MA200 logic is identical.

Wilshire / Buffett note:
  FRED series candidates tried in order: WILL5000PRFC, WILL5000IND, WILL5000INDFC.
  Divided by nominal GDP (FRED GDP series, quarterly, fill-forward) to get Buffett %.
"""

from __future__ import annotations
import csv, gzip, io, json, os, re, sys, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ─── Config ──────────────────────────────────────────────────────────────────
FRED_KEY = os.environ.get("FRED_API_KEY", "").strip()
if not FRED_KEY:
    print("ERROR: FRED_API_KEY not found.", file=sys.stderr)
    sys.exit(1)

NOW             = datetime.now(timezone.utc)
FRED_BASE       = "https://api.stlouisfed.org/fred/series/observations"
LATEST_IN       = Path("data/latest.json")
OUT             = Path("data/facts_figures.json")
OUT.parent.mkdir(parents=True, exist_ok=True)

DAILY_START     = (NOW - timedelta(days=365 * 3)).strftime("%Y-%m-%d")
QUARTERLY_START = (NOW - timedelta(days=365 * 20)).strftime("%Y-%m-%d")

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection":      "keep-alive",
    "Cache-Control":   "no-cache",
}


# ─── HTTP helpers ─────────────────────────────────────────────────────────────
def http_get(url: str, timeout: int = 20) -> bytes:
    req = urllib.request.Request(url, headers=BROWSER_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        enc = (r.headers.get("Content-Encoding") or "").lower()
        if enc == "gzip" or raw[:2] == b"\x1f\x8b":
            try:
                raw = gzip.decompress(raw)
            except Exception:
                pass
        return raw


def fred(series_id: str, observation_start: str | None = None) -> list[dict]:
    """Fetch FRED observations. Returns [] on any error."""
    params: dict = {
        "series_id":  series_id,
        "api_key":    FRED_KEY,
        "file_type":  "json",
        "sort_order": "asc",
    }
    if observation_start:
        params["observation_start"] = observation_start
    url = FRED_BASE + "?" + urllib.parse.urlencode(params)
    try:
        payload = json.loads(http_get(url).decode("utf-8"))
        obs = [
            o for o in payload.get("observations", [])
            if o.get("value") not in (None, "", ".")
        ]
        print(f"  ✓ FRED {series_id}: {len(obs)} obs")
        return obs
    except Exception as e:
        print(f"  ✗ FRED {series_id}: {e}", file=sys.stderr)
        return []


def fred_first(candidates: list[str], observation_start: str | None = None) -> list[dict]:
    """Try each series ID in order; return first non-empty result."""
    for sid in candidates:
        obs = fred(sid, observation_start)
        if obs:
            return obs
    return []


def hp(obs: list[dict], dec: int = 2) -> list[dict]:
    """Convert raw FRED obs to [{date, value}]."""
    out = []
    for o in obs:
        try:
            out.append({"date": o["date"], "value": round(float(o["value"]), dec)})
        except Exception:
            pass
    return out


def last_val(pts: list[dict], default: float = 0.0) -> float:
    return pts[-1]["value"] if pts else default


def yoy(pts: list[dict], lag: int = 4) -> list[dict]:
    """YoY % change; lag=4 for quarterly, lag=12 for monthly."""
    out = []
    for i in range(lag, len(pts)):
        prev = pts[i - lag]["value"]
        if prev == 0:
            continue
        out.append({
            "date":  pts[i]["date"],
            "value": round(((pts[i]["value"] / prev) - 1) * 100, 2),
        })
    return out


def ma(values: list[float], window: int) -> list[float | None]:
    out: list[float | None] = []
    for i in range(len(values)):
        if i < window - 1:
            out.append(None)
        else:
            out.append(round(sum(values[i - window + 1:i + 1]) / window, 2))
    return out


def rsi14(closes: list[float]) -> float | None:
    series = rsi14_series(closes, 14)
    vals = [v for v in series if v is not None]
    return vals[-1] if vals else None


def rsi14_series(closes: list[float], period: int = 14) -> list[float | None]:
    if len(closes) < period + 1:
        return [None] * len(closes)
    out: list[float | None] = [None] * len(closes)
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    if avg_loss == 0:
        out[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        out[period] = round(100 - 100 / (1 + rs), 1)

    for i in range(period + 1, len(closes)):
        gain = gains[i - 1]
        loss = losses[i - 1]
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
        if avg_loss == 0:
            out[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[i] = round(100 - 100 / (1 + rs), 1)
    return out


def stooq_history(symbol: str, years: int = 3) -> list[dict]:
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
    try:
        raw = http_get(url, timeout=18).decode("utf-8", errors="replace")
        rows = list(csv.DictReader(io.StringIO(raw)))
        cutoff = (NOW - timedelta(days=365 * years)).date()
        out = []
        for row in rows:
            ds = (row.get("Date") or row.get("date") or "").strip()
            close_s = (row.get("Close") or row.get("close") or "").strip()
            if not ds or not close_s:
                continue
            try:
                d = datetime.strptime(ds, "%Y-%m-%d").date()
                v = float(close_s)
            except Exception:
                continue
            if d < cutoff or v <= 0:
                continue
            out.append({"date": ds, "value": round(v, 2)})
        print(f"  ✓ STOOQ {symbol}: {len(out)} obs")
        return out
    except Exception as e:
        print(f"  ✗ STOOQ {symbol}: {e}", file=sys.stderr)
        return []


def scrape_cape_alt() -> float | None:
    targets = [
        ("https://www.currentmarketvaluation.com/models/s&p500-mean-reversion.php", [
            r'Shiller P/E Ratio[^\d]{0,80}([\d]{1,3}\.[\d]{1,2})',
            r'10-Year P/E Ratio[^\d]{0,80}([\d]{1,3}\.[\d]{1,2})',
        ]),
        ("https://www.currentmarketvaluation.com/models/price-earnings.php", [
            r'Shiller P/E Ratio[^\d]{0,80}([\d]{1,3}\.[\d]{1,2})',
            r'Current[^\d]{0,40}([\d]{1,3}\.[\d]{1,2})',
        ]),
    ]
    for url, patterns in targets:
        try:
            html = http_get(url, timeout=18).decode("utf-8", errors="replace")
            for pat in patterns:
                m = re.search(pat, html, flags=re.I | re.S)
                if m:
                    val = float(m.group(1))
                    if 5 < val < 100:
                        print(f"  ✓ CAPE alt scrape: {val}")
                        return val
        except Exception as e:
            print(f"  ✗ CAPE alt scrape ({url[-35:]}): {e}", file=sys.stderr)
    return None


def fallback_meta(label: str, reason: str, next_update_hours: int = 4) -> dict:
    return {
        "label": label,
        "reason": reason,
        "nextUpdate": f"Nächster regulärer Versuch voraussichtlich in rund {next_update_hours} Stunden.",
        "tooltip": f"Fallback aktiv: {label}. {reason} Für dich bedeutet das: Die Kennzahl bleibt sichtbar, basiert aber in diesem Lauf auf einer Ersatzquelle oder einem Ersatz-Proxy. Nächster regulärer Versuch voraussichtlich in rund {next_update_hours} Stunden.",
    }


# ─── Shiller CAPE scraper ─────────────────────────────────────────────────────
def scrape_cape() -> float | None:
    """
    Try to get current Shiller CAPE from multpl.com.
    Attempts the monthly table page first (simpler DOM), then the main page.
    Returns None if blocked or parse fails.
    """
    targets = [
        (
            "https://www.multpl.com/shiller-pe/table/by-month",
            [
                r'<td>[A-Z][a-z]+\s+\d{4}</td>\s*<td>\s*([\d]{1,3}\.[\d]{1,2})\s*</td>',
                r'<td>\s*([\d]{1,3}\.[\d]{1,2})\s*</td>',
            ],
        ),
        (
            "https://www.multpl.com/shiller-pe",
            [
                r'id=["\']current["\'][^>]*>\s*<[^>]+>\s*([\d]{1,3}\.[\d]{1,2})',
                r'id=["\']current["\'][^>]*>([\d]{1,3}\.[\d]{1,2})',
                r'"shillerPE"\s*:\s*([\d]{1,3}\.[\d]{1,2})',
            ],
        ),
    ]

    for url, patterns in targets:
        try:
            raw = http_get(url, timeout=18)
            if raw[:2] == b'\x1f\x8b':         # handle gzip
                import gzip
                raw = gzip.decompress(raw)
            html = raw.decode("utf-8", errors="replace")
            for pat in patterns:
                m = re.search(pat, html)
                if m:
                    val = float(m.group(1))
                    if 5 < val < 100:
                        print(f"  ✓ CAPE scrape: {val}")
                        return val
        except Exception as e:
            print(f"  ✗ CAPE scrape ({url[-35:]}): {e}", file=sys.stderr)

    print("  ✗ CAPE: all scrape attempts failed", file=sys.stderr)
    return None


# ─── Buffett Indicator ────────────────────────────────────────────────────────
def buffett_indicator(
    wilshire_pts: list[dict],
    gdp_pts:      list[dict],
) -> tuple[float | None, list[dict]]:
    """
    Buffett % = Wilshire 5000 Full-Cap Index / Nominal GDP (billions) x 100.

    The Wilshire 5000 Full-Cap index was originally scaled so 1 point ≈ $1 billion
    in total US market cap. GDP is in billions (FRED series GDP, quarterly).
    Cross-check: Wilshire ~48 000 / GDP ~28 800 × 100 ≈ 167 % – matches published sources.
    """
    if not wilshire_pts or not gdp_pts:
        return None, []

    sorted_gdp = sorted(gdp_pts, key=lambda x: x["date"])

    def gdp_at(date: str) -> float | None:
        last = None
        for g in sorted_gdp:
            if g["date"] <= date:
                last = g["value"]
            else:
                break
        return last

    history: list[dict] = []
    for p in wilshire_pts:
        g = gdp_at(p["date"])
        if g and g > 0:
            history.append({"date": p["date"], "value": round(p["value"] / g * 100, 1)})

    latest = history[-1]["value"] if history else None
    return latest, history[-120:]


# ─── Classification functions ─────────────────────────────────────────────────
def cape_classify(v: float | None) -> tuple[str, str, str]:
    if v is None:
        return ("m", "—",
                "Shiller CAPE temporär nicht verfügbar. Primärquelle nicht erreichbar – wird beim nächsten Update erneut versucht.")
    if v < 15:
        return ("l", "Historisch günstig",
                f"Shiller CAPE bei {v:.1f}. Seltene Bewertungsgelegenheit.")
    if v < 22:
        return ("l", "Moderat",
                f"Shiller CAPE bei {v:.1f}. Im fairen historischen Bereich.")
    if v < 27:
        return ("m", "Leicht erhöht",
                f"Shiller CAPE bei {v:.1f}. Leicht über dem langfristigen Mittel. Puffer nimmt ab.")
    if v < 32:
        return ("m", "Erhöht",
                f"Shiller CAPE bei {v:.1f}. Erhöht. Geringere Pufferkapazität bei negativen Schocks.")
    return ("h", "Stark erhöht",
            f"Shiller CAPE bei {v:.1f}. Historisch ambitionierte Bewertung. "
            "Erhöhte Empfindlichkeit gegenüber Enttäuschungen.")


def buffett_classify(v: float | None) -> tuple[str, str]:
    if v is None:
        return ("m",
                "Buffett-Indikator temporär nicht verfügbar. Marktwert- oder GDP-Daten konnten nicht abgerufen werden.")
    if v < 75:
        return ("l", f"Buffett-Indikator bei {v:.0f} %. Historisch günstig – "
                     "Marktkapitalisierung klar unter Wirtschaftsleistung.")
    if v < 100:
        return ("l", f"Buffett-Indikator bei {v:.0f} %. Im fairen Bewertungsbereich.")
    if v < 130:
        return ("m", f"Buffett-Indikator bei {v:.0f} %. "
                     "Leicht über dem historischen Mittelwert (~85–100 %).")
    if v < 160:
        return ("m", f"Buffett-Indikator bei {v:.0f} %. Klar erhöht. "
                     "Das erhöht die Empfindlichkeit gegenüber externen Schocks.")
    return ("h", f"Buffett-Indikator bei {v:.0f} %. "
                 "Historisch stark überhitzt. Marktkapitalisierung weit über BIP.")


def earnings_classify(v: float | None) -> tuple[str, str]:
    if v is None:
        return ("m", "Keine Gewinnwachstumsdaten verfügbar.")
    if v >= 10:
        return ("l", f"S&P-500-Gewinnwachstum YoY: +{v:.1f} %. "
                     "Starkes Wachstum stützt aktuelle Bewertungen.")
    if v >= 4:
        return ("l", f"S&P-500-Gewinnwachstum YoY: +{v:.1f} %. Solides Gewinnwachstum.")
    if v >= 0:
        return ("m", f"S&P-500-Gewinnwachstum YoY: +{v:.1f} %. "
                     "Moderates Wachstum – Bewertungen weniger gut abgesichert.")
    return ("h", f"S&P-500-Gewinnwachstum YoY: {v:.1f} %. "
                 "Gewinnrückgang erhöht Druck auf Bewertungsniveaus.")


def fed_classify(v: float) -> tuple[str, str]:
    if v >= 5.0:
        return ("h", f"US-Leitzins bei {v:.2f} %. "
                     "Klar restriktives Umfeld. Kreditkosten und Bewertungsdruck erhöht.")
    if v >= 4.0:
        return ("m", f"US-Leitzins bei {v:.2f} %. Komplizierter geworden. "
                     "Höhere Energiepreise erschweren eine lockere Zinsfantasie.")
    if v >= 2.5:
        return ("m", f"US-Leitzins bei {v:.2f} %. Neutral bis leicht restriktiv.")
    return ("l", f"US-Leitzins bei {v:.2f} %. Lockeres Zinsumfeld.")


def gdp_classify(v: float) -> tuple[str, str]:
    if v >= 3.0:
        return ("l", f"BIP-Wachstum (real, ann.) zuletzt +{v:.1f} %. "
                     "Starkes konjunkturelles Fundament.")
    if v >= 1.5:
        return ("l", f"BIP-Wachstum (real, ann.) zuletzt +{v:.1f} %. "
                     "Das Wachstumsrisiko hat zugenommen.")
    if v >= 0:
        return ("m", f"BIP-Wachstum (real, ann.) zuletzt +{v:.1f} %. "
                     "Das Wachstumsrisiko hat zugenommen.")
    return ("h", f"BIP-Wachstum (real, ann.) zuletzt {v:.1f} %. "
                 "Negatives Wachstum – Rezessionsrisiko erhöht.")


def rec_classify(prob: float, sahm: float) -> tuple[str, str]:
    if prob >= 50 or sahm >= 0.5:
        return ("h", f"Rezessionswahrscheinlichkeit {prob:.1f} % (FRED), "
                     f"Sahm Rule {sahm:.2f}. Klare Warnsignale aktiv.")
    if prob >= 25 or sahm >= 0.3:
        return ("m", f"Noch kein bestätigter Crash-Makro-Modus, aber klar fragiler "
                     f"als in ruhigeren Marktphasen. Modell bei {prob:.1f} %.")
    return ("l", f"Noch kein bestätigter Crash-Makro-Modus, aber klar fragiler "
                 f"als in ruhigeren Marktphasen. Modell bei {prob:.1f} %.")


def vix_classify(v: float) -> tuple[str, str, str]:
    if v <= 0:
        return ("m", "—", "Keine VIX-Daten.")
    if v >= 40:
        return ("h", "Extremes Stressniveau", f"VIX bei {v:.1f}. Historisch seltene Panikphase.")
    if v >= 30:
        return ("h", "Klarer Stress",         f"VIX bei {v:.1f}. Deutlich erhöhtes Absicherungsbedürfnis.")
    if v >= 20:
        return ("m", "Erhöhte Nervosität",    f"VIX bei {v:.1f}. Merklich über dem langjährigen Durchschnitt.")
    return     ("l", "Relativ ruhig",         f"VIX bei {v:.1f}. Unter dem langfristigen Durchschnitt von ~17–20.")


def phase_classify(stress: float) -> tuple[str, str, str]:
    if stress >= 60:
        return ("h", "Hohes Stressniveau",
                "Mehrere Signale bestätigen gleichzeitig eine anspruchsvolle Marktphase.")
    if stress >= 45:
        return ("m", "Defensivere Haltung sinnvoll", "Belastungssignale sind breiter sichtbar.")
    if stress >= 30:
        return ("m", "Erhöhte Vorsicht", "Mehr Spannungen als in ruhigeren Phasen.")
    return     ("l", "Normalphase",      "Signallage wirkt geordnet.")


def trend_classify(
    close: float | None, m50: float | None, m200: float | None, label: str = "Index"
) -> tuple[str, str, str]:
    if None in (close, m50, m200):
        return ("m", "Keine Daten", "Kursdaten nicht verfügbar.")
    if close > m50 > m200:
        return ("l", "Aufwärtstrend intakt",
                f"{label} über 50T ({m50:.0f}) und 200T ({m200:.0f}). Trendstruktur intakt.")
    if close < m200:
        return ("h", "Unter 200T-Durchschnitt",
                f"{label} unter 200T-Durchschnitt ({m200:.0f}). Längerfristiger Trendbruch.")
    if close < m50:
        return ("m", "Unter 50T-Durchschnitt",
                f"{label} unter 50T ({m50:.0f}), aber noch über 200T ({m200:.0f}).")
    return ("m", "Gemischtes Bild", f"{label} nahe 50T ({m50:.0f}). Kein klares Trendurteil.")


def rsi_classify(v: float | None) -> tuple[str, str, str]:
    if v is None:
        return ("m", "—", "RSI nicht berechenbar.")
    if v < 25:
        return ("l", "Stark überverkauft",
                f"RSI (14) bei {v:.1f}. Historisch oft im Bereich panischer Überreaktionen.")
    if v < 35:
        return ("l", "Überverkauft",
                f"RSI (14) bei {v:.1f}. Überverkauftes Niveau – Gegenbewegung möglich.")
    if v > 75:
        return ("h", "Überkauft", f"RSI (14) bei {v:.1f}. Kurzfristige Erschöpfung möglich.")
    return     ("m", "Neutral",   f"RSI (14) bei {v:.1f}. Kein Extremniveau in beide Richtungen.")


def drawdown_classify(dd: float | None) -> tuple[str, str, str]:
    if dd is None:
        return ("m", "—", "Drawdown nicht berechenbar.")
    if dd >= -5:
        return ("l", "Nahe Allzeithoch",    f"{dd:.1f} % vom Hoch. Kaum struktureller Druck.")
    if dd >= -10:
        return ("m", "Leichter Rückgang",   f"{dd:.1f} % vom Hoch. Normales Korrekturterritory.")
    if dd >= -20:
        return ("h", "Erhöhter Drawdown",   f"{dd:.1f} % vom Hoch. Spürbare Korrektur.")
    return     ("h", "Bärenmarkt-Niveau",   f"{dd:.1f} % vom Hoch. Bärenmarkt-Schwelle überschritten.")


# ─── Probability models ───────────────────────────────────────────────────────
def bottom_prob(
    vix: float, dd: float | None, rsi: float | None, rec: float, sahm: float
) -> tuple[int, str, str]:
    score, notes = 0, []
    if vix >= 30:   score += 25; notes.append(f"VIX {vix:.1f}")
    elif vix >= 22: score += 12
    if dd is not None:
        if dd <= -20: score += 25; notes.append(f"Drawdown {dd:.1f} %")
        elif dd <= -12: score += 15
    if rsi is not None:
        if rsi <= 30: score += 20; notes.append(f"RSI {rsi:.1f}")
        elif rsi <= 40: score += 10
    if rec < 25:  score += 15; notes.append("Rezessionsrisiko begrenzt")
    if sahm < 0.3: score += 15; notes.append("Sahm Rule unter Warnschwelle")
    score = min(score, 85)
    reason = "; ".join(notes[:3]) if notes else "Keine starken Bodenbildungssignale aktiv."
    interp = ("Signalkonstellation weist auf mögliche Erholungsphase hin."
              if score >= 40 else "Noch keine klare Bodenbildungskonstellation erkennbar.")
    return score, reason, interp


def crash_prob(
    stress: float, rec: float, vix: float, dd: float | None, gdp_g: float
) -> tuple[int, str, str]:
    score = 0
    if stress >= 60: score += 28
    elif stress >= 45: score += 18
    elif stress >= 30: score += 8
    if rec >= 40: score += 22
    elif rec >= 25: score += 14
    if vix >= 35: score += 16
    elif vix >= 25: score += 9
    if dd is not None and dd <= -15: score += 14
    if gdp_g < 0: score += 20
    elif gdp_g < 1.0: score += 8
    score = min(score, 90)
    reason = f"Stress {stress:.0f}/100, Rezessionsrisiko {rec:.1f} %, VIX {vix:.1f}."
    interp = ("Crash-Risiko erhöht – mehrere Stressachsen aktiv." if score >= 50
              else "Moderates Crash-Risiko – Situation beobachtungswürdig." if score >= 30
              else "Crash-Risiko derzeit begrenzt.")
    return score, reason, interp


def timing_qual(bp: int, cp: int) -> tuple[str, str, str]:
    too_early = min(round(bp * 0.55), 55)
    optimal   = min(round(bp * 0.28), 28)
    too_late  = 100 - too_early - optimal
    label  = f"Zu früh {too_early} % · Optimal {optimal} % · Zu spät {too_late} %"
    reason = f"Bottom-Signal {bp} %, Crash-Risiko {cp} %."
    interp = ("Kein klares Timing-Fenster erkennbar." if bp < 35
              else "Leichtes Timing-Signal – noch nicht breit bestätigt.")
    return label, reason, interp


# ─── Sentiment builder ────────────────────────────────────────────────────────
def build_sentiment(
    vix: float, fed: float, rec: float, gdp_g: float,
    sp_ytd: float, dd: float | None,
) -> dict:
    tags: list[str]  = []
    risks: list[str] = []

    if vix >= 28:   tags.append("Risk-off");         risks.append(f"VIX {vix:.1f} – Märkte preisen erhöhte Unsicherheit ein.")
    elif vix >= 21: tags.append("Erhöhte Nervosität")
    else:           tags.append("Relativ ruhig")

    if fed >= 4.5:
        tags.append("Zinsdruck")
        risks.append(f"US-Leitzins {fed:.2f} % – restriktives Umfeld wirkt weiter.")
    if rec >= 25:
        tags.append("Rezessionsrisiko")
        risks.append(f"Rezessionswahrscheinlichkeit {rec:.1f} % über Vorsichtsschwelle.")
    if gdp_g < 1.0:
        risks.append(f"BIP-Wachstum {gdp_g:.1f} % – konjunkturelle Unterstützung begrenzt.")
    if sp_ytd <= -10:
        tags.append("Marktkorrektur")
        risks.append(f"S&P 500 YTD {sp_ytd:.1f} % – Korrekturdynamik sichtbar.")
    if dd is not None and dd <= -15:
        risks.append(f"NASDAQ Drawdown {dd:.1f} % vom Hoch – Druck auf Growth-Titel.")

    if not risks:
        risks.append("Kein übergreifendes Hochrisikosignal aktiv.")

    mood = ("Risk-off: Marktteilnehmer präferieren Sicherheit." if "Risk-off" in tags
            else "Nervöses Bild: Einzelne Stresssignale sichtbar, aber keine vollständige Eskalation."
            if "Erhöhte Nervosität" in tags
            else "Verhalten konstruktiv, aber mit erhöhter Sensitivität gegenüber negativen Datenpunkten.")

    return {"marketMood": mood, "tags": tags, "risks": risks[:5]}


# ─── Static blocks ────────────────────────────────────────────────────────────
GEO = [
    {"field": "Russland / Ukraine",    "status": "Fortlaufender Konflikt",
     "impact": "Energiepreise, Getreide, Transportkorridore Schwarzes Meer."},
    {"field": "Naher Osten / Gaza",    "status": "Aktives Konfliktfeld",
     "impact": "Ölpreisrisiko, Hormuz-Passage, LNG-Routen, Gold als Safe Haven."},
    {"field": "Taiwan-Straße / China", "status": "Erhöhte Spannungen",
     "impact": "Halbleiterversorgung, Lieferketten, US-Dollar-Dynamik."},
    {"field": "Rotes Meer / Houthi",   "status": "Andauernde Störung",
     "impact": "Suez-Alternative, Frachtkosten, Lieferzeiten weltweit erhöht."},
]
POS_TRIGGERS = [
    "VIX fällt nachhaltig unter 20 (Beruhigungssignal).",
    "Fed signalisiert klare Zinspause oder Lockerungserwartungen.",
    "Rezessionsmodell stabilisiert sich unter 20 %.",
    "Marktbreite verbessert sich – mehr Titel über 200T-Durchschnitt.",
    "Gewinnrevisionen für S&P-500-Unternehmen drehen ins Positive.",
]
REFRESH_TRIGGERS = [
    "Fed-Entscheidung oder FOMC-Protokoll.",
    "Nächste Arbeitsmarktdaten (NFP, Sahm Rule).",
    "Quartals-BIP-Revision (FRED A191RL1Q225SBEA).",
    "Earnings-Season-Auftakt.",
    "Geopolitische Eskalation in bekannten Risikofeldern.",
]


# ─── Main ─────────────────────────────────────────────────────────────────────
def main() -> int:

    # ── 1. Base indicators from latest.json ───────────────────────────────────
    try:
        latest = json.loads(LATEST_IN.read_text(encoding="utf-8"))
        print(f"  ✓ Loaded {LATEST_IN}")
    except Exception as e:
        print(f"  ✗ Cannot read {LATEST_IN}: {e}", file=sys.stderr)
        latest = {}

    inds     = latest.get("indicators", {})
    fed_rate = float(inds.get("fedRate",      {}).get("value", 0))
    rec_prob = float(inds.get("recProb",      {}).get("value", 0))
    sahm     = float(inds.get("recProb",      {}).get("fastProxy", {}).get("value", 0))
    vix_now  = float(inds.get("tradeStress",  {}).get("components", {}).get("vix", 0))
    sp_ytd   = float(inds.get("sp500",        {}).get("ytd", 0))
    stress   = float(inds.get("tradeStress",  {}).get("value", 0))

    # ── 2. FRED fetches ───────────────────────────────────────────────────────
    print("\n[FRED – macro]")
    gdp_g_obs = hp(fred("A191RL1Q225SBEA", QUARTERLY_START), 1)   # real GDP growth
    gdp_g_val = last_val(gdp_g_obs)
    gdp_nom   = hp(fred("GDP", QUARTERLY_START), 0)               # nominal GDP (billions)
    cp_obs    = hp(fred("CP",  QUARTERLY_START), 1)               # corp profits after tax
    cp_yoy    = yoy(cp_obs, lag=4)
    earn_g    = cp_yoy[-1]["value"] if cp_yoy else None
    effr_obs  = hp(fred("EFFR",        DAILY_START), 2)
    vix_obs   = hp(fred("VIXCLS",      DAILY_START), 2)
    brent_obs = hp(fred("DCOILBRENTEU", DAILY_START), 2)

    # Growth proxy – prefer Stooq QQQ, fall back to FRED NASDAQ Composite
    print("\n[Growth proxy]")
    growth_fallback = None
    chart_label = "QQQ"
    chart_sub_label = "QQQ"
    nasdaq_raw = stooq_history("qqq.us", years=3)
    if not nasdaq_raw:
        print("\n[FRED – NASDAQ Composite as Growth fallback]")
        nasdaq_raw = hp(fred("NASDAQCOM", DAILY_START), 0)
        chart_label = "NASDAQ Composite"
        chart_sub_label = "NASDAQ Composite (Fallback)"
        if nasdaq_raw:
            growth_fallback = fallback_meta("Fallback: NASDAQ Composite", "QQQ-Kursdaten waren in diesem Lauf nicht zuverlässig erreichbar. Deshalb wird als Ersatz der NASDAQ Composite aus FRED verwendet.")
    nasdaq_vals = [p["value"] for p in nasdaq_raw]

    # Wilshire 5000 / market cap – try classic Wilshire IDs, then Fed market cap fallback
    print("\n[FRED – Wilshire 5000 for Buffett indicator]")
    buffett_fallback = None
    will_raw = hp(fred_first(["WILL5000PRFC", "WILL5000IND", "WILL5000INDFC", "WILL5000PR"], DAILY_START), 0)
    if not will_raw:
        will_raw = hp(fred("BOGZ1LM883164115Q", QUARTERLY_START), 0)
        if will_raw:
            buffett_fallback = fallback_meta("Fallback: Fed-Marktwertreihe", "Die üblichen Wilshire-5000-Reihen waren in diesem Lauf nicht verfügbar. Deshalb wird eine offizielle Fed-/FRED-Marktwertreihe für US-Aktien als Ersatz verwendet.")

    # ── 3. Shiller CAPE (scrape) ──────────────────────────────────────────────
    print("\n[CAPE scrape]")
    cape_val = scrape_cape()
    cape_fallback = None
    if cape_val is None:
        cape_val = scrape_cape_alt()
        if cape_val is not None:
            cape_fallback = fallback_meta("Fallback: Current Market Valuation", "Die Primärquelle multpl.com war in diesem Lauf nicht erreichbar. Deshalb wird eine öffentlich zugängliche Ersatzquelle genutzt.")

    # ── 4. Buffett Indicator ──────────────────────────────────────────────────
    print("\n[Buffett]")
    buffett_val, buffett_hist = buffett_indicator(will_raw, gdp_nom)

    # ── 5. Technical indicators from Growth proxy ─────────────────────────────
    ma50_vals  = ma(nasdaq_vals, 50)
    ma200_vals = ma(nasdaq_vals, 200)
    rsi_vals   = rsi14_series(nasdaq_vals, 14)

    idx_close = nasdaq_vals[-1] if nasdaq_vals else None
    ma50_cur  = ma50_vals[-1] if ma50_vals else None
    ma200_cur = ma200_vals[-1] if ma200_vals else None
    idx_ath   = max(nasdaq_vals) if nasdaq_vals else None
    dd_val    = (round((idx_close / idx_ath - 1) * 100, 1) if idx_close and idx_ath else None)
    rsi_val   = rsi14(nasdaq_vals)
    spread_cur = round(((ma50_cur / ma200_cur) - 1) * 100, 1) if (ma50_cur not in (None, 0) and ma200_cur not in (None, 0)) else None

    nasdaq_chart = [{
            "date": nasdaq_raw[i]["date"],
            "close": nasdaq_raw[i]["value"],
            "ma50": ma50_vals[i],
            "ma200": ma200_vals[i],
            "spreadPct": round(((ma50_vals[i] / ma200_vals[i]) - 1) * 100, 2) if (ma50_vals[i] not in (None, 0) and ma200_vals[i] not in (None, 0)) else None,
        } for i in range(len(nasdaq_raw))]
    rsi_chart = [{"date": nasdaq_raw[i]["date"], "value": rsi_vals[i]} for i in range(len(nasdaq_raw)) if rsi_vals[i] is not None]

    # ── 6. Classify ───────────────────────────────────────────────────────────

    cape_tone,  _,         cape_status  = cape_classify(cape_val)
    buff_tone,  buff_status             = buffett_classify(buffett_val)
    earn_tone,  earn_status             = earnings_classify(earn_g)
    fed_tone,   fed_status              = fed_classify(fed_rate)
    gdp_tone,   gdp_status              = gdp_classify(gdp_g_val)
    rec_tone,   rec_status              = rec_classify(rec_prob, sahm)
    vix_tone,   vix_title,  vix_note    = vix_classify(vix_now)
    ph_tone,    ph_title,   ph_status   = phase_classify(stress)
    tr_tone,    tr_title,   tr_note     = trend_classify(idx_close, ma50_cur, ma200_cur, chart_sub_label)
    rsi_tone,   rsi_title,  rsi_note    = rsi_classify(rsi_val)
    dd_tone,    dd_title,   dd_note     = drawdown_classify(dd_val)

    bp_score, bp_reason, bp_interp  = bottom_prob(vix_now, dd_val, rsi_val, rec_prob, sahm)
    cp_score, cp_reason, cp_interp  = crash_prob(stress, rec_prob, vix_now, dd_val, gdp_g_val)
    tq_label, tq_reason, tq_interp  = timing_qual(bp_score, cp_score)

    breadth_proxy = int(max(0, min(100, 100 - vix_now * 2.2)))
    om_tone   = ("h" if stress >= 60 or rec_prob >= 40
                 else "m" if stress >= 40 or rec_prob >= 25
                 else "l")
    om_title  = {"h": "Defensiv", "m": "Erhöhte Aufmerksamkeit", "l": "Geordnet"}[om_tone]
    om_status = {
        "h": "Mehrere Signalachsen unter Druck gleichzeitig.",
        "m": "Signale verschlechtern sich, aber noch keine breite Eskalation.",
        "l": "Kein übergreifendes Stresssignal aktiv.",
    }[om_tone]

    sentiment = build_sentiment(vix_now, fed_rate, rec_prob, gdp_g_val, sp_ytd, dd_val)

    # ── 7. Assemble output ────────────────────────────────────────────────────
    def spark(pts: list[dict], n: int = 60) -> list[dict]:
        return [{"value": p["value"]} for p in pts[-n:]]

    output = {
        "generatedAt": NOW.isoformat(),
        "meta": {
            "schemaVersion": "1.0",
            "sourceSummary": ["latest.json", "FRED", "Stooq", "Multpl", "Current Market Valuation"],
        },
        "marketStatus": {
            "phase": {
                "title":  ph_title,
                "value":  round(stress, 1),
                "unit":   "",
                "tone":   ph_tone,
                "status": ph_status,
            },
            "vix": {
                "title":  vix_title,
                "value":  round(vix_now, 1) if vix_now else None,
                "unit":   "",
                "tone":   vix_tone,
                "status": vix_note,
            },
            "breadth": {
                "title":  f"{breadth_proxy} % über 200T-Ø (Proxy)",
                "value":  breadth_proxy,
                "unit":   "%",
                "tone":   "l" if breadth_proxy >= 60 else "m" if breadth_proxy >= 40 else "h",
                "status": "Näherungswert abgeleitet aus VIX-Niveau. Direktdaten nicht via FRED verfügbar.",
            },
        },
        "overallMode": {
            "title":  om_title,
            "value":  None,
            "unit":   "",
            "tone":   om_tone,
            "status": om_status,
        },
        "technicalTriggers": [
            {
                "label":   "50T vs. 200T",
                "value":   spread_cur,
                "unit":    "%",
                "tone":    tr_tone,
                "status":  (f"50T liegt bei {spread_cur:+.1f} % gegenüber dem 200T. Schlusskurs {idx_close:.0f}, 50T {ma50_cur:.0f}, 200T {ma200_cur:.0f}. " + tr_note) if None not in (spread_cur, idx_close, ma50_cur, ma200_cur) else tr_note,
                "history": [{"value": p.get("spreadPct")} for p in nasdaq_chart[-90:] if p.get("spreadPct") is not None],
                "fallback": growth_fallback,
            },
            {
                "label":   "RSI (14)",
                "value":   rsi_val,
                "unit":    "",
                "tone":    ("h" if rsi_val is not None and rsi_val >= 70 else "l" if rsi_val is not None and rsi_val <= 30 else rsi_tone),
                "status":  rsi_note,
                "history": [{"value": p["value"]} for p in rsi_chart[-90:]],
                "fallback": growth_fallback,
            },
            {
                "label":   "Drawdown vom Hoch",
                "value":   dd_val,
                "unit":    "%",
                "tone":    ("h" if dd_val is not None and dd_val < 0 else "l"),
                "status":  dd_note,
                "history": [],
                "fallback": growth_fallback,
            },
        ],
        "valuation": [
            {
                "label":   "Shiller CAPE",
                "value":   round(cape_val, 1) if cape_val is not None else None,
                "unit":    "",
                "tone":    cape_tone,
                "status":  cape_status,
                "history": [],
                "fallback": cape_fallback,
            },
            {
                "label":   "Buffett-Indikator",
                "value":   int(round(buffett_val, 0)) if buffett_val is not None else None,
                "unit":    "%",
                "tone":    buff_tone,
                "status":  buff_status,
                "history": spark(buffett_hist, 60),
                "fallback": buffett_fallback,
            },
            {
                "label":   "Gewinnwachstum",
                "value":   round(earn_g, 1) if earn_g is not None else None,
                "unit":    "%",
                "tone":    earn_tone,
                "status":  earn_status,
                "history": spark(cp_yoy, 20),
            },
        ],
        "macro": [
            {
                "label":   "Zinsumfeld",
                "value":   round(fed_rate, 2),
                "unit":    "%",
                "tone":    fed_tone,
                "status":  fed_status,
                "history": spark(effr_obs, 60),
            },
            {
                "label":   "Wachstum",
                "value":   round(gdp_g_val, 1),
                "unit":    "%",
                "tone":    gdp_tone,
                "status":  gdp_status,
                "history": spark(gdp_g_obs, 20),
            },
            {
                "label":   "Rezessionsindikatoren",
                "value":   round(rec_prob, 1),
                "unit":    "%",
                "tone":    rec_tone,
                "status":  rec_status,
                "history": [],
            },
        ],
        "sentiment": sentiment,
        "geopolitics": GEO,
        "marketBottomProbability": {
            "title":          "Market Bottom Probability",
            "score":          bp_score,
            "scoreLabel":     f"{bp_score} %",
            "reason":         bp_reason,
            "interpretation": bp_interp,
        },
        "crashProbability": {
            "title":          "Crash Probability",
            "score":          cp_score,
            "scoreLabel":     f"{cp_score} %",
            "reason":         cp_reason,
            "interpretation": cp_interp,
        },
        "timingQuality": {
            "title":          "Timing-Qualität",
            "score":          0,
            "scoreLabel":     tq_label,
            "reason":         tq_reason,
            "interpretation": tq_interp,
        },
        "updateTriggers": {
            "positive": POS_TRIGGERS,
            "refresh":  REFRESH_TRIGGERS,
        },
        "timeVsTiming": {
            "timeInMarket": (
                "Langfristige Anlagepläne profitieren historisch von Korrekturen, "
                "wenn sie diszipliniert durchgehalten werden. Wer raus ist, "
                "verpasst die meisten Erholungstage."
            ),
            "marketTiming": (
                f"Das aktuelle Umfeld zeigt erhöhte Spannungen. Stress-Score {stress:.0f}/100. "
                "Wer neue Positionen aufbaut, profitiert von gestaffeltem Einstieg."
            ),
            "summary": (
                "Die Trennung zwischen langfristiger Logik und kurzfristiger Marktlesart "
                "bleibt der wichtigste Hebel gegen emotionale Fehlentscheidungen."
            ),
        },
        "charts": {
            "chartLabel": chart_label,
            "growthFallback": growth_fallback,
            "qqq":   nasdaq_chart[-300:],
            "rsi":   rsi_chart[-300:],
            "vix":   [{"date": p["date"], "value": p["value"]} for p in vix_obs[-300:]],
            "brent": [{"date": p["date"], "value": p["value"]} for p in brent_obs[-300:]],
        },
        "news": latest.get("news", []),
    }

    OUT.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n✓ Wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
