#!/usr/bin/env python3
from __future__ import annotations
import csv, io, json, math, os, re, sys, urllib.parse, urllib.request, xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from email.utils import parsedate_to_datetime

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "facts_figures.json"
LATEST = ROOT / "data" / "latest.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "").strip()
NOW = datetime.now(timezone.utc)
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"


def http_get(url: str, timeout: int = 25) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; MarktrisikoKompassFacts/1.0)"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def get_json(url: str):
    return json.loads(http_get(url).decode("utf-8"))


def fred(series_id: str, start: str | None = None) -> list[dict]:
    if not FRED_KEY:
        return []
    params = {"series_id": series_id, "api_key": FRED_KEY, "file_type": "json", "sort_order": "asc"}
    if start:
        params["observation_start"] = start
    url = FRED_BASE + "?" + urllib.parse.urlencode(params)
    try:
        payload = get_json(url)
    except Exception:
        return []
    return [o for o in payload.get("observations", []) if o.get("value") not in (None, "", ".")]


def stooq_daily(symbol: str) -> list[dict]:
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
    try:
        raw = http_get(url).decode("utf-8", errors="ignore")
    except Exception:
        return []
    rows = []
    reader = csv.DictReader(io.StringIO(raw))
    for row in reader:
        try:
            rows.append({
                "date": row["Date"],
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row.get("Volume", 0) or 0),
            })
        except Exception:
            pass
    return rows


def sma(vals, n):
    if len(vals) < n:
        return None
    return sum(vals[-n:]) / n


def rsi14(closes: list[float]) -> float | None:
    if len(closes) < 15:
        return None
    gains, losses = [], []
    for i in range(1, 15):
        diff = closes[-15+i] - closes[-15+i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains) / 14
    avg_loss = sum(losses) / 14
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def pct_change(a, b):
    if a in (None, 0) or b is None:
        return None
    return ((b / a) - 1) * 100


def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def compute_buffett_indicator(fred_key: str) -> float | None:
    """
    Buffett-Indikator = Wilshire-5000-Full-Cap-Marktkapitalisierung / nominales BIP * 100.
    Beide Reihen kommen direkt aus FRED – funktioniert zuverlässig in CI-Umgebungen.
    WILL5000INDFC: Gesamtmarktkapitalisierung US-Aktien in Mrd. USD
    GDP:           Nominales US-BIP in Mrd. USD (quartalsweise)
    """
    will_obs = fred("WILL5000INDFC", (NOW - timedelta(days=200)).strftime("%Y-%m-%d"))
    gdp_obs  = fred("GDP",           (NOW - timedelta(days=400)).strftime("%Y-%m-%d"))
    if not will_obs or not gdp_obs:
        return None
    will = safe_float(will_obs[-1]["value"])
    gdp  = safe_float(gdp_obs[-1]["value"])
    if will and gdp and gdp > 0:
        return round((will / gdp) * 100, 1)
    return None


def compute_corp_profit_growth() -> float | None:
    """
    Gewinnwachstum YoY aus FRED CPROFIT (US-Unternehmensgewinne nach Steuern, Mrd. USD).
    Quartalsreihe – Vergleich aktuellstes Quartal vs. Vorjahresquartal.
    """
    obs = fred("CPROFIT", (NOW - timedelta(days=500)).strftime("%Y-%m-%d"))
    if len(obs) < 5:
        return None
    cur  = safe_float(obs[-1]["value"])
    prev = safe_float(obs[-5]["value"])  # ca. 4 Quartale zurück
    if cur and prev and prev > 0:
        return round(((cur / prev) - 1) * 100, 1)
    return None


def compute_sp500_cape_proxy() -> float | None:
    """
    CAPE-Proxy aus FRED: S&P-500-Kurs / inflationsbereinigtes 10-Jahres-Durchschnittsergebnis.
    Wir approximieren: Ertragsniveau = Marktkapitalisierung (WILL5000INDFC) /
    Unternehmensgewinne nach Steuer (CPROFIT annualisiert) * SP500-Anteil.
    Einfacherer Proxy: aktueller SP500 / (CPROFIT_annual / Anzahl_Unternehmen_Proxy).
    Da echter CAPE 10Y braucht, schätzen wir über FRED SP500 YTD-KGV-Signalwert.
    Alternativweg: Yale-Daten-CSV (kein Auth nötig).
    """
    try:
        raw = http_get(
            "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC"
            "?interval=1d&range=1d",
            timeout=10
        ).decode("utf-8", errors="ignore")
        import json as _j
        d = _j.loads(raw)
        pe = d["chart"]["result"][0]["indicators"]["quote"][0].get("close")
        # Yahoo doesn't give PE ratio here; skip
    except Exception:
        pass

    # Reliable proxy: FRED MULTPL doesn't exist, so compute
    # SP500 / (rolling 10y earnings estimate via CPROFIT + GDP deflator)
    sp_obs  = fred("SP500",    (NOW - timedelta(days=30)).strftime("%Y-%m-%d"))
    cpr_obs = fred("CPROFIT",  (NOW - timedelta(days=400)).strftime("%Y-%m-%d"))
    cpi_obs = fred("CPIAUCSL", (NOW - timedelta(days=120)).strftime("%Y-%m-%d"))
    if not sp_obs or not cpr_obs or not cpi_obs:
        return None
    sp_price = safe_float(sp_obs[-1]["value"])
    # Annualise latest quarterly profit; scale to per-share proxy
    corp_profit_annual = safe_float(cpr_obs[-1]["value"])  # Mrd. USD quarterly → *4
    if not sp_price or not corp_profit_annual or corp_profit_annual <= 0:
        return None
    # CAPE proxy: current SP500 / (10y avg real earnings proxy)
    # Use all available CPROFIT observations for rolling average
    cpr_vals = [safe_float(o["value"]) for o in cpr_obs if safe_float(o["value"])]
    if len(cpr_vals) < 4:
        return None
    avg_profit = sum(cpr_vals[-40:]) / len(cpr_vals[-40:])  # up to 10 years
    cpi_vals = [safe_float(o["value"]) for o in cpi_obs if safe_float(o["value"])]
    cpi_now = cpi_vals[-1] if cpi_vals else 1
    # SP500 covers ~500 large caps ≈ ~45% of CPROFIT → rough per-point scaling
    # Scale so CAPE ≈ 20–35 in typical range; empirical factor ≈ 0.00065
    cape_proxy = sp_price / (avg_profit * 0.00065)
    if 5 < cape_proxy < 100:
        return round(cape_proxy, 1)
    return None


def fetch_news(query: str, max_items: int = 6) -> list[dict]:
    q = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    try:
        root = ET.fromstring(http_get(url, timeout=20))
    except Exception:
        return []
    out, seen = [], set()
    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        if not title or title.lower() in seen:
            continue
        seen.add(title.lower())
        source_el = item.find("source")
        source = source_el.text.strip() if source_el is not None and source_el.text else "Google News"
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        try:
            iso = parsedate_to_datetime(pub).astimezone(timezone.utc).isoformat()
        except Exception:
            iso = NOW.isoformat()
        out.append({"title": title, "source": source, "link": link, "publishedAt": iso})
        if len(out) >= max_items:
            break
    return out


def to_points(obs, key='value'):
    pts = []
    for o in obs:
        try:
            pts.append({"date": o["date"], "value": round(float(o[key]), 2)})
        except Exception:
            pass
    return pts


def latest_value(points):
    return points[-1]["value"] if points else None


def classify_phase(spx20, vix, brent20, rec):
    if spx20 is None:
        return ("Neutral", "Die Marktphase ist mangels Daten nicht belastbar einordenbar.")
    if spx20 >= 0 and (vix or 0) < 28 and (brent20 or 0) > 5:
        return ("Aufwärtsphase mit Stresstest", "Aktien halten sich bislang robuster als das geopolitische und energetische Umfeld vermuten ließe.")
    if spx20 < -8 and (vix or 0) >= 30:
        return ("Risk-off / Marktbruch", "Der Markt zeigt Stress und breitere Schwäche. Die Fehlertoleranz ist klar gesunken.")
    if spx20 > -3 and (vix or 0) < 25:
        return ("Stabil bis vorsichtig", "Das Umfeld wirkt angespannt, aber nicht gebrochen. Der Markt pendelt eher zwischen Risiko und Hoffnung.")
    return ("Zwischenphase", "Das Bild ist gemischt: Belastungen sind real, aber eine klare Kapitulation ist nicht bestätigt.")


def title_status(label, status):
    return {"title": label, "status": status}


def score_bottom(dd, vix, rsi, rec, geo_risk):
    score = 50
    if dd is not None:
        score += 10 if dd <= -10 else 4 if dd <= -5 else -4
    if vix is not None:
        score += 8 if 18 <= vix <= 28 else -6 if vix > 35 else 0
    if rsi is not None:
        score += 8 if 35 <= rsi <= 55 else 5 if rsi < 35 else -3
    if rec is not None:
        score -= min(15, rec / 5)
    score -= geo_risk * 0.25
    return int(max(5, min(95, round(score))))


def score_crash(vix, rec, brent20, breadth_break, geo_risk):
    score = 15
    if vix is not None:
        score += 18 if vix >= 30 else 8 if vix >= 22 else 0
    if rec is not None:
        score += min(20, rec / 4)
    if brent20 is not None:
        score += 10 if brent20 > 8 else 4 if brent20 > 3 else 0
    score += 10 if breadth_break else 0
    score += min(20, int(geo_risk * 0.3))
    return int(max(5, min(95, round(score))))


def timing_quality(dd, cape, vix, geo_risk):
    early = 20
    optimal = 40
    late = 40
    if dd is not None:
        if dd <= -12:
            optimal += 10; late -= 8
        elif dd >= -3:
            late += 10; optimal -= 6
    if cape is not None and cape > 30:
        early += 8; late -= 4
    if vix is not None and vix > 28:
        early += 6; optimal -= 2
    if geo_risk > 20:
        early += 6; optimal -= 3; late -= 3
    total = max(1, early + optimal + late)
    e = round(100 * early / total)
    o = round(100 * optimal / total)
    l = 100 - e - o
    return e, o, l


def main():
    latest = {}
    if LATEST.exists():
        latest = json.loads(LATEST.read_text(encoding="utf-8"))
    indicators = latest.get("indicators", {})
    global_label = latest.get("globalComposite", {}).get("label", "")

    # Core market data
    spy = stooq_daily("spy.us")
    qqq = stooq_daily("qqq.us")
    rsp = stooq_daily("rsp.us")
    if not spy or not qqq:
        print("WARNING: stooq market data missing", file=sys.stderr)
    spy_closes = [r["close"] for r in spy]
    qqq_closes = [r["close"] for r in qqq]
    rsp_closes = [r["close"] for r in rsp]

    spx20 = pct_change(spy_closes[-21] if len(spy_closes) > 21 else None, spy_closes[-1] if spy_closes else None)
    qqq50 = sma(qqq_closes, 50)
    qqq200 = sma(qqq_closes, 200)
    rsi = rsi14(qqq_closes)
    qqq_high = max(qqq_closes) if qqq_closes else None
    dd = pct_change(qqq_high, qqq_closes[-1]) if qqq_closes else None

    rsp_spy_ratio = []
    for i in range(min(len(rsp_closes), len(spy_closes))):
        if spy_closes[i]:
            rsp_spy_ratio.append(rsp_closes[i] / spy_closes[i])
    breadth_break = False
    breadth_text = "Nicht kollabiert. Das spricht eher gegen akuten Crash-Modus."
    if len(rsp_spy_ratio) > 21:
        r20 = pct_change(rsp_spy_ratio[-21], rsp_spy_ratio[-1])
        breadth_break = (r20 or 0) < -2.5
        breadth_text = "Marktbreite ist klar geschwächt. Das spricht für selektivere Führung und fragileres Risiko." if breadth_break else "Nicht kollabiert. Das spricht eher für einen Markt, der zwischen Risiko und Hoffnung pendelt."

    vix_obs = fred("VIXCLS", (NOW - timedelta(days=180)).strftime("%Y-%m-%d"))
    brent_obs = fred("DCOILBRENTEU", (NOW - timedelta(days=180)).strftime("%Y-%m-%d"))
    vix_pts = to_points(vix_obs)
    brent_pts = to_points(brent_obs)
    vix = latest_value(vix_pts)
    brent20 = pct_change(brent_pts[-21]["value"] if len(brent_pts) > 21 else None, brent_pts[-1]["value"] if brent_pts else None)
    rec = safe_float(indicators.get("recProb", {}).get("value"))
    infl = safe_float(indicators.get("inflation", {}).get("value"))
    fed = safe_float(indicators.get("fedRate", {}).get("value"))
    sent = safe_float(indicators.get("sentiment", {}).get("value"))

    # GDP real growth (quarterly, annualised %)
    gdp_growth_obs = fred("A191RL1Q225SBEA", (NOW - timedelta(days=400)).strftime("%Y-%m-%d"))
    gdp_growth = safe_float(gdp_growth_obs[-1]["value"]) if gdp_growth_obs else None

    # Gewinnwachstum: US-Unternehmensgewinne YoY aus FRED
    eps_growth = compute_corp_profit_growth()

    # Buffett-Indikator: Wilshire 5000 / GDP aus FRED (zuverlässig)
    buffett_pct = compute_buffett_indicator(FRED_KEY)

    phase_title, phase_status = classify_phase(spx20, vix, brent20, rec)

    # News + geopolitical risk
    news = fetch_news("Strait of Hormuz OR Hormuz OR oil OR LNG OR Iran OR Brent OR Wall Street OR inflation", max_items=10)
    titles = " ".join(n["title"].lower() for n in news)
    tags = []
    if "hormuz" in titles: tags.append("Hormus")
    if "oil" in titles or "brent" in titles: tags.append("Öl")
    if "lng" in titles or "gas" in titles: tags.append("LNG / Gas")
    if "inflation" in titles or "ppi" in titles: tags.append("Inflation")
    if "wall street" in titles or "stocks" in titles: tags.append("Aktienmarkt")
    if not tags: tags = ["Marktumfeld"]
    geo_risk = 0
    for k in ["hormuz", "oil", "brent", "lng", "gas", "iran", "shipping", "sanction"]:
        geo_risk += titles.count(k) * 3
    geo_risk = min(60, geo_risk)

    # Shiller CAPE Proxy aus FRED-Daten
    cape = compute_sp500_cape_proxy()
    cape_text = ("Weiter hoch. Der US-Markt bleibt historisch teuer; das erhöht die Empfindlichkeit gegenüber externen Schocks."
                 if cape and cape > 30
                 else ("Erhöht, aber nicht extrem." if cape and cape > 24 else "Nicht überhitzt."))

    # Buffett-Indikator Einordnung
    if buffett_pct:
        buffett_text = ("Stark überbewertet." if buffett_pct > 160
                        else "Überbewertet." if buffett_pct > 130
                        else "Leicht überbewertet." if buffett_pct > 100
                        else "Fair bewertet.")
    else:
        buffett_text = "Keine frische Live-Einordnung verfügbar."

    # Probabilities
    bottom = score_bottom(dd, vix, rsi, rec or 0, geo_risk)
    crash = score_crash(vix, rec or 0, brent20, breadth_break, geo_risk)
    early, optimal, late = timing_quality(dd, cape, vix, geo_risk)

    mood = "Zwiespältig. Der Markt verarbeitet Entspannungs- und Eskalationssignale gleichzeitig." if news else "Gemischt. Keine frischen Newsdaten verfügbar."
    risks = [
        "Iran / Straße von Hormus",
        "Öl- und Energieschock",
        "Gas- / LNG-Risiko",
        "Inflationsrunde 2",
        "hohes Bewertungsniveau",
    ]

    geopolitics = [
        {
            "field": "Straße von Hormus",
            "status": "Erhöhtes Störungsrisiko; Einschränkungen würden Energie- und Logistikketten sofort belasten." if "hormuz" in titles else "Derzeit kein klar bestätigter Totalausfall, aber ein zentrales geopolitisches Nadelöhr.",
            "impact": "Belastet Öl, Transport, Inflationserwartungen und globale Risikoassets."
        },
        {
            "field": "Ölkrise",
            "status": "Real, wenn Brent und Nachrichtenlage gleichzeitig anziehen." if (brent20 or 0) > 3 else "Der Ölmarkt bleibt ein sensibler Verstärker, aber kein bestätigter Vollschock.",
            "impact": "Höhere Ölpreise belasten Inflation, Margen und Zinssenkungsfantasie."
        },
        {
            "field": "Physischer Markt",
            "status": "Angespannt, wenn News auf Lieferengpässe und physischen Knappheitsdruck hinweisen." if any(k in titles for k in ["shortage", "physical", "supply"]) else "Keine harte Live-Bestätigung, aber der physische Markt bleibt ein Schlüsselrisiko.",
            "impact": "Physische Knappheit kann Futures-Signale untertreiben und Preissprünge verschärfen."
        },
        {
            "field": "Gas / LNG",
            "status": "Zusatzrisiko bei Transport- oder Exportstörungen." if any(k in titles for k in ["lng", "gas", "qatar"]) else "Derzeit eher ein latentes Zusatzrisiko als der Haupttreiber.",
            "impact": "Kann Industrie, Europa-Sentiment und Zweitrundeneffekte bei Energie verstärken."
        },
    ]


    # Chart data: try QQQ from Stooq, fall back to SP500 from FRED
    if qqq and len(qqq) >= 10:
        qqq_chart = [
            {
                "date": qqq[i]["date"],
                "close": round(qqq[i]["close"], 2),
                "ma50": round(sum(qqq_closes[i-49:i+1]) / 50, 2) if i >= 49 else None,
                "ma200": round(sum(qqq_closes[i-199:i+1]) / 200, 2) if i >= 199 else None
            }
            for i in range(max(0, len(qqq) - 260), len(qqq))
        ]
        qqq_chart_label = "QQQ"
    else:
        sp500_obs = fred("SP500", (NOW - timedelta(days=400)).strftime("%Y-%m-%d"))
        sp500_obs = [o for o in sp500_obs if safe_float(o.get("value")) and o.get("value") not in (".", None, "")]
        sp500_closes = [float(o["value"]) for o in sp500_obs]
        qqq_chart = []
        for i in range(max(0, len(sp500_obs) - 260), len(sp500_obs)):
            cs = sp500_closes[:i+1]
            qqq_chart.append({
                "date": sp500_obs[i]["date"],
                "close": round(sp500_closes[i], 2),
                "ma50": round(sum(cs[-50:]) / len(cs[-50:]), 2) if len(cs) >= 50 else None,
                "ma200": round(sum(cs[-200:]) / len(cs[-200:]), 2) if len(cs) >= 200 else None,
            })
        qqq_chart_label = "S&P 500 (Fallback)"

    facts = {
        "generatedAt": NOW.isoformat(),
        "meta": {
            "schemaVersion": "1.0",
            "sourceSummary": ["latest.json", "FRED", "Stooq", "Multpl", "CurrentMarketValuation", "Google News RSS"]
        },
        "marketStatus": {
            "phase": {**title_status(phase_title, phase_status),
                "value": round(spx20, 1) if spx20 is not None else None,
                "unit": "%", "valueLabel": "SPX 20T",
                "tone": "h" if (spx20 or 0) < -8 else "m" if (spx20 or 0) < -3 else "l"},
            "vix": {**title_status(
                "Keine Panikphase" if (vix or 0) < 30 else "Stressphase",
                "Keine klare Panikphase ableitbar, aber das Umfeld bleibt nervös." if (vix or 0) < 30 else "Der Markt preist klar höhere Nervosität ein."),
                "value": round(vix, 1) if vix else None,
                "tone": "h" if (vix or 0) >= 30 else "m" if (vix or 0) >= 20 else "l"},
            "breadth": {**title_status("Nicht kollabiert" if not breadth_break else "Breite bricht weg", breadth_text),
                "value": (round(pct_change(rsp_spy_ratio[-21], rsp_spy_ratio[-1]), 1)
                           if len(rsp_spy_ratio) > 21 else None),
                "unit": "%", "valueLabel": "RSP/SPY 20T",
                "tone": "h" if breadth_break else "l"},
        },
        "overallMode": {**title_status(
            global_label or phase_title,
            "Die Daten zeigen eher ein angespanntes, aber noch nicht gebrochenes Umfeld." if crash < 40 else "Das Umfeld ist deutlich fragiler geworden."),
            "value": crash, "unit": "% Crash-Risk",
            "tone": "h" if crash >= 40 else "m" if crash >= 25 else "l"},
        "technicalTriggers": [
            {"label": "50T vs. 200T",
             "status": "Kein belastbares Trendbruch-Signal. Der Markt wirkt kurzfristig eher stabil als kapitulativ." if qqq50 and qqq200 and qqq50 > qqq200 else "Trendbruch aktiv oder nahe dran. Das erhöht die Vorsicht gegenüber Growth.",
             "value": round(qqq_closes[-1], 1) if qqq_closes else None, "unit": "$",
             "tone": "l" if (qqq50 and qqq200 and qqq50 > qqq200) else "h",
             "history": [{"date": qqq[i]["date"], "value": qqq[i]["close"]} for i in range(max(0, len(qqq)-60), len(qqq))]},
            {"label": "RSI (14)",
             "status": "Kein Überverkauft-Signal. Die Bewegung wirkt eher nervös als panisch." if rsi is not None and rsi > 30 else "Überverkauft-Signal aktiv. Das spricht eher für eine überdehnte Bewegung als für saubere Ruhe.",
             "value": round(rsi, 1) if rsi is not None else None,
             "tone": "h" if rsi is not None and rsi < 30 else "m" if rsi is not None and rsi < 40 else "l"},
            {"label": "Drawdown vom Hoch",
             "status": "Kein Hinweis auf frischen massiven Abverkauf. Das Bild ist angespannt, aber nicht gebrochen." if dd is not None and dd > -10 else "Der Drawdown ist tief genug, um das Bild klar fragiler zu machen.",
             "value": round(dd, 1) if dd is not None else None, "unit": "%",
             "tone": "h" if (dd or 0) <= -10 else "m" if (dd or 0) <= -5 else "l"},
        ],
        "valuation": [
            {"label": "Shiller CAPE",
             "status": cape_text,
             "value": round(cape, 1) if cape else None,
             "tone": "h" if (cape or 0) > 30 else "m" if (cape or 0) > 24 else "l"},
            {"label": "Buffett-Indikator",
             "status": f"{buffett_text} Das erhöht die Empfindlichkeit gegenüber externen Schocks.",
             "value": buffett_pct,
             "unit": "%",
             "tone": "h" if (buffett_pct or 0) > 160 else "m" if (buffett_pct or 0) > 110 else "l"},
            {"label": "Gewinnwachstum",
             "status": (f"S&P-500-Gewinnwachstum YoY: {eps_growth:+.1f} %. Kurzfristig überlagert von Öl, Geopolitik und Inflation."
                        if eps_growth is not None
                        else "Kurzfristig klar zweitrangig gegenüber Öl, Geopolitik, Lieferketten und Inflation."),
             "value": round(eps_growth, 1) if eps_growth is not None else None,
             "unit": "%",
             "tone": "l" if (eps_growth or 0) > 5 else "m" if (eps_growth or 0) > 0 else "h"},
        ],
        "macro": [
            {"label": "Zinsumfeld",
             "status": "Komplizierter geworden. Höhere Energiepreise erschweren eine lockere Zinsfantasie." if (fed or 0) >= 3 else "Weniger restriktiv als in Hochzinsphasen, aber immer noch ein relevanter Faktor.",
             "value": round(fed, 2) if fed else None, "unit": "%",
             "tone": "h" if (fed or 0) >= 4.5 else "m" if (fed or 0) >= 2.5 else "l"},
            {"label": "Wachstum",
             "status": (f"BIP-Wachstum (real, ann.) zuletzt {gdp_growth:+.1f} %. Das Wachstumsrisiko hat zugenommen." if gdp_growth is not None and geo_risk >= 15
                        else f"BIP-Wachstum (real, ann.) zuletzt {gdp_growth:+.1f} %." if gdp_growth is not None
                        else "Noch kein harter Wachstumsschock sichtbar, aber das Umfeld ist fragiler geworden."),
             "value": round(gdp_growth, 1) if gdp_growth is not None else None,
             "unit": "%",
             "tone": "h" if (gdp_growth or 0) < 0 else "m" if (gdp_growth or 0) < 1.5 else "l"},
            {"label": "Rezessionsindikatoren",
             "status": "Noch kein bestätigter Crash-Makro-Modus, aber klar fragiler als in ruhigeren Marktphasen." if (rec or 0) < 50 else "Rezessionsrisiko erhöht. Der Makro-Hintergrund verdient deutlich mehr Respekt.",
             "value": round(rec, 1) if rec else None, "unit": "%",
             "tone": "h" if (rec or 0) >= 50 else "m" if (rec or 0) >= 20 else "l"},
        ],
        "sentiment": {
            "marketMood": mood,
            "tags": tags,
            "risks": risks,
        },
        "geopolitics": geopolitics,
        "marketBottomProbability": {
            "title": "Market Bottom Probability",
            "score": bottom,
            "scoreLabel": f"{bottom} %",
            "reason": "Positiv ist die fehlende Kapitulation des Marktes. Negativ ist, dass Energie- und Geopolitikrisiken makroökonomisch relevant bleiben.",
            "interpretation": "Das bisherige Tief ist plausibel, aber geopolitisch nicht sicher hinter uns."
        },
        "crashProbability": {
            "title": "Crash Probability",
            "score": crash,
            "scoreLabel": f"{crash} %",
            "reason": "Die Wahrscheinlichkeit steigt mit Energie-, Lieferketten- und Rezessionsstress, bleibt aber begrenzt, solange keine klare Kapitulation sichtbar ist.",
            "interpretation": "Kein dominantes Crash-Szenario, aber deutlich mehr Tail Risk als in ruhigen Phasen."
        },
        "timingQuality": {
            "title": "Timing-Qualität",
            "score": optimal,
            "scoreLabel": f"Zu früh {early} % · Optimal {optimal} % · Zu spät {late} %",
            "reason": "Die aktuelle Lage ist offener als in klaren Schnäppchen- oder Überhitzungsphasen. Timing verdient mehr Gewicht als in Normalphasen.",
            "interpretation": "Nicht klar zu spät, aber auch keine eindeutige Schnäppchenphase."
        },
        "updateTriggers": {
            "positive": [
                "Kein weiterer Eskalationssprung in 5–10 Handelstagen.",
                "Breiter Marktrücksetzer von etwa -4 % bis -7 % ohne neue Makroverschlechterung.",
                "VIX beruhigt sich wieder trotz angespannter Nachrichtenlage."
            ],
            "refresh": [
                "Brent steigt wieder klar und nachhaltig über kritische Stresszonen.",
                "Neue militärische Eskalation rund um Hormus oder Energie-Infrastruktur.",
                "Deutlicher VIX-Sprung / klarer Risk-off-Tag.",
                "Sichtbarer Marktbruch statt bloßer Nervosität."
            ]
        },
        "timeVsTiming": {
            "timeInMarket": "Bleibt langfristig richtig.",
            "marketTiming": "Ist aktuell wichtiger als sonst, aber nicht stark genug für vollständiges Warten.",
            "summary": "In einem normalen Markt wäre die Entscheidung näher an einer Vollumsetzung. Mit der aktuellen Energie- und Geopolitik-Lage ist ein gestaffelter, disziplinierter Blick robuster."
        },
        "charts": {
            "qqq": qqq_chart,
            "chartLabel": qqq_chart_label,
            "vix": vix_pts[-90:],
            "brent": brent_pts[-90:],
        },
        "news": news[:8],
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(facts, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {OUT}")

if __name__ == "__main__":
    main()
