#!/usr/bin/env python3
from __future__ import annotations
import json, math, os, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LEGACY_SCRIPT = ROOT / "_fetch_data_legacy_base.py"
LEGACY_OUT = ROOT / "_latest_base.json"
FINAL_OUT = ROOT.parent / "data" / "latest.json"

BASE_SCRIPT = r"""#!/usr/bin/env python3
from __future__ import annotations
import html, json, os, sys, urllib.error, urllib.parse, urllib.request, xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

FRED_KEY = os.environ.get("FRED_API_KEY", "").strip()
if not FRED_KEY:
    print("ERROR: FRED_API_KEY secret not found.", file=sys.stderr)
    sys.exit(1)

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
NOW = datetime.now(timezone.utc)
OUT = Path("data/latest.json")
MONTHLY_START = (NOW - timedelta(days=365 * 15)).strftime("%Y-%m-%d")
DAILY_START = (NOW - timedelta(days=365 * 6)).strftime("%Y-%m-%d")
QUARTERLY_START = (NOW - timedelta(days=365 * 20)).strftime("%Y-%m-%d")
YEARLY_START = "2010-01-01"
TWO_WEEKS_AGO = (NOW - timedelta(days=14)).strftime("%Y-%m-%d")
YEAR_START = f"{NOW.year}-01-01"
TICKER_ICONS = {"SPX":"📊","VIX":"⚡","BRENT":"🛢️","US 10Y":"📉","GOLD":"🟡","SILBER":"⚪","EUR/USD":"💱"}

def http_get(url:str, timeout:int=25)->bytes:
    req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0 (compatible; MarketRiskMonitor/3.0)"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def fred(series_id:str, observation_start:str|None=None, limit:int|None=None)->list[dict]:
    params={"series_id":series_id,"api_key":FRED_KEY,"file_type":"json","sort_order":"asc"}
    if observation_start:
        params["observation_start"] = observation_start
    if limit is not None:
        params["limit"] = str(limit)
    url = FRED_BASE + "?" + urllib.parse.urlencode(params)
    try:
        payload = json.loads(http_get(url).decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"ERROR {series_id}: HTTP {e.code}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"ERROR {series_id}: {e}", file=sys.stderr)
        return []
    obs = [o for o in payload.get("observations", []) if o.get("value") not in (None, "", ".")]
    print(f"✓ {series_id}: {len(obs)} valid obs")
    return obs

def first_available(series_ids:list[str], observation_start:str|None=None)->tuple[str,list[dict]]:
    for sid in series_ids:
        obs = fred(sid, observation_start=observation_start)
        if obs:
            return sid, obs
    return series_ids[0], []

def history_points(obs:list[dict], dec:int=2)->list[dict]:
    out=[]
    for o in obs:
        try:
            out.append({"date":o["date"],"value":round(float(o["value"]),dec)})
        except Exception:
            pass
    return out

def filter_to_actual_points(points:list[dict], cadence:str="daily")->list[dict]:
    if not points:
        return []
    today = NOW.date()
    current_year = today.year
    out=[]
    for p in points:
        ds = str(p.get("date", "")).strip()
        if not ds:
            continue
        try:
            d = datetime.strptime(ds, "%Y-%m-%d").date()
        except Exception:
            continue
        if cadence == "annual":
            if d.year >= current_year:
                continue
        elif d > today:
            continue
        out.append(p)
    return out

def latest_actual_value(points:list[dict], cadence:str="daily", default=None):
    pts = filter_to_actual_points(points, cadence)
    return pts[-1]["value"] if pts else default

def latest_actual_date(points:list[dict], cadence:str="daily")->str:
    pts = filter_to_actual_points(points, cadence)
    return pts[-1]["date"] if pts else ""

def build_yoy_history(obs:list[dict], dec:int=2, lag:int=12)->list[dict]:
    out=[]
    for i in range(lag, len(obs)):
        cur=float(obs[i]["value"])
        prev=float(obs[i-lag]["value"])
        if prev==0:
            continue
        out.append({"date":obs[i]["date"],"value":round(((cur/prev)-1)*100,dec)})
    return out

def latest_by_date(points:list[dict])->str:
    return points[-1]["date"] if points else ""

def last_value(obs:list[dict], default:float=0.0)->float:
    if not obs:
        return default
    try:
        return float(obs[-1]["value"])
    except Exception:
        return default

def ticker_item(label:str, obs:list[dict], unit:str="", dec:int=2)->dict|None:
    if not obs:
        return None
    cur=float(obs[-1]["value"])
    prev=float(obs[-2]["value"]) if len(obs)>1 else None
    return {
        "label":label,
        "icon":TICKER_ICONS.get(label,"•"),
        "val":round(cur,dec),
        "chg":round(cur-prev,dec) if prev is not None else None,
        "chgPct":round(((cur-prev)/prev)*100,2) if prev not in (None,0) else None,
        "unit":unit,
        "dec":dec,
    }

def ytd_stats(sp_history:list[dict])->tuple[float,float,float,str]:
    year_points=[p for p in sp_history if p["date"]>=YEAR_START]
    source=year_points if len(year_points)>=2 else sp_history
    if len(source)<2:
        return 0.0,0.0,0.0,""
    first, latest = source[0]["value"], source[-1]["value"]
    ytd = round(((latest/first)-1)*100,2) if first else 0.0
    return ytd, first, latest, source[-1]["date"]

def fill_forward_map(points:list[dict])->dict[str,float]:
    return {p["date"]:p["value"] for p in points}

def stress_score(vix:float, brent:float, eurusd:float, us10y:float, sp_ytd:float)->int:
    score=0
    score += 28 if vix>=30 else 22 if vix>=25 else 15 if vix>=20 else 8 if vix>=16 else 0
    score += 26 if brent>=110 else 20 if brent>=100 else 13 if brent>=90 else 7 if brent>=80 else 0
    score += 16 if eurusd<=1.00 else 12 if eurusd<=1.05 else 7 if eurusd<=1.08 else 3 if eurusd<=1.12 else 0
    score += 13 if us10y>=5 else 9 if us10y>=4.5 else 6 if us10y>=4 else 3 if us10y>=3.5 else 0
    score += 17 if sp_ytd<=-20 else 11 if sp_ytd<=-10 else 6 if sp_ytd<=-5 else 0
    return min(100, score)

def stress_label(score:int)->str:
    return "Hoch" if score>=60 else "Erhöht" if score>=35 else "Entspannt"

def build_stress_history(vix, brent, eurusd, us10y, sp500):
    maps=[fill_forward_map(x) for x in (vix, brent, eurusd, us10y, sp500)]
    vix_map, brent_map, eur_map, y10_map, sp_map = maps
    dates=sorted(set(vix_map)|set(brent_map)|set(eur_map)|set(y10_map)|set(sp_map))
    lv=lb=le=ly=ls=base=None
    out=[]
    for d in dates:
        if d in vix_map: lv=vix_map[d]
        if d in brent_map: lb=brent_map[d]
        if d in eur_map: le=eur_map[d]
        if d in y10_map: ly=y10_map[d]
        if d in sp_map:
            ls=sp_map[d]
            if base is None: base=ls
        if None in (lv,lb,le,ly,ls,base) or base==0:
            continue
        sp_ytd=((ls/base)-1)*100
        out.append({"date":d,"value":stress_score(float(lv),float(lb),float(le),float(ly),float(sp_ytd))})
    return out

def compute_region_label(score:float)->str:
    return "Angespannt" if score>=65 else "Beobachten" if score>=40 else "Stabil"

def safe_last(points:list[dict], default:float=0.0)->float:
    return points[-1]["value"] if points else default

def yoy_from_level(points:list[dict], lag:int)->list[dict]:
    out=[]
    for i in range(lag, len(points)):
        cur=points[i]["value"]; prev=points[i-lag]["value"]
        if prev == 0:
            continue
        out.append({"date":points[i]["date"],"value":round(((cur/prev)-1)*100,2)})
    return out

def build_region(name:str, inflation:dict|None, growth_or_rate:dict|None, fx:dict|None, driver_text:str, score:float)->dict:
    dates=[x.get("date","") for x in (inflation or {}, growth_or_rate or {}, fx or {}) if x and x.get("date")]
    latest=max(dates) if dates else ""
    return {
        "name":name,
        "score":round(score,1),
        "label":compute_region_label(score),
        "date":latest,
        "summary":driver_text,
        "driverText":driver_text,
        "inflation": inflation,
        "growthOrRate": growth_or_rate,
        "fx": fx,
    }

def clean_title(title:str)->str:
    title=html.unescape(title).strip()
    for sep in (" - ", " | ", " — "):
        if sep in title:
            return title.split(sep)[0].strip()
    return title

def classify_news(title:str)->dict:
    t=title.lower()
    if any(k in t for k in ["oil","brent","opec","hormuz","energy","gas"]):
        return {
            "bucket":"Energie / Inflation",
            "summary":"Die Meldung betrifft Energiepreise oder Lieferwege. Das ist wichtig, weil Energie direkt auf Inflation, Transportkosten und Risikoappetit wirkt.",
            "impact":"Hoch, wenn der Preisanstieg anhält oder Lieferwege gestört bleiben.",
            "probability":"Mittel bis hoch, wenn die Meldung anhaltende Angebotsknappheit beschreibt.",
            "horizon":"Sofort bis einige Wochen.",
            "priceEffect":"Vor allem Öl, Energieaktien, Inflationserwartungen, Staatsanleihen und zinssensitive Aktien könnten reagieren.",
            "assets":["Öl","Energieaktien","Inflation","Staatsanleihen"],
        }
    if any(k in t for k in ["fed","rate","rates","inflation","cpi","pce","yield","bond","ecb","boj"]):
        return {
            "bucket":"Zinsen / Inflation",
            "summary":"Die Meldung verschiebt Erwartungen an Zentralbanken, Zinsen oder Inflation. Das trifft oft direkt auf Bewertungen und Renditen.",
            "impact":"Mittel bis hoch, weil Zins- und Inflationspfade fast alle Anlageklassen beeinflussen.",
            "probability":"Hoch, wenn die Nachricht harte Daten oder klare Aussagen von Zentralbanken enthält.",
            "horizon":"Von sofort bis mehrere Wochen.",
            "priceEffect":"Besonders betroffen sind Staatsanleihen, Wachstumstitel, US-Dollar, Gold und breite Aktienindizes.",
            "assets":["Staatsanleihen","Aktien","US-Dollar","Gold"],
        }
    if any(k in t for k in ["earnings","guidance","profit","results","forecast"]):
        return {
            "bucket":"Unternehmensgewinne",
            "summary":"Die Meldung zeigt, ob Unternehmen mit Kosten, Nachfrage und Margendruck besser oder schlechter klarkommen als gedacht.",
            "impact":"Mittel, manchmal hoch – je nachdem, wie groß und richtungsweisend die betroffenen Unternehmen sind.",
            "probability":"Hoch, wenn mehrere große Unternehmen in dieselbe Richtung überraschen.",
            "horizon":"Kurzfristig bis zur nächsten Berichtswelle.",
            "priceEffect":"Direkt betroffen sind Einzelaktien, Sektoren und oft auch der Gesamtmarkt über die Gewinnschätzungen.",
            "assets":["Aktien","Sektoren","Gewinnschätzungen"],
        }
    if any(k in t for k in ["china","euro","europe","japan","brics","yuan","yen"]):
        return {
            "bucket":"Globales Wachstum",
            "summary":"Die Meldung betrifft wichtige Wirtschaftsblöcke außerhalb der USA. Das ist relevant für Nachfrage, Währungen, Industrie und Rohstoffe.",
            "impact":"Mittel, bei großen Wachstums- oder Währungssignalen auch hoch.",
            "probability":"Mittel, weil globale Themen oft über mehrere Tage in Marktpreise einsickern.",
            "horizon":"Tage bis Wochen.",
            "priceEffect":"Weltaktien, Rohstoffe, Exportwerte und regionale Währungen könnten besonders reagieren.",
            "assets":["Weltaktien","Währungen","Rohstoffe"],
        }
    if any(k in t for k in ["tariff","trade","sanction","shipping","supply chain"]):
        return {
            "bucket":"Handel / Lieferketten",
            "summary":"Die Meldung betrifft Zölle, Sanktionen oder Logistik. Das ist wichtig, weil solche Themen Preise, Margen und Verfügbarkeit von Vorprodukten beeinflussen.",
            "impact":"Mittel bis hoch, wenn die Störung mehrere Branchen oder Länder betrifft.",
            "probability":"Mittel, weil politische Schlagzeilen oft erst bestätigt werden müssen.",
            "horizon":"Sofort bis mehrere Wochen.",
            "priceEffect":"Besonders empfindlich reagieren Industrie, Transport, Rohstoffe, Inflationspreise und konjunktursensitive Aktien.",
            "assets":["Industrieaktien","Rohstoffe","Inflation","Transport"],
        }
    return {
        "bucket":"Marktkontext",
        "summary":"Die Meldung liefert zusätzlichen Kontext dafür, wie Risikoappetit, Wachstumserwartungen oder Inflation kurzfristig verschoben werden können.",
        "impact":"Eher mittel. Der Effekt hängt davon ab, ob weitere Daten die Meldung bestätigen.",
        "probability":"Mittel. Einzelmeldungen bewegen Märkte oft erst dann stärker, wenn Folgeinfos nachkommen.",
        "horizon":"Kurzfristig bis einige Tage.",
        "priceEffect":"Am ehesten betroffen sind breite Aktienindizes, Anleihen oder Währungen – je nachdem, worauf die Meldung einzahlt.",
        "assets":["Aktien","Anleihen","Währungen"],
    }

def fetch_news(max_items:int=6)->list[dict]:
    query=urllib.parse.quote("stock market OR inflation OR Federal Reserve OR oil OR recession OR earnings OR tariffs OR ECB OR China OR Japan when:3d")
    url=f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
    try:
        root=ET.fromstring(http_get(url, timeout=20))
    except Exception as e:
        print(f"WARNING news fetch failed: {e}", file=sys.stderr)
        return []
    items=[]; seen=set()
    for item in root.findall("./channel/item"):
        raw=item.findtext("title", default="").strip(); title=clean_title(raw)
        if not title or title.lower() in seen:
            continue
        seen.add(title.lower())
        link=item.findtext("link", default="").strip()
        pub=item.findtext("pubDate", default="").strip()
        source_el=item.find("source")
        source=source_el.text.strip() if source_el is not None and source_el.text else "Google News"
        try:
            pub_iso=parsedate_to_datetime(pub).astimezone(timezone.utc).isoformat()
        except Exception:
            pub_iso=NOW.isoformat()
        ctx=classify_news(title)
        items.append({
            "title":title,
            "source":source,
            "publishedAt":pub_iso,
            "link":link,
            **ctx,
        })
        if len(items)>=max_items:
            break
    return items

print("Fetching core series…")
cpi_raw=fred("CPIAUCSL", observation_start=MONTHLY_START)
effr_raw=fred("EFFR", observation_start=DAILY_START)
rec_raw=fred("RECPROUSM156N", observation_start=MONTHLY_START)
sahm_raw=fred("SAHMCURRENT", observation_start=MONTHLY_START)
sent_raw=fred("UMCSENT", observation_start=MONTHLY_START)
sp_raw=fred("SP500", observation_start=DAILY_START)
print("Fetching market series…")
vix_recent=fred("VIXCLS", observation_start=TWO_WEEKS_AGO)
brent_recent=fred("DCOILBRENTEU", observation_start=TWO_WEEKS_AGO)
us10y_recent=fred("DGS10", observation_start=TWO_WEEKS_AGO)
gold_recent=fred("GOLDAMGBD228NLBM", observation_start=TWO_WEEKS_AGO)
silver_recent=fred("SLVPRUSD", observation_start=TWO_WEEKS_AGO)
eurusd_recent=fred("DEXUSEU", observation_start=TWO_WEEKS_AGO)
print("Fetching long histories…")
vix_hist_raw=fred("VIXCLS", observation_start=DAILY_START)
brent_hist_raw=fred("DCOILBRENTEU", observation_start=DAILY_START)
us10y_hist_raw=fred("DGS10", observation_start=DAILY_START)
eurusd_hist_raw=fred("DEXUSEU", observation_start=DAILY_START)
print("Fetching global layer…")
euro_hicp_raw=fred("CP0000EZ19M086NEST", observation_start=MONTHLY_START)
ecb_rate_raw=fred("ECBDFR", observation_start=DAILY_START)
japan_gdp_raw=fred("JPNRGDPEXP", observation_start=QUARTERLY_START)
japan_fx_series, japan_fx_raw=first_available(["DEXJPUS","EXJPUS"], observation_start=DAILY_START)
japan_cpi_annual_raw=fred("JPNPCPIPCPPPT", observation_start=YEARLY_START)
china_gdp_raw=fred("CHNGDPRAPSMEI", observation_start=YEARLY_START)
china_fx_series, china_fx_raw=first_available(["DEXCHUS","EXCHUS"], observation_start=YEARLY_START)
china_cpi_annual_raw=fred("CHNPCPIPCPPPT", observation_start=YEARLY_START)

if len(cpi_raw)<13 or len(sp_raw)<2:
    print("ERROR: not enough core history", file=sys.stderr)
    sys.exit(1)

inflation_history=build_yoy_history(cpi_raw)
effr_history=history_points(effr_raw,2)
rec_history=history_points(rec_raw,1)
sahm_history=history_points(sahm_raw,2)
sent_history=history_points(sent_raw,1)
sp_history=history_points(sp_raw,2)
vix_hist=history_points(vix_hist_raw,2)
brent_hist=history_points(brent_hist_raw,2)
eurusd_hist=history_points(eurusd_hist_raw,4)
us10y_hist=history_points(us10y_hist_raw,2)
stress_history=build_stress_history(vix_hist, brent_hist, eurusd_hist, us10y_hist, sp_history)

inflation_value = inflation_history[-1]["value"] if inflation_history else 0.0
effr_value = effr_history[-1]["value"] if effr_history else 0.0
rec_value = rec_history[-1]["value"] if rec_history else 0.0
sahm_value = sahm_history[-1]["value"] if sahm_history else 0.0
sent_value = sent_history[-1]["value"] if sent_history else 0.0
sp_ytd, sp_first, sp_latest, sp_date = ytd_stats(sp_history)
stress_value = stress_history[-1]["value"] if stress_history else 0

# Global layer calculations

euro_hicp_yoy = build_yoy_history(euro_hicp_raw)
euro_infl = euro_hicp_yoy[-1]["value"] if euro_hicp_yoy else None
ecb_rate = safe_last(history_points(ecb_rate_raw,2), None)
eurusd_now = safe_last(eurusd_hist, None)
europe_score = 0.0
if euro_infl is not None:
    europe_score += 35 if euro_infl >= 4 else 24 if euro_infl >= 3 else 12 if euro_infl >= 2 else 5
if ecb_rate is not None:
    europe_score += 22 if ecb_rate >= 3.5 else 15 if ecb_rate >= 2.5 else 8 if ecb_rate >= 1.5 else 3
if eurusd_now is not None:
    europe_score += 18 if eurusd_now <= 1.03 else 12 if eurusd_now <= 1.07 else 6 if eurusd_now <= 1.10 else 2

europe = build_region(
    "Europa",
    {"value":euro_infl,"date":latest_by_date(euro_hicp_yoy),"unit":"%","cadence":"monthly","series":"CP0000EZ19M086NEST"},
    {"label":"ECB Einlagezins","value":ecb_rate,"date":latest_by_date(history_points(ecb_rate_raw,2)),"unit":"%","cadence":"daily","series":"ECBDFR"},
    {"label":"EUR/USD","value":eurusd_now,"date":latest_by_date(eurusd_hist),"unit":"","cadence":"daily","series":"DEXUSEU"},
    "Europa wird hier über Preisdruck, Zinsniveau und den Euro gegenüber dem US-Dollar gelesen. Ein schwächerer Euro und hoher Preisdruck erhöhen den Stress im Block.",
    min(100, europe_score),
)

japan_gdp_hist = history_points(japan_gdp_raw,2)
japan_gdp_yoy = yoy_from_level(japan_gdp_hist, 4)
japan_growth = japan_gdp_yoy[-1]["value"] if japan_gdp_yoy else None
japan_infl = latest_actual_value(history_points(japan_cpi_annual_raw,2), "annual", None)
japan_fx_hist = history_points(japan_fx_raw,4)
japan_fx = safe_last(japan_fx_hist, None)
japan_score = 0.0
if japan_growth is not None:
    japan_score += 30 if japan_growth <= 0 else 18 if japan_growth <= 1 else 10 if japan_growth <= 2 else 4
if japan_infl is not None:
    japan_score += 20 if japan_infl >= 3 else 12 if japan_infl >= 2 else 6 if japan_infl >= 1 else 2
if japan_fx is not None:
    japan_score += 22 if japan_fx >= 155 else 16 if japan_fx >= 145 else 10 if japan_fx >= 135 else 4

japan = build_region(
    "Japan",
    {"value":japan_infl,"date":latest_actual_date(history_points(japan_cpi_annual_raw,2), "annual"),"unit":"%","cadence":"annual","series":"JPNPCPIPCPPPT"},
    {"label":"BIP-Wachstum YoY","value":japan_growth,"date":latest_by_date(japan_gdp_yoy),"unit":"%","cadence":"quarterly","series":"JPNRGDPEXP"},
    {"label":"JPY pro USD","value":japan_fx,"date":latest_by_date(japan_fx_hist),"unit":"","cadence":"daily" if japan_fx_series.startswith("DEX") else "monthly","series":japan_fx_series},
    "Japan wird über Wachstum, Preisbild und Yen-Stärke gelesen. Ein schwacher Yen kann globalen Stress und importierten Preisauftrieb verstärken.",
    min(100, japan_score),
)

china_growth = latest_actual_value(history_points(china_gdp_raw,2), "annual", None)
china_infl = latest_actual_value(history_points(china_cpi_annual_raw,2), "annual", None)
china_fx_hist = history_points(china_fx_raw,4)
china_fx = safe_last(china_fx_hist, None)
china_score = 0.0
if china_growth is not None:
    china_score += 28 if china_growth < 4 else 18 if china_growth < 5 else 8 if china_growth < 6 else 4
if china_infl is not None:
    china_score += 22 if china_infl < 1 else 14 if china_infl < 2 else 8 if china_infl < 3 else 4
if china_fx is not None:
    china_score += 18 if china_fx >= 7.2 else 12 if china_fx >= 7.0 else 7 if china_fx >= 6.8 else 3

china = build_region(
    "China",
    {"value":china_infl,"date":latest_actual_date(history_points(china_cpi_annual_raw,2), "annual"),"unit":"%","cadence":"annual","series":"CHNPCPIPCPPPT"},
    {"label":"BIP-Wachstum","value":china_growth,"date":latest_actual_date(history_points(china_gdp_raw,2), "annual"),"unit":"%","cadence":"annual","series":"CHNGDPRAPSMEI"},
    {"label":"CNY pro USD","value":china_fx,"date":latest_by_date(china_fx_hist),"unit":"","cadence":"daily" if china_fx_series.startswith("DEX") else "monthly","series":china_fx_series},
    "China wird hier über Wachstum, Preisbild und Yuan-Stärke gelesen. Schwächeres Wachstum oder sehr niedriger Preisauftrieb können globalen Industrie- und Rohstoffdruck verstärken.",
    min(100, china_score),
)

global_composite = round((europe["score"] + japan["score"] + china["score"]) / 3, 1)
news_items = fetch_news(6)

output={
 "fetchedAt":NOW.isoformat(),
 "meta":{
   "schemaVersion":"3.0",
   "source":"FRED + Google News RSS",
   "notes":{
     "reload":"Das Dashboard lädt nur data/latest.json neu. Frische Daten kommen, wenn GitHub Actions diese Datei überschreibt.",
     "cadence":"Langsame Reihen können monatlich, quartalsweise oder jährlich kommen. Das Dashboard zeigt deshalb Datenstand und Takt getrennt.",
     "news":"Die Nachrichten sind ein kompakter Marktkontext und keine Anlageempfehlung."
   }
 },
 "indicators":{
   "inflation":{"value":inflation_value,"date":latest_by_date(inflation_history),"cadence":"monthly","series":"CPIAUCSL"},
   "fedRate":{"value":effr_value,"date":latest_by_date(effr_history),"cadence":"daily","series":"EFFR","displayName":"Effective Federal Funds Rate"},
   "recProb":{"value":rec_value,"date":latest_by_date(rec_history),"cadence":"monthly","series":"RECPROUSM156N","fastProxy":{"label":"Sahm Rule","value":sahm_value,"date":latest_by_date(sahm_history),"series":"SAHMCURRENT"}},
   "sp500":{"ytd":sp_ytd,"first":sp_first,"latest":sp_latest,"date":sp_date,"cadence":"daily","series":"SP500"},
   "sentiment":{"value":sent_value,"date":latest_by_date(sent_history),"cadence":"monthly","series":"UMCSENT"},
   "tradeStress":{"value":stress_value,"label":stress_label(stress_value),"date":latest_by_date(stress_history),"cadence":"daily","method":"proxy","components":{"vix":round(last_value(vix_hist_raw),2),"brent":round(last_value(brent_hist_raw),2),"eurusd":round(last_value(eurusd_hist_raw),4),"us10y":round(last_value(us10y_hist_raw),2)}}
 },
 "globalComposite":{"value":global_composite,"label":compute_region_label(global_composite),"date":NOW.date().isoformat()},
 "regions":{"europe":europe,"japan":japan,"china":china},
 "ticker":[
   ticker_item("SPX", sp_raw[-10:] if len(sp_raw)>=2 else sp_raw, "", 2),
   ticker_item("VIX", vix_recent, "", 2),
   ticker_item("BRENT", brent_recent, "USD", 2),
   ticker_item("US 10Y", us10y_recent, "%", 2),
   ticker_item("GOLD", gold_recent, "USD", 0),
   ticker_item("SILBER", silver_recent, "USD", 2),
   ticker_item("EUR/USD", eurusd_recent, "", 4)
 ],
 "history":{
   "inflation":inflation_history,
   "fedRate":effr_history,
   "recProb":rec_history,
   "sp500":sp_history,
   "sentiment":sent_history,
   "tradeStress":stress_history
 },
 "news":news_items
}
output["ticker"]=[x for x in output["ticker"] if x is not None]
OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"✓ Wrote {OUT}")
"""


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def trend_label(delta: float, threshold: float = 2.5) -> tuple[str, str]:
    if delta > threshold:
        return "up", "steigt"
    if delta < -threshold:
        return "down", "fällt"
    return "stable", "stabil"


def signal_role_confidence(key: str) -> tuple[str, str]:
    mapping = {
        "inflation": ("companion", "medium"),
        "fedRate": ("confirmation", "high"),
        "recProb": ("early_warning", "medium"),
        "sp500": ("confirmation", "medium"),
        "sentiment": ("context", "low"),
        "tradeStress": ("early_warning", "medium"),
    }
    return mapping[key]


def indicator_copy() -> dict:
    return {
        "inflation": {
            "uiLabel": "Inflation",
            "shortText": "Preisauftrieb im Jahresvergleich.",
            "tooltip": "Wichtig für Zinsen, Bewertungen und Kaufkraft.",
            "longText": "Inflation misst, wie stark das allgemeine Preisniveau steigt. Für Anleger ist sie wichtig, weil sie Zinspolitik, Kaufkraft und Margen beeinflussen kann.",
            "criticalWhen": "Kritischer bei hartnäckig hohem oder erneut steigendem Preisdruck.",
            "interpretation": "Wichtiger Umfeldtreiber, aber allein selten das ganze Bild."
        },
        "fedRate": {
            "uiLabel": "US-Leitzinsniveau",
            "shortText": "Grad des restriktiven Zinsumfelds.",
            "tooltip": "Relevant für Finanzierungskosten und Bewertungen.",
            "longText": "Das US-Zinsniveau wirkt auf Kreditkosten, Liquidität und Bewertungsniveaus. Es bestätigt eher ein Umfeld, als dass es es sehr früh ankündigt.",
            "criticalWhen": "Kritischer, wenn hohe Zinsen länger als erwartet bestehen bleiben.",
            "interpretation": "Eher Bestätigung eines strafferen Umfelds."
        },
        "recProb": {
            "uiLabel": "Rezessionswahrscheinlichkeit",
            "shortText": "Schätzung einer konjunkturellen Abschwächung.",
            "tooltip": "Hilft, Konjunkturrisiken früher zu sehen.",
            "longText": "Die Rezessionswahrscheinlichkeit verdichtet Konjunktursignale zu einer Schätzung. Der Sahm-Rule-Wert ist ein schneller Nebenindikator und sollte nicht isoliert gelesen werden.",
            "criticalWhen": "Kritischer bei steigender Tendenz und Bestätigung durch weitere Makrosignale.",
            "interpretation": "Frühwarnung mit mittlerer Belastbarkeit."
        },
        "sp500": {
            "uiLabel": "S&P 500 seit Jahresbeginn",
            "shortText": "Breites Marktverhalten im laufenden Jahr.",
            "tooltip": "Nützlich zur Bestätigung von Marktstress.",
            "longText": "Der S&P 500 zeigt, wie Risikoappetit, Gewinnerwartungen und Bewertung am Aktienmarkt zusammenspielen. Häufig eher Folge als Ursache eines schlechteren Umfelds.",
            "criticalWhen": "Kritischer bei deutlichen Rückgängen mit Bestätigung durch andere Indikatoren.",
            "interpretation": "Gut für Intensität und Bestätigung, aber kein Frühsignal."
        },
        "sentiment": {
            "uiLabel": "Verbraucherstimmung",
            "shortText": "Stimmungsbild der Verbraucher.",
            "tooltip": "Hilft bei der Einordnung des Konsumumfelds.",
            "longText": "Die Verbraucherstimmung zeigt, wie optimistisch oder vorsichtig Konsumenten ihre Lage sehen. Sie kann schnell schwanken und ist daher vor allem ein Kontextsignal.",
            "criticalWhen": "Kritischer, wenn schwache Stimmung mit harten Schwächesignalen zusammenfällt.",
            "interpretation": "Hilfreich, aber leicht überinterpretierbar."
        },
        "tradeStress": {
            "uiLabel": "Marktstress",
            "shortText": "Verdichteter Stressmesser aus Markt- und Preisvariablen.",
            "tooltip": "Schneller Spannungsmesser, aber nicht perfekt.",
            "longText": "Der Marktstress-Index bündelt VIX, Brent, EUR/USD, US-10Y und Marktverhalten. Er ist nützlich, um Spannungen schnell zu sehen, reagiert aber auch auf kurzfristiges Rauschen.",
            "criticalWhen": "Kritischer bei anhaltend erhöhten Werten und Bestätigung durch Makrosignale.",
            "interpretation": "Frühwarnsignal mit bewusst begrenzter Scheingenauigkeit."
        }
    }


def scenario_bundle(data: dict) -> list[dict]:
    inds = data["indicators"]
    stress = float(inds["tradeStress"]["value"])
    rec = float(inds["recProb"]["value"])
    infl = float(inds["inflation"]["value"])
    fed = float(inds["fedRate"]["value"])
    sp = float(inds["sp500"]["ytd"])
    sent = float(inds["sentiment"]["value"])

    base = clamp(62 - 0.30 * stress - 0.18 * rec - 0.35 * max(infl - 3, 0) - 0.25 * max(fed - 4.5, 0) + 0.10 * max(sp, 0), 5, 85)
    caution = clamp(22 + 0.20 * stress + 0.15 * rec + 0.20 * max(infl - 2.5, 0) + 0.10 * max(fed - 4, 0) - 0.08 * max(sp, 0), 10, 70)
    stress_scn = clamp(8 + 0.28 * stress + 0.30 * rec + 0.12 * max(-sp, 0) + 0.08 * max(85 - sent, 0), 3, 60)
    total = base + caution + stress_scn
    a = round(base / total * 100)
    b = round(caution / total * 100)
    c = 100 - a - b

    def drivers_for(kind: str) -> list[str]:
        drivers = []
        if kind == 'A':
            if stress < 35: drivers.append("Marktstress bleibt begrenzt")
            if rec < 30: drivers.append("Rezessionsrisiko wirkt noch nicht dominant")
            if sp >= 0: drivers.append("Aktienmarkt zeigt bisher keine klare Eskalation")
            if infl < 4: drivers.append("Preisdruck ist erhöht, aber nicht extrem")
        elif kind == 'B':
            if stress >= 30: drivers.append("Marktstress ist sichtbar erhöht")
            if rec >= 20: drivers.append("Konjunkturrisiko nimmt zu")
            if fed >= 4: drivers.append("Restriktives Zinsumfeld bleibt relevant")
            if sent < 80: drivers.append("Verbraucherstimmung stützt das Bild nicht klar")
        else:
            if stress >= 45: drivers.append("Stressindikatoren senden ein klareres Warnsignal")
            if rec >= 30: drivers.append("Rezessionsmodell zieht spürbar an")
            if sp < -8: drivers.append("Marktbewegung bestätigt den Druck")
            if infl >= 4 and fed >= 4.5: drivers.append("Restriktion und Preisdruck greifen gleichzeitig")
        return drivers[:4]

    changes = {
        'A': trend_label((50 - stress) + max(sp, 0) - rec * 0.6),
        'B': trend_label(stress * 0.6 + rec * 0.5 + max(infl - 2.5, 0) * 5 - max(sp, 0) * 0.3),
        'C': trend_label(stress * 0.8 + rec * 0.9 + max(-sp, 0) * 0.8 + max(85 - sent, 0) * 0.3 - 45),
    }
    reasons = {
        'A': {
            'up': "Die Wahrscheinlichkeit steigt, weil das Gesamtbild trotz einzelner Belastungen noch nicht breit eskaliert.",
            'down': "Die Wahrscheinlichkeit fällt, weil Stress- und Schwächesignale an Breite gewinnen.",
            'stable': "Die Wahrscheinlichkeit bleibt stabil, weil sich stützende und belastende Signale derzeit ungefähr ausgleichen."
        },
        'B': {
            'up': "Die Wahrscheinlichkeit steigt, weil mehrere Signale auf ein anspruchsvolleres Umfeld einzahlen, ohne bereits voll zu eskalieren.",
            'down': "Die Wahrscheinlichkeit fällt, weil sich das Umfeld wieder beruhigt oder die Belastung nicht breit genug bestätigt wird.",
            'stable': "Die Wahrscheinlichkeit bleibt stabil, weil Vorsichtssignale sichtbar sind, aber keine klare Richtungsbeschleunigung zeigen."
        },
        'C': {
            'up': "Die Wahrscheinlichkeit steigt, weil Stress, Konjunkturrisiko und Marktverhalten gleichzeitig schlechter werden.",
            'down': "Die Wahrscheinlichkeit fällt, weil die Belastung nachlässt oder keine breite Bestätigung findet.",
            'stable': "Die Wahrscheinlichkeit bleibt stabil, weil die Belastung zwar sichtbar ist, aber nicht klar weiter eskaliert."
        }
    }
    return [
        {
            "id": "A",
            "title": "Basisszenario: stabile Marktphase",
            "probability": a,
            "summary": "Wachstum bleibt grundsätzlich intakt, während Belastungsfaktoren überschaubar bleiben.",
            "supportedBy": drivers_for('A'),
            "probabilityChange": {"direction": changes['A'][0], "label": changes['A'][1], "reason": reasons['A'][changes['A'][0]]},
            "investorMeaning": "Für Anleger spricht dieses Bild eher für Disziplin und Einordnung als für hektische Reaktionen."
        },
        {
            "id": "B",
            "title": "Zwischenszenario: erhöhte Vorsicht",
            "probability": b,
            "summary": "Das Umfeld wird anspruchsvoller. Stresssignale nehmen zu, ohne bereits ein klares Eskalationsbild zu erzeugen.",
            "supportedBy": drivers_for('B'),
            "probabilityChange": {"direction": changes['B'][0], "label": changes['B'][1], "reason": reasons['B'][changes['B'][0]]},
            "investorMeaning": "Für Anleger steigen Nüchternheit, Pufferdenken und Regelorientierung an Bedeutung."
        },
        {
            "id": "C",
            "title": "Stressszenario: defensivere Marktphase",
            "probability": c,
            "summary": "Mehrere belastende Signale greifen gleichzeitig ineinander und erhöhen die Wahrscheinlichkeit einer klaren Risk-off-Phase.",
            "supportedBy": drivers_for('C'),
            "probabilityChange": {"direction": changes['C'][0], "label": changes['C'][1], "reason": reasons['C'][changes['C'][0]]},
            "investorMeaning": "Für Anleger wäre dieses Bild ein Hinweis auf erhöhte emotionale Disziplin und vorsichtigere Interpretation von Risiken."
        }
    ]


def phase_from_data(data: dict, scenarios: list[dict]) -> dict:
    stress = float(data["indicators"]["tradeStress"]["value"])
    c_prob = next(x["probability"] for x in scenarios if x["id"] == "C")
    b_prob = next(x["probability"] for x in scenarios if x["id"] == "B")
    if stress >= 60 or c_prob >= 35:
        key, label = "stress", "Hohes Stressniveau"
        summary = "Mehrere Signale bestätigen gleichzeitig eine anspruchsvolle Marktphase."
        interpretation = "Das Umfeld wirkt störanfälliger und weniger fehlertolerant."
    elif stress >= 45 or c_prob >= 25:
        key, label = "defensive", "Defensivere Haltung sinnvoll"
        summary = "Die Belastung ist breiter sichtbar. Einzelne Warnsignale greifen spürbar ineinander."
        interpretation = "Der Monitor signalisiert ein zunehmend anspruchsvolles Umfeld."
    elif stress >= 30 or b_prob >= 35:
        key, label = "caution", "Erhöhte Vorsicht"
        summary = "Das Gesamtbild bleibt geordnet, zeigt aber mehr Spannungen als in einer ruhigen Normalphase."
        interpretation = "Der Fokus verschiebt sich von Routine hin zu erhöhter Aufmerksamkeit."
    else:
        key, label = "normal", "Normalphase"
        summary = "Die Signallage wirkt insgesamt geordnet. Einzelne Belastungen dominieren das Gesamtbild noch nicht."
        interpretation = "Der Monitor liefert derzeit eher Orientierung als Alarm."
    return {
        "phase": {
            "label": label,
            "score": round(stress, 1),
            "summary": summary,
            "interpretation": interpretation,
            "actionMatrixKey": key,
        },
        "statusLine": {
            "dataAsOf": data.get("fetchedAt", ""),
            "updateCadence": "Gemischt: täglich bis jährlich",
            "systemNote": "Technische Meta-Informationen werden bewusst dezent gehalten."
        }
    }


def enrich(data: dict) -> dict:
    data["product"] = {
        "name": "Marktrisiko-Kompass",
        "version": "PRO",
        "claim": "Verständliche Markt- und Risikosignale für Privatanleger – klar, regelbasiert und ohne Terminal-Overload."
    }
    data.setdefault("meta", {})["schemaVersion"] = "4.0-pro"
    data["meta"]["language"] = "de"
    data["meta"].setdefault("notes", {})["productUse"] = "Der Marktrisiko-Kompass dient der Einordnung von Marktphasen. Er ersetzt keine Anlageberatung."

    copy = indicator_copy()
    for key, meta in copy.items():
        role, conf = signal_role_confidence(key)
        data["indicators"].setdefault(key, {}).update(meta)
        data["indicators"][key]["signalRole"] = role
        data["indicators"][key]["confidence"] = conf

    # Zusatzinfo für Sahm Rule / RECPROUSM156N
    rp = data["indicators"]["recProb"]
    rp["tooltipExtra"] = {
        "recProb": "RECPROUSM156N ist ein modellbasierter Rezessionsindikator. Er zeigt nicht, dass eine Rezession sicher kommt, sondern wie stark das Modell eine konjunkturelle Abschwächung einpreist.",
        "sahmRule": "Die Sahm Rule ist ein schneller Arbeitsmarkt-Frühindikator. Sie reagiert früher, ist aber allein kein fertiges Krisensignal."
    }

    data["signalFramework"] = {
        "roles": [
            {"key": "early_warning", "label": "Frühwarnsignal", "description": "Kann früh Spannungen anzeigen, ist allein aber noch keine saubere Bestätigung."},
            {"key": "confirmation", "label": "Bestätigungssignal", "description": "Bestätigt eine Entwicklung eher, als dass es sie früh ankündigt."},
            {"key": "companion", "label": "Begleitindikator", "description": "Hilft beim Gesamtverständnis, trägt das Urteil aber nicht allein."},
            {"key": "context", "label": "Kontextsignal", "description": "Dient vor allem der Einordnung und sollte nicht isoliert gelesen werden."}
        ],
        "confidenceLevels": [
            {"key": "low", "label": "Niedrigere Belastbarkeit", "description": "Eher fragil, stark schwankend oder nur schwach bestätigt."},
            {"key": "medium", "label": "Mittlere Belastbarkeit", "description": "Brauchbar, aber am besten im Zusammenspiel mit weiteren Signalen."},
            {"key": "high", "label": "Höhere Belastbarkeit", "description": "Über mehrere Datenpunkte oder mehrere Indikatoren gestützt."}
        ]
    }
    data["scenarios"] = scenario_bundle(data)
    data["hero"] = phase_from_data(data, data["scenarios"])
    data["actionMatrix"] = {
        "normal": {"title": "Normalphase", "description": "Die Datenlage wirkt geordnet.", "interpretation": "Signal und Lärm sauber trennen.", "neutralConsequence": "Fokus auf Regelwerk und Langfristigkeit."},
        "caution": {"title": "Erhöhte Vorsicht", "description": "Mehrere Signale verschlechtern sich.", "interpretation": "Die Aufmerksamkeit steigt.", "neutralConsequence": "Risikobewusstsein und Liquiditätsklarheit werden wichtiger."},
        "defensive": {"title": "Defensivere Haltung sinnvoll", "description": "Die Belastung wird breiter sichtbar.", "interpretation": "Das Umfeld wirkt weniger fehlertolerant.", "neutralConsequence": "Robustheit und Puffer gewinnen an Bedeutung."},
        "stress": {"title": "Hohes Stressniveau", "description": "Mehrere Signalarten bestätigen eine schwierige Phase.", "interpretation": "Emotionale Fehler werden wahrscheinlicher.", "neutralConsequence": "Priorität auf Stabilität, Disziplin und nüchterne Lesart."}
    }
    data["presets"] = [
        {"key": "defensiv", "label": "Defensiver Anleger", "description": "Gewichtet Stress und Konjunkturschwäche höher.", "weights": {"tradeStress":1.2,"recProb":1.2,"fedRate":1.1,"sentiment":1.0,"sp500":0.9,"inflation":1.0}},
        {"key": "ausgewogen", "label": "Ausgewogener Anleger", "description": "Balanciertes Standardprofil.", "weights": {"tradeStress":1.0,"recProb":1.0,"fedRate":1.0,"sentiment":1.0,"sp500":1.0,"inflation":1.0}},
        {"key": "opportunistisch", "label": "Opportunistischer Anleger", "description": "Weniger Übergewicht auf kurzfristige Ausschläge.", "weights": {"tradeStress":0.9,"recProb":1.0,"fedRate":1.0,"sentiment":0.9,"sp500":0.8,"inflation":1.0}},
        {"key": "etf_langfristig", "label": "Langfristiger ETF-Anleger", "description": "Fokussiert stärker auf Marktphase als auf Tagesrauschen.", "weights": {"tradeStress":0.9,"recProb":1.1,"fedRate":1.0,"sentiment":0.8,"sp500":0.8,"inflation":1.0}},
        {"key": "dividende", "label": "Einkommens- und Dividendenanleger", "description": "Beobachtet Zinsen und Stress etwas sensibler.", "weights": {"tradeStress":1.1,"recProb":1.1,"fedRate":1.1,"sentiment":0.9,"sp500":0.9,"inflation":1.0}}
    ]
    data["onboarding"] = {
        "title": "So liest du den Marktrisiko-Kompass",
        "steps": [
            {"title": "1. Gesamtphase", "text": "Zuerst die Gesamtphase lesen, nicht einzelne Ausschläge."},
            {"title": "2. Szenarien", "text": "Danach prüfen, welches Szenario aktuell gestützt wird und warum."},
            {"title": "3. Signalqualität", "text": "Erst dann Rollen- und Belastbarkeitslogik der Einzelindikatoren lesen."},
            {"title": "4. Kontext sauber einordnen", "text": "Nachrichten und Einzelwerte liefern Kontext, nicht automatisch Handlungsdruck."}
        ]
    }
    data["disclaimer"] = {
        "short": "Der Marktrisiko-Kompass dient der Einordnung von Marktphasen und ist keine Anlageberatung.",
        "long": "Dieses Produkt dient ausschließlich der strukturierten Einordnung von Markt-, Makro- und Stresssignalen. Es stellt weder eine individuelle Anlageberatung noch eine Kauf- oder Verkaufsempfehlung dar."
    }
    return data


def main() -> int:
    # Write legacy script to temp and run it unchanged, but redirect the output target.
    tmp_script = LEGACY_SCRIPT
    content = BASE_SCRIPT.replace('OUT = Path("data/latest.json")', f'OUT = Path(r"{LEGACY_OUT}")')
    tmp_script.write_text(content, encoding='utf-8')
    env = os.environ.copy()
    result = subprocess.run([sys.executable, str(tmp_script)], env=env)
    if result.returncode != 0:
        return result.returncode
    base_data = json.loads(LEGACY_OUT.read_text(encoding='utf-8'))
    pro_data = enrich(base_data)
    FINAL_OUT.write_text(json.dumps(pro_data, indent=2, ensure_ascii=False), encoding='utf-8')
    print(f"✓ Wrote {FINAL_OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
