"""Daily macro-relevant news intensity from GDELT 2.0 GKG.

The Global Knowledge Graph tags every news article with theme codes
(ECON_*, WB_*, etc.). For each day we download all 96 GKG 15-min files,
count articles tagged with our priority macro themes, and write two
signals per theme to the `signals` table under the pseudo-symbol `_news`:

    news_count_<theme>     absolute article count that day
    news_rate_<theme>      count / total articles that day  (0..1)
    news_total_articles    total articles processed that day

Rates are stable across days; counts are useful for spotting volume
spikes. The previous row-level GDELT events feed (clustered news mentions
masquerading as named events) is dropped — the events table is now seeded
only by curated regime events, ACLED conflicts, and central-bank
calendar releases. GKG's contribution is purely quantitative.
"""
from __future__ import annotations

import io
import argparse
import hashlib
import json
import math
import os
import sqlite3
import zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from urllib.parse import urlparse

try:
    import requests
except ImportError:  # pragma: no cover - fetch path reports this explicitly.
    requests = None

from config.config_fetch import DB_PATH, log
from config.db_setup import SCHEMA, add_missing_columns
from src.core.changes import enqueue_change_event
from src.core.db import connect_writable


GDELT_BASE = "http://data.gdeltproject.org/gdeltv2"
DEFAULT_WORKERS = 0
MAX_EXAMPLES_PER_BUCKET = 3
MAX_EXAMPLES_PER_DAY = 1_500
MAX_REVIEWS_PER_DAY = 250

# (GKG theme code, short label used in signal_name)
PRIORITY_THEMES: dict[str, str] = {
    "ECON_INFLATION":              "inflation",
    "ECON_INTEREST_RATES":         "interest_rates",
    "ECON_CENTRALBANK":            "central_bank",
    "WB_444_MONETARY_POLICY":      "monetary_policy",
    "ECON_BANKRUPTCY":             "bankruptcy",
    "WB_1234_BANKING_INSTITUTIONS":"banking",
    "ECON_OILPRICE":               "oil_price",
    "ECON_HOUSING_PRICES":         "housing",
    "ECON_DEBT":                   "debt",
    "ECON_CURRENCY_EXCHANGE_RATE": "currency",
    "ECON_STOCKMARKET":            "stockmarket",
    "TERROR":                      "terror",
}

# GKG column indices
COL_DATE = 1
COL_SOURCE_DOMAIN = 3
COL_THEMES = 7  # V1THEMES, semicolon-separated
COL_DOCUMENT = 4
COL_LOCATIONS = 9  # V1LOCATIONS, semicolon-separated type#name#country#adm1#lat#lon#feature
COL_TONE = 15

GDELT_STREAMS: tuple[str, ...] = (
    "economy_news",
    "policy_rates",
    "major_disaster",
    "political_risk",
    "conflict_security",
    "trade_sanctions_supply",
    "energy_commodities",
    "market_stress",
)

STREAM_THEME_RULES: dict[str, tuple[str, ...]] = {
    "economy_news": (
        "ECON_INFLATION", "ECON_DEBT", "ECON_BANKRUPTCY",
        "ECON_HOUSING_PRICES", "ECON_UNEMPLOYMENT", "ECON_TAXATION",
        "WB_470_INFLATION", "WB_1101_MACROECONOMIC_VULNERABILITY_AND_DEBT",
    ),
    "policy_rates": (
        "ECON_INTEREST_RATES", "ECON_CENTRALBANK", "WB_444_MONETARY_POLICY",
        "WB_445_CENTRAL_BANKS", "WB_467_FINANCIAL_SECTOR_POLICY",
    ),
    "major_disaster": (
        "NATURAL_DISASTER", "NATURAL_DISASTER_EARTHQUAKE",
        "NATURAL_DISASTER_FLOOD", "NATURAL_DISASTER_FLASH_FLOOD",
        "NATURAL_DISASTER_STORM", "NATURAL_DISASTER_HURRICANE",
        "NATURAL_DISASTER_TYPHOON", "NATURAL_DISASTER_WILDFIRE",
        "NATURAL_DISASTER_FOREST_FIRE", "NATURAL_DISASTER_DROUGHT",
        "NATURAL_DISASTER_LANDSLIDE", "NATURAL_DISASTER_TSUNAMI",
        "NATURAL_DISASTER_VOLCANO",
    ),
    "political_risk": (
        "ELECTION", "TAX_FNCACT_POLITICIAN", "LEGISLATION",
        "POLITICAL_TURMOIL", "WB_696_PUBLIC_SECTOR_MANAGEMENT",
        "WB_845_POLITICAL_ECONOMY",
    ),
    "conflict_security": (
        "TERROR", "MILITARY", "WAR", "ARMEDCONFLICT", "SECURITY_SERVICES",
        "WB_2432_FRAGILITY_CONFLICT_AND_VIOLENCE",
    ),
    "trade_sanctions_supply": (
        "SANCTIONS", "ECON_SANCTIONS", "TRADE", "ECON_TRADE_DISPUTE",
        "WB_1332_TRADE", "SUPPLY_CHAIN", "MARITIME",
    ),
    "energy_commodities": (
        "ECON_OILPRICE", "ENERGY", "OIL", "GAS", "WB_1015_ENERGY",
        "WB_827_AGRICULTURE", "FOOD_SECURITY",
    ),
    "market_stress": (
        "ECON_STOCKMARKET", "WB_1234_BANKING_INSTITUTIONS",
        "ECON_BANKRUPTCY", "ECON_CURRENCY_EXCHANGE_RATE",
        "FINANCIAL_MARKETS", "ECON_CREDIT", "ECON_FORECLOSURE",
    ),
}

STREAM_LABEL_IDS: dict[str, list[str]] = {
    "economy_news": [
        "stream:economy_news", "theme:growth", "theme:macro_news",
        "asset_impact:sp500",
    ],
    "policy_rates": [
        "stream:policy_rates", "theme:central_bank", "theme:rates",
        "theme:macro_news", "theme:market_moving_news",
        "asset_impact:sp500", "asset_impact:gold",
    ],
    "major_disaster": ["stream:major_disaster", "theme:disaster"],
    "political_risk": ["stream:political_risk", "theme:political"],
    "conflict_security": [
        "stream:conflict_security", "theme:conflict",
        "theme:market_moving_news", "asset_impact:gold",
    ],
    "trade_sanctions_supply": [
        "stream:trade_sanctions_supply", "theme:sanctions", "theme:trade",
        "theme:market_moving_news", "asset_impact:oil",
    ],
    "energy_commodities": [
        "stream:energy_commodities", "theme:energy", "theme:oil_price",
        "theme:market_moving_news", "asset_impact:oil",
    ],
    "market_stress": [
        "stream:market_stress", "theme:stockmarket", "theme:banking",
        "theme:market_moving_news", "asset_impact:sp500", "asset_impact:gold",
    ],
}

MACRO_NEWS_STREAMS = {"economy_news", "policy_rates"}
MARKET_MOVING_STREAMS = {
    "policy_rates", "conflict_security", "trade_sanctions_supply",
    "energy_commodities", "market_stress",
}
MACRO_THEME_MARKERS = (
    "ECON_INFLATION", "ECON_UNEMPLOYMENT", "ECON_CENTRALBANK",
    "ECON_INTEREST_RATES", "WB_470_INFLATION", "WB_444_MONETARY_POLICY",
    "WB_445_CENTRAL_BANKS", "ECON_HOUSING_PRICES",
)
MARKET_THEME_MARKERS = (
    "ECON_STOCKMARKET", "FINANCIAL_MARKETS", "ECON_CURRENCY_EXCHANGE_RATE",
    "ECON_CREDIT", "ECON_BANKRUPTCY", "ECON_OILPRICE",
    "SANCTIONS", "SUPPLY_CHAIN", "TERROR", "WAR", "ARMEDCONFLICT",
)
OIL_THEME_MARKERS = ("ECON_OILPRICE", "ENERGY", "OIL", "GAS", "SUPPLY_CHAIN", "MARITIME")
GOLD_THEME_MARKERS = (
    "ECON_INTEREST_RATES", "ECON_CENTRALBANK", "ECON_INFLATION",
    "FINANCIAL_MARKETS", "TERROR", "WAR", "ARMEDCONFLICT",
)
SP500_THEME_MARKERS = (
    "ECON_STOCKMARKET", "FINANCIAL_MARKETS", "ECON_CREDIT",
    "ECON_BANKRUPTCY", "ECON_CENTRALBANK", "ECON_INFLATION",
)


def _themes_match_any(top_themes: list[str], markers: tuple[str, ...]) -> bool:
    for theme in top_themes:
        upper = str(theme or "").upper()
        if any(marker in upper for marker in markers):
            return True
    return False


def _stream_label_ids(stream: str, top_themes: list[str]) -> list[str]:
    labels: list[str] = []

    def add(label: str) -> None:
        if label and label not in labels:
            labels.append(label)

    for label in STREAM_LABEL_IDS.get(stream, [f"stream:{stream}"]):
        add(label)
    if stream in MACRO_NEWS_STREAMS or _themes_match_any(top_themes, MACRO_THEME_MARKERS):
        add("theme:macro_news")
    if stream in MARKET_MOVING_STREAMS or _themes_match_any(top_themes, MARKET_THEME_MARKERS):
        add("theme:market_moving_news")
    if stream in {"energy_commodities", "trade_sanctions_supply"} or _themes_match_any(top_themes, OIL_THEME_MARKERS):
        add("asset_impact:oil")
    if stream in {"policy_rates", "conflict_security", "market_stress"} or _themes_match_any(top_themes, GOLD_THEME_MARKERS):
        add("asset_impact:gold")
    if stream in {"economy_news", "policy_rates", "market_stress"} or _themes_match_any(top_themes, SP500_THEME_MARKERS):
        add("asset_impact:sp500")
    return labels

STREAM_IMPACT_WEIGHTS: dict[str, float] = {
    "economy_news": 0.9,
    "policy_rates": 1.05,
    "major_disaster": 1.2,
    "political_risk": 1.0,
    "conflict_security": 1.25,
    "trade_sanctions_supply": 1.1,
    "energy_commodities": 1.15,
    "market_stress": 1.2,
}

COUNTRY_REGION = {
    "US": "US", "USA": "US",
    "SE": "Nordics", "SW": "Nordics", "NO": "Nordics", "FI": "Nordics", "DA": "Nordics",
    "DE": "EU", "FR": "EU", "IT": "EU", "ES": "EU", "NL": "EU", "BE": "EU", "PL": "EU",
    "UK": "Europe", "GB": "Europe", "EI": "Europe", "CH": "Europe",
    "JP": "Asia", "JA": "Asia", "CN": "Asia", "CHN": "Asia", "IN": "Asia", "KR": "Asia",
    "HK": "Asia", "TW": "Asia", "ID": "Asia", "TH": "Asia", "VM": "Asia",
    "IR": "Middle East", "IZ": "Middle East", "IS": "Middle East", "SA": "Middle East",
    "AE": "Middle East", "YM": "Middle East", "SY": "Middle East", "JO": "Middle East",
    "CA": "North America", "MX": "North America",
    "BR": "Latin America", "AR": "Latin America", "CI": "Latin America",
    "SF": "Africa", "NI": "Africa", "EG": "Africa", "ET": "Africa",
    "AS": "Oceania", "NZ": "Oceania",
}

COUNTRY_IMPORTANCE_WEIGHTS = {
    "US": 1.15, "CN": 1.12, "CHN": 1.12, "JP": 1.05, "JA": 1.05,
    "DE": 1.05, "FR": 1.03, "GB": 1.03, "UK": 1.03, "SE": 1.0,
    "IR": 1.12, "SA": 1.10, "AE": 1.05, "YM": 1.05, "UA": 1.10,
}

DISASTER_THEME_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("earthquake", ("NATURAL_DISASTER_EARTHQUAKE",)),
    ("flood", ("NATURAL_DISASTER_FLOOD", "NATURAL_DISASTER_FLASH_FLOOD")),
    ("storm", ("NATURAL_DISASTER_STORM", "NATURAL_DISASTER_HURRICANE", "NATURAL_DISASTER_TYPHOON")),
    ("wildfire", ("NATURAL_DISASTER_WILDFIRE", "NATURAL_DISASTER_FOREST_FIRE")),
    ("drought", ("NATURAL_DISASTER_DROUGHT",)),
    ("landslide", ("NATURAL_DISASTER_LANDSLIDE",)),
    ("tsunami", ("NATURAL_DISASTER_TSUNAMI",)),
    ("volcano", ("NATURAL_DISASTER_VOLCANO",)),
)

NOISY_DISASTER_TYPES = {
    "avalanche", "chill", "cold", "drowned", "drowning", "dust", "erosion",
    "fog", "freeze", "hail", "heat", "ice", "lightning", "monsoon", "rain",
    "snow", "strong_winds", "tornado", "wind",
}

DISASTER_TYPE_ALIASES = {
    "high_winds": "storm",
    "strong_winds": "storm",
    "severe_weather": "storm",
    "volcanic": "volcano",
}

DISASTER_TYPE_WEIGHTS = {
    "earthquake": 1.25,
    "flood": 1.10,
    "storm": 1.05,
    "wildfire": 1.10,
    "drought": 1.05,
    "landslide": 1.00,
    "tsunami": 1.25,
    "volcano": 1.10,
    "natural_disaster": 0.90,
}


def list_gkg_files_for(d: date) -> list[str]:
    out = []
    for h in range(24):
        for m in (0, 15, 30, 45):
            stamp = f"{d.year:04d}{d.month:02d}{d.day:02d}{h:02d}{m:02d}00"
            out.append(f"{GDELT_BASE}/{stamp}.gkg.csv.zip")
    return out


def parse_zip(content: bytes) -> list[list[str]]:
    zf = zipfile.ZipFile(io.BytesIO(content))
    name = zf.namelist()[0]
    text = zf.read(name).decode("utf-8", errors="replace")
    return [line.split("\t") for line in text.splitlines() if line]


def _disaster_type(themes: set[str]) -> str | None:
    for label, prefixes in DISASTER_THEME_RULES:
        if any(any(theme.startswith(prefix) for prefix in prefixes) for theme in themes):
            return label
    for theme in themes:
        if theme.startswith("NATURAL_DISASTER_"):
            raw = theme.removeprefix("NATURAL_DISASTER_").lower()
            return DISASTER_TYPE_ALIASES.get(raw, raw)
    if "NATURAL_DISASTER" in themes:
        return "natural_disaster"
    return None


def _is_noisy_disaster_type(disaster_type: str | None) -> bool:
    return str(disaster_type or "").lower() in NOISY_DISASTER_TYPES


def _location(row: list[str]) -> tuple[str, float | None, float | None, str]:
    if len(row) <= COL_LOCATIONS or not row[COL_LOCATIONS]:
        return "", None, None, ""
    for block in row[COL_LOCATIONS].split(";"):
        parts = block.split("#")
        if len(parts) < 6:
            continue
        country = parts[2].strip()
        if not country:
            continue
        try:
            lat = float(parts[4]) if parts[4] else None
            lon = float(parts[5]) if parts[5] else None
        except ValueError:
            lat, lon = None, None
        return country, lat, lon, parts[1].strip()
    return "", None, None, ""


def _theme_matches(theme: str, rule: str) -> bool:
    return theme == rule or theme.startswith(f"{rule}_")


def classify_streams(themes: set[str]) -> list[str]:
    """Map GKG theme codes to deterministic atlas stream names."""
    streams: list[str] = []
    for stream, rules in STREAM_THEME_RULES.items():
        if any(_theme_matches(theme, rule) for theme in themes for rule in rules):
            streams.append(stream)
    return streams


def _region_for_country(country: str | None) -> str:
    if not country:
        return "Global"
    key = country.strip().upper()
    return COUNTRY_REGION.get(key, "Other")


def _stream_scopes(country: str, region: str) -> list[tuple[str, str]]:
    scopes = [("Global", "")]
    if region and region != "Global":
        scopes.append((region, ""))
    if country:
        scopes.append((region or "Other", country))
    return scopes


def _source_domain(row: list[str]) -> str:
    if len(row) > COL_SOURCE_DOMAIN and row[COL_SOURCE_DOMAIN]:
        return row[COL_SOURCE_DOMAIN]
    doc = row[COL_DOCUMENT] if len(row) > COL_DOCUMENT else ""
    if doc.startswith("http"):
        return urlparse(doc).netloc
    return ""


def _tone(row: list[str]) -> float | None:
    if len(row) <= COL_TONE or not row[COL_TONE]:
        return None
    try:
        return float(str(row[COL_TONE]).split(",", maxsplit=1)[0])
    except ValueError:
        return None


def _example(row: list[str], stream: str, themes: set[str], country: str, region: str, place: str) -> dict:
    doc = row[COL_DOCUMENT] if len(row) > COL_DOCUMENT else ""
    domain = _source_domain(row)
    loc = place or country or region or "Global"
    title = f"{domain or 'GDELT'}: {stream.replace('_', ' ')} signal near {loc}"
    return {
        "title": title[:240],
        "url": doc if str(doc).startswith("http") else "",
        "source_domain": domain,
        "location_name": place,
        "theme_codes": sorted(themes)[:24],
        "tone": _tone(row),
    }


def _add_stream_hit(
    streams: dict[tuple[str, str, str], dict],
    stream: str,
    region: str,
    country: str,
    themes: set[str],
    example: dict,
) -> None:
    for scope_region, scope_country in _stream_scopes(country, region):
        key = (stream, scope_region, scope_country)
        item = streams.setdefault(
            key,
            {"count": 0, "themes": Counter(), "examples": []},
        )
        item["count"] += 1
        item["themes"].update(themes)
        if len(item["examples"]) < 10:
            seen = {e.get("url") or e.get("title") for e in item["examples"]}
            token = example.get("url") or example.get("title")
            if token and token not in seen:
                item["examples"].append(example)


def count_rows(rows: list[list[str]]) -> tuple[int, dict[str, int], dict[tuple[str, str], dict], dict[tuple[str, str, str], dict]]:
    counts = {label: 0 for label in PRIORITY_THEMES.values()}
    disasters: dict[tuple[str, str], dict] = {}
    streams: dict[tuple[str, str, str], dict] = {}
    total = 0
    for row in rows:
        if len(row) <= COL_THEMES:
            continue
        total += 1
        themes_field = row[COL_THEMES]
        if not themes_field:
            continue
        themes = set(themes_field.split(";"))
        for code, label in PRIORITY_THEMES.items():
            if code in themes:
                counts[label] += 1
        disaster_type = _disaster_type(themes)
        country, lat, lon, place = _location(row)
        region = _region_for_country(country)
        disaster_is_noisy = _is_noisy_disaster_type(disaster_type)
        for stream in classify_streams(themes):
            _add_stream_hit(streams, stream, region, country, themes, _example(row, stream, themes, country, region, place))
        if disaster_type:
            if not country:
                continue
            key = (country, disaster_type)
            item = disasters.setdefault(
                key,
                {"count": 0, "lat": lat, "lon": lon, "examples": []},
            )
            item["count"] += 1
            if item["lat"] is None and lat is not None:
                item["lat"] = lat
                item["lon"] = lon
            if len(item["examples"]) < 3:
                doc = row[COL_DOCUMENT] if len(row) > COL_DOCUMENT else ""
                if not disaster_is_noisy:
                    item["examples"].append(place or doc or country)
    return total, counts, disasters, streams


def fetch_file_counts(url: str) -> tuple[int, dict[str, int], dict[tuple[str, str], dict], dict[tuple[str, str, str], dict], str | None]:
    empty = {label: 0 for label in PRIORITY_THEMES.values()}
    if requests is None:
        return 0, empty, {}, {}, "requests is not installed"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 404:
            return 0, empty, {}, {}, None
        r.raise_for_status()
        total, counts, disasters, streams = count_rows(parse_zip(r.content))
        return total, counts, disasters, streams, None
    except (requests.RequestException, zipfile.BadZipFile, IndexError) as e:
        return 0, empty, {}, {}, str(e)


def fetch_day(d: date, workers: int = DEFAULT_WORKERS) -> tuple[int, dict[str, int], dict[tuple[str, str], dict], dict[tuple[str, str, str], dict]]:
    """Return (total_articles, {theme_label: count}) for one day."""
    counts = {label: 0 for label in PRIORITY_THEMES.values()}
    disasters: dict[tuple[str, str], dict] = {}
    streams: dict[tuple[str, str, str], dict] = {}
    total = 0
    errors = 0
    urls = list_gkg_files_for(d)
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(fetch_file_counts, url) for url in urls]
        for fut in as_completed(futures):
            file_total, file_counts, file_disasters, file_streams, err = fut.result()
            if err:
                errors += 1
                continue
            total += file_total
            for label, count in file_counts.items():
                counts[label] += count
            for key, item in file_disasters.items():
                dest = disasters.setdefault(
                    key,
                    {"count": 0, "lat": item["lat"], "lon": item["lon"], "examples": []},
                )
                dest["count"] += item["count"]
                if dest["lat"] is None and item["lat"] is not None:
                    dest["lat"] = item["lat"]
                    dest["lon"] = item["lon"]
                for example in item["examples"]:
                    if len(dest["examples"]) < 3 and example not in dest["examples"]:
                        dest["examples"].append(example)
            for key, item in file_streams.items():
                dest = streams.setdefault(
                    key,
                    {"count": 0, "themes": Counter(), "examples": []},
                )
                dest["count"] += item["count"]
                dest["themes"].update(item["themes"])
                for example in item["examples"]:
                    if len(dest["examples"]) < 10:
                        token = example.get("url") or example.get("title")
                        seen = {e.get("url") or e.get("title") for e in dest["examples"]}
                        if token and token not in seen:
                            dest["examples"].append(example)
    if errors:
        log(f"  {d}: skipped {errors}/{len(urls)} GKG files.", module="gdelt")
    return total, counts, disasters, streams


def _hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _baseline_30d(
    conn: sqlite3.Connection,
    iso: str,
    stream: str,
    region: str,
    country: str,
) -> tuple[float | None, float | None]:
    rows = conn.execute(
        """SELECT article_share
           FROM gdelt_streams
           WHERE stream = ? AND region = ? AND country = ?
             AND date >= date(?, '-30 days') AND date < ?
             AND article_share IS NOT NULL""",
        (stream, region, country, iso, iso),
    ).fetchall()
    values = [float(r[0]) for r in rows if r[0] is not None]
    if not values:
        return None, None
    avg = sum(values) / len(values)
    if len(values) < 2:
        return avg, None
    var = sum((v - avg) ** 2 for v in values) / len(values)
    return avg, math.sqrt(var)


def _persistence_bonus(conn: sqlite3.Connection, iso: str, stream: str, region: str, country: str) -> float:
    rows = conn.execute(
        """SELECT COUNT(DISTINCT date)
           FROM gdelt_streams
           WHERE stream = ? AND region = ? AND country = ?
             AND date >= date(?, '-6 days') AND date < ?
             AND article_count > 0""",
        (stream, region, country, iso, iso),
    ).fetchone()
    days = int(rows[0] or 0) if rows else 0
    return min(0.35, days * 0.07)


def _stream_scores(
    conn: sqlite3.Connection,
    iso: str,
    stream: str,
    region: str,
    country: str,
    count: int,
    total: int,
) -> tuple[float, float | None, float | None, float, float]:
    share = count / total if total > 0 else 0.0
    baseline, std = _baseline_30d(conn, iso, stream, region, country)
    z_score = (share - baseline) / std if baseline is not None and std and std > 0 else None
    ratio = (share / baseline) if baseline and baseline > 0 else 1.0
    count_component = math.log10(max(count, 0) + 1.0)
    z_component = max(float(z_score or 0.0), 0.0)
    ratio_component = max(ratio - 1.0, 0.0)
    severity = 0.55 + count_component * 0.85 + min(z_component, 3.0) * 0.45 + min(ratio_component, 2.0) * 0.6
    if stream in {"major_disaster", "conflict_security", "market_stress"}:
        severity += 0.20
    severity = round(max(0.1, min(5.0, severity)), 2)

    location_quality = 1.10 if country else (0.95 if region != "Global" else 0.85)
    country_weight = COUNTRY_IMPORTANCE_WEIGHTS.get(country.upper(), 1.0) if country else 1.0
    persistence = _persistence_bonus(conn, iso, stream, region, country)
    impact = severity * STREAM_IMPACT_WEIGHTS.get(stream, 1.0) * location_quality * country_weight * (1.0 + persistence)
    impact = round(max(0.0, min(5.0, impact)), 2)
    return share, baseline, z_score, severity, impact


def _disaster_signal_quality(stream: str, top_themes: list[str], count: int, severity: float, impact: float, country: str) -> bool:
    if stream != "major_disaster":
        return True
    disaster_types = {_disaster_type({theme}) for theme in top_themes}
    disaster_types.discard(None)
    if not disaster_types:
        return True
    if any(not _is_noisy_disaster_type(t) for t in disaster_types):
        return True
    return bool(country and count >= 25 and severity >= 3.5 and impact >= 3.5)


def _should_store_examples(stream: str, count: int, severity: float, impact: float) -> bool:
    if count >= 5:
        return True
    if severity >= 3.25:
        return True
    return stream in {"major_disaster", "conflict_security", "trade_sanctions_supply"} and count >= 3 and impact >= 2.5


def _needs_oracle_review(stream: str, count: int, z_score: float | None, severity: float, impact: float, region: str, country: str) -> bool:
    if severity >= 4.25 or impact >= 4.25:
        return True
    if z_score is not None and z_score >= 3.0 and count >= 10:
        return True
    if stream in {"major_disaster", "conflict_security"} and count >= 20 and impact >= 3.5:
        return True
    return False


def _oracle_review_comment(stream: str, region: str, country: str, count: int, severity: float, impact: float) -> str:
    scope = country or region or "Global"
    stream_label = stream.replace("_", " ")
    return (
        f"Review candidate: {stream_label} cluster for {scope}. "
        f"Article count {count:,}, severity {severity:.2f}, societal impact {impact:.2f}. "
        "Deterministic routing only; use review to check whether the cluster has real macro or market linkage."
    )


def _store_streams(
    conn: sqlite3.Connection,
    iso: str,
    total: int,
    streams: dict[tuple[str, str, str], dict],
) -> tuple[int, int]:
    aggregate_rows = []
    example_candidates = []
    review_candidates = []

    for (stream, region, country), item in streams.items():
        count = int(item["count"])
        if count <= 0:
            continue
        share, baseline, z_score, severity, impact = _stream_scores(
            conn, iso, stream, region, country, count, total,
        )
        top_themes = [theme for theme, _n in item["themes"].most_common(12)]
        labels = _stream_label_ids(stream, top_themes)
        if stream == "major_disaster":
            disaster_types = [_disaster_type({theme}) for theme in top_themes]
            type_weight = max(
                DISASTER_TYPE_WEIGHTS.get(t or "", 0.75 if _is_noisy_disaster_type(t) else 0.9)
                for t in disaster_types
            ) if disaster_types else 0.9
            severity = round(max(0.1, min(5.0, severity * type_weight)), 2)
            impact = round(max(0.0, min(5.0, impact * type_weight)), 2)
        aggregate_rows.append((
            iso, stream, region, country, count, total, share, baseline, z_score,
            severity, impact, json.dumps(labels), json.dumps(top_themes),
        ))
        signal_quality = _disaster_signal_quality(stream, top_themes, count, severity, impact, country)
        if signal_quality and _should_store_examples(stream, count, severity, impact):
            priority = (impact * 1000) + (severity * 100) + max(float(z_score or 0), 0) * 10 + math.log10(count + 1)
            for rank, example in enumerate(item["examples"][:MAX_EXAMPLES_PER_BUCKET], start=1):
                example_candidates.append((
                    priority,
                    (
                        iso, stream, region, country, rank, example.get("title"),
                        example.get("url"), example.get("source_domain"),
                        example.get("location_name"), json.dumps(example.get("theme_codes") or []),
                        json.dumps(labels), example.get("tone"),
                    ),
                ))
        if signal_quality and _needs_oracle_review(stream, count, z_score, severity, impact, region, country):
            payload = {
                "as_of": iso, "stream": stream, "region": region, "country": country,
                "count": count, "severity": severity, "impact": impact, "z_score": z_score,
                "labels": labels,
            }
            priority = (impact * 1000) + (severity * 100) + max(float(z_score or 0), 0) * 10 + math.log10(count + 1)
            review_candidates.append((priority, payload, _oracle_review_comment(stream, region, country, count, severity, impact)))

    before = conn.total_changes
    conn.executemany(
        """INSERT INTO gdelt_streams (
             date, stream, region, country, article_count, total_articles,
             article_share, baseline_30d, z_score, severity,
             societal_impact_score, labels_json, top_theme_codes_json
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(date, stream, region, country) DO UPDATE SET
             article_count=excluded.article_count,
             total_articles=excluded.total_articles,
             article_share=excluded.article_share,
             baseline_30d=excluded.baseline_30d,
             z_score=excluded.z_score,
             severity=excluded.severity,
             societal_impact_score=excluded.societal_impact_score,
             labels_json=excluded.labels_json,
             top_theme_codes_json=excluded.top_theme_codes_json,
             fetched_at=datetime('now')""",
        aggregate_rows,
    )
    example_rows = [
        row for _priority, row in sorted(example_candidates, key=lambda item: item[0], reverse=True)[:MAX_EXAMPLES_PER_DAY]
    ]
    conn.execute("DELETE FROM gdelt_stream_examples WHERE date = ?", (iso,))
    if example_rows:
        conn.executemany(
            """INSERT INTO gdelt_stream_examples (
                 date, stream, region, country, example_rank, title, url,
                 source_domain, location_name, theme_codes_json, labels_json, tone
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(date, stream, region, country, example_rank) DO UPDATE SET
                 title=excluded.title,
                 url=excluded.url,
                 source_domain=excluded.source_domain,
                 location_name=excluded.location_name,
                 theme_codes_json=excluded.theme_codes_json,
                 labels_json=excluded.labels_json,
                 tone=excluded.tone,
                 fetched_at=datetime('now')""",
            example_rows,
        )
    conn.execute(
        """DELETE FROM oracle_review_annotations
           WHERE source_table = 'gdelt_streams'
             AND review_type = 'high_impact_stream'
             AND source_id IN (SELECT id FROM gdelt_streams WHERE date = ?)""",
        (iso,),
    )
    review_rows = [
        (payload, comment)
        for _priority, payload, comment in sorted(review_candidates, key=lambda item: item[0], reverse=True)[:MAX_REVIEWS_PER_DAY]
    ]
    for payload, comment in review_rows:
        source_id = conn.execute(
            """SELECT id FROM gdelt_streams
               WHERE date = ? AND stream = ? AND region = ? AND country = ?""",
            (payload["as_of"], payload["stream"], payload["region"], payload["country"]),
        ).fetchone()
        source_id_value = source_id[0] if source_id else None
        conn.execute(
            """INSERT INTO oracle_review_annotations (
                 source_table, source_id, as_of, review_type, severity,
                 confidence, comment, input_hash
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(source_table, source_id, review_type, model) DO UPDATE SET
                 severity=excluded.severity,
                 confidence=excluded.confidence,
                 comment=excluded.comment,
                 input_hash=excluded.input_hash,
                 created_at=datetime('now')""",
            (
                "gdelt_streams", source_id_value,
                payload["as_of"], "high_impact_stream",
                payload["severity"], min(0.95, 0.55 + payload["severity"] / 10.0),
                comment, _hash(payload),
            ),
        )
        enqueue_change_event(
            conn,
            object_id="gdelt.stream",
            source_table="gdelt_streams",
            source_id=source_id_value,
            event_type="high_impact_cluster",
            priority=max(float(payload["severity"] or 0), float(payload["impact"] or 0)),
            labels=payload.get("labels") or [f"stream:{payload['stream']}"],
            metadata=payload,
            event_key=f"gdelt.stream:{payload['as_of']}:{payload['stream']}:{payload['region']}:{payload['country']}",
            oracle_review_required=True,
        )
    return conn.total_changes - before, len(review_rows)


def store(
    conn: sqlite3.Connection,
    d: date,
    total: int,
    counts: dict[str, int],
    disasters: dict[tuple[str, str], dict],
    streams: dict[tuple[str, str, str], dict],
) -> int:
    iso = d.isoformat()
    rows = [(iso, "_news", "news_total_articles", float(total))]
    for label, count in counts.items():
        rate = count / total if total > 0 else 0.0
        rows.append((iso, "_news", f"news_count_{label}", float(count)))
        rows.append((iso, "_news", f"news_rate_{label}", rate))
    before = conn.total_changes
    # Replace any prior values for this date+symbol+name (counts can drift
    # if a backfill day's files are still being added).
    conn.executemany(
        """INSERT INTO signals (date, symbol, signal_name, value)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(date, symbol, signal_name) DO UPDATE SET value = excluded.value""",
        rows,
    )
    disaster_rows = [
        (
            iso, country, disaster_type, int(item["count"]), total,
            item["lat"], item["lon"], " | ".join(item["examples"]),
        )
        for (country, disaster_type), item in disasters.items()
        if int(item["count"]) >= 2
    ]
    conn.executemany(
        """INSERT INTO gdelt_disaster_signals
           (date, country, disaster_type, article_count, total_articles,
            lat, lon, examples)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(date, country, disaster_type) DO UPDATE SET
             article_count=excluded.article_count,
             total_articles=excluded.total_articles,
             lat=excluded.lat,
             lon=excluded.lon,
             examples=excluded.examples,
             fetched_at=datetime('now')""",
        disaster_rows,
    )
    stream_changes, reviews = _store_streams(conn, iso, total, streams)
    if reviews:
        try:
            log(f"  {iso}: queued {reviews} high-impact GDELT stream review annotation(s).", module="gdelt")
        except OSError:
            pass
    return conn.total_changes - before


def last_news_date(conn: sqlite3.Connection) -> date | None:
    row = conn.execute(
        "SELECT MAX(date) FROM signals WHERE symbol = '_news'"
    ).fetchone()
    if row and row[0]:
        return datetime.strptime(row[0], "%Y-%m-%d").date()
    return None


def _worker_count(workers: int | None) -> int:
    if workers and workers > 0:
        return workers
    return max(4, min(32, os.cpu_count() or 8))


def compact_examples(
    conn: sqlite3.Connection,
    *,
    retain_days: int = 180,
    retain_reviewed_days: int = 730,
    apply: bool = False,
) -> dict[str, int]:
    reviewed_filter = """
        EXISTS (
          SELECT 1
          FROM gdelt_streams s
          JOIN oracle_review_annotations r
            ON r.source_table = 'gdelt_streams'
           AND r.source_id = s.id
           AND r.review_type = 'high_impact_stream'
          WHERE s.date = gdelt_stream_examples.date
            AND s.stream = gdelt_stream_examples.stream
            AND s.region = gdelt_stream_examples.region
            AND s.country = gdelt_stream_examples.country
        )
    """
    old_any = conn.execute(
        """SELECT COUNT(*) FROM gdelt_stream_examples
           WHERE date < date('now', ?)""",
        (f"-{retain_reviewed_days} days",),
    ).fetchone()[0]
    old_unreviewed = conn.execute(
        f"""SELECT COUNT(*) FROM gdelt_stream_examples
            WHERE date < date('now', ?)
              AND NOT ({reviewed_filter})""",
        (f"-{retain_days} days",),
    ).fetchone()[0]
    if not apply:
        return {"old_unreviewed": int(old_unreviewed or 0), "old_any": int(old_any or 0), "deleted": 0}
    before = conn.total_changes
    conn.execute(
        """DELETE FROM gdelt_stream_examples
           WHERE date < date('now', ?)""",
        (f"-{retain_reviewed_days} days",),
    )
    conn.execute(
        f"""DELETE FROM gdelt_stream_examples
            WHERE date < date('now', ?)
              AND NOT ({reviewed_filter})""",
        (f"-{retain_days} days",),
    )
    return {
        "old_unreviewed": int(old_unreviewed or 0),
        "old_any": int(old_any or 0),
        "deleted": conn.total_changes - before,
    }


def enforce_current_caps(
    conn: sqlite3.Connection,
    *,
    days: int | None = None,
    apply: bool = False,
) -> dict[str, int]:
    date_filter = "AND date >= date('now', ?)" if days is not None else ""
    example_params: tuple[object, ...] = (f"-{int(days)} days",) if days is not None else ()
    review_filter = "AND r.as_of >= date('now', ?)" if days is not None else ""
    review_params: tuple[object, ...] = (f"-{int(days)} days",) if days is not None else ()
    example_ids = [
        row[0] for row in conn.execute(
            f"""WITH ranked AS (
                  SELECT e.id,
                         ROW_NUMBER() OVER (
                           PARTITION BY e.date
                           ORDER BY COALESCE(s.societal_impact_score, 0) DESC,
                                    COALESCE(s.severity, 0) DESC,
                                    COALESCE(s.z_score, 0) DESC,
                                    COALESCE(s.article_count, 0) DESC,
                                    e.example_rank ASC,
                                    e.id ASC
                         ) AS rn
                  FROM gdelt_stream_examples e
                  LEFT JOIN gdelt_streams s
                    ON s.date = e.date
                   AND s.stream = e.stream
                   AND s.region = e.region
                   AND s.country = e.country
                  WHERE 1=1 {date_filter}
                )
                SELECT id FROM ranked WHERE rn > ?""",
            (*example_params, MAX_EXAMPLES_PER_DAY),
        )
    ]
    review_ids = [
        row[0] for row in conn.execute(
            f"""WITH ranked AS (
                  SELECT r.id,
                         ROW_NUMBER() OVER (
                           PARTITION BY r.as_of
                           ORDER BY COALESCE(s.societal_impact_score, 0) DESC,
                                    COALESCE(s.severity, 0) DESC,
                                    COALESCE(s.z_score, 0) DESC,
                                    COALESCE(s.article_count, 0) DESC,
                                    r.id ASC
                         ) AS rn
                  FROM oracle_review_annotations r
                  LEFT JOIN gdelt_streams s ON s.id = r.source_id
                  WHERE r.source_table = 'gdelt_streams'
                    AND r.review_type = 'high_impact_stream'
                    {review_filter}
                )
                SELECT id FROM ranked WHERE rn > ?""",
            (*review_params, MAX_REVIEWS_PER_DAY),
        )
    ]
    result = {"over_cap_examples": len(example_ids), "over_cap_reviews": len(review_ids), "deleted": 0}
    if not apply:
        return result
    before = conn.total_changes
    for ids, table in ((example_ids, "gdelt_stream_examples"), (review_ids, "oracle_review_annotations")):
        for i in range(0, len(ids), 900):
            chunk = ids[i:i + 900]
            if not chunk:
                continue
            placeholders = ",".join("?" for _ in chunk)
            conn.execute(f"DELETE FROM {table} WHERE id IN ({placeholders})", chunk)
    result["deleted"] = conn.total_changes - before
    return result


def main(
    cold_start_days: int = 2,
    backfill_days: int | None = None,
    workers: int | None = DEFAULT_WORKERS,
    include_today: bool = False,
):
    conn = connect_writable(DB_PATH)
    conn.executescript(SCHEMA)
    add_missing_columns(conn)
    workers = _worker_count(workers)

    # One-shot cleanup: remove the noisy row-level GDELT events the old
    # fetcher inserted before this redesign.
    legacy = conn.execute(
        "DELETE FROM events WHERE source = 'gdelt'"
    ).rowcount
    if legacy:
        conn.commit()
        log(f"Removed {legacy} legacy [GDELT] events rows.", module="gdelt")

    today = date.today()
    last = last_news_date(conn)
    if backfill_days is not None:
        start = today - timedelta(days=backfill_days)
        log(f"GKG forced backfill: last {backfill_days} day(s).", module="gdelt")
    else:
        start = (last + timedelta(days=1)) if last else (today - timedelta(days=cold_start_days))
    end = today if include_today else today - timedelta(days=1)

    if start > end:
        log("GKG up to date.", module="gdelt")
        conn.close()
        return {"inserted": 0, "latest_source_ts": last.isoformat() if last else None}

    total_inserted = 0
    d = start
    while d <= end:
        log(
            f"GKG fetching {d} ({len(list_gkg_files_for(d))} files, workers={workers})...",
            module="gdelt",
        )
        total, counts, disasters, streams = fetch_day(d, workers=workers)
        inserted = store(conn, d, total, counts, disasters, streams)
        conn.commit()
        log(f"  {d}: {total:,} articles; "
            f"top theme: {max(counts, key=counts.get)}={max(counts.values()):,}; "
            f"disaster markers={len(disasters)}; stream buckets={len(streams)}",
            module="gdelt")
        total_inserted += inserted
        d += timedelta(days=1)

    log(f"Done. +{total_inserted} signal rows.", module="gdelt")
    conn.close()
    return {"inserted": total_inserted, "latest_source_ts": (d - timedelta(days=1)).isoformat()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=None,
        help="Re-fetch and upsert the last N completed days instead of only missing days.",
    )
    parser.add_argument(
        "--cold-start-days",
        type=int,
        default=2,
        help="Days to fetch when no prior GDELT rows exist.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Parallel downloads per day. Defaults to CPU-based auto sizing. GDELT has 96 files/day.",
    )
    parser.add_argument(
        "--include-today",
        action="store_true",
        help="Also fetch today's partial GDELT files. Useful for hourly clients.",
    )
    parser.add_argument("--compact-examples", action="store_true", help="Inspect or apply bounded example retention.")
    parser.add_argument("--enforce-current-caps", action="store_true", help="Inspect or apply per-day example/review caps to existing rows.")
    parser.add_argument("--cap-days", type=int, default=None, help="Only enforce caps for the last N days; defaults to all GDELT rows.")
    parser.add_argument("--retain-example-days", type=int, default=180)
    parser.add_argument("--retain-reviewed-days", type=int, default=730)
    parser.add_argument("--apply", action="store_true", help="Apply compaction. Without this, compaction is a dry run.")
    args = parser.parse_args()
    if args.compact_examples or args.enforce_current_caps:
        conn = connect_writable(DB_PATH)
        try:
            conn.executescript(SCHEMA)
            add_missing_columns(conn)
            result = {}
            if args.compact_examples:
                result["retention"] = compact_examples(
                    conn,
                    retain_days=args.retain_example_days,
                    retain_reviewed_days=args.retain_reviewed_days,
                    apply=args.apply,
                )
            if args.enforce_current_caps:
                result["caps"] = enforce_current_caps(
                    conn,
                    days=args.cap_days,
                    apply=args.apply,
                )
            if args.apply:
                conn.commit()
            log(f"GDELT example compaction: {result}", module="gdelt")
        finally:
            conn.close()
    else:
        main(cold_start_days=args.cold_start_days, backfill_days=args.backfill_days,
             workers=args.workers, include_today=args.include_today)
