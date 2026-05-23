import requests
import sqlite3
import json
import time
from config.config_fetch import DB_PATH, log
from pyscbwrapper import SCB

# Metadata-konfig i YAML-liknande format (kan sparas som JSON eller .yaml vid behov)
SCB_ENDPOINTS_META = [
    {
        "base_url": "https://api.scb.se/OV0104/v1/doris/sv/ssd/PR/PR0101",
        "category": "inflation",
        "impact": "positive"
    },
    {
        "base_url": "https://api.scb.se/OV0104/v1/doris/sv/ssd/AM/AM0101",
        "category": "labour",
        "impact": "negative"
    },
    {
        "base_url": "https://api.scb.se/OV0104/v1/doris/sv/ssd/FM/FM0001",
        "category": "money",
        "impact": "positive"
    },
    {
        "base_url": "https://api.scb.se/OV0104/v1/doris/sv/ssd/NR/NR0103",
        "category": "gdp",
        "impact": "positive"
    },
    {
        "base_url": "https://api.scb.se/OV0104/v1/doris/sv/ssd/NV/NV0101",
        "category": "industry",
        "impact": "positive"
    },
    {
        "base_url": "https://api.scb.se/OV0104/v1/doris/sv/ssd/HA/HA0201",
        "category": "housing",
        "impact": "neutral"
    }
]

def get_leaf_endpoints(base_url, delay=0.5):
    """Gå ner i SCB-trädet och returnera alla faktiska (leaf) endpoints."""
    try:
        time.sleep(delay)
        response = requests.get(base_url)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            log(f"⚠️ Rate limit nådd, väntar 10 sek...", module="scb")
            time.sleep(10)
            return get_leaf_endpoints(base_url, delay)
        else:
            raise

    endpoints = []

    if isinstance(data, list):
        # Vanlig undermappslista
        for item in data:
            url = f"{base_url}/{item['id']}"
            if item.get("leaf", False):
                endpoints.append(url)
            else:
                endpoints.extend(get_leaf_endpoints(url, delay))
    elif isinstance(data, dict) and "variables" in data:
        endpoints.append(base_url)

    return endpoints

def build_query(variables, default_code):
    query = []
    codes = [v['code'] for v in variables]

    if "ContentsCode" in codes:
        query.append({"code": "ContentsCode", "selection": {"filter": "item", "values": [default_code]}})
    if "Region" in codes:
        query.append({"code": "Region", "selection": {"filter": "item", "values": ["00"]}})
    if "Kon" in codes:
        query.append({"code": "Kon", "selection": {"filter": "item", "values": ["1+2"]}})
    if "Alder" in codes:
        query.append({"code": "Alder", "selection": {"filter": "item", "values": ["20-74"]}})
    if "Tid" in codes:
        query.append({"code": "Tid", "selection": {"filter": "all", "values": []}})

    return query

conn = sqlite3.connect(DB_PATH)

for endpoint_meta in SCB_ENDPOINTS_META:
    try:
        leaf_endpoints = get_leaf_endpoints(endpoint_meta["base_url"])

        for endpoint in leaf_endpoints:
            try:
                log(f"Hämtar från {endpoint}...", module="scb")
                metadata = requests.get(endpoint).json()
                variables = metadata.get("variables", [])
                contents = [v for v in variables if v["code"] == "ContentsCode"]
                if not contents:
                    continue

                content_values = contents[0].get("values", [])
                for content_code in content_values:
                    query = build_query(variables, content_code)
                    payload = {"query": query, "response": {"format": "json"}}
                    time.sleep(0.5)  # eller 1.0 om det behövs
                    res = requests.post(endpoint, json=payload)
                    res.raise_for_status()
                    data = res.json()

                    added = 0
                    for entry in data['data']:
                        year_month = entry['key'][-1]
                        value = entry['values'][0]
                        if value != '..':
                            conn.execute("""
                                INSERT OR IGNORE INTO indicators
                                (date, country, category, indicator_name, value, unit, impact)
                                VALUES (?, 'SE', ?, ?, ?, ?, ?)
                            """, (
                                year_month + "-01",
                                endpoint_meta["category"],
                                metadata.get("title", endpoint),
                                float(value),
                                metadata.get("variables", [{}])[-1].get("values", [""])[0],
                                endpoint_meta["impact"]
                            ))
                            added += 1
                    if added:
                        log(f"{metadata.get('title', endpoint)}: {added} datapunkter lagrade.", module="scb")

            except Exception as inner_e:
                log(f"🔥 Fel i tabell {endpoint}: {inner_e}", module="scb")

    except Exception as e:
        log(f"🔥 Fel vid hämtning från {endpoint_meta['base_url']}: {e}", module="scb")

conn.commit()
conn.close()
print("SCB-data uppdaterad.")