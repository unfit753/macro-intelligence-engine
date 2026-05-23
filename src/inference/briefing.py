"""Generate session-aware macro briefings.

The old daily regional brief was too blunt for market timing: an Asia summary
written near the US close can sound stale before Asia opens, and Europe needs a
fresh pre-market read. This module now supports session variants while keeping
the same underlying regional data model.

    - latest signal snapshot for the region's key symbols
    - last 14d events whose country tag falls in the region
    - upcoming high-importance macro calendar events
    - persistent geopolitical/supply-chain watchpoints
    - GDELT disaster markers and sanctions context
    - the GKG news intensity table (with arrows vs 30d mean)
    - RSS reading-wire headlines
    - any predictions for region's symbols whose target date is now in
      the next horizon (so the briefing can flag what's already on
      record)

The prompt is short and asks Claude for `headline + summary_md +
things_to_watch[]`.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import sys

from anthropic import Anthropic
from dotenv import load_dotenv

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable
from src.inference.predict import MODEL


# Region -> {symbols, countries, label}
REGIONS: dict[str, dict] = {
    "global": {
        "label": "Global",
        "symbols": ["GC=F", "CL=F", "SPY", "TLT", "^VIX",
                    "^STOXX50E", "^OMX", "^N225"],
        "countries": [],   # empty = no filter, take all events
    },
    "us": {
        "label": "United States",
        "symbols": ["SPY", "TLT", "^VIX", "DX-Y.NYB", "GC=F", "CL=F",
                    "XLE", "XLF", "XLK", "XLI"],
        "countries": ["US"],
    },
    "eu_se": {
        "label": "Europe & Sweden",
        "symbols": ["^STOXX50E", "^OMX", "^GDAXI", "^FCHI", "^FTSE"],
        "countries": ["SE", "EU", "DE", "FR", "GB", "IT", "ES", "NL", "CH", "NO"],
    },
    "asia": {
        "label": "Asia (overnight indicator)",
        "symbols": ["^N225", "^HSI", "EWJ", "MCHI", "INDA", "EEM"],
        "countries": ["JP", "CN", "KR", "IN", "HK"],
    },
}

SESSION_PROFILES: dict[str, dict] = {
    "global_close": {
        "label": "Global close",
        "audience": "End-of-day global synthesis for tomorrow's European decision context.",
        "task": (
            "Focus on what changed through the completed US/EU session, what still matters "
            "overnight, and what would change tomorrow's European setup."
        ),
        "watch_wording": "tomorrow",
    },
    "us_postclose": {
        "label": "US post-close",
        "audience": "US close read-through for a Sweden/Europe-based user.",
        "task": (
            "Treat the US cash session as closed or nearly closed. Summarize read-through "
            "for rates, USD, gold, oil, US sectors and tomorrow's European open."
        ),
        "watch_wording": "tomorrow",
    },
    "eu_premarket": {
        "label": "EU/Sweden pre-market",
        "audience": "Pre-market decision context before Stockholm and European cash markets open.",
        "task": (
            "Focus on overnight Asia/US spillover, today's European macro calendar, OMX/DAX/STOXX "
            "risk skew, and what matters in the first trading hour."
        ),
        "watch_wording": "today",
    },
    "asia_preopen": {
        "label": "Asia pre-open",
        "audience": "Asia pre-open watch, used as an overnight indicator for tomorrow's Europe/Sweden setup.",
        "task": (
            "Do not write as if Asia has already traded unless the supplied prices/events show it. "
            "Frame this as a pre-open watch: catalysts, inherited US/EU pressure, FX/commodities, "
            "and what Asia could signal for the next OMX session."
        ),
        "watch_wording": "overnight",
    },
    "daily": {
        "label": "Daily",
        "audience": "General regional research summary.",
        "task": "Summarize the latest regional setup and immediate watch items.",
        "watch_wording": "next session",
    },
}

DEFAULT_SESSION_BY_REGION = {
    "global": "global_close",
    "us": "us_postclose",
    "eu_se": "eu_premarket",
    "asia": "asia_preopen",
}

DISPLAY_BRIEFINGS: dict[str, dict] = {
    "eu_se:eu_premarket": {
        "label": "🇪🇺 Europe/Sweden pre-market",
        "base_region": "eu_se",
        "session": "eu_premarket",
        "fallback": "eu_se",
    },
    "us:us_postclose": {
        "label": "🇺🇸 US post-close",
        "base_region": "us",
        "session": "us_postclose",
        "fallback": "us",
    },
    "asia:asia_preopen": {
        "label": "🌏 Asia pre-open",
        "base_region": "asia",
        "session": "asia_preopen",
        "fallback": "asia",
    },
    "global:global_close": {
        "label": "🌐 Global close",
        "base_region": "global",
        "session": "global_close",
        "fallback": "global",
    },
}


SYSTEM_PROMPT = (
    "You are a macro analyst writing a tight one-page daily briefing for "
    "a research surface focused on Sweden, Europe, and global signals. "
    "Use only the data given. Do not invent prices, "
    "events, or numbers. Distinguish hard scheduled catalysts from noisy "
    "passive-feed alerts. If the data shows nothing notable, say so — do "
    "not manufacture drama. Always include actionable 'things to watch' "
    "(short imperative bullets like 'Watch OMX for spillover from Nikkei "
    "weakness'). Submit via the submit_briefing tool."
)

BRIEFING_TOOL = {
    "name": "submit_briefing",
    "description": "Submit your regional macro briefing.",
    "input_schema": {
        "type": "object",
        "properties": {
            "headline": {
                "type": "string",
                "description": "Single-line summary, ≤120 chars.",
            },
            "summary_md": {
                "type": "string",
                "description": "2-4 short paragraphs in Markdown citing "
                               "specific signals, events and news intensity.",
            },
            "things_to_watch": {
                "type": "array", "items": {"type": "string"},
                "description": "3-6 imperative watch bullets for the relevant market session.",
            },
        },
        "required": ["headline", "summary_md", "things_to_watch"],
    },
}


def _signal_block(conn, symbol: str, as_of: dt.date) -> str:
    rows = conn.execute(
        """SELECT signal_name, value FROM signals
           WHERE symbol=? AND date=(SELECT MAX(date) FROM signals
                                    WHERE symbol=? AND date<=?)""",
        (symbol, symbol, as_of.isoformat()),
    ).fetchall()
    if not rows:
        return ""
    keep = {"ret_1d", "ret_1w", "ret_1m", "ret_3m", "ret_1y",
            "drawdown_252d", "rsi_14", "vol_30d_ann",
            "px_vs_ma50", "px_vs_ma200"}
    parts = []
    for n, v in rows:
        if n not in keep:
            continue
        if n.startswith("ret_") or n.startswith("px_vs_") or n in ("drawdown_252d", "vol_30d_ann"):
            parts.append(f"{n}={v*100:+.2f}%")
        elif n == "rsi_14":
            parts.append(f"rsi={v:.1f}")
    return f"`{symbol}`: " + ", ".join(parts) if parts else ""


def _events_block(conn, countries: list[str], as_of: dt.date, days: int = 14) -> list[tuple]:
    cutoff = (as_of - dt.timedelta(days=days)).isoformat()
    if countries:
        ph = ",".join("?" for _ in countries)
        rows = conn.execute(
            f"""SELECT date, country, type, source, title FROM events
                WHERE date BETWEEN ? AND ? AND country IN ({ph})
                ORDER BY date DESC LIMIT 30""",
            (cutoff, as_of.isoformat(), *countries),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT date, country, type, source, title FROM events
               WHERE date BETWEEN ? AND ? ORDER BY date DESC LIMIT 30""",
            (cutoff, as_of.isoformat()),
        ).fetchall()
    return rows


def _calendar_block(conn, countries: list[str], as_of: dt.date) -> list[tuple]:
    end = (as_of + dt.timedelta(days=45)).isoformat()
    if countries:
        regions = countries + (["UK"] if "GB" in countries else [])
        ph = ",".join("?" for _ in regions)
        rows = conn.execute(
            f"""SELECT date, time_local, region, category, importance, title,
                       expected, market_note, source
                FROM calendar_events
                WHERE date BETWEEN ? AND ? AND importance >= 4
                  AND region IN ({ph})
                ORDER BY date ASC, importance DESC LIMIT 14""",
            (as_of.isoformat(), end, *regions),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT date, time_local, region, category, importance, title,
                      expected, market_note, source
               FROM calendar_events
               WHERE date BETWEEN ? AND ? AND importance >= 4
               ORDER BY date ASC, importance DESC LIMIT 14""",
            (as_of.isoformat(), end),
        ).fetchall()
    return rows


def _risk_block(conn, countries: list[str]) -> list[tuple]:
    if countries:
        ph = ",".join("?" for _ in countries)
        return conn.execute(
            f"""SELECT name, region, country, category, severity, summary, updated_at
                FROM risk_hotspots
                WHERE active = 1 AND (country IN ({ph}) OR region IN ({ph}))
                ORDER BY severity DESC, name LIMIT 10""",
            (*countries, *countries),
        ).fetchall()
    return conn.execute(
        """SELECT name, region, country, category, severity, summary, updated_at
           FROM risk_hotspots
           WHERE active = 1
           ORDER BY severity DESC, name LIMIT 10"""
    ).fetchall()


def _disaster_block(conn, countries: list[str], as_of: dt.date) -> list[tuple]:
    cutoff = (as_of - dt.timedelta(days=7)).isoformat()
    if countries:
        ph = ",".join("?" for _ in countries)
        return conn.execute(
            f"""SELECT date, country, disaster_type, article_count, examples
                FROM gdelt_disaster_signals
                WHERE date >= ? AND country IN ({ph})
                ORDER BY article_count DESC, date DESC LIMIT 8""",
            (cutoff, *countries),
        ).fetchall()
    return conn.execute(
        """SELECT date, country, disaster_type, article_count, examples
           FROM gdelt_disaster_signals
           WHERE date >= ?
           ORDER BY article_count DESC, date DESC LIMIT 8""",
        (cutoff,),
    ).fetchall()


def _sanctions_block(conn, countries: list[str]) -> list[tuple]:
    if countries:
        ph = ",".join("?" for _ in countries)
        return conn.execute(
            f"""SELECT country, program, product, COUNT(*) AS rows
                FROM sanctions
                WHERE country IN ({ph})
                GROUP BY country, program, product
                ORDER BY rows DESC LIMIT 8""",
            (*countries,),
        ).fetchall()
    return conn.execute(
        """SELECT country, program, product, COUNT(*) AS rows
           FROM sanctions
           GROUP BY country, program, product
           ORDER BY rows DESC LIMIT 8"""
    ).fetchall()


def _rss_block(conn, countries: list[str], as_of: dt.date) -> list[tuple]:
    cutoff = (as_of - dt.timedelta(days=7)).isoformat()
    regions = []
    if not countries:
        regions = ["Global", "EU", "US"]
    else:
        if any(c in countries for c in ("EU", "SE", "DE", "FR", "GB", "UK")):
            regions.append("EU")
        if "US" in countries:
            regions.append("US")
        regions.append("Global")
    ph = ",".join("?" for _ in sorted(set(regions)))
    return conn.execute(
        f"""SELECT published_at, source, region, category, title
            FROM news_items
            WHERE substr(COALESCE(published_at, fetched_at), 1, 10) >= ?
              AND region IN ({ph})
            ORDER BY COALESCE(published_at, fetched_at) DESC LIMIT 10""",
        (cutoff, *sorted(set(regions))),
    ).fetchall()


def _news_block(conn, as_of: dt.date) -> str:
    """Just the moves: themes whose today vs 30d mean ratio is notable."""
    snap = dict(conn.execute(
        """SELECT signal_name, value FROM signals
           WHERE symbol='_news' AND date=(SELECT MAX(date) FROM signals
                                          WHERE symbol='_news' AND date<=?)""",
        (as_of.isoformat(),),
    ).fetchall())
    if not snap:
        return ""
    cutoff = (as_of - dt.timedelta(days=30)).isoformat()
    notable = []
    for sn, today in snap.items():
        if not sn.startswith("news_rate_"):
            continue
        theme = sn.replace("news_rate_", "")
        row = conn.execute(
            """SELECT AVG(value) FROM signals WHERE symbol='_news'
               AND signal_name=? AND date BETWEEN ? AND ?""",
            (sn, cutoff, as_of.isoformat()),
        ).fetchone()
        mean = row[0] if row and row[0] else None
        if not mean or mean <= 0:
            continue
        ratio = today / mean
        if ratio > 1.25 or ratio < 0.75:
            arrow = "↑" if ratio > 1 else "↓"
            notable.append(f"{theme} {arrow} {ratio:.2f}× ({today*100:.2f}% today vs {mean*100:.2f}% 30d)")
    return "; ".join(notable)


def storage_region(region_key: str, session: str | None = None) -> str:
    if not session or session == "daily":
        return region_key
    return f"{region_key}:{session}"


def render_region_brief(conn, region_key: str, as_of: dt.date, session: str | None = None) -> str:
    cfg = REGIONS[region_key]
    session_key = session or DEFAULT_SESSION_BY_REGION.get(region_key, "daily")
    session_cfg = SESSION_PROFILES[session_key]
    parts = [
        f"# {cfg['label']} — {session_cfg['label']} brief, as of {as_of.isoformat()}",
        "",
        "## Session context",
        f"- Audience: {session_cfg['audience']}",
        f"- Timing instruction: {session_cfg['task']}",
        f"- Watch-list horizon wording should be `{session_cfg['watch_wording']}`.",
        "",
    ]

    parts.append("## Signal snapshot")
    for sym in cfg["symbols"]:
        line = _signal_block(conn, sym, as_of)
        if line:
            parts.append(f"- {line}")
    parts.append("")

    events = _events_block(conn, cfg["countries"], as_of, days=14)
    if events:
        parts.append(f"## Recent events (last 14d, region-filtered)")
        for d, country, ev_type, source, title in events:
            parts.append(f"- **{d}** [{country or '?'}/{ev_type or '?'}/{source or '?'}] {title}")
    parts.append("")

    calendar = _calendar_block(conn, cfg["countries"], as_of)
    if calendar:
        parts.append("## Upcoming scheduled catalysts")
        for d, t, region, category, importance, title, expected, note, source in calendar:
            when = f"{d} {t or ''}".strip()
            parts.append(
                f"- **{when}** [{region}/{category}/imp {importance}/5] "
                f"{title}; expected/context: {expected or 'n/a'}; market note: {note or 'n/a'}"
            )
        parts.append("")

    risks = _risk_block(conn, cfg["countries"])
    if risks:
        parts.append("## Persistent geopolitical and supply-chain watchpoints")
        for name, region, country, category, severity, summary, updated_at in risks:
            parts.append(
                f"- **{name}** [{country or region}/{category}/severity {severity}/5]: "
                f"{summary or 'no summary'}"
            )
        parts.append("")

    disasters = _disaster_block(conn, cfg["countries"], as_of)
    if disasters:
        parts.append("## GDELT disaster markers (last 7d)")
        for d, country, disaster_type, count, examples in disasters:
            parts.append(
                f"- **{d}** [{country}/{disaster_type}] {count} tagged articles; "
                f"{examples or 'no examples'}"
            )
        parts.append("")

    sanctions = _sanctions_block(conn, cfg["countries"])
    if sanctions:
        parts.append("## Sanctions context")
        for country, program, product, rows in sanctions:
            product_txt = f", product hint={product}" if product else ""
            parts.append(
                f"- **{country or '?'}** / {program or 'program n/a'}: "
                f"{rows} OFAC row(s){product_txt}."
            )
        parts.append("")

    news = _news_block(conn, as_of)
    if news:
        parts.append("## News intensity (notable themes vs 30d baseline)")
        parts.append(news)
        parts.append("")

    rss = _rss_block(conn, cfg["countries"], as_of)
    if rss:
        parts.append("## RSS reading wire (not direct prediction input)")
        for published_at, source, region, category, title in rss:
            when = str(published_at or "")[:16]
            parts.append(f"- **{when}** [{source}/{region}/{category or 'news'}] {title}")
        parts.append("")

    parts.append("## Your task")
    parts.append(
        f"Write the {cfg['label']} {session_cfg['label']} briefing. Be concrete: cite specific "
        "symbols, events and themes from the data above. If the region "
        "looks quiet, say so — don't pad. Respect the session timing: "
        f"{session_cfg['task']} Submit via submit_briefing."
    )
    return "\n".join(parts)


def call_claude(brief_text: str) -> dict:
    client = Anthropic()
    msg = client.messages.create(
        model=MODEL, max_tokens=2048, system=SYSTEM_PROMPT,
        tools=[BRIEFING_TOOL],
        tool_choice={"type": "tool", "name": "submit_briefing"},
        messages=[{"role": "user", "content": brief_text}],
    )
    for block in msg.content:
        if block.type == "tool_use" and block.name == "submit_briefing":
            return block.input
    raise RuntimeError(f"No tool_use: {msg.stop_reason}")


def write(conn, region: str, as_of: dt.date, payload: dict, model: str) -> int:
    cur = conn.execute(
        """INSERT OR REPLACE INTO briefings
           (region, as_of, generated_at, headline, summary_md,
            things_to_watch, model)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            region, as_of.isoformat(),
            dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
            payload["headline"], payload["summary_md"],
            json.dumps(payload["things_to_watch"]),
            model,
        ),
    )
    conn.commit()
    return cur.lastrowid


def generate_one(region: str, as_of: dt.date | None = None, session: str | None = None) -> int:
    as_of = as_of or dt.date.today()
    session = session or DEFAULT_SESSION_BY_REGION.get(region, "daily")
    conn = connect_writable(DB_PATH)
    try:
        brief_text = render_region_brief(conn, region, as_of, session=session)
        stored_region = storage_region(region, session)
        log(f"Generating {stored_region} briefing as of {as_of} ({len(brief_text):,} chars)...",
            module="briefing")
        payload = call_claude(brief_text)
        bid = write(conn, stored_region, as_of, payload, MODEL)
        log(f"Briefing #{bid} ({stored_region}): {payload['headline']}", module="briefing")
        return bid
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    load_dotenv(override=True)
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY not set", file=sys.stderr)
        return 2
    p = argparse.ArgumentParser()
    p.add_argument("--region", default=None,
                   help="One of: " + ", ".join(REGIONS) + ". Omit to generate all four.")
    p.add_argument("--session", default=None,
                   help="One of: " + ", ".join(SESSION_PROFILES) + ". Defaults by region.")
    p.add_argument("--as-of", default=None, help="YYYY-MM-DD; defaults to today")
    args = p.parse_args(argv)
    as_of = dt.date.fromisoformat(args.as_of) if args.as_of else dt.date.today()
    if args.session and args.session not in SESSION_PROFILES:
        print(f"unknown session {args.session}", file=sys.stderr)
        return 2

    regions = [args.region] if args.region else list(REGIONS)
    for r in regions:
        if r not in REGIONS:
            print(f"unknown region {r}", file=sys.stderr); return 2
        try:
            generate_one(r, as_of, session=args.session)
        except Exception as e:
            log(f"{r} briefing failed: {e}", module="briefing")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
