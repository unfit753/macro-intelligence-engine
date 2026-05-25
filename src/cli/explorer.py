"""Interactive terminal explorer for Macro Intelligence Engine.

The explorer is deliberately dependency-free. It can read the configured
SQLite database through src.core.api, or run from a small synthetic snapshot
with --demo so the public repository is useful without shipping fetched data.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from src.core.compliance import public_disclaimer


REPO_ROOT = Path(__file__).resolve().parents[2]
DEMO_SNAPSHOT_PATH = REPO_ROOT / "examples" / "demo_snapshot.json"


@dataclass(frozen=True)
class Column:
    keys: tuple[str, ...]
    label: str
    width: int


@dataclass(frozen=True)
class View:
    key: str
    title: str
    description: str
    columns: tuple[Column, ...]
    detail_fields: tuple[Column, ...]
    loader: Callable[[int], list[dict[str, Any]]]
    demo_key: str


def _col(keys: str | Sequence[str], label: str, width: int) -> Column:
    if isinstance(keys, str):
        keys = (keys,)
    return Column(tuple(keys), label, width)


def _api():
    from src.core import api as core_api
    return core_api


def _limit_rows(rows: Iterable[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return list(rows)[: max(0, int(limit))]


VIEWS: tuple[View, ...] = (
    View(
        key="source-health",
        title="Source Health",
        description="Freshness and row-count checks for configured public feeds.",
        demo_key="source_health",
        loader=lambda limit: _limit_rows(_api().source_freshness(), limit),
        columns=(
            _col("source", "Source", 30),
            _col("status", "Status", 9),
            _col("age", "Age", 8),
            _col("rows", "Rows", 8),
            _col("cadence", "Cadence", 18),
        ),
        detail_fields=(
            _col("source", "Source", 24),
            _col("status", "Status", 12),
            _col("latest", "Latest", 28),
            _col("cadence", "Cadence", 20),
            _col("rows", "Rows", 10),
        ),
    ),
    View(
        key="current-events",
        title="Current Events",
        description="Scheduled catalysts, released actuals, alerts, and market-moving news.",
        demo_key="current_events",
        loader=lambda limit: _api().current_events(limit=limit),
        columns=(
            _col(("display_title", "title"), "Event", 34),
            _col("event_type", "Type", 18),
            _col("region", "Region", 14),
            _col("event_time", "Time", 18),
            _col("priority", "Priority", 8),
        ),
        detail_fields=(
            _col(("display_title", "title"), "Title", 24),
            _col(("display_summary", "summary"), "Summary", 80),
            _col("why_text", "Why", 80),
            _col("affected_assets_json", "Assets", 80),
        ),
    ),
    View(
        key="market-tape",
        title="Market Tape",
        description="Latest asset moves enriched with forecast metadata when available.",
        demo_key="market_tape",
        loader=lambda limit: _api().market_tape(limit=limit),
        columns=(
            _col("symbol", "Symbol", 12),
            _col("name", "Name", 24),
            _col("move_1d", "Move 1d", 10),
            _col("prediction_direction", "Forecast", 10),
            _col("prediction_confidence", "Conf", 8),
        ),
        detail_fields=(
            _col("name", "Name", 24),
            _col("symbol", "Symbol", 12),
            _col("asset_class", "Asset class", 16),
            _col("prediction_horizon", "Horizon", 12),
            _col("prediction_range", "Range", 30),
            _col("prediction_rationale", "Rationale", 80),
        ),
    ),
    View(
        key="catalysts",
        title="Next Macro Catalysts",
        description="Upcoming scheduled macro releases and scenario-ready events.",
        demo_key="next_macro_catalysts",
        loader=lambda limit: _api().next_macro_catalysts(limit=limit),
        columns=(
            _col(("title", "category"), "Catalyst", 34),
            _col("region", "Region", 12),
            _col(("release_date", "event_time"), "Date", 16),
            _col("importance", "Imp", 5),
            _col("expected", "Expected", 22),
        ),
        detail_fields=(
            _col("title", "Title", 24),
            _col("category", "Category", 16),
            _col("expected", "Expected", 40),
            _col("previous_value", "Previous", 14),
            _col("source", "Source", 20),
        ),
    ),
    View(
        key="data-catalog",
        title="Data Catalog",
        description="Named data objects that form the public backend contract.",
        demo_key="data_catalog",
        loader=lambda limit: _limit_rows(_api().named_data_overview(), limit),
        columns=(
            _col(("display_name", "object_id"), "Object", 30),
            _col("frontend_group", "Group", 16),
            _col("source_table", "Table", 22),
            _col("status", "Status", 9),
            _col("rows", "Rows", 8),
        ),
        detail_fields=(
            _col("object_id", "Object id", 24),
            _col("display_name", "Display name", 30),
            _col("description", "Description", 80),
            _col("cadence", "Cadence", 18),
            _col("latest", "Latest", 28),
        ),
    ),
)

VIEW_BY_KEY = {view.key: view for view in VIEWS}


class DataSource:
    def __init__(self, *, demo: bool = False, snapshot_path: Path = DEMO_SNAPSHOT_PATH) -> None:
        self.demo = demo
        self.snapshot_path = snapshot_path
        self._snapshot: dict[str, Any] | None = None

    def fetch(self, view: View, limit: int) -> list[dict[str, Any]]:
        if self.demo:
            snapshot = self.snapshot()
            rows = snapshot.get(view.demo_key, [])
            return _limit_rows(rows if isinstance(rows, list) else [], limit)
        return view.loader(limit)

    def snapshot(self) -> dict[str, Any]:
        if self._snapshot is None:
            self._snapshot = load_demo_snapshot(self.snapshot_path)
        return self._snapshot


def load_demo_snapshot(path: Path = DEMO_SNAPSHOT_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _value(row: dict[str, Any], column: Column) -> Any:
    for key in column.keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return ""


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if abs(value) <= 1 and value != 0:
            return f"{value:.2f}"
        return f"{value:.2f}".rstrip("0").rstrip(".")
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    text = str(value)
    if text.startswith("[") or text.startswith("{"):
        try:
            parsed = json.loads(text)
        except (TypeError, json.JSONDecodeError):
            return text
        return json.dumps(parsed, ensure_ascii=True, sort_keys=True)
    return text


def _clip(text: str, width: int) -> str:
    clean = " ".join(text.split())
    if len(clean) <= width:
        return clean
    if width <= 3:
        return clean[:width]
    return clean[: width - 3] + "..."


def _fit_columns(columns: Sequence[Column], max_width: int) -> list[int]:
    padding = 3 * (len(columns) - 1)
    available = max(48, max_width - padding)
    requested = sum(column.width for column in columns)
    if requested <= available:
        return [column.width for column in columns]
    scale = available / requested
    return [max(6, int(column.width * scale)) for column in columns]


def render_table(
    rows: Sequence[dict[str, Any]],
    columns: Sequence[Column],
    *,
    width: int | None = None,
    numbered: bool = True,
) -> str:
    if not rows:
        return "No rows available."
    terminal_width = width or shutil.get_terminal_size((110, 24)).columns
    number_width = 4 if numbered else 0
    col_widths = _fit_columns(columns, terminal_width - number_width)
    header = []
    if numbered:
        header.append("#".ljust(number_width))
    header.extend(column.label.ljust(col_widths[index]) for index, column in enumerate(columns))
    lines = ["   ".join(header), "-" * min(terminal_width, len("   ".join(header)))]
    for row_index, row in enumerate(rows, start=1):
        cells = []
        if numbered:
            cells.append(str(row_index).ljust(number_width))
        for index, column in enumerate(columns):
            value = _clip(_stringify(_value(row, column)), col_widths[index])
            cells.append(value.ljust(col_widths[index]))
        lines.append("   ".join(cells))
    return "\n".join(lines)


def render_detail(row: dict[str, Any], fields: Sequence[Column], *, width: int | None = None) -> str:
    terminal_width = width or shutil.get_terminal_size((110, 24)).columns
    label_width = min(18, max((len(field.label) for field in fields), default=10) + 2)
    body_width = max(40, terminal_width - label_width - 3)
    lines = []
    for field in fields:
        value = _stringify(_value(row, field))
        wrapped = textwrap.wrap(value, width=body_width) or [""]
        lines.append(f"{field.label.ljust(label_width)} {wrapped[0]}")
        for continuation in wrapped[1:]:
            lines.append(f"{''.ljust(label_width)} {continuation}")
    return "\n".join(lines)


def build_overview(source: DataSource, limit: int) -> list[dict[str, Any]]:
    health = source.fetch(VIEW_BY_KEY["source-health"], limit=200)
    events = source.fetch(VIEW_BY_KEY["current-events"], limit=limit)
    market = source.fetch(VIEW_BY_KEY["market-tape"], limit=limit)
    catalysts = source.fetch(VIEW_BY_KEY["catalysts"], limit=limit)
    warnings = [row for row in health if str(row.get("status", "")).lower() not in {"ok", "future"}]
    return [
        {
            "section": "Source health",
            "metric": f"{len(warnings)} warning(s)",
            "note": "Feeds look usable." if not warnings else ", ".join(str(row.get("source", "unknown")) for row in warnings[:3]),
        },
        {
            "section": "Current events",
            "metric": f"{len(events)} row(s)",
            "note": _stringify((events[0] if events else {}).get("display_title") or (events[0] if events else {}).get("title")),
        },
        {
            "section": "Market tape",
            "metric": f"{len(market)} asset(s)",
            "note": _stringify((market[0] if market else {}).get("name") or (market[0] if market else {}).get("symbol")),
        },
        {
            "section": "Catalysts",
            "metric": f"{len(catalysts)} upcoming",
            "note": _stringify((catalysts[0] if catalysts else {}).get("title") or (catalysts[0] if catalysts else {}).get("category")),
        },
    ]


OVERVIEW_COLUMNS = (
    _col("section", "Section", 18),
    _col("metric", "Metric", 16),
    _col("note", "Top read", 64),
)


def render_view(source: DataSource, view_key: str, limit: int) -> tuple[str, list[dict[str, Any]], Sequence[Column]]:
    if view_key == "overview":
        rows = build_overview(source, limit)
        body = render_table(rows, OVERVIEW_COLUMNS, numbered=False)
        return "Overview\n" + body, rows, ()
    view = VIEW_BY_KEY[view_key]
    rows = source.fetch(view, limit)
    body = render_table(rows, view.columns)
    return f"{view.title}\n{view.description}\n\n{body}", rows, view.detail_fields


def render_json(source: DataSource, view_key: str, limit: int) -> dict[str, Any]:
    if view_key == "overview":
        rows = build_overview(source, limit)
    else:
        rows = source.fetch(VIEW_BY_KEY[view_key], limit)
    return {
        "positioning": "research_only",
        "disclaimer": public_disclaimer(),
        "view": view_key,
        "demo": source.demo,
        "rows": rows,
    }


def interactive_loop(source: DataSource, limit: int) -> int:
    while True:
        print("\nMacro Intelligence Engine")
        print("Research only. Choose a view:\n")
        print("  0. Overview")
        for index, view in enumerate(VIEWS, start=1):
            print(f"  {index}. {view.title}")
        print("  q. Quit")
        choice = input("\nSelect view: ").strip().lower()
        if choice in {"q", "quit", "exit"}:
            return 0
        if choice == "0":
            view_key = "overview"
        elif choice.isdigit() and 1 <= int(choice) <= len(VIEWS):
            view_key = VIEWS[int(choice) - 1].key
        else:
            print("Unknown selection.")
            continue
        print()
        rendered, rows, detail_fields = render_view(source, view_key, limit)
        print(rendered)
        if rows and detail_fields:
            detail_choice = input("\nOpen row number for detail, or Enter to continue: ").strip()
            if detail_choice.isdigit() and 1 <= int(detail_choice) <= len(rows):
                print()
                print(render_detail(rows[int(detail_choice) - 1], detail_fields))
                input("\nPress Enter to continue.")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Explore Macro Intelligence Engine data from a terminal.",
    )
    parser.add_argument(
        "--view",
        choices=("overview",) + tuple(view.key for view in VIEWS),
        help="Render one view and exit. Omit for interactive menu.",
    )
    parser.add_argument("--demo", action="store_true", help="Use the bundled synthetic demo snapshot.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a terminal table.")
    parser.add_argument("--limit", type=int, default=12, help="Maximum rows per view.")
    parser.add_argument("--detail", type=int, help="Show detail for a 1-based row number.")
    parser.add_argument("--debug", action="store_true", help="Print database errors for local troubleshooting.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    source = DataSource(demo=args.demo)
    view_key = args.view
    if view_key is None and not sys.stdin.isatty():
        view_key = "overview"
    try:
        if view_key is None:
            return interactive_loop(source, args.limit)
        if args.json:
            print(json.dumps(render_json(source, view_key, args.limit), ensure_ascii=True, indent=2, sort_keys=True))
            return 0
        rendered, rows, detail_fields = render_view(source, view_key, args.limit)
        print(rendered)
        if args.detail is not None:
            if not detail_fields:
                print("\nThis view has no row detail.")
            elif args.detail < 1 or args.detail > len(rows):
                print(f"\nNo row {args.detail}.")
            else:
                print()
                print(render_detail(rows[args.detail - 1], detail_fields))
        return 0
    except Exception as exc:
        print("Unable to open or read the configured SQLite database, or a runtime dependency is missing.", file=sys.stderr)
        print("Try the bundled demo: python -m src.cli.explorer --demo", file=sys.stderr)
        if args.debug:
            print(f"Debug: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
