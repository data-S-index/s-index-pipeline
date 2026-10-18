"""
Shared helpers for the automation scripts: config, secrets, command-line day
ranges and the layout of the harvested files in the bunny.net storage zone.

Layout of a source's files in the storage zone (one set per day):

    <source>/raw/YYYY/<source>-YYYY-MM-DD.ndjson.gz       raw records
    <source>/slim/YYYY/<source>-YYYY-MM-DD.ndjson.gz      slim records
    <source>/manifests/YYYY/<source>-YYYY-MM-DD.json      counts and checksums

A day's manifest is uploaded after its files, so a day is harvested if and
only if its manifest exists.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Allow running without `pip install -e .`
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

# pylint: disable=wrong-import-position,wrong-import-order
from sindex.core.dates import _parse_date_strict  # noqa: E402
from bunny_storage import BunnyStorage  # noqa: E402

# pylint: enable=wrong-import-position,wrong-import-order

AUTOMATION_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = AUTOMATION_DIR / "config.yaml"
ENV_FILE = AUTOMATION_DIR.parent / ".env"

# First day the jobs harvest and load when no --date/--start is given
START_DATE = date(2026, 5, 1)

# The default range ends this many days before today (UTC). A harvested day is
# never revisited, so waiting a week lets DataCite records registered late
# (and today's, which is still in progress) be included.
END_DELAY_DAYS = 7

# Days each run processes by default. Small runs keep every job short; the
# harvest workflow runs twice a day, so a backlog shrinks by one day a day.
DAYS_PER_RUN = 1


# ---------------------------------------------------------------------------
# Config and secrets
# ---------------------------------------------------------------------------


def load_config(config_path: Path) -> dict:
    """Load the config YAML."""
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_secret(name: str) -> str:
    """Return `name` from .env or the environment, or exit if it isn't set."""
    load_dotenv(ENV_FILE)
    value = os.environ.get(name)
    if not value:
        sys.exit(f"{name} is not set in {ENV_FILE} or the environment.")
    return value


def make_storage(cfg: dict) -> BunnyStorage:
    """Return a client for the storage zone in the config's `storage` section."""
    return BunnyStorage(
        zone=cfg["storage"]["zone"],
        password=get_secret("BUNNY_STORAGE_PASSWORD"),
        host=cfg["storage"].get("host", "storage.bunnycdn.com"),
    )


def now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string (to the second)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ProgressLog:  # pylint: disable=too-few-public-methods
    """
    Print progress as separate lines, at most one every `every_seconds`.

    Unlike an in-place progress bar (carriage returns), this reads well in
    GitHub Actions logs and other non-terminal output. Call it with
    (done, total); pass final=True to always print the last line.
    """

    def __init__(self, label: str, every_seconds: float = 30.0):
        self.label = label
        self.every_seconds = every_seconds
        self.t0 = self.last = time.monotonic()

    def __call__(self, done: int, total: int | None = None, final: bool = False):
        now = time.monotonic()
        if not final and now - self.last < self.every_seconds:
            return
        self.last = now
        elapsed = now - self.t0
        rate = done / elapsed if elapsed > 0 else 0
        msg = f"{done:,} / {total:,} ({done / total:.0%})" if total else f"{done:,}"
        print(
            f"  {self.label}: {msg} records | {rate:,.0f} rec/s | "
            f"{elapsed:,.0f}s elapsed",
            flush=True,
        )


def github_run_url() -> str | None:
    """Return the URL of the current GitHub Actions run, or None outside Actions."""
    env = os.environ
    if "GITHUB_RUN_ID" not in env:
        return None
    return (
        f"{env['GITHUB_SERVER_URL']}/{env['GITHUB_REPOSITORY']}"
        f"/actions/runs/{env['GITHUB_RUN_ID']}"
    )


# ---------------------------------------------------------------------------
# Storage layout
# ---------------------------------------------------------------------------


def raw_path(source: str, day: date) -> str:
    """Path of a day's raw records in the storage zone."""
    return f"{source}/raw/{day.year}/{source}-{day.isoformat()}.ndjson.gz"


def slim_path(source: str, day: date) -> str:
    """Path of a day's slim records in the storage zone."""
    return f"{source}/slim/{day.year}/{source}-{day.isoformat()}.ndjson.gz"


def manifest_path(source: str, day: date) -> str:
    """Path of a day's manifest in the storage zone."""
    return f"{source}/manifests/{day.year}/{source}-{day.isoformat()}.json"


def list_harvested_days(
    storage: BunnyStorage, source: str, days: list[date]
) -> set[date]:
    """Return the days in `days` that have a manifest in the storage zone."""
    wanted = set(days)
    found = set()
    prefix = f"{source}-"
    for year in sorted({d.year for d in days}):
        for entry in storage.list_dir(f"{source}/manifests/{year}"):
            name = entry["ObjectName"]
            if entry["IsDirectory"] or not name.startswith(prefix):
                continue
            try:
                day = date.fromisoformat(
                    name.removeprefix(prefix).removesuffix(".json")
                )
            except ValueError:
                continue
            if day in wanted:
                found.add(day)
    return found


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add the day-range, status, force and config arguments shared by the jobs."""
    parser.add_argument("--date", help="Single day (YYYY-MM-DD).")
    parser.add_argument("--start", help="First day of a range (YYYY-MM-DD).")
    parser.add_argument("--end", help="Last day of a range, inclusive (YYYY-MM-DD).")
    parser.add_argument(
        "--max-days",
        type=int,
        default=DAYS_PER_RUN,
        help="Most days to process in this run, oldest first "
        f"(default: {DAYS_PER_RUN}; 0 = no limit).",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=f"Path to config YAML (default: {DEFAULT_CONFIG}).",
    )
    parser.add_argument(
        "--status", action="store_true", help="Show progress per day and exit."
    )


def resolve_days(args: argparse.Namespace) -> list[date]:
    """
    Return the days to work on, from --date, --start/--end or the defaults:
    START_DATE up to END_DELAY_DAYS before today (UTC).

    Exits with an error if the arguments conflict or the range is empty.
    """
    if args.date and (args.start or args.end):
        sys.exit("Use either --date or --start/--end, not both.")

    if args.date:
        return [_parse_date_strict(args.date)]

    default_end = datetime.now(timezone.utc).date() - timedelta(days=END_DELAY_DAYS)
    start = _parse_date_strict(args.start) if args.start else START_DATE
    end = _parse_date_strict(args.end) if args.end else default_end
    if end < start:
        sys.exit(f"End date {end} is before start date {start}.")
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def limit_days(todo: list[date], max_days: int) -> list[date]:
    """Return the first `max_days` of `todo` (all of them if max_days is 0)."""
    if max_days and len(todo) > max_days:
        print(
            f"Limiting this run to the first {max_days} of {len(todo)} day(s); "
            "the rest are left for the next run."
        )
        return todo[:max_days]
    return todo
