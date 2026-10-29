"""
Harvest new DataCite dataset records one day at a time and store them in the
bunny.net storage zone.

First half of the DataCite automation; datacite_load.py then loads the stored
files into Postgres. For each day it:
  1. Harvests the raw DataCite Dataset records created on that day
  2. Slims each record with `slim_datacite_record`
  3. Uploads both, gzipped, to the storage zone:
       datacite/raw/YYYY/datacite-YYYY-MM-DD.ndjson.gz
       datacite/slim/YYYY/datacite-YYYY-MM-DD.ndjson.gz
  4. Uploads the day's manifest, datacite/manifests/YYYY/datacite-YYYY-MM-DD.json,
     with its record counts and the files' sizes and SHA-256 checksums

The manifest is uploaded last, so it only exists once both files are in
place: a day with a manifest is done, and a day without one (never run,
failed or interrupted) is harvested again from scratch. Nothing is kept on
local disk between runs, so the script can run on a fresh machine such as a
GitHub Actions runner (see .github/workflows/datacite-harvest.yml).

A day that fails is reported and skipped; the remaining days still run and
the script exits with an error at the end.

Settings come from automation/config.yaml (`storage` and `datacite_harvest`
sections). BUNNY_STORAGE_PASSWORD is read from the repo's .env file or the
environment.

Usage:
    # Harvest the oldest day from START_DATE to a week ago (UTC; see
    # common.py) that has no manifest yet
    python automation/datacite_harvest.py

    # Catch up on several days in one run (0 = all of them)
    python automation/datacite_harvest.py --max-days 0

    # Harvest one specific day / a range, one day at a time (inclusive)
    python automation/datacite_harvest.py --date 2026-05-01
    python automation/datacite_harvest.py --start 2026-05-01 --end 2026-05-07

    # Show which days are done or pending
    python automation/datacite_harvest.py --status

Use --force to re-harvest days that are already done. Their records are only
reloaded into Postgres if datacite_load.py is also run with --force.
"""

from __future__ import annotations

import argparse
import gzip
import shutil
import sys
import tempfile
import time
import traceback
from datetime import date
from pathlib import Path

import orjson

# pylint: disable=wrong-import-position,wrong-import-order
import common  # noqa: E402  (also puts src/ on sys.path)
from bunny_storage import BunnyStorage  # noqa: E402
from sindex.core.http import make_session  # noqa: E402
from sindex.sources.datacite.jobs import (  # noqa: E402
    harvest_datacite_datasets_for_date_range_to_ndjson,
)
from sindex.sources.datacite.normalize import slim_datacite_record  # noqa: E402

# pylint: enable=wrong-import-position,wrong-import-order

SOURCE = "datacite"


# ---------------------------------------------------------------------------
# Harvest
# ---------------------------------------------------------------------------


def gzip_file(src: Path, dest: Path) -> None:
    """Write a gzipped copy of `src` to `dest`."""
    with open(src, "rb") as f_in, gzip.open(dest, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out, 1024 * 1024)


def slim_file(raw_file: Path, slim_file_gz: Path) -> tuple[int, int]:
    """
    Slim every record in `raw_file` and write them, gzipped, to `slim_file_gz`.

    Returns:
        (kept, bad) record counts.
    """
    kept = bad = 0
    with open(raw_file, "rb") as f_in, gzip.open(slim_file_gz, "wb") as f_out:
        for line in f_in:
            if not line.strip():
                continue
            try:
                slim = slim_datacite_record(orjson.loads(line))
                f_out.write(orjson.dumps(slim) + b"\n")
                kept += 1
            except Exception as e:  # pylint: disable=broad-exception-caught
                # Skip records that fail to slim instead of failing the day
                bad += 1
                print(f"  Could not slim record: {e}")
    return kept, bad


def harvest_day(  # pylint: disable=too-many-locals
    day: date, cfg: dict, storage: BunnyStorage, session, work_dir: Path
) -> None:
    """
    Harvest and slim the DataCite dataset records created on `day`, and upload
    both files and then the day's manifest to the storage zone.

    The day's local files are deleted when it finishes, whether or not it
    succeeded.
    """
    day_iso = day.isoformat()
    day_dir = work_dir / day_iso
    day_dir.mkdir()
    t0 = time.monotonic()

    try:
        print(f"[{day_iso}] Harvesting DataCite datasets")
        progress = common.ProgressLog(f"[{day_iso}] Harvested")
        n_raw = harvest_datacite_datasets_for_date_range_to_ndjson(
            start_date_str=day_iso,
            end_date_str=day_iso,
            window_days=1,
            page_size=cfg["page_size"],
            save_folder=str(day_dir),
            polite_sleep_seconds=None,
            session=session,
            on_page=progress,
        )
        # The harvest function names its file datacite-<start>-<end>.ndjson
        raw_file = day_dir / f"datacite-{day_iso}-{day_iso}.ndjson"

        print(f"[{day_iso}] Slimming records")
        slim_gz = day_dir / "slim.ndjson.gz"
        kept, bad = slim_file(raw_file, slim_gz)
        raw_gz = day_dir / "raw.ndjson.gz"
        gzip_file(raw_file, raw_gz)

        print(f"[{day_iso}] Uploading files to the storage zone")
        raw = storage.upload_file(raw_gz, common.raw_path(SOURCE, day))
        slim = storage.upload_file(slim_gz, common.slim_path(SOURCE, day))

        manifest = {
            "source": SOURCE,
            "date": day_iso,
            "harvested_at": common.now_iso(),
            "duration_seconds": round(time.monotonic() - t0, 1),
            "raw_records": n_raw,
            "slim_records": kept,
            "bad_records": bad,
            "raw": raw,
            "slim": slim,
            "run_url": common.github_run_url(),
        }
        # Uploaded last: its presence marks the day as done
        storage.put_bytes(
            common.manifest_path(SOURCE, day),
            orjson.dumps(manifest, option=orjson.OPT_INDENT_2),
        )
    finally:
        shutil.rmtree(day_dir, ignore_errors=True)

    common.report(
        f"[{day_iso}] Raw: {n_raw:,} | Slim: {kept:,} | Bad: {bad:,} | "
        f"Took: {manifest['duration_seconds']:,.0f}s"
    )


def print_status(storage: BunnyStorage, days: list[date], done: set[date]) -> None:
    """Print one status line per day in `days`, then a summary of all of them."""
    total_raw = total_slim = 0
    for day in days:
        if day not in done:
            print(f"{day.isoformat()}  pending")
            continue
        m = orjson.loads(storage.get_bytes(common.manifest_path(SOURCE, day)))
        total_raw += m["raw_records"]
        total_slim += m["slim_records"]
        print(
            f"{day.isoformat()}  done     raw={m['raw_records']:,}"
            f"  slim={m['slim_records']:,}  bad={m['bad_records']:,}"
            f"  took={m['duration_seconds']:,.0f}s  harvested={m['harvested_at']}"
        )
    print(f"\n{len(days)} day(s): done={len(done)}, pending={len(days) - len(done)}")
    print(f"Done days: raw={total_raw:,}  slim={total_slim:,}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse the command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Harvest new DataCite dataset records one day at a time "
        "into the bunny.net storage zone."
    )
    common.add_common_args(parser)
    parser.add_argument(
        "--force", action="store_true", help="Re-harvest days that are already done."
    )
    return parser.parse_args()


def main() -> None:
    """Harvest every day in the requested range that is not done yet."""
    args = parse_args()
    cfg = common.load_config(args.config)
    hcfg = cfg["datacite_harvest"]
    days = common.resolve_days(args)
    storage = common.make_storage(cfg)
    done = common.list_harvested_days(storage, SOURCE, days)

    if args.status:
        print_status(storage, days, done)
        return

    todo = [d for d in days if args.force or d not in done]
    print(
        f"{len(days)} day(s) in range, {len(done)} already done, "
        f"{len(todo)} to harvest."
    )
    todo = common.limit_days(todo, args.max_days)

    session = make_session(total_retries=6, backoff=2.0)
    failed = []
    with tempfile.TemporaryDirectory(prefix="datacite-harvest-") as work_dir:
        for i, day in enumerate(todo):
            if day in done:
                # Re-harvesting: drop the manifest first so the day never
                # looks done while its files are being replaced
                storage.delete(common.manifest_path(SOURCE, day))
            try:
                harvest_day(day, hcfg, storage, session, Path(work_dir))
            except Exception as e:  # pylint: disable=broad-exception-caught
                common.report(f"[{day.isoformat()}] Failed: {e!r}")
                traceback.print_exc()
                failed.append(day)
            if hcfg.get("sleep_seconds") and i < len(todo) - 1:
                time.sleep(hcfg["sleep_seconds"])

    if todo:
        common.report(
            f"Done. Harvested {len(todo) - len(failed)} of {len(todo)} day(s)."
        )
    if failed:
        sys.exit(f"{len(failed)} day(s) failed: {', '.join(map(str, failed))}")


if __name__ == "__main__":
    main()
