"""
Check loaded DataCite days for metadata changes and apply them.

Runs alongside the harvest, load and index jobs. A day's records can change
at DataCite long after the day was harvested (new title, authors, subjects,
...). For each day that has an "IngestLog" row, it:
  1. Asks DataCite for the day's Dataset records (created or registered on the
     day) whose `updated` date is on or after the day's last check, or, if it
     was never checked, the day it was harvested ("harvestedAt")
  2. Downloads the day's raw and slim files from the storage zone and replaces
     the records DataCite returned with their current versions. Only DOIs
     already in the day's files are considered; others belong to another day.
  3. For each record whose slim version changed, updates its "Dataset" row
     (title, description, version, publisher, dates, subjects) and replaces
     its "DatasetAuthor" rows, but only where the values actually differ
  4. Uploads the new raw and slim files over the old ones, then the day's
     manifest with their new checksums and an "updates" section
  5. Sends the changed datasets to Meilisearch and waits for the tasks
  6. Sets the day's "updatesCheckedAt", "updatesFound" (records DataCite
     returned that are in the day's files), "updatesApplied" (datasets changed
     in the database) and "fileSha256" (the new slim file) in "IngestLog"

The database changes and the "IngestLog" row are committed in one
transaction, after the uploads and the Meilisearch tasks succeeded. If
anything fails, the transaction is rolled back and the day is checked again
from the same date on the next run; every step gives the same result when
repeated. The day's files are read with the checksums in the storage zone's
manifest (not "IngestLog"), so a run that uploaded new files but failed
before committing is picked up cleanly.

DOIs and identifiers aren't changed: records are matched to "Dataset" rows by
the DOI they were loaded under, and "DatasetIdentifier" rows are left alone.
A record without a "Dataset" row (failed in the load) is counted as missing.

Days are checked least recently checked first. A day is due again
`recheck_after_days` after its last check (or its harvest), and each run
stops starting new days after `run_seconds`. Like the other jobs, nothing is
written while the site is in maintenance mode. A day that fails is reported
and skipped; the remaining days still run and the script exits with an error
at the end.

Settings come from automation/config.yaml (`storage`, `datacite_harvest`
page_size, `datacite_update`, and `maintenance_url` from `datacite_load`).
DATABASE_URL, BUNNY_STORAGE_PASSWORD, MEILISEARCH_API_URL and
MEILISEARCH_API_KEY are read from the repo's .env file or the environment.

Usage:
    # Check every due day, least recently checked first, until run_seconds
    python automation/datacite_update.py

    # Check at most 10 days
    python automation/datacite_update.py --max-days 10

    # Check one day / a range now, even if it isn't due yet
    python automation/datacite_update.py --date 2024-03-01
    python automation/datacite_update.py --start 2024-03-01 --end 2024-03-07

    # See what would change, without writing, uploading or indexing anything
    python automation/datacite_update.py --date 2024-03-01 --dry-run

    # Show how many days are checked and due
    python automation/datacite_update.py --status
"""

from __future__ import annotations

import argparse
import gzip
import sys
import tempfile
import time
import traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import orjson
import psycopg
from psycopg.rows import dict_row

# pylint: disable=wrong-import-position,wrong-import-order
import common  # noqa: E402  (also puts src/ on sys.path)
from bunny_storage import BunnyStorage  # noqa: E402
from datacite_index import Meilisearch, fetch_documents  # noqa: E402
from datacite_load import (  # noqa: E402
    AUTHOR_SQL,
    author_values,
    dataset_params,
    extract_identifiers,
    maintenance_mode,
)
from sindex.core.dates import _parse_date_strict  # noqa: E402
from sindex.core.http import make_session  # noqa: E402
from sindex.core.ids import _norm_doi  # noqa: E402
from sindex.sources.datacite.discovery import stream_datacite_records  # noqa: E402
from sindex.sources.datacite.normalize import slim_datacite_record  # noqa: E402

# pylint: enable=wrong-import-position,wrong-import-order

SOURCE = "datacite"

# SQL to run on the database by hand (also add the fields to the IngestLog
# model in prisma/schema.prisma):
#
# ALTER TABLE "IngestLog" ADD COLUMN "updatesCheckedAt" TIMESTAMP(3);
# ALTER TABLE "IngestLog" ADD COLUMN "updatesFound" INTEGER;
# ALTER TABLE "IngestLog" ADD COLUMN "updatesApplied" INTEGER;

DAYS_SQL = """
    SELECT day, "harvestedAt", "updatesCheckedAt", "updatesFound",
        "updatesApplied", COALESCE("updatesCheckedAt", "harvestedAt") AS since
    FROM "IngestLog"
    WHERE source = %s AND day BETWEEN %s AND %s
    ORDER BY since, day"""

LOCK_DAY_SQL = """
    SELECT COALESCE("updatesCheckedAt", "harvestedAt") AS since
    FROM "IngestLog" WHERE source = %s AND day = %s FOR UPDATE"""

DATASET_IDS_SQL = """
    SELECT d.id, d.identifier, d."identifierType"
    FROM "Dataset" d
    JOIN unnest(%s::text[], %s::text[]) AS k(identifier, "identifierType")
      ON d."identifierType" = k."identifierType" AND d.identifier = k.identifier"""

# Only touches the row if one of the values differs. The casts match the
# columns, so the comparison sees the values as the database would store them.
UPDATE_DATASET_SQL = """
    UPDATE "Dataset" SET
        title = %(title)s, description = %(description)s,
        version = %(version)s, publisher = %(publisher)s,
        "publisherId" = %(publisher_id)s, "publishedAt" = %(published_at)s,
        "pubYear" = %(pub_year)s, subjects = %(subjects)s, updated = %(now)s
    WHERE id = %(id)s
      AND (title, description, version, publisher, "publisherId",
           "publishedAt", "pubYear", subjects)
          IS DISTINCT FROM
          (%(title)s::text, %(description)s::text, %(version)s::text,
           %(publisher)s::text, %(publisher_id)s::text,
           %(published_at)s::timestamp(3), %(pub_year)s::int,
           %(subjects)s::text[])
    RETURNING id"""

AUTHORS_SQL = """
    SELECT "datasetId", "nameType", name, "nameIdentifiers", affiliations
    FROM "DatasetAuthor"
    WHERE "datasetId" = ANY(%s)
    ORDER BY "datasetId", id"""

DELETE_AUTHORS_SQL = 'DELETE FROM "DatasetAuthor" WHERE "datasetId" = ANY(%s)'

TOUCH_DATASETS_SQL = 'UPDATE "Dataset" SET updated = %s WHERE id = ANY(%s)'

MARK_CHECKED_SQL = """
    UPDATE "IngestLog" SET "updatesCheckedAt" = %s, "updatesFound" = %s,
        "updatesApplied" = %s, "fileSha256" = COALESCE(%s, "fileSha256")
    WHERE source = %s AND day = %s"""


class DryRun(Exception):
    """Raised to roll back a --dry-run day's transaction."""


# ---------------------------------------------------------------------------
# DataCite
# ---------------------------------------------------------------------------


def raw_doi(record: dict) -> str:
    """Return a raw DataCite record's normalized DOI ("" if it has none)."""
    return _norm_doi((record.get("attributes") or {}).get("doi") or "")


def slim_doi(record: dict) -> str:
    """Return the DOI a slim record is loaded under ("" if it has none)."""
    ids = extract_identifiers(record)
    return ids[0][0] if ids else ""


def fetch_updates(
    day: date, since: date, page_size: int, session
) -> tuple[dict[str, bytes], dict[str, dict], int]:
    """
    Return the day's DataCite records updated on or after `since`.

    Returns:
        (raw, slim, bad): the raw records as JSON bytes and their slim
        versions, both keyed by DOI, and how many records failed to slim
        (those are left out of both).
    """
    day_iso = day.isoformat()
    raw, slim, bad = {}, {}, 0
    progress = common.ProgressLog(f"[{day_iso}] Fetched")
    for record in stream_datacite_records(
        day_iso,
        day_iso,
        page_size=page_size,
        updated_after=since.isoformat(),
        include_registered=True,
        session=session,
        on_page=progress,
    ):
        doi = raw_doi(record)
        if not doi:
            continue
        try:
            slim[doi] = slim_datacite_record(record)
        except Exception as e:  # pylint: disable=broad-exception-caught
            bad += 1
            print(f"  [{day_iso}] Could not slim {doi}: {e}")
            continue
        raw[doi] = orjson.dumps(record)
    return raw, slim, bad


# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------


def rewrite_raw(src: Path, dest: Path, fresh: dict[str, bytes]) -> set[str]:
    """
    Copy the gzipped raw file `src` to `dest`, replacing the records in
    `fresh`. Returns the DOIs that were replaced.
    """
    replaced = set()
    with gzip.open(src, "rb") as f_in, gzip.open(dest, "wb") as f_out:
        for line in f_in:
            if not line.strip():
                continue
            doi = raw_doi(orjson.loads(line))
            if doi in fresh:
                replaced.add(doi)
                f_out.write(fresh[doi] + b"\n")
            else:
                f_out.write(line.rstrip(b"\n") + b"\n")
    return replaced


def rewrite_slim(
    src: Path, dest: Path, fresh: dict[str, dict]
) -> tuple[set[str], list[dict]]:
    """
    Copy the gzipped slim file `src` to `dest`, replacing the records in
    `fresh`.

    Returns:
        (found, changed): the DOIs of `fresh` that are in the file, and the
        new versions of the records that differ from the file's.
    """
    found, changed = set(), []
    with gzip.open(src, "rb") as f_in, gzip.open(dest, "wb") as f_out:
        for line in f_in:
            if not line.strip():
                continue
            old = orjson.loads(line)
            doi = slim_doi(old)
            if doi in fresh:
                found.add(doi)
                if fresh[doi] != old:
                    changed.append(fresh[doi])
                f_out.write(orjson.dumps(fresh[doi]) + b"\n")
            else:
                f_out.write(line.rstrip(b"\n") + b"\n")
    return found, changed


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def normalize_author(values: tuple) -> tuple:
    """Make an author's values comparable (None and "" / [] are the same)."""
    name_type, name, name_ids, affiliations = values
    return (
        name_type or "",
        name or "",
        list(name_ids or []),
        list(affiliations or []),
    )


def update_batch(  # pylint: disable=too-many-locals,too-many-branches
    cur: psycopg.Cursor, records: list[dict], now: datetime
) -> tuple:
    """
    Apply a batch of changed slim records to their "Dataset" and
    "DatasetAuthor" rows.

    Returns:
        (changed, missing): the records whose rows changed, and how many
        records have no "Dataset" row.
    """
    keys = {}
    for record in records:
        ids = extract_identifiers(record)
        if ids:
            keys[ids[0]] = record
    cur.execute(DATASET_IDS_SQL, ([k[0] for k in keys], [k[1] for k in keys]))
    by_id = {row[0]: keys[(row[1], row[2])] for row in cur.fetchall()}
    missing = len(records) - len(by_id)
    if not by_id:
        return [], missing

    params = []
    for dataset_id, record in by_id.items():
        p = dataset_params(record, extract_identifiers(record)[0], now)
        params.append(
            {
                "id": dataset_id,
                "title": p[4],
                "description": p[5],
                "version": p[6],
                "publisher": p[7],
                "publisher_id": p[8],
                "published_at": p[9],
                "pub_year": p[10],
                "subjects": p[11],
                "now": now,
            }
        )
    cur.executemany(UPDATE_DATASET_SQL, params, returning=True)
    changed_ids = set()
    for i in range(len(params)):
        if i:
            cur.nextset()
        row = cur.fetchone()
        if row:
            changed_ids.add(row[0])

    existing: dict[int, list[tuple]] = {i: [] for i in by_id}
    cur.execute(AUTHORS_SQL, (list(by_id),))
    for row in cur.fetchall():
        existing[row[0]].append(normalize_author(row[1:]))
    authors_changed = []
    new_authors = []
    for dataset_id, record in by_id.items():
        values = [author_values(a) for a in record.get("creators") or []]
        if [normalize_author(v) for v in values] != existing[dataset_id]:
            authors_changed.append(dataset_id)
            new_authors.extend((dataset_id, *v, now, now) for v in values)
    if authors_changed:
        cur.execute(DELETE_AUTHORS_SQL, (authors_changed,))
        if new_authors:
            cur.executemany(AUTHOR_SQL, new_authors)
        only_authors = [i for i in authors_changed if i not in changed_ids]
        if only_authors:
            cur.execute(TOUCH_DATASETS_SQL, (now, only_authors))
        changed_ids.update(authors_changed)

    return [by_id[i] for i in changed_ids], missing


def apply_changes(
    conn: psycopg.Connection, records: list[dict], now: datetime, counts: dict
) -> list[dict]:
    """
    Apply changed slim records in a savepoint, adding to `counts`.

    If the database rejects the batch, it is rolled back and retried one
    record at a time, so only the bad record(s) are counted as failed.

    Returns:
        The records whose rows changed.
    """
    try:
        with conn.transaction(), conn.cursor() as cur:
            changed, missing = update_batch(cur, records, now)
    except psycopg.DataError as e:
        if len(records) == 1:
            counts["failed"] += 1
            print(f"  Could not update {slim_doi(records[0])}: {e}")
            return []
        return [
            r for record in records for r in apply_changes(conn, [record], now, counts)
        ]
    counts["missing"] += missing
    return changed


# ---------------------------------------------------------------------------
# Days
# ---------------------------------------------------------------------------


def refresh_files(
    day: date, storage: BunnyStorage, fresh: tuple[dict, dict], work_dir: Path
) -> tuple[dict, dict, set[str], list[dict]]:
    """
    Download the day's raw and slim files (checked against the manifest) and
    write new copies with the `fresh` (raw, slim) records in place.

    Returns:
        (manifest, paths, found, changed): the day's manifest, the new local
        files by kind, the fresh DOIs that are in the day's files, and the
        slim records that changed.
    """
    manifest = orjson.loads(storage.get_bytes(common.manifest_path(SOURCE, day)))
    paths = {}
    for kind in ("raw", "slim"):
        old = work_dir / f"{kind}.old.gz"
        paths[kind] = work_dir / f"{kind}.new.gz"
        print(f"[{day.isoformat()}] Downloading {manifest[kind]['path']}", flush=True)
        storage.download_file(
            manifest[kind]["path"], old, sha256=manifest[kind]["sha256"]
        )
    rewrite_raw(work_dir / "raw.old.gz", paths["raw"], fresh[0])
    found, changed = rewrite_slim(work_dir / "slim.old.gz", paths["slim"], fresh[1])
    return manifest, paths, found, changed


def upload_files(
    day: date, storage: BunnyStorage, manifest: dict, paths: dict, updates: dict
) -> str:
    """
    Upload the new raw and slim files over the old ones, then the manifest
    with their checksums and `updates`. Returns the new slim file's SHA-256.
    """
    print(f"[{day.isoformat()}] Uploading the new files", flush=True)
    for kind in ("raw", "slim"):
        manifest[kind] = storage.upload_file(paths[kind], manifest[kind]["path"])
    manifest["updates"] = updates
    storage.put_bytes(
        common.manifest_path(SOURCE, day),
        orjson.dumps(manifest, option=orjson.OPT_INDENT_2),
    )
    return manifest["slim"]["sha256"]


def update_day(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    day: date,
    conn: psycopg.Connection,
    storage: BunnyStorage,
    search: Meilisearch | None,
    cfg: dict,
    work_dir: Path,
    *,
    session,
    dry_run: bool,
) -> None:
    """Check one day for updates and apply them (see the module docstring)."""
    day_iso = day.isoformat()
    t0 = time.monotonic()
    counts = dict.fromkeys(
        ("fetched", "bad", "found", "changed", "applied", "missing", "failed"), 0
    )
    try:
        with conn.transaction():
            # Keep this off a day datacite_load.py is (re)loading
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"IngestLog:{SOURCE}:{day_iso}",),
            )
            row = conn.execute(LOCK_DAY_SQL, (SOURCE, day)).fetchone()
            if row is None:
                print(f"[{day_iso}] No longer in IngestLog; skipping")
                return
            since = row[0].date()
            checked_at = datetime.now(timezone.utc)

            print(f"[{day_iso}] Fetching records updated since {since}", flush=True)
            raw, slim, counts["bad"] = fetch_updates(
                day, since, cfg["page_size"], session
            )
            counts["fetched"] = len(raw)

            new_sha, manifest, paths = None, {}, {}
            applied = []
            if raw:
                manifest, paths, found, changed = refresh_files(
                    day, storage, (raw, slim), work_dir
                )
                counts["found"], counts["changed"] = len(found), len(changed)
                now = datetime.now(timezone.utc)
                for i in range(0, len(changed), cfg["batch_size"]):
                    applied += apply_changes(
                        conn, changed[i : i + cfg["batch_size"]], now, counts
                    )
                counts["applied"] = len(applied)
                for record in applied:
                    print(f"  [{day_iso}] Updated {slim_doi(record)}")

            if dry_run:
                raise DryRun

            if counts["found"]:
                new_sha = upload_files(
                    day,
                    storage,
                    manifest,
                    paths,
                    {
                        "updated_at": common.now_iso(),
                        "since": since.isoformat(),
                        **{k: counts[k] for k in ("found", "changed", "applied")},
                        "run_url": common.github_run_url(),
                    },
                )
            if applied and search:
                send_documents(conn, search, applied, cfg, day_iso)

            conn.execute(
                MARK_CHECKED_SQL,
                (checked_at, counts["found"], counts["applied"], new_sha, SOURCE, day),
            )
    except DryRun:
        print(f"[{day_iso}] Dry run; rolled back")
    finally:
        for f in work_dir.glob("*.gz"):
            f.unlink(missing_ok=True)

    common.report(
        f"[{day_iso}] Fetched: {counts['fetched']:,} | "
        f"In file: {counts['found']:,} | Changed: {counts['changed']:,} | "
        f"Applied: {counts['applied']:,} | Missing: {counts['missing']:,} | "
        f"Failed: {counts['failed'] + counts['bad']:,} | "
        f"Took: {time.monotonic() - t0:,.0f}s"
    )


def send_documents(
    conn: psycopg.Connection,
    search: Meilisearch,
    records: list[dict],
    cfg: dict,
    day_iso: str,
) -> None:
    """Send the records' datasets to Meilisearch and wait for the tasks."""
    tasks = []
    with conn.cursor(row_factory=dict_row) as cur:
        for i in range(0, len(records), cfg["batch_size"]):
            documents = fetch_documents(cur, records[i : i + cfg["batch_size"]])
            if documents:
                tasks.append(search.add_documents(documents))
    print(f"[{day_iso}] Waiting for Meilisearch task(s) {tasks}", flush=True)
    deadline = time.monotonic() + cfg["task_wait_seconds"]
    for uid in tasks:
        if not search.wait_for_task(uid, deadline):
            raise TimeoutError(f"Meilisearch task {uid} not done in time")


def print_status(rows: list[dict], due: list[date], explicit: bool) -> None:
    """Print a summary of the days' checks (and one line per day if `explicit`)."""
    if explicit:
        for row in rows:
            if row["updatesCheckedAt"]:
                print(
                    f"{row['day'].isoformat()}  checked  "
                    f"{row['updatesCheckedAt']:%Y-%m-%d %H:%M}  "
                    f"found={row['updatesFound']:,}  "
                    f"applied={row['updatesApplied']:,}"
                )
            else:
                print(f"{row['day'].isoformat()}  never checked")
        print()
    checked = [r for r in rows if r["updatesCheckedAt"]]
    print(
        f"{len(rows)} day(s): checked={len(checked)}, "
        f"never checked={len(rows) - len(checked)}, due now={len(due)}"
    )
    if checked:
        applied = sum(r["updatesApplied"] or 0 for r in checked)
        print(f"Datasets updated so far: {applied:,}")
    if rows:
        oldest = rows[0]
        print(
            f"Least recently checked: {oldest['day']} "
            f"(since {oldest['since']:%Y-%m-%d})"
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse the command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Check loaded DataCite days for metadata changes and apply them."
    )
    parser.add_argument("--date", help="Single day (YYYY-MM-DD).")
    parser.add_argument("--start", help="First day of a range (YYYY-MM-DD).")
    parser.add_argument("--end", help="Last day of a range, inclusive (YYYY-MM-DD).")
    parser.add_argument(
        "--max-days",
        type=int,
        default=0,
        help="Most days to check in this run (default: 0 = until run_seconds).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change, without writing, uploading or indexing.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=common.DEFAULT_CONFIG,
        help=f"Path to config YAML (default: {common.DEFAULT_CONFIG}).",
    )
    parser.add_argument(
        "--status", action="store_true", help="Show how many days are checked and exit."
    )
    return parser.parse_args()


def day_range(args: argparse.Namespace) -> tuple[date, date, bool]:
    """Return the (first, last) day to consider and whether it was given."""
    if args.date and (args.start or args.end):
        sys.exit("Use either --date or --start/--end, not both.")
    if args.date:
        day = _parse_date_strict(args.date)
        return day, day, True
    first = _parse_date_strict(args.start) if args.start else date.min
    last = _parse_date_strict(args.end) if args.end else date.max
    if last < first:
        sys.exit(f"End date {last} is before start date {first}.")
    return first, last, bool(args.start or args.end)


def main() -> None:  # pylint: disable=too-many-locals
    """Check every due day (or the requested days) for updates."""
    args = parse_args()
    cfg = common.load_config(args.config)
    ucfg = {
        **cfg["datacite_update"],
        "page_size": cfg["datacite_harvest"]["page_size"],
    }
    maintenance_url = cfg["datacite_load"]["maintenance_url"]
    run_deadline = time.monotonic() + ucfg["run_seconds"]
    first, last, explicit = day_range(args)

    if not args.status and maintenance_mode(maintenance_url):
        common.report("The site is in maintenance mode; not updating anything.")
        return

    with psycopg.connect(common.get_secret("DATABASE_URL"), autocommit=True) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(DAYS_SQL, (SOURCE, first, last)).fetchall()
        # IngestLog times are stored as UTC without a time zone
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        due_before = now_utc - timedelta(days=ucfg["recheck_after_days"])
        todo = [r["day"] for r in rows if explicit or r["since"] < due_before]

        if args.status:
            print_status(rows, todo, explicit)
            return

        print(f"{len(rows)} day(s) in range, {len(todo)} to check.")
        if args.max_days and len(todo) > args.max_days:
            todo = todo[: args.max_days]
        if not todo:
            print("Done. Nothing to check.")
            return

        storage = common.make_storage(cfg)
        search = None
        if not args.dry_run:
            search = Meilisearch(
                common.get_secret("MEILISEARCH_API_URL"),
                common.get_secret("MEILISEARCH_API_KEY"),
                cfg["datacite_index"]["index"],
            )
        session = make_session(total_retries=6, backoff=2.0)

        done, failed = [], []
        with tempfile.TemporaryDirectory(prefix="datacite-update-") as work_dir:
            for day in todo:
                if time.monotonic() > run_deadline:
                    common.report(
                        f"Out of time; leaving {len(todo) - len(done) - len(failed)} "
                        "day(s) for the next run."
                    )
                    break
                if not args.dry_run and maintenance_mode(maintenance_url):
                    common.report(
                        f"The site went into maintenance mode; stopping before {day}."
                    )
                    break
                try:
                    update_day(
                        day,
                        conn,
                        storage,
                        search,
                        ucfg,
                        Path(work_dir),
                        session=session,
                        dry_run=args.dry_run,
                    )
                    done.append(day)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    common.report(f"[{day.isoformat()}] Failed: {e!r}")
                    traceback.print_exc()
                    failed.append(day)

    if done or failed:
        common.report(
            f"Done. Checked {len(done)} of {len(done) + len(failed)} day(s)."
        )
    if failed:
        sys.exit(f"{len(failed)} day(s) failed: {', '.join(map(str, failed))}")


if __name__ == "__main__":
    main()
