"""
Load harvested DataCite days from the bunny.net storage zone into Postgres.

Second half of the DataCite automation, after datacite_harvest.py. For each
day that has a manifest in the storage zone but no "IngestLog" row yet, it:
  1. Downloads the day's slim file and checks it against the manifest's SHA-256
  2. Inserts each record as one row in "Dataset", plus its rows in
     "DatasetAuthor" and "DatasetIdentifier" (all ids come from the tables'
     own sequences)
  3. Adds the day's "IngestLog" row with its record counts

A day's inserts and its "IngestLog" row are committed in one transaction, so
a day is either fully loaded and logged or not loaded at all; a failed or
interrupted day is simply loaded again on the next run. Days are loaded
oldest first, one per run by default (--max-days). A day that fails is reported
and skipped; the remaining days still run and the script exits with an error
at the end.

A record is skipped if "Dataset" already has a row with the same identifier
and identifierType (existing rows are not updated). A record the database
rejects is counted as failed and skipped without undoing the rest of the day.

Every inserted "Dataset" row gets url = TEST_URL so the uploaded records can
be found and deleted later (see the SQL below).

Settings come from automation/config.yaml (`storage` and `datacite_load`
sections). DATABASE_URL and BUNNY_STORAGE_PASSWORD
are read from the repo's .env file or the environment.

Usage:
    # Load the oldest harvested day from START_DATE to a week ago (UTC; see
    # common.py) that isn't loaded yet
    python automation/datacite_load.py

    # Catch up on several days in one run (0 = all of them)
    python automation/datacite_load.py --max-days 0

    # Load one specific day / a range (inclusive)
    python automation/datacite_load.py --date 2026-05-01
    python automation/datacite_load.py --start 2026-05-01 --end 2026-05-07

    # Show which days are harvested and loaded
    python automation/datacite_load.py --status

Use --force to load days again that already have an "IngestLog" row (e.g.
after re-harvesting them); their row is replaced with the new counts.
"""

from __future__ import annotations

import argparse
import gzip
import re
import sys
import tempfile
import time
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator

import orjson
import psycopg
from psycopg.rows import dict_row

# pylint: disable=wrong-import-position,wrong-import-order
import common  # noqa: E402  (also puts src/ on sys.path)
from bunny_storage import BunnyStorage  # noqa: E402

# pylint: enable=wrong-import-position,wrong-import-order

SOURCE = "datacite"

# Marks every dataset inserted by this script, so they can be deleted later
TEST_URL = "test"

# SQL to run on the database by hand:
#
# -- After any bulk load that sets ids explicitly (e.g. COPY), move the id
# -- sequences past the highest id, or inserts fail with a duplicate "_pkey"
# SELECT setval(pg_get_serial_sequence('"Dataset"', 'id'),
#               (SELECT MAX(id) FROM "Dataset"));
# SELECT setval(pg_get_serial_sequence('"DatasetAuthor"', 'id'),
#               (SELECT MAX(id) FROM "DatasetAuthor"));
# SELECT setval(pg_get_serial_sequence('"DatasetIdentifier"', 'id'),
#               (SELECT MAX(id) FROM "DatasetIdentifier"));
#
# -- Delete everything this script inserted ("DatasetAuthor" and
# -- "DatasetIdentifier" rows are removed by their cascading foreign keys),
# -- and its log rows so the days are loaded again
# DELETE FROM "Dataset" WHERE url = 'test';
# DELETE FROM "IngestLog" WHERE source = 'datacite';

DATASET_SQL = """
    INSERT INTO "Dataset" (source, identifier, "identifierType", url, title,
        description, version, publisher, "publisherId", "publishedAt",
        "pubYear", subjects, created, updated)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ("identifierType", identifier) DO NOTHING
    RETURNING id"""

AUTHOR_SQL = """
    INSERT INTO "DatasetAuthor" ("datasetId", "nameType", name,
        "nameIdentifiers", affiliations, created, updated)
    VALUES (%s, %s, %s, %s, %s, %s, %s)"""

IDENTIFIER_SQL = """
    INSERT INTO "DatasetIdentifier" ("datasetId", identifier, "identifierType",
        created, updated)
    VALUES (%s, %s, %s, %s, %s)"""

INGEST_LOG_SQL = """
    INSERT INTO "IngestLog" (source, day, records, inserted, skipped, failed,
        "filePath", "fileSha256", "harvestedAt", "durationSeconds", "runUrl",
        created, updated)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (source, day) DO UPDATE SET
        records = EXCLUDED.records,
        inserted = EXCLUDED.inserted,
        skipped = EXCLUDED.skipped,
        failed = EXCLUDED.failed,
        "filePath" = EXCLUDED."filePath",
        "fileSha256" = EXCLUDED."fileSha256",
        "harvestedAt" = EXCLUDED."harvestedAt",
        "durationSeconds" = EXCLUDED."durationSeconds",
        "runUrl" = EXCLUDED."runUrl",
        updated = EXCLUDED.updated"""

EPOCH_FALLBACK = datetime(1970, 1, 1, tzinfo=timezone.utc)

# Matches a trailing ISO 8601 offset like +05:30 or -22:00
TZ_OFFSET_RE = re.compile(r"[+-](\d{2}):\d{2}$")

# Control characters that Postgres text columns reject or that we don't want
BAD_CHARS_RE = re.compile("[\x00-\x1f\x7f-\x9f�]")


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------


def clean(s: str | None) -> str | None:
    """Remove control and replacement characters from a string."""
    return BAD_CHARS_RE.sub("", s) if s else s


def parse_published_at(value: str | None) -> datetime:
    """Parse an ISO date string, falling back to the epoch if it is invalid."""
    if not value or not isinstance(value, str):
        return EPOCH_FALLBACK
    s = value.strip()
    if s.endswith("Z"):
        s = f"{s[:-1]}+00:00"
    # Offsets beyond ±14:00 are invalid; drop them and assume UTC
    m = TZ_OFFSET_RE.search(s)
    if m and int(m.group(1)) > 14:
        s = s[:-6]
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return EPOCH_FALLBACK
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def extract_identifiers(record: dict) -> list[tuple[str, str]]:
    """Return the record's unique (identifier, identifierType) pairs, lowercased."""
    seen: dict[tuple[str, str], None] = {}
    for ident in record.get("identifiers") or []:
        value = ident.get("identifier")
        id_type = ident.get("identifier_type") or ident.get("identifierType")
        if value and id_type:
            seen[(value.lower(), id_type.lower())] = None
    return list(seen)


def dataset_params(record: dict, identifier: tuple[str, str], now: datetime) -> tuple:
    """Return the DATASET_SQL parameters for a record."""
    subjects = [clean(s) for s in record.get("subjects") or [] if isinstance(s, str)]
    return (
        record.get("source", ""),
        identifier[0],
        identifier[1],
        TEST_URL,
        clean(record.get("title", "")),
        clean(record.get("description")),
        record.get("version"),
        record.get("publisher"),
        record.get("publisher_id") or "",
        parse_published_at(record.get("publication_date")),
        record.get("pubyear") or 0,
        [s for s in subjects if s],
        now,
        now,
    )


def insert_batch(
    cur: psycopg.Cursor, records: list[dict], now: datetime
) -> tuple[int, int]:
    """
    Insert a batch of records with their authors and identifiers.

    Each table gets one pipelined executemany, so a batch costs a few round
    trips to the database instead of a few per record.

    Returns:
        (inserted, skipped) counts. Records without an identifier, or with a
        dataset already in the database, are skipped.
    """
    rows = [(r, ids) for r in records if (ids := extract_identifiers(r))]
    skipped = len(records) - len(rows)
    if not rows:
        return 0, skipped

    cur.executemany(
        DATASET_SQL,
        [dataset_params(record, ids[0], now) for record, ids in rows],
        returning=True,
    )

    inserted = 0
    authors, identifiers = [], []
    for i, (record, ids) in enumerate(rows):
        if i:
            cur.nextset()
        row = cur.fetchone()
        if row is None:
            skipped += 1  # already in the database
            continue
        inserted += 1
        dataset_id = row[0]
        for author in record.get("creators") or []:
            authors.append(
                (
                    dataset_id,
                    author.get("name_type", ""),
                    clean(author.get("name", "")),
                    [clean(x) for x in author.get("identifiers") or []],
                    [clean(a) for a in author.get("affiliations") or []],
                    now,
                    now,
                )
            )
        identifiers.extend(
            (dataset_id, value, id_type, now, now) for value, id_type in ids
        )

    if authors:
        cur.executemany(AUTHOR_SQL, authors)
    if identifiers:
        cur.executemany(IDENTIFIER_SQL, identifiers)
    return inserted, skipped


def insert_records(
    conn: psycopg.Connection, records: list[dict], now: datetime, counts: dict
) -> None:
    """
    Insert a batch of records in a savepoint, adding to `counts`.

    If the database rejects the batch, it is rolled back and retried one
    record at a time, so only the bad record(s) are counted as failed.
    """
    try:
        with conn.transaction(), conn.cursor() as cur:
            inserted, skipped = insert_batch(cur, records, now)
    except psycopg.DataError as e:
        if len(records) == 1:
            counts["failed"] += 1
            print(f"  Could not insert record: {e}")
            return
        for record in records:
            insert_records(conn, [record], now, counts)
        return
    counts["inserted"] += inserted
    counts["skipped"] += skipped


def read_batches(path: Path, batch_size: int, counts: dict) -> Iterator[list[dict]]:
    """
    Yield the records of a gzipped ndjson file in lists of `batch_size`.

    Lines that aren't valid JSON are counted as failed and skipped.
    """
    batch = []
    with gzip.open(path, "rb") as f:
        for line in f:
            if not line.strip():
                continue
            counts["records"] += 1
            try:
                batch.append(orjson.loads(line))
            except orjson.JSONDecodeError as e:
                counts["failed"] += 1
                print(f"  Could not parse record: {e}")
                continue
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


# ---------------------------------------------------------------------------
# Days
# ---------------------------------------------------------------------------


def loaded_days(conn: psycopg.Connection, days: list[date]) -> dict[date, dict]:
    """Return the "IngestLog" rows of the days in `days`, keyed by day."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """SELECT day, records, inserted, skipped, failed, "durationSeconds",
                   updated
               FROM "IngestLog"
               WHERE source = %s AND day BETWEEN %s AND %s""",
            (SOURCE, min(days), max(days)),
        )
        return {row["day"]: row for row in cur}


def load_day(  # pylint: disable=too-many-arguments,too-many-locals
    day: date,
    conn: psycopg.Connection,
    storage: BunnyStorage,
    cfg: dict,
    work_dir: Path,
    *,
    force: bool,
) -> None:
    """
    Download a day's slim file and insert its records and "IngestLog" row in
    one transaction.
    """
    day_iso = day.isoformat()
    manifest = orjson.loads(storage.get_bytes(common.manifest_path(SOURCE, day)))
    slim = manifest["slim"]
    local_file = work_dir / f"{SOURCE}-{day_iso}.ndjson.gz"

    print(f"[{day_iso}] Downloading {slim['path']} ({slim['bytes']:,} bytes)")
    storage.download_file(slim["path"], local_file, sha256=slim["sha256"])

    t0 = time.monotonic()
    now = datetime.now(timezone.utc)
    counts = {"records": 0, "inserted": 0, "skipped": 0, "failed": 0}
    try:
        with conn.transaction():
            # Keep two runs (e.g. Actions and a local run) off the same day
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"IngestLog:{SOURCE}:{day_iso}",),
            )
            if not force and loaded_days(conn, [day]):
                print(f"[{day_iso}] Already loaded by another run; skipping")
                return

            print(f"[{day_iso}] Inserting {manifest['slim_records']:,} records")
            progress = common.ProgressLog(f"[{day_iso}] Inserted")
            for batch in read_batches(local_file, cfg["batch_size"], counts):
                insert_records(conn, batch, now, counts)
                progress(counts["records"], manifest["slim_records"])

            duration = round(time.monotonic() - t0, 1)
            conn.execute(
                INGEST_LOG_SQL,
                (
                    SOURCE,
                    day,
                    counts["records"],
                    counts["inserted"],
                    counts["skipped"],
                    counts["failed"],
                    slim["path"],
                    slim["sha256"],
                    datetime.fromisoformat(manifest["harvested_at"]),
                    duration,
                    common.github_run_url(),
                    now,
                    now,
                ),
            )
    finally:
        local_file.unlink(missing_ok=True)

    print(
        f"[{day_iso}] Inserted: {counts['inserted']:,} | "
        f"Skipped: {counts['skipped']:,} | Failed: {counts['failed']:,} | "
        f"Took: {duration:,.0f}s"
    )


def print_status(days: list[date], harvested: set[date], loaded: dict) -> None:
    """Print one status line per day in `days`, then a summary of all of them."""
    n_waiting = 0
    for day in days:
        row = loaded.get(day)
        if row:
            print(
                f"{day.isoformat()}  loaded     records={row['records']:,}"
                f"  inserted={row['inserted']:,}  skipped={row['skipped']:,}"
                f"  failed={row['failed']:,}  took={row['durationSeconds']:,.0f}s"
                f"  loaded={row['updated']:%Y-%m-%d %H:%M}"
            )
        elif day in harvested:
            n_waiting += 1
            print(f"{day.isoformat()}  harvested  (not loaded yet)")
        else:
            print(f"{day.isoformat()}  pending    (not harvested yet)")
    print(
        f"\n{len(days)} day(s): loaded={len(loaded)}, "
        f"harvested but not loaded={n_waiting}, "
        f"not harvested={len(days) - len(loaded) - n_waiting}"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse the command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Load harvested DataCite days from the bunny.net storage "
        "zone into Postgres."
    )
    common.add_common_args(parser)
    parser.add_argument(
        "--force",
        action="store_true",
        help='Load days again that already have an "IngestLog" row.',
    )
    return parser.parse_args()


def main() -> None:
    # return  # disable execution for now
    """Load every harvested day in the requested range that is not loaded yet."""
    args = parse_args()
    cfg = common.load_config(args.config)
    lcfg = cfg["datacite_load"]
    days = common.resolve_days(args)
    storage = common.make_storage(cfg)
    harvested = common.list_harvested_days(storage, SOURCE, days)

    # autocommit so each conn.transaction() block is its own transaction
    with psycopg.connect(common.get_secret("DATABASE_URL"), autocommit=True) as conn:
        loaded = loaded_days(conn, days)

        if args.status:
            print_status(days, harvested, loaded)
            return

        todo = sorted(d for d in harvested if args.force or d not in loaded)
        print(
            f"{len(days)} day(s) in range, {len(harvested)} harvested, "
            f"{len(loaded)} already loaded, {len(todo)} to load."
        )
        todo = common.limit_days(todo, args.max_days)

        failed = []
        with tempfile.TemporaryDirectory(prefix="datacite-load-") as work_dir:
            for day in todo:
                try:
                    load_day(day, conn, storage, lcfg, Path(work_dir), force=args.force)
                except Exception:  # pylint: disable=broad-exception-caught
                    print(f"[{day.isoformat()}] Failed:")
                    traceback.print_exc()
                    failed.append(day)

    print(f"Done. Loaded {len(todo) - len(failed)} of {len(todo)} day(s).")
    if failed:
        sys.exit(f"{len(failed)} day(s) failed: {', '.join(map(str, failed))}")


if __name__ == "__main__":
    main()
