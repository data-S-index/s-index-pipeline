"""
Add loaded DataCite days to the Meilisearch "dataset" index.

Third step of the DataCite automation, after datacite_load.py. For each day
that has an "IngestLog" row but no "indexedAt" yet, it:
  1. Downloads the day's slim file (the "filePath" in its "IngestLog" row) and
     checks it against the row's "fileSha256"
  2. Looks up each record's "Dataset" row (by the same identifier
     datacite_load.py inserted it under) with its "DatasetAuthor" rows
  3. Sends those datasets to Meilisearch, waits for every task to succeed, and
     sets the day's "indexedAt" and "indexed" in "IngestLog"

Documents are keyed by "Dataset".id, so sending a dataset again replaces its
document: a day that fails or is interrupted is simply indexed again on the
next run. Records skipped by the load because their dataset was already in
the database are sent too (they replace the document that is already there).
Records without a "Dataset" row (failed in the load) are counted as missing.

Days are indexed oldest first, one per run by default (--max-days). A day that
fails is reported and skipped; the remaining days still run and the script
exits with an error at the end. Like datacite_load.py, nothing is sent while
the site is in maintenance mode.

Settings come from automation/config.yaml (`storage` and `datacite_index`
sections, plus `maintenance_url` from `datacite_load`). DATABASE_URL,
BUNNY_STORAGE_PASSWORD, MEILISEARCH_API_URL and MEILISEARCH_API_KEY are read
from the repo's .env file or the environment.

Usage:
    # Index the oldest loaded day from START_DATE to a week ago (UTC; see
    # common.py) that isn't indexed yet
    python automation/datacite_index.py

    # Catch up on several days in one run (0 = all of them)
    python automation/datacite_index.py --max-days 0

    # Index one specific day / a range (inclusive)
    python automation/datacite_index.py --date 2026-05-01
    python automation/datacite_index.py --start 2026-05-01 --end 2026-05-07

    # Show which days are loaded and indexed
    python automation/datacite_index.py --status

Use --force to index days again that already have an "indexedAt".
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
import traceback
from datetime import date, datetime, timezone
from pathlib import Path

import orjson
import psycopg
from psycopg.rows import dict_row

# pylint: disable=wrong-import-position,wrong-import-order
import common  # noqa: E402  (also puts src/ on sys.path)
from bunny_storage import BunnyStorage  # noqa: E402
from datacite_load import (  # noqa: E402
    extract_identifiers,
    maintenance_mode,
    read_batches,
)
from sindex.core.http import make_session  # noqa: E402

# pylint: enable=wrong-import-position,wrong-import-order

SOURCE = "datacite"

# SQL to run on the database by hand (also add the two fields to the
# IngestLog model in prisma/schema.prisma):
#
# ALTER TABLE "IngestLog" ADD COLUMN "indexedAt" TIMESTAMP(3);
# ALTER TABLE "IngestLog" ADD COLUMN "indexed" INTEGER;

DATASETS_SQL = """
    SELECT d.id, d.identifier, d.title, d."publishedAt", d.subjects
    FROM "Dataset" d
    JOIN unnest(%s::text[], %s::text[]) AS k(identifier, "identifierType")
      ON d."identifierType" = k."identifierType" AND d.identifier = k.identifier"""

AUTHORS_SQL = """
    SELECT "datasetId", name, "nameIdentifiers"
    FROM "DatasetAuthor"
    WHERE "datasetId" = ANY(%s)
    ORDER BY "datasetId", id"""

MARK_INDEXED_SQL = """
    UPDATE "IngestLog" SET "indexedAt" = %s, indexed = %s
    WHERE source = %s AND day = %s AND "fileSha256" = %s"""

# Meilisearch task statuses after which a task won't change any more
TASK_DONE = ("succeeded", "failed", "canceled")


# ---------------------------------------------------------------------------
# Meilisearch
# ---------------------------------------------------------------------------


class Meilisearch:
    """Minimal client for adding documents to one Meilisearch index."""

    def __init__(self, url: str, key: str, index: str):
        self.url = url.rstrip("/")
        self.index = index
        # Adding documents is idempotent (same ids replace), so POSTs are retried
        self.session = make_session(allowed_methods=("GET", "POST"))
        self.session.headers["Authorization"] = f"Bearer {key}"

    def add_documents(self, documents: list[dict]) -> int:
        """Queue `documents` to be added or replaced. Returns the task uid."""
        resp = self.session.post(
            f"{self.url}/indexes/{self.index}/documents",
            params={"primaryKey": "id"},
            data=orjson.dumps(documents),
            headers={"Content-Type": "application/json"},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()["taskUid"]

    def wait_for_task(self, uid: int, timeout: float) -> None:
        """
        Wait until the task is done.

        Raises:
            RuntimeError: If it fails, is canceled, or isn't done in `timeout`
                seconds.
        """
        deadline = time.monotonic() + timeout
        while True:
            resp = self.session.get(f"{self.url}/tasks/{uid}", timeout=30)
            resp.raise_for_status()
            task = resp.json()
            if task["status"] == "succeeded":
                return
            if task["status"] in TASK_DONE:
                raise RuntimeError(
                    f"Meilisearch task {uid} {task['status']}: {task.get('error')}"
                )
            if time.monotonic() > deadline:
                raise RuntimeError(f"Meilisearch task {uid} not done after {timeout}s")
            time.sleep(2)


# ---------------------------------------------------------------------------
# Documents
# ---------------------------------------------------------------------------


def published_at(value: datetime) -> str:
    """Format "publishedAt" as in the documents of the initial bulk index."""
    dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def fetch_documents(cur: psycopg.Cursor, records: list[dict]) -> list[dict]:
    """Return the search documents of the records' "Dataset" rows."""
    keys = [ids[0] for r in records if (ids := extract_identifiers(r))]
    if not keys:
        return []
    cur.execute(DATASETS_SQL, ([k[0] for k in keys], [k[1] for k in keys]))
    documents = {
        row["id"]: {
            "id": row["id"],
            "identifier": row["identifier"],
            "title": row["title"],
            "publishedAt": published_at(row["publishedAt"]),
            "subjects": row["subjects"],
            "authors": [],
            "nameIdentifiers": [],
        }
        for row in cur
    }
    if not documents:
        return []

    cur.execute(AUTHORS_SQL, (list(documents),))
    for row in cur:
        doc = documents[row["datasetId"]]
        doc["authors"].append(row["name"] or "")
        doc["nameIdentifiers"].extend(
            x for x in row["nameIdentifiers"] or [] if x and x.strip()
        )
    for doc in documents.values():
        # Remove duplicates, keeping the order
        doc["nameIdentifiers"] = list(dict.fromkeys(doc["nameIdentifiers"]))
    return list(documents.values())


# ---------------------------------------------------------------------------
# Days
# ---------------------------------------------------------------------------


def ingest_rows(conn: psycopg.Connection, days: list[date]) -> dict[date, dict]:
    """Return the "IngestLog" rows of the days in `days`, keyed by day."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """SELECT day, records, inserted, "filePath", "fileSha256",
                   "indexedAt", indexed
               FROM "IngestLog"
               WHERE source = %s AND day BETWEEN %s AND %s""",
            (SOURCE, min(days), max(days)),
        )
        return {row["day"]: row for row in cur}


def index_day(  # pylint: disable=too-many-arguments,too-many-locals
    row: dict,
    conn: psycopg.Connection,
    storage: BunnyStorage,
    search: Meilisearch,
    cfg: dict,
    work_dir: Path,
) -> None:
    """Send a loaded day's datasets to Meilisearch and mark it indexed."""
    day_iso = row["day"].isoformat()
    local_file = work_dir / f"{SOURCE}-{day_iso}.ndjson.gz"

    print(f"[{day_iso}] Downloading {row['filePath']}", flush=True)
    t_download = time.monotonic()
    storage.download_file(row["filePath"], local_file, sha256=row["fileSha256"])
    print(
        f"[{day_iso}] Downloaded {local_file.stat().st_size / 1e6:,.1f} MB "
        f"in {time.monotonic() - t_download:,.1f}s",
        flush=True,
    )

    t0 = time.monotonic()
    counts = {"records": 0, "failed": 0}
    indexed = 0
    tasks = []
    try:
        print(
            f"[{day_iso}] Sending {row['records']:,} records "
            f"in batches of {cfg['batch_size']:,}",
            flush=True,
        )
        progress = common.ProgressLog(f"[{day_iso}] Sent")
        with conn.cursor(row_factory=dict_row) as cur:
            for n, batch in enumerate(
                read_batches(local_file, cfg["batch_size"], counts), start=1
            ):
                t_batch = time.monotonic()
                documents = fetch_documents(cur, batch)
                t_fetch = time.monotonic() - t_batch
                task = "-"
                if documents:
                    task = search.add_documents(documents)
                    tasks.append(task)
                    indexed += len(documents)
                print(
                    f"  [{day_iso}] Batch {n:,}: fetched {len(documents):,} of "
                    f"{len(batch):,} ({t_fetch:.1f}s) | added as task {task} "
                    f"({time.monotonic() - t_batch - t_fetch:.1f}s) | "
                    f"{counts['records']:,} / {row['records']:,} records",
                    flush=True,
                )
                progress(counts["records"], row["records"])
        progress(counts["records"], row["records"], final=True)
    finally:
        local_file.unlink(missing_ok=True)

    print(f"[{day_iso}] Waiting for {len(tasks)} Meilisearch task(s)", flush=True)
    waiting = common.ProgressLog(f"[{day_iso}] Tasks done", every_seconds=10)
    for n, uid in enumerate(tasks, start=1):
        search.wait_for_task(uid, cfg["task_timeout_seconds"])
        waiting(n, len(tasks), final=n == len(tasks))

    # Only if the day wasn't loaded again (--force) in the meantime
    conn.execute(
        MARK_INDEXED_SQL,
        (datetime.now(timezone.utc), indexed, SOURCE, row["day"], row["fileSha256"]),
    )
    common.report(
        f"[{day_iso}] Indexed: {indexed:,} | "
        f"Missing: {counts['records'] - counts['failed'] - indexed:,} | "
        f"Took: {time.monotonic() - t0:,.0f}s"
    )


def print_status(days: list[date], rows: dict) -> None:
    """Print one status line per day in `days`, then a summary of all of them."""
    n_indexed = 0
    for day in days:
        row = rows.get(day)
        if row and row["indexedAt"]:
            n_indexed += 1
            print(
                f"{day.isoformat()}  indexed    documents={row['indexed']:,}"
                f"  indexed={row['indexedAt']:%Y-%m-%d %H:%M}"
            )
        elif row:
            print(f"{day.isoformat()}  loaded     (not indexed yet)")
        else:
            print(f"{day.isoformat()}  pending    (not loaded yet)")
    print(
        f"\n{len(days)} day(s): indexed={n_indexed}, "
        f"loaded but not indexed={len(rows) - n_indexed}, "
        f"not loaded={len(days) - len(rows)}"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse the command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Add loaded DataCite days to the Meilisearch dataset index."
    )
    common.add_common_args(parser)
    parser.add_argument(
        "--force",
        action="store_true",
        help='Index days again that already have an "indexedAt".',
    )
    return parser.parse_args()


def main() -> None:
    """Index every loaded day in the requested range that is not indexed yet."""
    args = parse_args()
    cfg = common.load_config(args.config)
    icfg = cfg["datacite_index"]
    days = common.resolve_days(args)

    if not args.status and maintenance_mode(cfg["datacite_load"]["maintenance_url"]):
        common.report("The site is in maintenance mode; not indexing anything.")
        return

    with psycopg.connect(common.get_secret("DATABASE_URL"), autocommit=True) as conn:
        rows = ingest_rows(conn, days)

        if args.status:
            print_status(days, rows)
            return

        indexed = {d for d, r in rows.items() if r["indexedAt"]}
        todo = sorted(d for d in rows if args.force or d not in indexed)
        print(
            f"{len(days)} day(s) in range, {len(rows)} loaded, "
            f"{len(indexed)} already indexed, {len(todo)} to index."
        )
        todo = common.limit_days(todo, args.max_days)
        if not todo:
            print("Done. Nothing to index.")
            return

        storage = common.make_storage(cfg)
        search = Meilisearch(
            common.get_secret("MEILISEARCH_API_URL"),
            common.get_secret("MEILISEARCH_API_KEY"),
            icfg["index"],
        )

        failed = []
        with tempfile.TemporaryDirectory(prefix="datacite-index-") as work_dir:
            for day in todo:
                if maintenance_mode(cfg["datacite_load"]["maintenance_url"]):
                    common.report(
                        f"The site went into maintenance mode; stopping before "
                        f"{day.isoformat()}."
                    )
                    todo = todo[: todo.index(day)]
                    break
                try:
                    index_day(rows[day], conn, storage, search, icfg, Path(work_dir))
                except Exception as e:  # pylint: disable=broad-exception-caught
                    common.report(f"[{day.isoformat()}] Failed: {e!r}")
                    traceback.print_exc()
                    failed.append(day)

    if todo:
        common.report(f"Done. Indexed {len(todo) - len(failed)} of {len(todo)} day(s).")
    if failed:
        sys.exit(f"{len(failed)} day(s) failed: {', '.join(map(str, failed))}")


if __name__ == "__main__":
    main()
