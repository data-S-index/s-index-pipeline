import json
import os
import time
from typing import Any, Dict, List

from sindex.core.dates import _norm_date_iso, get_best_dataset_date, get_realistic_date
from sindex.core.ids import _norm_dataset_id
from sindex.metrics.dedup import dedupe_citations_by_link
from sindex.metrics.weights import citation_weight

from .client import make_duckdb_conn, register_mdc_udfs
from .constants import DEFAULT_DB_PATH, DEFAULT_MDC_PATTERN
from .discovery import list_mdc_files, mdc_glob


def build_mdc_index(
    mdc_folder: str,
    *,
    pattern: str = DEFAULT_MDC_PATTERN,
    db_path: str = DEFAULT_DB_PATH,
) -> str:
    files = list_mdc_files(mdc_folder, pattern)
    if not files:
        raise FileNotFoundError(
            f"No MDC files found in {mdc_folder} matching {pattern}"
        )

    con = make_duckdb_conn(db_path, read_only=False)
    register_mdc_udfs(con)

    glob_path = mdc_glob(mdc_folder, pattern)

    con.execute("DROP TABLE IF EXISTS mdc_index")

    # 1) Ingest + normalize
    con.execute(f"""
        CREATE TABLE mdc_index AS
        SELECT
            norm_dataset_id(dataset) AS dataset_norm,
            norm_doi_url_or_raw(publication) AS citation_link,
            norm_date_iso_safe(publishedDate) AS citation_date
        FROM read_json_auto('{glob_path}')
        WHERE dataset IS NOT NULL
          AND publication IS NOT NULL;
    """)

    # 2) Clean failures
    con.execute("""
        DELETE FROM mdc_index
        WHERE dataset_norm IS NULL OR citation_link IS NULL OR citation_link = '';
    """)

    # 3) Dedupe multiple entries of similar (dataset_norm, citation_link)
    con.execute("DROP TABLE IF EXISTS mdc_index_dedup")
    con.execute("""
        CREATE TABLE mdc_index_dedup AS
        SELECT
            dataset_norm,
            citation_link,
            any_value(citation_date) AS citation_date
        FROM mdc_index
        GROUP BY dataset_norm, citation_link;
    """)
    con.execute("DROP TABLE mdc_index")
    con.execute("ALTER TABLE mdc_index_dedup RENAME TO mdc_index")

    # 4) Index for fast point lookups
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_mdc_dataset_norm ON mdc_index(dataset_norm)"
    )
    con.execute("ANALYZE mdc_index")

    con.close()
    return db_path


def find_citations_mdc_duckdb(
    target_id: str,
    *,
    dataset_pub_date: str | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> List[Dict[str, Any]]:
    target_norm = _norm_dataset_id(target_id)
    if not target_norm:
        return []

    if dataset_pub_date:
        try:
            dataset_pub_date = _norm_date_iso(dataset_pub_date)
            dataset_pub_date = get_realistic_date(dataset_pub_date)
        except ValueError:
            dataset_pub_date = None

    out: List[Dict[str, Any]] = []

    with make_duckdb_conn(db_path, read_only=True) as con:
        rows = con.execute(
            """
            SELECT citation_link, citation_date
            FROM mdc_index
            WHERE dataset_norm = ?
            """,
            [target_norm],
        ).fetchall()

    for citation_link, citation_date_raw in rows:
        citation_date = None
        if citation_date_raw:
            try:
                norm_iso_date = _norm_date_iso(str(citation_date_raw))
                citation_date = get_realistic_date(norm_iso_date)
            except ValueError:
                citation_date = None
        rec: Dict[str, Any] = {
            "dataset_id": target_id,
            "source": ["mdc"],
            "citation_link": citation_link,
            "citation_weight": citation_weight(dataset_pub_date, citation_date),
        }
        if citation_date:
            rec["citation_date"] = citation_date

        out.append(rec)

    return dedupe_citations_by_link(out)


def batch_find_citations_mdc_ducdb_optimized(
    slim_folder: str, out_ndjson: str, db_path: str = DEFAULT_DB_PATH
):
    start_time = time.time()

    # Filter for non-empty files
    all_files = [
        os.path.join(slim_folder, f)
        for f in os.listdir(slim_folder)
        if f.endswith(".ndjson")
    ]
    valid_files = [f for f in all_files if os.path.getsize(f) > 0]

    if not valid_files:
        print("No valid ndjson files found.")
        return

    # Create table with DOIs and dates
    with make_duckdb_conn(db_path, read_only=True) as con:
        # We use ignore_errors=True in case 1 line out of 50M is malformed
        # and format='auto' to let DuckDB handle the compression/nesting
        setup_query = """
        CREATE OR REPLACE TEMP TABLE input_data AS
        SELECT 
            (SELECT x.identifier FROM unnest(identifiers) AS t(x) 
             WHERE x.identifier_type = 'doi' LIMIT 1) AS doi,
            published_date,
            created_date
        FROM read_json_auto(?, ignore_errors=True)
        WHERE doi IS NOT NULL;
        """
        con.execute(setup_query, [valid_files])

        # Join with mdc citations table to find overlaping dois
        results = con.execute("""
            SELECT 
                i.doi,
                i.published_date,
                i.created_date,
                m.citation_link,
                m.citation_date
            FROM input_data i
            JOIN mdc_index m ON (_norm_dataset_id(i.doi) = m.dataset_norm)
        """)

        # Process matched citations and stream to ndjson file
        count = 0
        with open(out_ndjson, "w", encoding="utf-8") as f_out:
            while True:
                chunk = results.fetchmany(100000)
                if not chunk:
                    break

                for row in chunk:
                    doi, pub_d, cre_d, c_link, c_date_raw = row

                    # Apply your custom logic
                    dataset_date = get_best_dataset_date(pub_d, cre_d)

                    citation_date = None
                    if c_date_raw:
                        try:
                            # Standardize and check if realistic
                            norm_iso_date = _norm_date_iso(str(c_date_raw))
                            citation_date = get_realistic_date(norm_iso_date)
                        except (ValueError, TypeError):
                            citation_date = None

                    rec = {
                        "dataset_id": doi,
                        "source": ["mdc"],
                        "citation_link": c_link,
                        "citation_weight": citation_weight(dataset_date, citation_date),
                    }

                    if citation_date:
                        rec["citation_date"] = citation_date

                    f_out.write(json.dumps(rec) + "\n")
                    count += 1

                # Progress update
                elapsed = time.time() - start_time
                print(
                    f"\rCitations matched: {count:,} | Elapsed: {elapsed:.2f}s", end=""
                )

    print(f"\nProcessing complete. Output saved to {out_ndjson}")
