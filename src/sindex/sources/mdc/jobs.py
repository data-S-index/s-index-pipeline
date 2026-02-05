import json
import os
import time
from typing import Any, Dict, List

from sindex.core.dates import (
    _DEFAULT_CIT_MEN_DATE,
    _DEFAULT_CIT_MEN_YEAR,
    _norm_date_iso,
    get_realistic_date,
    is_realistic_integer_year,
)
from sindex.core.ids import _norm_dataset_id
from sindex.metrics.dedup import dedupe_citations_by_link
from sindex.metrics.weights import citation_weight_year

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

    # Ingest + normalize
    con.execute(f"""
        CREATE TABLE mdc_index AS
        SELECT
            norm_dataset_id(dataset) AS dataset_norm,
            norm_doi_url_or_raw(publication) AS citation_link,
            norm_date_iso_safe(publishedDate) AS citation_date,
            EXTRACT(YEAR FROM norm_date_iso_safe(publishedDate)::DATE)::INTEGER AS citation_year
        FROM read_json_auto('{glob_path}')
        WHERE dataset IS NOT NULL
          AND publication IS NOT NULL;
    """)

    # Clean failures
    con.execute("""
        DELETE FROM mdc_index
        WHERE dataset_norm IS NULL OR citation_link IS NULL OR citation_link = '';
    """)

    # Dedupe multiple entries of similar (dataset_norm, citation_link)
    con.execute("DROP TABLE IF EXISTS mdc_index_dedup")
    con.execute("""
        CREATE TABLE mdc_index_dedup AS
        SELECT
            dataset_norm,
            citation_link,
            any_value(citation_date) AS citation_date,
            any_value(citation_year) AS citation_year
        FROM mdc_index
        GROUP BY dataset_norm, citation_link;
    """)
    con.execute("DROP TABLE mdc_index")
    con.execute("ALTER TABLE mdc_index_dedup RENAME TO mdc_index")

    # Index for fast point lookups
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_mdc_dataset_norm ON mdc_index(dataset_norm)"
    )
    con.execute("ANALYZE mdc_index")

    con.close()
    return db_path


def find_citations_mdc_duckdb(
    target_id: str,
    *,
    dataset_pubyear: int | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> List[Dict[str, Any]]:
    target_norm = _norm_dataset_id(target_id)
    if not target_norm:
        return []

    if not is_realistic_integer_year(dataset_pubyear):
        dataset_pubyear = None

    out: List[Dict[str, Any]] = []

    with make_duckdb_conn(db_path, read_only=True) as con:
        rows = con.execute(
            """
            SELECT citation_link, citation_date, citation_year
            FROM mdc_index
            WHERE dataset_norm = ?
            """,
            [target_norm],
        ).fetchall()

    for citation_link, citation_date_raw, citation_year_raw in rows:
        citation_date = None
        if citation_date_raw:
            try:
                norm_iso_date = _norm_date_iso(str(citation_date_raw))
                citation_date = get_realistic_date(norm_iso_date)
            except ValueError:
                citation_date = None
        citation_year = None
        if is_realistic_integer_year(citation_year_raw):
            citation_year = citation_year_raw

        rec: Dict[str, Any] = {
            "dataset_id": target_id,
            "source": ["mdc"],
            "citation_link": citation_link,
            "citation_weight": citation_weight_year(dataset_pubyear, citation_year),
        }
        if citation_date:
            rec["citation_date"] = citation_date
            rec["placeholder_date"] = False
        else:
            rec["citation_date"] = _DEFAULT_CIT_MEN_DATE
            rec["placeholder_date"] = True

        if citation_year:
            rec["citation_year"] = citation_year
            rec["placeholder_year"] = False
        else:
            rec["citation_year"] = _DEFAULT_CIT_MEN_YEAR
            rec["placeholder_year"] = True

        out.append(rec)

    return dedupe_citations_by_link(out)


def batch_find_citations_mdc_ducdb_optimized(
    slim_folder: str, out_ndjson: str, db_path: str = DEFAULT_DB_PATH
):
    start_time = time.time()

    # Filter out non-empty files
    all_files = [
        os.path.join(slim_folder, f)
        for f in os.listdir(slim_folder)
        if f.endswith(".ndjson")
    ]
    valid_files = [f for f in all_files if os.path.getsize(f) > 0]

    if not valid_files:
        print("No valid ndjson files found.")
        return

    # Connect to DuckDB and process
    with make_duckdb_conn(db_path, read_only=True) as con:
        con.create_function("_norm_dataset_id", _norm_dataset_id)

        # Create temp table with priority ID logic and explicit schema
        setup_query = """
        CREATE OR REPLACE TEMP TABLE input_data AS
        SELECT 
            COALESCE(
                (SELECT x.identifier FROM unnest(identifiers) AS t(x) 
                 WHERE x.identifier_type = 'doi' LIMIT 1),
                identifiers[1].identifier
            ) AS best_id,
            pubyear
        FROM read_json_auto(?, 
            ignore_errors=True, 
            columns={
                'identifiers': 'STRUCT(identifier_type VARCHAR, identifier VARCHAR)[]',
                'pubyear': 'INTEGER'
            }
        )
        WHERE best_id IS NOT NULL;
        """
        con.execute(setup_query, [valid_files])

        # Join with mdc citations table using the normalized ID
        results = con.execute("""
            SELECT 
                i.best_id,
                i.pubyear,
                m.citation_link,
                m.citation_year,
                m.citation_date
            FROM input_data i
            JOIN mdc_index m ON (_norm_dataset_id(i.best_id) = m.dataset_norm)
        """)

        # Process matched citations and stream to ndjson file
        count = 0
        with open(out_ndjson, "w", encoding="utf-8") as f_out:
            while True:
                chunk = results.fetchmany(100000)
                if not chunk:
                    break

                for row in chunk:
                    best_id, pubyear, c_link, c_year, c_date_raw = row
                    dataset_pubyear = pubyear

                    citation_date = None
                    if c_date_raw:
                        try:
                            norm_iso_date = _norm_date_iso(str(c_date_raw))
                            citation_date = get_realistic_date(norm_iso_date)
                        except (ValueError, TypeError):
                            citation_date = None

                    citation_year = None
                    if is_realistic_integer_year(c_year):
                        citation_year = c_year

                    rec = {
                        "dataset_id": best_id,
                        "source": ["mdc"],
                        "citation_link": c_link,
                        "citation_weight": citation_weight_year(
                            dataset_pubyear, citation_year
                        ),
                    }

                    if citation_date:
                        rec["citation_date"] = citation_date
                        rec["placeholder_date"] = False
                    else:
                        rec["citation_date"] = _DEFAULT_CIT_MEN_DATE
                        rec["placeholder_date"] = True

                    if citation_year:
                        rec["citation_year"] = citation_year
                        rec["placeholder_year"] = False
                    else:
                        rec["citation_year"] = _DEFAULT_CIT_MEN_YEAR
                        rec["placeholder_year"] = True

                    f_out.write(json.dumps(rec) + "\n")
                    count += 1

                # Progress update
                elapsed = time.time() - start_time
                print(
                    f"\rCitations matched: {count:,} | Elapsed: {elapsed:.2f}s", end=""
                )

    print(f"\nProcessing complete. Output saved to {out_ndjson}")
    print(f"Total citations matched: {count:,}")
