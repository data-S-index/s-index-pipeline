import json
import os
import time
from datetime import datetime
from pathlib import Path

import duckdb

from sindex.core.dates import (
    _DEFAULT_CIT_MEN_DATE,
    _DEFAULT_CIT_MEN_YEAR,
    _norm_date_iso,
    get_realistic_date,
    is_realistic_integer_year,
)
from sindex.metrics.weights import citation_weight_year


def get_relevant_citations_block_from_ndjson(
    db_path, ndjson_folder, target_table, output_file_path, reset_log=True
):
    output_dir = os.path.dirname(output_file_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    con = duckdb.connect(db_path)

    con.execute("SET temp_directory = './duckdb_temp/'")
    con.execute("SET max_memory = '8GB'")
    con.execute("PRAGMA enable_progress_bar")

    if reset_log:
        con.execute("DROP TABLE IF EXISTS processed_files_citation_blocks")
        if os.path.exists(output_file_path):
            os.remove(output_file_path)

    con.execute("""
        CREATE TABLE IF NOT EXISTS processed_files_citation_blocks (
            filename VARCHAR PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    initial_count = con.execute(
        "SELECT count(*) FROM processed_files_citation_blocks"
    ).fetchone()[0]

    processed_set = {
        r[0]
        for r in con.execute(
            "SELECT filename FROM processed_files_citation_blocks"
        ).fetchall()
    }

    all_files = [str(f) for f in Path(ndjson_folder).glob("*.ndjson")]
    new_files = [f for f in all_files if os.path.basename(f) not in processed_set]

    if not new_files:
        print(f"[{datetime.now()}] No new files to process.")
        print(
            f"Status: {len(all_files):,} files found in folder, all {initial_count:,} are already logged."
        )
        con.close()
        return

    print(f"[{datetime.now()}] Batch processing {len(new_files)} files...")

    try:
        con.execute(f"""
            CREATE TEMP TABLE batch_results AS
            SELECT 
                A.identifiers, 
                A.citations,
                A.pubyear
            FROM read_ndjson_auto(
                {new_files}, 
                columns={{
                    'identifiers': 'JSON[]', 
                    'citations': 'JSON', 
                    'pubyear': 'INTEGER'
                }}
            ) AS A
            INNER JOIN {target_table} AS B 
                ON A.identifiers[1]->>'identifier' = B.dataset_id
        """)

        metrics = con.execute("""
            SELECT 
                count(*) as total_matched,
                count(*) filter (WHERE citations IS NOT NULL) as total_saved
            FROM batch_results
        """).fetchone()

        total_matched, total_saved = metrics

        con.execute(f"""
            COPY (SELECT * FROM batch_results WHERE citations IS NOT NULL) 
            TO '{output_file_path}' (FORMAT JSON)
        """)

        file_entries = [(os.path.basename(f),) for f in new_files]
        con.executemany(
            "INSERT INTO processed_files_citation_blocks (filename) VALUES (?)",
            file_entries,
        )

        print(f"[{datetime.now()}] Complete!")
        print(f"{'Total DOIs Matched:':<25} {total_matched:,}")
        print(f"{'Total Records Saved:':<25} {total_saved:,}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        con.execute("DROP TABLE IF EXISTS batch_results")
        con.close()


def dc_citations_date_to_year(db_path, input_ndjson, output_ndjson):
    """
    Joins an citation NDJSON file with dataset DuckDB table to add publication years,
    then processes citation dates and year based weights.
    """
    con = duckdb.connect(db_path)

    query = """
        SELECT 
            f.dataset_id,
            t.pubyear,
            f.citation_link,
            CAST(date_part('year', TRY_CAST(f.citation_date AS TIMESTAMP)) AS INTEGER) as c_year,
            f.citation_date
        FROM read_json_auto(?) f
        INNER JOIN my_datasets t ON f.dataset_id = t.dataset_id
    """

    print(f"Starting DuckDB Join on {input_ndjson}")
    start_time = time.time()

    results = con.execute(query, [input_ndjson])

    count = 0
    with open(output_ndjson, "w", encoding="utf-8") as f_out:
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
                    "source": ["datacite"],
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
            print(f"\rCitations matched: {count:,} | Elapsed: {elapsed:.2f}s", end="")

    print(f"\nProcessing complete. Output saved to {output_ndjson}")
    print(f"Total citations matched: {count:,}")
    con.close()
