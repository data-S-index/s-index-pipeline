import os
from datetime import datetime
from pathlib import Path

import duckdb

from sindex.core.dates import _norm_date_iso


def get_best_publication_date_datacite_record(attr):
    candidates = []

    # Extract "Issued" date - this seems most likely to be the date a dataset was published
    for d in attr.get("dates", []):
        if d.get("dateType") == "Issued" and d.get("date"):
            candidates.append(str(d.get("date")))
            break

    # Add other fallbacks to the candidate list
    candidates.append(attr.get("published"))
    candidates.append(attr.get("publicationYear"))

    # Iterate through candidates and return the first one that normalizes
    for candidate in candidates:
        if not candidate:
            continue
        try:
            return _norm_date_iso(str(candidate))
        except (ValueError, TypeError):
            continue  # Try the next candidate if normalization fails


def get_relevant_citations_block_from_ndjson(
    db_path, ndjson_folder, target_table, output_file_path, reset_log=False
):
    output_dir = os.path.dirname(output_file_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    con = duckdb.connect(db_path)

    con.execute("SET temp_directory = './duckdb_temp/'")
    con.execute("SET max_memory = '8GB'")

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
                A.publication_date,
                A.created_date
            FROM read_ndjson_auto(
                {new_files}, 
                columns={{
                    'identifiers': 'JSON[]', 
                    'citations': 'JSON', 
                    'publication_date': 'VARCHAR', 
                    'created_date': 'VARCHAR'
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
