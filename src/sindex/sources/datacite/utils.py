import os
import sys
import time
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
    # 1. PRE-PROCESS DIRECTORY & RESET
    output_dir = os.path.dirname(output_file_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # If reset is requested, kill the output file once BEFORE the loop starts
    if reset_log and os.path.exists(output_file_path):
        print(f"[{datetime.now()}] Deleting old output file for fresh start...")
        os.remove(output_file_path)

    con = duckdb.connect(db_path)
    con.execute("SET enable_progress_bar = false;")
    start_time = time.time()

    con.execute(f"CREATE INDEX IF NOT EXISTS idx_ds_id ON {target_table} (dataset_id);")

    if reset_log:
        con.execute("DROP TABLE IF EXISTS processed_files_citation_blocks")

    con.execute("""
        CREATE TABLE IF NOT EXISTS processed_files_citation_blocks (
            filename VARCHAR PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    processed_set = set(
        r[0]
        for r in con.execute(
            "SELECT filename FROM processed_files_citation_blocks"
        ).fetchall()
    )
    all_files = [str(f) for f in Path(ndjson_folder).glob("*.ndjson")]
    new_files = [f for f in all_files if os.path.basename(f) not in processed_set]

    if not new_files:
        print(f"[{datetime.now()}] No new files to process.")
        con.close()
        return

    total_scanned = 0
    total_matched = 0
    total_saved = 0

    print(f"Processing {len(new_files)} files...")

    try:
        for i, file_path in enumerate(new_files, 1):
            filename = os.path.basename(file_path)

            stats = con.execute(f"""
                SELECT 
                    count(*) as scanned,
                    count(B.dataset_id) as matched,
                    count(CASE WHEN B.dataset_id IS NOT NULL 
                               AND A.citations IS NOT NULL 
                               AND A.citations != '{{}}'::JSON THEN 1 END) as saved
                FROM read_ndjson_auto(
                    '{file_path}', 
                    columns={{'identifiers': 'JSON[]', 'citations': 'JSON'}}
                ) AS A
                LEFT JOIN {target_table} AS B 
                  ON A.identifiers[1]->>'identifier' = B.dataset_id
            """).fetchone()

            f_scanned, f_matched, f_saved = stats
            total_scanned += f_scanned
            total_matched += f_matched
            total_saved += f_saved

            elapsed = time.time() - start_time
            sys.stdout.write(
                f"\rFiles: {i}/{len(new_files)} | Scanned: {total_scanned:,} | "
                f"Matched: {total_matched:,} | Saved: {total_saved:,} | {elapsed:.1f}s"
            )
            sys.stdout.flush()

            # 2. THE REFINED WRITE LOGIC
            if f_saved > 0:
                # Strictly check disk state:
                # If file exists and isn't empty, we MUST append.
                if (
                    os.path.exists(output_file_path)
                    and os.path.getsize(output_file_path) > 0
                ):
                    mode = " (APPEND TRUE, FORMAT JSON)"
                else:
                    mode = " (FORMAT JSON)"

                con.execute(f"""
                    COPY (
                        SELECT A.identifiers, A.citations
                        FROM read_ndjson_auto(
                            '{file_path}', 
                            columns={{'identifiers': 'JSON[]', 'citations': 'JSON'}}
                        ) AS A
                        INNER JOIN {target_table} AS B 
                           ON A.identifiers[1]->>'identifier' = B.dataset_id
                        WHERE A.citations IS NOT NULL 
                          AND A.citations != '{{}}'::JSON
                    ) TO '{output_file_path}' {mode};
                """)

            con.execute(
                "INSERT INTO processed_files_citation_blocks (filename) VALUES (?)",
                [filename],
            )

        print("\nProcessing Complete!")
        # Final physical verification
        if os.path.exists(output_file_path):
            with open(output_file_path, "rb") as f:
                actual_lines = sum(1 for _ in f)
            print(f"Verified Lines in File:   {actual_lines:,}")
            if actual_lines != total_saved:
                print("WARNING: Physical file count does not match counter!")

    except Exception as e:
        print(f"\n[{datetime.now()}] Error: {e}")
    finally:
        con.close()
