import glob
import gzip
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor

import duckdb
import pandas as pd

from sindex.core.ids import _norm_doi


def shorten_id(url):
    return url.split("/")[-1] if url else None


def process_single_file(args):
    file_path, meta_out, cite_out = args

    # Example filename: 'updated_date=2024-01-01_part_000'
    parent_dir = os.path.basename(os.path.dirname(file_path))
    base_name = os.path.basename(file_path)
    unique_name = f"{parent_dir}_{base_name}"

    final_meta_path = f"{meta_out}/meta_{unique_name}.parquet"
    final_cite_path = f"{cite_out}/cite_{unique_name}.parquet"

    # We check if the final output files already exist, if so skip
    if (
        os.path.exists(final_meta_path)
        and os.path.getsize(final_meta_path) > 0
        and os.path.exists(final_cite_path)
        and os.path.getsize(final_cite_path) > 0
    ):
        return "Skipped"

    metadata_list, citations_list = [], []
    try:
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            for line in f:
                w = json.loads(line)
                work_id = shorten_id(w.get("id"))
                norm_doi = _norm_doi(w.get("doi") or "")

                # Table 1: Metadata
                topic = w.get("primary_topic") or {}
                metadata_list.append(
                    {
                        "oa_id": work_id,
                        "doi": norm_doi,
                        "pub_date": w.get("publication_date")
                        or str(w.get("publication_year", "")),
                        "topic_id": shorten_id(topic.get("id")),
                        "topic_name": topic.get("display_name"),
                        "topic_score": topic.get("score"),
                    }
                )

                # Table 2: Citations
                for r_url in w.get("referenced_works", []):
                    citations_list.append(
                        {"citing_oa_id": work_id, "cited_oa_id": shorten_id(r_url)}
                    )

        # Save results
        df_m = (
            pd.DataFrame(metadata_list)
            if metadata_list
            else pd.DataFrame(
                columns=[
                    "oa_id",
                    "doi",
                    "pub_date",
                    "topic_id",
                    "topic_name",
                    "topic_score",
                ]
            )
        )
        df_m.to_parquet(final_meta_path, compression="zstd", index=False)

        df_c = (
            pd.DataFrame(citations_list)
            if citations_list
            else pd.DataFrame(columns=["citing_oa_id", "cited_oa_id"])
        )
        df_c.to_parquet(final_cite_path, compression="zstd", index=False)

        return "Processed"
    except Exception as e:
        return f"Error {unique_name}: {e}"


def run_openalex_sweep(source_dir, meta_out, cite_out, max_workers=None):
    """
    Orchestrate the entire extraction process from OA .gz to parquets.
    """
    all_files = [
        os.path.join(r, f)
        for r, _, fs in os.walk(source_dir)
        for f in fs
        if f.endswith(".gz")
    ]
    tasks = [(f, meta_out, cite_out) for f in all_files]

    print(f"Extracting from {len(tasks)} files")
    processed, skipped, errors = 0, 0, []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(process_single_file, tasks):
            if result == "Processed":
                processed += 1
            elif result == "Skipped":
                skipped += 1
            else:
                errors.append(result)

            # Update
            sys.stdout.write(
                f"\rProgress: {processed + skipped}/{len(tasks)} (New: {processed}, Skipped: {skipped})"
            )
            sys.stdout.flush()

    return {"processed": processed, "skipped": skipped, "errors": errors}


def create_target_doi_table(db_path, folder_path):
    """
    Add DOIs from ndjson slimmed metadata to DUckDB table
    """
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    con = duckdb.connect(db_path)
    con.execute("SET enable_progress_bar=false;")

    empty_files = []
    error_files = []

    # Discovery
    search_pattern = os.path.join(folder_path, "**", "*.ndjson").replace("\\", "/")
    files = glob.glob(search_pattern, recursive=True)

    con.execute("CREATE TABLE IF NOT EXISTS target_dois (doi VARCHAR)")
    con.execute("DELETE FROM target_dois")  # Clears previous partial runs

    # Initialize the table
    con.execute("CREATE OR REPLACE TABLE target_dois (doi VARCHAR)")

    total_files = len(files)
    current_doi_count = 0
    print(f"Starting ingestion of {total_files} files...")

    # Ingestion Loop
    for idx, f in enumerate(files, 1):
        if os.path.getsize(f) == 0:
            empty_files.append(f)
            continue

        clean_f = f.replace("\\", "/")

        try:
            # Native JSON reader probably best here
            con.execute(f"""
                INSERT INTO target_dois
                SELECT lower(id_obj.identifier)
                FROM (
                    SELECT unnest(identifiers) as id_obj
                    FROM read_json_auto('{clean_f}', 
                                       format='newline_delimited', 
                                       ignore_errors=true)
                )
                WHERE lower(id_obj.identifier_type) = 'doi'
                  AND id_obj.identifier IS NOT NULL;
            """)
        except Exception as e:
            error_files.append((f, str(e)))
            continue

        # Update progress and current row count every 5 files
        if idx % 5 == 0 or idx == total_files:
            current_doi_count = con.execute(
                "SELECT count(*) FROM target_dois"
            ).fetchone()[0]
            print(
                f"\rFiles: {idx}/{total_files} | Total DOIs found: {current_doi_count:,}",
                end="",
                flush=True,
            )

    # Finalize
    print("\n\nFinalizing: Deduplicating and Indexing")
    con.execute(
        "CREATE TABLE target_dois_unique AS SELECT DISTINCT doi FROM target_dois"
    )
    con.execute("DROP TABLE target_dois")
    con.execute("ALTER TABLE target_dois_unique RENAME TO target_dois")
    con.execute("CREATE INDEX idx_target_doi ON target_dois (doi)")

    final_count = con.execute("SELECT count(*) FROM target_dois").fetchone()[0]
    print("\n Completed")
    print(f"Final unique DOIs: {final_count:,}")
    print(f"Empty files: {len(empty_files)} | Error files: {len(error_files)}")

    con.close()


def process_openalex_topics_for_dois(
    db_path, meta_folder, mem_limit="32GB", temp_dir=None, file_limit=None
):
    """
    Scans OpenAlex metadata parquet files to find target DOIs, identifies their topics,
    and deduplicates them to keep one entry per DOI in the table.
    """
    start_time = time.time()
    con = duckdb.connect(db_path)

    # Configuration
    con.execute(f"SET memory_limit = '{mem_limit}';")
    con.execute("SET preserve_insertion_order = false;")
    if temp_dir:
        os.makedirs(temp_dir, exist_ok=True)
        clean_temp_path = temp_dir.replace("\\", "/")
        con.execute(f"SET temp_directory = '{clean_temp_path}';")

    # Initialize Tables
    con.execute(
        "CREATE TABLE IF NOT EXISTS processed_meta_files (filename VARCHAR PRIMARY KEY)"
    )

    # This acts as our "collection bucket"
    con.execute("""
        CREATE TABLE IF NOT EXISTS my_datasets_topics_raw (
            oa_id VARCHAR, doi VARCHAR, pub_date VARCHAR, 
            topic_id VARCHAR, topic_name VARCHAR, topic_score DOUBLE
        )
    """)

    # File Discovery
    search_pattern = os.path.join(meta_folder, "*.parquet").replace("\\", "/")
    all_files = sorted(glob.glob(search_pattern))
    if file_limit:
        all_files = all_files[:file_limit]

    done_files = {
        row[0]
        for row in con.execute("SELECT filename FROM processed_meta_files").fetchall()
    }
    files_to_process = [f for f in all_files if f not in done_files]

    if not files_to_process:
        print("All metadata files have already been processed.")
    else:
        print(f"Starting scan of {len(files_to_process)} metadata files...")
        for i, file_path in enumerate(files_to_process, 1):
            clean_path = file_path.replace("\\", "/")
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(f"""
                    INSERT INTO my_datasets_topics_raw
                    SELECT m.oa_id, m.doi, m.pub_date, m.topic_id, m.topic_name, m.topic_score
                    FROM read_parquet('{clean_path}') m
                    INNER JOIN target_dois t ON m.doi = t.doi
                """)
                con.execute("INSERT INTO processed_meta_files VALUES (?)", [file_path])
                con.execute("COMMIT")
            except Exception as e:
                con.execute("ROLLBACK")
                print(f"\n[!] Error: {e}")
                break

            # Progress Reporting
            current_elapsed = time.time() - start_time
            time_str = time.strftime("%M:%S", time.gmtime(current_elapsed))
            print(
                f"\rScanned: {len(done_files) + i}/{len(all_files)} | Elapsed: {time_str}",
                end="",
                flush=True,
            )

    # Final deduplication into the requested table name
    print("\nFinalizing: Deduplicating and creating 'my_datasets_topics'...")
    con.execute("""
        CREATE OR REPLACE TABLE my_datasets_topics AS
        SELECT * EXCLUDE (row_num) FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY doi ORDER BY topic_score DESC) as row_num
            FROM my_datasets_topics_raw
        ) WHERE row_num = 1
    """)

    # Drop the raw table to save space
    con.execute("DROP TABLE my_datasets_topics_raw")

    final_count = con.execute("SELECT count(*) FROM my_datasets_topics").fetchone()[0]
    print(
        f"Done! Unique entries: {final_count:,} | Total Time: {(time.time() - start_time) / 60:.2f} min"
    )
    con.close()


def process_openalex_citations_for_dois(
    db_path, cite_folder, meta_folder, mem_limit="32GB", temp_dir=None
):
    """
    Creates 'my_datasets_citations' table by linking citations Parquets.
    Columns: cited_doi, cited_oa_id, citing_oa_id, citing_doi, citation_date
    """
    start_time = time.time()
    con = duckdb.connect(db_path)

    # Configuration
    con.execute(f"SET memory_limit = '{mem_limit}';")
    con.execute("SET enable_progress_bar = false;")
    if temp_dir:
        os.makedirs(temp_dir, exist_ok=True)
        clean_temp_path = temp_dir.replace("\\", "/")
        con.execute(f"SET temp_directory = '{clean_temp_path}';")

    # Citation bridge
    con.execute(
        "CREATE TABLE IF NOT EXISTS processed_cite_files (filename VARCHAR PRIMARY KEY)"
    )
    con.execute("""
        CREATE TABLE IF NOT EXISTS intermediate_links (
            citing_oa_id VARCHAR, 
            cited_oa_id VARCHAR, 
            cited_doi VARCHAR
        )
    """)

    cite_files = sorted(glob.glob(os.path.join(cite_folder, "*.parquet")))
    done_cite = {
        row[0]
        for row in con.execute("SELECT filename FROM processed_cite_files").fetchall()
    }
    to_process = [f for f in cite_files if f not in done_cite]

    if to_process:
        print(f"Step 1: Finding citations in {len(to_process)} files")
        for i, file_path in enumerate(to_process, 1):
            clean_path = file_path.replace("\\", "/")
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(f"""
                    INSERT INTO intermediate_links
                    SELECT c.citing_oa_id, c.cited_oa_id, d.doi
                    FROM read_parquet('{clean_path}') c
                    INNER JOIN my_datasets_topics d ON c.cited_oa_id = d.oa_id
                """)
                con.execute("INSERT INTO processed_cite_files VALUES (?)", [file_path])
                con.execute("COMMIT")
            except Exception as e:
                con.execute("ROLLBACK")
                print(f"\n[!] Error on {file_path}: {e}")
                break

            elapsed = time.time() - start_time
            print(
                f"\r > Progress: {len(done_cite) + i}/{len(cite_files)} | Elapsed: {time.strftime('%M:%S', time.gmtime(elapsed))}",
                end="",
                flush=True,
            )

    # Metadata lookup
    print("\n\nStep 2: Mapping citing IDs to DOIs and Dates")
    meta_glob = os.path.join(meta_folder, "*.parquet").replace("\\", "/")

    con.execute(f"""
        CREATE OR REPLACE TABLE my_datasets_citations AS
        SELECT 
            il.cited_doi, 
            il.cited_oa_id, 
            il.citing_oa_id,
            m.doi as citing_doi, 
            m.pub_date as citation_date
        FROM intermediate_links il
        LEFT JOIN read_parquet('{meta_glob}') m ON il.citing_oa_id = m.oa_id
    """)

    con.execute("DROP TABLE intermediate_links")

    total_min = (time.time() - start_time) / 60
    final_count = con.execute("SELECT count(*) FROM my_datasets_citations").fetchone()[
        0
    ]
    print(
        f"Done! Final citation count: {final_count:,} | Total Time: {total_min:.2f} min"
    )
    con.close()


def create_citation_weights_table(db_path):
    """
    Creates 'my_datasets_citations_weights' with a fallback weight of 1.0
    for citations with missing or invalid date information.
    """
    start_time = time.time()
    con = duckdb.connect(db_path)

    a = 0.33

    print(
        "Creating citation table with weights and pub_date (Defaulting NULL dates to 1.0)"
    )

    # COALESCE(calculation, 1.0) ensures that if the math fails (missing dates), we get a weight of 1.0
    con.execute(f"""
        CREATE OR REPLACE TABLE my_datasets_citations_weights AS
        SELECT 
            c.*, 
            t.pub_date AS pub_date,
            COALESCE(
                ROUND(
                    1.0 + {a} * ln(
                        1.0 + GREATEST(0, 
                            date_diff(
                                'day', 
                                TRY_CAST(t.pub_date AS DATE), 
                                TRY_CAST(c.citation_date AS DATE)
                            ) / 365.25
                        )
                    ), 2
                ), 
                1.0
            ) AS weight
        FROM my_datasets_citations c
        INNER JOIN my_datasets_topics t ON c.cited_oa_id = t.oa_id
    """)

    # Summary
    res = con.execute("""
        SELECT 
            COUNT(*), 
            COUNT(*) FILTER (WHERE weight = 1.0) as baseline_weights,
            AVG(weight) 
        FROM my_datasets_citations_weights
    """).fetchone()

    elapsed = time.time() - start_time
    print(f"Success! Table created in {elapsed:.2f}s")
    print(f"Total Citations: {res[0]:,}")
    print(f"Standard Citations (Weight 1.0): {res[1]:,}")
    print(f"Average Impact Score: {res[2]:.2f}")

    con.close()
