import glob
import gzip
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor

import duckdb
import pandas as pd

from sindex.core.dates import (
    _DEFAULT_CIT_MEN_DATE,
    _DEFAULT_CIT_MEN_YEAR,
    _norm_date_iso,
    get_best_dataset_date,
    get_realistic_date,
    is_realistic_integer_year,
)
from sindex.core.ids import _norm_doi
from sindex.metrics.weights import citation_weight_year


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
    Extracts raw data from OpenAlex snapshot .gz files into parquet files.

    Reads each .gz file in the source directory and produces two parquet files:
    - A metadata parquet containing oa_id, doi, publication date, and primary topic
    - A citation parquet containing (citing_oa_id, cited_oa_id) pairs

    Output parquet filenames are derived from the source .gz filename and its
    parent folder, so files from different snapshot folders can safely be written
    to the same output directories without collision.

    Already-extracted files are skipped automatically based on output file existence,
    making the function safe to re-run if interrupted.

    When to run:
        Run this once each time a new OpenAlex snapshot is downloaded, before any
        DuckDB processing steps. Point source_dir at the newly downloaded snapshot
        folder.

    Args:
        source_dir: Path to the folder containing downloaded OpenAlex .gz files
        meta_out: Path to the output folder for metadata parquets
        cite_out: Path to the output folder for citation parquets
        max_workers: Number of parallel processes (defaults to CPU count)
    """
    os.makedirs(meta_out, exist_ok=True)
    os.makedirs(cite_out, exist_ok=True)
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


def process_openalex_topics_for_datasets(
    db_path,
    dataset_db_path,
    meta_folders,
    new_datasets_since=None,
    mem_limit="32GB",
    temp_dir=None,
    file_limit=None,
    reset_file_tracking_table=False,
):
    """
    Scans OpenAlex metadata parquet files to find target DOIs and identify topics.

    Accepts multiple metadata folders processed in order, so newer parquets take
    priority over older ones for the same DOI. If a DOI is already matched it
    will not be overwritten by a later folder.

    When to run:
        Run after run_openalex_sweep() whenever new datasets have been added to
        my_datasets. Pass the newer parquet folder first in meta_folders so its
        topic assignments take priority. Include the full parquet history so new
        datasets can be matched across all snapshots.

    Args:
        db_path: Path to the main working DuckDB database where results are stored
        dataset_db_path: Path to the read-only dataset database containing my_datasets
        meta_folders: List of folders containing metadata parquet files, in priority
            order — newer/higher-priority folder first
        new_datasets_since: Optional timestamp string (e.g. '2026-05-01') to filter
            my_datasets by added_date, processing only datasets added after this date.
            If None, all datasets are searched.
        mem_limit: DuckDB memory limit
        temp_dir: Optional temp directory for DuckDB spill-to-disk
        file_limit: Optional cap on number of parquet files to process (for testing)
        reset_file_tracking_table: If True, resets only the file tracking table so
            all parquets are rescanned. Existing topics in my_datasets_topics are preserved.
    """
    start_time = time.time()
    con = duckdb.connect(db_path)

    # Attach the external database
    con.execute(f"ATTACH '{dataset_db_path}' AS dataset_db (READ_ONLY)")

    # Configuration
    con.execute(f"SET memory_limit = '{mem_limit}';")
    con.execute("SET enable_progress_bar = false;")
    con.execute("SET preserve_insertion_order = false;")
    if temp_dir:
        os.makedirs(temp_dir, exist_ok=True)
        clean_temp_path = temp_dir.replace("\\", "/")
        con.execute(f"SET temp_directory = '{clean_temp_path}';")

    # Optional tracking reset
    if reset_file_tracking_table:
        print("Resetting file tracking table, existing topics will be preserved...")
        con.execute("DROP TABLE IF EXISTS processed_meta_files")

    # Initialize Tables
    con.execute(
        "CREATE TABLE IF NOT EXISTS processed_meta_files (filename VARCHAR PRIMARY KEY)"
    )
    con.execute("""
        CREATE TABLE IF NOT EXISTS my_datasets_topics (
            oa_id VARCHAR,
            doi VARCHAR,
            topic_id VARCHAR,
            topic_name VARCHAR,
            topic_score DOUBLE,
            added_date TIMESTAMP
        )
    """)

    # Build dataset filter
    if new_datasets_since:
        dataset_filter = f"AND t.added_date >= '{new_datasets_since}'"
        print(f"Filtering to datasets added since {new_datasets_since}")
    else:
        dataset_filter = ""

    # File Discovery — collect from all folders in priority order (newer first)
    if isinstance(meta_folders, str):
        meta_folders = [meta_folders]

    all_files = []
    for folder in meta_folders:
        search_pattern = os.path.join(folder, "*.parquet").replace("\\", "/")
        all_files.extend(sorted(glob.glob(search_pattern)))

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
                    INSERT INTO my_datasets_topics
                    SELECT
                        m.oa_id,
                        m.doi,
                        m.topic_id,
                        m.topic_name,
                        m.topic_score,
                        CURRENT_TIMESTAMP AS added_date
                    FROM read_parquet('{clean_path}') m
                    INNER JOIN dataset_db.my_datasets t ON m.doi = t.dataset_id
                    {dataset_filter}
                    AND NOT EXISTS (
                        SELECT 1 FROM my_datasets_topics existing
                        WHERE existing.doi = m.doi
                    )
                """)
                con.execute("INSERT INTO processed_meta_files VALUES (?)", [file_path])
                con.execute("COMMIT")
            except Exception as e:
                con.execute("ROLLBACK")
                print(f"\n[!] Error processing {file_path}: {e}")
                break

            current_elapsed = time.time() - start_time
            time_str = time.strftime("%H:%M:%S", time.gmtime(current_elapsed))
            print(
                f"\rScanned: {len(done_files) + i}/{len(all_files)} | Elapsed: {time_str}",
                end="",
                flush=True,
            )

    con.execute("DETACH dataset_db")

    final_count = con.execute("SELECT count(*) FROM my_datasets_topics").fetchone()[0]
    print(f"\nDone! Unique entries: {final_count:,}")
    con.close()


def export_oa_topics_to_ndjson(db_path, output_path, since_date=None):
    """
    Exports OpenAlex topic assignments from my_datasets_topics to an NDJSON file.

    Each record contains dataset_id, topic_id, topic_name, score, and source.
    Only rows with a non-null topic_id are exported.

    Args:
        db_path: Path to the DuckDB database containing my_datasets_topics
        output_path: Path to the output NDJSON file
        since_date: Optional date string (e.g. '2026-05-01') to export only
            topics added on or after this date. If None, all topics are exported.
    """
    date_filter = f"AND added_date >= '{since_date}'" if since_date else ""

    with duckdb.connect(db_path) as con:
        row_count = con.execute(f"""
            SELECT count(*) 
            FROM my_datasets_topics 
            WHERE topic_id IS NOT NULL 
            AND topic_id != ''
            {date_filter}
        """).fetchone()[0]

        con.execute(f"""
            COPY (
                SELECT unpacked.* FROM (
                    SELECT 
                        struct_pack(
                            dataset_id := doi, 
                            topic_id := topic_id,
                            topic_name := topic_name,
                            score := topic_score,
                            source := 'openalex'
                        ) AS unpacked
                    FROM my_datasets_topics 
                    WHERE topic_id IS NOT NULL 
                    AND topic_id != ''
                    {date_filter}
                )
            ) TO '{output_path}' (FORMAT JSON)
        """)

    print(f"Export completed: {output_path}")
    print(f"Total rows written: {row_count:,}")


def process_openalex_citations_for_datasets_year(
    db_path,
    dataset_db_path,
    new_cite_folders,
    old_cite_folders,
    meta_folders,
    new_datasets_since=None,
    mem_limit="32GB",
    temp_dir=None,
    reset_step1=False,
):
    """
    Finds citations to datasets in my_datasets_topics and stores results in
    my_datasets_citations_year.

    Runs two passes in Step 1:
    - Pass 1: scans new_cite_folders for citations to all datasets
    - Pass 2: scans old_cite_folders for citations to new datasets only
      (filtered by new_datasets_since), catching historical citations to
      newly added datasets that predate the latest snapshot download.

    Step 2 resolves citing_oa_id to a DOI and publication date using metadata
    parquets, then joins against my_datasets to get the dataset pubyear.

    When to run:
        Run after process_openalex_topics_for_datasets(). Point new_cite_folders
        at only the new snapshot citation parquets. Point old_cite_folders at
        previous snapshot citation parquets and provide new_datasets_since to
        find historical citations to newly added datasets. Point meta_folders at
        all snapshots so citing works can be resolved fully.

    Args:
        db_path: Path to the main working DuckDB database where results are stored
        dataset_db_path: Path to the read-only dataset database containing my_datasets
        new_cite_folders: List of folders containing new snapshot citation parquets,
            scanned for citations to all datasets
        old_cite_folders: List of folders containing old snapshot citation parquets,
            scanned for citations to new datasets only
        meta_folders: List of folders containing metadata parquet files. Should
            include all snapshots so citing works can be resolved fully.
        new_datasets_since: Timestamp string (e.g. '2026-05-01') used to filter
            old_cite_folders to only match new datasets. Required when old_cite_folders
            is provided.
        mem_limit: DuckDB memory limit
        temp_dir: Optional temp directory for DuckDB spill-to-disk
        reset_step1: If True, resets both the file tracking table and intermediate
            links table for a clean Step 1 restart. Existing citations in
            my_datasets_citations_year are preserved.
    """
    start_time = time.time()
    con = duckdb.connect(db_path)

    # Attach the external database
    con.execute(f"ATTACH '{dataset_db_path}' AS dataset_db (READ_ONLY)")

    # Configuration
    con.execute(f"SET memory_limit = '{mem_limit}';")
    con.execute("SET enable_progress_bar = false;")
    if temp_dir:
        os.makedirs(temp_dir, exist_ok=True)
        clean_temp_path = temp_dir.replace("\\", "/")
        con.execute(f"SET temp_directory = '{clean_temp_path}';")

    # Optional resets
    if reset_step1:
        print("Resetting Step 1 tables, existing citations will be preserved...")
        con.execute("DROP TABLE IF EXISTS processed_cite_files")
        con.execute("DROP TABLE IF EXISTS intermediate_links")

    # Initialize Tables
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
    con.execute("""
        CREATE TABLE IF NOT EXISTS my_datasets_citations_year (
            cited_doi VARCHAR,
            cited_oa_id VARCHAR,
            citing_oa_id VARCHAR,
            citing_doi VARCHAR,
            citation_date VARCHAR,
            citation_year INTEGER,
            pubyear INTEGER,
            added_date TIMESTAMP
        )
    """)

    def scan_cite_files(folders, dataset_filter, pass_label):
        if isinstance(folders, str):
            folders = [folders]

        cite_files = []
        for folder in folders:
            cite_files.extend(sorted(glob.glob(os.path.join(folder, "*.parquet"))))

        done_cite = {
            row[0]
            for row in con.execute(
                "SELECT filename FROM processed_cite_files"
            ).fetchall()
        }
        to_process = [f for f in cite_files if f not in done_cite]

        if not to_process:
            print(f"{pass_label}: No new citation files to process.")
            return

        print(f"{pass_label}: Finding citations in {len(to_process)} files")
        for i, file_path in enumerate(to_process, 1):
            clean_path = file_path.replace("\\", "/")
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(f"""
                    INSERT INTO intermediate_links
                    SELECT c.citing_oa_id, c.cited_oa_id, d.doi
                    FROM read_parquet('{clean_path}') c
                    INNER JOIN my_datasets_topics d ON c.cited_oa_id = d.oa_id
                    {dataset_filter}
                    AND NOT EXISTS (
                        SELECT 1 FROM intermediate_links existing
                        WHERE existing.citing_oa_id = c.citing_oa_id
                        AND existing.cited_oa_id = c.cited_oa_id
                    )
                """)
                con.execute("INSERT INTO processed_cite_files VALUES (?)", [file_path])
                con.execute("COMMIT")
            except Exception as e:
                con.execute("ROLLBACK")
                print(f"\n[!] Error on {file_path}: {e}")
                break

            elapsed = time.time() - start_time
            print(
                f"\r > Progress: {i}/{len(to_process)} | Elapsed: {time.strftime('%H:%M:%S', time.gmtime(elapsed))}",
                end="",
                flush=True,
            )
        print()

    # Step 1 Pass 1: new cite folders — all datasets
    print("Step 1 - Pass 1: Scanning new citation parquets for all datasets")
    scan_cite_files(
        folders=new_cite_folders,
        dataset_filter="",
        pass_label="Pass 1",
    )

    # Step 1 Pass 2: old cite folders — new datasets only
    if old_cite_folders and new_datasets_since:
        print(
            f"Step 1 - Pass 2: Scanning old citation parquets for datasets added since {new_datasets_since}"
        )
        scan_cite_files(
            folders=old_cite_folders,
            dataset_filter=f"AND d.added_date >= '{new_datasets_since}'",
            pass_label="Pass 2",
        )
    elif old_cite_folders and not new_datasets_since:
        print(
            "Warning: old_cite_folders provided but new_datasets_since is missing — skipping Pass 2"
        )

    # Step 2: Resolving citing IDs to DOIs and joining pubyear
    print("\nStep 2: Mapping citing IDs and joining pubyear")

    if isinstance(meta_folders, str):
        meta_folders = [meta_folders]

    meta_glob_list = ", ".join(
        [
            f"'{os.path.join(folder, '*.parquet').replace(chr(92), '/')}'"
            for folder in meta_folders
        ]
    )

    count_before = con.execute(
        "SELECT count(*) FROM my_datasets_citations_year"
    ).fetchone()[0]

    con.execute(f"""
        INSERT INTO my_datasets_citations_year
        SELECT
            il.cited_doi,
            il.cited_oa_id,
            il.citing_oa_id,
            m.doi AS citing_doi,
            m.pub_date AS citation_date,
            YEAR(TRY_CAST(m.pub_date AS DATE)) AS citation_year,
            t.pubyear,
            CURRENT_TIMESTAMP AS added_date
        FROM intermediate_links il
        LEFT JOIN read_parquet([{meta_glob_list}]) m ON il.citing_oa_id = m.oa_id
        LEFT JOIN dataset_db.my_datasets t ON il.cited_doi = t.dataset_id
        WHERE NOT EXISTS (
            SELECT 1 FROM my_datasets_citations_year existing
            WHERE existing.citing_oa_id = il.citing_oa_id
            AND existing.cited_oa_id = il.cited_oa_id
        )
    """)

    con.execute("DROP TABLE IF EXISTS intermediate_links")
    con.execute("DETACH dataset_db")

    count_after = con.execute(
        "SELECT count(*) FROM my_datasets_citations_year"
    ).fetchone()[0]
    newly_added = count_after - count_before
    total_min = (time.time() - start_time) / 60

    print(
        f"Done! Newly added citations: {newly_added:,}, Total in table: {count_after:,}, Total Time: {total_min:.2f} min"
    )
    con.close()


def export_citations_to_ndjson_year(
    db_path, out_ndjson, since_date=None, batch_size=100000
):
    """
    Streams data from my_datasets_citations_year to an NDJSON file,
    calculating citation weights using pubyear and citation_year
    """
    start_time = time.time()
    con = duckdb.connect(db_path)
    con.execute("SET enable_progress_bar = false;")

    date_filter = f"WHERE added_date >= '{since_date}'" if since_date else ""

    results = con.execute(f"""
        SELECT 
            cited_doi,
            pubyear,
            citing_doi,
            citing_oa_id,
            citation_year,
            citation_date
        FROM my_datasets_citations_year
        {date_filter}
    """)

    count = 0
    print(f"Starting export to {out_ndjson}")

    with open(out_ndjson, "w", encoding="utf-8") as f_out:
        while True:
            chunk = results.fetchmany(batch_size)
            if not chunk:
                break

            for row in chunk:
                cited_doi, pubyear, citing_doi, citing_oa_id, c_year, c_date_raw = row
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

                # Citation link
                if citing_doi:
                    # Prefix DOI with https://doi.org/
                    citation_link = f"https://doi.org/{citing_doi}"
                else:
                    # Fallback to OpenAlex ID with https://openalex.org/
                    link_id = str(citing_oa_id).split("/")[
                        -1
                    ]  # Gets just the ID part if it's a full URL
                    citation_link = f"https://openalex.org/{link_id}"

                # Construct the JSON record
                rec = {
                    "dataset_id": cited_doi,
                    "source": ["openalex"],
                    "citation_link": citation_link,
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

            # Final progress update for the batch
            elapsed = time.time() - start_time
            print(
                f"\rProcessed: {count:,} | Elapsed: {elapsed:.2f}s", end="", flush=True
            )

    print(f"\nComplete. Total citation records: {count:,}")
    con.close()


def update_oa_pubdate_table(
    db_path, meta_folders, mem_limit="32GB", temp_dir=None, file_limit=None
):
    """
    Scans metadata parquet files and inserts new oa_id records into
    the openalex_pubdate table. Existing records are never modified.

    Processes all parquet files into a staging table first, then performs a
    single bulk insert at the end — only adding rows whose oa_id is not
    already in openalex_pubdate.

    When to run:
        Run after run_openalex_sweep() whenever new snapshot files have been
        extracted. Pass the new parquet folder in meta_folders.

    Args:
        db_path: Path to the DuckDB database where openalex_pubdate is stored
        meta_folders: List of folders containing metadata parquet files
        mem_limit: DuckDB memory limit
        temp_dir: Optional temp directory for DuckDB spill-to-disk
        file_limit: Optional cap on number of parquet files to process (for testing)
    """
    start_time = time.time()
    con = duckdb.connect(db_path)

    # Configuration
    con.execute(f"SET memory_limit = '{mem_limit}';")
    con.execute("SET enable_progress_bar = false;")
    con.execute("SET preserve_insertion_order = false;")

    if temp_dir:
        os.makedirs(temp_dir, exist_ok=True)
        clean_temp_path = temp_dir.replace("\\", "/")
        con.execute(f"SET temp_directory = '{clean_temp_path}';")

    # Initialize Table
    con.execute("""
        CREATE TABLE IF NOT EXISTS openalex_pubdate (
            oa_id VARCHAR,
            doi VARCHAR,
            pubdate VARCHAR
        )
    """)

    # File Discovery
    if isinstance(meta_folders, str):
        meta_folders = [meta_folders]

    all_files = []
    for folder in meta_folders:
        search_pattern = os.path.join(folder, "*.parquet").replace("\\", "/")
        all_files.extend(sorted(glob.glob(search_pattern)))

    if file_limit:
        all_files = all_files[:file_limit]

    if not all_files:
        print("No metadata files found.")
        con.close()
        return

    # Create staging table
    con.execute("DROP TABLE IF EXISTS temp_staging")
    con.execute("""
        CREATE TEMP TABLE temp_staging (
            oa_id VARCHAR,
            doi VARCHAR,
            pub_date VARCHAR
        )
    """)

    print(f"Starting ingestion of {len(all_files)} files into staging table")
    for i, file_path in enumerate(all_files, 1):
        clean_file_path = file_path.replace("\\", "/")
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"""
                INSERT INTO temp_staging
                SELECT oa_id, doi, pub_date
                FROM read_parquet('{clean_file_path}')
            """)
            con.execute("COMMIT")
        except Exception as e:
            con.execute("ROLLBACK")
            print(f"\n[!] Error processing {file_path}: {e}")
            break

        elapsed = time.time() - start_time
        print(
            f"\rProcessed: {i}/{len(all_files)} | "
            f"Elapsed: {time.strftime('%H:%M:%S', time.gmtime(elapsed))}",
            end="",
            flush=True,
        )

    # Ensure indexes exist before merge
    print("\nChecking/creating indexes")
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_oa_pubdate_oa_id ON openalex_pubdate (oa_id)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_oa_pubdate_doi ON openalex_pubdate (doi)"
    )
    print("\nIndexes verified on oa_id and doi columns.")

    # Bulk insert new oa_ids only
    print("\nMerging staging table into openalex_pubdate")

    staging_count = con.execute("SELECT count(*) FROM temp_staging").fetchone()[0]
    print(f"Rows in staging table: {staging_count:,}")

    count_before = con.execute("SELECT count(*) FROM openalex_pubdate").fetchone()[0]
    print(f"Rows in openalex_pubdate before merge: {count_before:,}")

    con.execute("""
        INSERT INTO openalex_pubdate
        SELECT s.oa_id, s.doi, s.pub_date
        FROM temp_staging s
        LEFT JOIN openalex_pubdate existing ON s.oa_id = existing.oa_id
        WHERE existing.oa_id IS NULL
    """)

    count_after = con.execute("SELECT count(*) FROM openalex_pubdate").fetchone()[0]
    new_rows = count_after - count_before
    print(f"New rows added: {new_rows:,}")
    print(f"Rows in openalex_pubdate after merge: {count_after:,}")

    con.execute("DROP TABLE IF EXISTS temp_staging")
    print("Merge complete.")

    final_count = con.execute("SELECT count(*) FROM openalex_pubdate").fetchone()[0]
    print(f"Done! Total items in 'openalex_pubdate': {final_count:,}")
    con.close()


## ---- OLD


def process_openalex_topics_for_datasets_old(
    db_path,
    dataset_db_path,
    meta_folder,
    mem_limit="32GB",
    temp_dir=None,
    file_limit=None,
    reset_tables=False,
):
    """
    Scans OpenAlex metadata parquet files to find target DOIs, identifies topics,
    and includes dates and a computed best_date using a Python UDF.
    """
    start_time = time.time()
    con = duckdb.connect(db_path)

    # Register the Python function as a DuckDB UDF
    try:
        con.execute("DROP FUNCTION get_best_date")
    except duckdb.CatalogException:
        pass

    con.create_function("get_best_date", get_best_dataset_date)

    # Attach the external database
    con.execute(f"ATTACH '{dataset_db_path}' AS dataset_db (READ_ONLY)")

    # Configuration
    con.execute(f"SET memory_limit = '{mem_limit}';")
    con.execute("SET enable_progress_bar = false;")
    con.execute("SET preserve_insertion_order = false;")
    if temp_dir:
        os.makedirs(temp_dir, exist_ok=True)
        clean_temp_path = temp_dir.replace("\\", "/")
        con.execute(f"SET temp_directory = '{clean_temp_path}';")

    # Optional Table Cleanup
    if reset_tables:
        print("Resetting tables: dropping existing progress and topic data...")
        con.execute("DROP TABLE IF EXISTS processed_meta_files")
        con.execute("DROP TABLE IF EXISTS my_datasets_topics")
        con.execute("DROP TABLE IF EXISTS my_datasets_topics_raw")

    # Initialize Tables
    con.execute(
        "CREATE TABLE IF NOT EXISTS processed_meta_files (filename VARCHAR PRIMARY KEY)"
    )

    con.execute("""
        CREATE TABLE IF NOT EXISTS my_datasets_topics_raw (
            oa_id VARCHAR, 
            doi VARCHAR, 
            publication_date VARCHAR, 
            created_date VARCHAR,
            topic_id VARCHAR, 
            topic_name VARCHAR, 
            topic_score DOUBLE
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
                    SELECT 
                        m.oa_id, 
                        m.doi, 
                        t.publication_date, 
                        t.created_date,
                        m.topic_id, 
                        m.topic_name, 
                        m.topic_score
                    FROM read_parquet('{clean_path}') m
                    INNER JOIN dataset_db.my_datasets t ON m.doi = t.dataset_id
                """)
                con.execute("INSERT INTO processed_meta_files VALUES (?)", [file_path])
                con.execute("COMMIT")
            except Exception as e:
                con.execute("ROLLBACK")
                print(f"\n[!] Error processing {file_path}: {e}")
                break

            current_elapsed = time.time() - start_time
            time_str = time.strftime("%M:%S", time.gmtime(current_elapsed))
            print(
                f"\rScanned: {len(done_files) + i}/{len(all_files)} | Elapsed: {time_str}",
                end="",
                flush=True,
            )

    # Final deduplication and computation of best_date
    if con.execute("SELECT count(*) FROM my_datasets_topics_raw").fetchone()[0] > 0:
        print("\nFinalizing: Computing 'best_date' and deduplicating...")
        con.execute("""
            CREATE OR REPLACE TABLE my_datasets_topics AS
            SELECT 
                * EXCLUDE (row_num),
                get_best_date(publication_date, created_date) AS best_date
            FROM (
                SELECT *, 
                ROW_NUMBER() OVER (PARTITION BY doi ORDER BY topic_score DESC) as row_num
                FROM my_datasets_topics_raw
            ) WHERE row_num = 1
        """)

    con.execute("DROP TABLE IF EXISTS my_datasets_topics_raw")
    con.execute("DETACH dataset_db")

    final_count = con.execute("SELECT count(*) FROM my_datasets_topics").fetchone()[0]
    print(
        f"\nDone! Unique entries: {final_count:,} | Total Time: {(time.time() - start_time) / 60:.2f} min"
    )
    con.close()


def process_openalex_citations_for_datasets_year_old(
    db_path,
    dataset_db_path,
    cite_folder,
    meta_folder,
    mem_limit="32GB",
    temp_dir=None,
    reset_tables=True,
):
    """
    Creates 'my_datasets_citations_year' table by linking citations Parquets.
    Links cited items to their original metadata and computes citation_year.
    """
    start_time = time.time()
    con = duckdb.connect(db_path)

    # Register the Python function as a DuckDB UDF
    try:
        con.execute("DROP FUNCTION get_best_date")
    except duckdb.CatalogException:
        pass

    con.create_function("get_best_date", get_best_dataset_date)

    # Attach the external database
    con.execute(f"ATTACH '{dataset_db_path}' AS dataset_db (READ_ONLY)")

    # Configuration
    con.execute(f"SET memory_limit = '{mem_limit}';")
    con.execute("SET enable_progress_bar = false;")
    if temp_dir:
        os.makedirs(temp_dir, exist_ok=True)
        clean_temp_path = temp_dir.replace("\\", "/")
        con.execute(f"SET temp_directory = '{clean_temp_path}';")

    # Optional Table Cleanup
    if reset_tables:
        print("Resetting tables: dropping existing citation progress and data...")
        con.execute("DROP TABLE IF EXISTS processed_cite_files")
        con.execute("DROP TABLE IF EXISTS intermediate_links")
        con.execute("DROP TABLE IF EXISTS my_datasets_citations_year")

    # Initialize Tracking Tables
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

    # Step 1: Finding citations
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

    # Step 2: Mapping citing IDs and joining external dates
    print("\n\nStep 2: Mapping citing IDs")
    meta_glob = os.path.join(meta_folder, "*.parquet").replace("\\", "/")

    # Join with external dataset_db.my_datasets using created_date
    con.execute(f"""
        CREATE OR REPLACE TABLE my_datasets_citations_year AS
        SELECT 
            il.cited_doi, 
            il.cited_oa_id, 
            il.citing_oa_id,
            m.doi as citing_doi, 
            m.pub_date as citation_date,
            YEAR(TRY_CAST(m.pub_date AS DATE)) as citation_year,
            t.pubyear
        FROM intermediate_links il
        LEFT JOIN read_parquet('{meta_glob}') m ON il.citing_oa_id = m.oa_id
        LEFT JOIN dataset_db.my_datasets t ON il.cited_doi = t.dataset_id
    """)

    con.execute("DROP TABLE IF EXISTS intermediate_links")
    con.execute("DETACH dataset_db")

    total_min = (time.time() - start_time) / 60
    final_count = con.execute(
        "SELECT count(*) FROM my_datasets_citations_year"
    ).fetchone()[0]
    print(
        f"Done! Final citation count: {final_count:,} | Total Time: {total_min:.2f} min"
    )
    con.close()
