import glob
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import duckdb
import orjson
import pandas as pd


def main_metadata(data):
    """
    Transforms a raw slim metadata record into the main metadata format for DuckDB tables.
    Returns None if the record should be skipped (missing dataset ID).
    """

    identifiers_list = data.get("identifiers")
    dataset_id = None
    if (
        identifiers_list
        and isinstance(identifiers_list, list)
        and len(identifiers_list) > 0
    ):
        dataset_id = identifiers_list[0].get("identifier")

    if not dataset_id:
        return None  # Skip

    # Output
    return {
        "dataset_id": dataset_id,
        "source": data.get("source"),
        "title": data.get("title"),
        "pubdate": data.get("publication_date"),  # ISO string
        "pubyear": data.get("pubyear"),
        "creators": data.get("creators"),
        "publisher": data.get("publisher"),
    }


def _worker_process_file(args):
    """
    Worker function for parallel processing: Processes a single slim metadata NDJSON file.
    """
    in_path, out_path, overwrite = args

    # Skip if output exists and we are not overwriting
    if out_path.exists() and not overwrite:
        return 0, 0, 0

    # Ensure directory exists
    out_path.parent.mkdir(parents=True, exist_ok=True)

    r = k = b = 0

    try:
        with open(in_path, "rb") as f_in, open(out_path, "wb") as f_out:
            output_buffer = []
            for line in f_in:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = orjson.loads(line)
                    r += 1

                    # Transform
                    meta = main_metadata(rec)

                    if meta:
                        output_buffer.append(orjson.dumps(meta))
                        k += 1

                    # Write in chunks of 1000 to keep memory low but speed high
                    if len(output_buffer) >= 1000:
                        f_out.write(b"\n".join(output_buffer) + b"\n")
                        output_buffer = []

                except (orjson.JSONDecodeError, ValueError):
                    b += 1
            # Final flush
            if output_buffer:
                f_out.write(b"\n".join(output_buffer) + b"\n")

        if k == 0:
            try:
                out_path.unlink()
            except OSError:
                pass

    except Exception as e:
        print(f"\n[Error] Processing {in_path.name}: {e}")
        if out_path.exists():
            try:
                out_path.unlink()
            except OSError:
                pass

    return r, k, b


def batch_process_metadata_from_slim(
    src_folder: str,
    dst_folder: str,
    overwrite: bool = False,
    one_line_progress: bool = True,
    workers: int = 4,
) -> dict:
    src, dst = Path(src_folder), Path(dst_folder)

    # Gather .ndjson files
    files = sorted(list(src.rglob("*.ndjson")))

    num_files = len(files)
    if num_files == 0:
        print(f"No .ndjson files found in {src}")
        return {}

    # Prepare tasks
    tasks = []
    for in_path in files:
        rel_path = in_path.relative_to(src)

        # Naming of the output files follows inputfilename-metadata.ndjson
        new_name = f"{in_path.stem}-metadata.ndjson"

        final_out_path = dst / rel_path.parent / new_name
        tasks.append((in_path, final_out_path, overwrite))

    total_in = total_out = total_bad = 0
    t0 = time.time()

    print(f"Processing {num_files} files using {workers} cores...")
    print(f"Input: {src}")
    print(f"Output: {dst}")

    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_index = {
            executor.submit(_worker_process_file, task): i
            for i, task in enumerate(tasks)
        }

        for idx, future in enumerate(as_completed(future_to_index), 1):
            r, k, b = future.result()
            total_in += r
            total_out += k
            total_bad += b

            if one_line_progress:
                print(f"\r[{idx}/{num_files}] files completed", end="", flush=True)

    if one_line_progress:
        print()

    dt = time.time() - t0
    rate = int(total_out / dt) if dt > 0 else 0

    summary = {
        "files_seen": num_files,
        "records_read": total_in,
        "records_kept": total_out,
        "records_bad_json": total_bad,
        "output_dir": str(dst.resolve()),
        "elapsed_sec": round(dt, 2),
        "rate_rec_per_sec": rate,
    }

    print(
        f"Done. files={num_files} kept={total_out:,} bad={total_bad:,} "
        f"time={dt:.1f}s rate≈{rate:,}/rec-per-sec"
    )
    return summary


def _get_file_sources(input_path):
    """
    Helper: Resolves file paths, deduplicates them, and returns a SQL-compatible list.
    Used for loading metadata.ndjson, citations.ndjson, etc. into DuckDB
    """
    all_files = set()  # using set to automatically remove duplicates

    if os.path.isfile(input_path):
        all_files.add(input_path)

    elif os.path.isdir(input_path):
        # Find files in root
        root_files = glob.glob(os.path.join(input_path, "*.ndjson"))
        all_files.update(root_files)

        # Find files recursively
        recursive_files = glob.glob(
            os.path.join(input_path, "**", "*.ndjson"), recursive=True
        )
        all_files.update(recursive_files)

    else:
        raise ValueError(f"Path not found: {input_path}")

    if not all_files:
        raise FileNotFoundError(f"No .ndjson files found in '{input_path}'")

    # Convert back to list and format for SQL
    unique_file_list = list(all_files)
    return str(unique_file_list).replace(
        "\\", "/"
    )  # the replace ensures Windows paths work in DuckDB SQL


def create_metadata_table(db_path, input_path):
    """
    Loads metadata from the simplified ndjson format into DuckDB
    """
    # Determine input files
    sources_sql = _get_file_sources(input_path)

    # Define schema
    schema_definition = {
        "dataset_id": "VARCHAR",
        "pubdate": "VARCHAR",
        "pubyear": "INTEGER",
        "creators": "JSON",
        "title": "VARCHAR",
        "source": "VARCHAR",
        "publisher": "VARCHAR",
    }

    with duckdb.connect(db_path) as con:
        con.execute("PRAGMA enable_progress_bar=true")

        # Create table
        query = f"""
        CREATE OR REPLACE TABLE metadata AS 
        SELECT 
            dataset_id,
            try_cast(pubdate AS DATE) as pub_ts,
            pubyear,
            creators,
            title,
            source,
            publisher
            
        FROM read_json_auto(
            {sources_sql}, 
            columns={schema_definition}, 
            ignore_errors=true, 
            union_by_name=true
        )
        """

        con.execute(query)

        # Verification
        row_count = con.execute("SELECT COUNT(*) FROM metadata").fetchone()[0]
        print(f"Metadata table created in '{db_path}'. Total rows: {row_count}")

        print("\nPreview")
        print(con.execute("SELECT * FROM metadata LIMIT 5").df())


def create_citations_table(db_path, input_path):
    """
    Loads citations ndjson into DuckDB
    """
    # Determine input files
    sources_sql = _get_file_sources(input_path)

    # Define schema
    schema = {
        "dataset_id": "VARCHAR",
        "citation_weight": "DOUBLE",
        "citation_date": "VARCHAR",
        "citation_year": "INTEGER",
        "source": "VARCHAR",
    }

    print(f"Loading Citations from: {input_path}")
    with duckdb.connect(db_path) as con:
        con.execute("PRAGMA enable_progress_bar=true")

        # Create table
        query = f"""
        CREATE OR REPLACE TABLE citations AS 
        SELECT 
            dataset_id, 
            try_cast(citation_date AS DATE) as cit_ts,
            citation_year,
            citation_weight, 
            source
        FROM read_json_auto(
            {sources_sql}, 
            columns={schema}, 
            ignore_errors=true, 
            union_by_name=true
        )
        """
        con.execute(query)

        # Verification
        count = con.execute("SELECT COUNT(*) FROM citations").fetchone()[0]
        print(f"Citations table created. Rows: {count:,}")

        print("\nPreview")
        print(con.execute("SELECT * FROM citations LIMIT 5").df())


def create_mentions_table(db_path, input_path):
    """
    Loads mentions ndjson into DuckDB
    """
    # Determine input files
    sources_sql = _get_file_sources(input_path)

    # Schema
    schema = {
        "dataset_id": "VARCHAR",
        "mention_weight": "DOUBLE",
        "mention_date": "VARCHAR",
        "mention_year": "INTEGER",
        "source": "VARCHAR",
    }

    print(f"Loading Mentions from: {input_path}")
    with duckdb.connect(db_path) as con:
        con.execute("PRAGMA enable_progress_bar")

        # Create Table
        query = f"""
        CREATE OR REPLACE TABLE mentions AS 
        SELECT 
            dataset_id, 
            try_cast(mention_date AS DATE) as men_ts,
            mention_year,
            mention_weight, 
            source
        FROM read_json_auto(
            {sources_sql}, 
            columns={schema}, 
            ignore_errors=true, 
            union_by_name=true
        )
        """
        con.execute(query)

        # Verification
        count = con.execute("SELECT COUNT(*) FROM mentions").fetchone()[0]
        print(f"Mentions table created. Rows: {count:,}")

        print("\nPreview")
        print(con.execute("SELECT * FROM mentions LIMIT 5").df())


def create_fair_scores_table(db_path, input_path):
    """
    Loads fair scores ndjson into DuckDB
    """
    # Determine input files
    sources_sql = _get_file_sources(input_path)

    # Schema
    schema = {"dataset_id": "VARCHAR", "score": "DOUBLE", "softwareVersion": "VARCHAR"}

    print(f"Loading FAIR Scores from: {input_path}")
    with duckdb.connect(db_path) as con:
        con.execute("PRAGMA enable_progress_bar")

        # Create Table
        query = f"""
        CREATE OR REPLACE TABLE fair_scores AS 
        SELECT 
            dataset_id, 
            score,
            softwareVersion 
        FROM read_json_auto(
            {sources_sql}, 
            columns={schema}, 
            ignore_errors=true, 
            union_by_name=true
        )
        """
        con.execute(query)

        # Verification
        count = con.execute("SELECT COUNT(*) FROM fair_scores").fetchone()[0]
        print(f"FAIR Scores table created. Rows: {count:,}")

        print("\nPreview")
        print(con.execute("SELECT * FROM fair_scores LIMIT 5").df())


def create_topics_table(db_path):
    """
    Create final topics table

    Topics from OA and custom model are already loaded as their own tables in the DB.

    Logic: Use OpenAlex topic if exist and confidence score > 0.5 else
    use topic from custom model if confidence score> than OA confidence score or if OA topic does not exist for the dataset

    The selected source is saved in a 'source' column as 'openalex' or 'custom_model'
    """
    con = duckdb.connect(db_path)
    con.execute("PRAGMA enable_progress_bar;")

    print("Creating final 'topics' table with score comparison logic")
    start_time = time.time()

    con.execute("""
        CREATE OR REPLACE TABLE topics AS
        SELECT 
            m.dataset_id,
            
            -- Topic Info
            CASE WHEN COALESCE(oa.score, 0) > 0.5 OR COALESCE(oa.score, 0) > COALESCE(custom.score, 0) THEN oa.topic_id ELSE custom.topic_id END AS topic_id,
            CASE WHEN COALESCE(oa.score, 0) > 0.5 OR COALESCE(oa.score, 0) > COALESCE(custom.score, 0) THEN oa.topic_name ELSE custom.topic_name END AS topic_name,
            CASE WHEN COALESCE(oa.score, 0) > 0.5 OR COALESCE(oa.score, 0) > COALESCE(custom.score, 0) THEN oa.score ELSE custom.score END AS score,
            
            -- Source Info
            CASE WHEN COALESCE(oa.score, 0) > 0.5 OR COALESCE(oa.score, 0) > COALESCE(custom.score, 0) THEN 'openalex' ELSE 'custom_model' END AS source,

            -- Hierarchy Info (Subfield, Field, Domain)
            CASE WHEN COALESCE(oa.score, 0) > 0.5 OR COALESCE(oa.score, 0) > COALESCE(custom.score, 0) THEN oa.subfield_id ELSE custom.subfield_id END AS subfield_id,
            CASE WHEN COALESCE(oa.score, 0) > 0.5 OR COALESCE(oa.score, 0) > COALESCE(custom.score, 0) THEN oa.subfield_name ELSE custom.subfield_name END AS subfield_name,
            
            CASE WHEN COALESCE(oa.score, 0) > 0.5 OR COALESCE(oa.score, 0) > COALESCE(custom.score, 0) THEN oa.field_id ELSE custom.field_id END AS field_id,
            CASE WHEN COALESCE(oa.score, 0) > 0.5 OR COALESCE(oa.score, 0) > COALESCE(custom.score, 0) THEN oa.field_name ELSE custom.field_name END AS field_name,
            
            CASE WHEN COALESCE(oa.score, 0) > 0.5 OR COALESCE(oa.score, 0) > COALESCE(custom.score, 0) THEN oa.domain_id ELSE custom.domain_id END AS domain_id,
            CASE WHEN COALESCE(oa.score, 0) > 0.5 OR COALESCE(oa.score, 0) > COALESCE(custom.score, 0) THEN oa.domain_name ELSE custom.domain_name END AS domain_name

        FROM metadata m
        LEFT JOIN topics_oa oa ON m.dataset_id = oa.dataset_id
        LEFT JOIN topics_custom_model custom ON m.dataset_id = custom.dataset_id
        WHERE oa.dataset_id IS NOT NULL OR custom.dataset_id IS NOT NULL;
    """)

    elapsed = time.time() - start_time
    print(f"Table created in {elapsed:.2f} seconds.")

    # Verification
    final_count = con.execute("SELECT count(*) FROM topics").fetchone()[0]
    print(f"Final 'topics' table contains {final_count:,} rows.")

    print("\nSample of rows where Custom Model won:")
    print(
        con.execute(
            "SELECT dataset_id, source, score FROM topics WHERE source='custom_model' LIMIT 5"
        ).df()
    )


def create_dataset_metrics_table(db_path):
    """
    Create tables that regroup all the metrics for a dataset
    """
    print(
        "Creating dataset_metrics table (topic, creators, FAIR score, 3-year metrics, etc. for each dataset)"
    )

    with duckdb.connect(db_path) as con:
        con.execute("PRAGMA enable_progress_bar")

        query = """
        CREATE OR REPLACE TABLE dataset_metrics AS
        WITH cit_stats AS (
            SELECT 
                c.dataset_id,
                m.pubyear,
                
                COUNT(*) as total_citations,
                SUM(c.citation_weight) as total_cit_weight,

                COUNT(CASE 
                    WHEN c.citation_year <= (m.pubyear + 2) THEN 1 
                    ELSE NULL 
                END) as cit_3yr,
                
                SUM(CASE 
                    WHEN c.citation_year <= (m.pubyear + 2) THEN c.citation_weight 
                    ELSE 0 
                END) as cit_weight_3yr
                
            FROM citations c
            JOIN metadata m ON c.dataset_id = m.dataset_id
            GROUP BY c.dataset_id, m.pubyear
        ),
        men_stats AS (
            SELECT 
                mn.dataset_id,
                m.pubyear,
                
                COUNT(*) as total_mentions,
                SUM(mn.mention_weight) as total_men_weight,

                COUNT(CASE 
                    WHEN mn.mention_year <= (m.pubyear + 2) THEN 1 
                    ELSE NULL 
                END) as men_3yr,
                
                SUM(CASE 
                    WHEN mn.mention_year <= (m.pubyear + 2) THEN mn.mention_weight 
                    ELSE 0 
                END) as men_weight_3yr
                
            FROM mentions mn
            JOIN metadata m ON mn.dataset_id = m.dataset_id
            GROUP BY mn.dataset_id, m.pubyear
        )
        SELECT 
            m.dataset_id,
            m.pubyear,
            
            m.creators,
            m.source as dataset_source,
            
            t.topic_id,
            t.topic_name,
            t.score as topic_score,
            t.subfield_id,
            t.subfield_name,
            t.field_id,
            t.field_name,
            t.domain_id,
            t.domain_name,
            
            f.score as fair_score,
            
            COALESCE(c.total_citations, 0) as total_citations,
            COALESCE(c.total_cit_weight, 0.0) as total_cit_weight,
            COALESCE(c.cit_3yr, 0) as cit_3yr, 
            COALESCE(c.cit_weight_3yr, 0.0) as cit_weight_3yr,
            
            COALESCE(mn.total_mentions, 0) as total_mentions,
            COALESCE(mn.total_men_weight, 0.0) as total_men_weight,
            COALESCE(mn.men_3yr, 0) as men_3yr,
            COALESCE(mn.men_weight_3yr, 0.0) as men_weight_3yr,

            (1.0/3.0) * (
                (COALESCE(f.score, 0) / 100.0) + 
                COALESCE(c.total_cit_weight, 0.0) + 
                COALESCE(mn.total_men_weight, 0.0)
            ) AS raw_dataset_index,

            (1.0/3.0) * (
                (COALESCE(f.score, 0) / 100.0) + 
                COALESCE(c.cit_weight_3yr, 0.0) + 
                COALESCE(mn.men_weight_3yr, 0.0)
            ) AS raw_dataset_index_3yr,


            
        FROM metadata m
        LEFT JOIN cit_stats c ON m.dataset_id = c.dataset_id
        LEFT JOIN men_stats mn ON m.dataset_id = mn.dataset_id
        LEFT JOIN fair_scores f ON m.dataset_id = f.dataset_id
        LEFT JOIN topics t ON m.dataset_id = t.dataset_id
        """

        con.execute(query)
        print("Query finished. Starting Checkpoint (saving to disk)")
        con.execute("CHECKPOINT")
        print("Checkpoint finished.")

        # Verify
        count = con.execute("SELECT COUNT(*) FROM dataset_metrics").fetchone()[0]
        print(f"Success! dataset_metrics table created. Total datasets: {count:,}")

        print("\nPreview")
        print(
            con.execute("""
            SELECT dataset_id, creators, topic_name, cit_3yr 
            FROM dataset_metrics 
            WHERE creators IS NOT NULL
            LIMIT 5
        """).df()
        )


def calculate_normalization_factors_topics(db_path, limit=None):
    """
    Create table with normalization factors based on regrouping datasets by their topics
    """
    print("Creating normalization_factors_topics table")

    with duckdb.connect(db_path) as con:
        query = """
            SELECT topic_id, topic_name, pubyear, fair_score, cit_weight_3yr, men_weight_3yr
            FROM dataset_metrics
            WHERE pubyear IS NOT NULL 
        """
        if limit:
            print(f" TESTING MODE: Sampling {limit} random datasets...")
            query += f" USING SAMPLE {limit}"

        df = con.execute(query).df()

    if df.empty:
        print("No data found. Skipping.")
        return

    df["topic_id"] = df["topic_id"].astype(str)
    df["topic_name"] = df["topic_name"].fillna("Unknown")

    results = []

    # Helper function for median calculation
    def get_stats(subset, t_id, t_name, year_val):
        f = subset["fair_score"].dropna()
        c = subset["cit_weight_3yr"].dropna()
        m = subset["men_weight_3yr"].dropna()
        return {
            "topic_id": t_id,
            "topic_name": t_name,
            "pubyear": year_val,
            "median_fair_score_3yr": float(f.median()) if not f.empty else None,
            "median_cit_weight_3yr": float(c.median()) if not c.empty else None,
            "median_men_weight_3yr": float(m.median()) if not m.empty else None,
            "n_fair": int(len(f)),
            "n_cit": int(len(c)),
            "n_men": int(len(m)),
        }

    # Global normalization factors (medians with all the datasets: all topics, all years)
    print("Generating global normalization factors")
    results.append(get_stats(df, None, None, None))

    # Topic lifetime normalization factors (medians for a given topic, all years)
    print(
        f"Generating normalization factors for {df['topic_id'].nunique()} topics (all years)"
    )

    for (t_id, t_name), group in df.groupby(["topic_id", "topic_name"]):
        results.append(get_stats(group, t_id, t_name, None))

    # Year wise
    min_year = int(df["pubyear"].min())
    max_year = int(df["pubyear"].max())

    start_year = min_year + 3
    end_year = max_year + 2

    if start_year > end_year:
        analysis_years = [max_year + 3]
    else:
        analysis_years = range(start_year, end_year)

    print("Generating medians for target years and topic")

    for year in analysis_years:
        # Yearly normalization factors (medians for a given year all topics combined)
        cit_mask_all = df["pubyear"].isin([year - 3, year - 2])
        fair_mask_all = df["pubyear"].isin([year - 3, year - 2, year - 1])

        f_all = df.loc[fair_mask_all, "fair_score"].dropna()
        c_all = df.loc[cit_mask_all, "cit_weight_3yr"].dropna()
        m_all = df.loc[cit_mask_all, "men_weight_3yr"].dropna()

        results.append(
            {
                "topic_id": None,
                "topic_name": None,
                "pubyear": int(year),
                "median_fair_score_3yr": float(f_all.median())
                if not f_all.empty
                else None,
                "median_cit_weight_3yr": float(c_all.median())
                if not c_all.empty
                else None,
                "median_men_weight_3yr": float(m_all.median())
                if not m_all.empty
                else None,
                "n_fair": int(len(f_all)),
                "n_cit": int(len(c_all)),
                "n_men": int(len(m_all)),
            }
        )

        # Topic-Year normalization factors (medians for a given topic and pub year)
        for (t_id, t_name), group in df.groupby(["topic_id", "topic_name"]):
            cit_mask = group["pubyear"].isin([year - 3, year - 2])
            fair_mask = group["pubyear"].isin([year - 3, year - 2, year - 1])

            f_vals = group.loc[fair_mask, "fair_score"].dropna()
            c_vals = group.loc[cit_mask, "cit_weight_3yr"].dropna()
            m_vals = group.loc[cit_mask, "men_weight_3yr"].dropna()

            results.append(
                {
                    "topic_id": t_id,
                    "topic_name": t_name,
                    "pubyear": int(year),
                    "median_fair_score_3yr": float(f_vals.median())
                    if not f_vals.empty
                    else None,
                    "median_cit_weight_3yr": float(c_vals.median())
                    if not c_vals.empty
                    else None,
                    "median_men_weight_3yr": float(m_vals.median())
                    if not m_vals.empty
                    else None,
                    "n_fair": int(len(f_vals)),
                    "n_cit": int(len(c_vals)),
                    "n_men": int(len(m_vals)),
                }
            )

    # Save
    df_norm = pd.DataFrame(results)

    # Fix types for DuckDB
    df_norm["topic_id"] = df_norm["topic_id"].astype("object")
    df_norm["topic_name"] = df_norm["topic_name"].astype("object")

    print(f"Saving {len(df_norm)} benchmark rows...")
    with duckdb.connect(db_path) as con:
        con.execute("DROP TABLE IF EXISTS normalization_factors_topics")
        con.execute("""
            CREATE TABLE normalization_factors_topics (
                topic_id VARCHAR, 
                topic_name VARCHAR,
                pubyear INTEGER,
                median_fair_score_3yr DOUBLE, 
                median_cit_weight_3yr DOUBLE, 
                median_men_weight_3yr DOUBLE,
                n_fair INTEGER, n_cit INTEGER, n_men INTEGER
            )
        """)

        con.register("df_view", df_norm)
        con.execute("INSERT INTO normalization_factors_topics SELECT * FROM df_view")
        con.unregister("df_view")

        print("normalization_factors_topics table created.")
        print("\nSample view (normalization factors for a topic for all years)")
        print(
            con.execute(
                """
                SELECT topic_id, topic_name, median_cit_weight_3yr, n_cit 
                FROM normalization_factors_topics 
                WHERE pubyear IS NULL AND topic_id IS NOT NULL 
                LIMIT 5
                """
            ).df()
        )


def calculate_normalization_factors_subfields(db_path, limit=None):
    """
    Create table with normalization factors based on regrouping datasets by their subfields
    (one level above topics)
    """
    print("Creating normalization_factors_subfields table")

    with duckdb.connect(db_path) as con:
        # Fetch data
        query = """
            SELECT subfield_id, subfield_name, pubyear, fair_score, cit_weight_3yr, men_weight_3yr
            FROM dataset_metrics
            WHERE pubyear IS NOT NULL 
        """
        if limit:
            print(f" TESTING MODE: Sampling {limit} random datasets")
            query += f" USING SAMPLE {limit}"

        df = con.execute(query).df()

    if df.empty:
        print("No data found. Skipping.")
        return

    # Ensure IDs are strings
    df["subfield_id"] = df["subfield_id"].astype(str)
    df["subfield_name"] = df["subfield_name"].fillna("Unknown")

    results = []

    # Helper function for median calculation
    def get_stats(subset, s_id, s_name, year_val):
        f = subset["fair_score"].dropna()
        c = subset["cit_weight_3yr"].dropna()
        m = subset["men_weight_3yr"].dropna()
        return {
            "subfield_id": s_id,
            "subfield_name": s_name,
            "pubyear": year_val,
            "median_fair_score_3yr": float(f.median()) if not f.empty else None,
            "median_cit_weight_3yr": float(c.median()) if not c.empty else None,
            "median_men_weight_3yr": float(m.median()) if not m.empty else None,
            "n_fair": int(len(f)),
            "n_cit": int(len(c)),
            "n_men": int(len(m)),
        }

    # Global normalization factors (medians with all the datasets: all topics, all years)
    print("Generating Global Benchmark...")
    results.append(get_stats(df, None, None, None))

    # Topic lifetime normalization factors (medians for a given topic, all years)
    print(f"Generating benchmarks for {df['subfield_id'].nunique()} subfields...")
    for (sub_id, sub_name), group in df.groupby(["subfield_id", "subfield_name"]):
        results.append(get_stats(group, sub_id, sub_name, None))

    # Year wise
    min_year = int(df["pubyear"].min())
    max_year = int(df["pubyear"].max())

    start_year = min_year + 3
    end_year = max_year + 2

    if start_year > end_year:
        analysis_years = [max_year + 3]
    else:
        analysis_years = range(start_year, end_year)

    print("Generating rolling medians for target years...")

    for year in analysis_years:
        # Yearly normalization factors (medians for a given year all topics combined)
        cit_mask_all = df["pubyear"].isin([year - 3, year - 2])
        fair_mask_all = df["pubyear"].isin([year - 3, year - 2, year - 1])

        f_all = df.loc[fair_mask_all, "fair_score"].dropna()
        c_all = df.loc[cit_mask_all, "cit_weight_3yr"].dropna()
        m_all = df.loc[cit_mask_all, "men_weight_3yr"].dropna()

        results.append(
            {
                "subfield_id": None,
                "subfield_name": None,
                "pubyear": int(year),
                "median_fair_score_3yr": float(f_all.median())
                if not f_all.empty
                else None,
                "median_cit_weight_3yr": float(c_all.median())
                if not c_all.empty
                else None,
                "median_men_weight_3yr": float(m_all.median())
                if not m_all.empty
                else None,
                "n_fair": int(len(f_all)),
                "n_cit": int(len(c_all)),
                "n_men": int(len(m_all)),
            }
        )

        # Topic-Year normalization factors (medians for a given topic and pub year)
        for (sub_id, sub_name), group in df.groupby(["subfield_id", "subfield_name"]):
            cit_mask = group["pubyear"].isin([year - 3, year - 2])
            fair_mask = group["pubyear"].isin([year - 3, year - 2, year - 1])

            f_vals = group.loc[fair_mask, "fair_score"].dropna()
            c_vals = group.loc[cit_mask, "cit_weight_3yr"].dropna()
            m_vals = group.loc[cit_mask, "men_weight_3yr"].dropna()

            results.append(
                {
                    "subfield_id": sub_id,
                    "subfield_name": sub_name,
                    "pubyear": int(year),
                    "median_fair_score_3yr": float(f_vals.median())
                    if not f_vals.empty
                    else None,
                    "median_cit_weight_3yr": float(c_vals.median())
                    if not c_vals.empty
                    else None,
                    "median_men_weight_3yr": float(m_vals.median())
                    if not m_vals.empty
                    else None,
                    "n_fair": int(len(f_vals)),
                    "n_cit": int(len(c_vals)),
                    "n_men": int(len(m_vals)),
                }
            )

    # Save
    df_norm = pd.DataFrame(results)

    # Fix types for DuckDB
    df_norm["subfield_id"] = df_norm["subfield_id"].astype("object")
    df_norm["subfield_name"] = df_norm["subfield_name"].astype("object")

    print(f"Saving {len(df_norm)} benchmark rows...")
    with duckdb.connect(db_path) as con:
        con.execute("DROP TABLE IF EXISTS normalization_factors_subfields")
        con.execute("""
            CREATE TABLE normalization_factors_subfields (
                subfield_id VARCHAR, 
                subfield_name VARCHAR,
                pubyear INTEGER,
                median_fair_score_3yr DOUBLE, 
                median_cit_weight_3yr DOUBLE, 
                median_men_weight_3yr DOUBLE,
                n_fair INTEGER, n_cit INTEGER, n_men INTEGER
            )
        """)

        con.register("df_view", df_norm)
        con.execute("INSERT INTO normalization_factors_subfields SELECT * FROM df_view")
        con.unregister("df_view")

        print("normalization_factors_subfields table created.")
        print(
            "\n-Preview (normalization factors for a subfield including all years) ---"
        )
        print(
            con.execute(
                """
                SELECT subfield_id, subfield_name, median_cit_weight_3yr, n_cit 
                FROM normalization_factors_subfields 
                WHERE pubyear IS NULL AND subfield_id IS NOT NULL 
                LIMIT 5
                """
            ).df()
        )


def calculate_normalization_factors(db_path, level_name, id_col, name_col, limit=None):
    """
    Generic function to create normalization tables based on either topics or subfields

    The level (toipcs or subfileds) is set through the level_name parameter

    Args:
        level_name (str): Suffix for table name (e.g., 'topics' -> 'normalization_factors_topics')
        id_col (str): The column to group by ID (e.g., 'topic_id')
        name_col (str): The column to group by Name (e.g., 'topic_name')
    """
    table_name = f"normalization_factors_{level_name}"
    print(f"Creating {table_name}")

    with duckdb.connect(db_path) as con:
        # Dynamic SQL to select the specific ID/Name columns
        query = f"""
            SELECT {id_col}, {name_col}, pubyear, fair_score, cit_weight_3yr, men_weight_3yr
            FROM dataset_metrics
            WHERE pubyear IS NOT NULL 
        """
        if limit:
            print(f" TESTING MODE: Sampling {limit} random datasets")
            query += f" USING SAMPLE {limit}"

        df = con.execute(query).df()

    if df.empty:
        print("No data found. Skipping.")
        return

    # Ensure IDs are strings and Names are filled
    df[id_col] = df[id_col].astype(str)
    df[name_col] = df[name_col].fillna("Unknown")

    results = []

    # Helper function for median calculation
    def get_stats(subset, group_id, group_name, year_val):
        f = subset["fair_score"].dropna()
        c = subset["cit_weight_3yr"].dropna()
        m = subset["men_weight_3yr"].dropna()
        return {
            id_col: group_id,  # Dynamic Key
            name_col: group_name,  # Dynamic Key
            "pubyear": year_val,
            "median_fair_score_3yr": float(f.median()) if not f.empty else None,
            "median_cit_weight_3yr": float(c.median()) if not c.empty else None,
            "median_men_weight_3yr": float(m.median()) if not m.empty else None,
            "n_fair": int(len(f)),
            "n_cit": int(len(c)),
            "n_men": int(len(m)),
        }

    # 1. Global Benchmark (All Data)
    print("Generating Global Benchmark...")
    results.append(get_stats(df, None, None, None))

    # 2. Group Lifetime Benchmark (Specific Group, All Years)
    print(f"Generating benchmarks for {df[id_col].nunique()} {level_name}...")
    for (g_id, g_name), group in df.groupby([id_col, name_col]):
        results.append(get_stats(group, g_id, g_name, None))

    # Prep for Time-Window Logic
    min_year = int(df["pubyear"].min())
    max_year = int(df["pubyear"].max())

    start_year = min_year + 3
    end_year = max_year + 2
    analysis_years = (
        range(start_year, end_year) if start_year <= end_year else [max_year + 3]
    )

    print("Generating rolling medians for target years...")

    for year in analysis_years:
        # 3. Yearly Global Benchmark (All Groups, Specific Year)
        cit_mask_all = df["pubyear"].isin([year - 3, year - 2])
        fair_mask_all = df["pubyear"].isin([year - 3, year - 2, year - 1])

        f_all = df.loc[fair_mask_all, "fair_score"].dropna()
        c_all = df.loc[cit_mask_all, "cit_weight_3yr"].dropna()
        m_all = df.loc[cit_mask_all, "men_weight_3yr"].dropna()

        # Append global row for this year
        res = {
            id_col: None,
            name_col: None,
            "pubyear": int(year),
            "median_fair_score_3yr": float(f_all.median()) if not f_all.empty else None,
            "median_cit_weight_3yr": float(c_all.median()) if not c_all.empty else None,
            "median_men_weight_3yr": float(m_all.median()) if not m_all.empty else None,
            "n_fair": int(len(f_all)),
            "n_cit": int(len(c_all)),
            "n_men": int(len(m_all)),
        }
        results.append(res)

        # 4. Group-Year Specific Benchmark (Specific Group, Specific Year)
        for (g_id, g_name), group in df.groupby([id_col, name_col]):
            cit_mask = group["pubyear"].isin([year - 3, year - 2])
            fair_mask = group["pubyear"].isin([year - 3, year - 2, year - 1])

            f_vals = group.loc[fair_mask, "fair_score"].dropna()
            c_vals = group.loc[cit_mask, "cit_weight_3yr"].dropna()
            m_vals = group.loc[cit_mask, "men_weight_3yr"].dropna()

            results.append(get_stats(group, g_id, g_name, int(year)))

    # Saving to DuckDB
    df_norm = pd.DataFrame(results)

    # Fix object types for DuckDB
    df_norm[id_col] = df_norm[id_col].astype("object")
    df_norm[name_col] = df_norm[name_col].astype("object")

    print(f"Saving {len(df_norm)} benchmark rows to {table_name}...")

    with duckdb.connect(db_path) as con:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"""
            CREATE TABLE {table_name} (
                {id_col} VARCHAR, 
                {name_col} VARCHAR,
                pubyear INTEGER,
                median_fair_score_3yr DOUBLE, 
                median_cit_weight_3yr DOUBLE, 
                median_men_weight_3yr DOUBLE,
                n_fair INTEGER, n_cit INTEGER, n_men INTEGER
            )
        """)

        con.register("df_view", df_norm)
        con.execute(f"INSERT INTO {table_name} SELECT * FROM df_view")
        con.unregister("df_view")

        print(f"Success! {table_name} created.")
        print(con.execute(f"SELECT * FROM {table_name} LIMIT 3").df())


def create_floored_normalization_factors_table(
    db_path,
    input_table,
    output_table,
    id_col,
    name_col,
    cit_floor=1.0,  # Minimum value for citations
    men_floor=1.0,  # Minimum value for mentions
    fair_min_base=13.46,  # Absolute minimum base for FAIR score
):
    """
    Create table with floor values for the normalization factors.
    ADDS A 'DEFAULT' ROW for fallback logic using the configured variables.
    """
    print(f"Creating floored table: {output_table} (from {input_table})")
    print(
        f"Configuration: Cit Floor={cit_floor}, Men Floor={men_floor}, FAIR Min Base={fair_min_base}"
    )

    with duckdb.connect(db_path) as con:
        # 1. Calculate Dynamic FAIR floor
        fair_floor_query = f"""
            SELECT GREATEST({fair_min_base}, MIN(median_fair_score_3yr)) 
            FROM {input_table}
        """
        fair_floor_val = con.execute(fair_floor_query).fetchone()[0]

        # Safety check if table is empty
        if fair_floor_val is None:
            fair_floor_val = fair_min_base

        print(f"-> Calculated Final FAIR Floor: {fair_floor_val}")

        # 2. Create the main table
        query = f"""
        CREATE OR REPLACE TABLE {output_table} AS
        SELECT 
            {id_col},
            {name_col},
            pubyear,
            
            -- Citations (Floored at {cit_floor})
            CASE 
                WHEN median_cit_weight_3yr < {cit_floor} THEN {cit_floor} 
                ELSE median_cit_weight_3yr 
            END as median_cit_weight_3yr,
            
            CASE 
                WHEN median_cit_weight_3yr < {cit_floor} THEN TRUE 
                ELSE FALSE 
            END as cit_is_floored,

            -- Mentions (Floored at {men_floor})
            CASE 
                WHEN median_men_weight_3yr < {men_floor} THEN {men_floor} 
                ELSE median_men_weight_3yr 
            END as median_men_weight_3yr,
            
            CASE 
                WHEN median_men_weight_3yr < {men_floor} THEN TRUE 
                ELSE FALSE 
            END as men_is_floored,

            -- FAIR Score (Floored at {fair_floor_val})
            CASE 
                WHEN median_fair_score_3yr < {fair_floor_val} THEN {fair_floor_val} 
                ELSE median_fair_score_3yr 
            END as median_fair_score_3yr,

            CASE 
                WHEN median_fair_score_3yr < {fair_floor_val} THEN TRUE 
                ELSE FALSE 
            END as fair_is_floored,

            n_fair, n_cit, n_men
            
        FROM {input_table}
        """
        con.execute(query)

        # 3. INSERT THE DEFAULT ROW
        print(f"-> Inserting DEFAULT row into {output_table}...")
        insert_default = f"""
        INSERT INTO {output_table} (
            {id_col}, {name_col}, pubyear,
            median_cit_weight_3yr, cit_is_floored,
            median_men_weight_3yr, men_is_floored,
            median_fair_score_3yr, fair_is_floored,
            n_fair, n_cit, n_men
        ) VALUES (
            'DEFAULT', 'Default Fallback', NULL,
            {cit_floor}, TRUE,
            {men_floor}, TRUE,
            {fair_floor_val}, TRUE,
            0, 0, 0
        )
        """
        con.execute(insert_default)

        # Verification
        count = con.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]
        print(f"Success. {output_table} created with {count:,} rows.\n")


def create_dataset_index_table(db_path, temp_dir=None):
    """
    Performance optimization strategy (could be slow if creating the full dataset_index table with all the columns):
    1. Create 'dataset_norm_index': A skinny table with only IDs, normalization factors, and dataset index.
    2. Create 'dataset_index': A virtual VIEW joining dataset_metrics + dataset_norm_index.

    Args:
        db_path (str): Path to the DuckDB database file.
        temp_dir (str, optional): Path to a directory on an SSD for temporary spill files.
                                  Essential for large joins on machines with limited RAM.
    """
    print("Creating dataset_index related tables")
    start_time = time.time()

    with duckdb.connect(db_path) as con:
        # Settings
        con.execute("PRAGMA enable_progress_bar=true")

        # Set the temporary directory if provided
        if temp_dir:
            print(f"Setting temp directory to: {temp_dir}")
            con.execute(f"SET temp_directory='{temp_dir}'")

        # con.execute("SET memory_limit='32GB'")
        # con.execute("SET threads=20")

        # STEP 1: Create skinny table with only IDs, normalization factors, and dataset index
        print("Generating 'dataset_norm_index' table")

        query_scores = """
        CREATE OR REPLACE TABLE dataset_norm_index AS
        SELECT 
            m.dataset_id, -- The JOIN Key
            
            -- 1. TOPIC NORMALIZATION FACTORS & AUDIT
            COALESCE(nt.median_fair_score_3yr, nt_def.median_fair_score_3yr) as t_norm_fair,
            COALESCE(nt.median_cit_weight_3yr, nt_def.median_cit_weight_3yr) as t_norm_cit,
            COALESCE(nt.median_men_weight_3yr, nt_def.median_men_weight_3yr) as t_norm_men,
            
            (m.pubyear - nt.pubyear) as t_norm_gap,

            CASE 
                WHEN nt.topic_id IS NULL THEN 'Default'
                WHEN m.pubyear = nt.pubyear THEN 'Exact Year'
                ELSE 'Closest Past Year'
            END as t_source,

            -- 2. SUBFIELD NORMALIZATION FACTORS & AUDIT
            COALESCE(ns.median_fair_score_3yr, ns_def.median_fair_score_3yr) as s_norm_fair,
            COALESCE(ns.median_cit_weight_3yr, ns_def.median_cit_weight_3yr) as s_norm_cit,
            COALESCE(ns.median_men_weight_3yr, ns_def.median_men_weight_3yr) as s_norm_men,

            (m.pubyear - ns.pubyear) as s_norm_gap,

            CASE 
                WHEN ns.subfield_id IS NULL THEN 'Default'
                WHEN m.pubyear = ns.pubyear THEN 'Exact Year'
                ELSE 'Closest Past Year'
            END as s_source,

            -- 3. FINAL CALCULATED INDICES
            (1.0/3.0) * ( 
                (fair_score / COALESCE(nt.median_fair_score_3yr, nt_def.median_fair_score_3yr)) + 
                (total_cit_weight / COALESCE(nt.median_cit_weight_3yr, nt_def.median_cit_weight_3yr)) + 
                (total_men_weight / COALESCE(nt.median_men_weight_3yr, nt_def.median_men_weight_3yr))
            ) as dataset_index_topic,

            (1.0/3.0) * ( 
                (fair_score / COALESCE(ns.median_fair_score_3yr, ns_def.median_fair_score_3yr)) + 
                (total_cit_weight / COALESCE(ns.median_cit_weight_3yr, ns_def.median_cit_weight_3yr)) + 
                (total_men_weight / COALESCE(ns.median_men_weight_3yr, ns_def.median_men_weight_3yr))
            ) as dataset_index_subfield

        FROM dataset_metrics m

        -- TOPIC JOIN (ASOF LEFT JOIN)
        ASOF LEFT JOIN (
            SELECT * FROM normalization_factors_topics_floored 
            WHERE topic_id != 'DEFAULT' ORDER BY pubyear 
        ) nt ON m.topic_id = nt.topic_id AND m.pubyear >= nt.pubyear

        LEFT JOIN normalization_factors_topics_floored nt_def 
        ON nt_def.topic_id = 'DEFAULT'

        -- SUBFIELD JOIN (ASOF LEFT JOIN)
        ASOF LEFT JOIN (
            SELECT * FROM normalization_factors_subfields_floored 
            WHERE subfield_id != 'DEFAULT' ORDER BY pubyear
        ) ns ON m.subfield_id = ns.subfield_id AND m.pubyear >= ns.pubyear

        LEFT JOIN normalization_factors_subfields_floored ns_def 
        ON ns_def.subfield_id = 'DEFAULT'
        """
        con.execute(query_scores)

        # STEP 2: Create virtual view table
        print("Creating virtual view 'dataset_index' table")

        query_view = """
        CREATE OR REPLACE VIEW dataset_index AS
        SELECT 
            m.*,
            s.* EXCLUDE (dataset_id)
        FROM dataset_metrics m
        JOIN dataset_norm_index s ON m.dataset_id = s.dataset_id
        """
        con.execute(query_view)

        # Verification
        row_count = con.execute("SELECT COUNT(*) FROM dataset_norm_index").fetchone()[0]
        end_time = time.time()

        print(f"Success! Process completed in {end_time - start_time:.2f} seconds.")
        print(f"Physical table 'dataset_norm_index': {row_count:,} rows.")
        print("Virtual View 'dataset_index' ready for analysis")

        print("\n Preview of dataset_index")
        print(
            con.execute("""
            SELECT*
            FROM dataset_index 
            LIMIT 5
        """).df()
        )


def create_creators_table(db_path, limit=None, temp_dir=None):
    """
    Explode dataset_index table into a virtual "view" table with one row per creator per dataset
    """
    # 1. Construct LIMIT clause
    limit_clause = ""
    if limit:
        print(f"TEST MODE: restricting processing to first {limit:,} datasets")
        limit_clause = f"LIMIT {limit}"
    else:
        print("FULL RUN: Creating optimized creators table")

    start_time = time.time()

    with duckdb.connect(db_path) as con:
        # Settings
        con.execute("PRAGMA enable_progress_bar=true")

        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=20")
        if temp_dir:
            print(f"-> Setting temp directory to: {temp_dir}")
            con.execute(f"SET temp_directory='{temp_dir}'")

        print("Exploding and parsing creators (writing minimal data to disk)")

        query_parsed = f"""
        CREATE OR REPLACE TABLE dataset_creators_parsed AS
        WITH source_subset AS (
            SELECT dataset_id, creators
            FROM dataset_metrics
            {limit_clause}
        ),
        raw_explode AS (
            SELECT 
                dataset_id, 
                c.value 
            FROM source_subset, 
            UNNEST(creators::json[]) as c(value)
        ),
        extracted_json AS (
            SELECT 
                dataset_id,
                value->>'$.name' as creator_name,
                value->>'$.name_type' as name_type,
                
                -- Optimization: Extract the raw JSON list ONCE here
                -- This prevents re-parsing it multiple times in the COALESCE below
                json_extract(value, '$.identifiers') as raw_ids_json,
                json_extract(value, '$.affiliations') as affiliations
            FROM raw_explode
        ),
        raw_identifiers AS (
            SELECT 
                *,
                -- Logic: Try to find ORCID, otherwise fallback to first item
                COALESCE(
                    -- Attempt 1: Look for 'orcid' type in the list (Fast Filter)
                    (list_filter(
                        raw_ids_json::JSON[], 
                        x -> lower(x->>'$.identifierType') = 'orcid' OR lower(x->>'$.scheme') = 'orcid'
                    )[1]->>'$.identifier'),
                    
                    (list_filter(
                        raw_ids_json::JSON[], 
                        x -> lower(x->>'$.identifierType') = 'orcid' OR lower(x->>'$.scheme') = 'orcid'
                    )[1]->>'$.value'),

                    -- Attempt 2: Fallback to first item
                    raw_ids_json->0->>'$.identifier',
                    raw_ids_json->0->>'$.value',
                    raw_ids_json->>0
                ) as raw_id_string
            FROM extracted_json
        )
        SELECT 
            dataset_id,
            creator_name,
            name_type,
            affiliations,
            
            -- FAST CLEANING (No Regex)
            -- Logic: If it contains 'orcid.org/', split by it and take the right side (the ID).
            --        Otherwise, just trim the string.
            LOWER(TRIM(
                CASE 
                    WHEN raw_id_string LIKE '%orcid.org/%' 
                    THEN (string_split(raw_id_string, 'orcid.org/')[2])
                    
                    WHEN raw_id_string LIKE 'orcid:%'
                    THEN (string_split(raw_id_string, 'orcid:')[2])
                    
                    ELSE raw_id_string 
                END
            )) as primary_identifier
            
        FROM raw_identifiers
        """
        con.execute(query_parsed)

        # 3. Create the View (Instant)
        print("Creating virtual view 'creators_table'")

        query_view = """
        CREATE OR REPLACE VIEW creators_table AS
        SELECT 
            c.*, 
            d.* EXCLUDE (dataset_id, creators)
        FROM dataset_creators_parsed c
        JOIN dataset_index d ON c.dataset_id = d.dataset_id
        """
        con.execute(query_view)

        # Verification
        row_count = con.execute(
            "SELECT COUNT(*) FROM dataset_creators_parsed"
        ).fetchone()[0]
        end_time = time.time()

        print(f"Success! Process completed in {end_time - start_time:.2f} seconds.")
        print(f"Physical Table 'dataset_creators_parsed': {row_count:,} rows.")

        print("\nPreview:")
        print(
            con.execute("""
            SELECT creator_name, primary_identifier
            FROM creators_table 
            WHERE primary_identifier IS NOT NULL
            LIMIT 5
        """).df()
        )


def create_s_index_identifier_name_affiliation_table(
    db_path, limit=None, temp_dir=None
):
    """
    Merge authors based on identifier only first, then based on name/affiliation pair
    """
    print("Creating s_index_identifier_name_affiliation table")
    start_time = time.time()

    # Limit is used for testing for instance on the first 1000 (if limit=1000) rows of creators_table
    if limit:
        print(f"TEST MODE (Limit {limit})")
        source_cte = f"""
        source_subset AS (
            SELECT * FROM dataset_creators_parsed LIMIT {limit}
        ),
        source_data AS (
            SELECT 
                c.*, 
                d.* EXCLUDE (dataset_id, creators)
            FROM source_subset c
            JOIN dataset_index d ON c.dataset_id = d.dataset_id
        )
        """
    else:
        # Full Run: Use the view directly.
        source_cte = """
        source_data AS (
            SELECT * FROM creators_table
        )
        """

    with duckdb.connect(db_path) as con:
        # Settings
        con.execute("PRAGMA enable_progress_bar=true")
        if temp_dir:
            con.execute(f"SET temp_directory='{temp_dir}'")

        # Best based on our testing: High memory, moderate threads (too many threads can cause thrashing)
        con.execute("SET memory_limit='64GB'")
        con.execute("SET threads=10")

        # Crucial for speed as order is not important here
        con.execute("SET preserve_insertion_order=false")

        print("Executing Single-Pass Aggregation...")

        query = f"""
        CREATE OR REPLACE TABLE s_index_identifier_name_affiliation AS
        WITH {source_cte},
        pre_processed AS (
            SELECT 
                -- Pass through metrics
                dataset_index_topic, dataset_index_subfield,
                total_cit_weight, total_men_weight, total_citations, total_mentions, fair_score,
                topic_id, topic_name, subfield_id, subfield_name, pubyear,
                name_type,
                primary_identifier,
                
                -- Fast Cleaning
                TRIM(creator_name::VARCHAR) as clean_name,

                -- Optimized List Parsing
                list_transform(
                    string_split(
                        regexp_replace(affiliations::VARCHAR, '[\\[\\]"\\\\]', '', 'g'), 
                        ','
                    ),
                    x -> TRIM(x)
                ) as affil_list_clean
            FROM source_data
        ),
        signature_generation AS (
            SELECT 
                *,
                -- Generate Group ID On-The-Fly (Pipelined)
                COALESCE(
                    primary_identifier, 
                    LOWER(clean_name) || '_' || list_sort(list_transform(affil_list_clean, x -> LOWER(x)))::VARCHAR
                ) as distinct_group_id
            FROM pre_processed
        )
        -- FINAL AGGREGATION (Directly from stream)
        SELECT 
            distinct_group_id,
            CASE 
                WHEN max(primary_identifier) IS NOT NULL THEN 'identifier'
                ELSE 'name_affiliation'
            END as grouping_method,

            -- Identity
            mode(clean_name) as display_name,
            max(primary_identifier) as primary_identifier, 
            list_distinct(flatten(list(affil_list_clean))) as all_affiliations,
            mode(name_type) as name_type,

            -- Domain
            mode(topic_id) as primary_topic_id,
            mode(topic_name) as primary_topic_name,
            mode(subfield_id) as primary_subfield_id,
            mode(subfield_name) as primary_subfield_name,

            count(distinct topic_id) as n_unique_topics,
            count(distinct subfield_id) as n_unique_subfields,
            min(pubyear) as first_pub_year,
            max(pubyear) as last_pub_year,

            -- Scores
            count(*) as n_datasets,
            sum(dataset_index_topic) as S_index_topics,
            sum(dataset_index_subfield) as S_index_subfield,
            avg(dataset_index_topic) as avg_dataset_index_topics,
            avg(dataset_index_subfield) as avg_dataset_index_subfield,

            -- Metrics
            sum(total_cit_weight) as total_cit_weight,
            sum(total_men_weight) as total_men_weight,
            sum(total_citations) as sum_total_citations,
            sum(total_mentions) as sum_total_mentions,
            avg(fair_score) as avg_fair_score

        FROM signature_generation
        WHERE primary_identifier IS NOT NULL OR clean_name IS NOT NULL
        GROUP BY distinct_group_id
        ORDER BY S_index_topics DESC
        """

        try:
            con.execute(query)

            # Verification
            stats = con.execute(
                "SELECT COUNT(*) FROM s_index_identifier_name_affiliation"
            ).fetchone()
            end_time = time.time()
            print(f"Success! Completed in {end_time - start_time:.2f} seconds.")
            print(f"Total authors: {stats[0]:,}")

        except Exception as e:
            print("\nError:", e)
