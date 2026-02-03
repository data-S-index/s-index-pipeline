import glob
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import duckdb
import orjson
import pandas as pd

from sindex.core.dates import _to_datetime_utc, get_best_dataset_date


def main_metadata(data):
    """
    Transforms a raw slim metadata record into the main metadata format for DuckDB tables.
    Returns None if the record should be skipped.
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

    raw_pubdate_str = get_best_dataset_date(
        data.get("publication_date"), data.get("created_date")
    )

    pubdate_obj = None
    pubyear = None

    if raw_pubdate_str:
        pub_dt = _to_datetime_utc(raw_pubdate_str)
        if pub_dt:
            pubdate_obj = pub_dt
            pubyear = pub_dt.year

    # Output
    return {
        "dataset_id": dataset_id,
        "source": data.get("source"),
        "title": data.get("title"),
        "pubdate": pubdate_obj,  # ISO string
        "pubyear": pubyear,
        "creators": data.get("creators"),
    }


def _worker_process_file(args):
    """
    Worker function: Processes a single slim metadata NDJSON file.
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
                        f_out.write(orjson.dumps(meta) + b"\n")
                        k += 1

                except (orjson.JSONDecodeError, ValueError):
                    b += 1

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
    workers: int = os.cpu_count(),
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
    }

    with duckdb.connect(db_path) as con:
        con.execute("PRAGMA enable_progress_bar=true")

        # Create table
        query = f"""
        CREATE OR REPLACE TABLE metadata AS 
        SELECT 
            dataset_id,
            try_cast(pubdate AS TIMESTAMP) as pub_ts,
            pubyear,
            creators,
            title,
            source
            
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
        "source": "VARCHAR",  # Added source
    }

    print(f"Loading Citations from: {input_path}")
    with duckdb.connect(db_path) as con:
        con.execute("PRAGMA enable_progress_bar=true")

        # Create table
        query = f"""
        CREATE OR REPLACE TABLE citations AS 
        SELECT 
            dataset_id, 
            try_cast(citation_date AS TIMESTAMP) as cit_ts,
            YEAR(try_cast(citation_date AS TIMESTAMP)) as citation_year,
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
            try_cast(mention_date AS TIMESTAMP) as men_ts,
            YEAR(try_cast(mention_date AS TIMESTAMP)) as mention_year,
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
    schema = {"dataset_id": "VARCHAR", "score": "DOUBLE"}

    print(f"Loading FAIR Scores from: {input_path}")
    with duckdb.connect(db_path) as con:
        con.execute("PRAGMA enable_progress_bar")

        # Create Table
        query = f"""
        CREATE OR REPLACE TABLE fair_scores AS 
        SELECT 
            dataset_id, 
            score 
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
    con = duckdb.connect(db_path)
    con.execute("PRAGMA enable_progress_bar;")

    print("Creating final 'topics' table with score comparison logic")
    start_time = time.time()

    # We use a CREATE TABLE AS SELECT (CTAS) statement
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
            COALESCE(mn.men_weight_3yr, 0.0) as men_weight_3yr
            
        FROM metadata m
        LEFT JOIN cit_stats c ON m.dataset_id = c.dataset_id
        LEFT JOIN men_stats mn ON m.dataset_id = mn.dataset_id
        LEFT JOIN fair_scores f ON m.dataset_id = f.dataset_id
        LEFT JOIN topics t ON m.dataset_id = t.dataset_id
        """

        con.execute(query)

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


def create_dataset_index_table(db_path):
    """
    Create table with dataset index (both based on topics and subfields normalization)
    """
    print("Creating dataset_index table")
    start_time = time.time()

    with duckdb.connect(db_path) as con:
        # Settings
        con.execute("SET memory_limit='32GB'")
        con.execute("PRAGMA enable_progress_bar=true")

        # Table
        query = """
        CREATE OR REPLACE TABLE dataset_index AS
        WITH base AS (
            SELECT 
                m.*,
                
                -- Topics level normalization factors
                GREATEST(COALESCE(nt1.median_fair_score_3yr, nt2.median_fair_score_3yr, ntl.median_fair_score_3yr, 20), 20) as t_norm_fair,
                CASE 
                    WHEN nt1.median_fair_score_3yr > 0 THEN 'Exact Year'
                    WHEN nt2.median_fair_score_3yr > 0 THEN 'Recent History'
                    WHEN ntl.median_fair_score_3yr > 0 THEN 'Topic Lifetime'
                    ELSE 'Global Default'
                END as t_source_fair,

                GREATEST(COALESCE(NULLIF(nt1.median_cit_weight_3yr, 0), NULLIF(nt2.median_cit_weight_3yr, 0), NULLIF(ntl.median_cit_weight_3yr, 0), 1.0), 1.0) as t_norm_cit,
                CASE 
                    WHEN nt1.median_cit_weight_3yr > 0 THEN 'Exact Year'
                    WHEN nt2.median_cit_weight_3yr > 0 THEN 'Recent History'
                    WHEN ntl.median_cit_weight_3yr > 0 THEN 'Topic Lifetime'
                    ELSE 'Global Default'
                END as t_source_cit,

                GREATEST(COALESCE(NULLIF(nt1.median_men_weight_3yr, 0), NULLIF(nt2.median_men_weight_3yr, 0), NULLIF(ntl.median_men_weight_3yr, 0), 1.0), 1.0) as t_norm_men,
                CASE 
                    WHEN nt1.median_men_weight_3yr > 0 THEN 'Exact Year'
                    WHEN nt2.median_men_weight_3yr > 0 THEN 'Recent History'
                    WHEN ntl.median_men_weight_3yr > 0 THEN 'Topic Lifetime'
                    ELSE 'Global Default'
                END as t_source_men,

                -- Subfields level normalization factors
                GREATEST(COALESCE(ns1.median_fair_score_3yr, ns2.median_fair_score_3yr, nsl.median_fair_score_3yr, 20), 20) as s_norm_fair,
                CASE 
                    WHEN ns1.median_fair_score_3yr > 0 THEN 'Exact Year'
                    WHEN ns2.median_fair_score_3yr > 0 THEN 'Recent History'
                    WHEN nsl.median_fair_score_3yr > 0 THEN 'Subfield Lifetime'
                    ELSE 'Global Default'
                END as s_source_fair,

                GREATEST(COALESCE(NULLIF(ns1.median_cit_weight_3yr, 0), NULLIF(ns2.median_cit_weight_3yr, 0), NULLIF(nsl.median_cit_weight_3yr, 0), 1.0), 1.0) as s_norm_cit,
                CASE 
                    WHEN ns1.median_cit_weight_3yr > 0 THEN 'Exact Year'
                    WHEN ns2.median_cit_weight_3yr > 0 THEN 'Recent History'
                    WHEN nsl.median_cit_weight_3yr > 0 THEN 'Subfield Lifetime'
                    ELSE 'Global Default'
                END as s_source_cit,

                GREATEST(COALESCE(NULLIF(ns1.median_men_weight_3yr, 0), NULLIF(ns2.median_men_weight_3yr, 0), NULLIF(nsl.median_men_weight_3yr, 0), 1.0), 1.0) as s_norm_men,
                CASE 
                    WHEN ns1.median_men_weight_3yr > 0 THEN 'Exact Year'
                    WHEN ns2.median_men_weight_3yr > 0 THEN 'Recent History'
                    WHEN nsl.median_men_weight_3yr > 0 THEN 'Subfield Lifetime'
                    ELSE 'Global Default'
                END as s_source_men

            FROM dataset_metrics m

            LEFT JOIN normalization_factors_topics nt1 ON m.topic_id = nt1.topic_id AND m.pubyear = nt1.pubyear
            LEFT JOIN normalization_factors_topics nt2 ON m.topic_id = nt2.topic_id AND (m.pubyear-1) = nt2.pubyear
            LEFT JOIN normalization_factors_topics ntl ON m.topic_id = ntl.topic_id AND ntl.pubyear IS NULL
            LEFT JOIN normalization_factors_subfields ns1 ON m.subfield_id = ns1.subfield_id AND m.pubyear = ns1.pubyear
            LEFT JOIN normalization_factors_subfields ns2 ON m.subfield_id = ns2.subfield_id AND (m.pubyear-1) = ns2.pubyear
            LEFT JOIN normalization_factors_subfields nsl ON m.subfield_id = nsl.subfield_id AND nsl.pubyear IS NULL
        )
        SELECT 
            -- Exclude intermediate raw norm columns to keep the table clean
            * EXCLUDE (t_norm_fair, t_norm_cit, t_norm_men, s_norm_fair, s_norm_cit, s_norm_men),
            
            -- Keep the final applied factors and sources for auditing
            t_norm_fair as t_norm_fair_final, t_norm_cit as t_norm_cit_final, t_norm_men as t_norm_men_final,
            s_norm_fair as s_norm_fair_final, s_norm_cit as s_norm_cit_final, s_norm_men as s_norm_men_final,

            -- Dataset Index Calculations
            (1.0/3.0) * ( (fair_score/t_norm_fair) + (total_cit_weight/t_norm_cit) + (total_men_weight/t_norm_men) ) as dataset_index_topic,
            (1.0/3.0) * ( (fair_score/s_norm_fair) + (total_cit_weight/s_norm_cit) + (total_men_weight/s_norm_men) ) as dataset_index_subfield
        FROM base
        """
        con.execute(query)

        # Report
        row_count = con.execute("SELECT COUNT(*) FROM dataset_index").fetchone()[0]
        end_time = time.time()

        print("Success! Table dataset_index created.")
        print(f"Total Rows: {row_count:,}")
        print(f"Execution Time: {end_time - start_time:.2f} seconds")


def create_creators_table(db_path, limit=None):
    """
    Creates the creators_table by exploding dataset_index over creators.
    Prioritizes ORCID for identifiers and normalizes them.
    """
    if limit:
        print(f"TEST MODE: restricting processing to first {limit:,} datasets")
    else:
        print("FULL RUN: Creating full creators_table")

    start_time = time.time()

    source_query = "SELECT * FROM dataset_index"
    if limit:
        source_query += f" LIMIT {limit}"

    with duckdb.connect(db_path) as con:
        con.execute("SET memory_limit='32GB'")

        query = f"""
        CREATE OR REPLACE TABLE creators_table AS
        WITH source_data AS (
            {source_query}
        ),
        flattened_creators AS (
            SELECT 
                d.dataset_id,
                d.pubyear,
                d.topic_id,
                d.topic_name,
                d.subfield_id,
                d.subfield_name,
                d.dataset_index_topic,
                d.dataset_index_subfield,
                d.total_cit_weight,
                d.total_men_weight,
                d.fair_score,
                d.total_citations, 
                d.total_mentions,
                c.value
            FROM source_data d,
            UNNEST(d.creators::json[]) as c(value)
        ),
        creators_with_raw_ids AS (
            SELECT 
                *,
                -- Extract Name and Name Type as text
                value->>'$.name' as creator_name,
                value->>'$.name_type' as name_type,
                
                -- Keep these as JSON types for complex logic
                json_extract(value, '$.identifiers') as raw_ids_json,
                json_extract(value, '$.affiliations') as affiliations
            FROM flattened_creators
        )
        SELECT 
            dataset_id,
            pubyear,
            topic_id,
            topic_name,
            subfield_id,
            subfield_name,
            creator_name,
            name_type,
            
            -- Indentifier handling (ORCID preferred, normalize)
            LOWER(TRIM(REGEXP_REPLACE(
                COALESCE(
                    -- Attempt 1: Specific ORCID (Complex Object)
                    -- Looks for keys 'identifierType' or 'scheme'
                    (list_filter(
                        raw_ids_json::JSON[], 
                        x -> lower(x->>'$.identifierType') = 'orcid' 
                             OR lower(x->>'$.scheme') = 'orcid'
                    )[1]->>'$.identifier'),
                    
                    (list_filter(
                        raw_ids_json::JSON[], 
                        x -> lower(x->>'$.identifierType') = 'orcid' 
                             OR lower(x->>'$.scheme') = 'orcid'
                    )[1]->>'$.value'),

                    -- Attempt 2: Fallback to First Object (Standard Keys)
                    -- Looks for identifiers[0].identifier
                    raw_ids_json->0->>'$.identifier',
                    raw_ids_json->0->>'$.value',
                    
                    -- Attempt 3: Fallback to First Simple String
                    -- Uses the arrow operator to extract text from the first array element
                    raw_ids_json->>0
                ),
                -- Regex to remove ORCID URL prefixes
                '^(https?://)?(www\.)?orcid\.org/|^orcid:', 
                ''
            ))) as primary_identifier,

            affiliations,
            dataset_index_topic,
            dataset_index_subfield,
            total_cit_weight,
            total_men_weight,
            fair_score,
            total_citations,
            total_mentions

        FROM creators_with_raw_ids
        """

        con.execute(query)

        count = con.execute("SELECT COUNT(*) FROM creators_table").fetchone()[0]
        end_time = time.time()

        print("-" * 50)
        print("Success! creators_table created.")
        print(f"Total exploded rows: {count:,}")
        print(f"Execution Time: {end_time - start_time:.2f} seconds")
        print("-" * 50)

        print("\nPreview")
        print(
            con.execute("""
            SELECT creator_name, primary_identifier 
            FROM creators_table 
            WHERE primary_identifier IS NOT NULL 
            LIMIT 5
        """).df()
        )


def create_s_index_identifier_table(db_path, limit=None):
    """
    Create table with S-index of researchers regrouped based on primary identifier
    Igoring creators with no identifier
    """
    print("Creating S_index_identifier table")
    start_time = time.time()

    source_query = "SELECT * FROM creators_table"
    if limit:
        source_query += f" LIMIT {limit}"

    with duckdb.connect(db_path) as con:
        con.execute("SET memory_limit='16GB'")

        query = f"""
        CREATE OR REPLACE TABLE S_index_identifier AS
        WITH source_data AS (
            {source_query}
        )
        SELECT 
            primary_identifier,
            
            -- 1. IDENTITY
            list_distinct(list(creator_name)) as creator_names,
            mode(name_type) as name_type,
            
            -- AFFILIATIONS (Cast JSON to VARCHAR[] for flattening)
            list_distinct(flatten(list(affiliations::VARCHAR[]))) as all_affiliations,

            -- 2. DOMAIN
            -- Primary Topic (Most Frequent)
            mode(topic_id) as primary_topic_id,      -- [NEW]
            mode(topic_name) as primary_topic_name,
            
            -- Primary Subfield (Most Frequent)
            mode(subfield_id) as primary_subfield_id,    -- [NEW]
            mode(subfield_name) as primary_subfield_name, -- [NEW]
            
            -- Breadth
            count(distinct topic_id) as n_unique_topics,
            count(distinct subfield_id) as n_unique_subfields,
            
            -- Timeline
            min(pubyear) as first_pub_year,
            max(pubyear) as last_pub_year,

            -- 3. ACTIVITY & SCORES
            count(*) as n_datasets,
            sum(dataset_index_topic) as S_index_topics,
            sum(dataset_index_subfield) as S_index_subfield,
            
            avg(dataset_index_topic) as avg_dataset_index_topics,
            avg(dataset_index_subfield) as avg_dataset_index_subfield,

            -- 4. RAW METRICS
            sum(total_cit_weight) as total_cit_weight,
            sum(total_men_weight) as total_men_weight,
            sum(total_citations) as sum_total_citations,
            sum(total_mentions) as sum_total_mentions,
            avg(fair_score) as avg_fair_score

        FROM source_data
        WHERE primary_identifier IS NOT NULL
        GROUP BY primary_identifier
        ORDER BY S_index_topics DESC
        """

        con.execute(query)

        row_count = con.execute("SELECT COUNT(*) FROM S_index_identifier").fetchone()[0]
        end_time = time.time()

        print("Success! S_index_identifier table created.")
        print(f"Total Unique researchers based on identifier: {row_count:,}")
        print(f"Execution time: {end_time - start_time:.2f} seconds")

        # Updated Preview to show new columns
        print("\nTop 5 Researchers (with Topic/Subfield IDs):")
        print(
            con.execute("""
            SELECT 
                primary_identifier, 
                primary_topic_name,
                primary_subfield_name,
                n_datasets, 
                S_index_topics 
            FROM S_index_identifier 
            LIMIT 5
        """).df()
        )


def create_s_index_name_affiliation_table(db_path, limit=None):
    """
    Create table with S-index of researchers regrouped based on name and affiliation
    Igoring creators with no name
    Regrouping is by set of affiliations such that John Smith MIT, John Smith Harvard,
    and John Smith MIT + Harvard, and John Smith <no affiliation> are regrouped separately
    """
    print("Creating s_index_name_affiliation table")
    start_time = time.time()

    source_query = "SELECT * FROM creators_table"
    if limit:
        source_query += f" LIMIT {limit}"

    with duckdb.connect(db_path) as con:
        # Settings
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=1")  # Serial mode for safety
        con.execute("SET memory_limit='32GB'")
        con.execute("SET temp_directory='duckdb_tmp'")

        query = f"""
        CREATE OR REPLACE TABLE s_index_name_affiliation AS
        WITH source_data AS (
            {source_query}
        )
        SELECT 
            -- 1. GROUPING KEYS
            LOWER(TRIM(creator_name)) as grouping_name,
            
            -- If affiliation is empty [], returns '[]' as the signature affiliation
            list_sort(
                list_transform(
                    affiliations::VARCHAR[], 
                    x -> LOWER(TRIM(x))
                )
            )::VARCHAR as affiliation_set_signature,
            
            -- 2. CONTEXT
            mode(name_type) as name_type,

            -- 3. DOMAIN & CAREER
            mode(topic_id) as primary_topic_id,
            mode(topic_name) as primary_topic_name,
            mode(subfield_id) as primary_subfield_id,
            mode(subfield_name) as primary_subfield_name,
            
            count(distinct topic_id) as n_unique_topics,
            count(distinct subfield_id) as n_unique_subfields,
            min(pubyear) as first_pub_year,
            max(pubyear) as last_pub_year,

            -- 4. SCORES
            count(*) as n_datasets,
            sum(dataset_index_topic) as S_index_topics,
            sum(dataset_index_subfield) as S_index_subfield,
            
            avg(dataset_index_topic) as avg_dataset_index_topics,
            avg(dataset_index_subfield) as avg_dataset_index_subfield,

            -- 5. RAW METRICS
            sum(total_cit_weight) as total_cit_weight,
            sum(total_men_weight) as total_men_weight,
            sum(total_citations) as sum_total_citations,
            sum(total_mentions) as sum_total_mentions,
            avg(fair_score) as avg_fair_score

        FROM source_data
        WHERE creator_name IS NOT NULL
        GROUP BY 1, 2
        ORDER BY S_index_topics DESC
        """

        try:
            con.execute(query)

            row_count = con.execute(
                "SELECT COUNT(*) FROM s_index_name_affiliation"
            ).fetchone()[0]
            end_time = time.time()

            print("Success! s_index_name_affiliation table created.")
            print(f"Total unique Name-Affiliation sets: {row_count:,}")
            print(f"Execution time: {end_time - start_time:.2f} seconds")

            print("\nPreview")
            print(
                con.execute("""
                SELECT 
                    grouping_name, 
                    affiliation_set_signature, 
                    n_datasets, 
                    S_index_topics 
                FROM s_index_name_affiliation 
                LIMIT 5
            """).df()
            )

        except Exception as e:
            print("\nError during execution:")
            print(e)


def create_s_index_identifier_name_affiliation_table(db_path, limit=None):
    """
    Merge authors based on identifier only first, then based on name/affiliation pair
    """
    print("Creating s_index_identifier_name_affiliation table")
    start_time = time.time()

    source_query = "SELECT * FROM creators_table"
    if limit:
        source_query += f" LIMIT {limit}"

    with duckdb.connect(db_path) as con:
        # Settings
        con.execute("SET memory_limit='64GB'")
        con.execute("SET temp_directory='duckdb_tmp'")
        con.execute("SET threads=8")

        query = f"""
        CREATE OR REPLACE TABLE s_index_identifier_name_affiliation AS
        WITH source_data AS (
            {source_query}
        ),
        cleaned_data AS (
            SELECT 
                *,
                -- 1. SANITIZE TEXT (Hex Roundtrip)
                TRY_CAST(from_hex(hex(creator_name)) AS VARCHAR) as safe_name_raw,
                TRY_CAST(from_hex(hex(CAST(affiliations AS VARCHAR))) AS VARCHAR) as safe_affil_raw
            FROM source_data
        ),
        parsed_affiliations AS (
            SELECT 
                *,
                -- 2. ROBUST LIST PARSING
                -- Instead of CAST(.. AS VARCHAR[]), we split by comma and clean up.
                -- This handles '["MIT", "Harvard"]' and weird JSON escapes safely.
                list_transform(
                    string_split(COALESCE(safe_affil_raw, ''), ','), 
                    x -> TRIM(regexp_replace(x, '[\\[\\]"''\\\\]', '', 'g'))
                ) as affil_list_clean
            FROM cleaned_data
        ),
        pre_processed AS (
            SELECT 
                *,
                LOWER(TRIM(safe_name_raw)) as clean_name,
                
                -- Create Signature from the manually cleaned list
                list_sort(
                    list_transform(affil_list_clean, x -> LOWER(x))
                )::VARCHAR as affil_signature
            FROM parsed_affiliations
        )
        SELECT 
            -- 1. GROUPING KEY
            COALESCE(
                primary_identifier, 
                clean_name || '_' || affil_signature
            ) as distinct_group_id,

            CASE 
                WHEN primary_identifier IS NOT NULL THEN 'identifier'
                ELSE 'name_affiliation'
            END as grouping_method,

            -- 2. IDENTITY
            mode(safe_name_raw) as display_name,
            max(primary_identifier) as primary_identifier, 
            
            
            list_distinct(flatten(list(affil_list_clean))) as all_affiliations,
            mode(name_type) as name_type,

            -- 3. DOMAIN
            mode(topic_id) as primary_topic_id,
            mode(topic_name) as primary_topic_name,
            mode(subfield_id) as primary_subfield_id,
            mode(subfield_name) as primary_subfield_name,

            count(distinct topic_id) as n_unique_topics,
            count(distinct subfield_id) as n_unique_subfields,
            
            min(pubyear) as first_pub_year,
            max(pubyear) as last_pub_year,

            -- 4. SCORES
            count(*) as n_datasets,
            sum(dataset_index_topic) as S_index_topics,
            sum(dataset_index_subfield) as S_index_subfield,
            
            avg(dataset_index_topic) as avg_dataset_index_topics,
            avg(dataset_index_subfield) as avg_dataset_index_subfield,

            -- 5. RAW METRICS
            sum(total_cit_weight) as total_cit_weight,
            sum(total_men_weight) as total_men_weight,
            sum(total_citations) as sum_total_citations,
            sum(total_mentions) as sum_total_mentions,
            avg(fair_score) as avg_fair_score

        FROM pre_processed
        WHERE primary_identifier IS NOT NULL OR clean_name IS NOT NULL
        
        GROUP BY 1, 2
        ORDER BY S_index_topics DESC
        """

        try:
            con.execute(query)

            stats = con.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN grouping_method = 'identifier' THEN 1 END) as by_id,
                    COUNT(CASE WHEN grouping_method = 'name_affiliation' THEN 1 END) as by_fallback
                FROM s_index_identifier_name_affiliation
            """).fetchone()

            end_time = time.time()
            print(f"Success! Completed in {end_time - start_time:.2f} seconds.")
            print(f"Total authors: {stats[0]:,}")
            print(f"  > By Identifier: {stats[1]:,}")
            print(f"  > By name & affiliation: {stats[2]:,}")

        except Exception as e:
            print("\nError:", e)
