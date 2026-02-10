import glob
import os

import duckdb
import orjson

from sindex.core.dates import (
    _DEFAULT_CIT_MEN_DATE,
    _DEFAULT_CIT_MEN_YEAR,
    _norm_date_iso,
    get_realistic_date,
    is_realistic_integer_year,
)
from sindex.metrics.weights import mention_weight_year


def extract_uspto_mentions(db_path, input_dir, output_ndjson):
    """
    Reads USPTO output files (NDJSON), unpacks DOIs/EMDB IDs, matches with DB,
    and saves the enriched metrics to a single output file.
    """

    # 1. Gather all input files
    # DuckDB can read a list of files or a glob string directly
    input_files = glob.glob(os.path.join(input_dir, "*.ndjson"))

    if not input_files:
        print(f"No .ndjson files found in {input_dir}")
        return

    # Normalize paths for SQL (handle Windows backslashes if needed)
    input_files_sql = [f.replace("\\", "/") for f in input_files]

    con = duckdb.connect(db_path)

    query = f"""
        WITH raw_patents AS (
            SELECT * FROM read_ndjson_auto({input_files_sql}, union_by_name=True)
        ),
        
        expanded_mentions AS (
            -- 1. Extract DOIs
            SELECT 
                mention_link,
                mention_date,
                UNNEST(doi) as extracted_id
            FROM raw_patents
            WHERE doi IS NOT NULL
            
            UNION ALL
            
            -- 2. Extract EMDB IDs
            SELECT 
                mention_link,
                mention_date,
                UNNEST(emdb_id) as extracted_id
            FROM raw_patents
            WHERE emdb_id IS NOT NULL
        )

        SELECT 
            d.dataset_id,
            d.pubyear,
            m.mention_link,
            m.mention_date,
            'uspto' as source_type,
            -- Attempt to parse year strictly from the date string for weighting
            try_cast(year(try_cast(m.mention_date AS DATE)) AS INTEGER) as raw_year
        FROM expanded_mentions m
        INNER JOIN my_datasets_all d
            ON d.dataset_id = m.extracted_id
    """

    total_lines_saved = 0
    print(f"Enriching {len(input_files)} USPTO files against {db_path}...")

    try:
        results = con.execute(query)

        # Use a temporary file for atomic write safety
        temp_output = output_ndjson + ".tmp"

        with open(temp_output, "wb") as f:
            while True:
                # Fetch in batches to keep memory usage low
                rows = results.fetchmany(10000)
                if not rows:
                    break

                for row in rows:
                    dataset_id, pubyear, m_link, m_date_raw, m_source, m_year_raw = row

                    # --- Date Normalization ---
                    mention_date = None
                    if m_date_raw:
                        try:
                            # Clean up date string (e.g. 20240101 -> 2024-01-01)
                            norm_iso = _norm_date_iso(str(m_date_raw))
                            mention_date = get_realistic_date(norm_iso)
                        except (ValueError, TypeError):
                            pass

                    mention_year = None
                    if is_realistic_integer_year(m_year_raw):
                        mention_year = m_year_raw
                    # Fallback: if SQL failed to parse year, try python from the cleaned date
                    elif mention_date:
                        try:
                            y = int(mention_date[:4])
                            if is_realistic_integer_year(y):
                                mention_year = y
                        except:
                            pass

                    # --- Construct Record ---
                    rec = {
                        "dataset_id": dataset_id,
                        "source": [m_source],  # formatted as list ["uspto"]
                        "mention_link": m_link,
                        "mention_weight": mention_weight_year(pubyear, mention_year),
                    }

                    # --- Flagging Logic (Dates) ---
                    if mention_date:
                        rec["mention_date"] = mention_date
                        rec["placeholder_date"] = False
                    else:
                        rec["mention_date"] = _DEFAULT_CIT_MEN_DATE
                        rec["placeholder_date"] = True

                    if mention_year:
                        rec["mention_year"] = mention_year
                        rec["placeholder_year"] = False
                    else:
                        rec["mention_year"] = _DEFAULT_CIT_MEN_YEAR
                        rec["placeholder_year"] = True

                    f.write(orjson.dumps(rec) + b"\n")
                    total_lines_saved += 1

        # Rename temp file to final on success
        if os.path.exists(output_ndjson):
            os.remove(output_ndjson)
        os.rename(temp_output, output_ndjson)

        print(f"Done! Matches found: {total_lines_saved:,}")
        print(f"Saved to: {output_ndjson}")

    except Exception as e:
        print(f"Error during enrichment: {e}")
        if os.path.exists(temp_output):
            os.remove(temp_output)
    finally:
        con.close()
