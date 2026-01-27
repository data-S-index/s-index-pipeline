import multiprocessing
import os

import duckdb
import orjson

from sindex.core.dates import get_best_dataset_date


def process_single_file_metadata(file_path):
    results = []
    try:
        with open(file_path, "rb") as infile:  # NOTE: 'rb' mode for orjson
            for line in infile:
                line = line.strip()
                if not line:
                    continue

                try:
                    # 1. Faster Load
                    data = orjson.loads(line)

                    # 2. Extract ID (Fail Fast)
                    identifiers_list = data.get("identifiers")
                    dataset_id = None
                    if (
                        identifiers_list
                        and isinstance(identifiers_list, list)
                        and len(identifiers_list) > 0
                    ):
                        dataset_id = identifiers_list[0].get("identifier")

                    if not dataset_id:
                        continue

                    # 3. Date Processing
                    pubdate_obj = get_best_dataset_date(
                        data.get("publication_date"), data.get("created_date")
                    )

                    # Logic to get year
                    pubyear = pubdate_obj.year if pubdate_obj else None

                    # 4. Creators
                    creators = data.get("creators")

                    # 5. Construct Output
                    # Note: We pass the datetime object (pubdate_obj) directly!
                    # orjson will serialize it to an ISO string automatically.
                    output_record = {
                        "dataset_id": dataset_id,
                        "pubdate": pubdate_obj,
                        "pubyear": pubyear,
                        "creators": creators,
                    }

                    # 6. Faster Dump (returns bytes)
                    # We decode to utf-8 string to keep it compatible with the file writer
                    results.append(orjson.dumps(output_record).decode("utf-8"))

                except (orjson.JSONDecodeError, ValueError):
                    continue

    except Exception as e:
        return [f"ERROR: {file_path} - {str(e)}"]

    return results


def batch_dataset_report_metadata(root_folder, output_filepath):
    # 1. Collect all file paths first
    print(f"Scanning files in {root_folder}...")
    all_files = []
    for dirpath, _, filenames in os.walk(root_folder):
        for filename in filenames:
            if filename.endswith((".ndjson", ".json", ".jsonl")):
                all_files.append(os.path.join(dirpath, filename))

    print(
        f"Found {len(all_files)} files. Starting processing with {multiprocessing.cpu_count()} cores..."
    )

    # 2. Process in parallel
    # Use 75% of available cores to keep system responsive, or all if on a server
    cpu_count = multiprocessing.cpu_count()
    workers = max(1, cpu_count - 1)

    count_saved = 0

    with open(output_filepath, "w", encoding="utf-8") as outfile:
        # Pool creates a pool of worker processes
        with multiprocessing.Pool(processes=workers) as pool:
            # imap_unordered is faster as it yields results as soon as they are ready
            # chunksize helps reduce communication overhead
            for result_batch in pool.imap_unordered(
                process_single_file_metadata, all_files, chunksize=10
            ):
                # result_batch is the list of strings returned by one file
                for res in result_batch:
                    if res.startswith("ERROR:"):
                        print(res)
                    else:
                        outfile.write(res + "\n")
                        count_saved += 1

                # Optional: Print progress every 100k lines (approx)
                if count_saved % 100000 == 0:
                    print(f"Processed {count_saved} records...", end="\r")

    print(f"\nDone! Total records saved: {count_saved}")


def merge_and_calculate_metrics(dataset_path, citations_path, output_path):
    print("Starting DuckDB Merge & Aggregation...")

    con = duckdb.connect()

    # We use TRY_CAST(citation_date AS DATE) to safely handle bad/missing dates.
    # The condition `year(...) <= d.pubyear + 3` handles your 3-year window logic.

    query = f"""
    COPY (
        SELECT 
            d.dataset_id,
            d.pubdate,
            d.pubyear,
            d.creators,
            
            -- 1. Create the full list of citations (excluding null joins)
            list({{'source': c.source, 
                   'citation_link': c.citation_link, 
                   'citation_date': c.citation_date, 
                   'citation_weight': c.citation_weight
                  }}) FILTER (WHERE c.citation_link IS NOT NULL) AS citations,

            -- 2. Count citations within 3 years of pubyear
            count(c.citation_link) FILTER (
                WHERE c.citation_date IS NOT NULL 
                AND d.pubyear IS NOT NULL
                AND year(TRY_CAST(c.citation_date AS DATE)) <= (d.pubyear + 3)
            ) AS citation_3years,

            -- 3. Sum citation weights within 3 years of pubyear
            COALESCE(sum(c.citation_weight) FILTER (
                WHERE c.citation_date IS NOT NULL 
                AND d.pubyear IS NOT NULL
                AND year(TRY_CAST(c.citation_date AS DATE)) <= (d.pubyear + 3)
            ), 0.0) AS citation_weight_3years

        FROM read_json_auto('{dataset_path}') d
        LEFT JOIN read_json_auto('{citations_path}') c
        ON d.dataset_id = c.dataset_id
        
        GROUP BY d.dataset_id, d.pubdate, d.pubyear, d.creators
        
    ) TO '{output_path}' (FORMAT JSON);
    """

    con.execute(query)
    print(f"Done! Processed 50M records. Output saved to {output_path}")
