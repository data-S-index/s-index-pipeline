import csv
import glob
import json  # Standard json for writing final files
import os
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

# Try to import orjson for speed (reading), fallback to standard json
try:
    import orjson
except ImportError:
    orjson = None
    print("orjson not found. Using standard json (slower).")


def process_file(filepath):
    """
    Worker function to process a single file.
    Extracts years from: 'pubyear', 'publication_year', 'published', and 'doi_created_date'.
    """
    local_stats = {
        "total_records": 0,
        
        # Presence Counts
        "count_has_pubyear": 0,
        "count_has_publication_year": 0,
        "count_has_published": 0,
        "count_has_doi_created_date": 0,
        
        # Year Distributions
        "pubyear_years": Counter(),
        "publication_year_years": Counter(),
        "published_years": Counter(),
        "doi_created_date_years": Counter(),
    }

    try:
        with open(filepath, "rb") as f:
            for line in f:
                if not line.strip():
                    continue

                try:
                    # READ (Speed: orjson if available)
                    if orjson:
                        record = orjson.loads(line)
                    else:
                        record = json.loads(line)

                    local_stats["total_records"] += 1

                    # --- 1. pubyear ---
                    # Logic: Integer or 4-digit string
                    val = record.get("pubyear")
                    if val:
                        local_stats["count_has_pubyear"] += 1
                        val_str = str(val).strip()
                        if val_str.isdigit() and len(val_str) == 4:
                            local_stats["pubyear_years"][val_str] += 1

                    # --- 2. publication_year ---
                    # Logic: Integer or 4-digit string
                    val = record.get("publication_year")
                    if val:
                        local_stats["count_has_publication_year"] += 1
                        val_str = str(val).strip()
                        if val_str.isdigit() and len(val_str) == 4:
                            local_stats["publication_year_years"][val_str] += 1

                    # --- 3. published ---
                    # Logic: ISO string, extract first 4 chars
                    val = record.get("published")
                    if val:
                        local_stats["count_has_published"] += 1
                        if isinstance(val, str) and len(val) >= 4:
                            year = val[:4]
                            if year.isdigit():
                                local_stats["published_years"][year] += 1

                    # --- 4. doi_created_date ---
                    # Logic: ISO string, extract first 4 chars
                    val = record.get("doi_created_date")
                    if val:
                        local_stats["count_has_doi_created_date"] += 1
                        if isinstance(val, str) and len(val) >= 4:
                            year = val[:4]
                            if year.isdigit():
                                local_stats["doi_created_date_years"][year] += 1

                except Exception:
                    continue

    except Exception as e:
        print(f"Error reading {filepath}: {e}")

    return local_stats


def analyze_json_files(folder_path, output_dir="."):
    print("Scanning for files...")
    files = glob.glob(os.path.join(folder_path, "**", "*.ndjson"), recursive=True)
    total_files = len(files)

    if total_files == 0:
        print("No .ndjson files found!")
        return

    os.makedirs(output_dir, exist_ok=True)
    print(f"Found {total_files} files. Starting processing...")

    # Global Aggregators
    total_stats = {
        "total_records": 0,
        "count_has_pubyear": 0,
        "count_has_publication_year": 0,
        "count_has_published": 0,
        "count_has_doi_created_date": 0,
        
        "pubyear_years": Counter(),
        "publication_year_years": Counter(),
        "published_years": Counter(),
        "doi_created_date_years": Counter(),
    }

    start_time = time.time()

    # MULTIPROCESSING LOOP
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_file, f) for f in files]

        for i, future in enumerate(as_completed(futures), 1):
            try:
                res = future.result()
                # Aggregate results
                total_stats["total_records"] += res["total_records"]
                
                total_stats["count_has_pubyear"] += res["count_has_pubyear"]
                total_stats["count_has_publication_year"] += res["count_has_publication_year"]
                total_stats["count_has_published"] += res["count_has_published"]
                total_stats["count_has_doi_created_date"] += res["count_has_doi_created_date"]
                
                total_stats["pubyear_years"].update(res["pubyear_years"])
                total_stats["publication_year_years"].update(res["publication_year_years"])
                total_stats["published_years"].update(res["published_years"])
                total_stats["doi_created_date_years"].update(res["doi_created_date_years"])
                
            except Exception as e:
                print(f"\nWorker error: {e}")

            # Live Progress Update
            msg = f"Progress: {i}/{total_files} files processed ({(i / total_files) * 100:.1f}%)"
            print(msg, end="\r")
            sys.stdout.flush()

    print()
    end_time = time.time()
    print(f"Processing complete in {end_time - start_time:.2f} seconds.")

    # --- SAVING CSVs ---
    print("Saving CSV reports...")

    def write_csv(filename, counter_obj):
        csv_path = os.path.join(output_dir, filename)
        # Sort by Year (earliest first)
        sorted_data = sorted(counter_obj.items(), key=lambda x: x[0])
        
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Year", "Count"])
            writer.writerows(sorted_data)
        print(f"Saved: {csv_path}")

    # 1. pubyear
    write_csv("pubyear_count.csv", total_stats["pubyear_years"])
    
    # 2. publication_year
    write_csv("publication_year_count.csv", total_stats["publication_year_years"])
    
    # 3. published
    write_csv("published_per_year_count.csv", total_stats["published_years"])
    
    # 4. doi_created_date
    write_csv("doi_created_date_per_year_count.csv", total_stats["doi_created_date_years"])

    # --- SUMMARY PRINT ---
    print("\n--- Summary ---")
    print(f"Total Records Scanned: {total_stats['total_records']:,}")
    print(f"Records with 'pubyear':          {total_stats['count_has_pubyear']:,}")
    print(f"Records with 'publication_year': {total_stats['count_has_publication_year']:,}")
    print(f"Records with 'published':        {total_stats['count_has_published']:,}")
    print(f"Records with 'doi_created_date': {total_stats['count_has_doi_created_date']:,}")

if __name__ == '__main__':
    # REPLACE WITH YOUR FOLDER PATH
    folder_path = r"D:\pipeline-data\records\slim-records"
    analyze_json_files(folder_path, output_dir="results")