import csv
import glob
import json  # Standard json for writing final files
import os
import shutil
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


def process_file(filepath, temp_dir):
    """
    Worker function to process a single file.
    Writes 'none' records to a temp file unique to this worker.
    """
    # Unique ID for this worker's temp file (using filename hash)
    worker_id = abs(hash(filepath))
    none_filename = os.path.join(temp_dir, f"none_part_{worker_id}.ndjson")

    local_stats = {
        "total_records": 0,
        "has_created_date": 0,
        "has_publication_date": 0,
        # New Combination Counters
        "count_none": 0,
        "count_both": 0,
        "count_just_created": 0,
        "count_just_pub": 0,
        # Year Distributions
        "created_years": Counter(),
        "pub_years": Counter(),
    }

    none_records_buffer = []

    try:
        with open(filepath, "rb") as f:
            for line in f:
                if not line.strip():
                    continue

                try:
                    # READ (Speed: orjson)
                    if orjson:
                        record = orjson.loads(line)
                    else:
                        record = json.loads(line)

                    local_stats["total_records"] += 1

                    # Extract Dates
                    c_date = record.get("created_date")
                    p_date = record.get("publication_date")

                    has_c = bool(c_date)
                    has_p = bool(p_date)

                    # Update Basic Counts
                    if has_c:
                        local_stats["has_created_date"] += 1
                    if has_p:
                        local_stats["has_publication_date"] += 1

                    # Update Combination Counts
                    if has_c and has_p:
                        local_stats["count_both"] += 1
                    elif has_c and not has_p:
                        local_stats["count_just_created"] += 1
                    elif not has_c and has_p:
                        local_stats["count_just_pub"] += 1
                    else:
                        local_stats["count_none"] += 1
                        # Save 'none' record to buffer
                        none_records_buffer.append(line.strip())

                    # Year Extraction (Optimized String Slicing)
                    if has_c and isinstance(c_date, str) and len(c_date) >= 4:
                        year = c_date[:4]
                        if year.isdigit():
                            local_stats["created_years"][year] += 1

                    if has_p and isinstance(p_date, str) and len(p_date) >= 4:
                        year = p_date[:4]
                        if year.isdigit():
                            local_stats["pub_years"][year] += 1

                except Exception:
                    continue

        # WRITE 'NONE' RECORDS TO TEMP FILE
        if none_records_buffer:
            with open(none_filename, "wb") as f_none:
                for rec in none_records_buffer:
                    f_none.write(rec + b"\n")

    except Exception as e:
        print(f"Error reading {filepath}: {e}")

    return local_stats


def analyze_json_files(folder_path, output_dir="."):
    print("Scanning for files...")
    # Matches .ndjson recursively
    files = glob.glob(os.path.join(folder_path, "**", "*.ndjson"), recursive=True)
    total_files = len(files)

    if total_files == 0:
        print("No .ndjson files found!")
        return

    # Create directories
    os.makedirs(output_dir, exist_ok=True)
    temp_dir = os.path.join(output_dir, "temp_none_files")
    os.makedirs(temp_dir, exist_ok=True)

    print(f"Found {total_files} files. Starting processing...")

    total_stats = {
        "total_records": 0,
        "has_created_date": 0,
        "has_publication_date": 0,
        "count_none": 0,
        "count_both": 0,
        "count_just_created": 0,
        "count_just_pub": 0,
        "created_years": Counter(),
        "pub_years": Counter(),
    }

    start_time = time.time()

    # MULTIPROCESSING LOOP
    with ProcessPoolExecutor() as executor:
        # Pass temp_dir to worker so it knows where to dump partial files
        futures = [executor.submit(process_file, f, temp_dir) for f in files]

        for i, future in enumerate(as_completed(futures), 1):
            try:
                res = future.result()
                # Aggregate all counters
                for key, value in res.items():
                    if isinstance(value, Counter):
                        total_stats[key].update(value)
                    else:
                        total_stats[key] += value
            except Exception as e:
                print(f"\nWorker error: {e}")

            msg = f"Progress: {i}/{total_files} files processed ({(i / total_files) * 100:.1f}%)"
            print(msg, end="\r")
            sys.stdout.flush()

    print()

    # MERGE TEMP 'NONE' FILES
    print("Merging 'none' records into none.ndjson...")
    final_none_path = os.path.join(output_dir, "none.ndjson")

    with open(final_none_path, "wb") as outfile:
        temp_files = glob.glob(os.path.join(temp_dir, "none_part_*.ndjson"))
        for tmp in temp_files:
            with open(tmp, "rb") as infile:
                shutil.copyfileobj(infile, outfile)
            os.remove(tmp)  # Cleanup immediately

    # Remove temp folder if empty
    try:
        os.rmdir(temp_dir)
    except OSError:
        pass

    end_time = time.time()
    print(f"Processing complete in {end_time - start_time:.2f} seconds.")

    # --- SAVING STATS ---
    print("Saving statistics...")

    # A. JSON Report
    json_path = os.path.join(output_dir, "full_stats.json")
    with open(json_path, "w") as f:
        clean_stats = total_stats.copy()
        clean_stats["created_years"] = dict(total_stats["created_years"])
        clean_stats["pub_years"] = dict(total_stats["pub_years"])
        json.dump(clean_stats, f, indent=4)

    # B. CSV Helper
    def write_sorted_csv(filename, counter_obj):
        csv_path = os.path.join(output_dir, filename)
        sorted_data = sorted(counter_obj.items(), key=lambda x: x[0])
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Year", "Count"])
            writer.writerows(sorted_data)

    write_sorted_csv("created_years.csv", total_stats["created_years"])
    write_sorted_csv("pub_years.csv", total_stats["pub_years"])

    print("\nSummary:")
    print(f"  Total Records:   {total_stats['total_records']:,}")
    print(f"  Both Dates:      {total_stats['count_both']:,}")
    print(f"  Just Created:    {total_stats['count_just_created']:,}")
    print(f"  Just Published:  {total_stats['count_just_pub']:,}")
    print(f"  No Dates (None): {total_stats['count_none']:,} (Saved to none.ndjson)")
