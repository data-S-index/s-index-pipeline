import glob
import os
import sys

import orjson


def merge_doi_fair_scores_ndjson_files(input_folder: str, output_file: str):
    """
    Merges all *.ndjson files found inside the input_folder and
    renames 'doi' to 'dataset_id'
    """
    search_pattern = os.path.join(input_folder, "*.ndjson")
    files = glob.glob(search_pattern)

    files = [
        f
        for f in files
        if os.path.isfile(f) and os.path.abspath(f) != os.path.abspath(output_file)
    ]

    if not files:
        print(f"No .ndjson files found in folder: {input_folder}")
        return

    total_lines = 0
    print(f"Found {len(files)} files. Starting merge with orjson...")

    # Open output in Binary Write mode ("wb")
    with open(output_file, "wb") as outfile:
        for fname in files:
            try:
                # Open input in Binary Read mode ("rb")
                with open(fname, "rb") as infile:
                    for line in infile:
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            # 1. Parse bytes
                            data = orjson.loads(line)

                            # 2. Rename the key
                            if "doi" in data:
                                data["dataset_id"] = data.pop("doi")

                            # 3. Dump back to bytes and add a byte-newline
                            outfile.write(orjson.dumps(data) + b"\n")

                            total_lines += 1

                            if total_lines % 100000 == 0:
                                sys.stdout.write(f"\rProcessed lines: {total_lines:,}")
                                sys.stdout.flush()

                        except orjson.JSONDecodeError:
                            print(f"\nWarning: Skipped invalid JSON line in {fname}")

            except PermissionError:
                print(f"\nSkipping {fname} (Permission Denied)")

    print(f"\rDone! Total lines in '{output_file}': {total_lines:,}    ")


def extrapolate_emdb_fair_scores(emdb_file_path, scores_file_path, output_file_path):
    existing_scores_map = {}

    print(f"Loading scores from {scores_file_path}...")
    try:
        with open(scores_file_path, "rb") as f:
            for line in f:
                if not line.strip():
                    continue
                record = orjson.loads(line)
                if "dataset_id" in record:
                    existing_scores_map[record["dataset_id"]] = record
    except FileNotFoundError:
        print(f"Warning: Scores file '{scores_file_path}' not found.")

    # Counters
    total_input_records = 0
    total_output_records = 0
    count_found = 0
    count_extrapolated = 0
    skipped_records = 0

    print(f"Processing {emdb_file_path}...")

    # Open Input as binary ('rb') and Output as binary ('wb') for max speed
    with open(emdb_file_path, "rb") as f_in, open(output_file_path, "wb") as f_out:
        for line in f_in:
            if not line.strip():
                continue

            total_input_records += 1

            emdb_record = orjson.loads(line)

            # Logic to extract identifier
            emd_id = None
            identifiers = emdb_record.get("identifiers", [])
            for item in identifiers:
                if item.get("identifier_type") == "emdb_id":
                    emd_id = item.get("identifier")
                    break

            if not emd_id:
                # print(f"Warning: Line {total_input_records} missing ID.")
                skipped_records += 1
                continue

            if emd_id in existing_scores_map:
                output_record = existing_scores_map[emd_id]
                count_found += 1
            else:
                output_record = {
                    "dataset_id": emd_id,
                    "score": 42.31,
                    "softwareVersion": "extrapolated",
                }
                count_extrapolated += 1

            # orjson.dumps returns bytes, so we write bytes directly
            # We append b'\n' manually
            f_out.write(orjson.dumps(output_record) + b"\n")
            total_output_records += 1

    # Summary
    print(f"Total records in EMDB file:   {total_input_records}")
    print(f"Total records written:        {total_output_records}")
    print(f"  - Found existing scores:    {count_found}")
    print(f"  - Extrapolated scores:      {count_extrapolated}")
    print(f"  - Skipped (no ID):          {skipped_records}")

    if total_input_records == total_output_records:
        print("SUCCESS: Input count matches output count.")
    else:
        print(
            f"Note: {total_input_records - total_output_records} records skipped due to missing IDs."
        )
