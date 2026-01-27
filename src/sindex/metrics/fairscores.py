import orjson


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
