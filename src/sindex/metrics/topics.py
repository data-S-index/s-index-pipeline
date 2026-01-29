import csv
import json
import os


def enhance_topics(ndjson_path, csv_path, output_path, limit=None):
    print("Starting  enhancement")

    print("Loading CSV")
    mapping = {}

    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            # Clean headers: remove potential whitespace around column names
            # This handles "topic_id, subfield_id" vs "topic_id,subfield_id"
            headers = [h.strip() for h in f.readline().split(",")]

            # Use DictReader with our clean headers
            reader = csv.DictReader(f, fieldnames=headers)

            for row in reader:
                # Get the raw ID
                raw_id = row.get("topic_id", "").strip()
                if not raw_id:
                    continue  # Skip empty rows

                # STANDARDIZE ID: Ensure it starts with "T"
                # If CSV has "10001", this makes it "T10001" to match your JSON
                key_id = f"T{raw_id}" if not raw_id.startswith("T") else raw_id

                # Prepare the data payload
                # We remove 'topic_id' and 'topic_name' so we don't duplicate them in the JSON
                payload = {
                    k: v.strip() if v else None
                    for k, v in row.items()
                    if k not in ("topic_id", "topic_name")
                }

                mapping[key_id] = payload

        print(f"CSV loaded. {len(mapping):,} topics indexed.")

    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    print(f"Processing{' first ' + str(limit) if limit else ''} lines")

    # Remove output file if already exists
    if os.path.exists(output_path):
        os.remove(output_path)

    count = 0
    matches = 0

    with (
        open(ndjson_path, "r", encoding="utf-8") as infile,
        open(output_path, "w", encoding="utf-8") as outfile,
    ):
        for line in infile:
            if not line.strip():
                continue

            # Stop if limit reached
            if limit and count >= limit:
                break

            try:
                # Parse JSON line
                data = json.loads(line)

                # Get ID from JSON (e.g., "T10765")
                t_id = str(data.get("topic_id", "")).strip()

                # Check for match
                if t_id in mapping:
                    data.update(mapping[t_id])
                    matches += 1

                # Write to output file
                outfile.write(json.dumps(data, ensure_ascii=False) + "\n")

                count += 1
                if count % 100_000 == 0:
                    print(
                        f"\rLines processed: {count:,} | Matches: {matches:,}",
                        end="",
                        flush=True,
                    )

            except json.JSONDecodeError:
                print(f"Skipping bad JSON at line {count}")

    print("\n\nDone!")
    print(f"Total lines: {count:,}")
    print(f"Total matches: {matches:,}")
    print(f"Saved to: {output_path}")
