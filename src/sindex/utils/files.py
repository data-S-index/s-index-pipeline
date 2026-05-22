import glob
import json
import os
import sys


def combine_ndjson_files(input_paths, output_path):
    """
    Combines multiple .ndjson files efficiently (text-mode) with a progress counter.
    """
    processed_count = 0

    try:
        with open(output_path, "w", encoding="utf-8") as outfile:
            for file_path in input_paths:
                with open(file_path, "r", encoding="utf-8") as infile:
                    for line in infile:
                        # Skip only truly empty lines (whitespace-only)
                        if not line.strip():
                            continue

                        # Write the raw line directly
                        outfile.write(line)

                        # Ensure line ends with \n
                        if not line.endswith("\n"):
                            outfile.write("\n")

                        processed_count += 1

                        # Update progress every 100k
                        if processed_count % 100000 == 0:
                            sys.stdout.write(f"\rLines processed: {processed_count:,}")
                            sys.stdout.flush()

        print(f"\nFinished! Total entries saved: {processed_count:,}")

    except Exception as e:
        print(f"\nAn error occurred: {e}")


def merge_ndjson_files_in_folder(input_folder: str, output_file: str):
    """
    Merges all *.ndjson files found inside the input_folder.
    """
    # Paths
    search_pattern = os.path.join(input_folder, "*.ndjson")
    files = glob.glob(search_pattern)

    # Filter out directories and the output file itself
    files = [
        f
        for f in files
        if os.path.isfile(f) and os.path.abspath(f) != os.path.abspath(output_file)
    ]

    if not files:
        print(f"No .ndjson files found in folder: {input_folder}")
        return

    total_lines = 0
    print(f"Found {len(files)} files in '{input_folder}'. Starting merge...")

    with open(output_file, "w", encoding="utf-8") as outfile:
        for fname in files:
            try:
                with open(fname, "r", encoding="utf-8") as infile:
                    # Initialize line to handle empty files
                    line = None
                    for line in infile:
                        outfile.write(line)
                        total_lines += 1

                        if total_lines % 10000 == 0:
                            sys.stdout.write(f"\rProcessed lines: {total_lines:,}")
                            sys.stdout.flush()

                    # Ensure the file ended with a newline (if it wasn't empty)
                    if line is not None and not line.endswith("\n"):
                        outfile.write("\n")

            except PermissionError:
                print(f"\nSkipping {fname} (Permission Denied)")

    print(f"\r Done! Total lines in '{output_file}': {total_lines:,}")


def filter_ndjson_by_identifier_prefix(
    input_folder: str, output_file: str, prefix: str
) -> int:
    ndjson_files = glob.glob(os.path.join(input_folder, "*.ndjson"))
    total_files = len(ndjson_files)
    print(f"Found {total_files} NDJSON files in '{input_folder}'")

    matches = 0

    with open(output_file, "w", encoding="utf-8") as out_f:
        for i, file_path in enumerate(ndjson_files, start=1):
            print(
                f"\rProcessed: {i}/{total_files} files, Prefix matches: {matches}",
                end="",
                flush=True,
            )
            with open(file_path, "r", encoding="utf-8") as in_f:
                for line in in_f:
                    line = line.strip()
                    if not line or prefix not in line:
                        continue
                    try:
                        record = json.loads(line)
                        if any(
                            str(id_obj.get("identifier", "")).startswith(prefix)
                            for id_obj in record.get("identifiers", [])
                        ):
                            out_f.write(line + "\n")
                            matches += 1
                    except json.JSONDecodeError:
                        continue

    print(f"\nDone. {matches} matching records written to '{output_file}'")
    return matches
