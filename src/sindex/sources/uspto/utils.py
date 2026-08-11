import glob
import multiprocessing
import os
import zipfile

import orjson


def count_patents_in_zip(zip_path):
    """
    Opens a zip, reads the XML as raw bytes, and counts '<?xml' tags.
    This is much faster than parsing.
    """
    count = 0
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            # Find the XML file
            xml_files = [f for f in z.namelist() if f.lower().endswith(".xml")]
            if not xml_files:
                return 0

            target_xml = xml_files[0]

            # Open as binary stream
            with z.open(target_xml) as f:
                while True:
                    chunk = f.read(1024 * 1024 * 10)  # 10MB chunk
                    if not chunk:
                        break
                    count += chunk.count(b"<?xml")

    except Exception as e:
        print(f"Error reading {zip_path}: {e}")
        return 0

    return count


def count_patents_in_zip_folder(input_dir):
    zip_files = glob.glob(os.path.join(input_dir, "*.zip"))
    total_files = len(zip_files)

    print(f"Found {total_files} zip files. Starting fast count")

    # Use all available cores
    cpu_count = multiprocessing.cpu_count()
    total_patents = 0

    with multiprocessing.Pool(processes=cpu_count) as pool:
        # Map the function to the files
        for i, result in enumerate(
            pool.imap_unordered(count_patents_in_zip, zip_files)
        ):
            total_patents += result

            # One-line progress
            print(
                f"Scanned {i + 1}/{total_files} files. Total Patents so far: {total_patents:,}",
                end="\r",
                flush=True,
            )

    print("\nDone counting!")
    print(f"Total Files Scanned: {total_files}")
    print(f"Total Patents Found: {total_patents:,}")


def count_stats_in_file_fast(file_path):
    """
    Worker function to count number of lines, dois, emdb ids in output file
    """
    c_patents = 0
    c_dois = 0
    c_emdbs = 0

    try:
        with open(file_path, "rb") as f:
            for line in f:
                if not line.strip():
                    continue

                try:
                    data = orjson.loads(line)

                    c_patents += 1

                    if "doi" in data:
                        c_dois += len(data["doi"])

                    if "emdb_id" in data:
                        c_emdbs += len(data["emdb_id"])

                except orjson.JSONDecodeError:
                    pass

    except Exception:
        return (0, 0, 0)

    return (c_patents, c_dois, c_emdbs)


def count_outputs_stats(output_dir):
    files = glob.glob(os.path.join(output_dir, "*.ndjson"))
    total_files = len(files)

    if total_files == 0:
        print(f"No .ndjson files found in {output_dir}")
        return

    print(f"Scanning {total_files} files using orjson...")

    total_patents = 0
    total_dois = 0
    total_emdbs = 0

    cpu_count = multiprocessing.cpu_count()

    with multiprocessing.Pool(processes=cpu_count) as pool:
        for i, (p, d, e) in enumerate(
            pool.imap_unordered(count_stats_in_file_fast, files)
        ):
            total_patents += p
            total_dois += d
            total_emdbs += e

            # Progress
            msg = f"Scanned {i + 1}/{total_files} | Patents: {total_patents:,} | DOIs: {total_dois:,} | EMDBs: {total_emdbs:,}"
            print(msg.ljust(100), end="\r", flush=True)

    print("\nDone counting!")
    print(f"Files Processed:      {total_files:,}")
    print(f"Total Patents Found:  {total_patents:,}")
    print(f"Total DOIs Found:     {total_dois:,}")
    print(f"Total EMDB IDs Found: {total_emdbs:,}")
