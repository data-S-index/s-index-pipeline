import glob
import io
import json
import multiprocessing
import os
import zipfile

from lxml import etree

from sindex.core.ids import _DOI_PATTERN, _EMDB_PATTERN, _norm_doi, _norm_emdb_id


def get_text_content(element):
    """Recursively retrieves all text from an element and its children."""
    return "".join(element.itertext())


def stream_uspto_xml(filename):
    """
    Reads a concatenated XML file and yields distinct patent XML strings.
    Splits content whenever a new '<?xml' declaration is found.
    """
    buffer = []
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            # Check for the start of a new XML document
            if line.strip().startswith("<?xml") and buffer:
                yield "".join(buffer)
                buffer = []
            buffer.append(line)

        # Yield the final patent in the buffer
        if buffer:
            yield "".join(buffer)


def process_patent_xml(xml_file_path, output_file_path):
    stats = {"scanned": 0, "saved": 0, "dois_found": 0, "emdb_ids_found": 0}

    print(f"Starting scan of {xml_file_path}...")

    # Parser configuration: recover=True helps with minor syntax errors
    parser = etree.XMLParser(recover=True, resolve_entities=False)

    with open(output_file_path, "w", encoding="utf-8") as f_out:
        # LOOP CHANGES: Iterate over the generator instead of iterparse
        for xml_content in stream_uspto_xml(xml_file_path):
            stats["scanned"] += 1

            try:
                # Parse the single patent string
                # We use .encode() because fromstring expects bytes or a string with encoding decl
                elem = etree.fromstring(xml_content.encode("utf-8"), parser=parser)

                # --- 1. Extract ID ---
                doc_number_node = elem.find(
                    ".//publication-reference/document-id/doc-number"
                )
                patent_id = (
                    doc_number_node.text if doc_number_node is not None else None
                )

                if not patent_id:
                    continue

                # --- 2. Extract Date ---
                date_node = elem.find(".//publication-reference/document-id/date")
                mention_date = date_node.text if date_node is not None else None

                # --- 3. Text & Regex ---
                full_text = get_text_content(elem)

                raw_dois = _DOI_PATTERN.findall(full_text)
                raw_emdbs = _EMDB_PATTERN.findall(full_text)

                clean_dois = sorted(list(set(_norm_doi(d) for d in raw_dois)))
                clean_emdbs = sorted(list(set(_norm_emdb_id(e) for e in raw_emdbs)))

                # --- 4. Save ---
                if clean_dois or clean_emdbs:
                    stats["saved"] += 1
                    stats["dois_found"] += len(clean_dois)
                    stats["emdb_ids_found"] += len(clean_emdbs)

                    record = {
                        "patent_id": patent_id,
                        "mention_link": f"https://image-ppubs.uspto.gov/dirsearch-public/print/downloadPdf/{patent_id}",
                        "mention_date": mention_date,
                        "doi": clean_dois,
                        "emdb_id": clean_emdbs,
                    }
                    f_out.write(json.dumps(record) + "\n")

                # --- 5. Cleanup ---
                # Explicitly clear the element to free memory
                elem.clear()

            except Exception as e:
                # Print error but keep going
                print(f"Error processing patent #{stats['scanned']}: {e}")

    # --- Summary ---
    print("Done!")
    print(f"Patents Scanned:      {stats['scanned']:,}")
    print(f"Patents Saved:        {stats['saved']:,}")
    print(f"Total DOIs Found:     {stats['dois_found']:,}")
    print(f"Total EMDB IDs Found: {stats['emdb_ids_found']:,}")
    print(f"Output saved to: {output_file_path}")


### Process zip files directly
def stream_uspto_xml_from_obj(file_obj):
    """
    Reads from an OPEN file object (stream) and yields distinct patent XML strings.
    """
    buffer = []
    # file_obj is already a TextIOWrapper (string stream)
    for line in file_obj:
        # Check for the start of a new XML document
        if line.strip().startswith("<?xml") and buffer:
            yield "".join(buffer)
            buffer = []
        buffer.append(line)

    # Yield the final patent in the buffer
    if buffer:
        yield "".join(buffer)


def process_single_zip(zip_path, output_dir):
    """
    Returns stats dictionary for a single zip file.
    """
    base_name = os.path.basename(zip_path).replace(".zip", ".ndjson")
    output_path = os.path.join(output_dir, base_name)

    # Initialize stats for this file
    stats = {"scanned": 0, "saved": 0, "dois_found": 0, "emdb_ids_found": 0}

    if os.path.exists(output_path):
        # If skipping, return empty stats
        return stats

    parser = etree.XMLParser(recover=True, resolve_entities=False)

    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            xml_files = [f for f in z.namelist() if f.lower().endswith(".xml")]

            if not xml_files:
                return stats

            target_xml = xml_files[0]

            with z.open(target_xml) as f_bin:
                with io.TextIOWrapper(f_bin, encoding="utf-8") as f_text:
                    with open(output_path, "w", encoding="utf-8") as f_out:
                        for xml_content in stream_uspto_xml_from_obj(f_text):
                            stats["scanned"] += 1

                            try:
                                elem = etree.fromstring(
                                    xml_content.encode("utf-8"), parser=parser
                                )

                                # Extract ID
                                doc_number_node = elem.find(
                                    ".//publication-reference/document-id/doc-number"
                                )
                                patent_id = (
                                    doc_number_node.text
                                    if doc_number_node is not None
                                    else None
                                )

                                if not patent_id:
                                    elem.clear()
                                    continue

                                # Extract Date
                                date_node = elem.find(
                                    ".//publication-reference/document-id/date"
                                )
                                mention_date = (
                                    date_node.text if date_node is not None else None
                                )

                                # Regex Search
                                full_text = get_text_content(elem)
                                raw_dois = _DOI_PATTERN.findall(full_text)
                                raw_emdbs = _EMDB_PATTERN.findall(full_text)

                                clean_dois = sorted(
                                    list(set(_norm_doi(d) for d in raw_dois))
                                )
                                clean_emdbs = sorted(
                                    list(set(_norm_emdb_id(e) for e in raw_emdbs))
                                )

                                if clean_dois or clean_emdbs:
                                    stats["saved"] += 1
                                    stats["dois_found"] += len(clean_dois)
                                    stats["emdb_ids_found"] += len(clean_emdbs)

                                    record = {
                                        "patent_id": patent_id,
                                        "mention_link": f"https://image-ppubs.uspto.gov/dirsearch-public/print/downloadPdf/{patent_id}",
                                        "mention_date": mention_date,
                                        "doi": clean_dois,
                                        "emdb_id": clean_emdbs,
                                        "source_file": base_name,
                                    }
                                    f_out.write(json.dumps(record) + "\n")

                                # Memory Cleanup
                                elem.clear()
                                while elem.getprevious() is not None:
                                    del elem.getparent()[0]

                            except Exception:
                                pass  # efficient skip on malformed chunks

    except Exception as e:
        print(f"  Error reading zip {zip_path}: {e}")

    return stats


def process_patents_zips(input_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    zip_files = glob.glob(os.path.join(input_dir, "*.zip"))
    total_files = len(zip_files)

    # Global Accumulator
    total_stats = {"scanned": 0, "saved": 0, "dois_found": 0, "emdb_ids_found": 0}

    print(f"Found {total_files} zip files. Starting...")

    for i, zip_path in enumerate(zip_files):
        print(
            f"Processing {i + 1}/{total_files}: {os.path.basename(zip_path)}",
            end="\r",
            flush=True,
        )

        # Process and get stats for this specific file
        file_stats = process_single_zip(zip_path, output_dir)

        # Accumulate
        total_stats["scanned"] += file_stats["scanned"]
        total_stats["saved"] += file_stats["saved"]
        total_stats["dois_found"] += file_stats["dois_found"]
        total_stats["emdb_ids_found"] += file_stats["emdb_ids_found"]

    print("\nDone!")
    print(f"Patents Scanned:      {total_stats['scanned']:,}")
    print(f"Patents Saved:        {total_stats['saved']:,}")
    print(f"Total DOIs Found:     {total_stats['dois_found']:,}")
    print(f"Total EMDB IDs Found: {total_stats['emdb_ids_found']:,}")
    print(f"Output saved to: {output_dir}")


## Parallel


def process_single_zip_parallel(args):
    zip_path, output_dir = args
    base_name = os.path.basename(zip_path).replace(".zip", ".ndjson")
    output_path = os.path.join(output_dir, base_name)

    # 1. Define Temporary File
    temp_path = output_path + ".tmp"

    stats = {"scanned": 0, "saved": 0, "dois_found": 0, "emdb_ids_found": 0}

    # Resume Logic: If final file exists, skip.
    if os.path.exists(output_path):
        return {"file": base_name, "stats": stats, "skipped": True}

    parser = etree.XMLParser(recover=True, resolve_entities=False)

    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            xml_files = [f for f in z.namelist() if f.lower().endswith(".xml")]
            if not xml_files:
                return {"file": base_name, "stats": stats, "skipped": False}
            target_xml = xml_files[0]

            with z.open(target_xml) as f_bin:
                with io.TextIOWrapper(f_bin, encoding="utf-8") as f_text:
                    # 2. Write to TEMP file first
                    with open(temp_path, "w", encoding="utf-8") as f_out:
                        for xml_content in stream_uspto_xml_from_obj(f_text):
                            stats["scanned"] += 1
                            try:
                                elem = etree.fromstring(
                                    xml_content.encode("utf-8"), parser=parser
                                )

                                doc_node = elem.find(
                                    ".//publication-reference/document-id/doc-number"
                                )
                                patent_id = (
                                    doc_node.text if doc_node is not None else None
                                )
                                if not patent_id:
                                    elem.clear()
                                    continue

                                date_node = elem.find(
                                    ".//publication-reference/document-id/date"
                                )
                                mention_date = (
                                    date_node.text if date_node is not None else None
                                )

                                full_text = get_text_content(elem)
                                raw_dois = _DOI_PATTERN.findall(full_text)
                                raw_emdbs = _EMDB_PATTERN.findall(full_text)

                                clean_dois = sorted(
                                    list(set(_norm_doi(d) for d in raw_dois))
                                )
                                clean_emdbs = sorted(
                                    list(set(_norm_emdb_id(e) for e in raw_emdbs))
                                )

                                if clean_dois or clean_emdbs:
                                    stats["saved"] += 1
                                    stats["dois_found"] += len(clean_dois)
                                    stats["emdb_ids_found"] += len(clean_emdbs)

                                    record = {
                                        "patent_id": patent_id,
                                        "mention_link": f"https://image-ppubs.uspto.gov/dirsearch-public/print/downloadPdf/{patent_id}",
                                        "mention_date": mention_date,
                                        "doi": clean_dois,
                                        "emdb_id": clean_emdbs,
                                        "source_file": base_name,
                                    }
                                    f_out.write(json.dumps(record) + "\n")

                                elem.clear()
                                while elem.getprevious() is not None:
                                    del elem.getparent()[0]
                            except Exception:
                                pass

        # 3. SUCCESS: Rename .tmp -> .ndjson
        # This operation is atomic (instant) on most OSs
        if os.path.exists(temp_path):
            os.replace(temp_path, output_path)

    except Exception:
        # 4. FAILURE: Cleanup
        # If the zip crashed halfway, delete the partial .tmp file
        if os.path.exists(temp_path):
            os.remove(temp_path)
        pass  # Return empty stats so script continues, but this file is effectively "not done"

    return {"file": base_name, "stats": stats, "skipped": False}


def process_patents_zip_parallel(input_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 1. Gather all ZIPs
    all_zips = glob.glob(os.path.join(input_dir, "*.zip"))
    total_found = len(all_zips)

    # 2. Filter out already processed files
    # Only process ZIPs if their .ndjson doesn't exist yet
    tasks = []
    skipped_count = 0

    for z in all_zips:
        expected_json = os.path.basename(z).replace(".zip", ".ndjson")
        if os.path.exists(os.path.join(output_dir, expected_json)):
            skipped_count += 1
        else:
            tasks.append((z, output_dir))

    to_process_count = len(tasks)

    # 3. Print Initial Stats
    print(f"{total_found} zip files found in {input_dir}")
    print(f"{skipped_count} matching outputs already exist in {output_dir}")

    if to_process_count == 0:
        print("All files processed. Nothing to do.")
        return

    print(f"Processing remaining {to_process_count} files")

    # 4. Start Pool
    total_stats = {"scanned": 0, "saved": 0, "dois_found": 0, "emdb_ids_found": 0}

    cpu_count = max(1, multiprocessing.cpu_count() - 6)

    with multiprocessing.Pool(processes=cpu_count) as pool:
        for i, result in enumerate(
            pool.imap_unordered(process_single_zip_parallel, tasks)
        ):
            s = result["stats"]

            total_stats["scanned"] += s["scanned"]
            total_stats["saved"] += s["saved"]
            total_stats["dois_found"] += s["dois_found"]
            total_stats["emdb_ids_found"] += s["emdb_ids_found"]

            # Progress
            msg = f"Progress: {i + 1}/{to_process_count} | (Scanned {total_stats['scanned']} patents, found {total_stats['saved']} with relevant IDs)"
            print(msg.ljust(100), end="\r", flush=True)

    print()
    print("\Done!")
    print(f"New Patents Scanned:      {total_stats['scanned']:,}")
    print(f"New Patents Saved:        {total_stats['saved']:,}")
    print(f"Total New DOIs Found:     {total_stats['dois_found']:,}")
    print(f"Total New EMDB IDs Found: {total_stats['emdb_ids_found']:,}")
    print(f"Output saved to: {output_dir}")
