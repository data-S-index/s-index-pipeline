import json
import os
from datetime import datetime
from pathlib import Path


def get_citation_blocks_from_ndjson(ndjson_folder, output_file_path):
    """Extracts records containing citations from NDJSON files and writes a
    slim citation block file retaining only identifier, citations, and pubyear.

    Reads processed DataCite slim records from NDJSON files and filters to
    only those with a citations field. Citation entries of type 'dois' are
    stored under citations.dois; all other types are stored under
    citations.other (omitted if empty).

    Args:
        ndjson_folder: Path to the folder containing input .ndjson files.
        output_file_path: Path to the output .ndjson file to write results to.
            Parent directories will be created if they do not exist. Any
            existing file at this path will be overwritten.

    Returns:
        None

    Raises:
        FileNotFoundError: If ndjson_folder does not exist.
        OSError: If the output file cannot be written.
    """
    output_dir = os.path.dirname(output_file_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    all_files = list(Path(ndjson_folder).glob("*.ndjson"))

    if not all_files:
        print(f"[{datetime.now()}] No files found.")
        return

    print(f"[{datetime.now()}] Processing {len(all_files)} files...")

    total_saved = 0
    with open(output_file_path, "w", encoding="utf-8") as out:
        for i, f in enumerate(all_files, 1):
            with open(f, encoding="utf-8") as infile:
                for line in infile:
                    record = json.loads(line)

                    citations = record.get("citations")
                    if not citations:
                        continue

                    doi = next(
                        (
                            item["identifier"]
                            for item in record.get("identifiers", [])
                            if item.get("identifier_type") == "doi"
                        ),
                        None,
                    )
                    if not doi:
                        continue

                    slim = {
                        "identifiers": [{"identifier": doi, "identifier_type": "doi"}],
                        "citations": citations,
                        "pubyear": record.get("pubyear"),
                    }
                    out.write(json.dumps(slim, ensure_ascii=False) + "\n")
                    total_saved += 1

            print(
                f"\r[{i}/{len(all_files)}] {f.name} — {total_saved:,} saved so far",
                end="",
                flush=True,
            )

    print(f"\n[{datetime.now()}] Complete! Total Records Saved: {total_saved:,}")


def get_new_citation_blocks(
    existing_citations_paths: str | list[str],
    update_citations_path: str,
    output_file_path: str,
    dataset_db_path: str | None = None,
    datasets_added_before: str | None = None,
) -> None:
    """Compares an updated citation block file against one or more existing ones
    and writes only records with new (dataset_id, citation_link) pairs not seen before.

    For each dataset in the update file, any citation DOIs or other citation links
    already present in any of the existing files for that dataset are excluded.
    Datasets with no net-new citations are dropped entirely. Datasets not seen
    before are written as-is. If dataset_db_path is provided, only datasets whose
    DOI exists in the my_datasets table are kept.

    Both citations.dois and citations.other are compared independently. The output
    citations block omits empty keys — if a dataset only has new DOI citations,
    citations.other is not written, and vice versa.

    Output records use the slim format:
        {"identifiers": [...], "citations": {"dois": [...], "other": [...]}, "pubyear": ...}

    Args:
        existing_citations_paths: Path or list of paths to existing
            citations_block.ndjson files. All known pairs are merged before
            comparison.
        update_citations_path: Path to the newly created citations_block from
            the update harvest.
        output_file_path: Path to write the net-new citation records to.
        dataset_db_path: Optional path to a DuckDB database containing a
            my_datasets table with a dataset_id column and an added_date column.
            When provided, only records whose DOI matches a known dataset_id
            are kept.
        datasets_added_before: Optional ISO date string (YYYY-MM-DD). When
            provided alongside dataset_db_path, only dataset IDs with
            added_date before this date are considered. Useful to exclude
            datasets added after the original harvest cutoff.

    Returns:
        None
    """
    if isinstance(existing_citations_paths, str):
        existing_citations_paths = [existing_citations_paths]

    # Optionally load known dataset IDs from DuckDB
    known_dataset_ids: set | None = None
    if dataset_db_path:
        import duckdb

        print(f"[{datetime.now()}] Loading known dataset IDs from {dataset_db_path}...")
        con = duckdb.connect(dataset_db_path, read_only=True)
        query = "SELECT dataset_id FROM my_datasets"
        if datasets_added_before:
            query += f" WHERE added_date < '{datasets_added_before}'"
        rows = con.execute(query).fetchall()
        con.close()
        known_dataset_ids = {row[0] for row in rows}
        print(
            f"[{datetime.now()}] Loaded {len(known_dataset_ids):,} known dataset IDs"
            + (
                f" added before {datasets_added_before}"
                if datasets_added_before
                else ""
            )
            + "."
        )

    # Build lookup: dataset_doi -> {"dois": set, "other_ids": set}
    # merged across all existing files
    print(
        f"[{datetime.now()}] Loading existing citation pairs from "
        f"{len(existing_citations_paths)} file(s)..."
    )
    existing: dict[str, dict] = {}
    for path in existing_citations_paths:
        file_count = 0
        with open(path, encoding="utf-8") as f:
            for line in f:
                record = json.loads(line)
                doi = next(
                    (
                        i["identifier"]
                        for i in record.get("identifiers", [])
                        if i.get("identifier_type") == "doi"
                    ),
                    None,
                )
                if doi:
                    if doi not in existing:
                        existing[doi] = {"dois": set(), "other_ids": set()}
                    existing[doi]["dois"].update(
                        record.get("citations", {}).get("dois", [])
                    )
                    existing[doi]["other_ids"].update(
                        obj.get("id")
                        for obj in record.get("citations", {}).get("other", [])
                        if obj.get("id")
                    )
                    file_count += 1
        print(f"  {Path(path).name}: {file_count:,} records loaded.")

    print(
        f"[{datetime.now()}] {len(existing):,} unique datasets across all existing files. "
        f"Comparing updates..."
    )

    output_dir = os.path.dirname(output_file_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    total_saved = 0
    total_new_pairs = 0
    total_skipped_unknown = 0
    with (
        open(update_citations_path, encoding="utf-8") as infile,
        open(output_file_path, "w", encoding="utf-8") as out,
    ):
        for line in infile:
            record = json.loads(line)
            doi = next(
                (
                    i["identifier"]
                    for i in record.get("identifiers", [])
                    if i.get("identifier_type") == "doi"
                ),
                None,
            )
            if not doi:
                continue

            if known_dataset_ids is not None and doi not in known_dataset_ids:
                total_skipped_unknown += 1
                continue

            update_dois = set(record.get("citations", {}).get("dois", []))
            update_other = {
                obj.get("id"): obj
                for obj in record.get("citations", {}).get("other", [])
                if obj.get("id")
            }

            known_entry = existing.get(doi, {})
            new_dois = update_dois - known_entry.get("dois", set())
            new_other = {
                id_val: obj
                for id_val, obj in update_other.items()
                if id_val not in known_entry.get("other_ids", set())
            }

            if not new_dois and not new_other:
                continue

            citations = {}
            if new_dois:
                citations["dois"] = sorted(new_dois)
            if new_other:
                citations["other"] = list(new_other.values())

            slim = {
                "identifiers": record.get("identifiers"),
                "citations": citations,
                "pubyear": record.get("pubyear"),
            }
            out.write(json.dumps(slim, ensure_ascii=False) + "\n")
            total_saved += 1
            total_new_pairs += len(new_dois) + len(new_other)

    print(
        f"[{datetime.now()}] Done. "
        f"{total_saved:,} datasets with new citations, "
        f"{total_new_pairs:,} new citation pairs total"
        + (
            f", {total_skipped_unknown:,} skipped (not in known datasets)"
            if known_dataset_ids is not None
            else ""
        )
        + "."
    )
