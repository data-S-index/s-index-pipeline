from __future__ import annotations

import json
import pathlib
import sys
import time

import requests

from sindex.core.ids import _norm_doi

from .constants import DEFAULT_PASSWORD, DEFAULT_USERNAME, FUJI_DEFAULT_BASE_URL
from .discovery import fair_evaluate_doi_url
from .normalize import fuji_report_from_response


def fair_evaluation_report(
    doi_or_url: str,
    *,
    base_url: str = FUJI_DEFAULT_BASE_URL,
    username: str | None = DEFAULT_USERNAME,
    password: str | None = DEFAULT_PASSWORD,
    session: requests.Session | None = None,
) -> dict:
    """
    Wrapper:
      - discovery.fair_evaluate_doi_url() -> raw F-UJI JSON
      - normalize.fuji_report_from_response() -> compact FAIR report
    """
    raw = fair_evaluate_doi_url(
        doi_or_url,
        base_url=base_url,
        username=username,
        password=password,
        session=session,
    )
    return fuji_report_from_response(raw)


def batch_fair_evaluation_report(
    input_folder: str, output_file: str, error_file: str = None
):
    """
    Iterates through ndjson files, evaluates FAIRness,
    and writes results/errors to separate ndjson files.
    """
    input_path = pathlib.Path(input_folder)
    output_path = pathlib.Path(output_file)

    # Default error file to current folder with a timestamp if not provided
    if error_file is None:
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        error_file = f"fair_errors_{timestamp}.ndjson"
    error_path = pathlib.Path(error_file)

    processed_count = 0
    error_count = 0

    files = list(input_path.glob("*.ndjson"))
    if not files:
        print(f"No .ndjson files found in {input_folder}")
        return

    print(f"Starting batch process on {len(files)} files...")

    with (
        output_path.open("w", encoding="utf-8") as f_out,
        error_path.open("w", encoding="utf-8") as f_err,
    ):
        for file_idx, file_path in enumerate(files, 1):
            with file_path.open("r", encoding="utf-8") as f_in:
                for line_num, line in enumerate(f_in, 1):
                    line = line.strip()
                    if not line:
                        continue

                    processed_count += 1

                    # Update progress line (live update)
                    status_msg = (
                        f"\r[File {file_idx}/{len(files)}] "
                        f"Processed: {processed_count} | Errors: {error_count} | "
                        f"Current: {file_path.name[:20]}..."
                    )
                    sys.stdout.write(status_msg)
                    sys.stdout.flush()

                    # Initialize variables for the error log scope
                    dataset_id = "Unknown"
                    doi_or_url = "N/A"

                    try:
                        # Parse JSON first
                        try:
                            data = json.loads(line)
                        except json.JSONDecodeError as je:
                            raise ValueError(
                                f"Invalid JSON on line {line_num}: {str(je)}"
                            )

                        # ID extraction logic
                        identifiers = data.get("identifiers", [])
                        primary_id = (
                            identifiers[0].get("identifier") if identifiers else None
                        )
                        url_fallback = data.get("url")

                        if primary_id and _norm_doi(primary_id):
                            doi_or_url = primary_id
                            dataset_id = primary_id
                        else:
                            doi_or_url = url_fallback
                            dataset_id = (
                                primary_id
                                if primary_id
                                else (url_fallback or "Unknown")
                            )

                        if not doi_or_url:
                            raise ValueError(
                                f"No valid DOI or URL found on line {line_num}"
                            )

                        # Evaluation report
                        report = fair_evaluation_report(doi_or_url)

                        result = {
                            "dataset_id": dataset_id,
                            "score": report["fair_score"],
                            "evaluationDate": report["evaluation_date"],
                            "metricVersion": report["fuji_metric_version"],
                            "softwareVersion": report["fuji_software_version"],
                        }
                        f_out.write(json.dumps(result) + "\n")
                        f_out.flush()

                    except Exception as e:
                        error_count += 1
                        error_log = {
                            "dataset_id": dataset_id,
                            "url": doi_or_url,
                            "error_message": str(e),
                            "file_source": file_path.name,
                            "line_number": line_num,
                            "timestamp": time.strftime(
                                "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                            ),
                        }
                        f_err.write(json.dumps(error_log) + "\n")
                        f_err.flush()

    print("\n\nProcessing Complete!")
    print(f"Successes saved to: {output_path.absolute()}")
    print(f"Errors saved to:    {error_path.absolute()}")
