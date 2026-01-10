# fair.py

import glob
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3.util.retry import Retry

from pipeline.normalize import _collect_unique_slimmed_dois, _norm_date_iso


def fair_evaluation(
    doi_or_url: str,
    base_url: str = "http://localhost:1071",
    username: str | None = None,
    password: str | None = None,
    session: requests.Session | None = None,
) -> dict:
    """
    Run a FAIR evaluation for a given DOI using a local F-UJI instance.

    This function sends a request to the F-UJI evaluation API and returns a
    structured response under the "fair_evaluation" key. Authentication must
    match the Basic Auth credentials configured in `fuji_server/config/users.py`.

    Args:
        doi_or_url:
            The dataset DOI (e.g. "10.5281/zenodo.12345") or a resolvable URL.
        base_url:
            Base URL where the F-UJI server is running
            (e.g. "http://localhost:1071").
        username:
            Basic Auth username for F-UJI, or `None` for no authentication.
        password:
            Basic Auth password for F-UJI, or `None` for no authentication.
        session:
            Optional `requests.Session` for connection reuse. If `None`, a
            one-off request is issued via the top-level `requests` API.

    Returns:
        {
            "fair_score": <percent FAIR score from F-UJI>
        }

    Raises:
        requests.HTTPError:
            If F-UJI returns a non-success HTTP status code.
        requests.RequestException:
            For network-related errors.
    """
    evaluate_url = f"{base_url.rstrip('/')}/fuji/api/v1/evaluate"

    auth = None
    if username is not None and password is not None:
        auth = HTTPBasicAuth(username, password)

    payload = {"object_identifier": doi_or_url}

    if session is None:
        resp = requests.post(evaluate_url, json=payload, auth=auth, timeout=60)
    else:
        resp = session.post(evaluate_url, json=payload, auth=auth, timeout=60)

    resp.raise_for_status()
    results = resp.json()

    # Extract relevant parts of the response
    report = {
        "fair_score": results["summary"]["score_percent"]["FAIR"],
        "evaluation_date": _norm_date_iso(results["end_timestamp"]),
        "fuji_metric_version": results["metric_version"],
        "fuji_software_version": results["software_version"],
    }

    return report


def batch_fair_evaluations(
    slim_folder: str,
    output_path: str,
    base_url: str = "http://localhost:1071",
    username: str | None = None,
    password: str | None = None,
) -> int:
    """
    Run F-UJI FAIR evaluations for all datasets found in slimmed DataCite NDJSON files.

    Each input NDJSON line is expected to be a slimmed DataCite record containing
    at least a top-level `"doi"` key. This function loops one by one through each
    line of the NDJSON files, and for each unique DOI (unique for a given run of
    this function), this function calls `fair_evaluation()` and writes one NDJSON
    line of the form:
        {
            "doi": "<dataset-doi>",
            "fair_score: "<FAIR score from F-UJI>",
            "evaluation_date": "<ISO end date/time of the evaluation provided by F-UJI>",
            "fuji_metric_version": "<F-UJI metric used for the evaluation",
            "fuji_software_version": "<F-UJI version>",
        }

    Args:
        slim_folder:
            Directory containing slimmed DataCite `.ndjson` files (one JSON object
            per line, with a normalized "doi" key).
        output_path:
            Path to the output NDJSON file (e.g. `fair_scores.ndjson`).
        base_url:
            Base URL where the local F-UJI server is running
            (default `"http://localhost:1071"`).
        username:
            Basic Auth username for F-UJI (must match users.py), or `None`.
        password:
            Basic Auth password for F-UJI (must match users.py), or `None`.

    Returns:
        The number of datasets successfully evaluated and written to `output_path`.

    Notes:
        - Duplicate DOIs across files are evaluated only once.
        - Datasets without a `"doi"` field are skipped.
        - If F-UJI returns an HTTP error for a DOI, that DOI is skipped.
    """
    pattern = os.path.join(slim_folder, "*.ndjson")
    files = glob.glob(pattern)

    seen_dois = set()
    success_count = 0

    os.makedirs(os.path.dirname(os.path.abspath(output_path)) or ".", exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as out_f:
        for path in files:
            print(f"Scanning {path} for DOIs to evaluate ...")
            with open(path, "r", encoding="utf-8") as in_f:
                for line in in_f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        print(f"  Skipping malformed JSON line in {path}")
                        continue

                    doi = rec.get("doi")
                    if not isinstance(doi, str) or not doi.strip():
                        print("  Skipping record without a valid 'doi' field")
                        continue

                    if doi in seen_dois:
                        continue  # skip DOI if already evaluated during this run
                    seen_dois.add(doi)

                    print(f"  Evaluating FAIR score for {doi} ...")
                    try:
                        report = fair_evaluation(
                            doi_or_url=doi,
                            base_url=base_url,
                            username=username,
                            password=password,
                        )
                    except requests.HTTPError as e:
                        print(f"  F-UJI HTTP error for {doi}: {e}")
                        continue
                    except requests.RequestException as e:
                        print(f"  F-UJI request failed for {doi}: {e}")
                        continue

                    out_obj = {
                        "doi": doi,
                        **report,
                    }
                    out_f.write(json.dumps(out_obj, ensure_ascii=False) + "\n")
                    success_count += 1

    print(f"Finished FAIR evaluations. Wrote {success_count} records to {output_path}")
    return success_count


_thread_local = threading.local()


def _get_thread_session() -> requests.Session:
    """
    Return a thread-local requests.Session configured with connection pooling.

    A separate Session is created per worker thread and reused for all requests
    issued by that thread. This enables HTTP keep-alive and connection pooling
    without cross-thread sharing of Session objects.
    """
    if getattr(_thread_local, "session", None) is None:
        session = requests.Session()

        retries = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=[
                "HEAD",
                "GET",
                "POST",
                "PUT",
                "DELETE",
                "OPTIONS",
                "TRACE",
            ],
        )
        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=retries,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        _thread_local.session = session

    return _thread_local.session


def batch_fair_evaluations_fast(
    slim_folder: str,
    output_path: str,
    base_url: str = "http://localhost:1071",
    username: str | None = None,
    password: str | None = None,
    max_workers: int | None = None,
) -> int:
    """
    Run FAIR F-UJI evaluations for all datasets found in slimmed DataCite NDJSON files.

    This functionis similar to batch_fair_evaluations but has been improved for speed.
    Contrary to batch_fair_evaluations which runs FAIR evaluation for one DOI at a time,
    this function first uses `_collect_unique_slimmed_dois()` to collect a unique set
    of DOIs from the slimmed NDJSON files in parallel. Then it
    evaluates them in parallel using `fair_evaluation()`. Each worker thread
    reuses its own `requests.Session` for better performance.

    For each successfully evaluated DOI, it writes one NDJSON line to `output_path`
    (same format as the ouput of batch_fair_evaluations)

    Args:
        slim_folder:
            Directory containing slimmed DataCite `.ndjson` files (one JSON object
            per line, with a normalized "doi" key).
        output_path:
            Path to the output NDJSON file (e.g. `fair_scores.ndjson`).
        base_url:
            Base URL where the local F-UJI server is running
            (default `"http://localhost:1071"`).
        username:
            Basic Auth username for F-UJI (must match users.py), or `None`.
        password:
            Basic Auth password for F-UJI (must match users.py), or `None`.
        max_workers:
            Maximum number of parallel F-UJI requests. If `None`, a reasonable
            default is chosen based on CPU count. For most setups, 4–8 is a good
            starting range.

    Returns:
        The number of datasets successfully evaluated and written to `output_path`.

    Notes:
        - Duplicate DOIs across files are evaluated only once.
        - Datasets without a valid `"doi"` field in the slimmed files are skipped
          by `_collect_unique_slimmed_dois()`.
        - If F-UJI returns an HTTP error for a DOI, that DOI is skipped.
        - HTTP requests are executed in parallel via threads; file writing happens
          in the main thread to avoid synchronization issues.
    """
    # Step 1 — Collect normalized DOIs from slimmed NDJSON files (parallel)
    target_norms = _collect_unique_slimmed_dois(slim_folder, "*.ndjson")
    if not target_norms:
        print("No DOIs found in slimmed files; nothing to evaluate.")
        return 0

    print()

    # Step 2: FAIR evaluation (parallel)
    total_dois = len(target_norms)

    os.makedirs(os.path.dirname(os.path.abspath(output_path)) or ".", exist_ok=True)

    if max_workers is None:
        cpu_count = os.cpu_count() or 1
        max_workers = min(32, cpu_count * 5)

    def _evaluate_doi(doi: str) -> dict | None:
        """
        Worker function to call fair_evaluation for a single DOI.

        Returns:
            A dict ready to be written as NDJSON, or None if evaluation failed.
        """
        session = _get_thread_session()

        try:
            report = fair_evaluation(
                doi_or_url=doi,
                base_url=base_url,
                username=username,
                password=password,
                session=session,
            )
        except requests.HTTPError as e:
            print(f"  F-UJI HTTP error for {doi}: {e}")
            return None
        except requests.RequestException as e:
            print(f"  F-UJI request failed for {doi}: {e}")
            return None

        return {"doi": doi, **report}

    success_count = 0

    with open(output_path, "w", encoding="utf-8") as out_f:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_evaluate_doi, doi): doi for doi in target_norms}

            start_time = time.time()
            for idx, future in enumerate(as_completed(futures), start=1):
                doi = futures[future]
                try:
                    result = future.result()
                except Exception as e:  # noqa: BLE001
                    print(f"\n  Unexpected error for {doi}: {e}")
                    continue

                if result is None:
                    continue

                out_f.write(json.dumps(result, ensure_ascii=False) + "\n")
                success_count += 1

                # progress counter
                elapsed = time.time() - start_time
                print(
                    f"\r  Progress: {idx}/{total_dois} DOIs processed — {elapsed:.1f}s elapsed",
                    end="",
                    flush=True,
                )

    # completion message
    print()
    print(
        f" Finished FAIR evaluations. Wrote {success_count} records to {output_path}"
    )
    return success_count
