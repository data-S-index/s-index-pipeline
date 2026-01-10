# citations.py
from __future__ import annotations

import glob
import json
import math
import os
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote

import ijson
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from normalize import (
    _collect_dois_and_pub_dates_from_slimmed_file,
    _collect_ids_and_pub_dates_from_slimmed_file,
    _norm_dataset_id,
    _norm_date_iso,
    _norm_doi,
    _norm_doi_url,
    _to_datetime_utc,
    _years_between,
)

USER_AGENT_OA = "openalex-citations (mailto:bvhpatel@gmail.com)"


# ------------- Utility/helper functions ------------- #


def _dedupe_citations_by_link(citations: List[Dict]) -> List[Dict]:
    """
    Deduplicate citation objects by ``citation_link``.

    This function is used after we get citations for a given dataset from a given source
    (like MDC, Open Alex, or DataCite) to make sure we don't have duplicated citations
    for the dataset from that source (e.g. MDC seems to have duplicated citation records).

    We only deduplicate by "citation_link" because the input citations are for one dataset/doi

    Upstream normalization guarantees for the input:
      - If a citation has a date, it is already normalized using ``_norm_date_iso``.
      - If a citation has *no* date, the key ``"citation_date"`` is simply absent.

    This is how duplication is managed:
      - For a given ``citation_link``, if *none* of the duplicates have a date,
        all entries are treated as equivalent and the *first* occurrence is kept.
      - If exactly one entry has a date, that entry is preferred.
      - If multiple duplicates have dates, the entry with the *earliest*
        ``citation_date`` is kept.

    Args:
        citations:
            A list of citation dictionaries. Each must contain a
            ``"citation_link"`` key and may contain a normalized
            ``"citation_date"`` key, and other keys.

    Returns:
        A list of deduplicated citation dictionaries, preserving the order of
        first appearance of each unique ``citation_link``.
    """
    grouped: Dict[str, Dict] = {}
    order: List[str] = []

    for c in citations:
        link = c.get("citation_link")
        if not isinstance(link, str):
            continue

        # First occurrence of this link, just save the citation dict
        if link not in grouped:
            grouped[link] = c
            order.append(link)
            continue

        # If not first occurence compare with existing one
        existing = grouped[link]

        date_existing_str = existing.get("citation_date")
        date_new_str = c.get("citation_date")

        # Case 1: existing has no date, new has --> keep new
        if date_existing_str is None and date_new_str is not None:
            grouped[link] = c
            continue

        # Case 2: existing has date, new does not --> keep existing
        if date_existing_str is not None and date_new_str is None:
            continue

        # Case 3: both have no date --> keep existing
        if date_existing_str is None and date_new_str is None:
            continue

        # Case 4: both have normalized ISO dates --> keep new if it has earlier citation date
        if datetime.fromisoformat(date_new_str) < datetime.fromisoformat(
            date_existing_str
        ):
            grouped[link] = c

    return [grouped[i] for i in order]


# ------------- MDC citations ------------- #


def iter_all_mdc_json(folder_path, pattern="*.json"):
    """
    Stream MDC corpus JSON files from a folder.

    This function is used when we search for citations to our datasets in the MDC corpus.
    Each file matching `pattern` is opened and streamed with `ijson.items(..., 'item')`,
    yielding one object at a time without loading the whole file into memory.

    A typical usage is:
        for record in iter_all_mdc_json("path/to/mdc"):
            <do something with the citation record>

    Args:
        folder_path: Directory containing MDC corpus JSON files (each file is a JSON array).
        pattern: Glob pattern to select files (default: "*.json").

    Yields:
        Dict objects for each item in the MDC JSON arrays.
    """
    files = glob.glob(os.path.join(folder_path, pattern))
    for file in files:
        print(f"Streaming {file}")
        with open(file, "rb") as f:
            for obj in ijson.items(f, "item"):
                yield obj


def find_citations_mdc(
    target_id: str,
    dataset_pub_date: str,
    mdc_folder: str,
    mdc_pattern: str = "*.json",
) -> List[Dict[str, Any]]:
    """Find MDC citations for a single dataset identifier.

    This function streams MDC JSON files from `mdc_folder` using
    `iter_all_mdc_json()` and returns all citation records whose `dataset`
    field matches the given `target_id` after normalization with
    `_norm_dataset_id()`.

    Each returned citation includes:
      - dataset_id: the original `target_id` passed to this function
      - source: ["mdc"]
      - citation_link: DOI URL or raw publication URL
      - citation_date: ISO date string (if available)
      - citation_weight: numeric weight computed from dataset and citation dates

    Args:
        target_id: Dataset identifier to match (e.g., DOI, DOI URL, "EMD-2451",
            or a URL containing such an identifier).
        dataset_pub_date: ISO publication date of the dataset, already
            normalized with `_norm_date_iso()`, used for weighting citations.
        mdc_folder: Directory containing MDC corpus JSON files.
        mdc_pattern: Glob pattern to select MDC JSON files (default `"*.json"`).

    Returns:
        A list of citation dicts, de-duplicated by `citation_link`.
    """
    target_norm = _norm_dataset_id(target_id)
    if not target_norm:
        return []

    results: List[Dict[str, Any]] = []

    for record in iter_all_mdc_json(mdc_folder, pattern=mdc_pattern):
        ds = record.get("dataset")
        ds_norm = _norm_dataset_id(ds)
        if not ds_norm or ds_norm != target_norm:
            continue

        citation_link_raw = record.get("publication") or ""
        # Prefer canonical DOI URL when possible
        citation_link = _norm_doi_url(citation_link_raw) or citation_link_raw
        if not citation_link:
            continue

        citation_date_raw = record.get("publishedDate") or ""
        try:
            citation_date_iso = (
                _norm_date_iso(citation_date_raw) if citation_date_raw else ""
            )
        except ValueError:
            citation_date_iso = ""

        rec: Dict[str, Any] = {
            "dataset_id": target_id,  # original input identifier
            "source": ["mdc"],
            "citation_link": citation_link,
        }
        if citation_date_iso:
            rec["citation_date"] = citation_date_iso

        rec["citation_weight"] = _citation_weight(dataset_pub_date, citation_date_iso)

        results.append(rec)

    return _dedupe_citations_by_link(results)


def batch_find_citations_mdc(
    slim_folder: str,
    mdc_folder: str,
    output_path: str,
    mdc_pattern: str = "*.json",
    progress_interval: int = 100_000,
) -> int:
    """
    Batch-find MDC citations for all datasets in slim metadata files.

    Loads slimmed NDJSON metadata from ``slim_folder`` via
    ``_collect_ids_and_pub_dates_from_slimmed_file()``, obtains normalized dataset
    identifiers, and streams MDC JSON files in ``mdc_folder`` to find citation
    records whose ``dataset`` field matches any of those normalized identifiers
    (using ``_norm_dataset_id()``).

    For each (dataset, unique citation) pair, one NDJSON line is written to
    ``output_path`` with the same shape as ``find_citations_mdc()``:

        {
          "dataset_id": "<original identifier from slim metadata>",
          "source": ["mdc"],
          "citation_link": "<DOI URL or raw publication URL>",
          "citation_date": "<ISO-8601 date>",    # omitted if unavailable
          "citation_weight": <float>
        }

    Args:
        slim_folder: Directory containing slim ``.ndjson`` metadata files.
            ``_collect_ids_and_pub_dates_from_slimmed_file()`` scans this recursively.
        mdc_folder: Directory containing MDC corpus JSON files. Each file is
            expected to be a JSON array of MDC records.
        output_path: Path to the output NDJSON file. One citation record is
            written per line.
        mdc_pattern: Glob pattern used to select MDC JSON files in
            ``mdc_folder`` (default ``"*.json"``).
        progress_interval: Print streaming progress every this many MDC records (default 100k).

    Returns:
        The number of NDJSON citation records written to ``output_path``.
    """
    # ---- Step 1: Collect normalized IDs and metadata from slim records ----
    norm_to_dataset_id, dataset_info = _collect_ids_and_pub_dates_from_slimmed_file(
        slim_folder
    )

    total_datasets = len(dataset_info)
    total_norm_ids = len(norm_to_dataset_id)

    print(
        f"[Slim Scan] Loaded {total_datasets} datasets, {total_norm_ids} normalized IDs."
    )
    if not norm_to_dataset_id:
        print("[Slim Scan] No identifiers found — exiting.")
        return 0

    target_norm_ids = set(norm_to_dataset_id.keys())

    # Map normalized ID -> dataset publication date (ISO)
    dataset_pub_dates: Dict[str, str] = {}
    for norm_id, dataset_id in norm_to_dataset_id.items():
        info = dataset_info.get(dataset_id, {})
        pub_date = info.get("pub_date", "") or ""
        dataset_pub_dates[norm_id] = pub_date

    # ---- Step 2: Stream MDC corpus once and bucket citations by normalized ID ----
    citations_by_norm_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    print("Streaming MDC corpus and collecting citations ...")
    processed_records = 0  # NEW
    found_citations = 0
    for record in iter_all_mdc_json(mdc_folder, pattern=mdc_pattern):
        processed_records += 1
        if processed_records % progress_interval == 0:
            print(
                f"\r[MDC Scan] Processed {processed_records:,} records, "
                f"found {found_citations:,} citations.",
                end="",
                flush=True,
            )

        ds_link = record.get("dataset")
        if not isinstance(ds_link, str):
            continue

        ds_norm = _norm_dataset_id(ds_link)
        if not ds_norm or ds_norm not in target_norm_ids:
            continue

        citation_link_raw = record.get("publication") or ""
        citation_link = _norm_doi_url(citation_link_raw) or citation_link_raw
        if not citation_link:
            continue

        citation_date_raw = record.get("publishedDate") or ""
        try:
            citation_date = (
                _norm_date_iso(citation_date_raw) if citation_date_raw else ""
            )
        except ValueError:
            citation_date = ""

        pub_date = dataset_pub_dates.get(ds_norm, "")

        rec_out: Dict[str, Any] = {
            "source": ["mdc"],
            "citation_link": citation_link,
        }
        if citation_date:
            rec_out["citation_date"] = citation_date
        rec_out["citation_weight"] = _citation_weight(pub_date, citation_date)

        citations_by_norm_id[ds_norm].append(rec_out)
        found_citations += 1

    # final count before removing duplicates
    print(
        f"\r[MDC Scan] Finished streaming MDC corpus. "
        f"Processed {processed_records:,} records, "
        f"found {found_citations:,} citations (before removing possible duplicates)."
    )

    # ---- Step 3: De-duplicate per identifier and write one line per citation ----
    os.makedirs(os.path.dirname(os.path.abspath(output_path)) or ".", exist_ok=True)

    written = 0
    with open(output_path, "w", encoding="utf-8") as out_f:
        for norm_id in target_norm_ids:
            raw_list = citations_by_norm_id.get(norm_id, [])
            mdc_citations = _dedupe_citations_by_link(raw_list)

            dataset_id = norm_to_dataset_id.get(norm_id)
            if not dataset_id:
                continue

            for cit in mdc_citations:
                out_obj: Dict[str, Any] = {
                    "dataset_id": dataset_id,
                    **cit,  # source, citation_link, citation_weight, optional citation_date
                }
                out_f.write(json.dumps(out_obj, ensure_ascii=False) + "\n")
                written += 1

    print(f" Done. Wrote {written} citation records to {output_path}")
    return written


# ------------- Open Alex citations ------------- #


def make_oa_session(
    total_retries: int = 6,
    backoff: float = 1.5,
    api_key: Optional[str] = None,
) -> requests.Session:
    """
    Create a requests.Session tuned for OpenAlex with retry/backoff.

    Retries common transient failures and 429/5xx responses, and reuses
    connections across many OpenAlex calls for better performance.

    Args:
        total_retries: Max retry attempts for connection/read/status errors.
        backoff: Exponential backoff factor in seconds.
        api_key: Optional OpenAlex API key. If provided, it will be sent on
                 every request as the `api_key` query parameter.

    Returns:
        A configured `requests.Session` instance.
    """
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT_OA})

    if api_key:
        # This ensures every `session.get(...)` automatically includes ?api_key=...
        s.params = getattr(s, "params", {})
        s.params["api_key"] = api_key

    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET"],
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def get_all_citing_works_oa(
    openalex_id,
    email=None,
    session=None,
    max_pages: int = 10,
):
    """
    Fetch all OpenAlex works that cite the given OpenAlex work ID.

    We first find the open alex id of a dataset based on its DOI
    and then use this function to get all the resources citing that id

    Uses the cursor-based pagination API:
      GET /works?filter=cites:{openalex_id}&per-page=200&cursor=*

    Args:
        openalex_id: The OpenAlex work ID (e.g., "W1234567890").
        email: An optional contact email; passed via `mailto` to be polite to the API.
        session: shared `requests.Session`

    Returns:
        A list of OpenAlex work dicts (raw API objects) that cite the target work.

    Raises:
        requests.HTTPError: If the OpenAlex API returns an HTTP error other than
            pagination completion.
    """
    results: List[dict] = []
    cursor = "*"
    page_count = 0

    while True:
        page_start = time.time()
        if page_count >= max_pages:
            # Optional: warn so you can detect truncation later
            print(
                f"WARNING: truncating citing works for OpenAlex ID {openalex_id} "
                f"after {max_pages} pages (~{max_pages * 200} citations)."
            )
            break

        url = f"https://api.openalex.org/works?filter=cites:{openalex_id}&per-page=200&cursor={cursor}"
        if email:
            url += f"&mailto={email}"

        try:
            r = session.get(url, timeout=60)
            _check_oa_rate_limit_json(r)
            r.raise_for_status()

        except OpenAlexRateLimitExceeded as e:
            print(
                f"[RATE-LIMIT] Hit the daily limit while fetching citing works "
                f"for OpenAlex ID {openalex_id}: {e}",
                flush=True,
            )
            raise
        except Exception as e:
            print(f"[PAGE-ERROR] {openalex_id} page {page_count}: {e}", flush=True)
            raise

        elapsed = time.time() - page_start
        if elapsed > 10:  # e.g., any page that takes >10 seconds
            print(
                f"[SLOW-PAGE] {openalex_id} page {page_count} took {elapsed:.1f}s",
                flush=True,
            )

        data = r.json()
        results.extend(data["results"])

        cursor = data["meta"]["next_cursor"]
        page_count += 1

        if not cursor:
            break

    return results


def find_citations_oa(
    doi: str,
    dataset_pub_date: str,
    email: Optional[str] = None,
    session: Optional[requests.Session] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, str]]:
    """
    Find citations to a given dataset DOI (identifier or DOI URL) from OpenAlex.

    Steps:
      1) Resolve the dataset to an OpenAlex work using its DOI/DOI-URL.
      2) Fetch all works that cite it via cursor pagination.
      3) Return normalized citation objects suitable for downstream merging.

    Normalization:
      - Input DOI is matched using the canonical DOI identifier (`_norm_doi`).
        If it input is detected to not be a DOI, the function returns an empty list
      - `citation_link`: canonical DOI URL if the citing work has a DOI;
        otherwise the OpenAlex work URL.
      - `citation_date`: normalized ISO-8601 string via `_norm_date_iso()`
        when available.

    Args:
        doi:
            The dataset DOI (e.g., "10.1234/abcd") or DOI URL
            (e.g., "https://doi.org/10.1234/abcd").
        dataset_pub_date: Str of the ISO publication date of the dataset already normalized
                          with `_norm_date_iso()`, used when computing `citation_weight`.
        email:
            Optional contact email for OpenAlex (`mailto` query parameter).
        session:
            Optional shared `requests.Session`. If None, a new retry-aware
            OpenAlex session is created.
        api_key:
            Optional OpenAlex API key. Only used if `session` is None; in that
            case, `make_oa_session(api_key=api_key)` is called.

    Returns:
        A list of dicts:
        [
          {
            "dataset_id": <input_doi>,
            "source": ["openalex"],
            "citation_link": <DOI URL or OpenAlex work URL>,
            "citation_date": <ISO-8601 date>  (key omitted if no date)
            "citation_weight": <e.g. 1.0 or 1.24>
          },
          ...
        ]
        Duplicates (by `citation_link`) are removed while preserving order.

    Raises:
        requests.HTTPError: For non-404 errors when resolving the dataset work.
    """
    target_doi = _norm_doi(doi)
    if not target_doi:
        return []

    s = session or make_oa_session(api_key=api_key)

    # OpenAlex supports /works/{doi-url} (just doi identifier does not work)
    work_lookup = f"https://api.openalex.org/works/{_norm_doi_url(target_doi)}"
    if email:
        sep = "&" if "?" in work_lookup else "?"
        work_lookup += f"{sep}mailto={email}"

    try:
        r = s.get(work_lookup, timeout=60)
        _check_oa_rate_limit_json(r)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if r is not None and r.status_code == 404:
            return []
        raise e

    dataset = r.json()
    openalex_id_url = dataset.get("id")  # e.g., "https://openalex.org/W123..."
    cited_by_count = dataset.get("cited_by_count", 0)
    if not cited_by_count:
        # No citing works known to OpenAlex → skip pagination
        return []
    if not isinstance(openalex_id_url, str) or "/" not in openalex_id_url:
        return []

    openalex_id = openalex_id_url.rsplit("/", 1)[-1]

    # Get all citing works
    citing_records = get_all_citing_works_oa(openalex_id, email=email, session=s)

    results: List[Dict[str, str]] = []
    for c in citing_records:
        doi_raw = c.get("doi") or ""
        citation_link = _norm_doi_url(doi_raw) or c.get("id", "")

        citation_date_raw = c.get("publication_date") or ""
        try:
            citation_date = (
                _norm_date_iso(citation_date_raw) if citation_date_raw else ""
            )
        except ValueError:
            citation_date = ""

        if citation_link:
            rec = {
                "dataset_id": doi,
                "source": ["openalex"],
                "citation_link": citation_link,
            }
            if citation_date:
                rec["citation_date"] = citation_date
            rec["citation_weight"] = _citation_weight(dataset_pub_date, citation_date)
            results.append(rec)

    return _dedupe_citations_by_link(results)


def batch_find_citations_oa(
    slim_folder: str,
    output_path: str,
    email: Optional[str] = None,
    api_key: Optional[str] = None,
) -> int:
    """
    Run OpenAlex citation searches for all DOIs found in slimmed DataCite NDJSON files.

    Each input NDJSON line in `slim_folder` is expected to be a slimmed
    DataCite record that includes at least a top-level `"doi"` key. For each
    unique DOI, this function calls `find_citations_oa()` and writes one
    NDJSON line per citation with the form:

        {
          "dataset_od": "<doi-as-found-in-slimmed-record>",
          "source": ["openalex"],
          "citation_link": "<DOI URL or OpenAlex work URL>",
          "citation_date": "<ISO-8601>"   # only if available
          "citation_weight": <e.g., 1.0 or 1.23>
        }

    Args:
        slim_folder:
            Directory containing `.ndjson` files with slimmed DataCite records
            (one JSON object per line, including a top-level `"doi"` field).
        output_path:
            Path to the output NDJSON file (e.g. `citations-openalex.ndjson`).
        email:
            Optional contact email passed to OpenAlex as the `mailto` parameter.
        api_key:
            Optional OpenAlex API key to authenticate requests and raise rate
            limits. Passed to `make_oa_session(api_key=api_key)`

    Returns:
        The number of citation records written to `output_path`
        (i.e., number of lines in the output file).

    Notes:
        - Uses a single retry-aware `requests.Session` for all OpenAlex calls.
        - DOIs are de-duplicated based on the raw `"doi"` string found in the
          slimmed records (each DOI queried at most once).
        - `find_citations_oa()` is responsible for de-duplicating citations per
          DOI based on `citation_link`.
    """
    # Step 1: collect unique DOIs from slimmed NDJSON files
    target_dois, dataset_pub_dates = _collect_dois_and_pub_dates_from_slimmed_file(
        slim_folder
    )
    if not target_dois:
        return 0

    print()

    # Step 2: get citing works
    os.makedirs(os.path.dirname(os.path.abspath(output_path)) or ".", exist_ok=True)

    session = make_oa_session(api_key=api_key)
    written = 0

    with open(output_path, "w", encoding="utf-8") as out_f:
        for idx, doi in enumerate(target_dois, start=1):
            print(
                f"[{idx}/{len(target_dois)}] Finding OpenAlex citations for {doi} ..."
            )

            try:
                citations = find_citations_oa(
                    doi=doi,
                    dataset_pub_date=dataset_pub_dates.get(doi, ""),
                    email=email,
                    session=session,
                    api_key=None,  # session already has api_key
                )
            except requests.HTTPError as e:
                print(f"   OpenAlex HTTP error for {doi}: {e}")
                continue
            except requests.RequestException as e:
                print(f"   OpenAlex request failed for {doi}: {e}")
                continue

            # Save one line per citations
            for cit in citations:
                out_f.write(json.dumps(cit, ensure_ascii=False) + "\n")
                written += 1

    print(
        f" Finished OpenAlex citation search. Wrote {written} citation records to {output_path}"
    )
    return written


# --- FAST OA


class OpenAlexRateLimitExceeded(Exception):
    def __init__(self, retry_after=None):
        self.retry_after = retry_after
        if retry_after is not None:
            msg = (
                f"OpenAlex rate limit exceeded. Retry after ~{retry_after:.0f} seconds."
            )
        else:
            msg = "OpenAlex rate limit exceeded."
        super().__init__(msg)


def _check_oa_rate_limit_json(r):
    """
    Check an OpenAlex JSON response for the 'Rate limit exceeded' message.

    If detected, raise OpenAlexRateLimitExceeded with the retryAfter time.
    """
    try:
        data = r.json()
    except ValueError:
        return  # Not JSON → nothing to check.

    # Detect the exact JSON you saw:
    # {"error":"Rate limit exceeded", "retryAfter":7367, ...}
    if isinstance(data, dict) and data.get("error") == "Rate limit exceeded":
        retry_after_val = data.get("retryAfter")
        retry_after = (
            float(retry_after_val)
            if isinstance(retry_after_val, (int, float))
            else None
        )
        raise OpenAlexRateLimitExceeded(retry_after)


def _print_progress(msg: str) -> None:
    # Clear previous line (160 spaces usually enough for long messages)
    sys.stdout.write("\r" + " " * 160 + "\r")
    sys.stdout.write(msg)
    sys.stdout.flush()


def build_chunked_oa_worklists(
    slim_folder: str,
    worklist_folder: str,
    chunk_size: int = 100_000,
) -> int:
    """
    Build multiple ordered DOI worklists (oldest datasets first) from slimmed NDJSON files.

    This function is needed for the fast oa citations, which go through the generated
    files to perform the query.

    Uses `_collect_dois_and_pub_dates_from_slimmed_file(slim_folder)` to discover
    all unique DOIs and their publication dates, then sorts them by pub date
    ascending (oldest first) and writes NDJSON worklists of the form:

      worklist_folder/openalex-dois-000.ndjson
      worklist_folder/openalex-dois-001.ndjson
      ...

    Each file contains up to `chunk_size` lines, with each line:

        {"doi": "<doi>", "dataset_pub_date": "<ISO-date-or-empty>"}

    Args:
        slim_folder:
            Directory containing slimmed DataCite NDJSON files.
        worklist_folder:
            Folder where chunked worklists will be written/overwritten.
        chunk_size:
            Maximum number of DOIs per worklist file.

    Returns:
        Total number of DOIs written across all worklist files.
    """
    os.makedirs(worklist_folder, exist_ok=True)

    target_dois, dataset_pub_dates = _collect_dois_and_pub_dates_from_slimmed_file(
        slim_folder
    )

    # Sort DOIs by publication date (oldest first).
    # Missing/empty dates are sent to the end
    def _sort_key(doi: str) -> str:
        return dataset_pub_dates.get(doi) or "9999-12-31T23:59:59"

    ordered_dois = sorted(target_dois, key=_sort_key)
    total = len(ordered_dois)
    if total == 0:
        print("No DOIs found to build worklists.")
        return 0

    chunk_index = 0
    written = 0
    f = None

    try:
        for idx, doi in enumerate(ordered_dois):
            local_idx = idx % chunk_size
            if local_idx == 0:
                # Close previous chunk file if open
                if f is not None:
                    f.close()
                # Open a new chunk file
                filename = f"openalex-dois-{chunk_index:03d}.ndjson"
                path = os.path.join(worklist_folder, filename)
                f = open(path, "w", encoding="utf-8")
                print(
                    f"\rCreating worklist chunk {chunk_index:03d} at {path}",
                    end="",
                    flush=True,
                )
                chunk_index += 1

            rec = {
                "doi": doi,
                "dataset_pub_date": dataset_pub_dates.get(doi, ""),
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            written += 1
    finally:
        if f is not None:
            f.close()

    print(
        f"Built {chunk_index} worklist chunks with {written} DOIs total "
        f"in folder {worklist_folder}"
    )
    return written


def _load_run_state(
    state_path: str,
    worklist_folder: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Load or initialize run state for all worklist chunks in a folder.

    This function is used to keep track of in the fast batch oa function.
    The state is a dict keyed by worklist filename, e.g.:

        {
          "openalex-dois-000.ndjson": {
            "status": "completed" | "in_progress" | "not_started",
            "last_processed_index": int  # -1 if none yet
          },
          ...
        }

    Args:
        state_path:
            Path to run_state.json.
        worklist_folder:
            Folder that contains worklist chunk files.

    Returns:
        A run_state dict.
    """
    # Discover worklist files
    files = sorted(f for f in os.listdir(worklist_folder) if f.endswith(".ndjson"))

    if os.path.exists(state_path):
        with open(state_path, "r", encoding="utf-8") as sf:
            state = json.load(sf)
    else:
        state = {}

    # Ensure every worklist file has an entry
    for fname in files:
        if fname not in state:
            state[fname] = {
                "status": "not_started",
                "last_processed_index": -1,
            }

    return state


def _save_run_state(state_path: str, state: Dict[str, Dict[str, Any]]) -> None:
    """Persist the run_state dict to disk."""
    tmp_path = state_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as sf:
        json.dump(state, sf, ensure_ascii=False, indent=2)
    os.replace(tmp_path, state_path)


class _SimpleRateLimiter:
    """
    Very simple token bucket for limiting units per second.

    A unit is one DOI processed. Since each DOI may involve
    multiple HTTP requests to OpenAlex, this is only an approximate
    request limiter, but it helps keep throughput under control.
    """

    def __init__(self, units_per_second: float):
        self.units_per_second = float(units_per_second)
        self.allowance = self.units_per_second
        self.last_check = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        """Block until we are allowed to process one more DOI."""
        while True:
            with self.lock:
                current = time.monotonic()
                elapsed = current - self.last_check
                self.last_check = current

                self.allowance += elapsed * self.units_per_second
                if self.allowance > self.units_per_second:
                    self.allowance = self.units_per_second

                if self.allowance >= 1.0:
                    self.allowance -= 1.0
                    return

            time.sleep(0.01)


def _worker_find_citations_oa(
    doi: str,
    dataset_pub_date: str,
    email: Optional[str],
    session,
    rate_limiter: Optional[_SimpleRateLimiter] = None,
) -> List[Dict[str, Any]]:
    """
    Worker wrapper: optionally rate-limit, then call `find_citations_oa`.

    Returns:
        List of normalized citation dicts (possibly empty).
    """
    start = time.time()
    if rate_limiter is not None:
        rate_limiter.acquire()

    try:
        citations = find_citations_oa(
            doi=doi,
            dataset_pub_date=dataset_pub_date,
            email=email,
            session=session,
            api_key=None,  # session already carries the key if any
        )
        return citations
    finally:
        elapsed = time.time() - start
        if elapsed > 60:  # or any threshold (e.g., 30s)
            print(f"[SLOW-DOI] {doi} took {elapsed:.1f}s", flush=True)


def _process_oa_worklist_chunk(
    worklist_path: str,
    worklist_fname: str,
    output_dir: str,
    email: Optional[str],
    api_key: Optional[str],
    run_state: Dict[str, Dict[str, Any]],
    state_path: str,
    concurrency: int = 8,
    dois_per_second: float = 2.0,
    checkpoint_interval: int = 100,
) -> Dict[str, int]:
    """
    Process a single worklist chunk file.

    Respects run_state[worklist_fname]["last_processed_index"] to resume
    from where it left off within this file.

    Writes:
      - citations-<basename>.ndjson
      - failed-<basename>.ndjson

    Updates run_state for this file and persists it periodically.

    Returns:
        Summary dict for this chunk:
            {
              "chunk_file": worklist_fname,
              "total_dois_in_chunk": ...,
              "new_dois_processed": ...,
              "success_dois": ...,
              "failed_dois": ...,
              "total_citations": ...,
            }
    """
    state_entry = run_state[worklist_fname]
    last_idx = state_entry.get("last_processed_index", -1)

    # Load DOIs from worklist, starting after last_idx
    doi_batch: List[Dict[str, Any]] = []
    total_in_chunk = 0

    with open(worklist_path, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            total_in_chunk += 1
            if idx <= last_idx:
                continue
            obj = json.loads(line)
            doi_batch.append(
                {
                    "local_index": idx,  # index within this file
                    "doi": obj["doi"],
                    "dataset_pub_date": obj.get("dataset_pub_date", ""),
                }
            )

    if not doi_batch:
        print(
            f"Chunk {worklist_fname}: nothing to do "
            f"(last_processed_index={last_idx}, total={total_in_chunk})"
        )
        # Mark completed if not already
        state_entry["status"] = "completed"
        state_entry["last_processed_index"] = total_in_chunk - 1
        _save_run_state(state_path, run_state)
        return {
            "chunk_file": worklist_fname,
            "total_dois_in_chunk": total_in_chunk,
            "new_dois_processed": 0,
            "success_dois": 0,
            "failed_dois": 0,
            "total_citations": 0,
        }

    new_dois_to_process = len(doi_batch)
    print(
        f"Chunk {worklist_fname}: resuming from index {last_idx + 1}, "
        f"processing {new_dois_to_process} DOIs "
        f"(total in chunk: {total_in_chunk})"
    )

    # Timing / throughput tracking
    chunk_start_time = time.time()
    processed_since_start = 0  # DOIs completed in this run for this chunk

    # Prepare output files for this chunk
    base = os.path.splitext(os.path.basename(worklist_fname))[
        0
    ]  # e.g. openalex-dois-003
    citations_path = os.path.join(output_dir, f"citations-{base}.ndjson")
    failures_path = os.path.join(output_dir, f"failed-{base}.ndjson")

    session = make_oa_session(api_key=api_key)
    rate_limiter = _SimpleRateLimiter(dois_per_second) if dois_per_second > 0 else None

    write_lock = threading.Lock()

    success_count = 0
    failed_count = 0
    total_citations = 0
    last_processed_index = last_idx

    # Mark as in_progress before starting
    state_entry["status"] = "in_progress"
    _save_run_state(state_path, run_state)

    with (
        open(citations_path, "a", encoding="utf-8") as cit_f,
        open(failures_path, "a", encoding="utf-8") as fail_f,
        ThreadPoolExecutor(max_workers=concurrency) as executor,
    ):
        futures = {}
        for item in doi_batch:
            local_index = item["local_index"]
            doi = item["doi"]
            pub_date = item["dataset_pub_date"]
            fut = executor.submit(
                _worker_find_citations_oa,
                doi=doi,
                dataset_pub_date=pub_date,
                email=email,
                session=session,
                rate_limiter=rate_limiter,
            )
            futures[fut] = (doi, pub_date, local_index)

        print(
            f"Chunk {worklist_fname}: submitted {len(futures)} futures "
            f"for {len(doi_batch)} DOIs",
            flush=True,
        )
        try:
            for completed_idx, fut in enumerate(
                as_completed(futures, timeout=600), start=1
            ):
                doi, pub_date, local_index = futures[fut]
                try:
                    citations = fut.result()

                except OpenAlexRateLimitExceeded as e:
                    print(
                        f"[RATE-LIMIT] Hit the daily OpenAlex limit in chunk {worklist_fname} "
                        f"for DOI {doi}: {e}",
                        flush=True,
                    )
                    raise

                except Exception as e:
                    failed_count += 1
                    err_rec = {"doi": doi, "error": str(e)}
                    with write_lock:
                        fail_f.write(json.dumps(err_rec, ensure_ascii=False) + "\n")
                    print(
                        f"[{completed_idx}/{new_dois_to_process}] "
                        f"DOI {doi} FAILED in chunk {worklist_fname}: {e}"
                    )
                else:
                    success_count += 1
                    num_cit = len(citations)
                    total_citations += num_cit
                    if num_cit:
                        with write_lock:
                            for rec in citations:
                                cit_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

                # Update last_processed_index (within this file)
                last_processed_index = max(last_processed_index, local_index)
                state_entry["last_processed_index"] = last_processed_index

                # Update timing / throughput
                processed_since_start += 1
                elapsed = time.time() - chunk_start_time
                doi_per_sec = processed_since_start / elapsed if elapsed > 0 else 0.0

                # Periodically persist run_state
                if checkpoint_interval > 0 and (
                    completed_idx % checkpoint_interval == 0
                    or completed_idx == new_dois_to_process
                ):
                    _save_run_state(state_path, run_state)

                if completed_idx % 100 == 0 or completed_idx == new_dois_to_process:
                    _print_progress(
                        f"Chunk {worklist_fname}: "
                        f"[{completed_idx}/{new_dois_to_process}] "
                        f"success={success_count}, failed={failed_count}, "
                        f"total_citations={total_citations}, "
                        f"last_processed_index={last_processed_index}, "
                        f"elapsed={elapsed:7.1f}s, "
                        f"rate={doi_per_sec:6.2f} doi/s"
                    )
        except FuturesTimeoutError:
            # No future completed in 600s → likely stuck HTTP / global outage.
            print(
                f"[WATCHDOG] No futures completed for 600s in chunk {worklist_fname}. "
                f"{len(futures)} DOIs still in flight.",
                flush=True,
            )
            raise
    # Mark chunk as completed
    state_entry["status"] = "completed"
    state_entry["last_processed_index"] = total_in_chunk - 1
    _save_run_state(state_path, run_state)

    total_elapsed = time.time() - chunk_start_time
    avg_rate = new_dois_to_process / total_elapsed if total_elapsed > 0 else 0.0

    summary = {
        "chunk_file": worklist_fname,
        "total_dois_in_chunk": total_in_chunk,
        "new_dois_processed": new_dois_to_process,
        "success_dois": success_count,
        "failed_dois": failed_count,
        "total_citations": total_citations,
    }

    print(
        f"Finished chunk {worklist_fname}:\n"
        f"  total_dois_in_chunk={total_in_chunk}\n"
        f"  new_dois_processed={new_dois_to_process}\n"
        f"  success_dois={success_count}\n"
        f"  failed_dois={failed_count}\n"
        f"  total_citations={total_citations}\n"
        f"  elapsed_time_sec={total_elapsed:.1f}\n"
        f"  avg_doi_per_sec={avg_rate:.2f}\n"
        f"  avg_doi_per_hour={avg_rate * 3600:.0f}\n"
        f"  citations file: {citations_path}\n"
        f"  failed file:    {failures_path}"
    )

    print()

    return summary


def fast_batch_find_citations_oa(
    worklist_folder: str,
    output_dir: str,
    email: Optional[str] = None,
    api_key: Optional[str] = None,
    concurrency: int = 8,
    dois_per_second: float = 2.0,
    checkpoint_interval: int = 100,
    state_filename: str = "run_state.json",
) -> Dict[str, int]:
    """
    Process all OpenAlex worklist chunks in a folder, with automatic resume.

    This function expects that `worklist_folder` contains one or more NDJSON
    files like:

        openalex-dois-000.ndjson
        openalex-dois-001.ndjson
        ...

    Each line in each file is:

        {"doi": "<doi>", "dataset_pub_date": "<ISO-or-empty>"}

    It maintains a run_state.json in `output_dir` that tracks, for each
    worklist file:

        - status: "not_started", "in_progress", "completed"
        - last_processed_index: last DOI index processed within that file

    On each run, it:

      - Loads or initializes run_state.
      - For each worklist file in sorted order:
          * If status == "completed": skip.
          * Else: call `_process_oa_worklist_chunk`, which resumes inside
            that file based on `last_processed_index`.

    Can safely re-run this function after any interruption; it will
    pick up from where it left off.

    Args:
        worklist_folder:
            Folder containing chunked worklist NDJSON files.
        output_dir:
            Folder where citations/failed shards and run_state.json will live.
        email:
            Optional contact email for OpenAlex (mailto).
        api_key:
            Optional OpenAlex API key.
        concurrency:
            Number of worker threads per chunk.
        dois_per_second:
            Approximate maximum number of DOIs to start per second.
        checkpoint_interval:
            How many completed DOIs (per chunk) between run_state saves.
        state_filename:
            Name of the run state JSON file under output_dir.

    Returns:
        A global summary dict across all chunks:
            {
              "total_dois_all_chunks": ...,
              "total_new_dois_processed": ...,
              "total_success_dois": ...,
              "total_failed_dois": ...,
              "total_citations": ...,
            }
    """
    os.makedirs(output_dir, exist_ok=True)
    state_path = os.path.join(output_dir, state_filename)

    run_state = _load_run_state(state_path, worklist_folder)

    # Ensure we process chunks in deterministic order
    worklist_files = sorted(
        f for f in os.listdir(worklist_folder) if f.endswith(".ndjson")
    )

    total_dois_all_chunks = 0
    total_new_dois_processed = 0
    total_success_dois = 0
    total_failed_dois = 0
    total_citations = 0

    for fname in worklist_files:
        worklist_path = os.path.join(worklist_folder, fname)
        state_entry = run_state[fname]
        status = state_entry.get("status", "not_started")
        last_idx = state_entry.get("last_processed_index", -1)

        # Count total DOIs in chunk (only needed for final global summary)
        with open(worklist_path, "r", encoding="utf-8") as f:
            chunk_total = sum(1 for _ in f)
        total_dois_all_chunks += chunk_total

        if status == "completed":
            print(
                f"Skipping chunk {fname}: status=completed, "
                f"last_processed_index={last_idx}, total={chunk_total}"
            )
            continue

        # Process or resume this chunk
        chunk_summary = _process_oa_worklist_chunk(
            worklist_path=worklist_path,
            worklist_fname=fname,
            output_dir=output_dir,
            email=email,
            api_key=api_key,
            run_state=run_state,
            state_path=state_path,
            concurrency=concurrency,
            dois_per_second=dois_per_second,
            checkpoint_interval=checkpoint_interval,
        )

        total_new_dois_processed += chunk_summary["new_dois_processed"]
        total_success_dois += chunk_summary["success_dois"]
        total_failed_dois += chunk_summary["failed_dois"]
        total_citations += chunk_summary["total_citations"]

    global_summary = {
        "total_dois_all_chunks": total_dois_all_chunks,
        "total_new_dois_processed": total_new_dois_processed,
        "total_success_dois": total_success_dois,
        "total_failed_dois": total_failed_dois,
        "total_citations": total_citations,
    }

    print(
        "\n=== Global OpenAlex citation harvesting summary ===\n"
        f"  total_dois_all_chunks:   {total_dois_all_chunks}\n"
        f"  total_new_dois_processed:{total_new_dois_processed}\n"
        f"  total_success_dois:      {total_success_dois}\n"
        f"  total_failed_dois:       {total_failed_dois}\n"
        f"  total_citations:         {total_citations}\n"
        f"  run_state file:          {state_path}\n"
    )

    return global_summary


def run_fast_batch_with_rate_limit_retries(
    worklist_folder: str,
    output_dir: str,
    email: Optional[str],
    api_key: Optional[str],
    concurrency: int = 8,
    dois_per_second: float = 2.0,
    checkpoint_interval: int = 100,
    buffer_seconds: float = 60.0,
) -> dict:
    """
    Run fast_batch_find_citations_oa with auto handle of rate limits.

    It automatically handle OpenAlex daily rate limit by sleeping until
    retryAfter and then retrying.

    Returns the final global summary when all worklist chunks are done.
    """
    attempt = 1
    while True:
        print(f"\n==== OpenAlex batch attempt {attempt} ====\n", flush=True)
        try:
            summary = fast_batch_find_citations_oa(
                worklist_folder=worklist_folder,
                output_dir=output_dir,
                email=email,
                api_key=api_key,
                concurrency=concurrency,
                dois_per_second=dois_per_second,
                checkpoint_interval=checkpoint_interval,
            )

            print("Batch completed without hitting the daily rate limit.", flush=True)
            return summary

        except OpenAlexRateLimitExceeded as e:
            # Use retry_after if provided; otherwise fall back to 2 hours
            retry_after = e.retry_after if e.retry_after is not None else 7200.0
            wait_for = retry_after + buffer_seconds

            print(
                f"[RATE-LIMIT] OpenAlex daily limit reached. "
                f"RetryAfter={retry_after:.0f}s. "
                f"Waiting an extra buffer of {buffer_seconds:.0f}s "
                f"→ total wait ~{wait_for:.0f}s "
                f"({wait_for / 3600:.2f} hours) before retrying.",
                flush=True,
            )

            # Live countdown every 100s
            remaining = wait_for
            step = 100.0
            while remaining > 0:
                this_sleep = min(step, remaining)
                print(
                    f"[RATE-LIMIT] Sleeping {this_sleep:.0f}s "
                    f"(~{remaining:.0f}s remaining, "
                    f"next retry at {time.ctime(time.time() + remaining)}).",
                    flush=True,
                )
                time.sleep(this_sleep)
                remaining -= this_sleep

            attempt += 1
            print("\n[RATE-LIMIT] Wait over. Retrying batch now...\n", flush=True)
            # loop continues, calls fast_batch_find_citations_oa again


# ------------- Datacite ------------- #


def _as_iso_from_dateparts(parts) -> str:
    """
    Convert a Crossref-style date-parts array to an ISO-8601 string.

    Crossref formats date parts as: [[YYYY, M, D]] or [[YYYY, M]] or [[YYYY]].

    Args:
        parts: Crossref 'date-parts' value (e.g., [[2024, 10, 31]]).

    Returns:
        ISO-8601 string like "2024-10-31T00:00:00" if possible, else "".
    """
    try:
        if not parts or not parts[0]:
            return ""
        nums = parts[0]
        if len(nums) >= 3:
            return _norm_date_iso(f"{nums[0]:04d}-{nums[1]:02d}-{nums[2]:02d}")
        if len(nums) == 2:
            return _norm_date_iso(f"{nums[0]:04d}-{nums[1]:02d}-01")
        if len(nums) == 1:
            return _norm_date_iso(f"{nums[0]:04d}-01-01")
    except Exception:
        pass
    return ""


def _fetch_openalex_pubdate(
    doi_or_doi_url: str,
    email: Optional[str] = None,
    session: Optional[requests.Session] = None,
    timeout: int = 30,
) -> str:
    """
    Fetch a publication date for a DOI from the OpenAlex API and normalize it.

    This function queries the OpenAlex Works API using the DOI as an external ID
    (e.g. GET /works/https://doi.org/<doi>) and returns a canonical ISO-8601
    date string using `_norm_date_iso`.

    Preference is given to the `publication_date` field; if that is not
    available, it falls back to `publication_year`.

    Args:
        doi_or_doi_url:
            DOI identifier or DOI URL.
        email:
            Optional contact email, sent to OpenAlex via the `mailto` parameter
            (recommended for heavy / scripted use).
        session:
            Optional shared `requests.Session`. If not provided, a fresh
            OpenAlex-tuned session is created via `make_oa_session()`.
        timeout:
            HTTP timeout in seconds.

    Returns:
        ISO-8601 string (normalized) if available, else "".
    """
    doi = _norm_doi(doi_or_doi_url)
    if not doi:
        return ""

    doi_url = _norm_doi_url(doi)

    s = session or make_oa_session()
    try:
        work_id = quote(doi_url, safe=":/")
        url = f"https://api.openalex.org/works/{work_id}"

        params = {}
        if email:
            params["mailto"] = email

        r = s.get(url, params=params, timeout=timeout)
        if r.status_code != 200:
            return ""

        data = r.json() or {}

        # 1) Try full publication_date first
        pub_date = data.get("publication_date") or ""
        if pub_date:
            try:
                return _norm_date_iso(pub_date)
            except Exception:
                # If for some reason this is malformed, fall back to year
                pass

        # 2) Fallback: publication_year -> year-only ISO
        year = data.get("publication_year")
        if year:
            try:
                return _norm_date_iso(str(year))
            except Exception:
                return ""
    except Exception:
        pass

    return ""


def _fetch_datacite_pubdate(doi_or_doi_url: str, timeout: int = 30) -> str:
    """
    Fetch a publication/created date for a DOI from the DataCite API and normalize it.

    Args:
        doi_or_doi_url: DOI identifier or DOI URL.
        timeout: HTTP timeout in seconds.

    Returns:
        ISO-8601 string (normalized) if available, else "".
    """
    doi = _norm_doi(doi_or_doi_url)
    if not doi:
        return ""
    try:
        url = f"https://api.datacite.org/works/{quote(doi, safe='')}"
        r = requests.get(url, timeout=timeout)
        if r.status_code != 200:
            return ""
        attrs = (r.json().get("data") or {}).get("attributes") or {}

        # DataCite commonly has 'created' (and sometimes 'published' etc.)
        for key in ("created", "published", "issued"):
            val = attrs.get(key)
            if val:
                try:
                    return _norm_date_iso(val)
                except Exception:
                    # keep looking at other keys if one fails to parse
                    continue
    except Exception:
        pass
    return ""


def _fetch_crossref_pubdate(doi_or_doi_url: str, timeout: int = 30) -> str:
    """
    Fetch a publication/issued date for a DOI from the Crossref API and normalize it.

    Tries, in order:
      - message.created.date-time
      - message.issued.date-parts
      - message.published-print.date-parts
      - message.published-online.date-parts

    Args:
        doi_or_doi_url: DOI identifier or DOI URL.
        timeout: HTTP timeout in seconds.

    Returns:
        ISO-8601 string (normalized) if available, else "".
    """
    doi = _norm_doi(doi_or_doi_url)
    if not doi:
        return ""
    try:
        url = f"https://api.crossref.org/works/{quote(doi, safe='')}"
        r = requests.get(url, timeout=timeout)
        if r.status_code != 200:
            return ""
        msg = r.json().get("message") or {}

        # 1) created.date-time
        created_dt = (msg.get("created") or {}).get("date-time")
        if created_dt:
            try:
                return _norm_date_iso(created_dt)
            except Exception:
                pass

        # 2) issued / published-* date-parts
        for key in ("issued", "published-print", "published-online"):
            dp = (msg.get(key) or {}).get("date-parts")
            iso = _as_iso_from_dateparts(dp) if dp else ""
            if iso:
                return iso
    except Exception:
        pass
    return ""


def _best_publication_date_for_doi(
    doi_or_doi_url: str, email: Optional[str] = None
) -> str:
    """
    Return the best-available normalized publication date for a DOI.

    Order of preference:
      1) Open Alex (has more generous rate limit)
      2) DataCite
      2) Crossref

    Args:
        doi_or_doi_url: DOI identifier or DOI URL.
        email: Unused here, kept for API symmetry with other callers.

    Returns:
        ISO-8601 string if found, else "".
    """
    return (
        _fetch_openalex_pubdate(doi_or_doi_url)
        or _fetch_datacite_pubdate(doi_or_doi_url)
        or _fetch_crossref_pubdate(doi_or_doi_url)
        or ""
    )


def find_citations_dc(
    target_doi: str,
    dataset_pub_date: str,
    citations: Dict[str, list],
    email: Optional[str] = None,
) -> List[Dict[str, str]]:
    """
    Convert a slimmed DataCite citations block into normalized citation records.

    Input shape (from `slim_datacite_metadata`):
        {
          "dois":  [ "<doi or doi url>", ... ],
          "other": [ {"id": "<identifier>", "type": "<type>"}, ... ]
        }

    Behavior:
      - For entries under "dois":
          * normalize to canonical DOI identifier (`_norm_doi`)
          * build canonical DOI URL (`_norm_doi_url`) for `citation_link`
          * look up a best publication date via `_best_publication_date_for_doi`
          * include "citation_date" only when non-empty
      - For entries under "other":
          * save their raw "id" as `citation_link` (no date lookup)

    Args:
        target_doi: normalized doi of the dataset
        citations: Slimmed DataCite citations  of the dataset as dict with keys "dois" and/or "other".
        email: Optional contact email

    Returns:
        A list of citation dicts:
        [
          {
          doi: <target_doi>
          "source": ["datacite"],
          "citation_link": "<link>",
          "citation_date": "<ISO>" if available,
          "citation_weight": <e.g. 1.0 or 1.23>
          ,
          ...
        ]
        Duplicates (by `citation_link`) are removed while preserving order.
    """
    results: List[Dict[str, str]] = []

    # DOIs → normalize + fetch date
    for citation_link_raw in (citations or {}).get("dois", []) or []:
        citation_doi = _norm_doi(citation_link_raw)
        if not citation_doi:
            continue
        citation_link = _norm_doi_url(citation_doi)
        rec = {
            "doi": target_doi,
            "source": ["datacite"],
            "citation_link": citation_link,
        }

        citation_date = _best_publication_date_for_doi(citation_doi, email=email)
        if citation_date:
            rec["citation_date"] = citation_date
        rec["citation_weight"] = _citation_weight(dataset_pub_date, citation_date)
        results.append(rec)

    # Other identifiers → keep id only
    for obj in (citations or {}).get("other", []) or []:
        if not isinstance(obj, dict):
            continue
        id_val = (obj.get("id") or "").strip()
        if not id_val:
            continue
        results.append(
            {
                "doi": target_doi,
                "source": ["datacite"],
                "citation_link": id_val,
                "citation_weight": 1.0,
            }
        )

    return _dedupe_citations_by_link(results)


def batch_find_citations_dc(
    slim_folder: str,
    output_path: str,
    email: Optional[str] = None,
) -> int:
    """
    Run DataCite-based citation extraction.

    This function extract citations provided by DataCite
    for all slimmed DataCite records in a folder and
    write one NDJSON line per (dataset DOI, citation)
    combination.

    Each input NDJSON line in `slim_folder` is expected to be a slimmed
    DataCite record that includes:
      - a top-level `"doi"` field for the dataset, and
      - an optional `"citations"` field in the format produced by
        `slim_datacite_metadata`, e.g.:

            "citations": {
              "dois":  [ "<doi or doi url>", ... ],
              "other": [ {"id": "<identifier>", "type": "<type>"}, ... ]
            }

    For each record, this function calls `find_citations_dc()` on the
    `"citations"` block and writes one NDJSON line per resulting citation
    with the form:

        {
          "doi": "<canonical dataset DOI URL>",
          "source": ["datacite"],
          "citation_link": "<DOI URL or other identifier>",
          "citation_date": "<ISO-8601>"   # only if available
        }

    Args:
        slim_folder:
            Directory containing `.ndjson` files with slimmed DataCite records
            (one JSON object per line).
        output_path:
            Path to the output NDJSON file (e.g. `citations-datacite.ndjson`).
        email:
            Optional contact email (currently unused by `find_citations_dc`,
            kept for interface symmetry).

    Returns:
        The number of citation records written to `output_path`
        (i.e., the number of NDJSON lines).

    Notes:
        - Dataset DOIs are normalized with `_norm_doi()` and then converted
          to canonical DOI URLs with `_norm_doi_url()` for the `"doi"` field.
        - Citation normalization, publication date lookup (via DataCite/Crossref),
          and per-DOI de-duplication by `citation_link` are delegated to
          `find_citations_dc()`.
        - Records without a valid `"doi"` or `"citations"` block are skipped.
    """
    slimmed_glob = os.path.join(slim_folder, "*.ndjson")
    slimmed_files = glob.glob(slimmed_glob)

    if not slimmed_files:
        print(f" No slimmed NDJSON files found in {slim_folder!r}")
        return 0

    # Ensure output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_path)) or ".", exist_ok=True)

    written = 0
    with open(output_path, "w", encoding="utf-8") as out_f:
        for path in slimmed_files:
            print(f"Scanning slimmed file {path} for DataCite citations ...")
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        rec: Dict[str, Any] = json.loads(line)
                    except json.JSONDecodeError:
                        print(f"   Skipping malformed JSON line in {path}")
                        continue

                    # 1) Skip early if there is no usable citations block
                    citations_block = rec.get("citations")
                    if not isinstance(citations_block, dict):
                        continue
                    target_doi = rec.get("doi")
                    dataset_pub_date = rec.get("publication_date", "")

                    # 2) Use existing function to get citations for this record
                    citations = find_citations_dc(
                        target_doi, dataset_pub_date, citations_block, email=email
                    )

                    for cit in citations:
                        out_f.write(json.dumps(cit, ensure_ascii=False) + "\n")
                        written += 1

    print(
        f" Finished DataCite citation extraction. Wrote {written} citation records to {output_path}"
    )
    return written


# -------------  Citation weight ------------- #
def _citation_weight(ds_dt: str, citation_dt: str) -> float:
    a = 0.33
    ds_dt = _to_datetime_utc(ds_dt)
    citation_dt = _to_datetime_utc(citation_dt)
    if ds_dt is None or citation_dt is None:
        delta_years = 0.0
    else:
        delta_years = _years_between(ds_dt, citation_dt)

    weight = 1.0 + a * math.log(1.0 + delta_years)
    return round(weight, 2)


# -------------  Records of unique citations ------------- #
def merge_citations(
    input_paths: Iterable[str],
    output_path: str,
) -> int:
    """
    Merge multiple NDJSON citation files into a single deduplicated NDJSON output.

    This function ingests several NDJSON files where each line is a citation record
    of the form (generic over identifier type):
        {
            "dataset_id": <identifier for the dataset (DOI, EMDB ID, etc.)>,
            "source": [<string>],
            "citation_link": <string>,
            "citation_date": <ISO 8601 date string or empty>,
            "citation_weight": <numeric weight>
        }

    Records are merged across sources using the tuple
    `(dataset_id, citation_link)` as the deduplication key. For each such group:

      • Sources are merged as a union of all sources (sorted, duplicates removed).

      • Citation date selection:
            - Records with a non-empty `citation_date` are preferred over
              records without a date.
            - If multiple records have dates, the record with the *earliest*
              date (chronologically smallest) is selected.
            - If the selected record has an empty date, the output omits the
              `citation_date` field entirely.

      • Citation weight is taken from the selected record (the one whose
        date wins the above comparison).

    The merged records are written to `output_path` as NDJSON, one record per line.

    Args:
        input_paths: Iterable of NDJSON file paths to read from.
        output_path: Path to the output NDJSON file containing merged citation records.

    Returns:
        The number of unique `(dataset_id, citation_link)` combinations written
        to the output file.
    """
    merged: dict[tuple[str, str], dict] = {}
    best_dt: dict[tuple[str, str], Optional[datetime]] = {}
    total_input_lines = 0

    for path in input_paths:
        if not path:
            continue

        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                total_input_lines += 1
                line = line.strip()
                if not line:
                    continue

                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Prefer generic "dataset_id"
                dataset_id = rec.get("dataset_id")
                link = rec.get("citation_link")
                if not dataset_id or not link:
                    continue

                key = (dataset_id, link)

                # Normalize sources to a list[str]
                src = rec.get("source") or []
                if isinstance(src, str):
                    src = [src]
                new_sources = set(src)

                date_str = rec.get("citation_date") or ""
                dt = _to_datetime_utc(date_str)

                existing = merged.get(key)

                if existing is None:
                    # First record for this (dataset_id, citation_link)
                    entry = {
                        "dataset_id": dataset_id,
                        "source": list(new_sources),
                        "citation_link": link,
                        "citation_weight": rec.get("citation_weight"),
                    }
                    if dt is not None:
                        entry["citation_date"] = date_str
                    merged[key] = entry
                    best_dt[key] = dt
                    continue

                # Merge sources
                existing_sources = set(existing.get("source", []))
                existing["source"] = sorted(existing_sources | new_sources)

                existing_dt = best_dt.get(key)
                replace = False

                # Prefer any dated record over no-date record
                if existing_dt is None and dt is not None:
                    replace = True
                # If both have dates, keep earliest
                elif existing_dt is not None and dt is not None and dt < existing_dt:
                    replace = True

                if replace:
                    existing["citation_weight"] = rec.get("citation_weight")
                    if dt is not None:
                        existing["citation_date"] = date_str
                    else:
                        # If selecting a no-date record, drop citation_date (should not happen)
                        existing.pop("citation_date", None)
                    best_dt[key] = dt

    # Write output
    with open(output_path, "w", encoding="utf-8") as out:
        for record in merged.values():
            json.dump(record, out, ensure_ascii=False)
            out.write("\n")

    # --- PRINT SUMMARY ---
    unique_count = len(merged)
    removed_count = total_input_lines - unique_count
    pct_reduction = (
        (removed_count / total_input_lines * 100) if total_input_lines else 0.0
    )

    print("\n[Merging Summary]")
    print(f"  Total input citation: {total_input_lines:,}")
    print(f"  Unique merged citations:    {unique_count:,}")
    print(f"  Removed duplicates:         {removed_count:,} ({pct_reduction:.1f}%)")
    print(f"  Output written to:          {output_path}")

    return len(merged)
