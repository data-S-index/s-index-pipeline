import json
import logging
import os
import time

from huggingface_hub import HfApi, HfFileSystem
from huggingface_hub.utils import HfHubHTTPError

from sindex.core.ids import _DOI_PATTERN, _EMDB_PATTERN

# Files
OUTPUT_FILE_MODELCARD = "hf_scan_results_modelcard.ndjson"
HISTORY_FILE_MODELCARD = "hf_scan_history_modelcard.txt"
ERROR_FILE_MODELCARD = "hf_scan_errors_modelcard.txt"

OUTPUT_FILE_README = "hf_scan_results_readme.ndjson"
HISTORY_FILE_README = "hf_scan_history_readme.txt"
ERROR_FILE_README = "hf_scan_errors_readme.txt"

CONCURRENCY_LIMIT = 50  # Number of parallel README downloads
BATCH_SIZE = 100  # How many models to group before saving to disk

# Pattern Registry (can add new ID types here in the future)
SCAN_PATTERNS = {
    "EMDB": _EMDB_PATTERN,
    "DOI": _DOI_PATTERN,
}

# Silence HF warnings so they don't show in the Jupyter botebook
logging.getLogger("huggingface_hub").setLevel(logging.ERROR)


def load_history(HISTORY_FILE):
    """Loads all previously processed IDs into a Python Set for fast lookup"""
    if not os.path.exists(HISTORY_FILE):
        return set()
    print("Loading history log")
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f)


# SCAN MODEL CARDS


def get_dataset_references(dataset_id, api, cache):
    """
    Checks a linked dataset for ANY ID in SCAN_PATTERNS.
    Returns:
      - List of dicts: Success (References found)
      - []: Success (No references found)
      - None: FAILURE (Connection error/timeout)
    """
    if dataset_id in cache:
        return cache[dataset_id]

    found_refs = []
    retries = 0

    while retries < 3:
        try:
            meta = api.dataset_info(dataset_id)

            # A. Scan Tags
            if meta.tags:
                for tag in meta.tags:
                    for label, pattern in SCAN_PATTERNS.items():
                        clean_tag = tag.replace("doi:", "")
                        matches = pattern.findall(clean_tag)
                        for match in matches:
                            found_refs.append(
                                {
                                    "type": label,
                                    "id": match.strip(".,"),
                                    "source": "linked_dataset_tag",
                                    "hf_dataset_id": dataset_id,
                                }
                            )

            # B. Scan Citation
            if meta.citation:
                for label, pattern in SCAN_PATTERNS.items():
                    matches = pattern.findall(meta.citation)
                    for match in matches:
                        found_refs.append(
                            {
                                "type": label,
                                "id": match.strip(".,"),
                                "source": "linked_dataset_citation",
                                "hf_dataset_id": dataset_id,
                            }
                        )

            # Success
            cache[dataset_id] = found_refs
            return found_refs

        except HfHubHTTPError as e:
            if e.response.status_code == 429:  # Rate limit
                time.sleep(5 * (retries + 1))
                retries += 1
            else:
                cache[dataset_id] = []
                return []

        except Exception:
            # Generic connection error
            retries += 1
            time.sleep(1)

    # We ran out of retries -> FAILURE
    return None


def scan_hfhub_modelcards(limit=None, token=None):
    api = HfApi(token=token)
    dataset_cache = {}

    processed_ids = load_history(HISTORY_FILE_MODELCARD)
    print(f"Loaded {len(processed_ids):,} previously scanned models.")

    while True:
        try:
            print("Connecting to Hugging Face Hub")
            models = api.list_models(
                cardData=True, limit=limit, sort="createdAt", direction=-1
            )

            session_scanned = 0
            session_saved = 0
            session_errors = 0
            skipped_count = 0

            with (
                open(
                    HISTORY_FILE_MODELCARD, "a", encoding="utf-8", buffering=1
                ) as hist_f,
                open(
                    OUTPUT_FILE_MODELCARD, "a", encoding="utf-8", buffering=1
                ) as out_f,
                open(ERROR_FILE_MODELCARD, "a", encoding="utf-8", buffering=1) as err_f,
            ):
                for model in models:
                    if model.id in processed_ids:
                        skipped_count += 1
                        if skipped_count % 5000 == 0:
                            print(
                                f"Skipped {skipped_count:,} known models...", end="\r"
                            )
                        continue

                    session_scanned += 1
                    if session_scanned % 50 == 0:
                        print(
                            f"Scanned {session_scanned:,} | Saved {session_saved:,} | Errors {session_errors:,} | ID: {model.id[:30]}...   ",
                            end="\r",
                        )

                    refs_found = []
                    has_error = False

                    # A. Scan Model Tags
                    if model.tags:
                        for tag in model.tags:
                            for label, pattern in SCAN_PATTERNS.items():
                                matches = pattern.findall(tag)
                                for match in matches:
                                    refs_found.append(
                                        {
                                            "type": label,
                                            "id": match.strip(".,"),
                                            "source": "tag",
                                        }
                                    )

                    # B. Scan Linked Datasets
                    data = model.cardData or getattr(model, "card_data", None)

                    if data and "datasets" in data:
                        raw = data["datasets"]
                        dataset_ids = [raw] if isinstance(raw, str) else raw

                        if isinstance(dataset_ids, list):
                            for d_id in dataset_ids:
                                # 1. Check Name
                                found_in_name = False
                                for label, pattern in SCAN_PATTERNS.items():
                                    matches = pattern.findall(d_id)
                                    for match in matches:
                                        refs_found.append(
                                            {
                                                "type": label,
                                                "id": match.strip(".,"),
                                                "source": "dataset_name",
                                            }
                                        )
                                        found_in_name = True

                                # 2. Check Metadata (Hop)
                                if not found_in_name:
                                    linked_refs = get_dataset_references(
                                        d_id, api, dataset_cache
                                    )

                                    if linked_refs is None:
                                        # Log this dataset ID to the error file
                                        err_f.write(
                                            f"dataset:{d_id} (linked from {model.id})\n"
                                        )
                                        has_error = True
                                    elif linked_refs:
                                        # Found refs
                                        refs_found.extend(linked_refs)

                    # C. Save results
                    if refs_found:
                        unique_refs = [
                            json.loads(x)
                            for x in set(
                                json.dumps(obj, sort_keys=True) for obj in refs_found
                            )
                        ]

                        entry = {
                            "model_id": model.id,
                            "url": f"https://huggingface.co/{model.id}",
                            "created_at": model.created_at.isoformat()
                            if hasattr(model, "created_at") and model.created_at
                            else None,
                            "references": unique_refs,
                        }
                        out_f.write(json.dumps(entry) + "\n")
                        session_saved += 1

                    # Update history log
                    hist_f.write(model.id + "\n")
                    processed_ids.add(model.id)

                    if has_error:
                        session_errors += 1

            print(
                f"\nProcessing complete. Scanned {session_scanned} new models. Saved {session_saved} entries. Errors {session_errors}."
            )
            break

        except (HfHubHTTPError, Exception) as e:
            print(f"\n\n[CRITICAL] Connection lost: {e}")
            print("Waiting 60 seconds")
            time.sleep(60)
            print("Restarting")


# FULL README SCAN


def get_readme_with_retry(fs, model_id, retries=5):
    file_path = f"{model_id}/README.md"

    while True:
        try:
            return fs.read_text(file_path)

        except FileNotFoundError:
            return "NOT_FOUND"

        except HfHubHTTPError as e:
            # CASE 1: Rate Limit (429) Wait longer
            if e.response.status_code == 429:
                print(f"\n[!] Rate limit hit on {model_id}. Cooling down for 60s...")
                time.sleep(60)
                continue

            # CASE 2: Other errors standard Retry
            if retries > 0:
                time.sleep(2)
                retries -= 1
            else:
                return None  # Failed after retries

        except Exception:
            # Generic connection errors
            if retries > 0:
                time.sleep(2)
                retries -= 1
            else:
                return None  # Failed after retries


def scan_hf_full_readme(limit=None, token=None):
    api = HfApi(token=token)
    fs = HfFileSystem(token=token)

    # Load history
    processed_ids = load_history(HISTORY_FILE_README)
    print(f"Loaded {len(processed_ids):,} previously scanned models.")

    while True:
        try:
            print("Connecting to Hugging Face Hub")
            models = api.list_models(limit=limit, sort="createdAt", direction=-1)

            session_scanned = 0
            session_saved = 0
            session_errors = 0
            skipped_count = 0

            # Open ALL THREE files
            with (
                open(HISTORY_FILE_README, "a", encoding="utf-8", buffering=1) as hist_f,
                open(OUTPUT_FILE_README, "a", encoding="utf-8", buffering=1) as out_f,
                open(ERROR_FILE_README, "a", encoding="utf-8", buffering=1) as err_f,
            ):
                for model in models:
                    if model.id in processed_ids:
                        skipped_count += 1
                        if skipped_count % 5000 == 0:
                            print(f"Skipped {skipped_count:,} known models", end="\r")
                        continue

                    # Call Helper
                    readme_result = get_readme_with_retry(fs, model.id)

                    session_scanned += 1
                    if session_scanned % 50 == 0:
                        print(
                            f"Scanned {session_scanned:,} | Saved {session_saved:,} | Errors {session_errors:,} | ID: {model.id[:30]}...",
                            end="\r",
                        )

                    # CASE 1: CRITICAL FAILURE (Network Error / Timeout)
                    if readme_result is None:
                        err_f.write(model.id + "\n")
                        session_errors += 1
                        continue  # Skip to next model, DO NOT write to history

                    # CASE 2: SUCCESS or DEFINITIVE 404
                    refs_found = []

                    if readme_result != "NOT_FOUND":
                        for label, pattern in SCAN_PATTERNS.items():
                            matches = pattern.findall(readme_result)
                            for match in matches:
                                clean_id = match.strip(".,")
                                refs_found.append(
                                    {
                                        "type": label,
                                        "id": clean_id,
                                        "source": "full_text",
                                    }
                                )

                    # Save references found
                    if refs_found:
                        unique_refs = [
                            json.loads(x)
                            for x in set(
                                json.dumps(obj, sort_keys=True) for obj in refs_found
                            )
                        ]
                        entry = {
                            "model_id": model.id,
                            "url": f"https://huggingface.co/{model.id}",
                            "created_at": model.created_at.isoformat()
                            if hasattr(model, "created_at") and model.created_at
                            else None,
                            "references": unique_refs,
                        }
                        out_f.write(json.dumps(entry) + "\n")
                        session_saved += 1

                    # Mark as processed in history
                    hist_f.write(model.id + "\n")
                    processed_ids.add(model.id)

            print(
                f"\nProcessing complete. Scanned {session_scanned}. Saved {session_saved}. Errors {session_errors}."
            )
            break

        except (HfHubHTTPError, Exception) as e:
            print(f"\n\n[CRITICAL] Main loop crashed: {e}")
            print("Waiting 60 seconds before restarting generator")
            time.sleep(60)
            print("Restarting")


# FULL README PARALLEL


async def fetch_and_scan_readme(semaphore, session, model, token):
    """
    Returns:
      - Dict: Success (References found)
      - "EMPTY": Success (No references found)
      - "NOT_FOUND": Success (404, no README exists)
      - None: FAILURE (Connection error, timeout - should go to error log)
    """
    url = f"https://huggingface.co/{model.id}/raw/main/README.md"
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    async with semaphore:
        retries = 3
        while retries > 0:
            try:
                async with session.get(url, headers=headers, timeout=10) as response:
                    # 1. Rate Limit (429) Wait and Retry
                    if response.status == 429:
                        print(
                            f"\n[!] Rate limit on {model.id}. Cooling down 10s...",
                            end="\r",
                        )
                        await asyncio.sleep(10)
                        continue

                    # 2. Missing File (404) Valid State
                    if response.status == 404:
                        return "NOT_FOUND"

                    # 3. Success (200)
                    if response.status == 200:
                        content = await response.text()

                        refs_found = []
                        if content:
                            for label, pattern in SCAN_PATTERNS.items():
                                matches = pattern.findall(content)
                                for match in matches:
                                    if isinstance(match, tuple):
                                        match = match[0]
                                    clean_id = match.strip(".,")
                                    refs_found.append(
                                        {
                                            "type": label,
                                            "id": clean_id,
                                            "source": "full_text",
                                        }
                                    )

                        if refs_found:
                            unique_refs = [
                                json.loads(x)
                                for x in set(
                                    json.dumps(obj, sort_keys=True)
                                    for obj in refs_found
                                )
                            ]
                            return {
                                "model_id": model.id,
                                "url": f"https://huggingface.co/{model.id}",
                                "created_at": model.created_at.isoformat()
                                if hasattr(model, "created_at") and model.created_at
                                else None,
                                "references": unique_refs,
                            }
                        return "EMPTY"  # Readme scanned but nothing found

                    # Server Errors (500, 502, etc) Retry
                    retries -= 1
                    await asyncio.sleep(1)

            except (aiohttp.ClientError, asyncio.TimeoutError):
                retries -= 1
                await asyncio.sleep(1)

    return None  # Retries exhausted FAILURE


async def run_parallel_scan(limit=None, token=None):
    api = HfApi(token=token)
    processed_ids = load_history()
    print(f"Loaded {len(processed_ids):,} previously scanned models.")

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async with aiohttp.ClientSession() as session:
        print("Connecting to Hugging Face Hub (Generator)...")
        model_generator = api.list_models(
            limit=limit, sort="createdAt", direction=-1, cardData=True
        )

        batch_tasks = []
        batch_models = []

        session_scanned = 0
        session_saved = 0
        session_errors = 0
        skipped_count = 0

        with (
            open(HISTORY_FILE_README, "a", encoding="utf-8", buffering=1) as hist_f,
            open(OUTPUT_FILE_README, "a", encoding="utf-8", buffering=1) as out_f,
            open(ERROR_FILE_README, "a", encoding="utf-8", buffering=1) as err_f,
        ):
            for model in model_generator:
                if model.id in processed_ids:
                    skipped_count += 1
                    if skipped_count % 10000 == 0:
                        print(f"Skipped {skipped_count:,} known models", end="\r")
                    continue

                batch_models.append(model)
                task = fetch_and_scan_readme(semaphore, session, model, token)
                batch_tasks.append(task)

                # Process Batch
                if len(batch_tasks) >= BATCH_SIZE:
                    results = await asyncio.gather(*batch_tasks)

                    for res, mod in zip(results, batch_models):
                        session_scanned += 1

                        if res is None:
                            # FAILURE: Log to Error File, do not add to History
                            err_f.write(mod.id + "\n")
                            session_errors += 1
                        else:
                            # SUCCESS (Even if Empty or 404): Log to History
                            hist_f.write(mod.id + "\n")
                            processed_ids.add(mod.id)

                            # If actual data found log to Output
                            if isinstance(res, dict):
                                out_f.write(json.dumps(res) + "\n")
                                session_saved += 1

                    print(
                        f"Scanned {session_scanned:,} | Saved {session_saved:,} | Errors {session_errors:,} | Queue {CONCURRENCY_LIMIT}",
                        end="\r",
                    )

                    batch_tasks = []
                    batch_models = []

            # Process Final Batch
            if batch_tasks:
                results = await asyncio.gather(*batch_tasks)
                for res, mod in zip(results, batch_models):
                    session_scanned += 1
                    if res is None:
                        err_f.write(mod.id + "\n")
                        session_errors += 1
                    else:
                        hist_f.write(mod.id + "\n")
                        if isinstance(res, dict):
                            out_f.write(json.dumps(res) + "\n")
                            session_saved += 1

    print(
        f"\nProcessing complete. Scanned {session_scanned}. Saved {session_saved}. Errors {session_errors}."
    )


def scan_hf_full_readme_parallel(limit=None, token=None):
    asyncio.run(run_parallel_scan(limit=limit, token=token))
