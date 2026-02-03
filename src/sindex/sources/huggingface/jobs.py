import json
import logging
import os
import re
import time

from huggingface_hub import HfApi
from huggingface_hub.utils import HfHubHTTPError

# Files
OUTPUT_FILE = "models_data_refs.ndjson"  # results
HISTORY_FILE = "processed_history.txt"  # log of all scanned IDs


# Silence HF warnings so they don't show in the Jupyter botebook
logging.getLogger("huggingface_hub").setLevel(logging.ERROR)


def get_dataset_doi(dataset_id, api, cache):
    """Finds DOI with rate limit handling"""
    if dataset_id in cache:
        return cache[dataset_id]
    doi = None
    retries = 0
    while retries < 3:
        try:
            dataset_meta = api.dataset_info(dataset_id)
            if dataset_meta.tags:
                for tag in dataset_meta.tags:
                    if tag.startswith("doi:"):
                        doi = tag.split("doi:")[1]
                        break
            if not doi and dataset_meta.citation:
                doi_match = re.search(
                    r"10\.\d{4,9}/[-._;()/:a-zA-Z0-9]+", dataset_meta.citation
                )
                if doi_match:
                    doi = doi_match.group(0)
            break
        except HfHubHTTPError as e:
            if e.response.status_code == 429:
                time.sleep(5 * (retries + 1))
                retries += 1
            else:
                break
        except Exception:
            break
    cache[dataset_id] = doi
    return doi


def load_history():
    """Loads all previously processed IDs into a Python Set fast lookup"""
    if not os.path.exists(HISTORY_FILE):
        return set()
    print("Loading history log...")
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        # Read lines into a set, stripping newlines
        return set(line.strip() for line in f)


def scan_hfhub_modelcards(limit=None, token=None):
    api = HfApi(token=token)
    dataset_cache = {}
    emdb_pattern = re.compile(r"EMD-\d{4,5}", re.IGNORECASE)

    # 1. Load history to skip already analyzed model cards
    processed_ids = load_history()
    print(f"Loaded {len(processed_ids):,} previously scanned models.")

    while True:
        try:
            print("Connecting to Hugging Face Hub")
            models = api.list_models(cardData=True, limit=limit)

            saved_count = 0
            scanned_session_count = 0
            skipped_count = 0

            # Open history
            with (
                open(HISTORY_FILE, "a", encoding="utf-8", buffering=1) as hist_f,
                open(OUTPUT_FILE, "a", encoding="utf-8", buffering=1) as out_f,
            ):
                for model in models:
                    # Skip model if already seen in a previous run
                    if model.modelId in processed_ids:
                        skipped_count += 1
                        if skipped_count % 10000 == 0:
                            print(
                                f"Skipped {skipped_count:,} known models.",
                                end="\r",
                            )
                        continue

                    scanned_session_count += 1

                    # Log progress
                    if scanned_session_count % 100 == 0:
                        print(
                            f"Scanned {scanned_session_count:,} new | Saved {saved_count:,} | Skipped {skipped_count:,} | ID: {model.modelId[:30]}...   ",
                            end="\r",
                            flush=True,
                        )

                    # Processing
                    refs_found = []

                    # 1. Tags
                    if model.tags:
                        for tag in model.tags:
                            for match in emdb_pattern.findall(tag):
                                refs_found.append(
                                    {
                                        "type": "EMDB",
                                        "id": match.upper(),
                                        "source": "tag",
                                    }
                                )

                    # 2. Datasets
                    if model.cardData and "datasets" in model.cardData:
                        raw = model.cardData["datasets"]
                        dataset_ids = [raw] if isinstance(raw, str) else raw

                        if isinstance(dataset_ids, list):
                            for d_id in dataset_ids:
                                emdb_matches = emdb_pattern.findall(d_id)
                                for match in emdb_matches:
                                    refs_found.append(
                                        {
                                            "type": "EMDB",
                                            "id": match.upper(),
                                            "source": "dataset_name",
                                        }
                                    )

                                if not emdb_matches:
                                    doi = get_dataset_doi(d_id, api, dataset_cache)
                                    if doi:
                                        refs_found.append(
                                            {
                                                "type": "DOI",
                                                "id": doi,
                                                "source": "metadata",
                                                "hf_dataset_id": d_id,
                                            }
                                        )

                    # Save
                    if refs_found:
                        unique_refs = [
                            json.loads(x)
                            for x in set(
                                json.dumps(obj, sort_keys=True) for obj in refs_found
                            )
                        ]
                        entry = {
                            "model_link": f"https://huggingface.co/{model.modelId}",
                            "published_date": model.createdAt.isoformat()
                            if hasattr(model, "createdAt") and model.createdAt
                            else None,
                            "references": unique_refs,
                        }
                        out_f.write(json.dumps(entry) + "\n")
                        saved_count += 1

                    # Update log
                    hist_f.write(model.modelId + "\n")
                    processed_ids.add(model.modelId)

            print(f"\nProcessing complete. Scanned {scanned_session_count} new models.")
            break

        except (HfHubHTTPError, Exception) as e:
            print(f"\n\n CONNECTION LOST: {e}")
            print(" Waiting 60 seconds")
            time.sleep(60)
            print(" Restarting")
