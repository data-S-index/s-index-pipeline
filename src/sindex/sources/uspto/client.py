import json
import os
import random
import re
import time
from datetime import datetime

import requests


def get_timestamp():
    """Returns current time string for prgoress log."""
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def log_failure(output_dir, file_name, reason):
    """Appends failed filename and reason to a log file."""
    log_path = os.path.join(output_dir, "failed_downloads.txt")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{get_timestamp()} - {file_name} - {reason}\n")


def download_patents(json_file_path, api_key, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 1. Load and Parse JSON
    print(f"{get_timestamp()} Loading JSON...")
    with open(json_file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    try:
        raw_list = data["bulkDataProductBag"][0]["productFileBag"]["fileDataBag"]
    except (KeyError, IndexError):
        print("Error: JSON structure invalid.")
        return

    # 2. Filter: Keep latest revisions only
    pattern = re.compile(r"(ipg\d{6})(?:_r(\d+))?\.zip", re.IGNORECASE)
    best_files = {}

    for file_info in raw_list:
        fname = file_info.get("fileName", "")
        match = pattern.match(fname)
        if not match:
            continue

        base_name = match.group(1)
        revision = int(match.group(2)) if match.group(2) else 0

        if base_name not in best_files or revision > best_files[base_name]["rev"]:
            best_files[base_name] = {"rev": revision, "data": file_info}

    final_list = [v["data"] for v in best_files.values()]
    final_list.sort(key=lambda x: x["fileName"], reverse=True)

    total_files = len(final_list)

    # 3. Pre-scan: Check what exists vs what needs downloading
    print(f"{get_timestamp()} Scanning {total_files} files to check existing status...")

    files_to_download = []
    already_existing = 0

    for file_info in final_list:
        file_name = file_info.get("fileName")
        local_path = os.path.join(output_dir, file_name)

        # Check if file exists and is valid size (>25KB)
        # When error the API seem to download error report as zip of about 17kb so we don't want to count that as "completed"
        if os.path.exists(local_path) and os.path.getsize(local_path) > 25000:
            already_existing += 1
        else:
            files_to_download.append(file_info)

    # Beginning stats
    print(f"Total Files in Set:       {total_files}")
    print(f"Already Downloaded:       {already_existing}")
    print(f"Remaining to Process:     {len(files_to_download)}")

    if not files_to_download:
        print("All files are already downloaded! Nothing to do.")
        return

    # 4. Download Loop
    session = requests.Session()
    session.headers.update(
        {
            "x-api-key": api_key,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://data.uspto.gov/",
        }
    )

    success_count = 0
    fail_count = 0
    total_queue = len(files_to_download)

    for i, file_info in enumerate(files_to_download):
        file_name = file_info.get("fileName")
        download_url = file_info.get("fileDownloadURI")
        local_path = os.path.join(output_dir, file_name)

        # Remove corrupt file if it exists (small file)
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
            except:
                pass

        # Progress
        msg = f"Processing [{i + 1}/{total_queue}] | Success: {success_count} | Failed: {fail_count} | Current: {file_name}"
        print(msg.ljust(100), end="\r", flush=True)

        try:
            with session.get(download_url, stream=True, timeout=60) as r:
                # Check for HTML error page
                if "html" in r.headers.get("Content-Type", "").lower():
                    fail_count += 1
                    log_failure(output_dir, file_name, "Blocked by WAF (HTML response)")
                    time.sleep(15)  # Wait a bit if blocked
                    continue

                r.raise_for_status()

                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            # If we get here, it worked
            success_count += 1
            time.sleep(random.uniform(10.0, 15.0))

        except Exception as e:
            fail_count += 1
            log_failure(output_dir, file_name, str(e))
            time.sleep(15)

    # 5. Final Summary
    print()
    print("Completed")
    print(f"Total Processed: {total_queue}")
    print(f"Successful:      {success_count}")
    print(f"Failed:          {fail_count}")
    print(f"Logs saved to:   {os.path.join(output_dir, 'download_failures.txt')}")


def download_patents_linear_old(json_file_path, api_key, output_dir):
    """
    Downloads USPTO patents sequentially with a final summary report.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Counter
    stats = {"existed": 0, "downloaded": 0, "failed": 0}

    # 1. Load and Parse JSON
    with open(json_file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    try:
        raw_list = data["bulkDataProductBag"][0]["productFileBag"]["fileDataBag"]
    except (KeyError, IndexError):
        print(f"{get_timestamp()} Error: JSON structure invalid.")
        return

    # 2. Keep latest revision when multiple
    pattern = re.compile(r"(ipg\d{6})(?:_r(\d+))?\.zip", re.IGNORECASE)
    best_files = {}

    print(f"{get_timestamp()} Analyzing {len(raw_list)} files for revisions...")

    for file_info in raw_list:
        fname = file_info.get("fileName", "")
        match = pattern.match(fname)

        if not match:
            continue

        base_name = match.group(1)
        rev_str = match.group(2)
        revision = int(rev_str) if rev_str else 0

        if base_name not in best_files:
            best_files[base_name] = {"rev": revision, "data": file_info}
        else:
            if revision > best_files[base_name]["rev"]:
                best_files[base_name] = {"rev": revision, "data": file_info}

    final_download_list = [v["data"] for v in best_files.values()]
    final_download_list.sort(key=lambda x: x["fileName"], reverse=True)

    print(
        f"{get_timestamp()} Filter complete. Queue: {len(final_download_list)} files."
    )
    print("-" * 30)

    # Session
    session = requests.Session()
    session.headers.update(
        {
            "x-api-key": api_key,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://data.uspto.gov/",
        }
    )

    # 4. Download
    for i, file_info in enumerate(final_download_list):
        file_name = file_info.get("fileName")
        download_url = file_info.get("fileDownloadURI")
        local_path = os.path.join(output_dir, file_name)

        progress_prefix = f"[{i + 1}/{len(final_download_list)}]"

        # Check existing files
        if os.path.exists(local_path):
            if os.path.getsize(local_path) > 25000:
                print(
                    f"{get_timestamp()} {progress_prefix} Skipping {file_name} (Already exists)"
                )
                stats["existed"] += 1
                continue
            else:
                print(
                    f"{get_timestamp()} {progress_prefix} Found corrupt file {file_name}. Deleting and retrying..."
                )
                try:
                    os.remove(local_path)
                except OSError:
                    pass

        print(f"{get_timestamp()} {progress_prefix} Downloading {file_name}...")

        try:
            with session.get(download_url, stream=True, timeout=60) as r:
                if "html" in r.headers.get("Content-Type", "").lower():
                    print(f"{get_timestamp()}     FAILED.")
                    log_failure(output_dir, file_name, "FAILED")
                    stats["failed"] += 1
                    time.sleep(15)
                    continue

                r.raise_for_status()

                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            # Sucess
            stats["downloaded"] += 1
            wait_time = random.uniform(10.0, 15.0)
            time.sleep(wait_time)

        except Exception as e:
            # Fail
            print(f"{get_timestamp()}     !FAILURE: {e}")
            log_failure(output_dir, file_name, str(e))
            stats["failed"] += 1

            wait_time = random.uniform(10.0, 15.0)
            time.sleep(wait_time)

    # 5. Summary
    total_now = stats["existed"] + stats["downloaded"]

    print("\nDone! Summary:")
    print(f"Files Originally Existed:  {stats['existed']}")
    print(f"Files Downloaded New:      {stats['downloaded']}")
    print(f"Files Failed:              {stats['failed']}")
    print(f"Total Valid Files on Disk: {total_now} / {len(final_download_list)}")
