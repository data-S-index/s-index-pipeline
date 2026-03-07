#!/usr/bin/env python3
import boto3
import subprocess
import re
import json
from concurrent.futures import ThreadPoolExecutor
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
import os
import traceback
import sys

ATHENA_DATABASE = 'default'
ATHENA_TABLE = 'part1'
ATHENA_OUTPUT = 's3://athena-results/athena-temp/'
BUCKET = 'athena-results'
OUTPUT_KEY = 'part1/results_1.ndjson'
SKIPPED_KEY = 'part1/skipped_part1.json'
PROCESSED_DIR = 'part1/log2/'

S3_URL = "https://softwareheritage.s3.amazonaws.com/content/"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


MAX_WORKERS = 30
MAX_RETRIES = 5
BATCH_SIZE = 1000
SAVE_EVERY = 25000
RATE_LIMIT_PAUSE = 30
CONSECUTIVE_THROTTLE_LIMIT = 10
MAX_BACKOFF = 60

athena = boto3.client('athena')
s3 = boto3.client('s3')

session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(
    pool_connections=100,
    pool_maxsize=100,
    max_retries=retry_strategy
)
session.mount("http://", adapter)
session.mount("https://", adapter)
session.headers.update({"User-Agent": "research-crawler/1.0"})
log("HTTPAdapter initialized - pool_connections=100, pool_maxsize=100")

consecutive_throttle = [0]

EMD_PATTERN = re.compile(r'\bEMD-\d{4,5}\b')

DOI_PATTERNS = [
    re.compile(r'\[.*?\]\((?:https?://)?(?:dx\.)?doi\.org/(10\.\d{4,}/[^\s\[\]]+)\)', re.IGNORECASE),
    re.compile(r'DOI:\s*(10\.\d{4,}/[^\s\[\]\<\>]+)', re.IGNORECASE),
    re.compile(r'doi:\s*(10\.\d{4,}/[^\s\[\]\<\>]+)', re.IGNORECASE),
    re.compile(r'(?:https?://)?(?:dx\.)?doi\.org/(10\.\d{4,}/[^\s\[\]\<\>]+)', re.IGNORECASE),
    re.compile(r'(?<![a-zA-Z:/])10\.\d{4,}/[^\s\[\]\<\>]+'),
]


def clean_doi(doi):
    doi = re.sub(r'^(https?://)?(dx\.)?doi\.org/', '', doi, flags=re.IGNORECASE)
    doi = re.sub(r'^doi:\s*', '', doi, flags=re.IGNORECASE)
    doi = re.sub(r'[.,;:?!\s\"\'\\\*<>]+$', '', doi)

    brackets = [('(', ')'), ('{', '}'), ('[', ']')]
    for opening, closing in brackets:
        while doi.endswith(closing) and doi.count(closing) > doi.count(opening):
            doi = doi[:-1]
        while doi.endswith(opening) and doi.count(opening) > doi.count(closing):
            doi = doi[:-1]

    return doi


def process_sha1(args):
    sha1, url, date = args
    start = time.time()

    for attempt in range(2):
        if time.time() - start > 5:
            return {"skipped": sha1, "reason": "total_timeout"}
        try:
            r = session.get(f"{S3_URL}{sha1}", timeout=2)  # 15'ten 2'ye

            if r.status_code != 200 or not r.content:
                return {"skipped": sha1, "reason": f"http_{r.status_code}"}

            try:
                content = subprocess.run(["pigz", "-d"], input=r.content, capture_output=True,
                                         timeout=2).stdout.decode("utf-8", errors="ignore")  # 10'dan 2'ye
            except Exception as e:
                return {"skipped": sha1, "reason": "decompress_fail"}

            if not content:
                return None
            raw_dois = []
            for p in DOI_PATTERNS:
                for d in p.findall(content):
                    if isinstance(d, tuple):
                        d = d[-1]
                    raw_dois.append(d)

            dois = list(set(d for d in (clean_doi(x) for x in raw_dois) if d))
            emds = list(set(EMD_PATTERN.findall(content)))

            if dois or emds:
                return {"repo_link": url, "repo_creation_date": str(date).split()[0] if date else "",
                        "dois_mentioned": dois, "emd_ids": emds}
            return None
        except Exception as e:
            log(f"ERROR attempt {attempt + 1}/{MAX_RETRIES} for {sha1[:8]}: {e}")
            time.sleep(min(10, 2 ** attempt))
    return {"skipped": sha1, "reason": "max_retries"}


log("Starting Athena query...")
query = f"SELECT url, sha1, visit_date FROM {ATHENA_DATABASE}.{ATHENA_TABLE}"
response = athena.start_query_execution(QueryString=query, QueryExecutionContext={'Database': ATHENA_DATABASE},
                                        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT})
query_execution_id = response['QueryExecutionId']

while True:
    status = athena.get_query_execution(QueryExecutionId=query_execution_id)
    state = status['QueryExecution']['Status']['State']
    if state == 'SUCCEEDED':
        log("Athena query SUCCEEDED")
        break
    elif state in ['FAILED', 'CANCELLED']:
        log(f"Athena query FAILED: {status}")
        exit(1)
    time.sleep(2)

csv_key = f"athena-temp/{query_execution_id}.csv"
local_csv = "/tmp/athena_input.csv"
log(f"Downloading CSV: {csv_key}")

for attempt in range(5):
    try:
        s3.download_file(BUCKET, csv_key, local_csv)
        file_size = os.path.getsize(local_csv)
        log(f"CSV downloaded: {file_size / (1024 * 1024):.1f} MB")
        break
    except Exception as e:
        if attempt < 4:
            time.sleep(10)
        else:
            exit(1)

ndjson_lines = []
skipped = []
processed_log = []
counter = 0
found = 0
batch = []
start_time = time.time()
chunk_num = 0

first_line = True

with open(local_csv, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if first_line:
            first_line = False
            continue

        if not line:
            continue

        parts = line.split(',', 2)
        if len(parts) >= 2:
            url = parts[0].strip('"')
            sha1 = parts[1].strip('"')
            date = parts[2].strip('"') if len(parts) > 2 else ""
            batch.append((sha1, url, date))
            processed_log.append(f"{url},{sha1}")

        if len(batch) >= BATCH_SIZE:
            if consecutive_throttle[0] >= CONSECUTIVE_THROTTLE_LIMIT:
                time.sleep(RATE_LIMIT_PAUSE)
                consecutive_throttle[0] = 0

            random.shuffle(batch)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                for result in executor.map(process_sha1, batch):
                    counter += 1
                    if result:
                        if "skipped" in result:
                            skipped.append(result)
                        else:
                            found += 1
                            ndjson_lines.append(json.dumps(result))

            elapsed = time.time() - start_time
            rate = counter / elapsed if elapsed > 0 else 0
            log(f"{counter} | Found: {found} | Skip: {len(skipped)} | {rate:.0f}/s")

            if counter % SAVE_EVERY == 0:
                chunk_num += 1
                s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body='\n'.join(ndjson_lines).encode('utf-8'))
                s3.put_object(Bucket=BUCKET, Key=SKIPPED_KEY, Body=json.dumps(skipped).encode('utf-8'))
                s3.put_object(Bucket=BUCKET, Key=f'{PROCESSED_DIR}_{chunk_num}.csv',
                              Body='\n'.join(processed_log).encode('utf-8'))

                # CLEAR MEMORY to prevent crashes
                ndjson_lines.clear()
                skipped.clear()
                processed_log.clear()

                log(f"Checkpoint {chunk_num} saved & memory cleared!")

            batch = []

if batch:
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for result in executor.map(process_sha1, batch):
            counter += 1
            if result:
                if "skipped" in result:
                    skipped.append(result)
                else:
                    found += 1
                    ndjson_lines.append(json.dumps(result))

if ndjson_lines or skipped or processed_log:
    chunk_num += 1

    # read the old docs
    try:
        old_results = s3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)['Body'].read().decode('utf-8')
    except:
        old_results = ""

    try:
        old_skipped = json.loads(s3.get_object(Bucket=BUCKET, Key=SKIPPED_KEY)['Body'].read().decode('utf-8'))
    except:
        old_skipped = []

    all_results = old_results + '\n'.join(ndjson_lines) if old_results else '\n'.join(ndjson_lines)
    all_skipped = old_skipped + skipped

    # save
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=all_results.encode('utf-8'))
    s3.put_object(Bucket=BUCKET, Key=SKIPPED_KEY, Body=json.dumps(all_skipped).encode('utf-8'))

    if processed_log:
        s3.put_object(Bucket=BUCKET, Key=f'{PROCESSED_DIR}_{chunk_num}.csv',
                      Body='\n'.join(processed_log).encode('utf-8'))

    log(f"Final chunk {chunk_num} saved!")

os.remove(local_csv)
log(f"DONE! {counter} | Found: {found} | {(time.time() - start_time) / 3600:.1f}h")