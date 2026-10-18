# Sindex Data Collection and Processing Pipelines

**Pipeline code used to collect data from various sources (DataCite, OpenAlex, MDC, etc.) and compute S-index**

[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]

[stars-shield]: https://img.shields.io/github/stars/data-S-index/s-index-pipeline.svg?style=flat-square
[stars-url]: https://github.com/data-S-index/s-index-pipeline/stargazers
[issues-shield]: https://img.shields.io/github/issues/data-S-index/s-index-pipeline.svg?style=flat-square
[issues-url]: https://github.com/data-S-index/s-index-pipeline/issues
[license-shield]: https://img.shields.io/github/license/data-S-index/s-index-pipeline.svg?style=flat-square
[license-url]: https://github.com/data-S-index/s-index-pipeline/blob/master/LICENSE

## ℹ️ About

This repository contains the Python-based code for collecting dataset metadata, calculating FAIR scores, identifying citations, extracting mentions, and computing Dataset Index and S-index. This repository houses the `sindex` source package along with testing and production workflows. We use this package to collect data for the Scholar Data database. More details are provided in the [Data Collection](https://docs.scholardata.io/data-collection/overview) section of the Scholar Data docs.

## 📂 Repository Organization

```text
├── .github/workflows/      # Scheduled GitHub Actions for the automation scripts
├── automation/             # Plain Python scripts for scheduled (e.g., daily) harvesting jobs
├── input/                  # Data needed to run some of the pipelines (e.g., MDC data)
├── output/                 # Processed data outputs organized by source
├── notebooks-batch/        # Notebook to run pipelines for actual data harvesting & processing
├── notebooks-test/         # Sandbox notebooks for unit testing functions
├── src/                    # The source code package (sindex)
│   └── sindex/
│       ├── config/         # Configuration and settings
│       ├── core/           # Core utilities (HTTP clients, I/O, ID parsing)
│       ├── enrich/         # Data enrichment modules
│       ├── metrics/        # Logic for compiling data to compute Dataset Index and S-index
│       ├── sources/        # API clients and other code to collect and process data from external sources
│       └── utils/          # Helper functions
├── environment.yml         # Conda environment definition
├── pyproject.toml          # Build system and dependency configuration
├── requirements.txt        # pip dependencies (mirrors environment.yml)
└── README.md               # Documentation
```

## 🛠️ Installation & Setup

We recommend using Anaconda to create and manage your development environment and using JupyterLab to run the notebook. All the subsequent instructions are provided assuming you are using [Anaconda (Python 3 version)](https://www.anaconda.com/products/individual) and JupyterLab.

### 1. Clone the repository

```bash
git clone https://github.com/data-S-index/s-index-pipeline.git
cd s-index-pipeline
```

### 2. Create and activate the environment

Install mise by following the [mise installation guide](https://mise.jdx.dev/getting-started.html).

```bash
# Install Python 3.12 and uv (as specified in mise.toml)
mise install

# Activate the environment
uv venv

# Install project dependencies
uv pip install -r requirements.txt
```

## 🚀 Usage

To run production jobs (large scale), navigate to the **`notebooks-batch/`** directory. E.g., run notebooks like `datacite_harvest.ipynb` or `emdb_harvest.ipynb` to harvest dataset metadata from DataCite and EMDB respectively..

### Automation scripts

The **`automation/`** directory contains plain Python scripts for incremental jobs that run on a schedule in GitHub Actions (or anywhere else). They keep nothing on local disk between runs: harvested files live in the `scholar-data-22f7` bunny.net storage zone, and what has been loaded lives in the database's `IngestLog` table. Settings for all scripts live in [`automation/config.yaml`](automation/config.yaml). Run the scripts from the repository root after installing [`automation/requirements.txt`](automation/requirements.txt) (or the full `requirements.txt`).

Secrets are read from `.env` or the environment (see [`.env.example`](.env.example)):

| Variable                 | Used by                     | Description                                                            |
| :----------------------- | :-------------------------- | :--------------------------------------------------------------------- |
| `BUNNY_STORAGE_PASSWORD` | both scripts                | Storage zone password (zone → FTP & API Access in bunny.net)           |
| `DATABASE_URL`           | `datacite_load.py`          | Postgres connection string                                             |
| `SINDEX_USER_AGENT`      | `datacite_harvest.py` (opt) | e.g. `"your-name (mailto:you@example.org)"` for DataCite's polite pool |

The DataCite automation is split into two jobs that only meet in the storage zone:

```text
datacite_harvest.py:  DataCite ──► raw + slim files ──► storage zone (+ day manifest)
datacite_load.py:     storage zone ──► Postgres (+ IngestLog row, same transaction)
```

#### DataCite harvest (`datacite_harvest.py`)

Script version of `notebooks-batch/datacite_harvest.ipynb` that harvests new DataCite datasets one day at a time. For each day it harvests the raw records of Datasets created that day, slims them, and uploads to the storage zone:

```text
datacite/raw/YYYY/datacite-YYYY-MM-DD.ndjson.gz      raw records
datacite/slim/YYYY/datacite-YYYY-MM-DD.ndjson.gz     slim records
datacite/manifests/YYYY/datacite-YYYY-MM-DD.json     record counts, file sizes and SHA-256 checksums
```

The manifest is uploaded last, so **a day is done if and only if its manifest exists**. A day without one (never run, failed or interrupted) is harvested again from scratch.

```bash
# Harvest the oldest day from START_DATE up to a week ago (UTC) that isn't done yet
python automation/datacite_harvest.py

# Catch up on several days in one run (0 = all of them)
python automation/datacite_harvest.py --max-days 0

# Harvest a specific day, or a range one day at a time (inclusive)
python automation/datacite_harvest.py --date 2026-10-15
python automation/datacite_harvest.py --start 2026-10-01 --end 2026-10-15

# Show which days are done or pending
python automation/datacite_harvest.py --status

# Re-harvest days that are already done
python automation/datacite_harvest.py --date 2026-10-15 --force
```

#### DataCite load (`datacite_load.py`)

Loads every harvested day that has no `IngestLog` row yet into `Dataset`, `DatasetAuthor` and `DatasetIdentifier`. It downloads the day's slim file, checks it against the manifest's checksum, inserts the records in batches and writes the day's `IngestLog` row (record, inserted, skipped and failed counts) **in the same transaction**, so a day is either fully loaded and logged or not at all. Records already in `Dataset` are skipped; records the database rejects are counted as failed without undoing the rest of the day.

```bash
# Load every harvested day that isn't loaded yet
python automation/datacite_load.py

# Load a specific day or range; show harvested/loaded status per day
python automation/datacite_load.py --date 2026-10-15
python automation/datacite_load.py --status

# Load days again that are already loaded (e.g. after re-harvesting them)
python automation/datacite_load.py --date 2026-10-15 --force
```

Both scripts work on the days from `START_DATE` (hardcoded in [`common.py`](automation/common.py), currently 2026-05-01) up to `END_DELAY_DAYS` (7) days before today (UTC). A harvested day is never revisited, so the week's delay lets records registered late, and today's still-incomplete records, be included. Each run processes **one day**, oldest first (override with `--max-days`, `0` = no limit), and keeps going past a failed day, exiting with an error at the end.

#### Configuration (`config.yaml`)

| Section            | Key             | Description                                    |
| :----------------- | :-------------- | :--------------------------------------------- |
| `storage`          | `zone`          | bunny.net storage zone name                    |
| `storage`          | `host`          | Storage API endpoint of the zone's main region |
| `datacite_harvest` | `page_size`     | DataCite API page size (max 1000)              |
| `datacite_harvest` | `sleep_seconds` | Pause between days                             |
| `datacite_load`    | `batch_size`    | Records inserted per batch                     |

#### GitHub Actions

- [`datacite-harvest.yml`](.github/workflows/datacite-harvest.yml) runs the harvest twice a day (03:17 and 15:17 UTC), one day per run, so a backlog shrinks by one day a day; to catch up faster, run it once with `--max-days 0`. Needs the `BUNNY_STORAGE_PASSWORD` secret and, optionally, the `SINDEX_USER_AGENT` variable.
- [`datacite-load.yml`](.github/workflows/datacite-load.yml) runs the load (one day per run) after every harvest run, plus daily at 09:47 UTC as a fallback. It reaches the database over Tailscale as an ephemeral `tag:ci` device. Needs the `BUNNY_STORAGE_PASSWORD`, `DATABASE_URL` (using the database's Tailscale address), `TAILSCALE_OAUTH_CLIENT_ID` and `TAILSCALE_OAUTH_SECRET` secrets.

Both can be started by hand from the Actions tab with a date, range, day limit or `force`. Scheduled and `workflow_run` triggers only fire for workflows on the default branch.

## 🔌 Sources Used

| Name                        | Use                            | Description                                                                                                                                                                      | Requirements                                                                  |
| :-------------------------- | :----------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------- |
| **DataCite**                | Metadata Harvest and Citations | Using the [DataCite API](https://support.datacite.org/docs/api) to harvest metadata of datasets with DOIs                                                                        | Email (for polite pool)                                                       |
| **EMDB**                    | Metadata Harvest               | Using the [Electron Microscopy Data Bank (EMDB) API](https://www.ebi.ac.uk/emdb/api/) to harvest the metadata of their datasets as an example of datasets without DOIs           | Email (for polite pool)                                                       |
| **F-UJI**                   | FAIR scoring                   | Using [F-UJI](https://github.com/pangaea-data-publisher/fuji) to calculate FAIR scores of datasets                                                                               | Follow F-UJI installation instructions                                        |
| **Make Data Count (MDC)**   | Citations                      | Using the [MDC Dataset Citation Corpus](https://doi.org/10.5281/zenodo.16901115) to find citations to datasets                                                                   | Download the JSON format files of the corpus to run the MDC-related pipelines |
| **OpenAlex**                | Citations and Date Enrichment  | Using the [OpenAlex snapshot](https://docs.openalex.org/download-all-data/openalex-snapshot) to identify citations and publication dates                                         | Download the snapshot to run OpenAlex related pipelines                       |
| **Crossref**                | Date Enrichment                | Using [Crossref API](https://www.crossref.org/documentation/retrieve-metadata/rest-api/) to find publication dates of citing sources                                             | Email (for polite pool)                                                       |
| **Software Heritage (SWH)** | Mentions                       | Using [Software Heritage Graph Dataset](https://docs.softwareheritage.org/devel/swh-export/graph/) to find mentions of datasets in GitHub READMEs                                | Follow SWH instruction to use Graph Dataset                                   |
| **HuggingFace**             | Mentions                       | Using [Hugging Face API](https://huggingface.co/docs/hub/en/api) to find mentions of datasets in model cards                                                                     | [User Access Token](https://huggingface.co/settings/tokens)                   |
| **USPTO**                   | Mentions                       | Using [United States Patent and Trademark Office (USPTO) bulk data of granted patents](https://data.uspto.gov/bulkdata/datasets/ptgrxml) to find mentions of datasets in patents | [API key](https://developer.uspto.gov/faq/tsdr-api-bulk-download)             |
| **GitHub**                  | Mentions                       | Using [GitHub API](https://docs.github.com/en/rest) for finding mentions in README (single dataset queries)                                                                      | [Personal Access Token](https://github.com/settings/tokens)                   |

## 📜 License

This work is licensed under
[MIT](https://opensource.org/licenses/mit). See [LICENSE](LICENSE) for more information.

## 🤝 Feedback and contribution

Use the [GitHub issues](https://github.com/data-S-index/s-index-pipeline/issues) for submitting feedback or making suggestions. You can also work the repository and submit a pull request with suggestions.
