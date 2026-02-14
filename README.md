# Sindex Data Collection and Processing Pipelines

**Pipelines used to collect data from various sources (DataCite, OpenAlex, MDC, etc.) and compute S-index**

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

Python-based pipelines for collecting dataset metadata, calculating FAIR scores, identifying citations, extracting mentions, and computing Dataset Index and S-index. This repository houses the `sindex` source package along with testing and production workflows. We used this code to compute Dataset Index of 49M+ datasets and the S-index of their 1M+ authors as part of our real-world testing and validation. The outputs were included in our NIH S-index Challenge Phase 2 proposal. We refer to the [S-index Hub](https://github.com/data-S-index/hub) for more information about our S-index and the Challenge. This code is under active development as we process more datasets, add more sources for citations and mentions, and so on.


## 📂 Project Organization

The repository is organized to separate the core library logic from the execution environments.

```text
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
└── README.md               # Project documentation
```

## 🛠️ Installation & Setup

We recommend using Anaconda to create and manage your development environment and using JupyterLab to run the notebook. All the subsequent instructions are provided assuming you are using [Anaconda (Python 3 version)](https://www.anaconda.com/products/individual) and JupyterLab.

### 1. Clone the repository

```bash
git clone https://github.com/data-S-index/s-index-pipeline.git
cd s-index-pipeline
```

### 2. Create and activate the environment

Create the isolated Conda environment using the provided YAML file.

```bash
conda env create -f environment.yml
conda activate s-index-pipeline
```

### 3. Install the project in "Editable" mode

This installs the `src` folder as a Python package (`sindex`), allowing you to import it in your notebooks while still being able to edit the code.

```bash
pip install -e .
```

### 4. Setup Jupyter Kernel

Register the environment so it appears as a kernel option in JupyterLab.

```bash
# Install ipykernel if not already in environment.yml
conda install ipykernel

# Register the kernel
python -m ipykernel install --user --name=s-index-pipeline

# Close
conda deactivate s-index-pipeline
```


### 5. Launch

```bash
# Launch Jupyter lab from your base environment (or any other where it is installed)
jupyter lab
```

## 🚀 Usage

To run production jobs (large scale), navigate to the **`notebooks-batch/`** directory. E.g., run notebooks like `datacite_harvest.ipynb` or `emdb_harvest.ipynb` to fetch harvest dataset metadata.

## 🔌 Sources Used

| Name | Use | Description | Requirements |
| :--- | :--- | :--- |  :--- |
| **DataCite** | Metadata Harvest and Citations| Using the [DataCite API](https://support.datacite.org/docs/api) to harvest metadata of datasets with DOIs  | Email (for polite pool) |
| **EMDB** | Metadata Harvest | Using the [Electron Microscopy Data Bank (EMDB) API](https://www.ebi.ac.uk/emdb/api/) to harvest the metadata of their datasets as an example of datasets without DOIs | Email (for polite pool)|
| **F-UJI** | FAIR scoring | Using [F-UJI](https://github.com/pangaea-data-publisher/fuji) to calculate FAIR scores of datasets | Follow F-UJI installation instructions |
| **Make Data Count (MDC)** | Citations | Using the [MDC Dataset Citation Corpus (v4.1)](https://doi.org/10.5281/zenodo.16901115) to find citations to datasets| Download the JSON format files of the corpus to run the MDC-related pipelines |
| **OpenAlex** | Citations and Date Enrichment| Using the [OpenAlex snapshot](https://docs.openalex.org/download-all-data/openalex-snapshot) to identify citations and publication dates | Download the snapshot to run OpenAlex related pipelines |
| **Crossref** | Date Enrichment | Using [Crossref API](https://www.crossref.org/documentation/retrieve-metadata/rest-api/) to find publication dates of citing sources | Email (for polite pool) |
| **Software Heritage (SWH)** | Mentions | Using [Software Heritage Graph Dataset](https://docs.softwareheritage.org/devel/swh-export/graph/) to find mentions of datasets in GitHub READMEs | Follow SWH instruction to use Graph Dataset |
| **HuggingFace** | Mentions | Using [Hugging Face API](https://huggingface.co/docs/hub/en/api) to find mentions of datasets in model cards | [User Access Token](https://huggingface.co/settings/tokens) |
| **USPTO** | Mentions | Using [United States Patent and Trademark Office (USPTO) bulk data of granted patents](https://data.uspto.gov/bulkdata/datasets/ptgrxml) to find mentions of datasets in patents | [API key](https://developer.uspto.gov/faq/tsdr-api-bulk-download) |
| **GitHub** | Mentions | Using [GitHub API](https://docs.github.com/en/rest) for finding mentions in README (single dataset queries) | [Personal Access Token](https://github.com/settings/tokens) |

## 📜 License
This work is licensed under
[MIT](https://opensource.org/licenses/mit). See [LICENSE](LICENSE) for more information.

## 🤝 Feedback and contribution
Use the [GitHub issues](https://github.com/data-S-index/s-index-pipeline/issues) for submitting feedback or making suggestions. You can also work the repository and submit a pull request with suggestions.

