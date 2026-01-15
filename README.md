# S-index Python

## About
This repo contains Python code for collecting the data necessary to compute Dataset-index and S-index.

## Repository Structure

```text
.
├── notebooks/           # Jupyter notebooks for testing src code
├── src/
│   └── sindex/          # Python code
│       ├── __init__.py
│       ├── core         # Core code for normalizing ids, dates, etc.
│       ├── sources      # Code for harvesting data from sources like DataCite, OpenAlex, GitHub
        ```
├── pyproject.toml       # Project configuration 
└── README.md
```


## Installation & Setup

We recommend using Anaconda to create and manage your development environment and using JupyterLab to run the notebooks. All the subsequent instructions are provided assuming you are using [Anaconda (Python 3 version)](https://www.anaconda.com/products/individual) and JupyterLab.

### 1. Clone the repository

```bash
git clone sindex-pipeline
cd sindex-pipeline
```

### 2. Download Make Data Count (MDC) DB

Donwload the DuckDB database created from the MDC corpus [here](https://drive.google.com/file/d/1UOLUw24OL0XPlfYp9SYpa-GAKJUIcRVe/view?usp=sharing) and add it under `input/mdc/`.


### 3. Create and activate conda environment

```sh
$ conda env create -f environment.yml
```

### 4. Install the project in editable mode

```bash
pip install -e .
```

### 5. Setup kernell for Jupyter lab to run the Jupyter notebook
```sh
$ conda activate sindex
$ conda install ipykernel
$ ipython kernel install --user --name=sindex
$ conda deactivate
```

