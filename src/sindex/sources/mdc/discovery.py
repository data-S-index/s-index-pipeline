import glob
import os


def list_mdc_files(folder: str, pattern: str) -> list[str]:
    files = sorted(glob.glob(os.path.join(folder, pattern)))
    return files


def mdc_glob(folder: str, pattern: str) -> str:
    # DuckDB likes forward slashes even on Windows
    return os.path.join(folder, pattern).replace("\\", "/")
