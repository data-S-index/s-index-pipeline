import duckdb

from sindex.core.dates import norm_date_iso_db
from sindex.core.ids import norm_dataset_id_db, norm_doi_url_or_raw

from .constants import DEFAULT_THREADS, ENABLE_PROGRESS


def make_duckdb_conn(
    db_path: str, *, read_only: bool = False
) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path, read_only=read_only)
    con.execute(f"PRAGMA threads={DEFAULT_THREADS}")
    if ENABLE_PROGRESS and not read_only:
        con.execute("PRAGMA enable_progress_bar=true")
    return con


def register_mdc_udfs(con: duckdb.DuckDBPyConnection) -> None:
    con.create_function("norm_dataset_id", norm_dataset_id_db, ["VARCHAR"], "VARCHAR")
    con.create_function(
        "norm_doi_url_or_raw", norm_doi_url_or_raw, ["VARCHAR"], "VARCHAR"
    )
    con.create_function("norm_date_iso_safe", norm_date_iso_db, ["VARCHAR"], "VARCHAR")
