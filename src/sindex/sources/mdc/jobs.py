from typing import Any, Dict, List

from sindex.core.ids import _norm_dataset_id
from sindex.metrics.dedup import dedupe_citations_by_link
from sindex.metrics.weights import citation_weight

from .client import make_duckdb_conn, register_mdc_udfs
from .constants import DEFAULT_DB_PATH, DEFAULT_MDC_PATTERN
from .discovery import list_mdc_files, mdc_glob


def build_mdc_index(
    mdc_folder: str,
    *,
    pattern: str = DEFAULT_MDC_PATTERN,
    db_path: str = DEFAULT_DB_PATH,
) -> str:
    files = list_mdc_files(mdc_folder, pattern)
    if not files:
        raise FileNotFoundError(
            f"No MDC files found in {mdc_folder} matching {pattern}"
        )

    con = make_duckdb_conn(db_path, read_only=False)
    register_mdc_udfs(con)

    glob_path = mdc_glob(mdc_folder, pattern)

    con.execute("DROP TABLE IF EXISTS mdc_index")

    # 1) ingest + normalize
    con.execute(f"""
        CREATE TABLE mdc_index AS
        SELECT
            norm_dataset_id(dataset) AS dataset_norm,
            norm_doi_url_or_raw(publication) AS citation_link,
            norm_date_iso_safe(publishedDate) AS citation_date
        FROM read_json_auto('{glob_path}')
        WHERE dataset IS NOT NULL
          AND publication IS NOT NULL;
    """)

    # 2) clean failures
    con.execute("""
        DELETE FROM mdc_index
        WHERE dataset_norm IS NULL OR citation_link IS NULL OR citation_link = '';
    """)

    # 3) dedupe (dataset_norm, citation_link)
    con.execute("DROP TABLE IF EXISTS mdc_index_dedup")
    con.execute("""
        CREATE TABLE mdc_index_dedup AS
        SELECT
            dataset_norm,
            citation_link,
            any_value(citation_date) AS citation_date
        FROM mdc_index
        GROUP BY dataset_norm, citation_link;
    """)
    con.execute("DROP TABLE mdc_index")
    con.execute("ALTER TABLE mdc_index_dedup RENAME TO mdc_index")

    # 4) index for fast point lookups
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_mdc_dataset_norm ON mdc_index(dataset_norm)"
    )
    con.execute("ANALYZE mdc_index")

    con.close()
    return db_path


def find_citations_mdc_duckdb(
    target_id: str,
    *,
    dataset_pub_date: str = "",
    db_path: str = DEFAULT_DB_PATH,
) -> List[Dict[str, Any]]:
    target_norm = _norm_dataset_id(target_id)
    if not target_norm:
        return []

    con = make_duckdb_conn(db_path, read_only=True)

    rows = con.execute(
        """
        SELECT citation_link, citation_date
        FROM mdc_index
        WHERE dataset_norm = ?
        """,
        [target_norm],
    ).fetchall()
    con.close()

    out: List[Dict[str, Any]] = []
    for citation_link, citation_date in rows:
        citation_date = citation_date or ""
        rec: Dict[str, Any] = {
            "dataset_id": target_id,
            "source": ["mdc"],
            "citation_link": citation_link,
            "citation_weight": citation_weight(dataset_pub_date, citation_date),
        }
        if citation_date:
            rec["citation_date"] = citation_date
        out.append(rec)

    return dedupe_citations_by_link(out)
