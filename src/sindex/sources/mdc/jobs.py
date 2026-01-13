from typing import Any, Dict, List
from .constants import DEFAULT_MDC_PATTERN, DEFAULT_DB_PATH
from .client import make_duckdb_conn, register_mdc_udfs
from .discovery import list_mdc_files, mdc_glob
import datetime
#from sindex.metrics.dedup import dedupe_citations_by_link
#from sindex.metrics.weights import citation_weight
from sindex.core.ids import _norm_dataset_id
import math
from sindex.core.dates import _to_datetime_utc, _years_between


# --- TO DELETE
def citation_weight(ds_dt: str, citation_dt: str) -> float:
    a = 0.33
    ds_dt = _to_datetime_utc(ds_dt)
    citation_dt = _to_datetime_utc(citation_dt)
    if ds_dt is None or citation_dt is None:
        delta_years = 0.0
    else:
        delta_years = _years_between(ds_dt, citation_dt)

    weight = 1.0 + a * math.log(1.0 + delta_years)
    return round(weight, 2)

def dedupe_citations_by_link(citations: List[Dict]) -> List[Dict]:
    """
    Deduplicate citation objects by ``citation_link``.

    This function is used after we get citations for a given dataset from a given source
    (like MDC, Open Alex, or DataCite) to make sure we don't have duplicated citations
    for the dataset from that source (e.g. MDC seems to have duplicated citation records).

    We only deduplicate by "citation_link" because the input citations are for one dataset/doi

    Upstream normalization guarantees for the input:
      - If a citation has a date, it is already normalized using ``_norm_date_iso``.
      - If a citation has *no* date, the key ``"citation_date"`` is simply absent.

    This is how duplication is managed:
      - For a given ``citation_link``, if *none* of the duplicates have a date,
        all entries are treated as equivalent and the *first* occurrence is kept.
      - If exactly one entry has a date, that entry is preferred.
      - If multiple duplicates have dates, the entry with the *earliest*
        ``citation_date`` is kept.

    Args:
        citations:
            A list of citation dictionaries. Each must contain a
            ``"citation_link"`` key and may contain a normalized
            ``"citation_date"`` key, and other keys.

    Returns:
        A list of deduplicated citation dictionaries, preserving the order of
        first appearance of each unique ``citation_link``.
    """
    grouped: Dict[str, Dict] = {}
    order: List[str] = []

    for c in citations:
        link = c.get("citation_link")
        if not isinstance(link, str):
            continue

        # First occurrence of this link, just save the citation dict
        if link not in grouped:
            grouped[link] = c
            order.append(link)
            continue

        # If not first occurence compare with existing one
        existing = grouped[link]

        date_existing_str = existing.get("citation_date")
        date_new_str = c.get("citation_date")

        # Case 1: existing has no date, new has --> keep new
        if date_existing_str is None and date_new_str is not None:
            grouped[link] = c
            continue

        # Case 2: existing has date, new does not --> keep existing
        if date_existing_str is not None and date_new_str is None:
            continue

        # Case 3: both have no date --> keep existing
        if date_existing_str is None and date_new_str is None:
            continue

        # Case 4: both have normalized ISO dates --> keep new if it has earlier citation date
        if datetime.fromisoformat(date_new_str) < datetime.fromisoformat(
            date_existing_str
        ):
            grouped[link] = c

    return [grouped[i] for i in order]


def build_mdc_index(
    mdc_folder: str,
    *,
    pattern: str = DEFAULT_MDC_PATTERN,
    db_path: str = DEFAULT_DB_PATH,
) -> str:
    files = list_mdc_files(mdc_folder, pattern)
    if not files:
        raise FileNotFoundError(f"No MDC files found in {mdc_folder} matching {pattern}")

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
    con.execute("CREATE INDEX IF NOT EXISTS idx_mdc_dataset_norm ON mdc_index(dataset_norm)")
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



