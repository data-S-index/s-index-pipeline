import duckdb
import orjson

from sindex.core.dates import (
    _DEFAULT_CIT_MEN_DATE,
    _DEFAULT_CIT_MEN_YEAR,
    _norm_date_iso,
    get_realistic_date,
    is_realistic_integer_year,
)
from sindex.core.ids import _norm_dataset_id
from sindex.metrics.weights import mention_weight_year


def enrich_swh_base_mentions(db_path, input_ndjson, output_ndjson):
    con = duckdb.connect(db_path)
    con.create_function("py_norm_id", _norm_dataset_id, return_type="VARCHAR")

    # SQL Logic:
    # 1. Read the input file directly.
    # 2. Extract the raw year from mention_date (for speed).
    # 3. Join with 'my_dataset' to get the missing 'pub_year'.
    query = f"""
        SELECT 
            r.dataset_id,
            d.pubyear,
            r.mention_link,
            r.mention_date,
            r.source,  -- Pass the source through (e.g. ["swh"])
            try_cast(year(try_cast(r.mention_date AS TIMESTAMP)) AS INTEGER) as raw_year
        FROM read_ndjson_auto('{input_ndjson}') r
        INNER JOIN my_datasets d
            -- Normalize the input ID just in case to ensure it matches your DB keys
            ON d.dataset_id = py_norm_id(r.dataset_id)
    """

    total_lines_saved = 0

    try:
        results = con.execute(query)
        print(f"Enriching records from {input_ndjson}...")

        with open(output_ndjson, "wb") as f:
            while True:
                rows = results.fetchmany(10000)
                if not rows:
                    break

                for row in rows:
                    # Unpack the columns we selected above
                    dataset_id, pubyear, m_link, m_date_raw, m_source, m_year_raw = row

                    # --- Date & Year Logic ---
                    mention_date = None
                    if m_date_raw:
                        try:
                            norm_iso_date = _norm_date_iso(str(m_date_raw))
                            mention_date = get_realistic_date(norm_iso_date)
                        except (ValueError, TypeError):
                            mention_date = None

                    mention_year = None
                    if is_realistic_integer_year(m_year_raw):
                        mention_year = m_year_raw

                    # --- Construct Final Record ---
                    rec = {
                        "dataset_id": dataset_id,
                        "source": m_source,  # dynamic from input
                        "mention_link": m_link,
                        "mention_weight": mention_weight_year(pubyear, mention_year),
                    }

                    # Flag Logic
                    if mention_date:
                        rec["mention_date"] = mention_date
                        rec["placeholder_date"] = False
                    else:
                        rec["mention_date"] = _DEFAULT_CIT_MEN_DATE
                        rec["placeholder_date"] = True

                    if mention_year:
                        rec["mention_year"] = mention_year
                        rec["placeholder_year"] = False
                    else:
                        rec["mention_year"] = _DEFAULT_CIT_MEN_YEAR
                        rec["placeholder_year"] = True

                    f.write(orjson.dumps(rec) + b"\n")
                    total_lines_saved += 1

        print(f"Done! Enriched {total_lines_saved} lines to {output_ndjson}")

    finally:
        con.close()

    return total_lines_saved


def load_metadata_map(filepath):
    """
    Reads metadata file and returns a dict: { 'EMD-XXXX': publication_year }
    """
    meta_map = {}
    print(f"Loading metadata from {filepath}...")

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                rec = json.loads(line)

                # Extract Publication Year
                pub_year = rec.get("pubyear")

                # Extract all EMDB IDs associated with this record
                identifiers = rec.get("identifiers", [])
                for ident in identifiers:
                    if ident.get("identifier_type") == "emdb_id":
                        emdb_id = ident.get("identifier")
                        if emdb_id:
                            meta_map[emdb_id] = pub_year

            except json.JSONDecodeError:
                continue

    print(f"Loaded {len(meta_map)} unique EMDB identifiers.")
    return meta_map
