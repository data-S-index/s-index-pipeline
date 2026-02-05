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


def find_mentions_hf_modelcard_refs(db_path, input_ndjson, output_ndjson):
    con = duckdb.connect(db_path)

    con.create_function("py_norm_id", _norm_dataset_id, return_type="VARCHAR")

    query = f"""
        WITH raw_refs AS (
            SELECT 
                py_norm_id(ref.id) AS norm_id,
                url AS mention_link,
                created_at,
                try_cast(year(try_cast(created_at AS TIMESTAMP)) AS INTEGER) as raw_year
            FROM read_ndjson_auto('{input_ndjson}')
            CROSS JOIN UNNEST("references") AS t(ref)
            WHERE ref.id IS NOT NULL 
        )
        SELECT 
            r.norm_id AS dataset_id,
            d.pubyear AS pubyear,
            r.mention_link AS m_link,
            r.raw_year AS m_year_raw,
            r.created_at AS m_date_raw
        FROM raw_refs r
        INNER JOIN my_datasets d
            ON d.dataset_id = r.norm_id
    """

    total_lines_saved = 0

    try:
        results = con.execute(query)
        print(f"Processing matches from {input_ndjson}...")

        with open(output_ndjson, "wb") as f:
            while True:
                rows = results.fetchmany(10000)
                if not rows:
                    break

                for row in rows:
                    dataset_id, pubyear, m_link, m_year_raw, m_date_raw = row

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

                    rec = {
                        "dataset_id": dataset_id,
                        "source": ["hf"],
                        "mention_link": m_link,
                        "mention_weight": mention_weight_year(pubyear, mention_year),
                    }

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
                    total_lines_saved += 1  # <--- Increment Counter

        print(f"Done! Saved {total_lines_saved} lines to {output_ndjson}")

    finally:
        con.close()

    return total_lines_saved
