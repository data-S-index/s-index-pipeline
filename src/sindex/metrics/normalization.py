# src/sindex/metrics/normalization.py
from __future__ import annotations

import duckdb

FALLBACK_TOPIC_ID = "ALL"
UNKNOWN_YEAR = -1


def _fetch_norm_row(
    con: duckdb.DuckDBPyConnection,
    *,
    table: str,
    topic_id: str,
    year: int,
) -> tuple[str, int, float, float, float] | None:
    return con.execute(
        f"""
        SELECT topic_id, year, ft_median, ctw_median, mtw_median
        FROM {table}
        WHERE topic_id = ? AND year = ?
        LIMIT 1
        """,
        [topic_id, int(year)],
    ).fetchone()


def _clamp_year_to_available_range(
    con: duckdb.DuckDBPyConnection,
    *,
    table: str,
    topic_id: str | None,
    year: int,
    fallback_topic_id: str = FALLBACK_TOPIC_ID,
) -> tuple[int, bool]:
    """
    Clamp year to nearest available year in the normalization table.
    Only considers real years (>= 0).
    """
    if topic_id:
        row = con.execute(
            f"""
            SELECT MIN(year), MAX(year)
            FROM {table}
            WHERE topic_id IN (?, ?) AND year >= 0
            """,
            [topic_id, fallback_topic_id],
        ).fetchone()
    else:
        row = con.execute(
            f"""
            SELECT MIN(year), MAX(year)
            FROM {table}
            WHERE topic_id = ? AND year >= 0
            """,
            [fallback_topic_id],
        ).fetchone()

    if not row or row[0] is None or row[1] is None:
        return year, False

    min_y, max_y = int(row[0]), int(row[1])

    if year < min_y:
        return min_y, True
    if year > max_y:
        return max_y, True
    return year, False


def get_topic_year_norm_factors(
    con: duckdb.DuckDBPyConnection,
    *,
    topic_id: str | None = None,
    year: int | None = None,
    table: str = "topic_norm_factors_mock",
    fallback_topic_id: str = FALLBACK_TOPIC_ID,
    unknown_year: int = UNKNOWN_YEAR,
    clamp_out_of_range_year: bool = True,
) -> dict:
    """
    Return normalization factors FT, CwT, MwT as a dict.

    Fallback order:
      1) (topic_id, year_used)
      2) (ALL, year_used)
      3) (topic_id, UNKNOWN_YEAR)
      4) (ALL, UNKNOWN_YEAR)

    Returns a dict with audit fields included.
    """
    # Resolve year
    used_year_clamp = False
    if year is None:
        year_used = int(unknown_year)
    else:
        year_requested = int(year)
        if clamp_out_of_range_year:
            year_used, used_year_clamp = _clamp_year_to_available_range(
                con,
                table=table,
                topic_id=topic_id,
                year=year_requested,
                fallback_topic_id=fallback_topic_id,
            )
        else:
            year_used = year_requested

    # topic + year_used
    if topic_id:
        row = _fetch_norm_row(con, table=table, topic_id=topic_id, year=year_used)
        if row:
            tid, yy, ft, ctw, mtw = row
            return {
                "FT": float(ft),
                "CTw": float(ctw),
                "MTw": float(mtw),
                "topic_id_used": tid,
                "year_used": yy,
                "topic_id_requested": topic_id,
                "year_requested": year,
                "used_year_clamp": used_year_clamp,
            }

    # ALL + year_used
    row = _fetch_norm_row(con, table=table, topic_id=fallback_topic_id, year=year_used)
    if row:
        tid, yy, ft, ctw, mtw = row
        return {
            "FT": float(ft),
            "CTw": float(ctw),
            "MTw": float(mtw),
            "topic_id_used": tid,
            "year_used": yy,
            "topic_id_requested": topic_id,
            "year_requested": year,
            "used_year_clamp": used_year_clamp,
        }

    # UNKNOWN_YEAR fallbacks (only if year was provided)
    if year is not None:
        if topic_id:
            row = _fetch_norm_row(
                con, table=table, topic_id=topic_id, year=int(unknown_year)
            )
            if row:
                tid, yy, ft, ctw, mtw = row
                return {
                    "FT": float(ft),
                    "CTw": float(ctw),
                    "MTw": float(mtw),
                    "topic_id_used": tid,
                    "year_used": yy,
                    "topic_id_requested": topic_id,
                    "year_requested": year,
                    "used_year_clamp": used_year_clamp,
                }

        row = _fetch_norm_row(
            con, table=table, topic_id=fallback_topic_id, year=int(unknown_year)
        )
        if row:
            tid, yy, ft, ctw, mtw = row
            return {
                "FT": float(ft),
                "CTw": float(ctw),
                "MTw": float(mtw),
                "topic_id_used": tid,
                "year_used": yy,
                "topic_id_requested": topic_id,
                "year_requested": year,
                "used_year_clamp": used_year_clamp,
            }

    raise KeyError(
        f"No normalization factors found in {table} for "
        f"topic={topic_id!r}, year={year!r}"
    )


def get_subfield_year_norm_factors(db_path, subfield_id, pubyear):
    """
    Returns a dictionary of normalization factors for a given subfield_id (str) and pubyear (int)
    """
    query = """
    SELECT 
        -- 1. Scores (Prioritize Specific 'ns', fallback to 'ns_def')
        COALESCE(ns.median_fair_score_3yr, ns_def.median_fair_score_3yr) as FT,
        COALESCE(ns.median_cit_weight_3yr, ns_def.median_cit_weight_3yr) as CTw,
        COALESCE(ns.median_men_weight_3yr, ns_def.median_men_weight_3yr) as MTw,

        -- 2. Year Gap (Calculated exactly like: m.pubyear - ns.pubyear)
        -- Returns None (NULL) if we fell back to Default
        (? - ns.pubyear) as n_year_gap,

        -- 3. Method String
        CASE 
            WHEN ns.subfield_id IS NULL THEN 'Default'
            WHEN ? = ns.pubyear THEN 'Exact Year'
            ELSE 'Closest Past Year'
        END as method

    FROM (SELECT 1) -- Dummy anchor
    
    -- Simulate the ASOF JOIN (Specific Subfield)
    LEFT JOIN (
        SELECT * FROM normalization_factors_subfields_floored 
        WHERE subfield_id = ? 
          AND pubyear <= ? 
        ORDER BY pubyear DESC 
        LIMIT 1
    ) ns ON true
    
    -- Join the Default Fallback
    LEFT JOIN (
        SELECT * FROM normalization_factors_subfields_floored 
        WHERE subfield_id = 'DEFAULT'
    ) ns_def ON true
    """

    params = [pubyear, pubyear, subfield_id, pubyear]

    with duckdb.connect(db_path) as con:
        result = con.execute(query, params).fetchone()

        if not result:
            return None

        return {
            "FT": result[0],
            "CTw": result[1],
            "MTw": result[2],
            "n_year_gap": result[3],
            "method": result[4],
        }
