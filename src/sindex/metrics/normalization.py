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
            tid, yy, ft, cwt, mwt = row
            return {
                "FT": float(ft),
                "CwT": float(cwt),
                "MwT": float(mwt),
                "topic_id_used": tid,
                "year_used": yy,
                "topic_id_requested": topic_id,
                "year_requested": year,
                "used_year_clamp": used_year_clamp,
            }

    # ALL + year_used
    row = _fetch_norm_row(con, table=table, topic_id=fallback_topic_id, year=year_used)
    if row:
        tid, yy, ft, cwt, mwt = row
        return {
            "FT": float(ft),
            "CwT": float(cwt),
            "MwT": float(mwt),
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
                tid, yy, ft, cwt, mwt = row
                return {
                    "FT": float(ft),
                    "CwT": float(cwt),
                    "MwT": float(mwt),
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
            tid, yy, ft, cwt, mwt = row
            return {
                "FT": float(ft),
                "CwT": float(cwt),
                "MwT": float(mwt),
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
