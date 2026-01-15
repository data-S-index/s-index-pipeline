from __future__ import annotations

from typing import Any, Mapping

import duckdb

UNKNOWN_YEAR = -1  # sentinel year

DEFAULT_MOCK_NORM_CFG: dict[str, Any] = {
    "seed": 42,
    # Baselines at year_start (per topic)
    "ft_base": (0.35, 0.75),
    "ctw_base": (0.00, 6.00),
    "mtw_base": (0.00, 3.00),
    # Nondecreasing yearly steps: step=0 with prob p_flat else U(0, step_max)
    "ft_step_max": 0.015,
    "ctw_step_max": 0.60,
    "mtw_step_max": 0.40,
    "ft_p_flat": 0.40,
    "ctw_p_flat": 0.30,
    "mtw_p_flat": 0.30,
    # Caps (keep mock plausible)
    "ft_cap": 0.95,
    "ctw_cap": 60.0,
    "mtw_cap": 30.0,
    # Cohort sizes (also nondecreasing)
    "n_base": (30, 800),
    "n_step_max": 60,
    "n_p_flat": 0.35,
}


def load_topics_csv_to_duckdb(
    con: duckdb.DuckDBPyConnection,
    *,
    csv_path: str,
    topics_table: str = "openalex_topics",
) -> None:
    con.execute(f"DROP TABLE IF EXISTS {topics_table}")
    con.execute(
        f"""
        CREATE TABLE {topics_table} AS
        SELECT
            topic_id,
            topic_name,
            subfield_name,
            field_name,
            domain_name
        FROM read_csv_auto(?, header=True);
        """,
        [csv_path],
    )

    con.execute(
        f"ALTER TABLE {topics_table} ADD COLUMN IF NOT EXISTS topic_id_short VARCHAR;"
    )
    con.execute(
        f"""
        UPDATE {topics_table}
        SET topic_id_short =
            CASE
                WHEN topic_id LIKE 'https://openalex.org/T%%'
                    THEN regexp_extract(topic_id, 'T\\d+')
                ELSE topic_id
            END;
        """
    )


def create_mock_topic_norm_factors_table(
    con: duckdb.DuckDBPyConnection,
    *,
    topics_table: str = "openalex_topics",
    topic_id_col: str = "topic_id",  # or "topic_id_short"
    topic_name_col: str = "topic_name",
    out_table: str = "topic_norm_factors_mock",
    year_start: int = 2010,
    year_end: int = 2025,  # inclusive
    cfg: Mapping[str, Any] | None = None,
    unknown_year: int = UNKNOWN_YEAR,
) -> None:
    """
    Create mock normalization factors for (topic, year), nondecreasing over time per topic.
    Also creates (topic, UNKNOWN_YEAR) rows where UNKNOWN_YEAR is pooled median across all years.
    Includes fallback topic_id='ALL' ("All datasets").
    """
    if year_end < year_start:
        raise ValueError("year_end must be >= year_start")

    c = dict(DEFAULT_MOCK_NORM_CFG)
    if cfg:
        c.update({k: v for k, v in cfg.items() if v is not None})

    ys = int(year_start)
    ye = int(year_end)
    ye_excl = ye + 1

    con.execute("SELECT setseed(0.42)")
    con.execute(f"DROP TABLE IF EXISTS {out_table}")

    ft0_lo, ft0_hi = map(float, c["ft_base"])
    c0_lo, c0_hi = map(float, c["ctw_base"])
    m0_lo, m0_hi = map(float, c["mtw_base"])

    ft_step_max = float(c["ft_step_max"])
    c_step_max = float(c["ctw_step_max"])
    m_step_max = float(c["mtw_step_max"])

    ft_p_flat = float(c["ft_p_flat"])
    c_p_flat = float(c["ctw_p_flat"])
    m_p_flat = float(c["mtw_p_flat"])

    ft_cap = float(c["ft_cap"])
    c_cap = float(c["ctw_cap"])
    m_cap = float(c["mtw_cap"])

    n0_lo, n0_hi = c["n_base"]
    n_step_max = int(c["n_step_max"])
    n_p_flat = float(c["n_p_flat"])

    # 1) Build the per-year (topic, year) table first
    con.execute(f"""
        CREATE TABLE {out_table} AS
        WITH topics_plus AS (
            SELECT
                {topic_id_col}   AS topic_id,
                {topic_name_col} AS topic_name,
                FALSE AS is_general
            FROM {topics_table}

            UNION ALL
            SELECT
                'ALL' AS topic_id,
                'All datasets' AS topic_name,
                TRUE AS is_general
        ),
        years AS (
            SELECT year
            FROM range({ys}, {ye_excl}) AS t(year)
        ),
        baselines AS (
            SELECT
                topic_id,
                topic_name,
                is_general,

                ({ft0_lo} + ({ft0_hi} - {ft0_lo}) * random()) AS ft_base,
                ({c0_lo}  + ({c0_hi}  - {c0_lo})  * random()) AS ctw_base,
                ({m0_lo}  + ({m0_hi}  - {m0_lo})  * random()) AS mtw_base,

                CAST(FLOOR({n0_lo} + ({n0_hi} - {n0_lo} + 1) * random()) AS INTEGER) AS n_base
            FROM topics_plus
        ),
        steps AS (
            SELECT
                b.*,
                y.year,

                CASE WHEN y.year = {ys} THEN 0.0
                     WHEN random() < {ft_p_flat} THEN 0.0
                     ELSE random() * {ft_step_max} END AS ft_step,

                CASE WHEN y.year = {ys} THEN 0.0
                     WHEN random() < {c_p_flat} THEN 0.0
                     ELSE random() * {c_step_max} END AS ctw_step,

                CASE WHEN y.year = {ys} THEN 0.0
                     WHEN random() < {m_p_flat} THEN 0.0
                     ELSE random() * {m_step_max} END AS mtw_step,

                CASE WHEN y.year = {ys} THEN 0
                     WHEN random() < {n_p_flat} THEN 0
                     ELSE CAST(FLOOR(random() * ({n_step_max} + 1)) AS INTEGER) END AS n_step
            FROM baselines b
            CROSS JOIN years y
        )
        SELECT
            topic_id,
            topic_name,
            year,

            ROUND(LEAST({ft_cap},  ft_base  + SUM(ft_step)  OVER w), 3) AS ft_median,
            ROUND(LEAST({c_cap},   ctw_base + SUM(ctw_step) OVER w), 3) AS ctw_median,
            ROUND(LEAST({m_cap},   mtw_base + SUM(mtw_step) OVER w), 3) AS mtw_median,

            (n_base + SUM(n_step) OVER w)::INTEGER AS n_datasets_f,
            (n_base + SUM(n_step) OVER w)::INTEGER AS n_datasets_c,
            (n_base + SUM(n_step) OVER w)::INTEGER AS n_datasets_m,

            is_general,
            TRUE AS is_mock
        FROM steps
        WINDOW w AS (
            PARTITION BY topic_id
            ORDER BY year
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        );
    """)

    # 2) Append UNKNOWN_YEAR rows: pooled median across all years for each topic_id
    #    (and a median cohort size as well)
    con.execute(f"""
        INSERT INTO {out_table}
        SELECT
            topic_id,
            topic_name,
            {int(unknown_year)} AS year,

            median(ft_median)  AS ft_median,
            median(ctw_median) AS ctw_median,
            median(mtw_median) AS mtw_median,

            CAST(median(n_datasets_f) AS INTEGER) AS n_datasets_f,
            CAST(median(n_datasets_c) AS INTEGER) AS n_datasets_c,
            CAST(median(n_datasets_m) AS INTEGER) AS n_datasets_m,

            is_general,
            TRUE AS is_mock
        FROM {out_table}
        WHERE year BETWEEN {ys} AND {ye}
        GROUP BY topic_id, topic_name, is_general;
    """)

    con.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{out_table}_topic_year ON {out_table}(topic_id, year)"
    )


def validate_mock_table_nondecreasing(
    con: duckdb.DuckDBPyConnection, *, table: str
) -> int:
    """
    Returns the number of topics with any monotonicity violations across real years only.
    (UNKNOWN_YEAR is excluded from this check.)
    """
    return con.execute(f"""
        WITH diffs AS (
            SELECT
                topic_id,
                year,
                ft_median,
                ctw_median,
                mtw_median,
                LAG(ft_median)  OVER (PARTITION BY topic_id ORDER BY year) AS ft_prev,
                LAG(ctw_median) OVER (PARTITION BY topic_id ORDER BY year) AS c_prev,
                LAG(mtw_median) OVER (PARTITION BY topic_id ORDER BY year) AS m_prev
            FROM {table}
            WHERE year >= 0
        )
        SELECT COUNT(DISTINCT topic_id)
        FROM diffs
        WHERE
            (ft_prev IS NOT NULL AND ft_median < ft_prev)
         OR (c_prev  IS NOT NULL AND ctw_median < c_prev)
         OR (m_prev  IS NOT NULL AND mtw_median < m_prev);
    """).fetchone()[0]
