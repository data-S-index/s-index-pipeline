from __future__ import annotations

import math

from sindex.core.dates import _to_datetime_utc, _years_between


def citation_weight(ds_dt: str | None, citation_dt: str | None) -> float:
    a = 0.33

    ds_dt_parsed = _to_datetime_utc(ds_dt)
    citation_dt_parsed = _to_datetime_utc(citation_dt)

    if ds_dt_parsed is None or citation_dt_parsed is None:
        delta_years = 0.0
    else:
        delta_years = max(0.0, _years_between(ds_dt_parsed, citation_dt_parsed))

    weight = 1.0 + a * math.log(1.0 + delta_years)
    return round(weight, 2)


def citation_weight_year(ds_year: int | None, citation_year: int | None) -> float:
    """
    Calculates citation weight based on the integer year difference.
    Formula: weight = 1 + 0.33 * ln(1 + delta_years)
    """
    a = 0.33

    # Handle missing data or invalid chronological order
    if ds_year is None or citation_year is None or citation_year < ds_year:
        delta_years = 0
    else:
        delta_years = citation_year - ds_year

    weight = 1.0 + a * math.log(1.0 + delta_years)

    return round(weight, 2)


def mention_weight(ds_dt: str | None, mention_dt: str | None) -> float:
    a = 0.33

    ds_dt_parsed = _to_datetime_utc(ds_dt)
    mention_dt_parsed = _to_datetime_utc(mention_dt)

    if ds_dt_parsed is None or mention_dt_parsed is None:
        delta_years = 0.0
    else:
        delta_years = max(0.0, _years_between(ds_dt_parsed, mention_dt_parsed))

    weight = 1.0 + a * math.log(1.0 + delta_years)
    return round(weight, 2)


def mention_weight_year(ds_year: int | None, mention_year: int | None) -> float:
    """
    Calculates mention weight based on the integer year difference.
    Formula: weight = 1 + 0.33 * ln(1 + delta_years)
    """
    a = 0.33

    # Handle missing data or invalid chronological order
    if ds_year is None or mention_year is None or mention_year < ds_year:
        delta_years = 0
    else:
        delta_years = mention_year - ds_year

    weight = 1.0 + a * math.log(1.0 + delta_years)

    return round(weight, 2)
