from __future__ import annotations

import math


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
