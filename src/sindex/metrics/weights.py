from __future__ import annotations

import math

from sindex.core.dates import _to_datetime_utc, _years_between


def mention_weight(dataset_pub_date: str, mention_date: str) -> float:
    a = 0.33
    ds_dt = _to_datetime_utc(dataset_pub_date)
    m_dt = _to_datetime_utc(mention_date)
    if ds_dt is None or m_dt is None:
        delta_years = 0.0
    else:
        delta_years = _years_between(ds_dt, m_dt)

    weight = 1.0 + a * math.log(1.0 + delta_years)
    return round(weight, 2)


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
