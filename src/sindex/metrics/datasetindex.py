from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sindex.core.dates import _dt_utc_or_today, _to_datetime_utc


def dataset_index(
    Fi: float,
    Ciw: float,
    Miw: float,
    *,
    FT: float = 0.5,
    CTw: float = 1.0,
    MTw: float = 1.0,
) -> float:
    """
    Compute the dataset index:

        (1/3) * (Fi / FT + Ciw / CTw + Miw / MTw)

    Threshold defaults:
      FT=0.5, CTw=1.0, MTw=1.0

    Args:
        Fi: FAIRness score for the dataset, normalized to [0, 1]
        FT: FAIRness threshold (field-weighted, median in [0, 1])
        Ciw: Cumulative weighted citation count
        CTw: Citation threshold (field-weighted)
        Miw: Cumulative weighted alternative mentions count
        MTw: Mentions threshold (field-weighted)

    Returns:
        Dataset index score (float)

    If any threshold is missing or <= 0, it is replaced with its default
    (to avoid divide-by-zero and invalid normalization).
    """
    # Validate primary inputs
    if not (0.0 <= Fi <= 1.0):
        raise ValueError(f"Fi must be normalized to [0, 1]; got {Fi}")

    if Ciw < 0 or Miw < 0:
        raise ValueError("Ciw and Miw must be >= 0")

    # Apply safe defaults for thresholds
    FT = FT if FT and FT > 0 else 0.5
    CTw = CTw if CTw and CTw > 0 else 1.0
    MTw = MTw if MTw and MTw > 0 else 1.0

    return ((Fi / FT) + (Ciw / CTw) + (Miw / MTw)) / 3.0


def dataset_index_timeseries(
    *,
    Fi: float,
    citations: list[dict[str, Any]] | None = None,
    mentions: list[dict[str, Any]] | None = None,
    pubdate: str | None = None,
    FT: float = 0.5,
    CTw: float = 1.0,
    MTw: float = 1.0,
    citation_date_key: str = "citation_date",
    citation_weight_key: str = "citation_weight",
    mention_date_key: str = "mention_date",
    mention_weight_key: str = "mention_weight",
) -> list[dict[str, Any]]:
    """
    Output: [{"date": <iso>, "dataset_index": <float>}, ...]

    - First point at pubdate (if provided).
    - Next points at each unique event date.
    - Missing/invalid event date -> today's date (day of computation, 00:00 UTC).
    """
    citations = citations or []
    mentions = mentions or []

    # today's date at 00:00 UTC (stable for the run)
    now_utc = datetime.now(timezone.utc)
    today_dt = datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc)

    # Normalize events
    events: list[tuple[datetime, str, float]] = []  # (dt, type, weight)

    for c in citations:
        dt = _dt_utc_or_today(c.get(citation_date_key), today_dt=today_dt)
        w = float(c.get(citation_weight_key, 0.0) or 0.0)
        events.append((dt, "citation", w))

    for m in mentions:
        dt = _dt_utc_or_today(m.get(mention_date_key), today_dt=today_dt)
        w = float(m.get(mention_weight_key, 0.0) or 0.0)
        events.append((dt, "mention", w))

    events.sort(key=lambda t: t[0])

    # Evaluation dates
    eval_dates: list[datetime] = []
    seen: set[datetime] = set()

    pub_dt = (
        _to_datetime_utc(pubdate) if pubdate else None
    )  # <-- your existing function
    if pub_dt is not None:
        eval_dates.append(pub_dt)
        seen.add(pub_dt)

    for dt, _, _ in events:
        if dt not in seen:
            eval_dates.append(dt)
            seen.add(dt)

    # Ensure pubdate is first point if provided; rest sorted ascending
    if pub_dt is not None:
        rest = sorted([d for d in eval_dates if d != pub_dt])
        eval_dates = [pub_dt] + rest
    else:
        eval_dates = sorted(eval_dates)

    if not eval_dates:
        eval_dates = [today_dt]

    # Compute cumulative sums and series
    out: list[dict[str, Any]] = []
    ciw = 0.0
    miw = 0.0
    i = 0

    for dt in eval_dates:
        while i < len(events) and events[i][0] <= dt:
            _, typ, w = events[i]
            if typ == "citation":
                ciw += w
            else:
                miw += w
            i += 1

        idx = dataset_index(Fi=Fi, FT=FT, Ciw=ciw, CTw=CTw, Miw=miw, MTw=MTw)
        out.append(
            {
                "date": dt.isoformat().replace("+00:00", "Z"),
                "dataset_index": idx,
            }
        )

    return out
