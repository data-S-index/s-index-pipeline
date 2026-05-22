from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def dataset_index(
    Fi: float,
    Ciw: float,
    Miw: float,
    *,
    FT: float = 13.46,
    CTw: float = 1.0,
    MTw: float = 1.0,
) -> float:
    """
    Compute the dataset index:

        (1/3) * (Fi / FT + Ciw / CTw + Miw / MTw)

    Threshold defaults:
      FT=0.5, CTw=1.0, MTw=1.0

    Args:
        Fi: FAIRness score for the dataset, normalized to [0, 100]
        FT: FAIRness threshold (field-weighted, median in [0, 100])
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
    if not (0.0 <= Fi <= 100.0):
        raise ValueError(f"Fi must be betwee 0% and 100%, currently {Fi}")

    if Ciw < 0 or Miw < 0:
        raise ValueError("Ciw and Miw must be >= 0")

    # Apply safe defaults for thresholds
    FT = FT if FT and FT > 0 else 13.46
    CTw = CTw if CTw and CTw > 0 else 1.0
    MTw = MTw if MTw and MTw > 0 else 1.0

    return ((Fi / FT) + (Ciw / CTw) + (Miw / MTw)) / 3.0


def dataset_index_year_timeseries(
    *,
    Fi: float,
    citations: list[dict[str, Any]] | None = None,
    mentions: list[dict[str, Any]] | None = None,
    pubyear: int | None = None,
    FT: float = 0.5,
    CTw: float = 1.0,
    MTw: float = 1.0,
    citation_year_key: str = "citation_year",
    citation_weight_key: str = "citation_weight",
    mention_year_key: str = "mention_year",
    mention_weight_key: str = "mention_weight",
) -> list[dict[str, Any]]:
    """
    Output: [{"year": <int>, "dataset_index": <float>}, ...]

    - Processes data at the year level of granularity.
    - Missing/invalid years default to the current calendar year.
    """
    citations = citations or []
    mentions = mentions or []

    # Fallback for missing years
    current_year = datetime.now(timezone.utc).year

    # 1. Normalize events to (year, type, weight)
    events: list[tuple[int, str, float]] = []

    for c in citations:
        yr = c.get(citation_year_key)
        yr = int(yr) if yr is not None else current_year
        w = float(c.get(citation_weight_key, 0.0) or 0.0)
        events.append((yr, "citation", w))

    for m in mentions:
        yr = m.get(mention_year_key)
        yr = int(yr) if yr is not None else current_year
        w = float(m.get(mention_weight_key, 0.0) or 0.0)
        events.append((yr, "mention", w))

    # Sort events by year
    events.sort(key=lambda t: t[0])

    # 2. Determine unique evaluation years
    eval_years: list[int] = []
    seen: set[int] = set()

    if pubyear is not None:
        eval_years.append(pubyear)
        seen.add(pubyear)

    for yr, _, _ in events:
        if yr not in seen:
            eval_years.append(yr)
            seen.add(yr)

    # Sort evaluation years: pubyear first, then others ascending
    if pubyear is not None:
        rest = sorted([y for y in eval_years if y != pubyear])
        eval_years = [pubyear] + rest
    else:
        eval_years = sorted(eval_years)

    if not eval_years:
        eval_years = [current_year]

    # 3. Compute cumulative index for each year
    out: list[dict[str, Any]] = []
    ciw = 0.0
    miw = 0.0
    event_idx = 0

    for yr in eval_years:
        while event_idx < len(events) and events[event_idx][0] <= yr:
            _, typ, w = events[event_idx]
            if typ == "citation":
                ciw += w
            else:
                miw += w
            event_idx += 1

        idx = dataset_index(Fi=Fi, FT=FT, Ciw=ciw, CTw=CTw, Miw=miw, MTw=MTw)

        out.append(
            {
                "year": yr,
                "dataset_index": idx,
            }
        )

    return out
