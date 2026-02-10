# pipeline/external/openalex/normalize.py

from __future__ import annotations

from typing import Dict, List

from sindex.core.dates import (
    _DEFAULT_CIT_MEN_DATE,
    _DEFAULT_CIT_MEN_YEAR,
    _norm_date_iso,
    get_realistic_date,
    is_realistic_integer_year,
)
from sindex.core.ids import _norm_doi_url
from sindex.metrics.dedup import dedupe_citations_by_link
from sindex.metrics.weights import citation_weight_year


def openalex_citing_works_to_citations(
    citing_records: List[dict],
    *,
    dataset_id: str,
    dataset_pubyear: int | None,
) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []

    for c in citing_records:
        doi_raw = c.get("doi") or ""
        citation_link = _norm_doi_url(doi_raw) or (c.get("id") or "")
        if not citation_link:
            continue

        # Try OpenAlex publication date
        citation_date = None
        pub_date_raw = c.get("publication_date")
        if pub_date_raw:
            try:
                norm_iso_date = _norm_date_iso(str(pub_date_raw))
                citation_date = get_realistic_date(norm_iso_date)
            except (ValueError, TypeError):
                citation_date = None

        # Fallback to OpenAlex publication year
        if not citation_date:
            pub_year_raw = c.get("publication_year")
            if pub_year_raw:
                try:
                    # Convert year to ISO format (e.g., "2024-01-01")
                    norm_iso_year = _norm_date_iso(str(pub_year_raw))
                    citation_date = get_realistic_date(norm_iso_year)
                except (ValueError, TypeError):
                    citation_date = None
        if citation_date:
            citation_year_raw = int(citation_date[:4])
        else:
            citation_year_raw = None
        citation_year = None
        if citation_year_raw:
            if is_realistic_integer_year(citation_year_raw):
                citation_year = citation_year_raw

        rec: Dict[str, object] = {
            "dataset_id": dataset_id,
            "source": ["openalex"],
            "citation_link": citation_link,
            "citation_weight": citation_weight_year(dataset_pubyear, citation_year),
        }

        if citation_date:
            rec["citation_date"] = citation_date
            rec["placeholder_date"] = False
        else:
            rec["citation_date"] = _DEFAULT_CIT_MEN_DATE
            rec["placeholder_date"] = True

        if citation_year:
            rec["citation_year"] = citation_year
            rec["placeholder_year"] = False
        else:
            rec["citation_year"] = _DEFAULT_CIT_MEN_YEAR
            rec["placeholder_year"] = True

        out.append(rec)

    return dedupe_citations_by_link(out)
