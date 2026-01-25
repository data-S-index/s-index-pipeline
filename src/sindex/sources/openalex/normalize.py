# pipeline/external/openalex/normalize.py

from __future__ import annotations

from typing import Dict, List

from sindex.core.dates import _norm_date_iso, get_realistic_date
from sindex.core.ids import _norm_doi_url
from sindex.metrics.dedup import dedupe_citations_by_link
from sindex.metrics.weights import citation_weight


def openalex_citing_works_to_citations(
    citing_records: List[dict],
    *,
    dataset_id: str,
    dataset_pub_date: str | None,
) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []

    for c in citing_records:
        doi_raw = c.get("doi") or ""
        citation_link = _norm_doi_url(doi_raw) or (c.get("id") or "")
        if not citation_link:
            continue

        citation_date = None

        # Try OpenAlex publication date
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

        rec: Dict[str, object] = {
            "dataset_id": dataset_id,
            "source": ["openalex"],
            "citation_link": citation_link,
            "citation_weight": citation_weight(dataset_pub_date, citation_date),
        }
        if citation_date:
            rec["citation_date"] = citation_date

        out.append(rec)

    return dedupe_citations_by_link(out)
