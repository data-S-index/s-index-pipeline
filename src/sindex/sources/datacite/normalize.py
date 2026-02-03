from __future__ import annotations

from typing import Any, Dict, List

from sindex.core.dates import _norm_date_iso, get_realistic_date
from sindex.core.ids import _norm_doi, _norm_doi_url
from sindex.enrich.pubdate.jobs import best_publication_date_for_doi
from sindex.metrics.dedup import dedupe_citations_by_link
from sindex.metrics.weights import citation_weight

from .utils import get_best_publication_date_datacite_record


def slim_datacite_record(metadata: dict) -> dict:
    """
    Produce a reduced version of a DataCite record.

    This function only keeps the metadata required for the S-index.
    This is intended to reduce the record's size and make records more uniform.

    This also normalizes certain values (e.g., all lowercase DOI, date format, etc.),
    and removes empty values.

    Args:
        metadata: Full DataCite metadata record (dict) including "attributes"
            and optionally "relationships" fields.

    Returns:
        A slimmed dictionary containing at minimum `"source": "datacite"` and,
        when present, keys including: doi, title, version, publisher,
        publication_date, creators, and citations (split into DOIs and
        other identifiers).

    Notes:
        - Only non-empty fields are preserved.
        - Creator objects are reduced to name, identifiers, and affiliations.
        - DOI citations are normalized to lowercase DOIs.
        - Non-DOI citations are kept as a list of objects with their IDs and types.
    """
    attr = metadata.get("attributes", {})
    out = {"source": "datacite"}

    # DOI
    out["identifiers"] = []

    doi = attr.get("doi", "")
    norm_doi = _norm_doi(doi)
    if norm_doi:
        out["identifiers"].append({"identifier": norm_doi, "identifier_type": "doi"})

    # URL
    url = attr.get("url", "")
    if url:
        out["url"] = url

    # Title
    titles = attr.get("titles", [])
    if titles and isinstance(titles, list):
        title = titles[0].get("title")
        if title:
            out["title"] = title

    # Subjects
    subjects = attr.get("subjects", [])
    if isinstance(subjects, list):
        subj_list = []
        seen = set()
        for s in subjects:
            if not isinstance(s, dict):
                continue
            val = (s.get("subject") or "").strip()
            if val and val not in seen:
                seen.add(val)
                subj_list.append(val)
        if subj_list:
            out["subjects"] = subj_list

    # Description/Abstract
    descriptions = attr.get("descriptions", [])
    if isinstance(descriptions, list):
        for d in descriptions:
            if not isinstance(d, dict):
                continue
            if (d.get("descriptionType") or "").lower() == "abstract":
                abs_text = (d.get("description") or "").strip()
                if abs_text:
                    out["description"] = abs_text
                break  # only take first abstract

    # Version
    version = attr.get("version")
    if version:
        out["version"] = version

    # Publisher
    publisher = attr.get("publisher")
    if publisher:
        out["publisher"] = publisher

    ## Dates
    # Publication date
    publication_date = get_best_publication_date_datacite_record(attr)
    if publication_date:
        out["publication_date"] = publication_date

    # Created date
    created = attr.get(
        "created", ""
    )  # This is the date DOI record was created on DataCite
    if created:
        try:
            out["created_date"] = _norm_date_iso(created)
        except ValueError:
            pass  # skip if issue during normalization
    ##

    # Creators
    creators_slim = []
    for c in attr.get("creators", []):
        c_slim = {}

        name = c.get("name")
        if name:
            c_slim["name"] = name

        nametype = c.get("nameType")
        if nametype:
            c_slim["name_type"] = nametype

        # all identifiers
        ids = [
            id_obj.get("nameIdentifier")
            for id_obj in c.get("nameIdentifiers", [])
            if id_obj.get("nameIdentifier")
        ]
        if ids:
            c_slim["identifiers"] = ids

        # all affiliations
        aff_list = []
        aff = c.get("affiliation", [])
        if isinstance(aff, list):
            for a in aff:
                if isinstance(a, str) and a:
                    aff_list.append(a)
                elif isinstance(a, dict):
                    nm = a.get("name")
                    if nm:
                        aff_list.append(nm)
        elif isinstance(aff, str) and aff:
            aff_list.append(aff)

        if aff_list:
            c_slim["affiliations"] = aff_list

        if c_slim:  # only append creator if it actually contains something useful
            creators_slim.append(c_slim)

    if creators_slim:
        out["creators"] = creators_slim

    # Citations: split into dois (doi) and other ({id, type})
    rlsp = metadata.get("relationships", {})
    citations_raw = rlsp.get("citations", {}).get("data", [])

    if isinstance(citations_raw, list) and citations_raw:
        dois: list[str] = []
        other_list: list[dict] = []

        for item in citations_raw:
            if not isinstance(item, dict):
                continue
            cid = (item.get("id") or "").strip()
            ctype = (item.get("type") or "").strip()

            if not cid:
                continue

            # Case-insensitive match for DOI type
            if ctype.lower() == "dois":
                norm = _norm_doi(cid)
                if norm:
                    dois.append(norm)
            else:
                # keep raw id + type (preserve as given, except strip)
                other_list.append({"id": cid, "type": ctype} if ctype else {"id": cid})

        # Deduplicate while preserving order
        if dois:
            seen = set()
            dedup_dois = []
            for u in dois:
                if u not in seen:
                    seen.add(u)
                    dedup_dois.append(u)
            if dedup_dois:
                out.setdefault("citations", {})["dois"] = dedup_dois

        if other_list:
            # Dedup by (id, type) tuple
            seen2 = set()
            dedup_other = []
            for obj in other_list:
                key = (obj.get("id"), obj.get("type"))
                if key not in seen2:
                    seen2.add(key)
                    dedup_other.append(obj)
            if dedup_other:
                out.setdefault("citations", {})["other"] = dedup_other

        # If "citations" ends up empty, don't keep the key
        if "citations" in out and not out["citations"]:
            out.pop("citations")

    return out


def datacite_citations_block_to_records(
    target_doi: str,
    citations: Dict[str, list] | None,
    *,
    dataset_pub_date: str | None = None,
) -> List[Dict[str, object]]:
    """
    Convert a slimmed DataCite citations block into normalized citation records.
    """

    if dataset_pub_date:
        try:
            dataset_pub_date = _norm_date_iso(dataset_pub_date)
            dataset_pub_date = get_realistic_date(dataset_pub_date)
        except ValueError:
            dataset_pub_date = None

    results: List[Dict[str, object]] = []

    # DOIs → normalize + fetch date
    for citation_link_raw in (citations or {}).get("dois", []) or []:
        citation_doi = _norm_doi(citation_link_raw)
        if not citation_doi:
            continue

        citation_link = _norm_doi_url(citation_doi)
        rec: Dict[str, object] = {
            "dataset_id": target_doi,
            "source": ["datacite"],
            "citation_link": citation_link,
        }

        citation_date = None
        citation_date_raw = best_publication_date_for_doi(citation_doi)
        if citation_date_raw:
            try:
                norm_iso_date = _norm_date_iso(str(citation_date_raw))
                citation_date = get_realistic_date(norm_iso_date)
            except (ValueError, TypeError):
                citation_date = None

        if citation_date:
            rec["citation_date"] = citation_date

        rec["citation_weight"] = citation_weight(dataset_pub_date, citation_date)

        results.append(rec)

    # For other identifiers we cannot get a citation_date
    for obj in (citations or {}).get("other", []) or []:
        if not isinstance(obj, dict):
            continue
        id_val = (obj.get("id") or "").strip()
        if not id_val:
            continue
        results.append(
            {
                "dataset_id": target_doi,
                "source": ["datacite"],
                "citation_link": id_val,
                "citation_weight": 1.0,
            }
        )

    return dedupe_citations_by_link(results)


def get_citation_date(doi: str, date_map: Dict[str, str]) -> str | None:
    """
    Hybrid lookup:
    1. Check the high-speed Parquet cache (date_map) first.
    2. Fallback to best_publication_date_for_doi only if missing.
    """
    # 1. High-speed cache check
    if doi in date_map:
        return date_map[doi]

    # 2. Fallback to API/DB lookup
    return best_publication_date_for_doi(doi)


def datacite_citations_block_to_records_optimized(
    target_doi: str,
    citations: Dict[str, list] | None,
    date_map: Dict[str, str],  # Pass the cache here
    *,
    dataset_pub_date: str | None = None,
) -> List[Dict[str, Any]]:
    if dataset_pub_date:
        try:
            dataset_pub_date = _norm_date_iso(dataset_pub_date)
            dataset_pub_date = get_realistic_date(dataset_pub_date)
        except ValueError:
            dataset_pub_date = None

    results = []

    for citation_link_raw in (citations or {}).get("dois", []) or []:
        citation_doi = _norm_doi(citation_link_raw)
        if not citation_doi:
            continue

        # Use the hybrid lookup
        citation_date = None
        citation_date_raw = get_citation_date(citation_doi, date_map)
        if citation_date_raw:
            try:
                norm_iso_date = _norm_date_iso(str(citation_date_raw))
                citation_date = get_realistic_date(norm_iso_date)
            except (ValueError, TypeError):
                citation_date = None

        rec = {
            "dataset_id": target_doi,
            "source": ["datacite"],
            "citation_link": _norm_doi_url(citation_doi),
        }

        if citation_date:
            rec["citation_date"] = citation_date
            # Ensure citation_weight handles these dates
            rec["citation_weight"] = citation_weight(dataset_pub_date, citation_date)
        else:
            rec["citation_weight"] = 1.0

        results.append(rec)

    # For other identifiers we cannot get a citation_date
    for obj in (citations or {}).get("other", []) or []:
        if not isinstance(obj, dict):
            continue
        id_val = (obj.get("id") or "").strip()
        if not id_val:
            continue
        results.append(
            {
                "dataset_id": target_doi,
                "source": ["datacite"],
                "citation_link": id_val,
                "citation_weight": 1.0,
            }
        )
    return dedupe_citations_by_link(results)
