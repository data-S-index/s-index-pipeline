from sindex.core.dates import _norm_date_iso
from sindex.core.ids import _norm_doi


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

    # Publication date
    created = attr.get("created", "")
    if created:
        try:
            out["publication_date"] = _norm_date_iso(created)
        except ValueError:
            pass  # skip if not valid

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
