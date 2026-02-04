from __future__ import annotations

from sindex.core.dates import _norm_date_iso, is_realistic_integer_year
from sindex.core.ids import _normalize_orcid


def _get_author_orcid_primary_citation(record: dict, author_name: str) -> str | None:
    """
    Search for an author by `valueOf_` and return a normalized ORCID URL.

    The "author_lists" key of the EMDB response does not include ORCIDs.
    However, there is a "primary_citation" key, which is likely the resource to
    cite for this dataset, where authors similar to the dataset authors are usually listed
    and where the ORCID is sometime provided for an author under a "ORCID" key.

    Normalized ORCID format: https://orcid.org/<NNNN-NNNN-NNNN-NNNN>

    Args:
        record: The EMDB-like metadata dict.
        author_name: Author name string, e.g. "Jane D".

    Returns:
        A normalized ORCID URL string if found, otherwise None.
    """
    authors = (
        record.get("crossreferences", {})
        .get("citation_list", {})
        .get("primary_citation", {})
        .get("citation_type", {})
        .get("author", [])
    )

    if authors:
        for a in authors:
            if a.get("valueOf_") == author_name:
                raw = a.get("ORCID")
                if not raw:
                    return None
                return _normalize_orcid(raw)

    return None


def slim_emdb_record(metadata: dict) -> dict:
    """
    Produce a reduced version of a EMDB record aligned with our slim metadata schema.

    This function only keeps the metadata required for the S-index.
    This is intended to reduce the record's size and make records more uniform.

    This also normalizes certain values (e.g., date format, etc.),
    and removes empty values.

    Args:
        metadata: Full EMDB metadata record (dict) inclunding "admin" key.

    Returns:
        A slimmed dictionary containing at minimum `"source": "emdb"` and,
        when present, keys including: identifiers, title, version, publisher,
        publication_date, and creators

    Notes:
        - Only non-empty fields are preserved.
        - Creator objects are reduced to name, identifiers, and affiliations.
    """
    out = {"source": "emdb"}

    # Identifier
    emdb_id = metadata["emdb_id"]
    out["identifiers"] = [{"identifier": emdb_id, "identifier_type": "emdb_id"}]

    # URL
    out["url"] = "https://www.ebi.ac.uk/emdb/" + emdb_id

    admin = metadata.get("admin")
    if admin:
        # Title
        title = admin.get("title")
        if title:
            out["title"] = title

        # Subjects
        subjects = admin.get("keywords")
        if subjects:
            out["subjects"] = [x.strip() for x in subjects.split(",")]

        # Description/Abstract
        # No description available for EMDB datasets

        # Version
        version = admin.get("version")
        if version:
            out["version"] = version

        # Publication date
        keydates = admin.get("key_dates")
        if keydates:
            created = keydates.get("deposition")
            try:
                norm_date = _norm_date_iso(created)
                # keep for backward compatibility we rely on publication_year now
                out["publication_date"] = norm_date

                # Extract the year (first 4 chars) and convert to integer
                if norm_date and len(norm_date) >= 4:
                    year_str = norm_date[:4]
                    y_int = int(year_str)
                    if is_realistic_integer_year(y_int):
                        out["pubyear"] = y_int

            except (ValueError, TypeError):
                pass

        # Creators
        creators_slim = []
        authors = admin.get("authors_list", {}).get("author", [])

        if authors:
            for c in authors:
                # Case 1: Author is a string → treat as raw name
                if isinstance(c, str):
                    name = c.strip()
                    c_slim = {"name": name}

                    # ORCID lookup (if possible)
                    orcid = _get_author_orcid_primary_citation(metadata, name)
                    if orcid:
                        c_slim["identifiers"] = [orcid]

                    creators_slim.append(c_slim)
                    continue

                # Case 2: Author is a dict → your existing logic
                if isinstance(c, dict):
                    c_slim = {}

                    name = c.get("valueOf_")
                    if name:
                        c_slim["name"] = name

                    nametype = c.get("instance_type", "")
                    if nametype == "author":
                        c_slim["name_type"] = "Personal"

                    # ORCID lookup
                    if name:
                        orcid = _get_author_orcid_primary_citation(metadata, name)
                        if orcid:
                            c_slim["identifiers"] = [orcid]

                    if c_slim:
                        creators_slim.append(c_slim)

                else:
                    # Unexpected type – show something but keep processing
                    print(
                        f"[WARN] Unexpected author type in EMDB record {emdb_id}: {type(c)} -> {c}"
                    )

            if creators_slim:
                out["creators"] = creators_slim

    # Publisher
    out["publisher"] = "The Electron Microscopy Data Bank (EMDB)"

    # Citations: split into dois (doi) and other ({id, type})
    # Not applicable to EMDB

    return out
