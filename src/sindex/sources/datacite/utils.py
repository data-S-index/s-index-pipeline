from sindex.core.dates import _norm_date_iso


def get_best_publication_date_datacite_record(attr):
    candidates = []

    # Extract "Issued" date - this seems most likely to be the date a dataset was published
    for d in attr.get("dates", []):
        if d.get("dateType") == "Issued" and d.get("date"):
            candidates.append(str(d.get("date")))
            break

    # Add other fallbacks to the candidate list
    candidates.append(attr.get("published"))
    candidates.append(attr.get("publicationYear"))

    # Iterate through candidates and return the first one that normalizes
    for candidate in candidates:
        if not candidate:
            continue
        try:
            return _norm_date_iso(str(candidate))
        except (ValueError, TypeError):
            continue  # Try the next candidate if normalization fails
