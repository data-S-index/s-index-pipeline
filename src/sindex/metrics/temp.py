def dataset_index_series_from_url(
    url: str,
    *,
    identifier: str | None = None,
    pubdate: str | None = None,
    topic_id: str | None = None,
) -> dict:
    """
    Build a dataset_report starting from a URL (not a DOI).

    This function intentionally skips DOI-dependent sources:
      - get_datacite_doi_record
      - get_primary_topic_for_doi
      - find_citations_oa
      - find_citations_dc_from_citation_block

    Args:
        url: Required dataset landing page URL.
        identifier: Optional dataset identifier to use for MDC/GitHub searches
                    (if omitted, we only use the URL as the identifier).
        pubdate: Optional publication date in any reasonable format; normalized to ISO if provided.
        topic_id: Optional OpenAlex topic id (e.g. "https://openalex.org/T12345" or "T12345").

    Returns:
        dataset_report dict with citations/mentions + normalization + dataset_index_series.
    """
    # Validate URL
    if not isinstance(url, str) or not url.strip():
        raise ValueError("url must be a non-empty string")
    url = url.strip()

    if not is_url(url):
        raise ValueError(f"Invalid URL format: {url}")

    if not _is_reachable(url):
        raise ValueError(f"'{url}' does not appear to be reachable.")

    norm_url = _norm_dataset_id(url)
    norm_identifier = _norm_dataset_id(identifier)

    dataset_report = {}
    dataset_report["input_url"] = url
    dataset_report["norm_url"] = norm_url
    dataset_report["input_identifier"] = identifier
    dataset_report["norm_identifier"] = norm_identifier

    # Non metadata, resolve pubdate
    if pubdate:
        try:
            pubdate = _norm_date_iso(pubdate)
        except ValueError as e:
            raise ValueError(f"Invalid pubdate '{pubdate}': {e}") from e
    pub_dt = _to_datetime_utc(pubdate)
    pubyear = pub_dt.year if pub_dt else None
    citations_block = None

    dataset_report["metadata"] = None

    # Domain (OpenALex topic)
    dataset_report["topic"] = topic_id

    # Get F-UJI FAIR score
    fair_report = fair_evaluation_report(url)
    dataset_report["fair"] = fair_report

    fair_score = None
    if fair_report and "fair_score" in fair_report:
        try:
            fair_score = float(fair_report["fair_score"])
        except Exception:
            fair_score = None

    # Citations (MDC only)
    citations_list: list[list[dict]] = []

    citations_mdc = find_citations_mdc_duckdb(
        url, dataset_pub_date=pubdate, db_path=default_mdc_db_path()
    )
    if citations_mdc:
        citations_list.append(citations_mdc)

    citations = merge_citations_dicts(citations_list) if citations_list else None
    dataset_report["citations"] = citations

    # Mentions
    mentions_list: list[list[dict]] = []

    mentions_github = find_github_mentions_for_dataset_id(
        url,
        dataset_pub_date=pubdate,
    )
    if mentions_github:
        mentions_list.append(mentions_github)

    mentions = merge_mentions_dicts(mentions_list) if mentions_list else None
    dataset_report["mentions"] = mentions

    # 7) Normalization factors
    try:
        with duckdb.connect(default_norm_db_path()) as con:
            norm = get_topic_year_norm_factors(
                con,
                topic_id=topic_id,
                year=pubyear,
                table="topic_norm_factors_mock",
            )
    except KeyError:
        norm = None

    dataset_report["normalization_factors"] = norm

    # 8) Dataset Index series
    Fi = (float(fair_score) / 100.0) if fair_score is not None else 0.0

    FT = norm["FT"] if norm else 0.5
    CTw = norm["CTw"] if norm else 1.0
    MTw = norm["MTw"] if norm else 1.0

    dataset_index_series = dataset_index_timeseries(
        Fi=Fi,
        citations=citations,
        mentions=mentions,
        pubdate=pubdate_iso,
        FT=FT,
        CTw=CTw,
        MTw=MTw,
    )

    dataset_report["dataset_index_series"] = dataset_index_series or None
    return dataset_report
