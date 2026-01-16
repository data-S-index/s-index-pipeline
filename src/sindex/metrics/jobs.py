import os

import duckdb

from sindex.core.dates import _norm_date_iso, _to_datetime_utc
from sindex.core.http import _is_reachable, is_url
from sindex.core.ids import _norm_dataset_id, _norm_doi, _norm_doi_url, is_working_doi
from sindex.metrics.citations import merge_citations_dicts
from sindex.metrics.datasetindex import dataset_index_timeseries
from sindex.metrics.mentions import merge_mentions_dicts
from sindex.metrics.normalization import get_topic_year_norm_factors
from sindex.sources.datacite.discovery import get_datacite_doi_record
from sindex.sources.datacite.jobs import find_citations_dc_from_citation_block
from sindex.sources.datacite.normalize import slim_datacite_record
from sindex.sources.fuji.jobs import fair_evaluation_report
from sindex.sources.github.discovery import find_github_mentions_for_dataset_id
from sindex.sources.mdc.jobs import find_citations_mdc_duckdb
from sindex.sources.openalex.jobs import find_citations_oa, get_primary_topic_for_doi


def default_mdc_db_path():
    current_dir = os.getcwd()
    parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
    mdc_path = os.path.join(parent_dir, "input", "mdc")
    db_path = os.path.join(mdc_path, "mdc_index.duckdb")
    return db_path


def default_norm_db_path():
    current_dir = os.getcwd()
    parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
    norm_path = os.path.join(parent_dir, "input", "mock_norm")
    db_path = os.path.join(norm_path, "mock_norm.duckdb")
    return db_path


def dataset_index_series_from_doi(doi):
    # Validate DOI and normalize
    norm_doi = _norm_doi(doi)
    if not norm_doi:
        raise ValueError(f"Invalid DOI format: {doi}")

    norm_doi_url = _norm_doi_url(norm_doi)
    if not is_working_doi(norm_doi_url, allow_blocked=True):
        raise ValueError(f"'{norm_doi_url}' does not appear to resolve.")

    dataset_report = {}
    dataset_report["input_doi"] = doi
    dataset_report["norm_doi"] = norm_doi
    dataset_report["norm_doi_url"] = norm_doi_url

    # Get metadata from DataCite and create slim version
    rec = get_datacite_doi_record(norm_doi)

    if not rec:
        # Happens if DOI is valid but not found in DataCite (e.g. DOI of a manuscript)
        slim = None
        pubdate = None
        pubyear = None
        citations_block = None
    else:
        slim = slim_datacite_record(rec)
        pubdate = slim.get("publication_date")
        pub_dt = _to_datetime_utc(pubdate)
        pubyear = pub_dt.year if pub_dt else None
        citations_block = slim.get("citations")

    dataset_report["metadata"] = slim

    # Get domain (OpenAlex topic)
    topic_result = get_primary_topic_for_doi(norm_doi)

    topic_id = None
    dataset_report["topic"] = None

    if topic_result and topic_result.get("topic_score", 0.0) > 0.5:
        dataset_report["topic"] = topic_result
        topic_id = topic_result.get("topic_id")

    # Get F-UJI FAIR score
    fair_report = fair_evaluation_report(norm_doi_url)
    dataset_report["fair"] = fair_report

    fair_score = None
    if fair_report and "fair_score" in fair_report:
        try:
            fair_score = float(fair_report["fair_score"])
        except Exception:
            fair_score = None

    # Citations
    citations_list = []
    citations_mdc = find_citations_mdc_duckdb(
        doi, dataset_pub_date=pubdate, db_path=default_mdc_db_path()
    )
    if citations_mdc:
        citations_list.append(citations_mdc)

    citations_oa = find_citations_oa(doi, dataset_pub_date=pubdate)
    if citations_oa:
        citations_list.append(citations_oa)

    if citations_block:
        citations_dc = find_citations_dc_from_citation_block(
            doi, citations_block, dataset_pub_date=pubdate
        )
        if citations_dc:
            citations_list.append(citations_dc)

    citations = merge_citations_dicts(citations_list) if citations_list else None
    dataset_report["citations"] = citations

    # Mentions
    mentions_list = []
    mentions_github = find_github_mentions_for_dataset_id(doi, dataset_pub_date=pubdate)
    if mentions_github:
        mentions_list.append(mentions_github)

    mentions = merge_mentions_dicts(mentions_list)
    if mentions:
        dataset_report["mentions"] = mentions
    else:
        dataset_report["mentions"] = None

    # Normalization factors
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

    # Dataset Index series
    Fi = (float(fair_score) / 100.0) if fair_score is not None else 0.0

    FT = norm["FT"] if norm else 0.5
    CTw = norm["CTw"] if norm else 1.0
    MTw = norm["MTw"] if norm else 1.0

    dataset_index_series = dataset_index_timeseries(
        Fi=Fi,
        citations=citations,
        mentions=mentions,
        pubdate=pubdate,
        FT=FT,
        CTw=CTw,
        MTw=MTw,
    )

    if dataset_index_series:
        dataset_report["dataset_index_series"] = dataset_index_series
    else:
        dataset_report["dataset_index_series"] = None

    return dataset_report


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

    if not url.startswith(("http://", "https://")):
        raise ValueError(
            f"Invalid URL format: {url} (must start with http:// or https://)"
        )
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

    # Dataset Index series
    Fi = (float(fair_score) / 100.0) if fair_score is not None else 0.0

    FT = norm["FT"] if norm else 0.5
    CTw = norm["CTw"] if norm else 1.0
    MTw = norm["MTw"] if norm else 1.0

    dataset_index_series = dataset_index_timeseries(
        Fi=Fi,
        citations=citations,
        mentions=mentions,
        pubdate=pubdate,
        FT=FT,
        CTw=CTw,
        MTw=MTw,
    )

    dataset_report["dataset_index_series"] = dataset_index_series or None
    return dataset_report
