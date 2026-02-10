from sindex.core.dates import is_realistic_integer_year
from sindex.core.http import _is_reachable, is_url
from sindex.core.ids import _norm_dataset_id, _norm_doi, _norm_doi_url, is_working_doi
from sindex.metrics.citations import merge_citations_dicts
from sindex.metrics.datasetindex import (
    dataset_index_year_timeseries,
)
from sindex.metrics.mentions import merge_mentions_dicts
from sindex.metrics.normalization import (
    get_subfield_year_norm_factors,
)
from sindex.metrics.topics import get_subfield_id_from_topic_id
from sindex.sources.datacite.discovery import get_datacite_doi_record
from sindex.sources.datacite.jobs import find_citations_dc_from_citation_block
from sindex.sources.datacite.normalize import slim_datacite_record
from sindex.sources.fuji.jobs import fair_evaluation_report
from sindex.sources.github.discovery import find_github_mentions_for_dataset_id
from sindex.sources.mdc.jobs import find_citations_mdc_duckdb
from sindex.sources.openalex.jobs import find_citations_oa, get_primary_topic_for_doi


def default_mdc_db_path():
    db_path = r"D:\pipeline-data\external\mdc-corpus\mdc_index.duckdb"
    return db_path


def default_norm_db_path():
    db_path = "input/subfield_norm_factors.duckdb"
    return db_path


def topics_table_path():
    db_path = "input/openalex_topic_mapping_table.csv"
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
        pubyear = None
        citations_block = None
    else:
        slim = slim_datacite_record(rec)
        pubyear = slim.get("pubyear")
        citations_block = slim.get("citations")

    dataset_report["metadata"] = slim

    # Get domain (OpenAlex topic)
    topic_result = get_primary_topic_for_doi(norm_doi)
    # if not topic_result:
    #    topic_result = custom_model(norm_doi) #add Jamey's model here

    subfield_id = None
    dataset_report["topic"] = None

    if topic_result:
        try:
            topic_id = topic_result.get("topic_id")
            subfield_id = get_subfield_id_from_topic_id(topics_table_path(), topic_id)
            topic_result["subfield_id"] = subfield_id
            dataset_report["topic"] = topic_result
        except Exception:
            pass

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
        doi, dataset_pubyear=pubyear, db_path=default_mdc_db_path()
    )
    if citations_mdc:
        citations_list.append(citations_mdc)

    citations_oa = find_citations_oa(doi, dataset_pubyear=pubyear)
    if citations_oa:
        citations_list.append(citations_oa)

    if citations_block:
        citations_dc = find_citations_dc_from_citation_block(
            doi, citations_block, dataset_pubyear=pubyear
        )
        if citations_dc:
            citations_list.append(citations_dc)

    citations = merge_citations_dicts(citations_list) if citations_list else None
    dataset_report["citations"] = citations

    # Mentions
    mentions_list = []
    mentions_github = find_github_mentions_for_dataset_id(doi, dataset_pubyear=pubyear)
    if mentions_github:
        mentions_list.append(mentions_github)

    mentions = merge_mentions_dicts(mentions_list)
    if mentions:
        dataset_report["mentions"] = mentions
    else:
        dataset_report["mentions"] = None

    # Normalization factors
    try:
        norm = get_subfield_year_norm_factors(
            default_norm_db_path(), subfield_id=subfield_id, pubyear=pubyear
        )
    except KeyError:
        norm = None

    dataset_report["normalization_factors"] = norm

    # Dataset Index series
    Fi = fair_score if fair_score is not None else 0.0

    FT = norm["FT"] if norm else 13.46
    CTw = norm["CTw"] if norm else 1.0
    MTw = norm["MTw"] if norm else 1.0

    dataset_index_series = dataset_index_year_timeseries(
        Fi=Fi,
        citations=citations,
        mentions=mentions,
        pubyear=pubyear,
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
    pubyear: int | None = None,
    subfield_id: str | None = None,
    subfield_name: str | None = None,
) -> dict:
    """
    Build a dataset_report starting from a URL (not a DOI).

    This function intentionally skips DOI-dependent sources:
      - get_datacite_doi_record
      - get_primary_topic_for_doi
      - find_citations_oa
      - find_citations_dc_from_citation_block
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
    if not is_realistic_integer_year(pubyear):
        pubyear = None

    dataset_report["metadata"] = None

    # Domain (OpenALex topic)
    if subfield_id:
        topic_obj = {"subfield_id": subfield_id}
        if subfield_name:
            topic_obj["subfield_name"] = subfield_name
        dataset_report["topic"] = topic_obj
    else:
        dataset_report["topic"] = None

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
        url, dataset_pubyear=pubyear, db_path=default_mdc_db_path()
    )
    if citations_mdc:
        citations_list.append(citations_mdc)

    citations = merge_citations_dicts(citations_list) if citations_list else None
    dataset_report["citations"] = citations

    # Mentions
    mentions_list: list[list[dict]] = []

    mentions_github = find_github_mentions_for_dataset_id(
        url,
        dataset_pubyear=pubyear,
    )
    if mentions_github:
        mentions_list.append(mentions_github)

    mentions = merge_mentions_dicts(mentions_list) if mentions_list else None
    dataset_report["mentions"] = mentions

    # 7) Normalization factors
    try:
        norm = get_subfield_year_norm_factors(
            default_norm_db_path(), subfield_id=subfield_id, pubyear=pubyear
        )
    except KeyError:
        norm = None

    dataset_report["normalization_factors"] = norm

    # Dataset Index series
    Fi = fair_score if fair_score is not None else 0.0

    FT = norm["FT"] if norm else 13.46
    CTw = norm["CTw"] if norm else 1.0
    MTw = norm["MTw"] if norm else 1.0

    dataset_index_series = dataset_index_year_timeseries(
        Fi=Fi,
        citations=citations,
        mentions=mentions,
        pubyear=pubyear,
        FT=FT,
        CTw=CTw,
        MTw=MTw,
    )

    dataset_report["dataset_index_series"] = dataset_index_series or None
    return dataset_report
