import os
from datetime import datetime

import duckdb

from sindex.core.ids import _norm_doi, _norm_doi_url, is_working_doi
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
    # Check valid DOI and normalize
    norm_doi = _norm_doi(doi)
    if not norm_doi:
        raise ValueError(f"Invalid DOI format:{doi}")
    norm_doi_url = _norm_doi_url(norm_doi)
    if not is_working_doi(norm_doi_url):
        raise ValueError(f"'{norm_doi_url}' is not a working DOI.")

    # Ids
    dataset_report = {}
    dataset_report["doi"] = doi
    dataset_report["norm_doi"] = norm_doi
    dataset_report["norm_doi_url"] = norm_doi_url

    # Get metadata from DataCite and create slim version
    rec = get_datacite_doi_record(norm_doi)
    slim = slim_datacite_record(rec)
    pubdate = slim["publication_date"]
    pubyear = datetime.fromisoformat(pubdate).year
    citations_block = slim["citations"]
    dataset_report["metadata"] = slim

    # Get field (OpenAlex topic)
    topic_result = get_primary_topic_for_doi(norm_doi)
    if topic_result:
        if topic_result["topic_score"] > 0.5:
            dataset_report["topic"] = topic_result
            topic_id = topic_result["topic_id"]
        else:
            dataset_report["topic"] = None
            topic_id = None
    else:
        dataset_report["topic"] = None
        topic_id = None

    # Get F-UJI FAIR score
    fair_report = fair_evaluation_report(norm_doi_url)
    dataset_report["fair"] = fair_report
    fair_score = fair_report["fair_score"]

    # Citations
    citations_list = []
    citations_mdc = find_citations_mdc_duckdb(
        norm_doi, dataset_pub_date=pubdate, db_path=default_mdc_db_path()
    )
    if citations_mdc:
        citations_list.append(citations_mdc)

    citations_oa = find_citations_oa(norm_doi_url, dataset_pub_date=pubdate)
    if citations_oa:
        citations_list.append(citations_oa)
    citations_dc = find_citations_dc_from_citation_block(
        norm_doi_url, citations_block, pubdate
    )
    if citations_dc:
        citations_list.append(citations_dc)

    citations = merge_citations_dicts(citations_list)
    if citations:
        dataset_report["citations"] = citations
    else:
        dataset_report["citations"] = None

    # Mentions
    mentions_list = []
    mentions_github = find_github_mentions_for_dataset_id(norm_doi, pubdate)
    if mentions_github:
        mentions_list.append(mentions_github)

    mentions = merge_mentions_dicts(mentions_list)
    if mentions:
        dataset_report["mentions"] = mentions
    else:
        dataset_report["mentions"] = None

    # Normalization factors
    con = duckdb.connect(default_norm_db_path())
    norm = get_topic_year_norm_factors(
        con,
        topic_id=topic_id,
        year=pubyear,
        table="topic_norm_factors_mock",
    )

    if norm:
        dataset_report["normalization_factors"] = norm
    else:
        dataset_report["normalization_factors"] = None

    # Dataset Index
    Fi = fair_score / 100.0
    dataset_index_series = dataset_index_timeseries(
        Fi=Fi,
        citations=citations,
        mentions=mentions,
        pubdate=pubdate,
        FT=norm["FT"],
        CTw=norm["CwT"],
        MTw=norm["MwT"],
    )

    if dataset_index_series:
        dataset_report["dataset_index_series"] = dataset_index_series
    else:
        dataset_report["dataset_index_series"] = None

    return dataset_report
