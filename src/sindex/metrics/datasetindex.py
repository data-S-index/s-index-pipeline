from sindex.core.ids import _norm_doi, _norm_doi_url, is_working_doi
from sindex.sources.datacite.discovery import get_datacite_doi_record
from sindex.sources.datacite.normalize import slim_datacite_record
from sindex.sources.fuji.jobs import fair_evaluation_report
from sindex.sources.openalex.jobs import get_primary_topic_for_doi


def dataset_index_from_doi(doi):
    # Check valid DOI and normalize
    norm_doi = _norm_doi(doi)
    if not norm_doi:
        raise ValueError(f"Invalid DOI format:{doi}")
    norm_doi_url = _norm_doi_url(norm_doi)
    if not is_working_doi(norm_doi_url):
        raise ValueError(f"'{norm_doi_url}' is not a working DOI.")

    dataset_report = {}
    dataset_report["doi"] = doi
    dataset_report["norm_doi"] = norm_doi
    dataset_report["norm_doi_url"] = norm_doi_url

    # Get metadata from DataCite and create slim version
    rec = get_datacite_doi_record(norm_doi)
    slim = slim_datacite_record(rec)
    pubdate = slim["publication_date"]

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

    # Get F-UJI FAIR score
    fair_report = fair_evaluation_report(norm_doi_url)
    dataset_report["topic"] = fair_report
    fair_score = fair_report["fair_score"]

    # Citations

    # Mentions

    # Normalization factors

    # Dataset Index
