def norm_citations(dataset_pub_date, dataset_domain):

    if dataset_pub_date or dataset_domain:
        c = 1 # query table of citations in the past three years per domain
    else:
        c = 1

    return c

def norm_mentions(dataset_pub_date, dataset_domain):

    if dataset_pub_date or dataset_domain:
        m = 1 # query table of mentions in the past three years per domain
    else:
        m = 1

    return m