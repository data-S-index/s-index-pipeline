from __future__ import annotations

import hashlib
import os
import time
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterator, List, Optional

from sindex.extract.mentions import normalize_doi_list, normalize_url_list
from sindex.extract.pdf import extract_text_from_pdf
from sindex.extract.text import extract_dois, extract_urls, strip_trailing_punct

from .client import UNDLClient
from .discover import iter_recids


def _marcxml_strings(xml_text: str) -> List[str]:
    """
    Broadly extract textual content from MARCXML:
      - all controlfield text
      - all subfield text
    Then run DOI/URL extractors over these strings.
    """
    root = ET.fromstring(xml_text)
    out: List[str] = []

    for el in root.iter():
        if el.tag.endswith("controlfield"):
            if el.text and el.text.strip():
                out.append(el.text.strip())
        elif el.tag.endswith("subfield"):
            if el.text and el.text.strip():
                out.append(el.text.strip())
    return out


def _marcxml_856u_urls(xml_text: str) -> List[str]:
    """
    Extract MARC 856 $u URLs (electronic location and access).
    """
    root = ET.fromstring(xml_text)
    urls: List[str] = []

    for df in root.iter():
        if df.tag.endswith("datafield") and df.attrib.get("tag") == "856":
            for sf in df:
                if sf.tag.endswith("subfield") and sf.attrib.get("code") == "u":
                    if sf.text and sf.text.strip():
                        urls.append(sf.text.strip())
    return urls


def _looks_like_pdf_url(url: str) -> bool:
    # Heuristic only. Improve later with HEAD(Content-Type) if needed.
    return url.lower().endswith(".pdf")


def mine_record(
    client: UNDLClient,
    *,
    recid: int,
    download_pdfs: bool = False,
    pdf_dir: Optional[str] = None,
    scan_pdf_text: bool = True,
) -> Dict[str, Any]:
    xml = client.fetch_record_marcxml(recid=recid)

    strings = _marcxml_strings(xml)

    dois: List[str] = []
    urls: List[str] = []

    # Scan all text
    for s in strings:
        dois.extend(extract_dois(s))
        urls.extend(extract_urls(s))

    # Add explicit 856u URLs
    urls.extend(_marcxml_856u_urls(xml))

    # Normalize + de-dupe
    dois = normalize_doi_list(dois)
    urls = normalize_url_list([strip_trailing_punct(u) for u in urls if u])

    pdfs_scanned = []
    if download_pdfs and pdf_dir:
        os.makedirs(pdf_dir, exist_ok=True)
        pdf_urls = [u for u in urls if _looks_like_pdf_url(u)]

        for u in pdf_urls:
            safe = hashlib.sha256(u.encode("utf-8")).hexdigest()[:24]
            out_path = os.path.join(pdf_dir, f"{recid}_{safe}.pdf")

            if not os.path.exists(out_path):
                ok = client.download_file(url=u, out_path=out_path)
                if not ok:
                    pdfs_scanned.append({"pdf_url": u, "downloaded": False})
                    continue

            pdfs_scanned.append({"pdf_url": u, "downloaded": True, "path": out_path})

            if scan_pdf_text:
                text = extract_text_from_pdf(out_path)
                dois2 = normalize_doi_list(extract_dois(text))
                urls2 = normalize_url_list(extract_urls(text))

                # merge into main lists
                doi_set = {x.lower() for x in dois}
                for d in dois2:
                    if d.lower() not in doi_set:
                        dois.append(d)
                        doi_set.add(d.lower())
                for uu in urls2:
                    if uu not in set(urls):
                        urls.append(uu)

    return {
        "source": "undl",
        "recid": recid,
        "dois": dois,
        "urls": urls,
        "pdfs_scanned": pdfs_scanned,
        "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def extract_undl_mentions(
    *,
    query: str,
    client: Optional[UNDLClient] = None,
    base_url: Optional[str] = None,
    rg: int = 100,
    ln: str = "en",
    rm: str = "wrd",
    max_records: int = 0,
    state_path: Optional[str] = None,
    download_pdfs: bool = False,
    pdf_dir: Optional[str] = None,
    scan_pdf_text: bool = True,
    yield_errors: bool = True,
) -> Iterator[Dict[str, Any]]:
    """
    Stream UN Digital Library records and extract DOI/URL mentions per record.

    This implementation uses MARCXML (of=xm) because JSON output (of=recjson)
    may return HTTP 202 + HTML from some environments.
    """
    if client is None:
        client = UNDLClient(base_url=base_url) if base_url else UNDLClient()

    for recid in iter_recids(
        client,
        query=query,
        rg=rg,
        ln=ln,
        rm=rm,
        max_records=max_records,
        state_path=state_path,
    ):
        try:
            yield mine_record(
                client,
                recid=recid,
                download_pdfs=download_pdfs,
                pdf_dir=pdf_dir,
                scan_pdf_text=scan_pdf_text,
            )
        except Exception as e:
            if not yield_errors:
                raise
            yield {
                "source": "undl",
                "recid": recid,
                "error": repr(e),
                "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
