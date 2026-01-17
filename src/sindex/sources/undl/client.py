from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

from .constants import DEFAULT_BASE_URL, DEFAULT_HEADERS, SEARCH_PATH


@dataclass
class RetryConfig:
    retries: int = 5
    backoff_initial: float = 1.0
    backoff_max: float = 60.0
    timeout_s: int = 60


class UNDLClient:
    """
    UN Digital Library client.

    IMPORTANT: JSON output (of=recjson) can return HTTP 202 + HTML from some networks.
    XML (of=xm, MARCXML) is reliable, so this client is XML-first.
    """

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        session: Optional[requests.Session] = None,
        headers: Optional[Dict[str, str]] = None,
        retry: Optional[RetryConfig] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        if headers:
            self.session.headers.update(headers)
        self.retry = retry or RetryConfig()

    def _get_text(self, path: str, params: Dict[str, Any]) -> requests.Response:
        url = self.base_url + path
        backoff = self.retry.backoff_initial

        last_status = None
        last_ct = None
        last_preview = None
        last_url = None

        for attempt in range(self.retry.retries):
            r = None
            try:
                r = self.session.get(
                    url,
                    params=params,
                    timeout=self.retry.timeout_s,
                    headers={"Accept": "text/xml,application/xml;q=0.9,*/*;q=0.8"},
                )

                last_status = r.status_code
                last_ct = r.headers.get("Content-Type")
                last_url = r.url

                text = r.text or ""
                last_preview = text[:300].replace("\n", " ").replace("\r", " ").strip()

                # Retry transient / queued responses
                if r.status_code in (202, 429, 500, 502, 503, 504):
                    time.sleep(backoff)
                    backoff = min(backoff * 2, self.retry.backoff_max)
                    continue

                r.raise_for_status()

                # Occasionally get empty body even with 200
                if not text.strip():
                    if attempt < self.retry.retries - 1:
                        time.sleep(backoff)
                        backoff = min(backoff * 2, self.retry.backoff_max)
                        continue

                return r

            except Exception:
                if attempt == self.retry.retries - 1:
                    raise RuntimeError(
                        "UNDL request failed after retries "
                        f"(last_status={last_status}, last_content_type={last_ct}, last_url={last_url}, "
                        f"last_preview={last_preview!r}, params={params})"
                    )
                time.sleep(backoff)
                backoff = min(backoff * 2, self.retry.backoff_max)

        # Should never get here because we either return or raise above
        raise RuntimeError("UNDL request failed unexpectedly")

    @staticmethod
    def _parse_recids_from_marcxml(xml_text: str) -> List[int]:
        # MARCXML: controlfield tag="001" is recid
        recids: List[int] = []
        root = ET.fromstring(xml_text)

        for el in root.iter():
            if el.tag.endswith("controlfield") and el.attrib.get("tag") == "001":
                t = (el.text or "").strip()
                if t.isdigit():
                    recids.append(int(t))
        return recids

    def fetch_recids_page(
        self,
        *,
        query: str,
        jrec: int,
        rg: int = 100,
        ln: str = "en",
        rm: str = "wrd",
    ) -> Tuple[List[int], Optional[int]]:
        """
        Return (recids, next_jrec) for a query page, using MARCXML output.

        Uses:
          /search?of=xm&p=<query>&rg=<rg>&jrec=<jrec>
        """
        params = {
            "ln": ln,
            "p": query,
            "rm": rm,
            "of": "xm",
            "rg": rg,
            "jrec": jrec,
        }
        r = self._get_text(SEARCH_PATH, params)
        recids = self._parse_recids_from_marcxml(r.text)
        next_jrec = (jrec + rg) if (recids and len(recids) == rg) else None
        return recids, next_jrec

    def fetch_record_marcxml(
        self,
        *,
        recid: int,
        ln: str = "en",
        rm: str = "wrd",
    ) -> str:
        """
        Fetch one record as MARCXML via p=recid:<id>&of=xm.
        """
        params = {
            "ln": ln,
            "p": f"recid:{recid}",
            "rm": rm,
            "of": "xm",
            "rg": 1,
            "jrec": 1,
        }
        r = self._get_text(SEARCH_PATH, params)
        return r.text

    def download_file(
        self,
        *,
        url: str,
        out_path: str,
        timeout_s: int = 120,
        retries: int = 4,
    ) -> bool:
        backoff = self.retry.backoff_initial

        for attempt in range(retries):
            try:
                with self.session.get(url, stream=True, timeout=timeout_s) as r:
                    if r.status_code in (202, 429, 500, 502, 503, 504):
                        time.sleep(backoff)
                        backoff = min(backoff * 2, self.retry.backoff_max)
                        continue
                    r.raise_for_status()
                    with open(out_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 64):
                            if chunk:
                                f.write(chunk)
                return True
            except Exception:
                if attempt == retries - 1:
                    return False
                time.sleep(backoff)
                backoff = min(backoff * 2, self.retry.backoff_max)

        return False
