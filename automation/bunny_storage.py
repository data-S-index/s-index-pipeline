"""
Minimal client for a bunny.net storage zone, using its HTTP API.

API reference: https://docs.bunny.net/reference/storage-api

Files are addressed by their path inside the zone, e.g.
"datacite/slim/2026/datacite-2026-10-17.ndjson.gz".
"""

from __future__ import annotations

import hashlib
import io
from pathlib import Path
from typing import BinaryIO

from sindex.core.http import make_session

# (connect, read) timeouts in seconds
TIMEOUT = (10, 300)


class BunnyStorage:
    """Upload, download, list and delete files in one storage zone."""

    def __init__(self, zone: str, password: str, host: str = "storage.bunnycdn.com"):
        """
        Args:
            zone: Storage zone name.
            password: The zone's password (sent as the AccessKey header).
            host: Storage API endpoint of the zone's main region.
        """
        self.base_url = f"https://{host}/{zone}"
        # Own session so the AccessKey header is never sent to other hosts
        self.session = make_session(
            total_retries=5, backoff=2.0, allowed_methods=("GET", "PUT", "DELETE")
        )
        self.session.headers["AccessKey"] = password

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def put_bytes(self, path: str, data: bytes) -> dict:
        """
        Upload `data` to `path`, replacing any file already there.

        bunny.net checks the upload against its SHA-256 and rejects it if the
        two don't match.

        Returns:
            {"path": path, "bytes": size, "sha256": hex digest}
        """
        sha256 = hashlib.sha256(data).hexdigest()
        self._put(path, io.BytesIO(data), sha256)
        return {"path": path, "bytes": len(data), "sha256": sha256}

    def upload_file(self, local_path: Path, path: str) -> dict:
        """
        Upload a local file to `path`, streaming it from disk.

        Returns the same dict as put_bytes.
        """
        local_path = Path(local_path)
        digest = hashlib.sha256()
        with open(local_path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(chunk)
        sha256 = digest.hexdigest()
        with open(local_path, "rb") as f:
            self._put(path, f, sha256)
        return {"path": path, "bytes": local_path.stat().st_size, "sha256": sha256}

    def _put(self, path: str, body: BinaryIO, sha256: str) -> None:
        # Always pass a file-like body, never raw bytes: urllib3 sends a bytes
        # body as one SSL write, so the socket timeout would cap the *whole*
        # upload. A file object is sent in small blocks, each with its own
        # timeout, and urllib3 rewinds it before a retry.
        resp = self.session.put(
            self._url(path),
            data=body,
            headers={
                "Content-Type": "application/octet-stream",
                "Checksum": sha256.upper(),
            },
            timeout=TIMEOUT,
        )
        resp.raise_for_status()

    def get_bytes(self, path: str) -> bytes | None:
        """Return the contents of the file at `path`, or None if it doesn't exist."""
        resp = self.session.get(self._url(path), timeout=TIMEOUT)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.content

    def download_file(
        self, path: str, local_path: Path, sha256: str | None = None
    ) -> None:
        """
        Download the file at `path` to `local_path`.

        Raises:
            ValueError: If `sha256` is given and the download doesn't match it.
        """
        digest = hashlib.sha256()
        with self.session.get(self._url(path), stream=True, timeout=TIMEOUT) as resp:
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
                    digest.update(chunk)
        if sha256 and digest.hexdigest() != sha256.lower():
            Path(local_path).unlink(missing_ok=True)
            raise ValueError(
                f"Checksum mismatch for {path}: expected {sha256}, "
                f"got {digest.hexdigest()}"
            )

    def list_dir(self, path: str) -> list[dict]:
        """
        List the files and folders directly inside the folder at `path`.

        Returns:
            bunny.net's entries (with ObjectName, IsDirectory, Length, ...), or
            an empty list if the folder doesn't exist.
        """
        resp = self.session.get(
            self._url(path.rstrip("/") + "/"),
            headers={"Accept": "application/json"},
            timeout=TIMEOUT,
        )
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        return resp.json()

    def delete(self, path: str) -> None:
        """Delete the file at `path`. Does nothing if it doesn't exist."""
        resp = self.session.delete(self._url(path), timeout=TIMEOUT)
        if resp.status_code != 404:
            resp.raise_for_status()
