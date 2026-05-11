"""
Stage 1 of the ETL pipeline: download the latest fuel-prices CSV.

Workflow:
  1. GET the CSV from gov.uk with a generous timeout.
  2. Validate it looks like a real CSV (size > 1 KB, has commas, not HTML).
  3. Compute sha256 — used downstream to skip duplicate runs.
  4. Save to a temp file. Caller owns lifecycle (deletion).
  5. Retry on 5xx / network errors with exponential backoff.

If all retries fail, raise DownloadError. The orchestrator catches it and
sends a CRITICAL admin notification.
"""
import asyncio
import hashlib
import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path

import httpx

from etl import config

log = logging.getLogger(__name__)


class DownloadError(Exception):
    """Raised when download fails after all retries."""


@dataclass
class DownloadResult:
    """Output of the download stage."""
    path: Path          # temp file with the CSV
    sha256: str         # 64-char hex digest
    byte_size: int      # total bytes downloaded


async def download_csv(local_csv_path: str | Path | None = None) -> DownloadResult:
    """
    Download the latest fuel-prices CSV with retries.

    Args:
        local_csv_path: optional path to a local CSV. When provided, skip
            the network and use this file as if it were just downloaded.
            Useful for offline development, replaying historical CSVs from
            the GitHub Releases archive, or testing against fixtures.

    Returns:
        DownloadResult with file path, sha256 digest, and byte size.

    Raises:
        DownloadError after exhausting all retries (network mode only).
    """
    if local_csv_path is not None:
        return _read_local(Path(local_csv_path))

    last_error: Exception | None = None

    for attempt in range(config.DOWNLOAD_MAX_RETRIES):
        try:
            return await _download_once()
        except (httpx.HTTPError, ValueError) as exc:
            last_error = exc
            if attempt < config.DOWNLOAD_MAX_RETRIES - 1:
                backoff = config.DOWNLOAD_RETRY_BACKOFF_SECONDS[attempt]
                log.warning(
                    "Download attempt %d/%d failed (%s). Retrying in %ds…",
                    attempt + 1, config.DOWNLOAD_MAX_RETRIES, exc, backoff,
                )
                await asyncio.sleep(backoff)
            else:
                log.error("All %d download attempts failed.", config.DOWNLOAD_MAX_RETRIES)

    raise DownloadError(f"Download failed after {config.DOWNLOAD_MAX_RETRIES} attempts: {last_error}")


def _read_local(path: Path) -> DownloadResult:
    """
    Read a local CSV file and present it as a DownloadResult.

    Computes sha256 the same way as the network path so deduplication still
    works. The path returned is the original path — caller should NOT delete
    it (it's not a temp file).
    """
    if not path.exists():
        raise DownloadError(f"Local CSV not found: {path}")

    sha = hashlib.sha256()
    byte_size = 0
    with path.open("rb") as f:
        while chunk := f.read(64 * 1024):
            sha.update(chunk)
            byte_size += len(chunk)

    log.info("Using local CSV %s (%d bytes)", path, byte_size)

    # Run the same validation as the network path
    _validate_csv(path, byte_size)

    return DownloadResult(
        path=path,
        sha256=sha.hexdigest(),
        byte_size=byte_size,
    )


async def _download_once() -> DownloadResult:
    """
    Single download attempt. Raises HTTPError or ValueError on failure.

    We stream the body to disk in chunks. The CSV is 5-6 MB — fits in RAM —
    but streaming keeps memory predictable and matches our 'bulk-friendly'
    approach throughout ETL.
    """
    log.info("Downloading CSV from %s", config.FUEL_PRICES_CSV_URL)

    # Save to a temp file. tempfile.NamedTemporaryFile with delete=False
    # so the file survives this function — caller will clean up.
    tmp = tempfile.NamedTemporaryFile(
        prefix="fuel_csv_", suffix=".csv", delete=False,
    )
    tmp_path = Path(tmp.name)
    tmp.close()

    sha = hashlib.sha256()
    byte_size = 0

    async with httpx.AsyncClient(timeout=config.DOWNLOAD_TIMEOUT_SECONDS) as client:
        async with client.stream("GET", config.FUEL_PRICES_CSV_URL) as response:
            response.raise_for_status()  # raises on 4xx/5xx

            with tmp_path.open("wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=64 * 1024):
                    sha.update(chunk)
                    f.write(chunk)
                    byte_size += len(chunk)

    # Sanity check the downloaded payload before declaring success.
    _validate_csv(tmp_path, byte_size)

    return DownloadResult(
        path=tmp_path,
        sha256=sha.hexdigest(),
        byte_size=byte_size,
    )


def _validate_csv(path: Path, byte_size: int) -> None:
    """
    Quick checks that gov.uk gave us a real CSV, not a maintenance page.

    Raises ValueError on failure — the caller treats this as a transient
    error and retries. This is correct: gov.uk does occasionally return
    HTML during maintenance.
    """
    if byte_size < config.DOWNLOAD_MIN_CSV_BYTES:
        raise ValueError(
            f"Downloaded file too small ({byte_size} bytes < "
            f"{config.DOWNLOAD_MIN_CSV_BYTES}). Probably an error page."
        )

    # Read just the first 4 KB for content checks. We don't need the whole file.
    with path.open("r", encoding="utf-8", errors="replace") as f:
        head = f.read(4096)

    head_lower = head.lower()
    if "<html" in head_lower or "<!doctype html" in head_lower:
        raise ValueError("Downloaded payload is HTML, not CSV (gov.uk maintenance page?).")

    if "," not in head:
        raise ValueError("Downloaded payload has no commas — doesn't look like a CSV.")

    # Optional: verify the first column is what we expect. This catches
    # silent format changes early, before the parser blows up.
    if not head.startswith("forecourt_update_timestamp"):
        # Don't raise — the parser will surface a clearer error if needed.
        # Just warn so we have a paper trail.
        log.warning(
            "CSV first column is not 'forecourt_update_timestamp'. "
            "Format may have changed. First 80 chars: %r",
            head[:80],
        )
