"""
Stage 2 of the ETL pipeline: archive the CSV to GitHub Releases.

GitHub Releases give us free, immutable, public-URL storage for the raw
CSVs. Each successful download creates a new release, attaching the CSV
as an asset.

Why this matters:
  - Audit trail (every CSV ever processed is preserved with timestamp).
  - Replay (we can rebuild mart from staging by re-loading any past CSV).
  - No extra infrastructure (S3, Cloudflare R2 etc).

This stage is optional. If GITHUB_TOKEN or GITHUB_REPO is not set, we skip
it and continue. The orchestrator stores release_url=NULL in raw.fuel_data_dumps
and ETL still works — just without the archive.

Authentication: uses GITHUB_TOKEN. In GitHub Actions this is provided
automatically by the runner; for local runs you'd use a personal access
token with `repo` scope.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

import httpx

from etl import config

log = logging.getLogger(__name__)


# GitHub REST API endpoints
_API_BASE = "https://api.github.com"
_UPLOAD_BASE = "https://uploads.github.com"


class UploadError(Exception):
    """Raised when upload fails. Caller decides whether to fail-hard or skip."""


async def upload_to_release(csv_path: Path, sha256: str) -> str | None:
    """
    Upload the given CSV to a new GitHub Release.

    Args:
        csv_path: local path to the CSV file.
        sha256: digest, used in the release tag for traceability.

    Returns:
        Public URL of the uploaded asset, or None if archival was skipped
        (because GITHUB_TOKEN/GITHUB_REPO weren't configured).

    Raises:
        UploadError on actual API failure.
    """
    if not config.GITHUB_TOKEN or not config.GITHUB_REPO:
        log.info("GitHub archival skipped (GITHUB_TOKEN/GITHUB_REPO not set).")
        return None

    # Tag: fuel-prices-YYYY-MM-DD-HHMM, with hash suffix for uniqueness
    now = datetime.now(timezone.utc)
    tag = f"fuel-prices-{now:%Y-%m-%d-%H%M}-{sha256[:8]}"
    release_name = f"Fuel prices · {now:%Y-%m-%d %H:%M} UTC"

    headers = {
        "Authorization": f"Bearer {config.GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    async with httpx.AsyncClient(timeout=60, headers=headers) as client:
        # 1. Create the release
        create_url = f"{_API_BASE}/repos/{config.GITHUB_REPO}/releases"
        create_payload = {
            "tag_name": tag,
            "name": release_name,
            "body": (
                f"Raw fuel-prices CSV.\n\n"
                f"- Downloaded at: {now.isoformat()}\n"
                f"- SHA-256: `{sha256}`\n"
                f"- File size: {csv_path.stat().st_size} bytes"
            ),
            "draft": False,
            "prerelease": False,
        }
        r = await client.post(create_url, json=create_payload)
        if r.status_code not in (200, 201):
            raise UploadError(f"Release creation failed: HTTP {r.status_code} — {r.text[:200]}")

        release = r.json()
        upload_url_template = release["upload_url"]
        # Strip the OpenAPI-style suffix '{?name,label}' if present
        upload_url = upload_url_template.split("{")[0]

        # 2. Upload the asset
        asset_name = csv_path.name
        upload_full = f"{upload_url}?name={asset_name}"
        upload_headers = {**headers, "Content-Type": "text/csv"}

        with csv_path.open("rb") as f:
            content = f.read()

        r2 = await client.post(
            upload_full,
            content=content,
            headers=upload_headers,
        )
        if r2.status_code not in (200, 201):
            raise UploadError(f"Asset upload failed: HTTP {r2.status_code} — {r2.text[:200]}")

        asset_url = r2.json().get("browser_download_url")
        log.info("Uploaded CSV to GitHub Release: %s", asset_url)
        return asset_url
