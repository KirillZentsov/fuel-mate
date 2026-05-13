"""
Fuel Finder API client.

Talks to the official UK government Fuel Finder API at
fuel-finder.service.gov.uk. Replaces the old CSV-download path which
was killed by gov.uk's WAF in early 2026 — now we authenticate with
OAuth2 (client credentials flow) and fetch JSON via paginated endpoints.

What this module provides:
  - `FuelFinderClient` — high-level async client that owns the token,
    handles retries, and yields stations and prices.

What it doesn't provide:
  - Parsing into our domain types (`ParsedStation`/`ParsedPrice`).
    That's `shared.csv_parser` — it has both CSV and JSON parsers.
  - Writing to the DB. That's `etl.load_staging`.

Rate limits we respect:
  - 100 RPM hard limit (we do ~16 batches × 2 endpoints = ~32 requests
    per run, far below).
  - 1 concurrent request per client — we don't fan out.
  - On 429, the docs say "Try again in 5 minutes" — we wait 300s.
  - On 5xx, exponential backoff (5s, 15s, 30s).

Error handling philosophy:
  - 403 on a non-auth endpoint means our token expired. Try once more
    after re-issuing the token. If it fails again, give up — something
    deeper is wrong.
  - 4xx other than 403/429 means we screwed up the request. Don't retry,
    raise to the caller.
  - 5xx/network: retry with backoff. If all retries fail, raise.

Token caching:
  - For MVP we get a fresh token at client construction. The ETL run
    takes ~30-60s, well within the 1h token lifetime.
  - We do NOT use the refresh_token — full re-auth each run is simpler
    and the docs explicitly allow it (we're not on a hot path).
  - Future: cache the token in a Supabase table for cross-run reuse.
"""
import asyncio
import logging
from dataclasses import dataclass
from typing import AsyncIterator, Optional

import httpx

from etl import config

log = logging.getLogger(__name__)


# ── Exceptions ───────────────────────────────────────────────────────

class FuelFinderError(Exception):
    """Base class. Catch this for "ETL download stage failed"."""


class FuelFinderAuthError(FuelFinderError):
    """OAuth failed — invalid credentials or unreachable token endpoint."""


class FuelFinderRateLimitError(FuelFinderError):
    """429 returned. Caller should not retry quickly."""


class FuelFinderUpstreamError(FuelFinderError):
    """5xx or network failure after all retries."""


# ── Token + result types ─────────────────────────────────────────────

@dataclass
class _Token:
    """Internal — current OAuth credentials."""
    access_token: str
    token_type: str = "Bearer"
    expires_in: int = 3600
    # Production API doesn't always return refresh tokens; we accept the
    # absence gracefully. We don't use refresh in MVP anyway.
    refresh_token: str = ""
    refresh_token_expires_in: int = 0


# ── The client ───────────────────────────────────────────────────────

class FuelFinderClient:
    """
    Async client for fuel-finder.service.gov.uk.

    Usage:
        async with FuelFinderClient(client_id=..., client_secret=...) as c:
            async for station in c.iter_stations():
                ...
            async for station_with_prices in c.iter_prices():
                ...

    The context manager ensures the underlying httpx.AsyncClient is closed.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        base_url: str = "https://www.fuel-finder.service.gov.uk",
        timeout_seconds: float = 90.0,
    ):
        if not client_id or not client_secret:
            raise FuelFinderAuthError(
                "client_id and client_secret are required. "
                "Set FUEL_FINDER_CLIENT_ID and FUEL_FINDER_CLIENT_SECRET."
            )
        self._client_id = client_id
        self._client_secret = client_secret
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._http: Optional[httpx.AsyncClient] = None
        self._token: Optional[_Token] = None

    async def __aenter__(self) -> "FuelFinderClient":
        self._http = httpx.AsyncClient(timeout=self._timeout)
        await self._authenticate()
        return self

    async def __aexit__(self, *exc) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    # ── Auth ─────────────────────────────────────────────────────────

    async def _authenticate(self) -> None:
        """
        Exchange client_id/secret for an access token.

        Called once at startup. We don't refresh proactively — for an ETL
        run of <60s, the 1-hour token comfortably covers us.
        """
        url = f"{self._base_url}/api/v1/oauth/generate_access_token"
        log.info("Authenticating with Fuel Finder API…")

        try:
            r = await self._http.post(
                url,
                json={
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                },
                headers={"Content-Type": "application/json"},
            )
        except httpx.HTTPError as exc:
            raise FuelFinderAuthError(f"Token endpoint unreachable: {exc}") from exc

        if r.status_code == 401:
            raise FuelFinderAuthError(
                "Invalid client credentials (401). Check FUEL_FINDER_CLIENT_ID "
                "and FUEL_FINDER_CLIENT_SECRET."
            )
        if r.status_code >= 400:
            raise FuelFinderAuthError(
                f"Token endpoint returned HTTP {r.status_code}: {r.text[:200]}"
            )

        try:
            body = r.json()
        except ValueError as exc:
            raise FuelFinderAuthError(
                f"Token endpoint returned non-JSON: {r.text[:200]}"
            ) from exc

        # The data envelope is mandatory.
        data = body.get("data") or {}
        access_token = data.get("access_token")
        if not access_token:
            raise FuelFinderAuthError(
                f"Token endpoint response missing 'data.access_token'. "
                f"Got fields: {list(data.keys())}. Body head: {r.text[:200]}"
            )

        # Everything else has sensible fallbacks. Staging docs document
        # refresh_token + refresh_token_expires_in but production omits
        # them — we don't actually use refresh tokens, so this is fine.
        self._token = _Token(
            access_token=access_token,
            token_type=data.get("token_type", "Bearer"),
            expires_in=int(data.get("expires_in") or 3600),
            refresh_token=data.get("refresh_token", ""),
            refresh_token_expires_in=int(data.get("refresh_token_expires_in") or 0),
        )

        # NEVER log the actual token — even at DEBUG level. Just confirm success.
        if self._token.refresh_token_expires_in:
            log.info(
                "Authenticated. Access token valid for %ds; refresh token for %ds.",
                self._token.expires_in,
                self._token.refresh_token_expires_in,
            )
        else:
            log.info(
                "Authenticated. Access token valid for %ds (no refresh token in response).",
                self._token.expires_in,
            )

    def _auth_headers(self) -> dict:
        if self._token is None:
            raise FuelFinderAuthError("Not authenticated. Use as async context manager.")
        return {"Authorization": f"{self._token.token_type} {self._token.access_token}"}

    # ── GET with retry ───────────────────────────────────────────────

    async def _get_with_retry(self, path: str, params: dict) -> list | None:
        """
        GET <base>/<path>?<params> with retry. Returns the parsed JSON body
        (an array of station objects per the API spec), or None if the
        endpoint signalled end-of-pagination via HTTP 404.

        Retry policy:
          - 5xx, network errors  → backoff [5s, 15s, 30s], then give up
          - 429                  → wait 300s, retry once
          - 403                  → re-auth once, retry once
          - 404                  → return None (end of paginated stream)
          - 4xx other            → no retry, raise immediately
        """
        url = f"{self._base_url}{path}"
        backoffs = [5, 15, 30]
        retried_auth = False
        retried_rate_limit = False

        for attempt in range(len(backoffs) + 1):
            try:
                r = await self._http.get(url, params=params, headers=self._auth_headers())
            except httpx.HTTPError as exc:
                if attempt < len(backoffs):
                    log.warning(
                        "GET %s attempt %d failed (%s). Retrying in %ds…",
                        path, attempt + 1, exc, backoffs[attempt],
                    )
                    await asyncio.sleep(backoffs[attempt])
                    continue
                raise FuelFinderUpstreamError(
                    f"GET {path} failed after retries: {exc}"
                ) from exc

            # Server responded — interpret the status code.
            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError as exc:
                    raise FuelFinderUpstreamError(
                        f"GET {path} returned non-JSON body: {r.text[:200]}"
                    ) from exc

            if r.status_code == 403 and not retried_auth:
                # Token might have expired mid-run. Re-auth once.
                log.warning("Got 403 on %s. Re-authenticating and retrying once.", path)
                await self._authenticate()
                retried_auth = True
                continue

            if r.status_code == 404:
                # The gov.uk API uses 404 to signal end-of-pagination — the
                # batch number we asked for doesn't exist because we've
                # walked past the last batch. The body looks like:
                #   {"data": {"message": "Requested batch N is not available"}}
                # We treat this as a normal "no more data" signal, not an error.
                log.debug("%s batch %s: 404 (end of pagination).",
                          path, params.get("batch-number"))
                return None

            if r.status_code == 429 and not retried_rate_limit:
                # The docs literally say "Try again in 5 minutes". Don't be cute.
                log.warning("Rate limited (429) on %s. Waiting 300s before single retry.", path)
                await asyncio.sleep(300)
                retried_rate_limit = True
                continue

            if 500 <= r.status_code < 600:
                if attempt < len(backoffs):
                    log.warning(
                        "GET %s attempt %d got HTTP %d. Retrying in %ds…",
                        path, attempt + 1, r.status_code, backoffs[attempt],
                    )
                    await asyncio.sleep(backoffs[attempt])
                    continue
                raise FuelFinderUpstreamError(
                    f"GET {path} got HTTP {r.status_code} after retries: {r.text[:200]}"
                )

            # 4xx other than 403/429 — not our problem to retry.
            raise FuelFinderError(
                f"GET {path} returned HTTP {r.status_code}: {r.text[:200]}"
            )

        # Unreachable.
        raise FuelFinderUpstreamError(f"GET {path} fell through retry loop unexpectedly.")

    # ── Iterators over paginated endpoints ───────────────────────────

    async def iter_stations(self) -> AsyncIterator[dict]:
        """
        Yield station metadata objects from `/api/v1/pfs`, one at a time.

        Stops when a batch returns an empty array (end-of-pagination signal).
        """
        async for obj in self._iter_paginated("/api/v1/pfs"):
            yield obj

    async def iter_prices(self) -> AsyncIterator[dict]:
        """
        Yield price-per-station objects from `/api/v1/pfs/fuel-prices`.

        Each yielded object has node_id + a list of {fuel_type, price, …}.
        """
        async for obj in self._iter_paginated("/api/v1/pfs/fuel-prices"):
            yield obj

    async def _iter_paginated(self, path: str) -> AsyncIterator[dict]:
        """
        Walk the batch-number pagination until we hit the end.

        The API signals end-of-stream in two possible ways:
          - HTTP 404 with body "Requested batch N is not available"
            (observed in production)
          - HTTP 200 with an empty array
            (legal per the spec; not seen in practice but cheap to support)

        Either way we stop iterating.
        """
        batch = 1
        total = 0
        while True:
            page = await self._get_with_retry(path, {"batch-number": batch})

            if page is None:
                # 404 — past the last batch.
                log.info("%s: paginated through %d batches, %d total items.",
                         path, batch - 1, total)
                return

            # Tolerate {data: [...]} envelope just in case the API ever
            # wraps the array — staging docs and prod responses both use
            # a bare array but defensive parsing costs nothing.
            if isinstance(page, dict) and "data" in page:
                page = page["data"]
            if not isinstance(page, list):
                raise FuelFinderUpstreamError(
                    f"{path} batch {batch}: expected a list, got {type(page).__name__}"
                )

            if not page:
                # Empty array — also end of stream.
                log.info("%s: paginated through %d batches, %d total items.",
                         path, batch - 1, total)
                return

            for item in page:
                yield item
                total += 1

            batch += 1


# ── Convenience function (matches the old download_csv signature shape) ─

async def fetch_all(
    client_id: str,
    client_secret: str,
) -> tuple[list[dict], list[dict]]:
    """
    Convenience helper: pull every station + every price into two lists.

    Used by `etl.download` to keep the pipeline orchestrator simple. The
    in-memory load is fine — at ~8000 stations + ~25000 prices the total
    payload is ~10-15 MB.

    Returns:
        (stations_raw, prices_raw) — exactly as the API returns them, before
        normalisation. The parser in `shared.csv_parser` consumes these.
    """
    stations_raw: list[dict] = []
    prices_raw: list[dict] = []

    async with FuelFinderClient(
        client_id=client_id, client_secret=client_secret,
    ) as client:
        async for s in client.iter_stations():
            stations_raw.append(s)
        async for p in client.iter_prices():
            prices_raw.append(p)

    log.info("API fetch complete: %d stations, %d price records.",
             len(stations_raw), len(prices_raw))
    return stations_raw, prices_raw
