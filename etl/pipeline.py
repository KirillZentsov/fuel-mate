"""
ETL pipeline orchestrator.

Runs all stages in sequence, measures timing, and emits admin notifications
on failure or success. Designed to be invoked from GitHub Actions via:

    python -m etl.pipeline

For local development:

    python -m etl.pipeline --local-csv path/to/file.csv

Stage flow (per spec section 6.2):
  1. download   — fetch CSV from gov.uk (or read local file)
  2. dedup      — check sha256 in raw.fuel_data_dumps; short-circuit if known
  3. archive    — upload CSV to GitHub Release (best-effort, optional)
  4. load       — INSERT raw row + COPY into staging.{stations,prices}
  5. refresh    — UPSERT mart.{stations,prices_current} + INSERT prices_history
  6. notify     — admin channel: ✅ OK / ℹ️ skipped / 🚨 failed

Stages 6 (alerts) and 7 (daily summary) live in separate modules — see
spec section 6.8 and 6.10. They run after the bot is in place.
"""
import argparse
import asyncio
import contextlib
import logging
import sys
import time
from pathlib import Path

from etl import config
from etl.download import download_csv, DownloadError
from etl.upload_release import upload_to_release, UploadError
from etl.load_staging import load_csv_into_staging
from etl.refresh_mart import refresh_mart
from shared import admin_notifier

log = logging.getLogger(__name__)


# Stage timings, populated as we run. Used for the admin summary.
_timings: dict[str, float] = {}


@contextlib.contextmanager
def _timed(stage_name: str):
    """Context manager that records how long a stage took."""
    start = time.monotonic()
    try:
        yield
    finally:
        _timings[stage_name] = time.monotonic() - start
        log.info("Stage %r took %.1fs", stage_name, _timings[stage_name])


async def run_pipeline(local_csv: Path | None = None) -> int:
    """
    Run the full ETL pipeline. Returns process exit code (0 OK, 1 failure).

    Args:
        local_csv: optional path to a local CSV. If provided, the network
            download is skipped.
    """
    overall_start = time.monotonic()
    log.info("ETL pipeline starting (DRY_RUN=%s, local=%s)",
             config.DRY_RUN, local_csv)

    try:
        # Stage 1 — download
        with _timed("download"):
            dl = await download_csv(local_csv_path=local_csv)
        log.info("Downloaded %d bytes, sha256=%s…", dl.byte_size, dl.sha256[:12])

        # Stage 2 — dedup short-circuit lives inside load_csv_into_staging.
        # We can't avoid the upload before knowing duplicate status, so the
        # archive step happens AFTER load (slight reordering vs spec section
        # 6.2 — described in the comment block at top of this file).

        # Stage 3 — load (also handles dedup)
        with _timed("load_staging"):
            load_result = await load_csv_into_staging(
                csv_path=dl.path,
                sha256=dl.sha256,
                file_name=Path(dl.path).name,
            )

        if load_result.is_duplicate:
            await _notify_duplicate(dl.sha256)
            return 0

        # Stage 4 — archive to GitHub Release (best-effort).
        # Failure here is non-fatal: the data is already in the database.
        # We log a warning, send a non-critical admin note, and continue.
        release_url: str | None = None
        with _timed("upload_release"):
            try:
                release_url = await upload_to_release(dl.path, dl.sha256)
            except UploadError as exc:
                log.warning("Archive upload failed (non-fatal): %s", exc)
                await admin_notifier.notify_warning(
                    f"GitHub release upload failed: {exc}\n"
                    f"Data already in DB. Continuing."
                )

        if release_url:
            # Update the raw row with the URL, now that we have it.
            await _update_release_url(load_result.dump_id, release_url)

        # Stage 5 — refresh mart
        with _timed("refresh_mart"):
            refresh_result = await refresh_mart(dump_id=load_result.dump_id)

        # Stage 6 — admin OK notification
        total_seconds = time.monotonic() - overall_start
        await _notify_success(load_result, refresh_result, total_seconds)

        # Slow-run warning
        if total_seconds > config.ETL_SLOW_THRESHOLD_SECONDS:
            await _notify_slow(total_seconds)

        return 0

    except DownloadError as exc:
        await admin_notifier.notify_critical(
            f"ETL failed at stage: download\n{exc}"
        )
        log.exception("Pipeline failed at download stage")
        return 1

    except Exception as exc:  # noqa: BLE001
        # Catch-all for parser errors, asyncpg errors, network glitches, etc.
        # Identify which stage we crashed in by inspecting which timings are
        # already recorded.
        last_stage = list(_timings.keys())[-1] if _timings else "unknown"
        await admin_notifier.notify_critical(
            f"ETL failed at stage: {last_stage}\n"
            f"<code>{type(exc).__name__}: {exc}</code>"
        )
        log.exception("Pipeline failed at stage %r", last_stage)
        return 1


# ──────────────────────────────────────────────────────────────────────
# Admin-notification helpers
# ──────────────────────────────────────────────────────────────────────

async def _notify_success(load_result, refresh_result, total_seconds: float) -> None:
    """Send the per-spec ETL completion summary."""
    timing_lines = "\n".join(
        f"  · {name}: {dur:.1f}s" for name, dur in _timings.items()
    )
    text = (
        f"<b>ETL completed</b>\n"
        f"Duration: {total_seconds:.1f}s\n"
        f"{timing_lines}\n"
        f"Stations: {load_result.station_count}\n"
        f"Price changes: {refresh_result.prices_changed}\n"
        f"Dump ID: {load_result.dump_id}"
    )
    await admin_notifier.notify_ok(text)


async def _notify_duplicate(sha256: str) -> None:
    """Spec section 6.9 — duplicate sha256 deserves an INFO note, not OK."""
    text = (
        f"ETL skipped — duplicate CSV\n"
        f"Same sha256 (<code>{sha256[:12]}…</code>) as last run."
    )
    await admin_notifier.notify_info(text)


async def _notify_slow(seconds: float) -> None:
    """Per spec section 8.4 — slow-ETL warning."""
    timing_lines = "\n".join(
        f"  · {name}: {dur:.1f}s" for name, dur in _timings.items()
    )
    text = (
        f"ETL took {seconds:.0f}s "
        f"(threshold {config.ETL_SLOW_THRESHOLD_SECONDS}s)\n"
        f"Stages:\n{timing_lines}"
    )
    await admin_notifier.notify_warning(text)


async def _update_release_url(dump_id: int, release_url: str) -> None:
    """Patch the raw.fuel_data_dumps row with the GitHub Release URL."""
    import asyncpg
    conn = await asyncpg.connect(config.DATABASE_URL)
    try:
        await conn.execute(
            "UPDATE raw.fuel_data_dumps SET release_url = $1 WHERE id = $2",
            release_url, dump_id,
        )
    finally:
        await conn.close()


# ──────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────

def _setup_logging() -> None:
    """Configure logging once at startup."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fuel Mate ETL pipeline",
    )
    parser.add_argument(
        "--local-csv",
        type=Path,
        default=None,
        help="Path to a local CSV file. Skips the network download. "
             "Useful for development and replaying past CSVs.",
    )
    return parser.parse_args()


def main() -> int:
    _setup_logging()
    args = _parse_args()
    return asyncio.run(run_pipeline(local_csv=args.local_csv))


if __name__ == "__main__":
    sys.exit(main())
