"""
Admin notification helper — sends messages to the private Telegram admin channel.

Used by both the bot and the ETL pipeline to surface technical and business
events: deploys, errors, daily summaries, milestone notifications, etc.

Hard rules (from spec sections 8.1, 8.5):
  - NEVER include user names, usernames, postcodes, coordinates, or any
    free-form text the user typed. Only user_id (numeric) is allowed for debugging.
  - NEVER raise exceptions to caller. Telegram outages must not crash the bot
    or kill the ETL run. We log and move on.

Configuration (env vars):
  - TELEGRAM_BOT_TOKEN   — same bot used for end-user messaging
  - ADMIN_CHAT_ID        — numeric chat_id of the private channel
                           (negative number for channels)

Throttling: identical messages within 5 minutes are suppressed to avoid
spamming the channel during a thrashing failure.
"""
import os
import time
import logging

import httpx

log = logging.getLogger(__name__)

_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
_CHAT_ID = os.getenv("ADMIN_CHAT_ID")

# In-process throttle. This is per-process — the bot and ETL run in different
# processes and their throttle states are independent. That's fine: ETL runs
# briefly so its state doesn't matter; the bot is the long-lived process.
_last_sent: dict[str, float] = {}
_THROTTLE_SECONDS = 300

# HTTP timeout — be patient enough for slow networks but fail fast on outages
_HTTP_TIMEOUT = 10


async def _send(severity: str, text: str) -> None:
    """
    Internal sender. Catches all exceptions — never raises.

    Severity is a short emoji marker prepended to the message. Sound is
    silenced for benign events (INFO, OK, DEPLOY) so the owner's phone
    only buzzes for actionable issues (CRITICAL, WARNING).
    """
    if not _TOKEN or not _CHAT_ID:
        log.warning("Admin notifier not configured — TELEGRAM_BOT_TOKEN or "
                    "ADMIN_CHAT_ID is missing. Skipping notification.")
        return

    full_text = f"{severity} {text}"

    # Throttle by first 100 chars — same root message in tight loop is suppressed.
    # Different messages bypass the throttle, so we never lose unique events.
    key = full_text[:100]
    now = time.time()
    if _last_sent.get(key, 0) > now - _THROTTLE_SECONDS:
        log.info("Throttled admin notification: %s", key[:50])
        return
    _last_sent[key] = now

    # Don't notify the owner's phone for routine events
    silent = severity in ("ℹ️", "✅", "🚀")

    url = f"https://api.telegram.org/bot{_TOKEN}/sendMessage"
    payload = {
        "chat_id": _CHAT_ID,
        "text": full_text,
        "parse_mode": "HTML",
        "disable_notification": silent,
    }

    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            r = await client.post(url, json=payload)
            if r.status_code != 200:
                # Log only the status — body might contain the bot token-derived URL
                log.warning("Admin notification failed: HTTP %d", r.status_code)
    except Exception as exc:  # noqa: BLE001 — defensive notifier
        # We must never propagate. Log and continue.
        log.warning("Admin notification error: %s", exc)


# ──────────────────────────────────────────────────────────────────────
# Public API — one function per severity per spec section 8.3
# ──────────────────────────────────────────────────────────────────────

async def notify_critical(text: str) -> None:
    """🚨 Need action now — bot crashed, DB unreachable, ETL failed 3x."""
    await _send("🚨", text)


async def notify_warning(text: str) -> None:
    """⚠️ Potential issue — slow ETL, rate limit, stale data."""
    await _send("⚠️", text)


async def notify_info(text: str) -> None:
    """ℹ️ Business event — new user, milestone, ETL skipped (duplicate)."""
    await _send("ℹ️", text)


async def notify_ok(text: str) -> None:
    """✅ Planned operation — ETL completed, daily summary."""
    await _send("✅", text)


async def notify_deploy(text: str) -> None:
    """🚀 Code release — new bot version deployed."""
    await _send("🚀", text)
