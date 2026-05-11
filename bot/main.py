"""
Bot entry point.

Run:
    python -m bot.main

Behaviour:
  1. Bootstrap: init DB pool, load postcodes file into RAM.
  2. Register routers in priority order (more specific first).
  3. Install error middleware (catches handler exceptions, sends admin alert,
     replies with friendly fallback).
  4. Send 🚀 admin notification — bot is starting.
  5. Run long-polling loop until SIGINT/SIGTERM.
  6. Graceful shutdown: close pool, notify admin (best effort).

Polling, not webhooks: see CLAUDE.md rule #7.
"""
import asyncio
import logging
import sys
from typing import Any, Awaitable, Callable

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, TelegramObject

from bot import config, db, messages
from bot.handlers import favourites, help as help_handler, search, settings, start
from bot.repositories import postcodes
from shared import admin_notifier

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Error middleware
# ──────────────────────────────────────────────────────────────────────

async def error_middleware(
    handler: Callable[[TelegramObject, dict], Awaitable[Any]],
    event: TelegramObject,
    data: dict,
) -> Any:
    """
    Catch any exception escaping a handler.

    - Log the full traceback.
    - Send admin a CRITICAL notification with user_id only (privacy).
    - Reply to the user with the generic SYSTEM_ERROR text.

    The bot keeps running. One crashed handler call must not bring down
    the whole process.
    """
    try:
        return await handler(event, data)
    except Exception as exc:  # noqa: BLE001
        # Try to extract user_id for the admin note (privacy: only the ID)
        user_id: int | None = None
        if isinstance(event, Message):
            user_id = event.from_user.id if event.from_user else None
        elif hasattr(event, "from_user") and event.from_user:
            user_id = event.from_user.id

        log.exception("Unhandled exception in handler")

        # Notify admin (best effort — can't let admin notifier failure cascade)
        try:
            await admin_notifier.notify_critical(
                f"Bot crashed in handler\n"
                f"User: <code>{user_id}</code>\n"
                f"Error: <code>{type(exc).__name__}: {str(exc)[:200]}</code>"
            )
        except Exception:  # noqa: BLE001
            pass

        # Reply to the user, if we have a Message we can reply to
        if isinstance(event, Message):
            try:
                await event.answer(messages.SYSTEM_ERROR, parse_mode="HTML")
            except Exception:  # noqa: BLE001
                pass

        # Don't re-raise — we've handled it.
        return None


# ──────────────────────────────────────────────────────────────────────
# Bootstrap
# ──────────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    """Configure root logger before anything else logs."""
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quiet down aiogram's inner chatter — we only want our own logs at INFO
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)
    logging.getLogger("aiogram.dispatcher").setLevel(logging.WARNING)


async def on_startup() -> None:
    """Run before polling starts. Failures here abort the bot."""
    log.info("Initialising database pool…")
    await db.init_pool()

    log.info("Loading postcodes…")
    postcodes.load_postcodes()

    log.info("Bot startup complete.")
    # Best-effort deploy notification. If admin channel isn't configured,
    # admin_notifier silently skips; that's fine.
    await admin_notifier.notify_deploy("Bot started")


async def on_shutdown(bot: Bot) -> None:
    """Run during shutdown. Best-effort cleanup."""
    log.info("Shutting down…")
    try:
        await admin_notifier.notify_info("Bot shutting down")
    except Exception:  # noqa: BLE001
        pass
    try:
        await db.close_pool()
    except Exception:  # noqa: BLE001
        pass
    try:
        await bot.session.close()
    except Exception:  # noqa: BLE001
        pass


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    setup_logging()
    log.info("Fuel Mate bot starting up.")

    # All bot messages default to HTML parse mode unless overridden per-call
    bot = Bot(
        token=config.TELEGRAM_BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp = Dispatcher(storage=MemoryStorage())

    # Middleware — registers for both message and callback updates
    dp.message.middleware(error_middleware)
    dp.callback_query.middleware(error_middleware)

    # Router order matters — more specific routers first, catch-all last.
    # Within each router, aiogram dispatches by F.* filters in registration order.
    dp.include_router(start.router)
    dp.include_router(search.router)
    dp.include_router(favourites.router)
    dp.include_router(settings.router)
    dp.include_router(help_handler.router)  # catch-all, must be last

    # Bootstrap (DB, postcodes) and announce on Telegram
    await on_startup()

    try:
        # Drop any pending updates from before the bot was running.
        # If we don't, a queue of stale messages from a previous crash could
        # bombard the bot at startup.
        await bot.delete_webhook(drop_pending_updates=True)

        log.info("Polling for updates…")
        await dp.start_polling(bot)
    finally:
        await on_shutdown(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Bot stopped by signal.")
        sys.exit(0)
