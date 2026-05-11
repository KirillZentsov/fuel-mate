"""
Help and fallback handlers.

This router is registered LAST so it acts as a catch-all:
  - /help, /unknown_cmd → reply with help / "try /help"
  - any other text the user sends that no other handler picked up → silent

Spec section 10.7: 'User sends random text in main menu — silent. Don't
be a chatty bot.' We deliberately do nothing for plain text that isn't
recognised by other handlers.
"""
import logging

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message

from bot import messages

log = logging.getLogger(__name__)

router = Router(name="help")


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    """Show the help text."""
    await message.answer(messages.HELP_TEXT, parse_mode="HTML")


@router.message(F.text.startswith("/"))
async def unknown_command(message: Message) -> None:
    """
    Any /command not handled by another router is unknown.
    We register this AFTER other routers so by the time we get a message,
    no one else wanted it.
    """
    log.info("Unknown command from user_id=%d", message.from_user.id)
    await message.answer(messages.UNKNOWN_COMMAND, parse_mode="HTML")


# Note: NO fallback for plain (non-command) text. Per spec 10.7, the bot
# stays silent. Random text is dropped without reply.
