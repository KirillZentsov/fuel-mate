"""
Favourites handler.

Triggers:
  - "Favourites" button on main menu
  - /favourites command

Behaviour:
  - Empty state: explain how to add favourites.
  - Non-empty: header line + one card per favourite with its own Remove button.
  - Cleans up orphan rows (favourites pointing to deleted stations) and
    notifies the user once if any were removed (per spec section 10.5).

Note: the actual Remove callback (`rmv:<prefix>`) is handled by search.py,
because the same callback is used from the detail card. Code locality won
out over module boundaries — when a callback is shared, we put the handler
where the callback originates more often.
"""
import logging

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message

from bot import cards, keyboards, messages
from bot.repositories import favourites, users

log = logging.getLogger(__name__)

router = Router(name="favourites")


@router.message(Command("favourites"))
@router.message(F.text == messages.BTN_FAVOURITES)
async def show_favourites(message: Message) -> None:
    """Render the user's favourites list (or empty state)."""
    user_id = message.from_user.id
    log.info("Favourites view from user_id=%d", user_id)

    user = await users.get(user_id)
    if user is None:
        # Edge case — user got here without /start. Show empty state safely.
        await message.answer(
            messages.FAVOURITES_EMPTY,
            parse_mode="HTML",
            reply_markup=keyboards.main_menu(),
        )
        return

    # Step 1 — clean orphans first so we don't render stale data
    n_removed = await favourites.remove_orphans(user_id)
    if n_removed > 0:
        await message.answer(messages.FAVOURITES_ORPHAN_NOTICE, parse_mode="HTML")

    # Step 2 — fetch the (now clean) list
    favs = await favourites.list_for_user(user_id, fuel_type=user.fuel_type)

    if not favs:
        await message.answer(
            messages.FAVOURITES_EMPTY,
            parse_mode="HTML",
            reply_markup=keyboards.main_menu(),
        )
        return

    # Step 3 — header line
    await message.answer(
        messages.FAVOURITES_HEADER.format(count=len(favs)),
        parse_mode="HTML",
    )

    # Step 4 — one card per favourite, each with its own Remove button.
    # Sending separate messages lets the user remove one without the others
    # disappearing.
    for f in favs:
        text = cards.format_favourite_card(f, fuel=user.fuel_type)
        await message.answer(
            text,
            parse_mode="HTML",
            reply_markup=keyboards.favourite_card(f.station_id),
        )
