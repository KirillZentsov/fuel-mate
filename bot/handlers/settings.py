"""
Settings handler.

Triggers:
  - "Settings" button on main menu
  - /settings command
  - /stop command (unsubscribe with confirmation)

Layout: a single message that gets edited as the user navigates between
sub-screens (root → fuel picker → root, etc.). This keeps the chat tidy
and matches the spec's "in-place" feel.
"""
import logging

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from bot import config, keyboards, messages
from bot.repositories import users
from shared import admin_notifier

log = logging.getLogger(__name__)

router = Router(name="settings")


# ──────────────────────────────────────────────────────────────────────
# Entry points
# ──────────────────────────────────────────────────────────────────────

@router.message(Command("settings"))
@router.message(F.text == messages.BTN_SETTINGS)
async def show_settings(message: Message) -> None:
    """Show the settings root screen."""
    user_id = message.from_user.id
    user = await users.get(user_id)
    if user is None:
        # Defensive — user should have gone through /start
        await message.answer(messages.SYSTEM_ERROR, parse_mode="HTML")
        return

    text = _settings_summary(user)
    await message.answer(
        text, parse_mode="HTML",
        reply_markup=keyboards.settings_root(),
    )


# ──────────────────────────────────────────────────────────────────────
# Root navigation
# ──────────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "set:root")
async def cb_settings_root(callback: CallbackQuery) -> None:
    """Go back to the settings root from a sub-screen."""
    await callback.answer()
    user = await users.get(callback.from_user.id)
    if user is None:
        return
    try:
        await callback.message.edit_text(
            _settings_summary(user),
            parse_mode="HTML",
            reply_markup=keyboards.settings_root(),
        )
    except Exception:  # noqa: BLE001
        pass


# ──────────────────────────────────────────────────────────────────────
# Fuel picker
# ──────────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "set:fuel")
async def cb_settings_fuel_screen(callback: CallbackQuery) -> None:
    """Show the fuel picker."""
    await callback.answer()
    user = await users.get(callback.from_user.id)
    if user is None:
        return
    try:
        await callback.message.edit_text(
            messages.SETTINGS_FUEL_PROMPT,
            parse_mode="HTML",
            reply_markup=keyboards.settings_fuel(current=user.fuel_type),
        )
    except Exception:  # noqa: BLE001
        pass


@router.callback_query(F.data.startswith("fuel:"))
async def cb_set_fuel(callback: CallbackQuery) -> None:
    """User picked a fuel — save and confirm."""
    await callback.answer()
    fuel_code = callback.data.split(":", 1)[1]
    if fuel_code not in messages.FUEL_LABELS:
        return

    await users.update_fuel(callback.from_user.id, fuel_code)
    fuel_label = messages.FUEL_LABELS[fuel_code]
    try:
        await callback.message.edit_text(
            messages.SETTINGS_FUEL_UPDATED.format(fuel=fuel_label),
            parse_mode="HTML",
            reply_markup=keyboards.settings_root(),
        )
    except Exception:  # noqa: BLE001
        pass


# ──────────────────────────────────────────────────────────────────────
# Radius picker
# ──────────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "set:radius")
async def cb_settings_radius_screen(callback: CallbackQuery) -> None:
    """Show the radius picker."""
    await callback.answer()
    user = await users.get(callback.from_user.id)
    if user is None:
        return
    try:
        await callback.message.edit_text(
            messages.SETTINGS_RADIUS_PROMPT,
            parse_mode="HTML",
            reply_markup=keyboards.settings_radius(current=user.search_radius),
        )
    except Exception:  # noqa: BLE001
        pass


# 'sra:' prefix here disambiguates from the 'rad:' empty-results retry in search.py.

@router.callback_query(F.data.startswith("sra:"))
async def cb_set_radius(callback: CallbackQuery) -> None:
    """User picked a radius — save and confirm."""
    await callback.answer()
    try:
        radius = int(callback.data.split(":", 1)[1])
    except ValueError:
        return
    if radius not in config.RADIUS_OPTIONS:
        return

    await users.update_radius(callback.from_user.id, radius)
    try:
        await callback.message.edit_text(
            messages.SETTINGS_RADIUS_UPDATED.format(radius=radius),
            parse_mode="HTML",
            reply_markup=keyboards.settings_root(),
        )
    except Exception:  # noqa: BLE001
        pass


# ──────────────────────────────────────────────────────────────────────
# Alerts picker
# ──────────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "set:alerts")
async def cb_settings_alerts_screen(callback: CallbackQuery) -> None:
    """Show the alerts mode picker."""
    await callback.answer()
    user = await users.get(callback.from_user.id)
    if user is None:
        return
    try:
        await callback.message.edit_text(
            "Choose your alert preference:",
            parse_mode="HTML",
            reply_markup=keyboards.settings_alerts(current=user.alerts_mode),
        )
    except Exception:  # noqa: BLE001
        pass


@router.callback_query(F.data.startswith("alm:"))
async def cb_set_alerts(callback: CallbackQuery) -> None:
    """User picked an alerts mode — save and confirm."""
    await callback.answer()
    mode = callback.data.split(":", 1)[1]
    if mode not in messages.ALERTS_MODE_LABELS:
        return

    await users.update_alerts_mode(callback.from_user.id, mode)

    confirmation = {
        "all_changes": messages.SETTINGS_ALERTS_ALL,
        "big_only":    messages.SETTINGS_ALERTS_BIG,
        "off":         messages.SETTINGS_ALERTS_OFF,
    }.get(mode, "")

    try:
        await callback.message.edit_text(
            confirmation,
            parse_mode="HTML",
            reply_markup=keyboards.settings_root(),
        )
    except Exception:  # noqa: BLE001
        pass


# ──────────────────────────────────────────────────────────────────────
# Unsubscribe / /stop
# ──────────────────────────────────────────────────────────────────────

@router.message(Command("stop"))
async def cmd_stop(message: Message, state: FSMContext) -> None:
    """First step of unsubscribe — show the confirmation dialog."""
    await state.clear()
    await message.answer(
        messages.UNSUBSCRIBE_CONFIRM,
        parse_mode="HTML",
        reply_markup=keyboards.unsubscribe_confirm(),
    )


@router.callback_query(F.data == "set:stop")
async def cb_settings_unsubscribe(callback: CallbackQuery) -> None:
    """'Unsubscribe' from the settings menu — same flow as /stop."""
    await callback.answer()
    try:
        await callback.message.edit_text(
            messages.UNSUBSCRIBE_CONFIRM,
            parse_mode="HTML",
            reply_markup=keyboards.unsubscribe_confirm(),
        )
    except Exception:  # noqa: BLE001
        pass


@router.callback_query(F.data == "cnf:del")
async def cb_confirm_delete(callback: CallbackQuery, state: FSMContext) -> None:
    """User confirmed deletion — actually delete and notify admin."""
    await callback.answer()
    user_id = callback.from_user.id

    # Capture stats before delete (for admin note)
    user = await users.get(user_id)
    days_since = None
    fav_count = 0
    if user is not None:
        from bot.repositories import favourites
        try:
            fav_count = await favourites.count_for_user(user_id)
        except Exception:  # noqa: BLE001
            pass

    await users.delete_user(user_id)
    await state.clear()

    try:
        await callback.message.edit_text(
            messages.UNSUBSCRIBE_DONE,
            parse_mode="HTML",
        )
    except Exception:  # noqa: BLE001
        await callback.message.answer(messages.UNSUBSCRIBE_DONE, parse_mode="HTML")

    # Admin notification (privacy: only user_id and aggregate stats)
    days_str = f"days_since={days_since}" if days_since is not None else "days_since=?"
    await admin_notifier.notify_info(
        f"User unsubscribed\n"
        f"User: <code>{user_id}</code>\n"
        f"Had favourites: {fav_count}"
    )


# Note: 'cnf:cancel' is handled in search.py (shared cancellation).


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def _settings_summary(user) -> str:
    """Format the settings root header with current values."""
    return messages.SETTINGS_HEADER.format(
        fuel=messages.FUEL_LABELS.get(user.fuel_type, user.fuel_type),
        radius=user.search_radius,
        alerts=messages.ALERTS_MODE_LABELS.get(user.alerts_mode, user.alerts_mode),
    )
