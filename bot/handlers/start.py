"""
/start command — entry point for new users and "welcome back" for returning ones.

Two-step onboarding for new users (per spec section 7.2):
  Step 1: pick fuel type (inline buttons)
  Step 2: hint about Find Fuel (with main menu visible)

Returning users get a single welcome-back line with their settings summary.
"""
import logging

from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from bot import keyboards, messages
from bot.repositories import users
from shared import admin_notifier

log = logging.getLogger(__name__)

router = Router(name="start")


# Milestones at which we send a special admin notification (per spec 8.4).
_MILESTONES = (10, 25, 50, 100, 250, 500, 1000, 2500, 5000)


# ──────────────────────────────────────────────────────────────────────
# /start command
# ──────────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    """
    /start handler.

    - New user: show step-1 fuel picker.
    - Returning user: clear any FSM state, show welcome back + main menu.
    """
    user_id = message.from_user.id
    log.info("Got /start from user_id=%d", user_id)

    # Always clear FSM — /start is a hard reset
    await state.clear()

    user, is_new = await users.get_or_create(user_id)

    if is_new:
        # Step 1 of 2 — fuel choice
        await message.answer(
            messages.WELCOME_NEW,
            parse_mode="HTML",
            reply_markup=_onboarding_fuel_keyboard(),
        )
        # Notify admin asynchronously — don't let a Telegram outage block onboarding
        await _notify_new_user(user_id)
    else:
        # Returning user
        await message.answer(
            messages.WELCOME_BACK.format(
                fuel=messages.FUEL_LABELS.get(user.fuel_type, user.fuel_type),
                radius=user.search_radius,
                alerts=messages.ALERTS_MODE_LABELS.get(user.alerts_mode, user.alerts_mode),
            ),
            parse_mode="HTML",
            reply_markup=keyboards.main_menu(),
        )


# ──────────────────────────────────────────────────────────────────────
# Onboarding step 1 — fuel pick
# ──────────────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("onb_fuel:"))
async def cb_onboarding_fuel(callback: CallbackQuery, state: FSMContext) -> None:
    """
    User tapped a fuel button during onboarding.

    Save their choice and move to step 2.
    """
    await callback.answer()  # dismiss the loading spinner
    fuel_code = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id

    if fuel_code not in messages.FUEL_LABELS:
        log.warning("Unknown fuel code in callback: %r", fuel_code)
        return

    await users.update_fuel(user_id, fuel_code)

    # Edit the original step-1 message: turn it into the step-2 instructions.
    fuel_label = messages.FUEL_LABELS[fuel_code]
    try:
        await callback.message.edit_text(
            messages.ONBOARDING_STEP2.format(fuel=fuel_label),
            parse_mode="HTML",
        )
    except Exception:  # noqa: BLE001 — message might already be unmodifiable
        # Fallback: send a new message
        await callback.message.answer(
            messages.ONBOARDING_STEP2.format(fuel=fuel_label),
            parse_mode="HTML",
        )

    # Show the main menu (it appears as a reply keyboard, separate from the message)
    await callback.message.answer(
        "👇",  # tiny pointer so the keyboard appearance feels intentional
        reply_markup=keyboards.main_menu(),
    )


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def _onboarding_fuel_keyboard() -> InlineKeyboardMarkup:
    """Step-1 keyboard: 4 fuel options as inline buttons."""
    rows = [
        [InlineKeyboardButton(
            text=label,
            callback_data=f"onb_fuel:{code}",
        )]
        for code, label in messages.FUEL_LABELS.items()
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _notify_new_user(user_id: int) -> None:
    """Send admin notifications for new registrations and milestones."""
    try:
        total = await users.total_active_count()
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to count users for admin notification: %s", exc)
        return

    # Always announce a new signup
    await admin_notifier.notify_info(
        f"New user\nUser: <code>{user_id}</code>\nTotal users now: {total}"
    )

    # Milestone bonus
    if total in _MILESTONES:
        await admin_notifier.notify_info(f"Milestone — {total} users")
