"""
Telegram keyboards — both Reply (main menu) and Inline (everywhere else).

Spec section 7.7 describes the keyboard structure. This module produces
aiogram-compatible objects on demand; nothing here keeps state.

callback_data format (all <= 64 bytes):
  det:<prefix16>            show detail card for station whose ID starts with prefix
  trk:<prefix16>            track this station
  rmv:<prefix16>            remove from favourites
  rpl:<old16>:<new16>       replace favourite (limit-full flow)
  srt:cheap | srt:near      change sort mode in results
  pg:<N>                    paginate to page N
  rad:<N>                   re-search at radius N
  fuel:<e10|e5|b7s|b7p>     set fuel type
  alm:<all_changes|big_only|off>   set alerts mode
  cnf:<del|cancel>          confirmation buttons (currently only /stop)
  noop                      placeholder for buttons that do nothing locally

We never embed user data in callback_data — it's all opaque IDs, never
strings the user typed.
"""
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

from bot import config, messages


# ──────────────────────────────────────────────────────────────────────
# Reply keyboards (main menu — persistent at bottom of chat)
# ──────────────────────────────────────────────────────────────────────

def main_menu() -> ReplyKeyboardMarkup:
    """
    Persistent main menu — single row with two main actions:
       [ Find Fuel ] [ Favourites ]

    Settings and other less-frequent actions live in the bot commands menu
    (configured via @BotFather → /setcommands), accessible via the blue
    burger button in Telegram clients.
    """
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=messages.BTN_FIND_FUEL),
                KeyboardButton(text=messages.BTN_FAVOURITES),
            ],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def find_fuel_choice() -> ReplyKeyboardMarkup:
    """
    Postcode-or-location prompt. The location button uses Telegram's
    request_location feature so the user gets a native 'Share location' UI.
    """
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=messages.BTN_ENTER_POSTCODE)],
            [KeyboardButton(
                text=messages.BTN_SHARE_LOCATION,
                request_location=True,
            )],
            [KeyboardButton(text=messages.BTN_BACK_TO_MENU)],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def remove_keyboard() -> ReplyKeyboardRemove:
    """Hide the reply keyboard. Used when transitioning to inline-only flows."""
    return ReplyKeyboardRemove()


# ──────────────────────────────────────────────────────────────────────
# Inline keyboards — results page
# ──────────────────────────────────────────────────────────────────────

def results_keyboard(
    station_ids: list[str],
    page: int,
    total_pages: int,
    sort_mode: str = "cheap",
    page_offset: int = 0,
) -> InlineKeyboardMarkup:
    """
    Build the inline keyboard for a results page.

    Layout:
        [ 6 ] [ 7 ] [ 8 ] [ 9 ] [ 10 ]    (numbers match the table — global indexes)
        [ ◀ Prev ]      [ Next ▶ ]
        [ Cheapest ★ ] [ Nearest ]
        [ Back to menu ]

    Args:
        station_ids: IDs of stations on THIS page (max 5).
        page: 1-based page number.
        total_pages: total number of pages.
        sort_mode: 'cheap' or 'near' (controls which one gets the ★).
        page_offset: number of stations on prior pages — used to compute the
            global index for each button label so the digits match the table.
    """
    rows: list[list[InlineKeyboardButton]] = []

    # Row 1 — numbered buttons. Numbers = global station index in the result list.
    number_row = []
    for i, sid in enumerate(station_ids, start=page_offset + 1):
        prefix = sid[:16]  # callback_data limit
        number_row.append(InlineKeyboardButton(
            text=str(i), callback_data=f"det:{prefix}",
        ))
    if number_row:
        rows.append(number_row)

    # Row 2 — pagination (only show if there's more than one page)
    if total_pages > 1:
        pag_row = []
        if page > 1:
            pag_row.append(InlineKeyboardButton(
                text=messages.BTN_PREV, callback_data=f"pg:{page - 1}",
            ))
        if page < total_pages:
            pag_row.append(InlineKeyboardButton(
                text=messages.BTN_NEXT, callback_data=f"pg:{page + 1}",
            ))
        if pag_row:
            rows.append(pag_row)

    # Row 3 — sort mode toggles, with star on the active one
    cheapest_label = messages.BTN_CHEAPEST + (" ★" if sort_mode == "cheap" else "")
    nearest_label  = messages.BTN_NEAREST  + (" ★" if sort_mode == "near"  else "")
    rows.append([
        InlineKeyboardButton(text=cheapest_label, callback_data="srt:cheap"),
        InlineKeyboardButton(text=nearest_label,  callback_data="srt:near"),
    ])

    # Row 4 — back
    rows.append([InlineKeyboardButton(
        text=messages.BTN_BACK_TO_MENU, callback_data="menu",
    )])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def empty_results_keyboard() -> InlineKeyboardMarkup:
    """
    Quick radius pick when no stations were found. Lets the user re-search
    at a wider radius without retyping the postcode.
    """
    radius_row = [
        InlineKeyboardButton(text=f"{r} mi", callback_data=f"rad:{r}")
        for r in config.RADIUS_OPTIONS
    ]
    return InlineKeyboardMarkup(inline_keyboard=[
        radius_row,
        [InlineKeyboardButton(
            text=messages.BTN_BACK_TO_MENU, callback_data="menu",
        )],
    ])


# ──────────────────────────────────────────────────────────────────────
# Inline keyboards — detail card
# ──────────────────────────────────────────────────────────────────────

def detail_card(
    station_id: str,
    is_tracked: bool,
    map_url: str | None = None,
) -> InlineKeyboardMarkup:
    """
    Buttons under a station detail card.

       [ Navigate ] [ Track / Tracked ★ ]
       [ Back ]

    Navigate is a URL button opening Google Maps. Track flips between
    'Track' (not tracked) and 'Tracked ★' (already tracked).
    """
    prefix = station_id[:16]
    track_text = messages.BTN_TRACKED if is_tracked else messages.BTN_TRACK
    track_callback = f"rmv:{prefix}" if is_tracked else f"trk:{prefix}"

    top_row: list[InlineKeyboardButton] = []
    if map_url:
        top_row.append(InlineKeyboardButton(
            text=messages.BTN_NAVIGATE, url=map_url,
        ))
    top_row.append(InlineKeyboardButton(
        text=track_text, callback_data=track_callback,
    ))

    return InlineKeyboardMarkup(inline_keyboard=[
        top_row,
        [InlineKeyboardButton(text=messages.BTN_BACK, callback_data="back_results")],
    ])


# ──────────────────────────────────────────────────────────────────────
# Inline keyboards — favourites
# ──────────────────────────────────────────────────────────────────────

def favourite_card(station_id: str) -> InlineKeyboardMarkup:
    """
    Buttons under a single favourite card:  [ Remove ]
    """
    prefix = station_id[:16]
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=messages.BTN_REMOVE, callback_data=f"rmv:{prefix}",
        )],
    ])


def favourites_full_dialog(
    new_station_id: str,
    existing: list[tuple[str, str]],  # list of (station_id, display_name_with_price)
) -> InlineKeyboardMarkup:
    """
    "Replace one of your 3 favourites?" dialog.

    Each existing favourite gets a 'Replace <name> (<price>p)' button.
    Plus a Cancel button.
    """
    new_prefix = new_station_id[:16]
    rows = []
    for sid, label in existing:
        old_prefix = sid[:16]
        rows.append([InlineKeyboardButton(
            text=f"Replace {label}",
            callback_data=f"rpl:{old_prefix}:{new_prefix}",
        )])
    rows.append([InlineKeyboardButton(
        text=messages.BTN_CANCEL, callback_data="cnf:cancel",
    )])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ──────────────────────────────────────────────────────────────────────
# Inline keyboards — settings
# ──────────────────────────────────────────────────────────────────────

def settings_root() -> InlineKeyboardMarkup:
    """Top-level settings menu."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⛽ Fuel",   callback_data="set:fuel")],
        [InlineKeyboardButton(text="📍 Radius", callback_data="set:radius")],
        [InlineKeyboardButton(text="🔔 Alerts", callback_data="set:alerts")],
        [InlineKeyboardButton(text="🚫 Unsubscribe", callback_data="set:stop")],
        [InlineKeyboardButton(text=messages.BTN_BACK_TO_MENU, callback_data="menu")],
    ])


def settings_fuel(current: str) -> InlineKeyboardMarkup:
    """Fuel picker — checkmark on the user's current fuel."""
    rows = []
    for code, label in messages.FUEL_LABELS.items():
        marker = " ✓" if code == current else ""
        rows.append([InlineKeyboardButton(
            text=f"{label}{marker}", callback_data=f"fuel:{code}",
        )])
    rows.append([InlineKeyboardButton(text=messages.BTN_BACK, callback_data="set:root")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def settings_radius(current: int) -> InlineKeyboardMarkup:
    """Radius picker — radii in one row.

    Uses 'sra:' prefix (Settings RAdius) to disambiguate from the 'rad:'
    prefix used in the empty-results retry flow — we don't want a Settings
    pick to also re-trigger a search.
    """
    radius_row = []
    for r in config.RADIUS_OPTIONS:
        marker = " ✓" if r == current else ""
        radius_row.append(InlineKeyboardButton(
            text=f"{r} mi{marker}", callback_data=f"sra:{r}",
        ))
    return InlineKeyboardMarkup(inline_keyboard=[
        radius_row,
        [InlineKeyboardButton(text=messages.BTN_BACK, callback_data="set:root")],
    ])


def settings_alerts(current: str) -> InlineKeyboardMarkup:
    """Alerts mode picker."""
    rows = []
    for mode, label in messages.ALERTS_MODE_LABELS.items():
        marker = " ✓" if mode == current else ""
        rows.append([InlineKeyboardButton(
            text=f"{label}{marker}", callback_data=f"alm:{mode}",
        )])
    rows.append([InlineKeyboardButton(text=messages.BTN_BACK, callback_data="set:root")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ──────────────────────────────────────────────────────────────────────
# Inline keyboards — confirmations
# ──────────────────────────────────────────────────────────────────────

def unsubscribe_confirm() -> InlineKeyboardMarkup:
    """Two-button confirmation for /stop."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=messages.BTN_YES_DELETE, callback_data="cnf:del",
        )],
        [InlineKeyboardButton(
            text=messages.BTN_CANCEL, callback_data="cnf:cancel",
        )],
    ])
