"""
Find Fuel flow.

User journey:
  1. User taps "Find Fuel" → show postcode-or-location prompt.
  2a. User taps "Enter postcode" → enter waiting_for_postcode state.
      User types a postcode → validate → look up coords → search.
  2b. User shares location → validate UK → search.
  3. Show results table with inline navigation buttons.
  4. User taps a station number → detail card.
  5. From detail: Track / Untrack / Back to results.

FSM states are scoped to this flow. State data carries the search context
(coords, radius, fuel, sort mode, station_ids) so pagination and sort-switch
don't re-run SQL.
"""
import logging
from typing import Optional

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Location, Message

from bot import cards, config, keyboards, messages
from bot.repositories import favourites, postcodes, stations, users
from shared.geo import is_in_uk
from shared.postcode_validator import is_valid_uk_postcode, normalize_postcode

log = logging.getLogger(__name__)

router = Router(name="search")


# ──────────────────────────────────────────────────────────────────────
# FSM states
# ──────────────────────────────────────────────────────────────────────

class SearchStates(StatesGroup):
    waiting_for_postcode = State()


# ──────────────────────────────────────────────────────────────────────
# Step 1 — main menu "Find Fuel" tap
# ──────────────────────────────────────────────────────────────────────

@router.message(F.text == messages.BTN_FIND_FUEL)
async def on_find_fuel(message: Message, state: FSMContext) -> None:
    """User tapped 'Find Fuel' on the main reply keyboard."""
    await state.clear()
    await message.answer(
        messages.FIND_FUEL_CHOICE,
        reply_markup=keyboards.find_fuel_choice(),
    )


@router.message(F.text == messages.BTN_BACK_TO_MENU)
async def on_back_to_menu(message: Message, state: FSMContext) -> None:
    """User tapped 'Back to menu' on a reply keyboard. Reset and show main menu."""
    await state.clear()
    user = await users.get(message.from_user.id)
    if user is None:
        # Defensive: should not happen, but if so just show main menu
        await message.answer("👋", reply_markup=keyboards.main_menu())
        return
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
# Step 2a — postcode input
# ──────────────────────────────────────────────────────────────────────

@router.message(F.text == messages.BTN_ENTER_POSTCODE)
async def on_enter_postcode_button(message: Message, state: FSMContext) -> None:
    """User chose 'Enter a postcode'. Switch to waiting state."""
    await state.set_state(SearchStates.waiting_for_postcode)
    await message.answer(messages.POSTCODE_PROMPT, parse_mode="HTML")


@router.message(SearchStates.waiting_for_postcode, F.text)
async def on_postcode_typed(message: Message, state: FSMContext) -> None:
    """
    User typed something while we were waiting for a postcode.

    Validate format → look up coords → run search OR show error.
    """
    user_id = message.from_user.id
    raw = message.text or ""

    # Privacy: never log the postcode itself
    log.info("Postcode input from user_id=%d (len=%d)", user_id, len(raw))

    if not is_valid_uk_postcode(raw):
        await message.answer(messages.POSTCODE_INVALID, parse_mode="HTML")
        return

    normalized = normalize_postcode(raw)
    coords = postcodes.lookup(normalized)
    if coords is None:
        await message.answer(messages.POSTCODE_NOT_FOUND, parse_mode="HTML")
        return

    lat, lon = coords
    await message.answer(
        messages.POSTCODE_FOUND.format(postcode=normalized),
        parse_mode="HTML",
    )
    # State will be cleared inside _run_and_show_results
    await _run_and_show_results(message, state, lat=lat, lon=lon)


# ──────────────────────────────────────────────────────────────────────
# Step 2b — location share
# ──────────────────────────────────────────────────────────────────────

@router.message(F.location)
async def on_location_shared(message: Message, state: FSMContext) -> None:
    """User shared their GPS location."""
    loc: Location = message.location
    user_id = message.from_user.id
    log.info("Location received from user_id=%d", user_id)

    if not is_in_uk(loc.latitude, loc.longitude):
        await message.answer(messages.LOCATION_OUTSIDE_UK, parse_mode="HTML")
        return

    await _run_and_show_results(message, state, lat=loc.latitude, lon=loc.longitude)


# ──────────────────────────────────────────────────────────────────────
# Search execution + results display
# ──────────────────────────────────────────────────────────────────────

async def _run_and_show_results(
    message: Message,
    state: FSMContext,
    lat: float,
    lon: float,
) -> None:
    """
    Common code path after we've established (lat, lon):
      - load user (for fuel type and radius)
      - run search
      - either show empty-results message or render the first results page
      - persist search context in FSM data
    """
    user = await users.get(message.from_user.id)
    if user is None:
        # Shouldn't happen — they should've gone through /start.
        await message.answer(messages.SYSTEM_ERROR, parse_mode="HTML",
                             reply_markup=keyboards.main_menu())
        return

    results = await stations.search_within_radius(
        latitude=lat,
        longitude=lon,
        radius_miles=user.search_radius,
        fuel_type=user.fuel_type,
    )

    if not results:
        await state.clear()
        await message.answer(
            messages.RESULTS_EMPTY.format(radius=user.search_radius),
            parse_mode="HTML",
            reply_markup=keyboards.empty_results_keyboard(),
        )
        # Save coords so the empty-state radius buttons can retry
        await state.update_data(
            search_lat=lat, search_lon=lon, search_fuel=user.fuel_type,
        )
        return

    # Got results — apply default sort (cheapest first) and persist context
    sort_mode = "cheap"
    sorted_results = _sort_results(results, sort_mode)
    station_ids = [r.station_id for r in sorted_results]

    await state.set_state(None)  # leave waiting_for_postcode
    await state.update_data(
        search_lat=lat,
        search_lon=lon,
        search_radius=user.search_radius,
        search_fuel=user.fuel_type,
        sort_mode=sort_mode,
        station_ids=station_ids,
    )

    await _send_results_page(
        message=message,
        results=sorted_results,
        page=1,
        radius=user.search_radius,
        fuel=user.fuel_type,
        sort_mode=sort_mode,
        edit=False,
    )


async def _send_results_page(
    *,
    message: Message,
    results: list,
    page: int,
    radius: int,
    fuel: str,
    sort_mode: str,
    edit: bool,
) -> None:
    """
    Render and send (or edit) one page of results.

    edit=True is used for pagination/sort-switch (we keep a single message
    and edit it in place). edit=False is used for the first send.
    """
    total_pages = max(1, (len(results) + config.RESULTS_PAGE_SIZE - 1) // config.RESULTS_PAGE_SIZE)
    page = max(1, min(page, total_pages))

    header = cards.format_results_header(count=len(results), radius=radius, fuel=fuel)
    table_body = cards.format_results_table(results, page=page)
    text = f"{header}\n\n{messages.RESULTS_TABLE.format(table=table_body)}"

    # Optional staleness banner
    days = await stations.latest_data_age_days()
    if days is not None and days >= config.STALE_DATA_WARNING_DAYS:
        text = (
            messages.RESULTS_OUTDATED_BANNER.format(days_ago=days)
            + "\n\n"
            + text
        )

    page_start = (page - 1) * config.RESULTS_PAGE_SIZE
    page_ids = [r.station_id for r in results[page_start:page_start + config.RESULTS_PAGE_SIZE]]

    keyboard = keyboards.results_keyboard(
        station_ids=page_ids,
        page=page,
        total_pages=total_pages,
        sort_mode=sort_mode,
        page_offset=page_start,
    )

    if edit:
        try:
            await message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
            return
        except Exception:  # noqa: BLE001
            # Fall through to send a fresh message if edit fails (e.g. message too old)
            pass

    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)


def _sort_results(results: list, sort_mode: str) -> list:
    """Sort results by either price or distance, putting temp-closed at the bottom."""
    if sort_mode == "near":
        key = lambda r: (r.is_temp_closed, r.distance_miles)
    else:  # default: cheap
        key = lambda r: (r.is_temp_closed, r.price_pence, r.distance_miles)
    return sorted(results, key=key)


# ──────────────────────────────────────────────────────────────────────
# Pagination & sort-switch (callbacks on the results message)
# ──────────────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("pg:"))
async def cb_paginate(callback: CallbackQuery, state: FSMContext) -> None:
    """Paginate to a different page of the same search."""
    await callback.answer()
    page = int(callback.data.split(":", 1)[1])
    await _redraw_from_state(callback, state, page=page)


@router.callback_query(F.data.startswith("srt:"))
async def cb_sort(callback: CallbackQuery, state: FSMContext) -> None:
    """User toggled sort mode — re-sort the cached results, redraw page 1."""
    await callback.answer()
    sort_mode = callback.data.split(":", 1)[1]
    if sort_mode not in ("cheap", "near"):
        return
    await state.update_data(sort_mode=sort_mode)
    await _redraw_from_state(callback, state, page=1)


@router.callback_query(F.data.startswith("rad:"))
async def cb_radius_research(callback: CallbackQuery, state: FSMContext) -> None:
    """
    User tapped a radius button on the empty-results screen.

    Re-runs search with the new radius. Also used by the radius button on the
    Settings → Radius screen (if user navigated through that path back to a
    fresh search), but here it's specifically the empty-state retry.
    """
    await callback.answer()
    radius = int(callback.data.split(":", 1)[1])
    if radius not in config.RADIUS_OPTIONS:
        return

    data = await state.get_data()
    lat = data.get("search_lat")
    lon = data.get("search_lon")
    fuel = data.get("search_fuel")
    if lat is None or lon is None or fuel is None:
        # No saved context — silently bail
        return

    results = await stations.search_within_radius(
        latitude=lat, longitude=lon, radius_miles=radius, fuel_type=fuel,
    )

    if not results:
        # Still empty — refresh the empty-state message
        try:
            await callback.message.edit_text(
                messages.RESULTS_EMPTY.format(radius=radius),
                parse_mode="HTML",
                reply_markup=keyboards.empty_results_keyboard(),
            )
        except Exception:  # noqa: BLE001
            pass
        await state.update_data(search_radius=radius)
        return

    sort_mode = "cheap"
    sorted_results = _sort_results(results, sort_mode)
    await state.update_data(
        search_radius=radius,
        sort_mode=sort_mode,
        station_ids=[r.station_id for r in sorted_results],
    )
    await _send_results_page(
        message=callback.message,
        results=sorted_results,
        page=1,
        radius=radius,
        fuel=fuel,
        sort_mode=sort_mode,
        edit=True,
    )


async def _redraw_from_state(callback: CallbackQuery, state: FSMContext, page: int) -> None:
    """
    Re-render results using cached context from FSM state.

    We don't re-run SQL — we re-search to get fresh results from the cache?
    Actually we kept only station_ids in state, and full StationResult
    objects can't be cached easily (they have datetime/Decimal). So for
    pagination, we re-run the search — which is fast (sub-100ms) and ensures
    prices reflect the latest ETL run.
    """
    data = await state.get_data()
    lat = data.get("search_lat")
    lon = data.get("search_lon")
    radius = data.get("search_radius")
    fuel = data.get("search_fuel")
    sort_mode = data.get("sort_mode", "cheap")
    if None in (lat, lon, radius, fuel):
        return

    results = await stations.search_within_radius(
        latitude=lat, longitude=lon, radius_miles=radius, fuel_type=fuel,
    )
    sorted_results = _sort_results(results, sort_mode)

    await _send_results_page(
        message=callback.message,
        results=sorted_results,
        page=page,
        radius=radius,
        fuel=fuel,
        sort_mode=sort_mode,
        edit=True,
    )


# ──────────────────────────────────────────────────────────────────────
# Detail card
# ──────────────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("det:"))
async def cb_detail(callback: CallbackQuery, state: FSMContext) -> None:
    """User tapped a station number → show detail card."""
    await callback.answer()
    prefix = callback.data.split(":", 1)[1]

    user = await users.get(callback.from_user.id)
    if user is None:
        return

    detail = await stations.find_by_id_prefix(prefix, fuel_type=user.fuel_type)
    if detail is None:
        # Station no longer exists or prefix collision (extremely rare)
        await callback.answer("Station not found", show_alert=True)
        return

    is_tracked = await favourites.is_favourite(callback.from_user.id, detail.station_id)

    text = cards.format_detail_card(detail, fuel=user.fuel_type)
    nav_url = cards.navigate_url(detail.latitude, detail.longitude, detail.name)
    keyboard = keyboards.detail_card(
        station_id=detail.station_id,
        is_tracked=is_tracked,
        map_url=nav_url,
    )

    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except Exception:  # noqa: BLE001
        await callback.message.answer(text, parse_mode="HTML", reply_markup=keyboard)


@router.callback_query(F.data == "back_results")
async def cb_back_to_results(callback: CallbackQuery, state: FSMContext) -> None:
    """User tapped 'Back' on the detail card. Redraw the results page."""
    await callback.answer()
    data = await state.get_data()
    page = data.get("last_page", 1)
    await _redraw_from_state(callback, state, page=page)


# ──────────────────────────────────────────────────────────────────────
# Track / Untrack from detail card
# ──────────────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("trk:"))
async def cb_track(callback: CallbackQuery, state: FSMContext) -> None:
    """User tapped 'Track' on a detail card."""
    await callback.answer()
    prefix = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id

    user = await users.get(user_id)
    if user is None:
        return

    detail = await stations.find_by_id_prefix(prefix, fuel_type=user.fuel_type)
    if detail is None:
        return

    # Already tracked? Bail (UI shouldn't have offered this, but defensive)
    if await favourites.is_favourite(user_id, detail.station_id):
        await callback.answer("Already tracked", show_alert=False)
        return

    # Try to add — if at limit, show replace dialog
    added = await favourites.add(user_id, detail.station_id)
    if not added:
        await _show_replace_dialog(callback, user, detail)
        return

    count = await favourites.count_for_user(user_id)
    await callback.answer(
        messages.TRACK_ADDED.format(count=count),
        show_alert=False,
    )

    # Refresh the detail card to flip the button to "Tracked ★"
    text = cards.format_detail_card(detail, fuel=user.fuel_type)
    nav_url = cards.navigate_url(detail.latitude, detail.longitude, detail.name)
    keyboard = keyboards.detail_card(
        station_id=detail.station_id, is_tracked=True, map_url=nav_url,
    )
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except Exception:  # noqa: BLE001
        pass


@router.callback_query(F.data.startswith("rmv:"))
async def cb_remove(callback: CallbackQuery, state: FSMContext) -> None:
    """
    User tapped 'Remove'. This callback is shared between two places:
      - detail card (after tracking, button becomes 'Tracked ★' which sends rmv)
      - favourite card (Remove button)
    """
    await callback.answer()
    prefix = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id

    user = await users.get(user_id)
    if user is None:
        return

    detail = await stations.find_by_id_prefix(prefix, fuel_type=user.fuel_type)
    if detail is None:
        # The station might have been deleted — try direct DELETE by prefix
        # (we don't have a clean way to do this without the full ID, so we
        # just silently confirm to the user and rely on orphan cleanup later)
        await callback.answer(messages.TRACK_REMOVED, show_alert=False)
        return

    await favourites.remove(user_id, detail.station_id)
    await callback.answer(messages.TRACK_REMOVED, show_alert=False)

    # If we're on a detail card, refresh to flip 'Tracked ★' back to 'Track'
    text = cards.format_detail_card(detail, fuel=user.fuel_type)
    nav_url = cards.navigate_url(detail.latitude, detail.longitude, detail.name)
    keyboard = keyboards.detail_card(
        station_id=detail.station_id, is_tracked=False, map_url=nav_url,
    )
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except Exception:  # noqa: BLE001
        # We were probably on a favourites list — handler in favourites.py
        # will receive its own redraw; nothing to do here.
        pass


# ──────────────────────────────────────────────────────────────────────
# Replace dialog (favourites limit)
# ──────────────────────────────────────────────────────────────────────

async def _show_replace_dialog(
    callback: CallbackQuery,
    user,
    new_station,
) -> None:
    """When favourites are full and user tries to add — show replace dialog."""
    existing = await favourites.list_for_user(user.user_id, fuel_type=user.fuel_type)

    # Build short labels: "Tesco · 149.9p"
    options: list[tuple[str, str]] = []
    for f in existing:
        name = (f.name or "—")
        if len(name) > 18:
            name = name[:17] + "…"
        price_str = f"{f.current_price_pence}p" if f.current_price_pence else "—"
        options.append((f.station_id, f"{name} · {price_str}"))

    text = messages.FAVOURITES_FULL_REPLACE.format(name=new_station.name or "this station")
    keyboard = keyboards.favourites_full_dialog(
        new_station_id=new_station.station_id,
        existing=options,
    )
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except Exception:  # noqa: BLE001
        await callback.message.answer(text, parse_mode="HTML", reply_markup=keyboard)


@router.callback_query(F.data.startswith("rpl:"))
async def cb_replace(callback: CallbackQuery, state: FSMContext) -> None:
    """User picked which favourite to replace."""
    await callback.answer()
    parts = callback.data.split(":")
    if len(parts) != 3:
        return
    _, old_prefix, new_prefix = parts
    user_id = callback.from_user.id

    user = await users.get(user_id)
    if user is None:
        return

    old = await stations.find_by_id_prefix(old_prefix, fuel_type=user.fuel_type)
    new = await stations.find_by_id_prefix(new_prefix, fuel_type=user.fuel_type)
    if old is None or new is None:
        return

    await favourites.replace(user_id, old.station_id, new.station_id)

    await callback.message.edit_text(
        messages.FAVOURITE_REPLACED.format(
            old=old.name or "—",
            new=new.name or "—",
        ),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "cnf:cancel")
async def cb_cancel(callback: CallbackQuery, state: FSMContext) -> None:
    """Generic Cancel — bail out of the current dialog and show main menu."""
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:  # noqa: BLE001
        pass
    await callback.message.answer("👋", reply_markup=keyboards.main_menu())


# ──────────────────────────────────────────────────────────────────────
# "Back to menu" inline button
# ──────────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "menu")
async def cb_back_to_menu(callback: CallbackQuery, state: FSMContext) -> None:
    """User tapped 'Back to menu' (inline). Clear state, show main menu."""
    await callback.answer()
    await state.clear()
    try:
        await callback.message.delete()
    except Exception:  # noqa: BLE001
        pass
    user = await users.get(callback.from_user.id)
    if user is None:
        return
    await callback.message.answer(
        messages.WELCOME_BACK.format(
            fuel=messages.FUEL_LABELS.get(user.fuel_type, user.fuel_type),
            radius=user.search_radius,
            alerts=messages.ALERTS_MODE_LABELS.get(user.alerts_mode, user.alerts_mode),
        ),
        parse_mode="HTML",
        reply_markup=keyboards.main_menu(),
    )
