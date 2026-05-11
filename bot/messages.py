"""
All user-facing strings.

PER CLAUDE.md RULE #1: these strings are reproduced verbatim from spec
section 9. Do not paraphrase, "polish", or improve them. Tone of voice
decisions belong to the product owner.

Conventions:
  - HTML parse mode (use <b>, <i>, <code>, <s>)
  - British English (favourites, postcode, petrol, centre)
  - Active voice, action-oriented
  - Functional emoji only
"""

# ── Onboarding ───────────────────────────────────────────────────────

WELCOME_NEW = (
    "👋 Welcome to Fuel Mate\n\n"
    "I help UK drivers find the cheapest fuel nearby "
    "using official UK government data.\n\n"
    "<b>Step 1 of 2</b> — Which fuel do you use?"
)

ONBOARDING_STEP2 = (
    "✓ Step 1 done — fuel set to <b>{fuel}</b>\n\n"
    "<b>Step 2 of 2</b> — How to find fuel:\n\n"
    "📨 <b>Enter postcode</b> — search any UK area\n"
    "📍 <b>Share location</b> — finds stations near you\n\n"
    "Tap <b>Find Fuel</b> below to start."
)

WELCOME_BACK = (
    "Welcome back\n\n"
    "Fuel: <b>{fuel}</b> · Radius: <b>{radius} mi</b> · Alerts: <b>{alerts}</b>"
)


# ── Find fuel flow ───────────────────────────────────────────────────

FIND_FUEL_CHOICE = "📍 How would you like to search?"

POSTCODE_PROMPT = (
    "📨 Enter a UK postcode (e.g. <code>CO4 9ED</code>):"
)

POSTCODE_FOUND = "📍 Found <code>{postcode}</code>"

POSTCODE_INVALID = (
    "✗ That doesn't look like a valid UK postcode.\n"
    "Please try again (e.g. <code>SW1A 1AA</code>)."
)

POSTCODE_NOT_FOUND = (
    "✗ I couldn't find that postcode in my data.\n"
    "Please double-check and try again."
)

LOCATION_OUTSIDE_UK = (
    "Fuel Mate covers UK only.\n"
    "Try a UK postcode like <code>CO4 9ED</code>."
)


# ── Results ──────────────────────────────────────────────────────────

RESULTS_HEADER = "⛽ <b>{count} stations within {radius} mi</b> · {fuel}"

RESULTS_TABLE = "<pre>{table}</pre>"  # monospace table content

RESULTS_EMPTY = (
    "No stations found within <b>{radius} miles</b>.\n"
    "Try a larger radius — tap the radius button below."
)

RESULTS_OUTDATED_BANNER = (
    "⚠️ Data may be outdated. Last sync: <b>{days_ago} days ago</b>"
)


# ── Detail card status lines ─────────────────────────────────────────

STATUS_OPEN_NOW = "🟢 Open now"
STATUS_24H = "🔵 24/7"
STATUS_CLOSED = "⚪ Closed"
STATUS_TEMP_CLOSED = "🔴 Temporarily closed"

PRICE_STALE_WARNING = "⚠️ Price updated {days} days ago"

TRACK_ADDED = "Added to favourites · {count}/3"
TRACK_REMOVED = "Removed from favourites"


# ── Favourites ───────────────────────────────────────────────────────

FAVOURITES_HEADER = "⭐ Your favourites ({count}/3):"

FAVOURITES_EMPTY = (
    "You have no tracked stations yet.\n\n"
    "Use <b>Find Fuel</b> to search and tap <b>Track</b> on any station."
)

FAVOURITES_ADD_PROMPT = "Want to track another station?"

FAVOURITES_FULL_REPLACE = (
    "⭐ Your favourites are full <b>(3/3)</b>.\n"
    "Replace one with <b>{name}</b>?"
)

FAVOURITE_REPLACED = "Replaced <b>{old}</b> with <b>{new}</b>"

FAVOURITES_ORPHAN_NOTICE = (
    "ℹ️ One of your tracked stations is no longer available "
    "and has been removed from favourites."
)

PRICE_CHANGE_DOWN = "↓ -{delta}p"
PRICE_CHANGE_UP   = "↑ +{delta}p"


# ── Settings ─────────────────────────────────────────────────────────

SETTINGS_HEADER = (
    "⚙️ <b>Settings</b>\n\n"
    "Fuel:   <b>{fuel}</b>\n"
    "Radius: <b>{radius} mi</b>\n"
    "Alerts: <b>{alerts}</b>"
)

SETTINGS_FUEL_PROMPT = "Select your fuel type:"
SETTINGS_FUEL_UPDATED = "✓ Fuel updated to <b>{fuel}</b>"

SETTINGS_RADIUS_PROMPT = "Choose your default search radius:"
SETTINGS_RADIUS_UPDATED = "✓ Radius updated to <b>{radius} mi</b>"

SETTINGS_ALERTS_ALL = (
    "Alerts: <b>All changes</b>\n"
    "You'll get notified of any price change."
)
SETTINGS_ALERTS_BIG = (
    "Alerts: <b>Big changes only (≥2p)</b>\n"
    "Recommended — less noise."
)
SETTINGS_ALERTS_OFF = (
    "Alerts: <b>Off</b>\n"
    "You won't get any notifications."
)

UNSUBSCRIBE_CONFIRM = (
    "✗ Are you sure you want to unsubscribe?\n\n"
    "This will delete:\n"
    "• Your fuel preferences\n"
    "• Your tracked stations\n"
    "• Your alert history\n\n"
    "You can always come back with /start."
)

UNSUBSCRIBE_DONE = (
    "Your data has been deleted.\n"
    "Send /start any time to come back."
)


# ── Help and commands ────────────────────────────────────────────────

HELP_TEXT = (
    "<b>Fuel Mate help</b>\n\n"
    "<b>Find Fuel</b> — search by location or postcode\n"
    "<b>Favourites</b> — your tracked stations\n"
    "<b>Settings</b> — fuel type, radius, alerts\n\n"
    "<b>Commands</b>\n"
    "/start — restart the bot\n"
    "/favourites — view favourites\n"
    "/settings — open settings\n"
    "/help — this message\n"
    "/stop — unsubscribe"
)

UNKNOWN_COMMAND = "Try /help to see what I can do."


# ── Errors ───────────────────────────────────────────────────────────

SYSTEM_ERROR = (
    "⚠️ Sorry, I'm having trouble right now.\n"
    "Please try again in a moment."
)


# ── Alerts (push notifications, sent by ETL — kept here for centralisation) ──

ALERT_PRICE_DROP = (
    "↓ <b>{fuel} -{delta}p</b> — <b>{name}</b>\n"
    "📍 {postcode} · {brand}\n"
    "<s>{old}p</s> → <b>{new}p</b>"
)

ALERT_PRICE_RISE = (
    "↑ <b>{fuel} +{delta}p</b> — <b>{name}</b>\n"
    "📍 {postcode} · {brand}\n"
    "<s>{old}p</s> → <b>{new}p</b>"
)


# ── Button labels ────────────────────────────────────────────────────

BTN_FIND_FUEL = "Find Fuel"
BTN_FAVOURITES = "Favourites"
BTN_SETTINGS = "Settings"
BTN_BACK_TO_MENU = "Back to menu"
BTN_ENTER_POSTCODE = "Enter a postcode"
BTN_SHARE_LOCATION = "Share my location"

BTN_NAVIGATE = "Navigate"
BTN_TRACK = "Track"
BTN_TRACKED = "Tracked ★"
BTN_REMOVE = "Remove"
BTN_BACK = "Back"

BTN_CHEAPEST = "Cheapest"
BTN_NEAREST = "Nearest"
BTN_PREV = "◀ Prev"
BTN_NEXT = "Next ▶"

BTN_CANCEL = "Cancel"
BTN_TRY_AGAIN = "Try again"
BTN_YES_DELETE = "Yes, delete everything"
BTN_ADD_STATION = "Add station · {used}/3"


# ── Display labels (lookup tables) ───────────────────────────────────

# Fuel codes → user-facing names. Mirrors bot.config.FUEL_TYPES,
# duplicated here so the messages module is self-contained.
FUEL_LABELS = {
    "e10": "Unleaded",
    "e5":  "Super Unleaded",
    "b7s": "Diesel",
    "b7p": "Premium Diesel",
}

ALERTS_MODE_LABELS = {
    "all_changes": "All changes",
    "big_only":    "Big changes only (≥2p)",
    "off":         "Off",
}
