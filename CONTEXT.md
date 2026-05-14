# Fuel Mate — Project Context

> **Purpose of this file:** complete snapshot of the project for a new
> development session (AI assistant or a returning human developer).
> Read this first before making changes.
>
> **Not committed to git** — this file is in `.gitignore`. Local-only.
> Update it after significant changes; treat it as a living document.

**Last updated:** May 2026
**Maintainer:** Kirill Zentsov ([@KirillZentsov](https://github.com/KirillZentsov))

---

## 1. TL;DR

Fuel Mate is a production Telegram bot that helps UK drivers find the
cheapest petrol or diesel nearby, using the official UK Fuel Finder API.

- **Live in production**, serving real users on Telegram as
  [@GetFuelMateBot](https://t.me/GetFuelMateBot).
- **Bot:** runs on Railway 24/7 (Python 3.12, aiogram, polling mode).
- **Database:** PostgreSQL on Supabase free tier (UK region, ~7,800 stations).
- **ETL:** fetches from gov.uk OAuth API. **Currently run manually**
  1-2× per day from the developer's machine. Automated cron is blocked
  by gov.uk's AWS WAF — see section 5 for the full story.
- **Tests:** 101 passing, 1 skipped.
- **Status:** MVP-complete and stable. Roadmap items are post-MVP polish.

If you only read one section, read section 3 (design decisions) and
section 5 (open problems).

---

## 2. What we built

### High-level flow

```
UK driver  ──/start, /find, /favourites──▶  Telegram Bot (Railway)
                                                  │
                                                  ▼  reads
                                            PostgreSQL (Supabase, UK)
                                                  ▲  writes
                                                  │
   gov.uk Fuel Finder API ──JSON──▶  ETL pipeline (local cron, manual)
                                                  │
                                                  ▼  notifies
                                            Admin channel (Telegram)
```

The bot and ETL are **fully decoupled**: the bot reads from `mart.*`
materialised tables, the ETL writes them. Either side can be restarted /
redeployed without affecting the other.

### Tech stack

| Layer            | Choice                                              |
|------------------|-----------------------------------------------------|
| Language         | Python 3.12                                         |
| Bot framework    | aiogram 3.13 (async, polling — not webhooks)       |
| DB driver        | asyncpg + raw SQL (deliberately no ORM)            |
| HTTP client      | httpx (async)                                       |
| Database         | PostgreSQL 15 on Supabase, eu-west-2 region        |
| Bot host         | Railway, Hobby plan ($5 free credit / month)       |
| ETL trigger      | Manual `python -m etl.pipeline` from dev machine   |
| External API     | gov.uk Fuel Finder API (OAuth 2.0 client credentials) |
| Postcode index   | Local gzipped CSV (1.8M postcodes, ~16 MB)         |

### Where things live

| Resource              | Location                                        |
|-----------------------|-------------------------------------------------|
| Source code           | `github.com/KirillZentsov/fuel-mate` (public)  |
| Bot runtime           | Railway project "fuel-mate" → service "fuel-mate" |
| Database              | Supabase project `********`        |
| Supabase region       | eu-west-2 (London) — required for GDPR / latency |
| Admin notifications   | Telegram channel "Fuel Mate Admin" (private)   |
| ETL credentials       | Local `.env` + GitHub Secrets (for future automation) |
| BotFather             | Bot `@GetFuelMateBot` registered there         |

---

## 3. Critical design decisions & rationale

This section captures **why** we made each call. Future changes should
respect the reasoning or explicitly invalidate it.

### 3.1 Four-schema database layout

We have `raw`, `staging`, `mart`, `app` schemas instead of one flat
namespace. This is the "medallion" pattern.

- **`raw.fuel_data_dumps`** — every ETL fetch logs one row here with
  its sha256. Used for dedup short-circuit and audit trail.
- **`staging.stations`, `staging.prices`** — parsed but un-validated
  rows from the most recent fetch. Could in theory hold history of all
  fetches but we truncate per dump for simplicity.
- **`mart.stations`, `mart.prices_current`, `mart.prices_history`** —
  what the bot reads from. UPSERT semantics: latest snapshot per
  station, current price per (station, fuel), historical price changes.
- **`app.users`, `app.favourites`** — user state.

Why it paid off: the CSV → API migration touched only `download.py`
and the parser. The bot, mart, and app layers didn't change because
they read from `mart.*`, which is source-agnostic.

### 3.2 asyncpg + raw SQL (no ORM)

Reasons:
- We control exact SQL — important for COPY operations (~25k rows/run),
  JSONB queries, and PostgreSQL-specific features like advisory locks.
- asyncpg is the fastest Python driver, important for a bot that handles
  search queries during user interaction.
- An ORM (SQLAlchemy etc.) would add 30-50ms per query for the same
  result and make COPY operations awkward.

Trade-off: no schema migrations from code, no model-validation layer.
We pay for this with raw SQL discipline in `bot/repositories/*.py` —
all queries live there, nowhere else.

### 3.3 Source-agnostic ETL with two parsers

`shared/csv_parser.py` exports two functions: `parse_api_response()` and
`parse_fuel_csv()`. Both produce identical `ParsedStation` dataclasses.
`etl/download.py` routes between them based on whether `--local-csv`
was passed.

Why this matters: when the gov.uk WAF killed our CSV path, we kept the
CSV parser around for `--local-csv` offline replay. It's also a
genuinely useful debug capability — we can save a gov.uk CSV, replay
it months later, and verify behaviour.

### 3.4 Canonical sha256 for dedup (not raw response hash)

The API returns batches in unpredictable order and stamps
`price_last_updated` with timestamps that tick even when data doesn't
change. If we hashed the raw response, every run would produce a
different digest and dedup would be useless.

The canonical hash digests the sorted list of
`(station_id, fuel_type, price, effective_timestamp)` tuples. Cosmetic
changes don't produce a new digest; real price changes always do.

### 3.5 UK postcodes in RAM (not external API)

There's no clean free UK postcode → lat/lon service. Postcodes.io has
rate limits, Google has cost. We load a gzipped CSV (~16 MB on disk,
~250 MB in RAM after parsing) into a dict at bot startup.

Trade-off: bot uses ~280 MB RAM total (vs ~30 MB without postcodes).
Comfortably within Railway's free tier. Lookups are O(1) with no
network call. Cold-start adds 2-3 seconds to bot startup.

### 3.6 Advisory locks for the 3-favourites limit

`favourites.add()` needs to enforce "max 3 per user". A naive
`SELECT COUNT(*) ... FOR UPDATE` doesn't work in PostgreSQL on
aggregated queries. Solution: `pg_advisory_xact_lock($user_id)`
serialises concurrent inserts **per user**. Different users don't block
each other; the same user can't race past the limit even from two
devices.

### 3.7 Polling instead of webhooks

For a bot of this scale (sub-100 users), polling is simpler:
- No public URL needed
- No TLS termination configuration
- No webhook health probes

Trade-off: small constant background traffic (long-poll requests to
Telegram every few seconds). Negligible.

### 3.8 Reply keyboard with 2 buttons + commands in menu

Layout decision per spec section 9:
- Reply keyboard (always visible): `[Find Fuel]` and `[Favourites]`
- Burger menu (`/setcommands`): `/start /favourites /settings /help /stop`

Why split: reply keyboard is for the two most common actions. Settings
and Help are infrequent — burger menu keeps the main UI uncluttered.

### 3.9 HTML parse_mode (not MarkdownV2)

aiogram 3 supports both. We chose HTML because:
- Easier to handle apostrophes in UK station names (`McDonald's`)
- `<code>`, `<b>`, `<i>` are forgiving — MarkdownV2 throws on
  unescaped `_`, `*`, `(`, `)`, `[`, `]`, `~`, ` and several others

### 3.10 What we deliberately did NOT build (yet)

These were considered and explicitly deferred:

- **Mini App** — telegram inline web view for visual map. Adds
  significant complexity (auth, hosting, state sync) for marginal UX
  gain over native Telegram messages.
- **Sparkline charts** — price history visualisation in detail cards.
  Spec section 9 says no; we agree — adds image rendering pipeline for
  decoration.
- **Real-time push** for price drops on tracked favourites. **Planned**
  but not built — see roadmap section 9.
- **Daily admin summary** — number of signups, retention, error rate.
  Same, planned, not built.
- **Multi-language support** — bot is UK-only, English-only. Deferred
  indefinitely.
- **Anything beyond UK** — explicit scope limit.

---

## 4. Project status

### Working end-to-end

| Component                | Status                                  |
|--------------------------|-----------------------------------------|
| Bot in Telegram          | ✅ Live 24/7 on Railway                |
| Database                 | ✅ Supabase, ~7,800 stations indexed   |
| ETL — fetch              | ✅ OAuth API working                   |
| ETL — parse              | ✅ Handles all observed API quirks     |
| ETL — load + refresh     | ✅ Atomic transactions, idempotent     |
| Admin notifications      | ✅ ✅ / ℹ️ / 🚨 messages to channel   |
| Postcode resolution      | ✅ 1.8M postcodes in RAM               |
| User registration        | ✅ /start onboarding (2-step)          |
| Search by postcode       | ✅ With radius selection               |
| Search by location share | ✅ With radius selection               |
| Detail card              | ✅ Open-now indicator, address, brand  |
| Favourites (max 3)       | ✅ Add/remove/list                     |
| Settings                 | ✅ Fuel type, radius, alerts toggle    |
| /stop deletion           | ✅ Cascading delete + re-onboarding    |
| /help                    | ✅ Static help text                    |
| Tests                    | ✅ 101 passing, 1 skipped              |

### Deferred (post-MVP, intentional)

| Feature                  | Notes                                   |
|--------------------------|-----------------------------------------|
| Push alerts              | Price drops on tracked favourites. Spec section 6.8. Estimated 150 LoC. |
| Daily admin summary      | Stats to admin channel. Spec section 6.10. Estimated 100 LoC. |
| Automated ETL            | Blocked by gov.uk WAF — see section 5. |
| Power BI dashboard       | View `mart.vw_prices_history` is ready; just needs BI connection. |

---

## 5. Known issues & open problems

### 5.1 ⚠️ The big one: gov.uk WAF blocks datacenter IPs

**Symptom:** any HTTP request to `*.fuel-finder.service.gov.uk` from
GitHub Actions / Railway / Anthropic sandbox / AWS / Azure / GCP
returns **HTTP 403 Forbidden** with header `x-deny-reason: ...`.
Same code from the developer's home IP returns 200 OK.

**Confirmation:** we ran a diagnostic workflow (`diag-govuk.yml`,
since deleted from main) that:
1. Probed with fake credentials → got 403 (not 401), proving WAF rejects
   before the auth layer.
2. Probed with real credentials → got 403, confirming the block isn't
   credential-related.
3. Observed gov.uk DNS resolves to `*.cloudfront.net` IPs — they use
   AWS WAF via CloudFront, not a custom rule.

**What we've tried:**
- ✘ Different User-Agent strings (Chrome 91, Chrome 120, Firefox)
- ✘ Full browser-simulation headers (Sec-Fetch-*, Sec-Ch-Ua, …)
- ✘ Different request library (httpx, requests, curl)
- ✘ Re-running workflows hoping for a "good" IP from the pool
- ✘ OAuth migration (assumed WAF was on the CSV endpoint specifically;
  it's on the whole domain)

**Workaround in use:** developer runs ETL manually 1-2× per day from
their home machine.

**Permanent fix options** (in order of recommendation):

1. **Self-hosted GitHub Actions runner** on the developer's Windows
   machine. Free, ~30 min setup, requires laptop to be online during
   cron times (10:15 and 16:15 UK).
2. **Small non-AWS VPS** (e.g. Hetzner CX11 at €3/mo) with
   self-hosted runner, or just a cron + git pull setup. Reliable, low
   cost, residential-like IP.
3. **Cloudflare Worker proxy** between GitHub Actions and gov.uk. Free
   tier covers 100k requests/day, we need ~150. About 1-2 hours to set
   up. Adds a maintenance surface.
4. **Email gov.uk support** asking for IP whitelist or a CI/CD-friendly
   endpoint. No-cost long shot, possibly weeks for a response.

Recommended sequence: try option 4 (low effort, send and forget) +
implement option 1 or 2 in parallel.

### 5.2 Minor: one or two `mart.prices_current` rows occasionally stale

Some retailers may not report a given fuel in a given dump. Our
`refresh_mart.py` upserts (doesn't delete) so a (station_id, fuel_type)
row that "drops out" stays at its last value, with `last_updated_at`
frozen at the previous dump time.

This is **intentional**: a stale price with a `⚠️ updated N days ago`
warning is more useful than no data at all. The bot's detail card
shows the warning automatically based on `forecourt_updated_at`.

Currently a handful out of ~25,600 rows. Not a bug, just data hygiene.

### 5.3 Static postcodes file

`data/postcodes.csv.gz` is committed to the repo. It was generated from
the Royal Mail PAF (public part) at one point in time. Real UK
postcodes change occasionally (new estates, retired codes).

Impact: low — bot says "couldn't find that postcode" for brand-new
addresses. Solution: regenerate the file yearly, replace, commit.

---

## 6. Operations playbook

### 6.1 Morning ETL run (manual, the current workaround)

```cmd
cd C:\Users\Kirill\Desktop\fuel-mate
venv\Scripts\activate
python -m etl.pipeline
```

`DATABASE_URL` is read from `.env`. The command takes ~10-60 seconds
depending on gov.uk's response time. Watch for one of:
- `✅ ETL completed` in admin channel → data refreshed.
- `ℹ️ ETL skipped — no new data` → sha256 matched, dedup worked.
- `🚨 ETL failed` → check the traceback in the terminal.

Run again in the late afternoon (~16:30 UK) to catch the second daily
gov.uk update.

### 6.2 Common errors and fixes

| Error                                          | Likely cause + fix |
|-----------------------------------------------|--------------------|
| `403 Forbidden` on OAuth token endpoint        | WAF block (see 5.1). Run from residential IP, or use a workaround. |
| `ConnectionRefusedError [WinError 1225]`       | `DATABASE_URL` points at stopped Docker. Check `.env` points at Supabase pooler. |
| `Tenant or user not found`                     | Username in pooler URL must be `postgres.<projectref>`, not just `postgres`. |
| `cannot import name 'X' from 'etl.foo'`        | Stale `__pycache__`. Delete `__pycache__/` dirs and retry. |
| Bot doesn't reply on Telegram                  | Railway deployment may be down. Check Deployments tab. |

### 6.3 Where to look for logs

- **Bot runtime logs:** Railway → project fuel-mate → service fuel-mate
  → Logs. Live, last ~hour visible.
- **ETL run logs:** wherever you ran it (terminal in VS Code).
- **Admin channel:** ✅ / ℹ️ / 🚨 events with stage info and timing.
- **DB-level audit:** `SELECT * FROM raw.fuel_data_dumps ORDER BY id
  DESC LIMIT 20;` — last 20 ETL runs.

### 6.4 Useful SQL queries

Pasted into DBeaver / psql, these are diagnostic one-liners:

```sql
-- Freshness distribution
SELECT 
  CASE
    WHEN forecourt_updated_at > NOW() - INTERVAL '24 hours' THEN '< 24 hours'
    WHEN forecourt_updated_at > NOW() - INTERVAL '48 hours' THEN '24-48 hours'
    WHEN forecourt_updated_at > NOW() - INTERVAL '7 days' THEN '2-7 days'
    ELSE '> 1 week'
  END AS freshness, COUNT(*) AS rows
FROM mart.prices_current GROUP BY freshness;

-- Cheapest E10 in country right now (sanity check that data is populated)
SELECT s.name, s.brand, s.postcode, pc.price_pence
FROM mart.prices_current pc
JOIN mart.stations s USING (station_id)
WHERE pc.fuel_type = 'e10' AND s.is_perm_closed IS NOT TRUE
ORDER BY pc.price_pence ASC LIMIT 10;

-- ETL run history
SELECT id, file_name, sha256, row_count, loaded_at
FROM raw.fuel_data_dumps ORDER BY id DESC LIMIT 20;

-- User counts (active in last week / month)
SELECT
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE last_active_at > NOW() - INTERVAL '7 days')  AS active_7d,
  COUNT(*) FILTER (WHERE last_active_at > NOW() - INTERVAL '30 days') AS active_30d
FROM app.users;

-- Detail of a specific station
SELECT * FROM mart.stations WHERE station_id = '...';
SELECT * FROM mart.prices_current WHERE station_id = '...';
```

### 6.5 Regular maintenance

**Weekly:**
- Check Railway usage stays under $4 of $5 credit.
- Check Supabase storage under ~400 MB (free tier limit 500 MB).
- Open the bot, send `/start`, confirm responsive.

**Monthly:**
- `pip list --outdated` — review minor upgrades. Always run `pytest`
  after.
- Optional: `pg_dump $DATABASE_URL -f backups/YYYY-MM-DD.sql` for a
  paranoid backup. (Supabase does its own daily backups on free tier.)

**Quarterly:**
- Refresh README "Project status" section if it's drifted.
- Consider rotating Supabase password + bot token if either has been
  shared anywhere unexpected.

---

## 7. Repository structure & key files

```
fuel-mate/
├── bot/                     # Telegram bot
│   ├── handlers/            # /start, search, favourites, settings, help
│   ├── repositories/        # users, favourites, stations, postcodes
│   ├── cards.py             # Detail card / list card formatters
│   ├── keyboards.py         # InlineKeyboardBuilder factories
│   ├── messages.py          # ALL user-facing strings (spec section 9)
│   ├── config.py            # Env var loading
│   ├── db.py                # asyncpg pool + JSONB codec registration
│   └── main.py              # Polling entrypoint
├── etl/
│   ├── api_client.py        # OAuth + paginated GETs to gov.uk
│   ├── download.py          # Routes API vs local-CSV
│   ├── load_staging.py      # asyncpg COPY into staging
│   ├── refresh_mart.py      # UPSERT mart from staging
│   ├── upload_release.py    # GitHub release archive (local-CSV mode only)
│   ├── pipeline.py          # Stage orchestrator + admin notifications
│   └── config.py            # Env var loading (ETL side)
├── shared/                  # Used by both bot and ETL
│   ├── csv_parser.py        # JSON-API and legacy-CSV parsers
│   ├── geo.py               # Haversine, bbox helpers
│   ├── postcode_validator.py
│   └── admin_notifier.py    # Telegram client for admin channel
├── sql/migrations/          # 0001-0006: extensions, schemas, mart, ...
├── tests/                   # 101 unit tests
├── data/postcodes.csv.gz    # 1.8M UK postcodes (16 MB, committed)
├── alembic/                 # Empty scaffolding — we use raw SQL migrations
├── docs/images/             # README screenshots
├── .github/workflows/
│   └── etl.yml              # Cron-driven ETL (currently CRON DISABLED — see 5.1)
├── Dockerfile               # Bot container
├── railway.json             # Railway deploy config
├── requirements.txt         # Bot runtime deps
├── requirements-etl.txt     # ETL deps (httpx, etc.)
├── requirements-dev.txt     # pytest et al.
├── pyproject.toml           # Black, ruff, pytest config
├── .env.example             # Template — do NOT commit real .env
├── README.md                # Portfolio-grade public README
├── CLAUDE.md                # Project rules (frozen schema, etc.)
├── FUEL_MATE_SPEC.md        # Full technical spec
└── CONTEXT.md               # THIS FILE — in .gitignore
```

### Files to edit carefully

- **`sql/migrations/*.sql`** — schema is frozen by spec. Don't change
  without a 0007+ migration file and a plan for production data.
- **`bot/messages.py`** — strings are quoted verbatim from spec
  section 9. Don't paraphrase ("Press" vs "Tap" matters).
- **`shared/csv_parser.py`** — well-tested. Each change runs against
  101 tests.
- **`bot/handlers/start.py`** — onboarding flow is delicate; the 2-step
  question is part of UX spec.

---

## 8. Environment & secrets

This list contains **names only**, not values. Real values live in:
- Locally: `~/Desktop/fuel-mate/.env` (gitignored)
- Railway: project fuel-mate → service fuel-mate → Variables tab
- GitHub Actions: repo → Settings → Secrets and variables → Actions

### Required for the bot (Railway)

| Variable             | Notes                                                 |
|---------------------|--------------------------------------------------------|
| `TELEGRAM_BOT_TOKEN` | From @BotFather                                       |
| `ADMIN_CHAT_ID`      | Telegram channel ID, negative number `-100…`         |
| `DATABASE_URL`       | Supabase **pooler** URL (IPv4); never the direct URL |
| `POSTCODES_PATH`     | Optional; default `data/postcodes.csv.gz` is correct |

### Required for the ETL (locally + GitHub Actions)

All of the above, plus:

| Variable                      | Notes                                            |
|------------------------------|--------------------------------------------------|
| `FUEL_FINDER_CLIENT_ID`       | From developer.fuel-finder.service.gov.uk       |
| `FUEL_FINDER_CLIENT_SECRET`   | Same — keep secret, rotate if leaked            |

### Database connection string format

```
postgresql://postgres.<project_ref>:<password>@aws-1-eu-west-2.pooler.supabase.com:5432/postgres
```

⚠️ `aws-1-` prefix is correct for our project. The number after `aws-`
varies by Supabase project — confirm from Supabase dashboard if creating
a new connection.

---

## 9. Roadmap — "what's next"

Prioritised. Pick from the top when you have time.

### Tier 1 — closes outstanding issues (do these soon)

1. **Pick an automation path for the ETL** (section 5.1).
   - Easiest: self-hosted GitHub Actions runner on Kirill's machine.
   - Most reliable: Hetzner VPS for €3/mo with cron + git pull.
   - Cleanest: Cloudflare Worker proxy + restore the cron in `etl.yml`.

2. **Email gov.uk support** asking about CI/CD-friendly API access.
   Low effort, no commitment. Draft template was discussed; main asks
   are (a) whitelist GitHub Actions IPs, (b) prod endpoint without WAF,
   or (c) confirmation of an alternative endpoint.

### Tier 2 — high-value post-MVP features

3. **Push alerts** for price drops on tracked favourites
   (spec section 6.8). Scope: `etl/detect_alerts.py` +
   `etl/send_alerts.py`. ~150 LoC. Runs after each successful ETL.
   Compare `mart.prices_current` to the just-loaded staging; if
   price dropped on a (station, fuel) that's in someone's favourites,
   send them a Telegram message.

4. **Daily admin summary** to admin channel (spec section 6.10).
   `etl/daily_summary.py`. ~100 LoC. Runs once a day. Reports:
   signups today, users active in 7d/30d, error rate last 24h.

### Tier 3 — polish

5. **Regenerate `data/postcodes.csv.gz`** annually from the latest
   Royal Mail PAF (public dataset).

6. **Power BI dashboard** connected to `mart.vw_prices_history`. View
   already exists; nothing to do code-side.

7. **CI for the bot** — GitHub Actions workflow that runs `pytest` on
   PRs. Currently we just rely on local test runs. ~20 LoC of YAML.

8. **Better lessons-learned section in README** with screenshots from
   admin channel showing real failures.

### Things NOT on the roadmap

To save time on rediscussion later:

- ❌ Mini App / web view
- ❌ Map rendering / sparkline charts
- ❌ Multi-language support
- ❌ Anything outside the UK
- ❌ Migrating off Supabase (free tier is fine until ~500 MB)
- ❌ Migrating off Railway (Hobby fits our usage)

---

## 10. How to use this document with an AI assistant

If you're starting a new development session with Claude / another LLM:

1. **Upload this file at the start of the session.** It's the fastest
   way to give the assistant complete context.

2. **Direct the assistant to specific sections** based on your task:
   - "Read sections 3, 5, 6 — I want to add a new feature"
   - "Read sections 5, 8 — I want to debug a production issue"
   - "Read sections 7, 9 — I want to refactor something"

3. **Don't trust the assistant to remember the project across
   sessions.** Even with this file loaded, ask it to summarise its
   understanding before making changes. Catches misunderstandings
   early.

4. **Update this file at the end of significant sessions.** If new
   design decisions were made, add them to section 3. If new problems
   emerged, add them to section 5. If you implemented something from
   the roadmap, move it to section 4.

5. **The repo also has `CLAUDE.md`** with project-wide rules
   (frozen schema, exact user-facing strings, etc.). That file IS
   committed and shared with collaborators. Treat it as immutable
   project policy. This `CONTEXT.md` is your private working memory.

### Recommended opening prompt for a new session

```
I'm continuing work on Fuel Mate, a Telegram bot for UK fuel prices.

[upload CONTEXT.md]

Please read this and confirm:
1. Your one-paragraph summary of what the project is and its current state.
2. The 2-3 most important constraints you should respect when proposing changes.
3. Any clarifying questions before we start [today's task].

Today's task: <describe>
```

---

## 11. Quick reference card

If you're skimming, these are the things most likely to bite you:

- **Don't change the DB schema** without a numbered migration and an
  understanding of the bot's queries that depend on it.
- **Don't paraphrase user-facing strings.** The spec quotes them
  verbatim for a reason.
- **The ETL runs manually, not on cron.** This is currently a feature,
  not a bug — gov.uk blocks our automated runners.
- **HTML parse mode, not MarkdownV2.** Escape `&`, `<`, `>` in user
  input before composing messages.
- **`.env` is local-only.** Real secrets live in Railway / GitHub
  Secrets in production. Never commit `.env`.
- **Tests must pass before push.** `pytest tests/` — 101 should pass,
  1 skipped. Any other count means something broke.

---

*End of CONTEXT.md*
