# Fuel Mate

Telegram bot helping UK drivers find the cheapest fuel nearby, using official UK government data.

> **Status:** Work in progress. Full setup instructions will land here on completion.
> For now see `CLAUDE.md` for project rules and `FUEL_MATE_SPEC.md` for the full technical specification.

## Stack at a glance

- Python 3.12 · aiogram 3.13 · asyncpg
- PostgreSQL on Supabase (UK region)
- Bot: Railway · ETL: GitHub Actions cron · CSV archive: GitHub Releases
