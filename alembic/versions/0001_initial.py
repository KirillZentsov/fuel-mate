"""Initial schema: all four schemas + raw/staging/mart/app tables + BI views.

This single revision applies the six numbered SQL files in sql/migrations/
in order. Each file is plain SQL — they are the source of truth for the
database schema, and Alembic is used here purely as a runner.

If you ever need to add a 7th migration:
  1. Add sql/migrations/0007_*.sql with your CREATE/ALTER statements.
  2. Generate a new Alembic revision:
       alembic revision -m "your description"
  3. In the new revision file, follow the same pattern as below:
       _execute_sql_file('0007_yourname.sql')
       And in downgrade(), the matching DROP / reverse statements.

Revision ID: 0001_initial
Revises:
Create Date: 2026-05-07
"""
from pathlib import Path

from alembic import op


# Alembic identifiers ─ required at module level
revision = "0001_initial"
down_revision = None      # this is the first revision
branch_labels = None
depends_on = None


# Migrations live next to the project root (not inside alembic/).
# Path resolution: this file is at fuel-mate/alembic/versions/0001_initial.py
# so 3 levels up lands us at fuel-mate/, then we descend into sql/migrations/.
MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "sql" / "migrations"


def _execute_sql_file(filename: str) -> None:
    """Read a .sql file from sql/migrations/ and execute its contents."""
    sql_path = MIGRATIONS_DIR / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"Migration file not found: {sql_path}")
    sql = sql_path.read_text(encoding="utf-8")
    op.execute(sql)


def upgrade() -> None:
    """
    Apply all DDL needed for a fresh database.

    Order matters: schemas must exist before tables, raw before staging
    (FK dependency), mart before app (FK dependency from app.favourites
    to mart.stations), and views last.
    """
    _execute_sql_file("0001_init_schemas.sql")
    _execute_sql_file("0002_raw_tables.sql")
    _execute_sql_file("0003_staging_tables.sql")
    _execute_sql_file("0004_mart_tables.sql")
    _execute_sql_file("0005_app_tables.sql")
    _execute_sql_file("0006_views.sql")


def downgrade() -> None:
    """
    Reverse the upgrade.

    We DROP each schema with CASCADE, which removes all contained tables,
    indexes, views, and constraints in one statement. Order is the reverse
    of upgrade(): app first (depends on mart), then mart, then staging
    (depends on raw), then raw. Public schema is left untouched — it holds
    Alembic's own bookkeeping table.

    WARNING: this is destructive and DROPs all data in those schemas.
    Only use in development or with explicit recovery plan.
    """
    op.execute("DROP SCHEMA IF EXISTS app     CASCADE;")
    op.execute("DROP SCHEMA IF EXISTS mart    CASCADE;")
    op.execute("DROP SCHEMA IF EXISTS staging CASCADE;")
    op.execute("DROP SCHEMA IF EXISTS raw     CASCADE;")
