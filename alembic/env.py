"""
Alembic migration environment.

This file is invoked by `alembic upgrade head`, `alembic downgrade -1`, etc.
Its job is to:
  1. Load DATABASE_URL from environment (.env locally, secrets in CI).
  2. Open a synchronous psycopg2 connection (Alembic core is sync, even though
     the rest of the app uses asyncpg).
  3. Configure Alembic to handle our multi-schema layout (raw / staging / mart / app).
  4. Run the requested migration.

We do NOT use SQLAlchemy ORM here. Migrations are plain SQL files in
sql/migrations/, executed by `op.execute()` from the revision script.
"""

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# Load .env so DATABASE_URL is available when running `alembic` locally.
# In GitHub Actions and Railway, env vars are injected by the platform;
# load_dotenv() will silently do nothing if there's no .env file.
from dotenv import load_dotenv
load_dotenv()

# Alembic Config object — gives access to values in alembic.ini
config = context.config

# Set up Python logging via the [loggers] / [handlers] sections of alembic.ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Inject DATABASE_URL into the SQLAlchemy config that Alembic uses internally.
# We require it to be set — fail loudly if it isn't.
database_url = os.environ.get("DATABASE_URL")
if not database_url:
    raise RuntimeError(
        "DATABASE_URL is not set. "
        "Create a .env file from .env.example or export the variable."
    )
config.set_main_option("sqlalchemy.url", database_url)


# We do not use SQLAlchemy models, so target_metadata stays None.
# This means `alembic revision --autogenerate` won't work — that's intentional.
# All schema changes go through hand-written .sql files.
target_metadata = None


def run_migrations_offline() -> None:
    """
    'Offline' mode generates SQL scripts without connecting to the database.

    We don't use this mode in practice (we always run online against Supabase),
    but Alembic requires both modes to be defined. Implementing it for
    completeness.
    """
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        # Crucial for our project: Alembic needs to be aware of multiple schemas
        # so its bookkeeping (alembic_version table) doesn't collide with them.
        include_schemas=True,
        version_table_schema="public",
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """
    'Online' mode connects directly to the database and executes migrations.
    This is what `alembic upgrade head` uses.
    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        # NullPool: no connection pooling. Migrations are short-lived; we want
        # connections opened and closed cleanly each run, especially important
        # when running through Supabase's PgBouncer.
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            # See note in run_migrations_offline().
            include_schemas=True,
            # Alembic stores its bookkeeping table in `public` schema.
            # Our app schemas (raw/staging/mart/app) stay clean.
            version_table_schema="public",
        )

        with context.begin_transaction():
            context.run_migrations()


# Entry point — Alembic invokes one of these depending on the command.
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
