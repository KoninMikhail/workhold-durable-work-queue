from logging.config import fileConfig
import os
import re

from alembic import context
from sqlalchemy import engine_from_config, pool, text

from queue_service.db import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

database_url = os.environ.get("DATABASE_URL")
if database_url:
    # ConfigParser interpolates '%', so escape it in the URL.
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))

target_metadata = Base.metadata

_SCHEMA_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def _version_table_schema() -> str | None:
    """Optional isolated schema for alembic_version + unqualified search_path."""
    schema = os.environ.get("ALEMBIC_VERSION_TABLE_SCHEMA", "").strip()
    if not schema:
        return None
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise RuntimeError(
            f"ALEMBIC_VERSION_TABLE_SCHEMA is not a safe identifier: {schema!r}"
        )
    return schema


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    schema = _version_table_schema()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        version_table_schema=schema,
    )

    with context.begin_transaction():
        context.run_migrations()


def _ensure_wide_alembic_version_table(connection, schema: str | None) -> None:
    """Create alembic_version with VARCHAR(128) before Alembic's default VARCHAR(32).

    Revision id ``0001_physical_contract_foundations`` is 33 characters and does
    not fit Alembic's stock version_num column.
    """
    # Qualified name when isolating to a test schema; else search_path/public.
    table = (
        f"{schema}.alembic_version" if schema is not None else "alembic_version"
    )
    connection.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                version_num VARCHAR(128) NOT NULL PRIMARY KEY
            )
            """
        )
    )
    connection.commit()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    schema = _version_table_schema()
    with connectable.connect() as connection:
        if schema is not None:
            # Session-level search_path so unqualified DDL lands in the test schema.
            connection.execute(text(f"SET search_path TO {schema}"))
            connection.commit()
        _ensure_wide_alembic_version_table(connection, schema)
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            version_table_schema=schema,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
