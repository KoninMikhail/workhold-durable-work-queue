from configparser import ConfigParser
from pathlib import Path

from queue_service.db import Base

ROOT = Path(__file__).resolve().parents[1]


def test_alembic_ini_uses_postgresql_psycopg() -> None:
    ini = ConfigParser()
    read = ini.read(ROOT / "alembic.ini", encoding="utf-8")
    assert read, "alembic.ini must exist"
    url = ini.get("alembic", "sqlalchemy.url")
    assert url.startswith("postgresql+psycopg://")


def test_base_metadata_is_available() -> None:
    assert Base.metadata is not None
