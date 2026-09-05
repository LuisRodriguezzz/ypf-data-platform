"""Configuracion de la ingesta leida de `config/local.env` (nunca hardcodeada)."""

from __future__ import annotations

import os
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# pipelines/ingest/settings.py -> raiz del repo
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_FILE = REPO_ROOT / "config" / "local.env"
ENV_FILE_VAR = "LAKEHOUSE_ENV_FILE"


def env_file_path() -> Path:
    """Ruta del .env: `LAKEHOUSE_ENV_FILE` si esta definida, si no `config/local.env`."""
    override = os.environ.get(ENV_FILE_VAR)
    return Path(override).expanduser() if override else DEFAULT_ENV_FILE


class Settings(BaseSettings):
    """Variables de entorno usadas por la ingesta."""

    model_config = SettingsConfigDict(
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_region: str = "us-east-1"
    s3_landing_bucket: str = "landing"
    postgres_dsn: str
    ckan_base_url: str = "http://datos.energia.gob.ar"


def load_settings(env_file: Path | str | None = None) -> Settings:
    """Construye Settings desde un .env explicito o el resuelto por `env_file_path`."""
    path = Path(env_file) if env_file is not None else env_file_path()
    return Settings(_env_file=path if path.exists() else None)  # type: ignore[call-arg]
