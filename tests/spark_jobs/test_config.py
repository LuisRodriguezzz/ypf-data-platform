"""Tests de la configuración de los jobs (precedencia entorno / .env)."""

from __future__ import annotations

from pipelines.spark_jobs.config import load_config, read_env_file

ENV_SAMPLE = """
# comentario
S3_ENDPOINT_URL=http://localhost:9000
S3_ACCESS_KEY_ID=lakehouse

POSTGRES_DSN=postgresql://lakehouse:secreto@localhost:5432/lakehouse
"""


def write_env(tmp_path, content: str):
    path = tmp_path / "local.env"
    path.write_text(content, encoding="utf-8")
    return path


def test_read_env_file_ignora_comentarios_y_lineas_vacias(tmp_path):
    values = read_env_file(write_env(tmp_path, ENV_SAMPLE))
    assert values["S3_ENDPOINT_URL"] == "http://localhost:9000"
    assert values["S3_ACCESS_KEY_ID"] == "lakehouse"
    assert len(values) == 3


def test_read_env_file_inexistente_devuelve_vacio(tmp_path):
    assert read_env_file(tmp_path / "no-existe.env") == {}


def test_load_config_usa_el_env_file_cuando_falta_la_variable(tmp_path, monkeypatch):
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    config = load_config(write_env(tmp_path, ENV_SAMPLE))
    assert config.s3_endpoint_url == "http://localhost:9000"


def test_load_config_prioriza_el_entorno_sobre_el_env_file(tmp_path, monkeypatch):
    # En el contenedor las variables apuntan a los hostnames internos y tienen que ganar.
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    config = load_config(write_env(tmp_path, ENV_SAMPLE))
    assert config.s3_endpoint_url == "http://minio:9000"


def test_load_config_cae_al_valor_por_defecto(tmp_path, monkeypatch):
    monkeypatch.delenv("S3_LANDING_BUCKET", raising=False)
    config = load_config(write_env(tmp_path, ENV_SAMPLE))
    assert config.s3_landing_bucket == "landing"
