"""Tests de las reglas puras del job bronze (no requieren Spark ni JVM)."""

from __future__ import annotations

import pytest

from pipelines.spark_jobs.bronze_rules import (
    LandedFile,
    clean_column_name,
    latest_ok_query,
    namespace_of,
    pending_files,
    postgres_jdbc,
    s3a_uri,
)


def landed(resource_id: str, sha256: str) -> LandedFile:
    return LandedFile(
        resource_id=resource_id,
        resource_name=f"recurso {resource_id}",
        landing_key=f"energia/produccion_pozo/resource_id={resource_id}/x.csv",
        sha256=sha256,
        ingest_date="2026-09-05",
    )


def test_s3a_uri_arma_la_ruta_del_objeto():
    assert s3a_uri("landing", "energia/x.csv") == "s3a://landing/energia/x.csv"


def test_s3a_uri_no_duplica_la_barra_inicial():
    assert s3a_uri("landing", "/energia/x.csv") == "s3a://landing/energia/x.csv"


def test_clean_column_name_quita_el_bom():
    assert clean_column_name("\ufeffidempresa") == "idempresa"


def test_clean_column_name_quita_espacios():
    assert clean_column_name("  anio ") == "anio"


def test_clean_column_name_deja_intacto_un_nombre_normal():
    assert clean_column_name("prod_pet") == "prod_pet"


def test_namespace_of():
    assert namespace_of("lake.bronze.produccion_pozo") == "lake.bronze"


def test_pending_files_incluye_recursos_nuevos():
    files = [landed("a", "sha-a"), landed("b", "sha-b")]
    assert pending_files(files, {"a": "sha-a"}) == [files[1]]


def test_pending_files_incluye_recursos_con_hash_cambiado():
    files = [landed("a", "sha-nueva")]
    assert pending_files(files, {"a": "sha-vieja"}) == files


def test_pending_files_vacio_si_todo_esta_cargado():
    files = [landed("a", "sha-a"), landed("b", "sha-b")]
    assert pending_files(files, {"a": "sha-a", "b": "sha-b"}) == []


def test_latest_ok_query_filtra_dataset_y_status():
    query = latest_ok_query("produccion_pozo")
    assert "dataset = 'produccion_pozo'" in query
    assert "status = 'ok'" in query
    assert "DISTINCT ON (resource_id)" in query


def test_latest_ok_query_rechaza_un_dataset_raro():
    with pytest.raises(ValueError):
        latest_ok_query("produccion'; DROP TABLE ingestion_manifest; --")


def test_postgres_jdbc_traduce_el_dsn():
    url, properties = postgres_jdbc("postgresql://lakehouse:secreto@postgres:5432/lakehouse")
    assert url == "jdbc:postgresql://postgres:5432/lakehouse"
    assert properties["user"] == "lakehouse"
    assert properties["password"] == "secreto"
    assert properties["driver"] == "org.postgresql.Driver"


def test_postgres_jdbc_asume_el_puerto_por_defecto():
    url, _ = postgres_jdbc("postgresql://u:p@localhost/lakehouse")
    assert url == "jdbc:postgresql://localhost:5432/lakehouse"
