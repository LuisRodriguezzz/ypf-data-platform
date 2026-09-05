"""Tests del lector de YAML de los jobs.

La referencia es PyYAML: el host sí lo tiene, así que se compara la salida contra
`yaml.safe_load` sobre los archivos reales del repo. Si alguien escribe YAML fuera del
subconjunto soportado, estos tests lo muestran antes de que falle el job en el contenedor.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pipelines.spark_jobs.yaml_lite import parse_scalar, parse_yaml, strip_comment

# tests/spark_jobs/test_yaml_lite.py -> raíz del repo
REPO = Path(__file__).resolve().parents[2]
CONFIG_FILES = [
    REPO / "pipelines/spark_jobs/bronze_tables.yaml",
    REPO / "pipelines/contracts/produccion_pozo.yaml",
    REPO / "pipelines/contracts/pozo_primera_produccion.yaml",
]


@pytest.mark.parametrize("path", CONFIG_FILES, ids=lambda path: path.name)
def test_coincide_con_pyyaml_en_los_archivos_del_repo(path: Path):
    text = path.read_text(encoding="utf-8")
    assert parse_yaml(text) == yaml.safe_load(text)


def test_mapa_simple():
    assert parse_yaml("table: lake.silver.x\nsource: lake.bronze.x\n") == {
        "table": "lake.silver.x",
        "source": "lake.bronze.x",
    }


def test_lista_en_linea():
    assert parse_yaml("primary_key: [idpozo, anio, mes]") == {
        "primary_key": ["idpozo", "anio", "mes"]
    }


def test_lista_en_linea_vacia():
    assert parse_yaml("include: []") == {"include": []}


def test_lista_de_mapas_anidada():
    texto = """
columns:
  - name: mes
    type: int
    min: 1
    max: 12
  - name: empresa
    type: string
    nullable: false
"""
    assert parse_yaml(texto) == {
        "columns": [
            {"name": "mes", "type": "int", "min": 1, "max": 12},
            {"name": "empresa", "type": "string", "nullable": False},
        ]
    }


def test_lista_al_mismo_nivel_que_la_clave():
    assert parse_yaml("years:\n- 2020\n- 2021\n") == {"years": [2020, 2021]}


def test_mapa_anidado():
    assert parse_yaml("datasets:\n  fractura:\n    - table: t\n") == {
        "datasets": {"fractura": [{"table": "t"}]}
    }


def test_ignora_comentarios_y_lineas_vacias():
    assert parse_yaml("# encabezado\n\nkey: 1  # al final\n") == {"key": 1}


def test_no_corta_en_un_numeral_entre_comillas():
    assert parse_yaml('match: "a#b"') == {"match": "a#b"}


def test_strip_comment_respeta_las_comillas():
    assert strip_comment('match: "a # b"  # real') == 'match: "a # b"  '


def test_documento_vacio():
    assert parse_yaml("# solo comentarios\n") == {}


def test_clave_sin_valor_es_nula():
    assert parse_yaml("dedupe_by:\n") == {"dedupe_by": None}


def test_valor_con_dos_puntos_adentro():
    assert parse_yaml("description: unidades: m3") == {"description": "unidades: m3"}


@pytest.mark.parametrize(
    ("token", "esperado"),
    [
        ("12", 12),
        ("-0.01", -0.01),
        ("true", True),
        ("False", False),
        ("null", None),
        ("~", None),
        ("texto suelto", "texto suelto"),
        ("'con comillas'", "con comillas"),
        ('"\\\\(DDJJ\\\\)"', "\\(DDJJ\\)"),
        ("2024-01-31", "2024-01-31"),
    ],
)
def test_parse_scalar(token: str, esperado):
    assert parse_scalar(token) == esperado


def test_una_linea_sin_dos_puntos_falla():
    with pytest.raises(ValueError):
        parse_yaml("esto no es yaml")
