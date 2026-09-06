# ADR 0007 — CI en GitHub Actions

**Estado:** aceptada · 2026-09-05

## Contexto

El repo se sube a GitHub como público. Un repo público tiene minutos ilimitados en GitHub
Actions (los privados tienen cuota mensual), así que no hay motivo para buscar otro runner.

Levantar el stack completo (Podman, MinIO, Spark, Iceberg REST catalog) en el runner de CI es
posible pero caro de mantener: hoy ese stack corre sobre Podman en Windows (ADR 0004, ADR
0006), y reproducirlo en `ubuntu-latest` sería un segundo entorno a sostener aparte del que ya
se usa a diario.

## Decisión

Un solo workflow `ci` en `push` y `pull_request` sobre `main`, con dos jobs:

- `lint-y-tests`: `uv sync`, `ruff check`, `ruff format --check` y `pytest`. Corre contra
  Moto y SQLite (ver `tests/ingest/conftest.py`), no contra servicios reales.
- `dags-importan`: instala Airflow en un venv aparte (no vive en `uv.lock`: Airflow no se
  instala en Windows, donde se desarrolla el repo) y corre `scripts/check_dags.py`, que carga
  los DAGs con `DagBag` y falla si hay errores de import. No levanta el scheduler ni el
  webserver de Airflow.

Lo que el CI no valida: que los jobs de Spark corren de verdad contra Iceberg, que
`DockerOperator` puede hablar con el socket de Podman, ni la ingesta contra las fuentes
públicas reales. Esa integración se corre local, a mano, contra el compose de
`infra/docker/`.

## Consecuencias

- Un cambio que rompe un DAG por un typo o un import faltante se detecta en minutos, sin
  esperar a correrlo en Airflow local.
- El CI puede dar verde con un job de Spark roto en runtime (contrato mal armado, tabla
  Iceberg inexistente): esa clase de bug sigue dependiendo de la corrida local.
- Cuando el destino `aws` (ADR 0001) tenga Terraform en `infra/terraform/`, este workflow suma
  un job de `terraform validate` (y `terraform fmt -check`) sobre ese directorio. No corre
  `plan` ni `apply`: eso requeriría credenciales de AWS en el repo público.
- **Actualización (ADR 0014):** `plan` y `apply` viven en un workflow aparte,
  `.github/workflows/deploy.yml`, y no necesitan credenciales en el repo: se autentican con
  OIDC contra un rol por ambiente. `ci.yml` no cambió de alcance —sigue sin tocar AWS— y por
  eso los dos workflows están separados: el CI corre en cada push de cualquier rama y el
  despliegue solo desde `main`.
