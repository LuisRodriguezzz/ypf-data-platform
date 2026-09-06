# Recorrido del código: ypf-data-platform

Este documento es un recorrido guiado por el código real del repositorio, pensado para
alguien que sabe Python y SQL pero no vio Spark, Iceberg, Airflow ni AWS en profundidad.
No es un resumen de los READMEs: cada afirmación sale de un archivo concreto, y cuando se
cita código son las líneas reales del repo en la fecha de este recorrido (2026-09-06).

Nota de alcance: la primera edición de este documento (sesiones 1 a 8) se escribió cuando
`pipelines/reservas/` era un módulo con un solo docstring, sin lógica, y dbt, streaming y ML
todavía no existían en el repo. Esa brecha ya se cerró: las sesiones 9 a 12, agregadas en esta
edición, cubren el parser de XLSX de reservas, gold con dbt, streaming con Kafka y el modelo de
ML de completación de producción. `pipelines/contracts/fractura.yaml` sigue mencionado solo en
su mecánica genérica (el contrato como tal), no en sus reglas de negocio específicas.

---

## 1. Cómo usar este recorrido

Son 12 sesiones de 45 a 60 minutos. Cada una asume que hiciste la anterior. La idea no es leer
este documento de corrido, sino abrir el editor, leer el archivo real al lado del resumen y
correr el comando sugerido. Si no tenés el stack local levantado, igual leé el código: la
sesión 5 explica cómo levantarlo.

| Sesión | Tema | Archivos centrales |
|---|---|---|
| 1 | El mapa | estructura de carpetas, `config/local.env` |
| 2 | La ingesta | `pipelines/ingest/*.py`, `datasets.yaml` |
| 3 | Bronze | `pipelines/spark_jobs/{config,session,bronze_rules,bronze_load}.py` |
| 4 | Silver y contratos | `pipelines/spark_jobs/{silver_rules,silver_load}.py`, `pipelines/contracts/*.yaml` |
| 5 | Orquestación local | `infra/docker/compose.yaml`, `orchestration/dags/*.py` |
| 6 | AWS | `infra/terraform/*.tf`, `pipelines/aws/*.py` |
| 7 | Tests y CI | `tests/**`, `.github/workflows/ci.yml` |
| 8 | Las decisiones | `docs/adr/0001` a `0012` |
| 9 | Reservas y el parser de XLSX | `pipelines/reservas/*.py` |
| 10 | Gold con dbt | `pipelines/dbt/**` |
| 11 | Streaming | `pipelines/streaming/*.py`, perfil `streaming` del compose |
| 12 | ML y monitoreo | `pipelines/ml/*.py`, `pipelines/dbt/models/monitoreo/*`, `orchestration/dags/{ml_mensual,monitoreo_diario}.py` |

Cada sesión trae tres cosas: **qué leer** (los archivos, en orden), **qué correr** (comandos
reales del repo) y **qué tenés que poder explicar al terminar** (preguntas para responder sin
volver a mirar el código).

Prerequisito para las sesiones 3 a 6: Postgres y MinIO corriendo (perfil `core` del compose) y,
para 3-5, además el perfil `spark` o `airflow`. La sesión 2 corre sola con `uv sync` si tenés
Postgres y credenciales de S3/MinIO; si no, se puede leer sin ejecutar nada.

Prerequisitos de las sesiones nuevas: la 9 solo necesita el perfil `core` (pyiceberg no levanta
Spark). La 10 necesita el perfil `spark` (dbt corre dentro del runner). La 11 necesita además el
perfil `streaming` (Kafka). La 12 necesita el perfil `spark` y, para ver el tracking real, el
perfil `mlflow`.

---

## 2. Sesión 1 — El mapa

### Qué leer

Nada de código todavía: primero el árbol de carpetas. Esta es la estructura relevante del
repo (se omiten cachés, `.venv` y artefactos de build):

```
pipelines/
  ingest/           CLI + lógica de ingesta a landing (sesión 2)
    cli.py            comandos `ingest list|run|manifest|datasets`
    registry.py       lee datasets.yaml, valida y filtra fuentes
    ckan.py           cliente HTTP de la API de CKAN del portal de Energía
    runner.py         orquesta la corrida: descubre, decide, descarga
    manifest.py       tabla `ingestion_manifest` en Postgres (idempotencia)
    storage.py        subida a S3/MinIO en streaming, multipart + sha256
    settings.py       configuración (pydantic-settings) de config/local.env
    datasets.yaml     registro declarativo de fuentes
  spark_jobs/       transformaciones landing -> bronze -> silver (sesiones 3-4)
    config.py         configuración sin pydantic (runner solo trae stdlib+PySpark)
    session.py        SparkSession y catálogo Iceberg según el destino
    bronze_rules.py   reglas puras de bronze (testeables sin JVM)
    bronze_load.py    job de Spark que carga bronze
    bronze_tables.yaml  mapeo recurso -> tabla bronze
    silver_rules.py   el contrato YAML convertido en SQL
    silver_load.py    job de Spark que aplica el contrato y escribe silver
    requirements-runner.txt  dependencias que se instalan en el runner
  contracts/        un YAML por tabla silver (sesión 4)
    produccion_pozo.yaml, pozo_primera_produccion.yaml, fractura.yaml, reservas.yaml
  aws/              wrappers finos para Glue (sesiones 6, 9 y 10): ingest_job.py, bronze_job.py,
                    silver_job.py, bronze_reservas_job.py, gold_dbt_job.py, ssm.py
  reservas/         parser de XLSX + bronze con pyiceberg, sin Spark (sesión 9)
    parser.py         encabezado jerárquico, rangos fusionados, forma ancha -> forma larga
    bronze_load.py    escribe a Iceberg con pyiceberg (sin spark-submit)
  dbt/              gold: modelo dimensional sobre silver (sesión 10)
    run_dbt.py, profiles.yml, dbt_project.yml, macros/, models/{dimensiones,hechos,marts,
    monitoreo}/, tests/, sources.yml
  streaming/        telemetría de pozos por Kafka (sesión 11)
    replay_3w.py      productor: intercalado, tardíos, idempotencia
    consume_telemetria.py  consumidor: readStream, watermark, ventanas, dos queries
    pozo_map.py, eventos.py, fetch_3w.py
  ml/               modelo de completación de producción (sesión 12)
    datos.py, entrenar.py, predecir.py, registro.py
orchestration/dags/
  runner.py         arma el DockerOperator que lanza el runner de Spark
  produccion_pozo_mensual.py, fractura_diaria.py, reservas_mensual.py
  ml_mensual.py, monitoreo_diario.py, alertas.py
infra/
  docker/  compose.yaml (perfiles core, spark, airflow, streaming, mlflow), spark-defaults.conf,
           postgres/init.sql, .env.example
  terraform/  s3.tf, iam.tf, glue.tf, stepfunctions.tf, athena.tf, variables.tf,
              outputs.tf, versions.tf
scripts/  spark-submit.ps1/.sh, dbt.ps1, streaming-up.ps1, streaming-demo.ps1, aws_deploy.ps1/.sh,
          aws_logs.ps1, check_dags.py, check_lake.py
tests/    ingest/, spark_jobs/, reservas/, streaming/, ml/
docs/     adr/0001 a 0012, semana-0-derisking.md, fuentes/, ml/, aprendizaje/
config/   local.env — única fuente de configuración para correr todo en local
```

### El flujo landing → bronze → silver

Todo el proyecto es una cadena de tres capas (un patrón que se conoce como **arquitectura
medallion**):

```
fuentes públicas (CKAN, ZIP por HTTP)          telemetría de pozos (3W, vía Kafka)
        │  pipelines/ingest  (streaming, sha256, manifiesto)     │  replay_3w.py -> topic
        ▼                                                        ▼  consume_telemetria.py
   landing (S3/MinIO)          CSV/ZIP/XLSX crudos          readStream + watermark + ventana
        │  pipelines/spark_jobs/bronze_load.py                   │  (dos queries)
        │  pipelines/reservas/bronze_load.py (pyiceberg, sin Spark)
        ▼                                                        ▼
   bronze (Iceberg)            mismos datos, todo string    bronze.telemetria_pozo (crudo)
        │  pipelines/spark_jobs/silver_load.py + contrato YAML
        ▼                                                        ▼
   silver (Iceberg)            tipado, deduplicado, cuarentena    silver.telemetria_pozo_1min
        │  pipelines/dbt (sesión 10): ref/source, un modelo por vez
        ▼
   gold (Iceberg, dbt)         dim_pozo (SCD2), fact_*, marts
        │  pipelines/ml (sesión 12): entrenar.py (MLflow) -> predecir.py
        ▼
   gold.prediccion_produccion_12m   inferencia batch, con pyiceberg
```

Landing no es una tabla: son objetos en un bucket S3, organizados por key
(`{prefijo}/resource_id=.../ingest_date=.../archivo`). Bronze, silver y gold sí son tablas
Iceberg — un formato de tabla, no una base de datos, que agrega sobre Parquet un catálogo de
esquemas, particiones y snapshots. El porqué de cada paso se ve en las sesiones 2 a 4 (landing/
bronze/silver "clásicos"), 9 (el bronze de reservas, con pyiceberg y sin Spark), 10 (gold, con
dbt) y 11 (la rama de streaming, que tiene su propio bronze y silver por fuera de los contratos
YAML). Sobre gold se para, además, el modelo de ML de la sesión 12.

### Los tres puntos de entrada

Hay exactamente tres maneras de ejecutar algo en este repo, y conviene tenerlas separadas
desde el principio:

1. **La CLI de ingesta**: `uv run ingest run --dataset produccion_pozo`. Corre en el host o
   dentro del runner de Spark cuando la lanza Airflow.
2. **`spark-submit`**: `scripts/spark-submit.ps1 pipelines/spark_jobs/bronze_load.py --dataset produccion_pozo`.
   Levanta un contenedor efímero de Spark, corre el script y muere.
3. **Los DAGs de Airflow**: cada tarea es, por dentro, uno de los dos comandos anteriores,
   lanzado con un `DockerOperator` (sesión 5).

En AWS estos tres puntos de entrada tienen equivalentes finos en `pipelines/aws/*.py`
(sesión 6): la misma lógica, con un wrapper que traduce argumentos de Glue a variables de
entorno.

### Cómo viaja la configuración

Esta es la idea que atraviesa todo el repo: **el código nunca tiene una URL ni una
credencial escrita adentro**. Todo sale de variables de entorno, y quién las pone cambia
según dónde corre el código:

```env
# config/local.env líneas 1-21
LAKEHOUSE_TARGET=local
S3_ENDPOINT_URL=http://localhost:9000
S3_ACCESS_KEY_ID=lakehouse
S3_SECRET_ACCESS_KEY=lakehouse-dev-2026
...
POSTGRES_DSN=postgresql://lakehouse:lakehouse-dev-2026@localhost:5432/lakehouse
CKAN_BASE_URL=http://datos.energia.gob.ar
```

- **Host directo** (`uv run ingest ...`): `settings.py` arma un `Settings` (pydantic-settings)
  que lee `config/local.env`; cualquier variable ya exportada en el shell pisa el archivo.
- **Docker/Podman Compose**: `compose.yaml` define bloques `x-*-env` con los mismos nombres de
  variable apuntando a hostnames internos (`http://minio:9000` en vez de `localhost:9000`).
  `airflow` recibe esas variables y se las reenvía a cada runner que lanza (`FORWARDED_ENV`).
- **AWS**: Terraform pone las mismas variables como argumentos por defecto del job de Glue
  (`glue.tf`). El wrapper las lee con `getResolvedOptions` y las pone en `os.environ`, así
  `config.py` las ve igual que en local.

Es la misma variable recorriendo cuatro capas sin que el código de negocio se entere de en
cuál está corriendo — la base del ADR 0001 ("un stack, dos destinos"), sesión 8.

### Qué correr

```powershell
uv sync --all-groups
uv run ingest datasets
type config\local.env
```

### Qué tenés que poder explicar al terminar

- Qué diferencia hay entre landing, bronze y silver, y por qué landing no es una tabla.
- Los tres puntos de entrada del repo y en qué corre cada uno.
- Cómo llega la misma variable de entorno (por ejemplo `POSTGRES_DSN`) desde
  `config/local.env` hasta un job de Spark corriendo dentro de un contenedor.

---

## 3. Sesión 2 — La ingesta, hilo de ejecución

Esta sesión sigue, paso a paso, qué pasa cuando alguien corre:

```powershell
uv run ingest run --dataset produccion_pozo --only 2024
```

### `cli.py`: el punto de entrada

`app = typer.Typer(...)` define la CLI con Typer, que convierte funciones Python en
subcomandos leyendo sus anotaciones de tipo. El comando `run` es:

```python
# pipelines/ingest/cli.py líneas 102-121
@app.command("run")
def run_ingest(
    dataset: DatasetOpt,
    only: OnlyOpt = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Solo lista lo que haria")] = False,
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
) -> None:
    """Ingesta un dataset a landing y registra el resultado en el manifiesto."""
    _configure_logging(verbose)
    spec, settings = _context(dataset)
    session = build_session()
    summary = run(
        spec,
        manifest=Manifest(settings.postgres_dsn),
        storage=LandingStorage.from_settings(settings),
        ckan=_ckan_client(spec, settings, session),
        session=session,
        only=only,
        dry_run=dry_run,
    )
```

`_context(dataset)` busca la fuente en el registro y carga la configuración. `run_ingest` no
tiene lógica propia: arma las cuatro dependencias (`Manifest`, `LandingStorage`, `CkanClient`,
la sesión HTTP) y se las pasa a `run()` de `runner.py` — la CLI arma objetos, el runner los
usa, lo que permite testear `run` sin typer ni consola (sesión 7).

### `registry.get_dataset`: qué fuente es esta

```python
# pipelines/ingest/registry.py líneas 92-98
def get_dataset(name: str, path: Path | str | None = None) -> DatasetSpec:
    """Devuelve una fuente por nombre; error claro si no existe."""
    specs = load_registry(path)
    try:
        return specs[name]
    except KeyError:
        raise KeyError(f"dataset {name!r} no esta en el registro: {sorted(specs)}") from None
```

`load_registry` lee `datasets.yaml` y construye un `DatasetSpec` (`@dataclass(frozen=True)`)
por entrada, con validación en `__post_init__` (líneas 30-41) que revienta temprano si, por
ejemplo, `source_type: ckan` no trae `ckan_package_id`, o un patrón de `include`/`exclude` no
compila como regex. Para `produccion_pozo` la entrada real es:

```yaml
# pipelines/ingest/datasets.yaml líneas 15-33
- name: produccion_pozo
  source_type: ckan
  ckan_package_id: produccion-de-petroleo-y-gas-por-pozo
  landing_prefix: energia/produccion_pozo
  include:
    - "\\(DDJJ abiertas y cerradas\\)"
    - "No Convencional"
    - "^Capítulo IV - Pozos$"
    - "^Padrón de Pozos"
  exclude:
    - "(?i)shape"
  formats: ["CSV"]
```

El comentario del YAML explica el porqué de `include`: el portal publica cada año en dos
familias (una "normal" y otra "DDJJ abiertas y cerradas"), y se eligió provisoriamente la
segunda. `formats: ["CSV"]` existe porque "Capítulo IV - Pozos" está publicado dos veces con
el mismo nombre, en CSV y en SHP: el nombre solo no alcanza para descartar el shapefile.

### `ckan.package_show`: qué hay en el portal

`CkanClient` es un cliente de solo lectura de la API `action` de CKAN (el software del portal
de datos abiertos). `package_show` pide todos los recursos de un paquete:

```python
# pipelines/ingest/ckan.py líneas 106-120
def package_show(self, package_id: str) -> list[Resource]:
    """Recursos de un paquete; lanza si CKAN responde error."""
    url = f"{self.base_url}/api/3/action/package_show"
    response = self.session.get(
        url, params={"id": package_id}, timeout=self.timeout, allow_redirects=True
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success", False):
        raise RuntimeError(f"CKAN respondio error para {package_id}: {payload.get('error')}")
    raw_resources = payload.get("result", {}).get("resources", []) or []
    resources = [resource_from_dict(r, self.host_suffix) for r in raw_resources]
    return resources
```

Cada recurso crudo pasa por `resource_from_dict`, que normaliza tipos: el `size` a veces llega
como string y hay que convertirlo a `int`; `last_modified` a veces viene nulo y se usa
`created` como sustituto. El resultado es una lista de `Resource` (`@dataclass(frozen=True)`).

Un detalle que aparece dos veces en el código: `force_http`.

```python
# pipelines/ingest/ckan.py líneas 40-46
def force_http(url: str, host_suffix: str = PORTAL_HOST_SUFFIX) -> str:
    """Baja a http:// las URLs del portal (https redirige 301 a http)."""
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if parts.scheme == "https" and (host == host_suffix or host.endswith("." + host_suffix)):
        return urlunsplit(("http", parts.netloc, parts.path, parts.query, parts.fragment))
    return url
```

El portal `energia.gob.ar` responde con un redirect 301 de https a http; sin esta función cada
descarga pagaría esa vuelta extra de red. Se aplica sobre la URL base del cliente y sobre cada
URL de recurso, así el request nunca toca https en este dominio.

`build_session` arma una `requests.Session` con reintentos automáticos:

```python
# pipelines/ingest/ckan.py líneas 49-70
def build_session(
    total_retries: int = 4,
    backoff_factor: float = 1.0,
    pool_maxsize: int = 8,
) -> requests.Session:
    """Session con reintentos y backoff exponencial para 429/5xx."""
    retry = Retry(
        total=total_retries,
        ...
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
```

**Backoff exponencial**: entre reintento y reintento la espera crece (con `backoff_factor=1.0`:
~1s, 2s, 4s, 8s) en vez de reintentar de inmediato, para no seguir golpeando un servidor que ya
devuelve 503 por sobrecarga. Solo se reintenta sobre `GET`/`HEAD` (idempotentes: pedirlos dos
veces no cambia nada del lado del servidor) y solo sobre esos códigos de estado.

### `runner.discover`: qué recursos entran en esta corrida

```python
# pipelines/ingest/runner.py líneas 106-135
def discover(
    spec: DatasetSpec,
    ckan: CkanClient | None = None,
    session: requests.Session | None = None,
    only: str | None = None,
) -> list[Resource]:
    """Recursos de la fuente tras include/exclude, `--only` y deduplicacion por resource_id."""
    if spec.source_type == "ckan":
        candidates = ckan.package_show(spec.ckan_package_id or "")
    else:
        candidates = http_file_resources(spec, session or build_session())

    only_pattern = re.compile(only, re.IGNORECASE) if only else None
    selected: dict[str, Resource] = {}
    for resource in candidates:
        if not spec.matches(resource.name, resource.format):
            continue
        if only_pattern and not only_pattern.search(resource.name):
            continue
        if not resource.url:
            continue
        selected.setdefault(resource.id, resource)
    return list(selected.values())
```

Tres filtros en cadena: `spec.matches` (`include`/`exclude`/`formats`), `--only` (regex de
línea de comandos para acotar una corrida sin tocar el YAML), y la deduplicación por
`resource_id` con un `dict` (`selected.setdefault`) — "el portal publica 2024 dos veces con
ids distintos y nombres iguales", deduplicar por nombre perdería uno de los dos recursos.

Para `source_type: http_file` (el caso de `reservas`), no hay API que devuelva metadatos: se
piden por `HEAD` (`head_metadata`, líneas 71-83 de `runner.py`) — un `GET` sin cuerpo de
respuesta, que alcanza para saber tamaño y `Last-Modified` sin bajar el archivo. Como esta
fuente no tiene un `id` propio de portal, `stable_url_id` (en `storage.py`) genera uno
determinístico con un hash de la URL.

### `manifest.latest_ok` e `is_unchanged_by_metadata`: idempotencia en dos niveles

Antes de bajar nada, se pregunta si hace falta. `Manifest.latest_ok` busca la última fila
`status = 'ok'` de ese recurso (nunca `unchanged` ni `failed`):

```python
# pipelines/ingest/manifest.py líneas 83-97
def latest_ok(self, dataset: str, resource_id: str) -> dict[str, Any] | None:
    """Ultima corrida con status `ok` (la ultima descarga real) del recurso."""
    stmt = (
        select(ingestion_manifest)
        .where(
            ingestion_manifest.c.dataset == dataset,
            ingestion_manifest.c.resource_id == resource_id,
            ingestion_manifest.c.status == STATUS_OK,
        )
        .order_by(desc(ingestion_manifest.c.finished_at), desc(ingestion_manifest.c.id))
        .limit(1)
    )
```

Con ese resultado, `is_unchanged_by_metadata` compara tamaño y fecha de modificación contra
lo que reporta la fuente ahora:

```python
# pipelines/ingest/runner.py líneas 141-148
def is_unchanged_by_metadata(previous: dict[str, Any] | None, resource: Resource) -> bool:
    """True si tamaño y last_modified de origen coinciden con la ultima corrida ok."""
    if not previous or resource.size is None or resource.last_modified is None:
        return False
    return (
        previous["size_bytes_source"] == resource.size
        and previous["last_modified_source"] == resource.last_modified
    )
```

Si coinciden, no hace falta tocar la red: se copia el resultado anterior y se cierra la fila
como `unchanged` sin descargar. Si no, se descarga de verdad y recién ahí se compara el
**contenido** (sha256): igual, aunque cambió la fecha, sigue siendo `unchanged`. Solo contenido
nuevo es `ok` — "idempotencia en dos niveles": uno barato (metadata) y uno caro pero exacto
(hash) para cuando el primero no alcanza.

### `stream_download`: streaming, no descarga a disco

```python
# pipelines/ingest/runner.py líneas 151-157
def stream_download(session: requests.Session, url: str) -> Iterator[bytes]:
    """GET en streaming: el contenido pasa a landing sin tocar el disco local."""
    with session.get(
        force_http(url), stream=True, timeout=DOWNLOAD_TIMEOUT, allow_redirects=True
    ) as response:
        response.raise_for_status()
        yield from response.iter_content(chunk_size=DOWNLOAD_CHUNK)
```

`stream=True` le dice a `requests` que no baje la respuesta entera de una: abre la conexión y
deja que el llamador pida bloques con `iter_content`. Es un generador: no ejecuta nada hasta
que alguien itera sobre ella (`storage.upload_stream`). Un archivo de 300 MB nunca ocupa
300 MB en RAM ni en disco: pasa en bloques de 1 MB directo hacia la subida.

### `storage.upload_stream`: multipart, `_rebuffer` y sha256 al vuelo

```python
# pipelines/ingest/storage.py líneas 125-159 (resumido)
def upload_stream(self, key: str, chunks: Iterable[bytes]) -> UploadResult:
    """Sube un iterador de chunks con multipart y calcula sha256 al vuelo."""
    digest = hashlib.sha256()
    total = 0
    upload_id = self.client.create_multipart_upload(Bucket=self.bucket, Key=key)["UploadId"]
    parts: list[dict[str, object]] = []
    try:
        for number, part in enumerate(_rebuffer(chunks, self.part_size), start=1):
            digest.update(part)
            total += len(part)
            response = self.client.upload_part(
                Bucket=self.bucket, Key=key, PartNumber=number, UploadId=upload_id, Body=part,
            )
            parts.append({"ETag": response["ETag"], "PartNumber": number})
        # sin partes (contenido vacío): abort + put_object("") en vez de completar sin partes
        self.client.complete_multipart_upload(
            Bucket=self.bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts},
        )
    except Exception:
        self.client.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload_id)
        raise
    return UploadResult(key=key, sha256=digest.hexdigest(), size_bytes=total)
```

**Multipart upload**: S3 sube un objeto grande en partes independientes (`upload_part`) que
recién se ensamblan al final (`complete_multipart_upload`); cada parte se reintenta sola sin
perder las anteriores, con un mínimo de 5 MB salvo la última. Acá `PART_SIZE` es 8 MB, pero
`stream_download` entrega chunks de 1 MB: `_rebuffer` los reagrupa:

```python
# pipelines/ingest/storage.py líneas 60-71
def _rebuffer(chunks: Iterable[bytes], part_size: int) -> Iterator[bytes]:
    """Reagrupa chunks arbitrarios en bloques de `part_size` (el ultimo puede ser menor)."""
    buffer = bytearray()
    for chunk in chunks:
        if not chunk:
            continue
        buffer.extend(chunk)
        while len(buffer) >= part_size:
            yield bytes(buffer[:part_size])
            del buffer[:part_size]
    if buffer:
        yield bytes(buffer)
```

Un generador que emite bloques de exactamente `part_size` (salvo el último). `digest.update`
actualiza el hash **sobre las mismas partes que se suben**: el sha256 sale del contenido
completo sin releerlo — un **hash como identidad del contenido**, justo lo que compara
`_download` para decidir `ok` vs `unchanged`. Si algo falla, el `except` aborta el multipart y
relanza la excepción, que termina en `manifest.finish_failed`.

### `manifest.finish_ok`: cerrando el ciclo

```python
# pipelines/ingest/manifest.py líneas 99-131
def start(self, *, dataset, source_type, resource_id, ..., landing_key=None) -> int:
    """Abre una fila en estado `failed` (pesimista) y devuelve su id."""
    stmt = insert(ingestion_manifest).values(..., status=STATUS_FAILED, error=None, started_at=_now())
    with self.engine.begin() as conn:
        run_id = conn.execute(stmt).inserted_primary_key[0]
    return int(run_id)
```

Esto es el **pesimismo del manifiesto**: la fila nace en `failed`, no en un estado neutro. Si
el proceso muere entre `start()` y el `finish_*` (corte de luz, OOM-kill, `Ctrl+C`), queda como
evidencia de un intento incompleto. Solo el cierre exitoso la promueve a `ok`/`unchanged`.

### Cómo se arma todo: `process_resource`

```python
# pipelines/ingest/runner.py líneas 266-287
def process_resource(spec, resource, manifest, storage, session, ingest_date) -> RunItem:
    """Ingesta un recurso. Nunca lanza: un recurso roto no corta la corrida."""
    try:
        previous = manifest.latest_ok(spec.name, resource.id)
        if is_unchanged_by_metadata(previous, resource):
            return _skip_download(manifest, spec, resource, previous, ingest_date)
        return _download(manifest, storage, session, spec, resource, previous, ingest_date)
    except Exception as exc:
        return RunItem(resource_id=resource.id, resource_name=resource.name,
                        status=STATUS_FAILED, error=f"{type(exc).__name__}: {exc}")
```

El `except Exception` es amplio a propósito: que uno de N recursos falle no puede tirar abajo
los N-1 restantes. `run()` recorre `discover(...)` llamando a `process_resource`, y acumula
todo en un `RunSummary` con contadores y un `exit_code` de 1 si hubo algún `failed` — así CI o
un DAG detectan la falla sin parsear logs.

### Qué correr

```powershell
uv run ingest list --dataset produccion_pozo --only 2024
uv run ingest run --dataset produccion_pozo --only 2024 --dry-run
uv run ingest run --dataset produccion_pozo --only 2024
uv run ingest manifest --dataset produccion_pozo -n 5
```

### Qué tenés que poder explicar al terminar

- Por qué la comparación de metadata no basta y hace falta una segunda por sha256.
- Qué significa que `start()` inserte en estado `failed`.
- Qué es multipart upload y por qué existe `_rebuffer`.

---

## 4. Sesión 3 — Bronze

### `config.py`: configuración sin dependencias

```python
# pipelines/spark_jobs/config.py líneas 1-6
"""Configuración de los jobs del lakehouse, leída del entorno.

No usa pydantic a propósito: la imagen del runner de Spark solo trae la stdlib y PySpark
(ver ADR 0004), así que este módulo tiene que funcionar sin dependencias.
"""
```

Distinto de `settings.py` (que sí usa `pydantic-settings`): el runner de Spark es una imagen
oficial de Apache sin el resto de dependencias del proyecto. `load_config` lee el entorno con
`os.environ.get`, y si falta algo cae a `read_env_file` (parser manual de `KEY=VALUE`) sobre
`config/local.env`. `is_aws` y `s3_scheme` son los dos únicos lugares donde el código "sabe"
en qué destino corre: `s3_scheme` devuelve `"s3a"` en local y `"s3"` en Glue.

### `session.py`: qué es una SparkSession y qué es un catálogo Iceberg

Una **SparkSession** es el punto de entrada único a todo lo que hace Spark: leer archivos,
correr SQL, escribir tablas. Detrás arranca una JVM y, con `local[*]`, corre en un solo
proceso usando todos los núcleos como un mini-clúster de un nodo. Un **catálogo Iceberg** es
el registro que le dice a Spark qué namespaces y tablas existen, dónde están sus archivos, su
esquema y su historial de snapshots — lo que permite escribir `lake.bronze.produccion_pozo` en
SQL y que Spark sepa a qué carpeta de qué bucket corresponde.

Hay dos funciones que arman ese catálogo de dos maneras distintas, y `build_spark` elige una
según `conf.is_aws`:

```python
# pipelines/spark_jobs/session.py líneas 16-40
def _catalogo_rest(builder, conf: LakehouseConfig):
    """Destino local: catálogo Iceberg REST y objetos en MinIO (endpoint y claves propias)."""
    catalog = f"spark.sql.catalog.{CATALOG}"
    return (
        builder.master("local[*]")
        .config(catalog, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog}.type", "rest")
        .config(f"{catalog}.uri", conf.iceberg_catalog_uri)
        .config(f"{catalog}.warehouse", conf.iceberg_warehouse)
        .config(f"{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{catalog}.s3.endpoint", conf.s3_endpoint_url)
        .config(f"{catalog}.s3.path-style-access", "true")
        ...
    )
```

```python
# pipelines/spark_jobs/session.py líneas 43-55
def _catalogo_glue(builder, conf: LakehouseConfig):
    """Destino aws: catálogo Glue y objetos en S3 con las credenciales del rol del job."""
    catalog = f"spark.sql.catalog.{CATALOG}"
    return (
        builder.config(catalog, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"{catalog}.warehouse", conf.glue_warehouse)
        .config(f"{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    )
```

Las dos configuran un catálogo `lake`, pero una habla con un catálogo REST propio (MinIO, con
endpoint y claves explícitas) y la otra con el Glue Data Catalog (sin endpoint ni claves: usa
las credenciales del rol del job). El código de los jobs escribe `lake.bronze.x` sin saber
cuál está detrás — la implementación concreta del ADR 0001.

### `bronze_rules.py`: las reglas puras

Este módulo no importa PySpark. Contiene funciones que se pueden testear sin JVM (ver sesión
7) porque trabajan sobre tipos simples: strings, dataclasses, listas.

```python
# pipelines/spark_jobs/bronze_rules.py líneas 71-76
def table_for_resource(rules: list[TableRule], resource_name: str) -> str | None:
    """Tabla del primer patrón que coincide; None si el recurso no está mapeado."""
    for rule in rules:
        if re.search(rule.match, resource_name, re.IGNORECASE):
            return rule.table
    return None
```

Esto resuelve, para cada recurso ingerido, a qué tabla bronze va, según
`bronze_tables.yaml`:

```yaml
# pipelines/spark_jobs/bronze_tables.yaml líneas 15-23
produccion_pozo:
  - match: "\\(DDJJ abiertas y cerradas\\)"
    table: lake.bronze.produccion_pozo
  - match: "No Convencional"
    table: lake.bronze.produccion_pozo_no_convencional
  - match: "^Capítulo IV - Pozos$"
    table: lake.bronze.pozo_catalogo
  - match: "^Padrón de Pozos"
    table: lake.bronze.pozo_primera_produccion
```

El comentario del YAML explica la razón de fondo: `produccion_pozo` no es homogéneo. "No
Convencional" es un subconjunto de los anuales (mezclarlo duplicaría filas), "Capítulo IV -
Pozos" es un catálogo con otro esquema, y el padrón trae solo tres columnas — mejor una tabla
por tipo de recurso; lo que no matchea se saltea con `WARNING` en vez de forzarlo.

`pending_files` decide qué hay que (re)cargar:

```python
# pipelines/spark_jobs/bronze_rules.py líneas 97-102
def pending_files(landed, loaded_sha256: dict[str, str]) -> list[LandedFile]:
    """Recursos nuevos o cuyo sha256 cambió respecto de lo ya cargado en bronze."""
    return [file for file in landed if loaded_sha256.get(file.resource_id) != file.sha256]
```

Lo mismo que la idempotencia de la ingesta, aplicado a bronze: si el sha256 del manifiesto es
igual al ya cargado, no hace falta releer ese CSV.

Bronze necesita leer el manifiesto, que vive en Postgres. Como el runner no tiene SQLAlchemy
instalado, se lee por JDBC directamente desde Spark:

```python
# pipelines/spark_jobs/bronze_rules.py líneas 105-117
def latest_ok_query(dataset: str) -> str:
    """Subconsulta JDBC con la última corrida `ok` de cada recurso del dataset."""
    if not dataset.replace("_", "").isalnum():
        raise ValueError(f"dataset invalido: {dataset}")
    return (
        "(SELECT DISTINCT ON (resource_id) "
        "resource_id, resource_name, landing_key, sha256, ingest_date "
        "FROM ingestion_manifest "
        f"WHERE dataset = '{dataset}' AND status = 'ok' "
        "ORDER BY resource_id, finished_at DESC, id DESC) AS ultimo_ok"
    )
```

El chequeo `dataset.replace("_", "").isalnum()` existe porque `dataset` termina interpolado
directo en un string SQL: interpolar sin validar es la forma clásica de abrir una inyección.

### `bronze_load.py`: el hilo de ejecución

Comando: `spark-submit pipelines/spark_jobs/bronze_load.py --dataset produccion_pozo`.

1. `parse_args` valida `--dataset` contra `dataset_names()`, con `--resource-id` opcional para
   recargar solo uno.
2. `build_spark(...)` arma la SparkSession, como se vio arriba.
3. `read_manifest(...)` corre la subconsulta JDBC de `latest_ok_query`: la única lectura de
   Postgres del job, trae la última corrida `ok` de cada recurso.
4. Por tabla destino, `CREATE NAMESPACE IF NOT EXISTS` (un namespace es, en SQL corriente, un
   esquema) y `loaded_sha256(spark, table)` para saber qué ya está cargado:

```python
# pipelines/spark_jobs/bronze_load.py líneas 53-64
def loaded_sha256(spark: SparkSession, table: str) -> dict[str, str]:
    """sha256 ya cargado por recurso. Vacío si la tabla todavía no existe."""
    if not spark.catalog.tableExists(table):
        return {}
    rows = (
        spark.table(table)
        .groupBy("_resource_id")
        .agg(F.max("_source_sha256").alias("sha256"))
        .collect()
    )
    return {row["_resource_id"]: row["sha256"] for row in rows}
```

5. `pending_files(files, loaded_sha256(...))` filtra lo que falta cargar.
6. Por cada archivo pendiente, `load_resource` lee el CSV, agrega metadatos y escribe la
   partición:

```python
# pipelines/spark_jobs/bronze_load.py líneas 67-87
def read_landing_csv(spark: SparkSession, uri: str) -> DataFrame:
    """CSV crudo como strings, con los nombres de columna sin BOM."""
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .csv(uri)
    )
    return df.toDF(*[clean_column_name(name) for name in df.columns])

def with_metadata(df: DataFrame, file: LandedFile) -> DataFrame:
    """Columnas de linaje: de dónde salió la fila y cuándo entró."""
    return (
        df.withColumn("_resource_id", F.lit(file.resource_id))
        .withColumn("_source_key", F.lit(file.landing_key))
        .withColumn("_source_sha256", F.lit(file.sha256))
        .withColumn("_ingest_date", F.to_date(F.lit(file.ingest_date)))
        .withColumn("_loaded_at", F.current_timestamp())
        .withColumn("data_origin", F.lit("real"))
    )
```

`inferSchema: false` es la clave de "todo string": Spark no intenta adivinar tipos, cada
columna queda como texto. Es a propósito: bronze no tipa ni limpia — eso es de silver. Si
bronze tipara, un valor mal formado se perdería (`null`) antes de poder auditarlo.

Las **columnas `_`** son metadatos de **linaje**: de qué recurso salió cada fila, qué archivo
de landing, qué hash, cuándo se ingirió y cuándo se cargó. `data_origin` (`"real"` acá) es la
marca de trazabilidad: distingue datos reales de simulados o derivados.

```python
# pipelines/spark_jobs/bronze_load.py líneas 90-102
def write_partition(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Crea la tabla la primera vez; después reemplaza solo la partición del recurso."""
    if spark.catalog.tableExists(table):
        df.writeTo(table).option("merge-schema", "true").overwritePartitions()
        return
    (
        df.writeTo(table)
        .using("iceberg")
        .partitionedBy(F.col("_resource_id"))
        .tableProperty("write.spark.accept-any-schema", "true")
        .create()
    )
```

Una **partición** es un subconjunto de una tabla separado por el valor de una columna (acá,
`_resource_id`). `overwritePartitions()` reemplaza solo las que trae el DataFrame nuevo, sin
tocar las demás — recargar 2024 no toca 2006-2023. `merge-schema: true` tolera que un año
traiga columnas que otro no tenía, y `write.spark.accept-any-schema` habilita esa tolerancia.

Al final, `main()` corre `spark.stop()` en un `finally`: siempre cierra la SparkSession, corra
bien o mal el resto.

### `infra/docker/spark-defaults.conf`: por qué los jars van ahí y no en el código

```
# infra/docker/spark-defaults.conf líneas 13-18
spark.jars.packages  org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1,org.apache.hadoop:hadoop-aws:3.4.1,org.postgresql:postgresql:42.7.7
spark.jars.ivy       /home/spark/.ivy2
spark.driver.memory  4g
```

Un **jar** es un paquete compilado de Java/Scala (equivalente a una wheel, para la JVM).
`spark.jars.packages` le dice a Spark qué bajar de Maven antes de arrancar; no puede vivir en
`session.py` porque cuando `build_spark()` corre, la JVM ya levantó, y ese `.config(...)`
puesto ahí se ignora (o falla con `ClassNotFoundException`). `spark.jars.ivy` fija dónde
quedan cacheados (`ivy-cache`), así la primera corrida baja ~700 MB y las siguientes no.

### Qué correr

```powershell
podman-compose -f infra\docker\compose.yaml --profile core up -d
scripts\spark-submit.ps1 pipelines/spark_jobs/bronze_load.py --dataset produccion_pozo
uv run python scripts/check_lake.py --namespace bronze
```

### Qué tenés que poder explicar al terminar

- Qué es una SparkSession y por qué hace falta un catálogo aparte del motor.
- Qué cambia entre `_catalogo_rest` y `_catalogo_glue`.
- Por qué bronze no tipa nada y qué perdería si lo hiciera.
- Por qué los jars están en `spark-defaults.conf` y no en `session.py`.

---

## 5. Sesión 4 — Silver y contratos

### El formato del contrato, campo por campo

Un contrato es un YAML en `pipelines/contracts/`. Este es el de `produccion_pozo`, con sus
campos de cabecera:

```yaml
# pipelines/contracts/produccion_pozo.yaml líneas 5-11
table: lake.silver.produccion_pozo
source: lake.bronze.produccion_pozo
primary_key: [idpozo, anio, mes]
partition_by: [anio]
dedupe_by: fechaingreso
```

- `table`: la tabla silver que produce el job. `source`: la tabla bronze de la que lee.
- `primary_key`: columnas que identifican una fila única; el job deduplica por ellas.
- `partition_by`: columnas de partición Iceberg de la tabla silver.
- `dedupe_by`: opcional. Ante una clave repetida gana la fila con el valor más alto de esta
  columna (`fechaingreso`: un pozo puede tener una declaración rectificada, gana la más nueva).

Y cada columna, por ejemplo:

```yaml
# pipelines/contracts/produccion_pozo.yaml líneas 78-83
- name: tef
  type: double
  nullable: true
  min: 0
  max: 744
  description: "Horas efectivas de produccion en el mes; 744 = 31 dias por 24 horas"
```

`type` (`int`, `bigint`, `double`, `string`, `boolean`, `date`, `timestamp`), `nullable`,
`description`, y opcionalmente `min`/`max`/`allowed_values`. El contrato de `produccion_pozo`
tiene 38 columnas (verificado por un test); el de `pozo_primera_produccion` solo 3 (`idpozo`,
`anio`, `mes`): una fila por pozo con su primera fecha de producción.

### `silver_rules.py`: de YAML a SQL

Este módulo es, literalmente, el contrato de datos convertido en código (ADR 0005). No tiene
reglas de negocio propias: todo lo que hace es leer el YAML y generar expresiones SQL.

```python
# pipelines/spark_jobs/silver_rules.py líneas 178-189
def cast_expression(column: ContractColumn) -> str:
    """Expresión SQL que convierte la columna string de bronze al tipo del contrato."""
    source = f"nullif(trim(`{column.name}`), '')"
    if column.type == "string":
        return source
    if column.type == "boolean":
        return f"CASE lower({source}) WHEN 't' THEN true WHEN 'f' THEN false END"
    return f"try_cast({source} AS {SPARK_TYPES[column.type]})"
```

`trim` recorta espacios; `nullif(x, '')` convierte un string vacío en `NULL`. `boolean` no
tiene cast directo para `'t'`/`'f'`, se arma con `CASE`. Para el resto, `try_cast` en vez de
`cast`: con ANSI (Spark 4), un `cast` que falla revienta el job; `try_cast` da `NULL` en la
fila que no castea, sin tirar abajo la carga completa.

```python
# pipelines/spark_jobs/silver_rules.py líneas 197-216
def reject_rules(contract: Contract) -> list[tuple[str, str]]:
    """Pares (condición SQL que es verdadera cuando la fila viola la regla, motivo)."""
    rules = []
    for column in contract.columns:
        value = cast_expression(column)
        if column.minimum is not None:
            rules.append((f"{value} < {column.minimum}", f"{column.name} menor que {column.minimum}"))
        if column.maximum is not None:
            rules.append((f"{value} > {column.maximum}", f"{column.name} mayor que {column.maximum}"))
        if column.allowed_values:
            allowed = ", ".join(sql_string(v) for v in column.allowed_values)
            rules.append((f"{value} NOT IN ({allowed})", f"{column.name} fuera de allowed_values"))
    return rules
```

Cada regla es un par `(condición, motivo)`. `reject_reason_expression` las junta con
`concat_ws('; ', CASE WHEN ... THEN 'motivo' END, ...)`: `concat_ws` ignora los `NULL`, así que
una regla que no se viola no aporta texto, y si ninguna se viola el resultado es `''` (vacío,
no `NULL` — importante, porque `silver_load.py` filtra comparando contra `''`).

### `silver_load.py`: el hilo de ejecución

Comando: `spark-submit pipelines/spark_jobs/silver_load.py --contract produccion_pozo`.

Por cada recurso pendiente (mismo patrón que bronze: comparar sha256 de bronze contra el ya
cargado en silver, con `pending_resources`), el job hace:

```python
# pipelines/spark_jobs/silver_load.py líneas 82-93
def flag_rejects(spark, contract, resource_id) -> DataFrame:
    """Filas de bronze del recurso con una columna `reject_reason` (vacía si está bien)."""
    source = spark.table(contract.source).filter(F.col("_resource_id") == resource_id)
    return source.withColumn("reject_reason", F.expr(reject_reason_expression(contract)))

def typed_rows(flagged, contract) -> DataFrame:
    """Filas aceptadas, casteadas al tipo del contrato y con el linaje de bronze."""
    accepted = flagged.filter("reject_reason = ''")
    return accepted.selectExpr(*select_expressions(contract), *LINEAGE).withColumn(
        "_silver_loaded_at", F.current_timestamp()
    )

def rejected_rows(flagged, contract) -> DataFrame:
    """Filas rechazadas con sus strings originales: la cuarentena es para auditar."""
    original = [f"`{name}`" for name in column_names(contract)]
    return flagged.filter("reject_reason <> ''").selectExpr(
        "reject_reason", "current_timestamp() AS _rejected_at", *original, *LINEAGE,
    )
```

Punto central de **checks duros vs. blandos**: `flag_rejects` marca cada fila con su
`reject_reason` (vacío si no viola nada). Las que violan `min`/`max`/`allowed_values`
(blandos) van a la **cuarentena** con sus valores **originales sin castear** — auditar un
rechazo requiere ver el dato tal cual llegó. Las que pasan siguen a `typed_rows`, que recién
ahí castea de verdad.

```python
# pipelines/spark_jobs/silver_load.py líneas 107-114
def deduplicate(df: DataFrame, contract: Contract) -> DataFrame:
    """Una fila por clave primaria: gana la de `dedupe_by` más alto (la rectificativa)."""
    if not contract.dedupe_by:
        return df.dropDuplicates(list(contract.primary_key))
    orden = Window.partitionBy(*contract.primary_key).orderBy(
        F.col(contract.dedupe_by).desc_nulls_last()
    )
    return df.withColumn("_orden", F.row_number().over(orden)).filter("_orden = 1").drop("_orden")
```

Una **ventana** (`Window`) calcula algo "por grupo" sin colapsar filas (a diferencia de un
`GROUP BY`). `partitionBy` agrupa por clave, `orderBy(dedupe_by desc)` ordena cada grupo y
`row_number()` asigna 1, 2, 3... `_orden = 1` es la "ganadora" — la rectificativa más reciente.
Sin `dedupe_by`, se usa `dropDuplicates`, sin criterio de desempate.

```python
# pipelines/spark_jobs/silver_load.py líneas 117-132
def measure(df: DataFrame, contract: Contract) -> Measures:
    """Filas, claves distintas y nulos por columna obligatoria, en una sola pasada."""
    obligatorias = required_columns(contract)
    aggregations = [
        F.count(F.lit(1)).alias("filas"),
        F.count_distinct(*[F.col(name) for name in contract.primary_key]).alias("claves"),
    ]
    aggregations += [
        F.count(F.when(F.col(name).isNull(), 1)).alias(f"nulos_{name}") for name in obligatorias
    ]
    row = df.agg(*aggregations).first()
    return Measures(rows=row["filas"], keys=row["claves"], nulls={...})
```

Cuenta filas totales, claves primarias distintas (si `rows != keys` sobrevivieron duplicados a
la deduplicación) y nulos por cada columna `nullable: false`, todo en una sola pasada. Con esas
medidas, `hard_failures` decide si el recurso puede publicarse:

```python
# pipelines/spark_jobs/silver_rules.py líneas 85-96
def hard_failures(measures: Measures, rows_in: int, rows_rejected: int) -> list[str]:
    """Motivos por los que un recurso no puede entrar a silver."""
    failures = [
        f"{name}: {count} nulos en una columna no nullable"
        for name, count in measures.nulls.items() if count
    ]
    if measures.rows != measures.keys:
        failures.append(f"clave primaria duplicada: {measures.rows} filas y {measures.keys} claves")
    if too_many_rejects(rows_in, rows_rejected):
        failures.append(f"rechazos {rows_rejected / rows_in:.2%} sobre el umbral tolerado")
    return failures
```

`too_many_rejects` corta si más del 1 % (`REJECT_THRESHOLD = 0.01`) de las filas se rechazaron:
un porcentaje alto no es "unas pocas filas sueltas" sino indicio de que cambió el esquema de la
fuente o el contrato está mal escrito, y ahí conviene frenar antes que publicar datos a medias.

Si `hard_failures` no está vacío, el recurso **no se escribe** y el job termina con código 1
más adelante. Si está vacío, `write_partitions` escribe (mismo patrón que en bronze):

```python
# pipelines/spark_jobs/silver_load.py líneas 151-182
def load_resource(spark, contract, resource_id) -> RunReport:
    """Procesa un recurso completo y devuelve qué pasó con él."""
    started = time.monotonic()
    report = RunReport(resource_id=resource_id)
    flagged = flag_rejects(spark, contract, resource_id).cache()
    try:
        report.rows_in = flagged.count()
        rejected = rejected_rows(flagged, contract)
        report.rows_rejected = rejected.count()
        if report.rows_rejected:
            write_partitions(spark, rejected, rejects_table(contract), ["_resource_id"])
        accepted = deduplicate(typed_rows(flagged, contract), contract)
        measures = measure(accepted, contract)
        report.rows_out = measures.rows
        report.hard_failures = hard_failures(measures, report.rows_in, report.rows_rejected)
        if report.hard_failures:
            return report
        write_partitions(spark, accepted, contract.table, list(contract.partition_by))
    finally:
        flagged.unpersist()
    return report
```

`.cache()` marca `flagged` para que Spark lo mantenga en memoria (o disco) después de calcularlo
una vez, en vez de recalcularlo desde cero. Se recorre tres veces (contar, filtrar rechazados,
castear aceptados); sin `.cache()`, Spark releería y reevaluaría `reject_reason_expression` tres
veces. `.unpersist()` en el `finally` libera esa memoria apenas termina el recurso.

Por último, `record_runs` deja constancia de todo en `dq_runs`:

```python
# pipelines/spark_jobs/silver_load.py líneas 185-209
def record_runs(spark, contract, reports: list[RunReport]) -> None:
    """Historial de calidad: una fila por recurso y corrida, para poder consultarlo."""
    now = datetime.now(timezone.utc)
    rows = [(now, contract.name, report.resource_id, report.rows_in, report.rows_out,
              report.rows_rejected, " | ".join(report.hard_failures), run_status(report))
             for report in reports]
    df = spark.createDataFrame(rows, schema=DQ_RUNS_SCHEMA)
    table = dq_runs_table(contract)
    if spark.catalog.tableExists(table):
        df.writeTo(table).append()
        return
    df.writeTo(table).using("iceberg").create()
```

`dq_runs` (data quality runs) es el historial: cada corrida de silver, para cada recurso que
procesó, deja una fila con cuántas entraron, cuántas salieron, cuántas se rechazaron y si
falló. Es lo que consulta `scripts/check_lake.py --namespace silver` para responder "¿cuándo
corrió esto la última vez y qué pasó?" sin tener que abrir logs.

### Qué correr

```powershell
scripts\spark-submit.ps1 pipelines/spark_jobs/silver_load.py --contract produccion_pozo
uv run python scripts/check_lake.py --namespace silver
```

### Qué tenés que poder explicar al terminar

- La diferencia entre un check duro y uno blando, con un ejemplo de este contrato.
- Por qué la cuarentena guarda los valores originales y no los casteados.
- Qué hace la ventana de `deduplicate` sin `dedupe_by`.
- Por qué `flagged` se cachea.

---

## 6. Sesión 5 — Orquestación local

### `compose.yaml`, servicio por servicio

```yaml
# infra/docker/compose.yaml líneas 3-8
# Perfiles:
#   core      -> MinIO (S3), Postgres (metadata), catálogo Iceberg REST      [siempre]
#   spark     -> runner efímero de Spark (ADR 0004), no es un servicio        [etapa 1]
#   airflow   -> orquestador; lanza el runner por tarea (ADR 0006)            [etapa 1]
#   streaming -> broker de Kafka (KRaft) para la telemetría 3W                 [etapa 2]
#   mlflow    -> tracking server y model registry del módulo de ML (ADR 0012)  [etapa 2]
```

Un **perfil** de Docker Compose es una etiqueta que agrupa servicios: si no se pide el perfil,
el servicio no se levanta. Esto permite tener un solo archivo de compose para todo el
proyecto sin que levantar el stack básico traiga también Airflow, Kafka o MLflow. Los perfiles
`streaming` y `mlflow` se retoman en detalle en las sesiones 11 y 12; acá solo entran en el
inventario de servicios.

- **`minio`**: object storage compatible con S3. Puertos `9000` (API), `9001` (consola).
  `healthcheck` con `mc ready local` para que los dependientes esperen a que esté listo.
- **`minio-init`**: `restart: "no"`; corre una vez, crea `landing`/`lakehouse` y activa
  versionado en `landing`.
- **`postgres`**: manifiesto de ingesta y bases de Airflow. `init.sql` crea además una base
  `iceberg` sin uso real (el catálogo usa SQLite, ADR 0003).
- **`iceberg-rest`**: catálogo Iceberg por HTTP (`8181`), SQLite como backend (la imagen
  oficial no trae el driver de Postgres).
- **`spark`**: el runner efímero (ADR 0004). Sin `command`: se lanza con `podman-compose
  --profile spark run --rm spark ...`. Monta `pipelines/`, `config/`, `spark-defaults.conf` e
  `ivy-cache`.
- **`airflow`**: `standalone` levanta scheduler, webserver y triggerer en un proceso; no
  ejecuta código propio, lanza el runner por tarea (ADR 0006). Monta el socket de Podman como
  volumen, no bind mount (el cliente Windows traduce mal esas rutas).

### `runner.py` (DAGs) y `DockerOperator`

Un **operador** en Airflow es una clase que sabe ejecutar un tipo de tarea (correr un script
Python, mandar un email, lanzar un contenedor). `DockerOperator` es el que lanza un contenedor
y espera a que termine.

```python
# orchestration/dags/runner.py líneas 53-81
def runner_task(task_id: str, command: str) -> DockerOperator:
    """Tarea que corre `command` dentro del runner, con las dependencias ya instaladas."""
    return DockerOperator(
        task_id=task_id,
        image=IMAGE,
        command=[
            "bash", "-c",
            "python3 -m pip install --user --quiet --disable-pip-version-check "
            f"-r /app/pipelines/spark_jobs/requirements-runner.txt && {command}",
        ],
        docker_url="unix:///var/run/podman/podman.sock",
        network_mode=NETWORK,
        mounts=MOUNTS,
        environment={
            "HOME": "/home/spark", "PYTHONPATH": "/app", ...,
            **{name: os.environ[name] for name in FORWARDED_ENV},
        },
        auto_remove="success",
    )
```

El **socket de Podman** es el archivo por el que se habla con el motor de contenedores por su
API, igual que haría el cliente `podman`/`docker` desde una terminal. `MOUNTS` monta lo mismo
que `spark-defaults.conf`/`ivy-cache` en el compose (el runner de Airflow queda idéntico al de
`spark-submit.ps1`), y `FORWARDED_ENV` reenvía las variables tal cual: las credenciales viven
una sola vez, en el compose, y no se copian en cada DAG.

### Los tres DAGs

```python
# orchestration/dags/produccion_pozo_mensual.py líneas 9-41 (resumido)
with DAG(
    dag_id="produccion_pozo_mensual",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
) as dag:
    ingesta >> bronze >> silver_produccion >> silver_padron
```

- **`schedule`**: cuándo dispara Airflow el DAG (`@monthly`, `@daily`, o un cron). No corre
  nada hasta que el DAG está "despausado" (`airflow dags unpause`).
- **`catchup`**: si `True`, Airflow correría retroactivamente una vez por cada intervalo
  pasado desde `start_date`. Acá siempre `False`: solo interesa la próxima corrida programada.
- **`max_active_runs=1`**: no deja que dos corridas del mismo DAG se solapen (cada tarea lanza
  un contenedor con un driver de Spark de varios GB de RAM).
- **`retries: 1`**: si una tarea falla, Airflow la reintenta una vez antes de marcar el DAG
  como fallado.

Los tres pasos de `produccion_pozo_mensual` están encadenados **linealmente** (`>>`) aun cuando
las dos tareas silver son independientes entre sí: cada runner levanta un driver de Spark de
4 GB y la máquina tiene 16, así que correrlas en paralelo compite por la misma RAM sin ahorrar
tiempo.

`fractura_diaria` tiene el mismo patrón con `schedule="@daily"` (si el sha256 no cambió, silver
no hace nada gracias a `pending_resources`). `reservas_mensual` encadena `ingesta >> bronze >>
silver` igual que `produccion_pozo_mensual`, con una diferencia: su tarea `bronze` no es un
`spark-submit`, es `python3 -m pipelines.reservas.bronze_load` (pyiceberg, sin Spark) — se
retoma en detalle en la sesión 9.

### Qué correr

```powershell
cd infra\docker
podman-compose --profile core --profile airflow up -d
podman exec ypf-lakehouse_airflow_1 airflow dags unpause fractura_diaria
podman exec ypf-lakehouse_airflow_1 airflow dags trigger fractura_diaria
```

### Qué tenés que poder explicar al terminar

- Para qué sirven los perfiles del compose y por qué `spark` no es un "servicio" en el
  sentido normal.
- Qué hace un `DockerOperator` y de dónde saca las credenciales que le pasa al contenedor.
- Qué significan `schedule`, `catchup`, `max_active_runs` y `retries` en un DAG concreto.
- Por qué `produccion_pozo_mensual` encadena sus tareas linealmente en vez de en paralelo.

---

## 7. Sesión 6 — AWS

### `infra/terraform/*.tf`, archivo por archivo

| Archivo | Qué crea | Por qué |
|---|---|---|
| `versions.tf` | Provider AWS y **state local** | Entorno efímero, una sola persona; un backend remoto pediría bucket y tabla de locks para nada |
| `s3.tf` | Un bucket (`ypf-lakehouse-<account_id>`) con prefijos `landing/`, `warehouse/`, `artifacts/`, `athena-results/` | `force_destroy`, bloqueo público, cifrado, versionado y ciclo de vida (30 días versiones viejas, 7 días resultados de Athena) |
| `iam.tf` | Tres roles IAM (Glue, Step Functions, scheduler), permiso mínimo cada uno | Glue lee/escribe el bucket y descifra el SSM; Step Functions solo arranca los tres jobs; el scheduler solo la máquina de estados |
| `glue.tf` | Glue Data Catalog y tres jobs **genéricos**: `ingest_landing` (Python shell, DPU mínima), `bronze_load`/`silver_load` (Glue ETL, `--datalake-formats iceberg`) | Leen su script de `s3://<bucket>/artifacts/`: tras un cambio de código alcanza con `aws_deploy.ps1`, sin `apply` |
| `stepfunctions.tf` | Una máquina de estados por pipeline, mismos jobs genéricos | El schedule de EventBridge nace **deshabilitado** |
| `athena.tf` | Workgroup con resultados forzados a `athena-results/` | Nadie puede saltearse esa configuración |
| `variables.tf`/`outputs.tf` | Entradas y salidas (`lakehouse_bucket`, `glue_jobs`, `state_machine_arns`) | Las leen los scripts de despliegue |

Nota de lectura: los jobs de Glue fueron genéricos desde la refactorización de fractura (`ingest_landing`, `bronze_load`, `silver_load`) y las máquinas de estado se crean con `for_each` sobre el mapa `local.pipelines` de `stepfunctions.tf`. `outputs.tf` expone `glue_jobs` y `state_machine_arns` (un mapa por pipeline). Cuando leas Terraform, comprobá siempre con `terraform output` que los nombres del código coinciden con los del estado.

### Lo que agregaron reservas y gold: dos jobs más, y el mapa `local.pipelines` creció

`glue.tf` tiene ahora una base de datos de Glue nueva para gold y dos jobs más, con el mismo
criterio de "job genérico, argumentos por Terraform" que ya usaban `bronze_load`/`silver_load`:

```hcl
# infra/terraform/glue.tf líneas 14-17
resource "aws_glue_catalog_database" "gold" {
  name        = "gold"
  description = "Capa gold: modelo dimensional construido con dbt sobre Athena (ADR 0010)."
}
```

`bronze_reservas` es el único job de bronze que **no** es Spark. Corre en Python shell, igual
que `ingest_landing`, porque el ZIP anual de reservas pesa 400 KB y el trabajo real es
desarmar un Excel con encabezado jerárquico (sesión 9) — algo que Spark ni siquiera sabe leer,
y levantar una JVM de varios GB para eso sería puro desperdicio:

```hcl
# infra/terraform/glue.tf líneas 101-132 (resumido)
# Bronze de reservas: el ZIP anual son 400 KB y el trabajo es desarmar un cuadro de Excel,
# algo que Spark no lee. Va como Python shell y escribe la tabla Iceberg con pyiceberg contra
# el Glue Data Catalog (pipelines/reservas/bronze_load.py).
resource "aws_glue_job" "bronze_reservas" {
  name = "bronze_reservas"
  # 1 DPU (16 GB) y no 1/16: openpyxl levanta la planilla entera en memoria y pyarrow arma
  # las 200.000 filas antes de escribirlas. En 1 GB no entra.
  max_capacity = 1
  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "${local.artifacts_uri}/bronze_reservas_job.py"
  }
  default_arguments = merge(local.target_arguments, {
    "--additional-python-modules"  = "pyiceberg[glue]==0.10.0,pyarrow==17.0.0,openpyxl==3.1.5,sqlalchemy==2.0.52,psycopg[binary]==3.2.13,pydantic-settings==2.9.1"
    "--POSTGRES_DSN_SSM_PARAMETER" = var.postgres_dsn_ssm_parameter
  })
}
```

Un detalle de packaging que solo aparece acá: `pyarrow` va listado aparte y no como el extra
`pyiceberg[glue,pyarrow]`, porque Glue separa esa lista por comas y una lista de extras entre
corchetes la rompería. `pydantic-settings` entra aunque el job no la use directo, porque
importar el módulo de manifiesto reexporta `Settings` de `pipelines.ingest`.

`gold_dbt` corre sobre Glue **5.0** (el job type con Spark), aunque dbt-athena no usa Spark
para nada — el motivo es Python: Python shell sigue clavado en 3.9, y dbt-core dejó de dar
soporte a esa versión en la 1.11; Glue 5.0 trae Python 3.11, la misma versión que corre en local:

```hcl
# infra/terraform/glue.tf líneas 153-187 (resumido)
resource "aws_glue_job" "gold_dbt" {
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${local.artifacts_uri}/gold_dbt_job.py"
  }
  default_arguments = {
    # Acá el wheel se instala CON dependencias (sin --no-deps, al revés que bronze/silver):
    # pip tiene que resolver las de dbt.
    "--additional-python-modules" = "${local.artifacts_uri}/${var.wheel_name},dbt-core==1.11.14,dbt-athena==1.11.0"
    "--AWS_REGION"       = var.region
    "--ATHENA_WORKGROUP" = aws_athena_workgroup.lakehouse.name
    "--ATHENA_DATABASE"  = "awsdatacatalog"
    "--S3_STAGING_DIR"   = "${local.bucket_uri}/athena-results/"
    "--S3_DATA_DIR"      = "${local.bucket_uri}/warehouse/gold/"
  }
}
```

En `stepfunctions.tf`, `local.pipelines` gana una entrada `reservas_mensual` cuyo paso `bronze`
apunta al job nuevo, con el mismo esqueleto `ingesta -> bronze -> silver` de siempre:

```hcl
# infra/terraform/stepfunctions.tf líneas 12-39 (resumido)
locals {
  pipelines = {
    produccion_pozo_mensual = { dataset = "produccion_pozo", contract = "produccion_pozo",
      bronze_job = aws_glue_job.bronze_load.name, cron = "cron(0 6 1 * ? *)" }
    fractura_diaria = { dataset = "fractura", contract = "fractura",
      bronze_job = aws_glue_job.bronze_load.name, cron = "cron(0 7 * * ? *)" }
    reservas_mensual = {
      dataset  = "reservas"
      contract = "reservas"
      # El único pipeline cuyo bronze no es Spark: el ZIP anual es un cuadro de Excel y lo
      # parsea un Python shell (glue.tf). El `--dataset` de abajo le llega igual y lo ignora,
      # porque este job carga una sola tabla.
      bronze_job = aws_glue_job.bronze_reservas.name
      cron = "cron(0 6 1 * ? *)"
    }
  }
```

Gold es distinto: no tiene ingesta ni bronze ni silver, es un solo paso. Entra igual al mismo
`for_each` de máquinas de estado fusionando su definición con `merge(...)`, para no duplicar el
recurso de la máquina ni el del schedule de EventBridge:

```hcl
# infra/terraform/stepfunctions.tf líneas 94-109 (resumido)
{
  gold_mensual = {
    Comment       = "gold: dbt build sobre silver, con Athena de motor"
    QueryLanguage = "JSONata"
    StartAt       = "gold"
    States = {
      gold = {
        Type      = "Task"
        Resource  = "arn:aws:states:::glue:startJobRun.sync"
        Arguments = { JobName = aws_glue_job.gold_dbt.name }
        End       = true
      }
    }
  }
},
```

Su cron (`cron(0 6 1 * ? *)`) es el mismo día/hora que los pipelines mensuales de fuentes: gold
corre después de que terminaron de correr sus fuentes ese mismo primer día del mes. `outputs.tf`
suma los dos jobs a la lista existente:

```hcl
# infra/terraform/outputs.tf líneas 6-15
output "glue_jobs" {
  value = [
    aws_glue_job.ingest_landing.name,
    aws_glue_job.bronze_load.name,
    aws_glue_job.bronze_reservas.name,
    aws_glue_job.silver_load.name,
    aws_glue_job.gold_dbt.name,
  ]
}
```

Y en `pipelines/aws/*.py` aparecen dos wrappers nuevos, con el mismo estilo fino de los ya
vistos. `bronze_reservas_job.py` repite el patrón de `ingest_job.py` (instala el wheel a mano
porque corre en Python shell, antes de que el paquete exista):

```python
# pipelines/aws/bronze_reservas_job.py líneas 47-69 (resumido)
def main() -> int:
    logging.basicConfig(level=logging.INFO, ..., force=True)
    args = getResolvedOptions(sys.argv, ["WHEEL_S3_URI", "POSTGRES_DSN_SSM_PARAMETER", *ENV_ARGS])
    for name in ENV_ARGS:
        os.environ[name] = args[name]
    instalar_paquete(args["WHEEL_S3_URI"])
    from pipelines.aws.ssm import parameter_value
    from pipelines.reservas.bronze_load import main as bronze_main
    os.environ["POSTGRES_DSN"] = parameter_value(args["POSTGRES_DSN_SSM_PARAMETER"], args["S3_REGION"])
    return bronze_main([])
```

`force=True` en `basicConfig` es necesario porque el runtime de Glue ya configuró el logging
raíz antes de que el script arranque; sin ese parámetro las líneas `INFO` nunca llegarían a
CloudWatch. `gold_dbt_job.py` no reusa `run_dbt.py` (que asume una SparkSession que en Athena
no existe): invoca a dbt directo contra el target `aws`:

```python
# pipelines/aws/gold_dbt_job.py líneas 40-56 (resumido)
def main() -> int:
    args = getResolvedOptions(sys.argv, [*ENV_ARGS])
    for name in ENV_ARGS:
        os.environ[name] = args[name]
    os.environ["DBT_PROJECT_DIR"] = str(PROJECT_DIR)
    os.environ["DBT_PROFILES_DIR"] = str(PROJECT_DIR)
    os.environ["DBT_TARGET_PATH"] = str(ARTIFACTS_DIR / "target")
    os.environ["DBT_LOG_PATH"] = str(ARTIFACTS_DIR / "logs")
    from dbt.cli.main import dbtRunner
    result = dbtRunner().invoke(["build", "--target", "aws"])
    return 0 if result.success else 1
```

`PROJECT_DIR` sale de `Path(pipelines.__file__).resolve().parent / "dbt"`: como el proyecto de
dbt viaja adentro del wheel, sus archivos quedan en disco de verdad al instalarlo con pip (a
diferencia de `ingest_job.py`, que lo descomprime a mano como zip). `ARTIFACTS_DIR` manda
`target/`/`logs/` a `/tmp/dbt` porque el paquete instalado es de solo lectura. Se usa `build` (no
`run` + `test` por separado) para que un test que falla frene la publicación de una tabla no
validada, en vez de dejarla escrita igual. Los dos wrappers repiten la misma nota que
`bronze_job.py`: nunca `sys.exit(0)`, porque Glue marca como fallo cualquier `SystemExit`, sea
cual sea el código.

### `pipelines/aws/*.py`: los wrappers

La idea común a los tres wrappers: son finos, casi no tienen lógica propia. Traducen
argumentos de Glue a variables de entorno y llaman al mismo código que corre en local.

```python
# pipelines/aws/bronze_job.py líneas 1-28 (completo, salvo comentarios ya citados)
def main() -> int:
    args = getResolvedOptions(sys.argv, ["dataset", "POSTGRES_DSN_SSM_PARAMETER", *ENV_ARGS])
    for name in ENV_ARGS:
        os.environ[name] = args[name]
    os.environ["POSTGRES_DSN"] = parameter_value(
        args["POSTGRES_DSN_SSM_PARAMETER"], args["S3_REGION"]
    )
    return bronze_main(["--dataset", args["dataset"]])
```

`getResolvedOptions` lee los argumentos `--clave valor` del job (los de Terraform, o los que
sobreescribe la máquina de estados). El wrapper exporta lo necesario a `os.environ`, resuelve
el DSN con `parameter_value` y llama a `bronze_main` — la misma `main()` que corre en local.

El wrapper de ingesta (`ingest_job.py`) es el único con lógica propia real, porque Python shell
(el job de Glue más barato) trae Python 3.9, y el proyecto exige `>=3.11`:

```python
# pipelines/aws/ingest_job.py líneas 32-45 (docstring resumido)
def instalar_paquete(uri: str) -> None:
    """Descomprime el wheel del proyecto en /tmp y lo pone en el path de módulos."""
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    local = "/tmp/paquete.whl"
    boto3.client("s3").download_file(bucket, key, local)
    with zipfile.ZipFile(local) as wheel:
        wheel.extractall(PAQUETE_DIR)
    sys.path.insert(0, PAQUETE_DIR)
```

Un **wheel** (`.whl`) es el formato empaquetado de una distribución Python — en el fondo, un
zip. `pip install` valida el `Requires-Python` de `pyproject.toml` y rechazaría este wheel bajo
Python 3.9; la solución es no usar `pip`: bajar el wheel, abrirlo como zip, y agregar esa
carpeta al `sys.path`. Motivo adicional: los contratos se leen con `Path(...).read_text()`, y
un wheel instalado por zipimport no expone sus archivos de datos como archivos reales.

Y en los tres wrappers aparece la misma nota al final:

```python
# pipelines/aws/bronze_job.py líneas 31-37
if __name__ == "__main__":
    codigo = main()
    if codigo:
        sys.exit(codigo)
```

Particularidad del runtime de Glue: cualquier `SystemExit` —incluso `sys.exit(0)`, que en Unix
significa "todo bien"— hace que Glue marque la corrida como fallida. Por eso nunca se llama a
`sys.exit` cuando el código de retorno es 0, solo cuando de verdad hubo un error.

### La máquina de Step Functions, estado por estado

```hcl
# infra/terraform/stepfunctions.tf líneas 47-74 (resumido)
States = {
  ingesta = {
    Type = "Task"
    Resource = "arn:aws:states:::glue:startJobRun.sync"
    Arguments = { JobName = aws_glue_job.ingest_landing.name, Arguments = ... }
    Next = "bronze"
  }
  bronze = { ... Next = "silver" }
  silver = { ... End = true }
}
```

Cada estado usa `glue:startJobRun.sync` — el sufijo `.sync` hace que Step Functions **espere**
a que el job termine y **falle la ejecución si el job falla**, la misma semántica que las
dependencias `>>` de un DAG, en JSON. Los argumentos se arman con JSONata, mezclando los
valores fijos del pipeline con el input de la ejecución, para acotar una corrida sin tocar la
definición de la máquina.

### `scripts/aws_deploy.ps1` y `aws_logs.ps1`

`aws_deploy.ps1` hace `uv build --wheel`, busca el bucket con `terraform output` y sube el
wheel más los tres wrappers a `s3://<bucket>/artifacts/`: tras un cambio de código alcanza con
volver a correrlo, sin Terraform.

`aws_logs.ps1` evita navegar CloudWatch a mano: pide la última corrida de cada job (`aws glue
get-job-runs`) y filtra las líneas que importan ("pendientes", "resumen", "ERROR") en vez de
mostrar todo el ruido de Spark o pip.

### Qué correr

```powershell
cd infra\terraform
terraform init
terraform plan
..\..\scripts\aws_deploy.ps1
$arn = terraform output -json state_machine_arns | ConvertFrom-Json | Select-Object -ExpandProperty produccion_pozo_mensual
aws stepfunctions start-execution --state-machine-arn $arn --input '{"ingesta": {"--only": "^Padr"}, "silver": {"--contract": "pozo_primera_produccion"}}'
scripts\aws_logs.ps1
```

### Qué tenés que poder explicar al terminar

- Por qué la ingesta va en Python shell y bronze/silver en Glue ETL (Spark).
- Cómo llega el DSN de Postgres a un job de Glue sin aparecer en los argumentos.
- Por qué `sys.exit(0)` está prohibido en los wrappers de Glue.
- La inconsistencia entre `outputs.tf` y `glue.tf`, y por qué `terraform validate` no la detecta.
- Por qué `bronze_reservas` es Python shell y `gold_dbt` es Glue ETL aunque ninguno de los dos
  use Spark de verdad para su trabajo principal.
- Cómo entra `gold_mensual` al mismo `for_each` de máquinas de estado que los pipelines de
  ingesta-bronze-silver, siendo un flujo de un solo paso.

---

## 8. Sesión 7 — Tests y CI

### Cómo están organizados los tests

`tests/ingest/` prueba la ingesta; `tests/spark_jobs/` prueba las reglas puras de bronze y
silver. No hay tests que levanten Spark: la lógica de calidad vive en módulos que no importan
PySpark (sesiones 3 y 4), lo que permite testearlos con `pytest` corriente, en segundos.

### Qué es Moto

`moto` es una librería que simula los servicios de AWS en memoria, interceptando las llamadas
que boto3 haría por red. `tests/ingest/conftest.py` usa `mock_aws()`:

```python
# tests/ingest/conftest.py líneas 81-98
@pytest.fixture
def s3_client(aws_credentials):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client

@pytest.fixture
def storage(s3_client) -> LandingStorage:
    return LandingStorage(
        endpoint_url="http://unused", access_key_id="testing", secret_access_key="testing",
        region="us-east-1", bucket=BUCKET, client=s3_client, part_size=5 * 1024 * 1024,
    )
```

Dentro de `with mock_aws()`, cualquier llamada de boto3 a S3 queda atrapada por Moto y
respondida en memoria: los tests de `storage.py` (subida multipart, sha256) corren contra la
lógica real de `LandingStorage`, sin tocar MinIO ni AWS.

### Qué es `responses`

`responses` hace lo mismo que Moto pero para HTTP genérico (no específico de AWS):
intercepta las llamadas de la librería `requests` y les da una respuesta programada.

```python
# tests/ingest/test_ckan.py líneas 39-46
@responses.activate
def test_package_show_parsea_y_normaliza():
    responses.add(
        responses.GET, f"{CKAN_BASE}/api/3/action/package_show", json=PACKAGE_SHOW, status=200,
    )
    resources = CkanClient(CKAN_BASE).package_show("produccion-de-petroleo-y-gas-por-pozo")
```

Con `@responses.activate`, cualquier `session.get(...)` que coincida con la URL registrada
devuelve el JSON programado en vez de salir a la red real: así se simulan casos como "CKAN
devuelve un tamaño como string" sin depender del portal real.

`tests/ingest/test_runner.py` combina las dos: `responses` simula CKAN y las descargas, Moto
simula S3, y `Manifest("sqlite://")` (SQLite en memoria) simula Postgres. El resultado es un
test end-to-end de `run()` completo —deduplicación, `--only`, `dry_run`, un recurso que falla
con 500 sin cortar la corrida— que corre en milisegundos sin ningún servicio externo.

### Cómo se testea sin Spark

`tests/spark_jobs/test_silver_rules.py` es el ejemplo más claro: prueba `cast_expression`,
`reject_rules`, `reject_reason_expression` y `hard_failures` comparando **strings de SQL**
contra lo esperado, sin ejecutar ese SQL en ningún motor:

```python
# tests/spark_jobs/test_silver_rules.py líneas 140-143
def test_cast_numerico_usa_try_cast():
    assert cast_expression(columna("prod_pet", "double")) == (
        "try_cast(nullif(trim(`prod_pet`), '') AS DOUBLE)"
    )
```

Esto es posible porque `silver_rules.py` no ejecuta SQL: solo lo *genera* como texto. El
propio README es honesto sobre el límite: "los de `tests/spark_jobs/` no levantan Spark; la
integración se valida corriendo el job" — que esa expresión sea válida en Spark SQL de verdad
solo se verifica corriéndolo contra el compose local.

### Qué hace cada job de `ci.yml`

```yaml
# .github/workflows/ci.yml líneas 21-45 (resumido)
lint-y-tests:
  - uv sync --all-groups
  - uv run ruff check .
  - uv run ruff format --check .
  - uv run pytest
```

Corre en `ubuntu-latest`, instala con `uv sync`, aplica **ruff** (linter y formateador de
Python en Rust) en modo chequeo y corre toda la suite de `pytest`, contra Moto y SQLite, sin
servicios reales.

```yaml
# .github/workflows/ci.yml líneas 46-68 (resumido)
terraform:
  - terraform fmt -check -recursive
  - terraform init -backend=false
  - terraform validate
```

Sin credenciales de AWS: `-backend=false` alcanza para que `validate` resuelva el provider y
los tipos, sin backend de state ni credenciales reales. Esto es justo lo que **no** detecta la
inconsistencia de `outputs.tf` de la sesión 6: `validate` no comprueba que cada referencia
apunte a un recurso que el `plan` real construiría.

```yaml
# .github/workflows/ci.yml líneas 70-108 (resumido)
dags-importan:
  - pip install "apache-airflow==3.3.1" ... --constraint ".../constraints-3.12.txt"
  - python scripts/check_dags.py
```

Airflow no vive en `uv.lock` porque no se instala en Windows (donde se desarrolla el repo); este
job lo instala aparte, fijado con un archivo de *constraints* oficial. `check_dags.py` usa
`DagBag`, la misma clase que el scheduler usa para descubrir DAGs, sin levantar scheduler ni
webserver — solo para detectar errores de import en minutos.

### Qué correr

```powershell
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
```

### Qué tenés que poder explicar al terminar

- Qué simulan Moto y `responses`, y por qué ninguno de los dos necesita red real.
- Por qué se puede testear la lógica de silver sin instalar Spark.
- Qué NO prueba el CI (integración real con Iceberg, con el socket de Podman, con las
  fuentes públicas) y por qué esa es una decisión consciente, no un olvido.
- Qué diferencia hay entre `terraform validate` y `terraform plan` en términos de qué
  errores detecta cada uno.

---

## 9. Sesión 8 — Las decisiones

### Los doce ADR, en una tabla

| ADR | Decisión | Alternativa descartada | Por qué |
|---|---|---|---|
| 0001 | Stack parametrizado por destino (`local`/`aws`) | Databricks Free Edition | Restringe egress y no permite storage propio |
| 0002 | DuckDB local, Athena en AWS | Trino en local | Cuesta ~2 GB de RAM permanentes; la máquina de dev tiene 16 |
| 0003 | Catálogo Iceberg local con SQLite | Postgres como backend | La imagen oficial no trae driver de Postgres |
| 0004 | Spark en contenedor efímero bajo demanda | Java/Spark en Windows, o clúster real | `winutils.exe`/`HADOOP_HOME` da problemas; `local[*]` alcanza |
| 0005 | Contrato de datos por tabla, en YAML versionado | Reglas embebidas o herramienta de DQ aparte | El YAML es documentación legible y entrada real del job |
| 0006 | Airflow solo orquesta, lanza el runner por Docker/Podman | `PythonOperator` en Airflow, o `KubernetesPodOperator` | Evita duplicar dependencias; evita pedir un clúster |
| 0007 | CI sin levantar el stack completo | Reproducir Podman+MinIO+Spark+Iceberg en CI | Sostener un segundo entorno es caro |
| 0008 | Glue + Step Functions, Neon como Postgres | EMR, MWAA, RDS | MWAA cuesta ~350 USD/mes esté o no corriendo algo |
| 0009 | Gold se modela con dbt-spark (`method: session`) dentro del mismo runner efímero que bronze y silver | dbt-duckdb sobre Iceberg en MinIO | Con DuckDB, gold quedaría en Parquet suelto registrado aparte; `method: session` reutiliza la SparkSession que ya existe y gold queda en `lake.gold`, igual que las capas de abajo |
| 0010 | En AWS, gold corre con dbt-athena dentro de un job de Glue 5.0 (Spark), aunque Spark no se usa | `dbt-glue` (sesión interactiva de Glue), o dbt desde GitHub Actions con OIDC | `dbt-glue` suma un motor más que mantener; GitHub Actions saca a gold de Step Functions y parte el pipeline entre dos orquestadores. Se usa Glue 5.0 y no Python shell porque Python shell sigue en 3.9 y dbt-core dejó de soportarlo en la 1.11 |
| 0011 | Streaming con Kafka (un broker, KRaft) y Spark Structured Streaming, dos queries (bronze crudo + silver por ventana de 1 minuto) | Kinesis local emulado, Redpanda, Flink, o escribir bronze directo desde el productor | Kinesis local no es el servicio real; Flink sumaría un segundo motor para un solo job; escribir bronze desde el productor perdería el sentido del ejercicio (que el checkpoint del consumidor evite duplicar en un reinicio) |
| 0012 | HistGradientBoostingRegressor de scikit-learn, split `GroupKFold` por yacimiento, tracking en MLflow propio (Postgres + MinIO), inferencia batch a Iceberg | Deep learning; split aleatorio; servir el modelo como endpoint HTTP | Con 351 filas y 17 columnas una red neuronal tiene más parámetros que ejemplos; el split aleatorio infla el R² (0,737 contra 0,381 real) por fuga entre pozos del mismo yacimiento; el dato de entrada cambia una vez por mes, un endpoint no aporta nada frente a una tabla batch |

### Quince preguntas de entrevista sobre este proyecto

1. **¿Por qué bronze guarda todo como string?** Un valor mal formado, si bronze lo tipa, se
   pierde (`null`) antes de poder auditarlo. Tipar y descartar es de silver, que guarda lo
   rechazado en cuarentena.

2. **¿Cómo decide la ingesta si un recurso cambió sin descargarlo?** Compara `size` y
   `last_modified` contra la última corrida `ok`. Si no coinciden, descarga y recién ahí
   compara el sha256 del contenido.

3. **¿Por qué la fila del manifiesto nace en `failed`?** Para que un proceso que muere a mitad
   de camino deje evidencia de un intento incompleto. Solo el cierre exitoso la promueve a
   `ok`/`unchanged`.

4. **¿Check duro vs. blando en silver?** El duro (nulo indebido, clave duplicada, >1 % de
   rechazos) frena el job con código 1. El blando manda la fila a cuarentena y la carga sigue.

5. **¿Por qué los jars van en `spark-defaults.conf`?** Porque cuando el código arma la
   `SparkSession` la JVM ya arrancó; deben declararse antes de que arranque.

6. **¿Qué cambia entre local y AWS Glue?** Solo la configuración: catálogo Iceberg, esquema de
   URI (`s3a://` vs `s3://`) y credenciales. El código del job es idéntico.

7. **¿Por qué Airflow no ejecuta la lógica de negocio?** Para no mantener dos entornos con el
   mismo código que podrían desincronizarse; cada tarea lanza el runner efímero.

8. **¿Por qué el DSN nunca viaja como argumento de un job de Glue?** Los argumentos quedan
   visibles en consola y `get-job-runs`; el job recibe el *nombre* de un parámetro SSM.

9. **¿Cómo se prueba calidad de datos sin instalar Spark?** Separando reglas puras
   (`bronze_rules.py`, `silver_rules.py`) que generan strings de SQL, testeables sin JVM.

10. **¿Por qué el state de Terraform es local?** El entorno es efímero, una sola persona: un
    backend remoto pediría un bucket y una tabla de locks para nada.

11. **¿Por qué gold corre con `dbt-spark method: session` y no con un servicio de Spark
    aparte?** Porque `method: session` no abre ninguna conexión: reutiliza la SparkSession que
    ya arma `build_spark()` en el mismo proceso, así que gold no suma ningún servicio nuevo,
    solo un `spark-submit` más.

12. **¿Por qué el job `gold_dbt` corre sobre Glue 5.0 (Spark) si dbt-athena no usa Spark para
    nada?** Porque Python shell sigue clavado en Python 3.9 y dbt-core dejó de soportar esa
    versión en la 1.11; Glue 5.0 trae Python 3.11, la misma versión que corre en local.

13. **¿Cómo conviven las 13 particiones del topic `telemetria_pozo` con solo 13 pozos?** La
    clave del mensaje es `idpozo`, así que cada pozo cae siempre en la misma partición y sus
    lecturas llegan ordenadas; con 13 claves y 13 particiones el hash no reparte una por una,
    pero eso no importa porque lo que se necesita es el orden por pozo, no un reparto parejo.

14. **¿Por qué el split para entrenar el modelo es por yacimiento (`GroupKFold`) y no aleatorio
    (`KFold`)?** Porque dos pozos del mismo yacimiento comparten la roca; con un split aleatorio
    casi todo pozo de test tiene un vecino en train y el modelo "aprueba" por ubicación, no por
    entender la completación. La diferencia medida es de 36 puntos de R² (0,737 vs 0,381).

15. **¿Por qué `predecir.py` corre para los 3.825 pozos no convencionales y no solo para los 351
    usados en el entrenamiento?** Porque el caso de uso real es predecir para un pozo recién
    fracturado que todavía no cumplió el año de producción; `prod_pet_12m_real` queda nula en
    esos y con valor en los que sí lo cumplieron, lo que permite medir con el tiempo cuánto se
    equivocó el modelo.

---

## 10. Sesión 9 — Reservas y el parser de XLSX

Esta sesión sigue `pipelines/reservas/parser.py` y `bronze_load.py`: el primer bronze del
repo que no usa Spark.

### El encabezado jerárquico: por qué no alcanza con "leer la fila 7"

El Excel de reservas no es una tabla: es un cuadro de doble entrada con cuatro niveles de
encabezado fusionados (tipo de recurso, categoría, certeza, fluido) sobre cinco columnas de
identificación (docstring, `parser.py` líneas 1-9). El parser no ubica esas filas por posición
fija, sino por vocabulario: cada nivel tiene su propio diccionario cerrado que además traduce
el rótulo crudo a un valor limpio:

```python
# pipelines/reservas/parser.py líneas 56-63
FLUIDOS = {"PET": "petroleo", "GAS": "gas"}
TIPOS_RECURSO = {"CONVENCIONAL": "convencional", "NO CONVENCIONAL": "no_convencional"}
CATEGORIAS = {"RESERVAS": "reservas", "RECURSOS CONTINGENTES": "recursos_contingentes"}
CERTEZAS = {"COMPROBADAS": "comprobadas", "PROBABLES": "probables", "POSIBLES": "posibles"}

# Los recursos contingentes no se subdividen en certeza. Se marca con un valor y no con
# vacio porque `certeza` es parte de la clave primaria, y una clave con nulos no es clave.
SIN_CERTEZA = "no_aplica"
```

`find_header_row` busca la fila que trae `OPERADOR` (da los nombres de columnas de
identificación); `find_label_rows` recorre hacia arriba desde ahí buscando, fila por fila, qué
vocabulario aparece — sin asumir una distancia fija, porque entre 2020 y 2021-2024 cambia la
cantidad de filas de título del archivo. `read_layout` arma, por cada columna que no sea de
identificación, un `ValueColumn` con los cuatro niveles ya traducidos; el bloque
`CONVENCIONAL + NO CONVENCIONAL` se descarta porque no matchea contra `TIPOS_RECURSO` — es la
suma de los otros dos bloques, un total derivable que rompería la unicidad de la clave si se
guardara.

### Rangos fusionados: propagar, pero solo hacia la derecha

Se usa **openpyxl**, que deja el valor real solo en la celda superior-izquierda de un rango
fusionado; el resto son `None`. `expand_merges` los propaga a mano:

```python
# pipelines/reservas/parser.py líneas 141-155
def expand_merges(sheet: Worksheet) -> dict[tuple[int, int], Any]:
    """Valores de los rangos fusionados propagados hacia la derecha (no hacia abajo).

    openpyxl deja el valor solo en la celda de arriba a la izquierda del rango. Se propaga
    a lo ancho porque un rotulo fusionado horizontalmente encabeza todas esas columnas;
    no se propaga a lo alto porque un rango vertical (RECURSOS CONTINGENTES ocupa la fila
    de categoria y la de certeza) significa que ese bloque no se subdivide, no que la
    subdivision se llame igual que el bloque.
    """
    valores: dict[tuple[int, int], Any] = {}
    for rango in sheet.merged_cells.ranges:
        origen = sheet.cell(row=rango.min_row, column=rango.min_col).value
        for column in range(rango.min_col, rango.max_col + 1):
            valores[(rango.min_row, column)] = origen
    return valores
```

El bucle interno solo recorre columnas de la misma fila (`rango.min_row`): nunca hay un bucle
sobre filas. `RECURSOS CONTINGENTES` es justo el caso vertical: ocupa a la vez la fila de
categoría y la de certeza, y eso significa "sin subdivisión de certeza", no "la certeza se
llama igual". Por eso, cuando `read_layout` busca la certeza de esas columnas y no encuentra
ningún valor de `CERTEZAS`, cae al `SIN_CERTEZA` de arriba.

### Del cuadro ancho a la fila larga

El Excel trae hasta 24 columnas de valor por fila (yacimiento/operador); el parser emite una
fila por **celda de valor individual**, con columnas fijas (`parser.py` líneas 29-43:
`operador, cuenca, provincia, concesion, yacimiento, hoja, tipo_recurso, categoria, certeza,
fluido, unidad, valor, anio_corte`):

```python
# pipelines/reservas/parser.py líneas 265-290 (resumido)
def parse_sheet(grid: Grid, hoja: str, anio_corte: int) -> ParseResult:
    """Filas largas de una hoja: una por celda de valor del cuadro."""
    layout = read_layout(grid)
    for row in range(layout.header_row + 1, grid.max_row + 1):
        identity = {name: text(grid.value(row, col)) for name, col in layout.identity.items()}
        if is_empty_row(identity) or is_total_row(identity):
            continue
        for value in layout.values:
            result.rows.append({
                **identity, "hoja": hoja,
                "tipo_recurso": value.tipo_recurso, "categoria": value.categoria,
                "certeza": value.certeza, "fluido": value.fluido, "unidad": value.unidad,
                "valor": text(grid.value(row, value.column)), "anio_corte": str(anio_corte),
            })
    return result
```

Una fila ancha con 16 celdas de dato reales (8 convencional + 8 no convencional, sin el bloque
derivado) se convierte en 16 filas largas. Es la forma que después se consulta con `GROUP BY
certeza` sin conocer de antemano las 24 columnas del Excel. `is_total_row` descarta filas
`TOTAL`/`TOTALES`/`TOTAL GENERAL` (la suma de la columna, no un yacimiento). Por encima están
`parse_bytes` (recorre las hojas del workbook), `parse_file` y `parse_zip`/`xlsx_from_zip`
(abren el ZIP de landing y extraen el único XLSX que contiene).

### Escritura a Iceberg con pyiceberg, sin Spark

El docstring de `bronze_load.py` explica el porqué: bronze de reservas mueve un ZIP de 400 KB,
no cientos de MB — levantar una JVM de varios GB para eso sería desperdicio. Se escribe con
**pyiceberg** contra el mismo catálogo que usa Spark (REST en local, Glue en AWS):

```python
# pipelines/reservas/bronze_load.py líneas 71-83
def bronze_schema() -> Schema:
    """Todo string salvo las dos marcas de tiempo: bronze no tipa (lo hace silver)."""
    campos = [
        NestedField(indice, nombre, StringType(), required=False)
        for indice, nombre in enumerate(LONG_COLUMNS, start=1)
    ]
    ...
    return Schema(*campos)
```

A diferencia de Spark, donde el esquema Iceberg se infiere del DataFrame, pyiceberg no tiene un
motor de ejecución detrás que asigne `field_id`: cada `NestedField` lo enumera el propio código.
El particionado usa la API de `pyiceberg.partitioning` (una partición por `_resource_id`, igual
que el bronze de Spark), y la escritura reemplaza `spark.writeTo` por:

```python
# pipelines/reservas/bronze_load.py líneas 230-253 (resumido)
def write_partition(table: Table, rows, resource_id: str, replace: bool) -> None:
    arrow = to_arrow(rows, table.schema().as_arrow())
    if replace:
        table.overwrite(arrow, overwrite_filter=EqualTo("_resource_id", resource_id))
        return
    table.append(arrow)
```

`to_arrow` convierte la lista de diccionarios Python a una tabla **pyarrow** con el esquema
Arrow derivado del esquema Iceberg. `table.append()` agrega un recurso nuevo; `table.overwrite`
con un `EqualTo` reemplaza solo la partición del recurso que cambió de sha256 — el equivalente
explícito de `overwritePartitions()` de Spark, pero sin motor distribuido: todo el parseo y el
armado de filas corre en un solo proceso Python, y pyiceberg solo escribe los Parquet y el
snapshot.

El resto del hilo de ejecución reutiliza piezas ya conocidas: `read_manifest` usa el mismo
`Manifest`/SQLAlchemy de la CLI de ingesta (no JDBC, porque este módulo sí trae el paquete
completo), `loaded_sha256` hace con `scan().to_arrow()` y `group_by` lo que Spark hace con
`groupBy().agg(F.max(...))`, y `with_lineage` agrega las mismas seis columnas de linaje que el
bronze de Spark. Después, el DAG `reservas_mensual` encadena `ingesta >> bronze >> silver`,
donde `silver` es el job de Spark de siempre — silver no sabe ni le importa que bronze se haya
escrito con pyiceberg.

### Qué correr

```powershell
podman-compose -f infra\docker\compose.yaml --profile core up -d
uv run ingest run --dataset reservas --only 2024
uv run python -m pipelines.reservas.bronze_load
uv run python scripts/check_lake.py --namespace bronze
scripts\spark-submit.ps1 pipelines/spark_jobs/silver_load.py --contract reservas
uv run pytest tests/reservas -q
```

### Qué tenés que poder explicar al terminar

- Por qué `expand_merges` propaga los rangos fusionados solo hacia la derecha y nunca hacia
  abajo, con el ejemplo de `RECURSOS CONTINGENTES`.
- Qué es la forma "larga" de una fila y por qué una sola fila del Excel se convierte en 16
  filas de bronze.
- Por qué este bronze usa pyiceberg en vez de `spark-submit`, y qué tuvo que reimplementar
  (idempotencia por sha256, linaje, particionado) para no perder ninguna garantía del bronze
  de Spark.
- Por qué `certeza` no puede quedar vacía en los recursos contingentes: el motivo de SQL
  (clave con nulos) y el motivo concreto de Spark (`count_distinct` descarta filas con algún
  componente nulo).

---

## 11. Sesión 10 — Gold con dbt

Esta es la sesión más larga de las cuatro nuevas: `pipelines/dbt/` entero.

### Qué es dbt, en los términos de este repo

**dbt** (data build tool) convierte SQL en un pipeline: cada `.sql` en `models/` es un
`SELECT` que dbt envuelve en un `CREATE TABLE AS`, resuelve en qué orden correr según qué
modelo lee a cuál, corre los tests declarados en YAML y arma un grafo de dependencias — sin
que nadie escriba el orden a mano. Bronze y silver son jobs de PySpark función por función
(sesiones 3-4); gold es una decena de modelos dimensionales con dependencias cruzadas, y el ADR
0009 es explícito: escribir eso como otro job de PySpark "significaría reimplementar a mano el
grafo de dependencias, el orden de ejecución, la documentación y los tests: exactamente lo que
dbt hace y hace bien".

### `ref()` y `source()`

`source()` apunta a una tabla que dbt no construye — acá, las tablas silver, declaradas en
`sources.yml` con su columna de linaje:

```yaml
# pipelines/dbt/models/sources.yml líneas 13-24 (resumido)
sources:
  - name: silver
    schema: silver
    loaded_at_field: _silver_loaded_at
    tables:
      - name: produccion_pozo
        description: "Producción mensual declarada por pozo (DDJJ), 2006 en adelante."
```

`ref()` apunta a otro modelo de dbt:

```sql
-- pipelines/dbt/models/hechos/fact_produccion_mensual.sql líneas 39-43
con_pozo as (
    select p.*, d.pozo_key
    from produccion p
    left join {{ ref('dim_pozo') }} d
```

`source()` marca dónde termina el territorio de dbt; `ref()` es lo que arma el grafo — dbt lee
todos los `ref()` para saber que `fact_produccion_mensual` corre después de `dim_pozo`, sin que
nadie lo declare aparte.

### `run_dbt.py` y `profiles.yml`: dbt adentro de la SparkSession

La pieza más particular de la sesión. dbt normalmente abre su propia conexión al motor; acá
corre **dentro** del mismo proceso que ya levantó Spark para bronze y silver:

```python
# pipelines/dbt/run_dbt.py líneas 1-13 (docstring)
"""Lanza dbt sobre la misma SparkSession que usan bronze y silver.

dbt-spark con `method: session` no arma la sesión: cada consulta hace
`SparkSession.builder.getOrCreate()` y usa la que encuentre. Este lanzador la crea antes con
`build_spark`, así gold escribe en el catálogo `lake` con exactamente la misma configuración
que las capas de abajo.
"""
```

```python
# pipelines/dbt/run_dbt.py líneas 41-51
def main(argv: list[str]) -> int:
    configure_environment()
    from dbt.cli.main import dbtRunner  # se importa después: dbt lee estas variables al importarse

    spark = build_spark("dbt_gold")
    try:
        result = dbtRunner().invoke(argv)
    finally:
        spark.stop()
    return 0 if result.success else 1
```

`build_spark` es la misma función de `session.py` que arma bronze y silver. El truco es
`method: session` del adaptador:

```yaml
# pipelines/dbt/profiles.yml líneas 12-17
local:
  type: spark
  method: session
  host: localhost
  schema: gold
  threads: 1  # un solo driver de Spark de 4 GB: los modelos van de a uno
```

`method: session` le dice al adaptador que no abra ninguna conexión propia: cada consulta que
dbt manda hace `getOrCreate()`, y como Python solo tiene una JVM por proceso, eso devuelve la
sesión que `run_dbt.py` ya creó. dbt nunca se entera de que existe MinIO o un catálogo REST:
escribe SQL contra `lake.gold.dim_pozo` y la sesión ya sabe traducirlo a Iceberg + S3. En AWS el
mismo `profiles.yml` define un target `aws` con `type: athena`, que no necesita ningún motor
propio: manda el SQL compilado directo a Athena, sobre las mismas tablas del Glue Data Catalog
(invocado por `pipelines/aws/gold_dbt_job.py`, sesión 6). ADR 0002 queda modificado en su mitad
local: DuckDB deja de ser el motor de gold y sigue solo para consultas exploratorias.

### Modelo por modelo

`dbt_project.yml` fija la materialización — cómo dbt persiste el `SELECT` — en `table` para
todo gold (nunca vista ni incremental: cada corrida reconstruye el modelo entero), y apaga la
carpeta `monitoreo` fuera de local (`+enabled: "{{ target.name == 'local' }}"`), porque sus
modelos leen streaming y ML, que en AWS no existen.

**`dim_pozo`** es una **SCD tipo 2** (slowly changing dimension: en vez de sobrescribir un
atributo cuando cambia, abre una fila nueva con su propia ventana de vigencia), reconstruida
de una pasada con ventanas y no con `dbt snapshot` (los 21 años de historia ya están en silver,
no llegan de a uno). CTE por CTE:

```sql
-- pipelines/dbt/models/dimensiones/dim_pozo.sql líneas 35-50 (resumido)
huella as (
    select *, concat_ws('|', coalesce(empresa,''), coalesce(tipoestado,''), ...) as atributos
    from historia
),
cambios as (
    select *, case
        when lag(atributos) over (partition by idpozo order by mes_declarado) is null then 1
        when lag(atributos) over (partition by idpozo order by mes_declarado) <> atributos then 1
        else 0
    end as abre_tramo
    from huella
),
tramos as (
    select *, sum(abre_tramo) over (
        partition by idpozo order by mes_declarado
        rows between unbounded preceding and current row
    ) as tramo
    from cambios
),
```

`historia` trae un mes-pozo por fila con los ocho atributos rastreados. `huella` los junta en
un string (`coalesce` porque en SQL `null <> 'X'` no da verdadero: sin él, un atributo que pasa
a `NULL` no se detectaría como cambio). `cambios` usa `lag()` para marcar con 1 el primer mes de
cada pozo o cualquier mes cuya huella difiera de la anterior. `tramos` es la suma acumulada de
esas aperturas — la manera estándar de numerar "rachas" sin `GROUP BY`. `resumen` colapsa cada
tramo con `min`/`max(mes_declarado)` (rango de vigencia) y `max_by(sigla, mes_declarado)` para
los atributos no rastreados. `vigencias` marca `es_vigente` en el tramo más alto de cada pozo.
El `select` final arma `pozo_key` (hash de `idpozo` + `vigente_desde`) y cierra `vigente_hasta`
en `NULL` si el tramo es el vigente, o en el último día del último mes si ya cerró.

**`fact_produccion_mensual`** cuelga cada mes del tramo de `dim_pozo` que estaba vigente en ese
mes, con un join por rango de fechas:

```sql
-- pipelines/dbt/models/hechos/fact_produccion_mensual.sql líneas 35-44
con_pozo as (
    select p.*, d.pozo_key
    from produccion p
    left join {{ ref('dim_pozo') }} d
        on p.idpozo = d.idpozo
        and p.mes_declarado >= d.vigente_desde
        and (d.vigente_hasta is null or p.mes_declarado <= d.vigente_hasta)
),
```

Un mes de producción de 2014 se cuelga del tramo que describía al pozo en 2014, no del estado
actual. El modelo se materializa completo y no incremental porque silver reescribe particiones
enteras cuando un recurso cambia de sha256 (rectificativas de años viejos incluidas): un
incremental por año nuevo perdería esas correcciones.

**El mart** (`mart_pozo_completacion_produccion.sql`) es la tabla de features que consume ML:
un pozo fracturado por fila, con el diseño de la completación de un lado y lo que produjo del
otro. `ultima_fractura` se queda con la declaración más reciente por pozo (`row_number() over
(partition by idpozo order by fecha_inicio_fractura desc, ...)`); `acumulados` suma la
producción de los primeros 3/6/12 meses usando `meses_desde_primera_produccion`, la misma
columna que arma `fact_produccion_mensual` con la macro `meses_entre()`.

### Tests: declarativos y singulares

Los declarativos van en los `.yml`, como `not_null`/`unique`/`accepted_values`/`relationships`
sobre una columna. El más ilustrativo de integridad referencial real:

```yaml
# pipelines/dbt/models/hechos/fact_produccion_mensual.yml líneas 26-33
- name: fecha_key
  data_tests:
    - not_null
    - relationships:
        arguments: { to: ref('dim_fecha'), field: fecha_key }
```

Ese `relationships` fue el que encontró tres fechas futuras mal cargadas en fractura (día y mes
invertidos en el Adjunto IV) — por eso `dim_fecha` genera su calendario hasta fin del año en
curso y no hasta el mes de hoy. Los singulares son consultas SQL sueltas en `tests/`: el test
pasa si la consulta **no devuelve filas**. El más interesante prueba una invariante de la SCD2
misma — que dos tramos del mismo pozo nunca estén vigentes al mismo tiempo (si se solaparan, el
join por rango duplicaría producción):

```sql
-- pipelines/dbt/tests/dim_pozo_vigencias_sin_solapamiento.sql líneas 15-23
select a.idpozo, a.vigente_desde as desde_a, b.vigente_desde as desde_b
from tramos a
join tramos b
    on a.idpozo = b.idpozo
    and a.vigente_desde < b.vigente_desde
    and b.vigente_desde <= a.vigente_hasta
```

Otro reconcilia gold contra silver sumando producción de 2024 en las dos capas y comparando la
diferencia contra un umbral, y otro prueba unicidad de la clave compuesta `(idpozo, fecha_key)`
de `fact_produccion_mensual` con `group by ... having count(*) > 1` — la unicidad de una clave
compuesta no entra en un `data_tests` de columna, por eso necesita un singular.

El `.yml` de `fact_reservas` es, además, la evidencia documental de `certeza = no_aplica` de la
sesión 9: `accepted_values` la lista junto con `comprobadas`/`probables`/`posibles`, con la
descripción "'no_aplica' en los recursos contingentes, que la planilla no subdivide".

### Macros de dialecto para Athena

Athena corre sobre Trino, que comparte casi todo el SQL con Spark salvo media docena de
funciones. `macros/dialecto.sql` las resuelve con `adapter.dispatch` (dbt elige qué
implementación usar según el adaptador activo):

```sql
-- pipelines/dbt/macros/dialecto.sql líneas 14-26
{% macro md5(expresion) -%}
    {{ return(adapter.dispatch('md5', 'gold')(expresion)) }}
{%- endmacro %}

{% macro default__md5(expresion) -%}
    md5({{ expresion }})
{%- endmacro %}

{% macro athena__md5(expresion) -%}
    {# En Trino md5 toma y devuelve varbinary; el cast es porque no toda clave es texto. #}
    lower(to_hex(md5(to_utf8(cast({{ expresion }} as varchar)))))
{%- endmacro %}
```

Y `serie_de_meses` (la que arma el calendario de `dim_fecha`): en Spark es `explode(sequence(...))`
en el `SELECT`; en Trino el equivalente es `unnest`, que va en el `FROM` y no en el `SELECT`, con
el intervalo entre comillas. Cada macro se llama igual que la función de Spark, así el SQL de
los modelos no cambia según el destino — ningún modelo llama `adapter.dispatch` directamente.

### `dbt source freshness`

`sources.yml` declara, por fuente, `loaded_at_field` y dos umbrales de frescura — un chequeo de
**actualidad**, no de estructura: silver puede estar perfectamente tipada y vacía de novedades
porque el portal dejó de publicar, y eso el contrato de silver no lo detecta:

```yaml
# pipelines/dbt/models/sources.yml líneas 23-29
- name: produccion_pozo
  freshness:
    warn_after: {count: 35, period: day}
    error_after: {count: 45, period: day}
```

35/45 días para lo mensual, 2/5 para `fractura` (se republica todos los días), 400/460 para
`reservas` (publicación anual). Las fuentes sin cadencia real (streaming, `dq_runs`) se declaran
igual pero sin bloque `freshness`: nunca se chequean. Se retoma en la sesión 12, con el DAG que
corre `dbt source freshness`.

### Qué correr

```powershell
podman-compose -f infra\docker\compose.yaml --profile core up -d
scripts\dbt.ps1 build
scripts\dbt.ps1 test --select fact_produccion_mensual
scripts\dbt.ps1 source freshness
uv run python scripts/check_lake.py --namespace gold
```

### Qué tenés que poder explicar al terminar

- Qué significa `method: session` en `profiles.yml` y por qué eso le permite a dbt escribir en
  el mismo catálogo Iceberg que bronze y silver sin abrir ninguna conexión propia.
- Cómo arma `dim_pozo` sus tramos de vigencia sin usar `dbt snapshot`, y por qué acá esa
  alternativa no aplica.
- Qué prueba el join por rango de `fact_produccion_mensual` contra `dim_pozo`, y qué test
  evitaría que dos tramos del mismo pozo se solapen.
- La diferencia entre un test declarativo y uno singular, con un ejemplo de cada uno de este
  proyecto.

---

## 12. Sesión 11 — Streaming

Esta sesión sigue `pipelines/streaming/replay_3w.py` (productor) y
`pipelines/streaming/consume_telemetria.py` (consumidor).

### Kafka en dos frases

El repo levanta un único broker (`apache/kafka:4.1.2`, modo KRaft: broker y controller en el
mismo proceso, sin Zookeeper). El **topic** real es `telemetria_pozo`, con 13 **particiones**
("los 13 equipos concurrentes que reporta el RTIC", `compose.yaml` líneas 272-274). Cada
mensaje lleva como clave el `idpozo`, así que todas las lecturas de un mismo pozo caen siempre
en la misma partición y llegan ordenadas — con 13 claves y 13 particiones el hash no reparte
una por una, pero lo que importa es el orden por pozo, no el reparto parejo. El **offset** es
la posición de un mensaje dentro de una partición: `consume_telemetria.py` fija
`startingOffsets: "earliest"`, que solo aplica cuando no hay checkpoint. Structured Streaming
no configura un `group.id` explícito; cada query tiene su propio checkpoint en
`s3a://lakehouse/checkpoints/<query>/`, que cumple el rol de un **consumer group**: recordar
hasta dónde leyó cada consumidor independiente.

### El productor: intercalado, tardíos, idempotencia

**Intercalado**: la fuente de eventos no procesa un archivo de 3W a la vez. `main()` arma un
único flujo con `heapq.merge(*[leer(...) for archivo in archivos], key=lambda l: l[0])`: cada
archivo (un pozo) entrega lecturas ordenadas por su offset relativo al inicio, y `heapq.merge`
los intercala por ese offset — los N pozos "avanzan juntos como si midieran al mismo tiempo".

**Tardíos**: `PlanTardios` (`eventos.py`) modela el corte del enlace satelital: una fracción de
eventos (`--late-fraction`, 5% por defecto) se retiene y se emite con demora, entre
`--late-min` (30 s) y `--late-max` (120 s) de tiempo de evento:

```python
# pipelines/streaming/eventos.py líneas 144-152
def demora_tardia(rng: random.Random, plan: PlanTardios) -> float | None:
    """Segundos que se retiene esta lectura, o None si sale al instante."""
    if plan.fraccion <= 0 or rng.random() >= plan.fraccion:
        return None
    return rng.uniform(plan.minimo_s, plan.maximo_s)
```

El evento retenido va a un heap `pendientes` y se publica más tarde, cuando el offset de emisión
ya superó su vencimiento — así llega después de eventos con tiempo de evento mayor, que es
justo lo que tiene que tolerar el watermark. Existe únicamente para poder probarlo: sin datos
tardíos, `withWatermark` nunca tendría nada que descartar ni tolerar.

**Idempotencia**, a nivel del protocolo Kafka (no la del sha256 de la ingesta):

```python
# pipelines/streaming/replay_3w.py líneas 238-243
# enable.idempotence: sin esto, un reintento de librdkafka sobre un mensaje que el broker
# ya escribio deja el evento duplicado en el topic. Con idempotencia el broker descarta el
# reenvio, que es lo que hace que "eventos publicados == filas en bronze" se sostenga.
producer = Producer({"bootstrap.servers": servidores, "linger.ms": 50, "enable.idempotence": True})
```

El broker asigna una secuencia por productor/partición y descarta un reenvío duplicado si el
productor tuvo que reintentar tras un timeout.

### El consumidor: `readStream`, watermark, ventanas, checkpoint

```python
# pipelines/streaming/consume_telemetria.py líneas 88-104 (resumido)
def leer_eventos(spark: SparkSession, config: LakehouseConfig) -> DataFrame:
    crudo = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_EVENTOS_POR_BATCH)
        .load()
    )
    return crudo.select(F.from_json(F.col("value").cast("string"), esquema_evento()).alias("evento")).select("evento.*")
```

`maxOffsetsPerTrigger` (40.000) acota cuántos eventos entran por micro-batch: 13 pozos a 60x son
~16.000 eventos por trigger de 20 s, el doble deja margen para recuperar atraso sin volver un
solo batch enorme.

```python
# pipelines/streaming/consume_telemetria.py líneas 46-47, 118-121
WATERMARK = "2 minutes"
VENTANA = "1 minute"
...
eventos.withWatermark("event_time", WATERMARK).groupBy(F.window("event_time", VENTANA), "idpozo").agg(*agregaciones)
```

El **watermark** es sobre `event_time` (tiempo de evento, no de llegada), con umbral de 2
minutos: Spark recuerda el máximo `event_time` visto y considera cerrada una ventana cuando ese
máximo ya superó el fin de la ventana más el umbral; un evento más viejo que eso se descarta de
la agregación. El valor sale del ADR 0011: "es el orden de magnitud de un corte de enlace
satelital: cubre la reconexión típica sin obligar a Spark a mantener estado de las últimas
horas". Las **ventanas** son de 1 minuto, tumbling (sin slide), agrupadas también por `idpozo`,
con conteo y `avg`/`min`/`max` de los sensores clave más `max(class)` (si hubo algún evento
anómalo etiquetado en el minuto).

El **checkpoint** guarda, por query, los offsets de Kafka ya procesados y el estado de la
agregación, en su propia carpeta de S3/MinIO (`checkpoints/<nombre>/`); al reiniciar, Spark
retoma exactamente donde había quedado, sin reprocesar lo ya escrito.

Las **dos queries**: `telemetria_bronze` escribe todos los eventos crudos a
`lake.bronze.telemetria_pozo` sin filtrar nada (un evento tardío queda igual en bronze);
`telemetria_silver_1min` escribe la agregación por ventana a
`lake.silver.telemetria_pozo_1min`. Ambas leen del mismo topic con `leer_eventos()` invocado dos
veces — dos lecturas independientes, no una compartida.

### El problema del catálogo SQLite

```python
# pipelines/streaming/consume_telemetria.py líneas 193-198
# El catalogo local es SQLite y acepta un solo escritor (ADR 0003): si los dos commits se
# pisan, el que pierde recibe un SQLITE_BUSY que ni el busy_timeout puede reintentar, el
# catalogo devuelve 500 y la query muere. Dos medidas para que no coincidan:
#   - arrancar silver despues, porque el batch caro es el primero (el que recupera atraso);
#   - un segundo mas de trigger, porque Spark alinea los micro-batches a multiplos
#     absolutos del intervalo y con el mismo numero coincidirian siempre.
```

El catálogo REST local usa SQLite (ADR 0003), que acepta un único escritor. Con dos queries de
streaming commiteando en paralelo, si dos commits coinciden, el que pierde recibe
`SQLITE_BUSY`, el catálogo responde 500 e Iceberg lo traduce a una excepción que mata la query
sin poder reintentar. Cuatro medidas: arrancar `silver` medio trigger después que `bronze`;
triggers desincronizados (`bronze` cada N segundos, `silver` cada N+1); `maxOffsetsPerTrigger`
acota el tamaño del primer batch tras un reinicio; y `busy_timeout=30000` en la URI JDBC del
catálogo. Aun así, cuando se pisan, la conexión que pierde queda con la transacción abierta y
el catálogo deja de aceptar escrituras hasta reiniciar el contenedor — lo primero que hay que
mirar si un job empieza a fallar con 500 sin motivo. En AWS el catálogo es Glue y el problema no
existe.

### El experimento de reinicio

Documentado con números reales en `docs/fuentes/telemetria_3w.md`. Una corrida de 10 minutos
publicó 468.001 eventos, con 468.001 filas exactas en bronze y 0 descartados por watermark (la
tolerancia real, `watermark + trigger × speed` ≈ 22 minutos de tiempo de evento a 60x, cubre los
cortes simulados de 30-120 s). Una segunda corrida, con cortes largos (30-60 minutos de tiempo
de evento) y el consumidor **matado con `podman kill` a mitad de camino** y vuelto a levantar,
sumó 234.001 eventos más: bronze acumuló exactamente 702.002 filas entre las dos corridas, sin
ningún duplicado, y 4.588 eventos quedaron fuera de la agregación de silver por el watermark —
pero enteros en bronze, que es justamente para lo que sirve tener el crudo. El checkpoint
retoma el offset donde estaba y no reprocesa lo ya escrito, ni siquiera tras un `kill` a mitad
de un micro-batch.

### Qué correr

```powershell
uv run python -m pipelines.streaming.fetch_3w --classes 0,2,7
uv run python -m pipelines.streaming.pozo_map
scripts\streaming-up.ps1
scripts\streaming-demo.ps1 -Segundos 600 -Velocidad 60
uv run python scripts/check_lake.py --namespace silver --table telemetria_pozo_1min
```

### Qué tenés que poder explicar al terminar

- Por qué el productor rebasea el `event_time` en vez de mandar el timestamp original de cada
  archivo de 3W, y qué pasaría con el watermark si no lo hiciera.
- Cómo se calcula la tolerancia real a un evento tardío (`watermark + trigger × speed`) y por
  qué un corte de 30-120 s no descarta nada a 60x pero uno de 30-60 minutos sí.
- Qué significa que el catálogo SQLite acepte un solo escritor, y qué medidas toma el código
  para que las dos queries no colisionen.
- Por qué matar el consumidor con `podman kill` y volver a levantarlo no duplicó ni perdió
  filas en bronze.

---

## 13. Sesión 12 — ML y monitoreo

Esta sesión sigue `pipelines/ml/{datos,entrenar,predecir}.py`, el DAG `ml_mensual`, el DAG
`monitoreo_diario` y `alertas.py`. Los modelos de dbt `salud_pipeline`/`calidad_por_corrida` ya
se vieron en la sesión 10; acá se retoma solo `dbt source freshness` desde el lado del DAG.

### `datos.py`: filtros, capping, target en log

De 4.635 pozos fracturados en el mart, tres filtros dejan 351 para entrenar: solo `NO
CONVENCIONAL` (en el convencional la fractura es una intervención sobre un pozo que ya
producía, otra relación causal) y solo los que declararon los 12 meses completos (un pozo
truncado enseñaría que el diseño produce poco cuando en realidad falta historia). **Capping**:
dos columnas se recortan a un tope físico, sin descartar el pozo entero:

```python
# pipelines/ml/datos.py líneas 64-70
TOPES = {"presion_maxima_psi": 20_000.0, "potencia_equipos_fractura_hp": 100_000.0}

def aplicar_topes(pozos: pd.DataFrame) -> pd.DataFrame:
    """Recorta presión y potencia en sus máximos físicos (docs/fuentes/fractura.md)."""
    recortado = pozos.copy()
    for columna, tope in TOPES.items():
        recortado[columna] = recortado[columna].astype(float).clip(upper=tope)
    return recortado
```

El contrato de silver ya manda a cuarentena lo que supera esos rangos, pero el mart puede traer
valores cargados antes de esa regla (hasta 209.640 psi en algunas filas, errores de unidad, no
pozos monstruosos). El **target** (`prod_pet_12m`) se modela en `log1p`:

```python
# pipelines/ml/datos.py líneas 22-25
# El objetivo es el acumulado de petroleo de los primeros 12 meses. Se modela en log1p porque
# la distribucion va de 0 a 155.000 m3: en escala original el error de los pozos grandes se
# come al de todos los demas.
```

`log1p` (no `log`) porque el target puede ser 0 (114 de 351 pozos acumularon cero). Al volver a
escala original, `a_escala_original` acota con un `TECHO_M3` de cordura: un modelo que se
desmadre en escala log daría un `expm1` infinito. Tres *features* derivadas (arena por metro,
agua por etapa, etapas por metro) suben el R² fuera de muestra de 0,304 a 0,337.

### `entrenar.py`: `GroupKFold`, baselines, HGB, SHAP, MLflow

```python
# pipelines/ml/entrenar.py líneas 1-14 (docstring, resumido)
# Como se valida: GroupKFold sobre el yacimiento. Dos pozos del mismo yacimiento comparten la
# roca; si uno queda en train y su vecino en test, el modelo aprueba por saber donde esta el
# pozo y no por entender la completacion. Con split aleatorio el R2 sube de 0,38 a 0,74: esa
# diferencia es exactamente la fuga que el split por grupo evita medir de mas.
```

**`GroupKFold`** garantiza que todas las filas de un mismo grupo (`areayacimiento`, no el pozo)
caigan del mismo lado del split. Se compara contra dos **baselines** honestos: la mediana del
train (`DummyRegressor`) y una regresión lineal sobre las mismas features (que da un R²
catastrófico, −11,59 en log, por extrapolar fuera de rango). El modelo real es
**`HistGradientBoostingRegressor`** (boosting de árboles de scikit-learn), con categóricas
codificadas por `OrdinalEncoder` (un entero por categoría, no one-hot: un árbol parte por
umbral y no necesita la expansión) y una grilla de seis combinaciones elegida a mano, no por
`GridSearchCV` ("con 351 pozos una grilla grande solo sirve para sobreajustar la validación").

**SHAP** (SHapley Additive exPlanations: valores de teoría de juegos que reparten la predicción
entre las features que la explican) se calcula exacto para árboles con `shap.TreeExplainer`, sin
muestreo, y se guarda como CSV de importancia media y un `summary_plot` en PNG.

**MLflow** trackea todo dentro de un `with mlflow.start_run()`: parámetros (cantidad de pozos,
yacimientos, folds), métricas de cada baseline y del HGB, y artefactos (CSVs, SHAP, el PNG, y el
`Pipeline` completo de scikit-learn con `mlflow.sklearn.log_model`). El **alias de modelo** — la
forma en que MLflow 3 reemplaza los viejos *stages* (`Staging`/`Production`) por una etiqueta de
texto libre — se mueve solo si el modelo superó al baseline:

```python
# pipelines/ml/entrenar.py líneas 266-279 (resumido)
def registrar_champion(uri_modelo: str, mejora: bool) -> str | None:
    version = mlflow.register_model(uri_modelo, registro.MODELO).version
    if not mejora:
        logger.warning("el modelo no supera al baseline: se registra v%s sin alias", version)
        return version
    mlflow.MlflowClient().set_registered_model_alias(registro.MODELO, registro.ALIAS, version)
    return version
```

```python
# pipelines/ml/registro.py líneas 13-17, 34-36
MODELO = "completacion_produccion_12m"
# El alias reemplaza a los stages, que MLflow 3 ya no usa: `models:/<modelo>@champion` siempre
# apunta a la ultima version que supero al baseline.
ALIAS = "champion"
```

Toda versión entrenada queda registrada, pero solo `champion` sirve para inferencia: un modelo
peor jamás llega a producción por el solo hecho de haberse entrenado, y `entrenar.py` devuelve
código 1 si no hubo mejora, lo que hace fallar la tarea y evita que `predecir` corra con un
modelo peor.

### `predecir.py`: inferencia batch a Iceberg

El modelo se carga siempre por alias, nunca por versión: `mlflow.sklearn.load_model(registro.uri_champion())`
resuelve a `models:/completacion_produccion_12m@champion`. Corre sobre los 3.825 pozos no
convencionales (no solo los 351 de entrenamiento), porque el caso de uso real es el pozo que
todavía no cumplió el año — `prod_pet_12m_real` queda nulo en esos y con valor en los que sí,
lo que permite medir la deriva del modelo con el tiempo. La escritura usa pyiceberg, igual que
reservas:

```python
# pipelines/ml/predecir.py líneas 87-105 (resumido)
def escribir(tabla: Table, filas: pd.DataFrame) -> None:
    """Reemplaza el contenido completo de la tabla por la corrida actual."""
    arrow = pa.Table.from_pandas(filas, schema=tabla.schema().as_arrow(), preserve_index=False)
    if tabla.current_snapshot() is None:
        tabla.append(arrow)
        return
    tabla.overwrite(arrow)
```

`tabla.overwrite(arrow)` reemplaza el contenido entero en un solo snapshot atómico: nunca hay
un momento en que la tabla se lea vacía.

### El DAG `ml_mensual`

```python
# orchestration/dags/ml_mensual.py líneas 30-49 (resumido)
MLFLOW = "MLFLOW_TRACKING_URI=http://mlflow:5000"
...
entrenar = runner_task("entrenar", f"{MLFLOW} python3 -m pipelines.ml.entrenar")
predecir = runner_task("predecir", f"{MLFLOW} python3 -m pipelines.ml.predecir")
entrenar >> predecir
```

Corre el día 2 a las 7, veinticinco horas después de `gold_mensual`. `MLFLOW_TRACKING_URI` no
está en `FORWARDED_ENV` (la lista compartida por todos los DAGs); en vez de tocar esa lista para
un solo DAG, la variable se antepone al comando (`VAR=valor comando`, válido en el `bash -c` con
el que arranca el runner). `python3` pelado y no `spark-submit`: 351 pozos en pandas no
necesitan una JVM, el runner se usa solo porque ya trae el volumen con las dependencias.

### `dbt source freshness` y el DAG `monitoreo_diario`

```python
# orchestration/dags/monitoreo_diario.py líneas 43-52 (resumido)
salud = runner_task("salud", f"{SPARK_SUBMIT} /app/pipelines/dbt/run_dbt.py build --select monitoreo")
frescura = runner_task("frescura", f"{SPARK_SUBMIT} /app/pipelines/dbt/run_dbt.py source freshness")
salud >> frescura
```

`salud` reconstruye `gold.salud_pipeline`/`gold.calidad_por_corrida` (sesión 10) y corre primero
a propósito: si `frescura` fuera primero y fallara, la tarea que deja la foto del pipeline
quedaría en `skipped` justo el día que hace falta mirarla. `frescura` corre `dbt source
freshness` y termina con código distinto de cero si alguna fuente pasó su `error_after` — es la
tarea que hace fallar el DAG. Con `retries: 0`: si una fuente está vieja, volver a preguntar da
lo mismo.

### `alertas.py`: el único punto de aviso del repo

```python
# orchestration/dags/alertas.py (completo, resumido)
def avisar_falla(context) -> None:
    """Deja en el log qué falló y el link para ir a verlo."""
    tarea = context["task_instance"]
    corrida = context["dag_run"]
    url = f"{BASE_URL}/dags/{tarea.dag_id}/runs/{corrida.run_id}/tasks/{tarea.task_id}"
    logger.error("ALERTA | dag=%s | tarea=%s | corrida=%s | intento=%s | %s",
                 tarea.dag_id, tarea.task_id, corrida.run_id, tarea.try_number, url)
```

Se engancha una sola vez, en el `default_args` de cada DAG (`on_failure_callback=avisar_falla`),
así cualquier tarea que falle —ingesta, bronze, silver, dbt, ML— pasa por acá. Hoy solo deja una
línea de log con el link directo a la tarea fallada en la UI de Airflow; es un punto de
extensión único: el día que haya un canal real (correo, Slack, PagerDuty), se conecta acá y en
ningún otro lado.

### Qué correr

```powershell
podman-compose -f infra\docker\compose.yaml --profile core --profile mlflow up -d
uv run python -m pipelines.ml.entrenar
uv run python -m pipelines.ml.predecir --dry-run
cd pipelines\dbt
uv run python run_dbt.py build --select monitoreo
uv run python run_dbt.py source freshness
```

### Qué tenés que poder explicar al terminar

- Por qué `GroupKFold` agrupa por `areayacimiento` y no por pozo, y qué pasa con el R² con un
  split aleatorio.
- Qué es un alias de modelo en el registry de MLflow, y por qué `predecir.py` nunca recibe un
  número de versión como parámetro.
- Por qué el target se modela en `log1p` y qué rol cumple `TECHO_M3` al volver a la escala
  original.
- La diferencia entre `warn_after` y `error_after` en `dbt source freshness`.
- Por qué `monitoreo_diario` corre `salud` antes que `frescura` aunque `frescura` sea la tarea
  que puede fallar el DAG.

---

## 14. Glosario

- **SparkSession**: punto de entrada único a Spark; arranca la JVM y expone la API para leer,
  transformar y escribir datos.
- **Catálogo (Iceberg)**: registro de qué namespaces y tablas existen, sus archivos, esquema
  e historial de snapshots.
- **Namespace**: el equivalente Iceberg de un esquema SQL (`lake.bronze`, `lake.silver`).
- **Snapshot**: versión inmutable de una tabla Iceberg en un momento dado; cada escritura crea
  uno nuevo.
- **Partición**: subconjunto de una tabla separado por el valor de una columna
  (`_resource_id`, `anio`).
- **`overwritePartitions()`**: reemplaza solo las particiones que trae el DataFrame nuevo.
- **Linaje**: metadatos que rastrean una fila hasta su origen (archivo, hash, fecha).
- **Idempotencia**: una operación da el mismo resultado si se repite; correr un job dos veces
  no duplica nada.
- **Streaming**: procesar datos en bloques a medida que llegan, sin tenerlos todos en memoria.
- **Multipart upload**: subida de un objeto S3 en partes independientes que se ensamblan al
  final; permite reintentar una parte sin perder las demás.
- **sha256**: hash criptográfico; contenidos idénticos producen el mismo hash, usable como
  identidad del contenido.
- **Backoff exponencial**: la espera entre reintentos crece (1s, 2s, 4s...) para no saturar un
  servicio que ya falla.
- **DSN**: cadena de conexión a una base de datos.
- **JDBC**: protocolo Java para bases relacionales; Spark lo usa para leer Postgres sin
  librerías Python.
- **Wheel (`.whl`)**: formato empaquetado estándar de Python; en el fondo, un zip.
- **DAG**: grafo acíclico dirigido; en Airflow, tareas y sus dependencias.
- **Operador (Airflow)**: clase que ejecuta un tipo de tarea (`DockerOperator` lanza un
  contenedor).
- **Catchup**: si Airflow corre retroactivamente las corridas "perdidas" desde `start_date`.
- **Perfil (Compose)**: etiqueta que agrupa servicios; sin pedirlo, el servicio no se levanta.
- **Contenedor efímero**: se lanza para una tarea puntual y se destruye al terminar.
- **DPU**: Data Processing Unit, unidad de cómputo con la que Glue cobra sus jobs.
- **Glue Data Catalog**: catálogo de metadatos administrado por AWS; actúa como catálogo
  Iceberg.
- **State machine**: en Step Functions, la definición de un flujo de pasos y transiciones.
- **`.sync`**: sufijo que hace esperar a que la operación asíncrona (un job de Glue) termine.
- **SSM Parameter Store**: guarda configuración y secretos; un `SecureString` se cifra con KMS.
- **IAM Role**: identidad que un servicio "asume" para obtener permisos, sin credenciales fijas.
- **Terraform state**: archivo que registra qué recursos reales corresponden a la configuración.
- **Moto**: simula servicios de AWS en memoria para tests, interceptando boto3.
- **`responses`**: simula respuestas HTTP para tests, interceptando `requests`.
- **Ventana (Window function)**: cálculo SQL "por grupo" sin colapsar filas, a diferencia de
  `GROUP BY`.
- **Cuarentena**: tabla donde se guardan las filas rechazadas por un check blando, con motivo.
- **dbt model**: un `.sql` en `models/` que dbt convierte en una tabla o vista; el `SELECT` es
  todo lo que hay que escribir, dbt resuelve el orden y la materialización.
- **`ref()`**: función de dbt que apunta a otro modelo; arma el grafo de dependencias sin que
  nadie declare el orden a mano.
- **`source()`**: función de dbt que apunta a una tabla que dbt no construye (acá, silver);
  marca dónde termina el territorio de dbt.
- **Macro (dbt)**: función reutilizable en Jinja/SQL; en este repo, además, el mecanismo con el
  que se resuelven diferencias de dialecto entre Spark y Athena (`adapter.dispatch`).
- **Materialización**: cómo dbt persiste un modelo (`table`, `view`, incremental); acá, siempre
  `table`.
- **SCD tipo 2 (slowly changing dimension)**: dimensión que guarda historia abriendo una fila
  nueva con su propia ventana de vigencia en vez de sobrescribir el atributo que cambió.
- **openpyxl**: librería Python para leer/escribir XLSX; solo la celda superior-izquierda de un
  rango fusionado trae valor, el resto hay que propagarlo a mano.
- **pyiceberg**: implementación pura Python del formato de tabla Iceberg, sin JVM; expone
  `load_catalog`, `table.append`, `table.overwrite`, `table.scan()`.
- **Topic (Kafka)**: el canal de mensajes con nombre al que un productor escribe y del que un
  consumidor lee.
- **Partición (Kafka)**: subdivisión de un topic; los mensajes con la misma clave siempre caen
  en la misma partición y llegan ordenados entre sí.
- **Offset**: posición de un mensaje dentro de una partición de Kafka.
- **Consumer group**: identidad de un consumidor independiente, usada para recordar hasta dónde
  leyó; en Structured Streaming, ese rol lo cumple el checkpoint de cada query.
- **Watermark**: el máximo tiempo de evento visto menos un umbral; un evento más viejo que eso
  se descarta de una agregación en streaming.
- **Checkpoint (Structured Streaming)**: carpeta donde Spark guarda los offsets procesados y el
  estado de una query, para retomarla exacta tras un reinicio.
- **`GroupKFold`**: variante de validación cruzada que obliga a que todas las filas de un mismo
  grupo caigan del mismo lado del split, para no filtrar información entre train y test.
- **SHAP**: valores de teoría de juegos que reparten una predicción entre las features que la
  explican; `TreeExplainer` los calcula exacto para modelos de árboles.
- **Alias de modelo (MLflow)**: etiqueta de texto libre sobre una versión registrada de un
  modelo (reemplaza a los viejos *stages*); un cliente pide `models:/<nombre>@<alias>`.
- **Freshness (dbt source freshness)**: chequeo de actualidad de una fuente: compara su última
  carga contra dos umbrales (`warn_after`, `error_after`), a diferencia de un contrato de datos,
  que chequea estructura y valores, no antigüedad.
