# Recorrido del código: ypf-data-platform

Este documento es un recorrido guiado por el código real del repositorio, pensado para
alguien que sabe Python y SQL pero no vio Spark, Iceberg, Airflow ni AWS en profundidad.
No es un resumen de los READMEs: cada afirmación sale de un archivo concreto, y cuando se
cita código son las líneas reales del repo en la fecha de este recorrido (2026-09-05).

Nota de alcance: mientras se escribía este documento había otro trabajo en curso sobre
`pipelines/reservas/` (hoy un módulo con un solo docstring, sin lógica) y sobre
`pipelines/contracts/fractura.yaml`. Se mencionan donde corresponde porque ya están enganchados
en `datasets.yaml`, `bronze_tables.yaml` y el DAG `fractura_diaria`, pero su contenido de
negocio queda para una segunda edición: acá se explica solo la mecánica genérica, no las reglas
específicas de fractura ni de reservas.

---

## 1. Cómo usar este recorrido

Son 8 sesiones de 45 a 60 minutos. Cada una asume que hiciste la anterior. La idea no es leer
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
| 8 | Las decisiones | `docs/adr/0001` a `0008` |

Cada sesión trae tres cosas: **qué leer** (los archivos, en orden), **qué correr** (comandos
reales del repo) y **qué tenés que poder explicar al terminar** (preguntas para responder sin
volver a mirar el código).

Prerequisito para las sesiones 3 a 6: Postgres y MinIO corriendo (perfil `core` del compose) y,
para 3-5, además el perfil `spark` o `airflow`. La sesión 2 corre sola con `uv sync` si tenés
Postgres y credenciales de S3/MinIO; si no, se puede leer sin ejecutar nada.

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
    produccion_pozo.yaml, pozo_primera_produccion.yaml, fractura.yaml (en curso)
  aws/              wrappers finos para Glue (sesión 6): ingest_job.py, bronze_job.py,
                    silver_job.py, ssm.py
  reservas/         stub sin lógica (segunda edición)
  ml/, streaming/   carpetas reservadas, vacías por ahora
orchestration/dags/
  runner.py         arma el DockerOperator que lanza el runner de Spark
  produccion_pozo_mensual.py, fractura_diaria.py, reservas_mensual.py
infra/
  docker/  compose.yaml, spark-defaults.conf, postgres/init.sql, .env.example
  terraform/  s3.tf, iam.tf, glue.tf, stepfunctions.tf, athena.tf, variables.tf,
              outputs.tf, versions.tf
scripts/  spark-submit.ps1/.sh, aws_deploy.ps1/.sh, aws_logs.ps1, check_dags.py, check_lake.py
tests/    ingest/, spark_jobs/
docs/     adr/0001 a 0008, semana-0-derisking.md, fuentes/, aprendizaje/
config/   local.env — única fuente de configuración para correr todo en local
```

### El flujo landing → bronze → silver

Todo el proyecto es una cadena de tres capas (un patrón que se conoce como **arquitectura
medallion**):

```
fuentes públicas (CKAN, ZIP por HTTP)
        │  pipelines/ingest  (streaming, sha256, manifiesto)
        ▼
   landing (S3/MinIO)          CSV crudos, tal como los publica la fuente
        │  pipelines/spark_jobs/bronze_load.py
        ▼
   bronze (Iceberg)            mismos datos, todo string, con columnas de linaje
        │  pipelines/spark_jobs/silver_load.py + contrato YAML
        ▼
   silver (Iceberg)            tipado, deduplicado, con cuarentena de filas rechazadas
```

Landing no es una tabla: son objetos en un bucket S3, organizados por key
(`{prefijo}/resource_id=.../ingest_date=.../archivo`). Bronze y silver sí son tablas Iceberg —
un formato de tabla, no una base de datos, que agrega sobre Parquet un catálogo de esquemas,
particiones y snapshots. El porqué de cada paso se ve en las sesiones 2 a 4.

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
# infra/docker/compose.yaml líneas 3-9
# Perfiles:
#   core      -> MinIO (S3), Postgres (metadata), catálogo Iceberg REST      [siempre]
#   spark     -> runner efímero de Spark (ADR 0004), no es un servicio        [etapa 1]
#   airflow   -> orquestador; lanza el runner por tarea (ADR 0006)            [etapa 1]
#   streaming -> Kafka + productor de replay 3W                               [etapa 2]
```

Un **perfil** de Docker Compose es una etiqueta que agrupa servicios: si no se pide el perfil,
el servicio no se levanta. Esto permite tener un solo archivo de compose para todo el
proyecto sin que levantar el stack básico traiga también Airflow o Kafka.

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
no hace nada gracias a `pending_resources`). `reservas_mensual` solo tiene la tarea de ingesta
— todavía no hay tabla bronze para los ZIP (recordá que `pipelines/reservas/` sigue en
desarrollo, sin lógica de descompresión).

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

### Los ocho ADR, en una tabla

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

### Diez preguntas de entrevista sobre este proyecto

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

---

## 10. Glosario

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
