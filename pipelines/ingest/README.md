# Ingesta a landing

Baja las fuentes públicas del upstream argentino a `s3://landing` en streaming y registra
cada intento en la tabla `ingestion_manifest` de Postgres. No escribe nada en disco local.

## Uso

```bash
uv run ingest datasets                                   # fuentes del registro
uv run ingest list --dataset produccion_pozo             # recursos y si están al día
uv run ingest run  --dataset produccion_pozo --only 2024 # ingesta filtrando por nombre
uv run ingest run  --dataset fractura --dry-run          # solo muestra qué haría
uv run ingest manifest --dataset produccion_pozo -n 20   # últimas filas del manifiesto
```

Configuración: `config/local.env` (o la ruta en `LAKEHOUSE_ENV_FILE`). Cualquier variable de
entorno pisa el archivo, que es como corre dentro de los contenedores (hostnames internos).
`run` devuelve código de salida 1 si algún recurso falló; el resto de la corrida sigue igual.

Airflow la corre igual pero dentro del runner de Spark (ADR 0006):
`python3 -m pipelines.ingest.cli run --dataset produccion_pozo`. Ese contenedor trae Python
3.10, así que el paquete no puede usar sintaxis ni stdlib de 3.11 (ver `_now` en `manifest.py`).

## Decisiones

- **Registro declarativo** (`datasets.yaml`): agregar una fuente no requiere tocar código.
  Además de `include`/`exclude` (regex sobre el nombre) hay `formats`, porque el recurso
  "Capítulo IV - Pozos" existe con el mismo nombre en CSV y en SHP.
- **Familia DDJJ**: de las dos familias por año se ingesta la de "DDJJ abiertas y cerradas".
  La deduplicación es por `resource_id`, no por nombre: el portal repite 2024 con dos ids.
- **HTTP plano**: `https://datos.energia.gob.ar` redirige 301 a `http`. `force_http` baja el
  esquema para los hosts `*.energia.gob.ar` y así se evita el redirect en cada descarga.
- **Idempotencia en dos niveles**: si `size` y `last_modified` de origen coinciden con la
  última corrida `ok`, se registra `unchanged` sin descargar. Si cambiaron, se descarga y se
  compara el sha256: contenido igual también es `unchanged`. Solo contenido nuevo es `ok`.
- **Streaming con multipart de 8 MB**: el sha256 se calcula sobre los mismos bytes que se
  suben, sin buffer completo en RAM ni archivo temporal (307 MB en ~21 s contra MinIO local).
- **Fila pesimista**: `start()` inserta en estado `failed` y el cierre la promueve. Un proceso
  que muere a mitad de camino deja la evidencia en el manifiesto en vez de perderla.
- **`reservas` fuera de CKAN**: ZIP anual por URL. Como no hay id de portal, el `resource_id`
  es un hash corto y estable de la URL. Verificado por HEAD: solo 2020-2024 están publicados.
