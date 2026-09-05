# Contratos de datos

Un contrato por tabla silver, en YAML versionado. Lo lee una persona para saber qué hay en
la tabla y lo lee `pipelines/spark_jobs/silver_load.py` para castear y validar: no hay dos
definiciones que puedan desincronizarse (ADR 0005).

```powershell
scripts\spark-submit.ps1 pipelines/spark_jobs/silver_load.py --contract produccion_pozo
```

## Formato

| Campo | Qué es |
|---|---|
| `table` | tabla silver que produce el job |
| `source` | tabla bronze de la que lee |
| `primary_key` | columnas que identifican una fila; se deduplica por ellas |
| `partition_by` | columnas de partición Iceberg; el job reemplaza esas particiones |
| `dedupe_by` | opcional: ante claves repetidas gana la fila con el valor más alto |
| `columns` | lista de columnas, en el orden en que quedan en silver |

Cada columna declara `name`, `type` (`int`, `bigint`, `double`, `string`, `boolean`, `date`,
`timestamp`), `nullable`, `description` y, opcionalmente, `min`, `max` o `allowed_values`.

## Cómo se aplican

- **Casteo**: bronze guarda todo como string. El job recorta (`trim`), convierte `""` en null
  y castea con `try_cast`; `boolean` se arma desde los `t`/`f` que traen los CSV del portal.
- **Checks duros** (`nullable`, columnas ausentes, clave primaria duplicada): frenan el job
  con código 1. La tabla no se toca.
- **Checks blandos** (`min`, `max`, `allowed_values`): la fila no entra a silver, va a
  `<table>_rejects` con el motivo y sus strings originales. Si se rechaza más del 1 % de las
  filas de un recurso, es un problema de fondo y el job también falla.
- Toda corrida queda en `lake.silver.dq_runs`, con filas de entrada, salida y rechazos.

Un cast que falla (texto donde va un número) da null en silencio salvo que la columna sea
`nullable: false`. Si hiciera falta, se agrega como motivo de rechazo en `silver_rules.py`.
