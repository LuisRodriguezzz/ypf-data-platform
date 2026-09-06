# ADR 0010 — En aws, gold corre con dbt-athena dentro de un job de Glue

**Estado:** aceptada · 2026-09-06

## Contexto

El ADR 0009 dejó gold resuelta en local: dbt con el adaptador de Spark en modo sesión, adentro
del runner efímero. Para el destino `aws` quedaba pendiente el adaptador y, con él, dos
preguntas: qué motor ejecuta el SQL de los modelos y en qué proceso corre dbt.

La primera ya estaba contestada desde el ADR 0002: Athena. Lee las mismas tablas Iceberg del
Glue Data Catalog que escriben bronze y silver, no hay nada que levantar y se paga por TB
escaneado. La alternativa sería `dbt-glue`, que lanza una sesión interactiva de Glue por
corrida: más caro y un motor más que mantener para el mismo resultado.

La segunda no era obvia. dbt necesita un proceso donde correr, y este proyecto no tiene ni una
máquina ni un contenedor propio en AWS.

## Decisión

**Un job de Glue `gold_dbt` que corre `dbt build --target aws`.** El proyecto de dbt viaja
adentro del wheel (`pipelines/dbt/`), que Glue instala con pip junto con `dbt-core==1.11.14` y
`dbt-athena==1.11.0`; el wrapper `pipelines/aws/gold_dbt_job.py` traduce los argumentos del job
a las variables que lee el target `aws` de `profiles.yml` e invoca a `dbtRunner`. La máquina de
estados `gold_mensual` lo dispara, igual que las de las fuentes disparan sus tres jobs.

**Sobre Glue 5.0 (Spark) y no sobre Python shell, aunque Spark no se use.** Es lo que más
costó decidir y es el desvío del plan original. Glue Python shell sigue clavado en Python 3.9;
dbt-core lo dejó de soportar en la 1.11 y `dbt-athena` en la 1.10.2. En 3.9 lo más nuevo que
resuelve pip es `dbt-athena` 1.9.5 con un `dbt-core` de dos versiones menores atrás del que
corre en local: dos stacks de dbt distintos entre destinos, para ahorrar unos centavos. Glue
5.0 trae Python 3.11, así que el mismo job type que ya usan bronze y silver corre exactamente
las versiones del runner local. Se paga un clúster de dos DPU que dbt no toca —el trabajo lo
hace Athena— y son unos 0,15 USD por corrida.

La otra alternativa era correr dbt desde GitHub Actions con credenciales OIDC. Es más barato
todavía (cero), pero saca a gold de Step Functions: el pipeline quedaría partido entre dos
orquestadores, `aws_logs.ps1` no la vería y no habría forma de encadenar gold detrás de las
fuentes. La comodidad de tener una sola máquina de estados vale más que 15 centavos.

**Las diferencias de dialecto viven en `macros/dialecto.sql`, una macro `dispatch` por
función.** Athena es Trino: comparte casi todo con Spark SQL y se separa en media docena de
funciones. Cada una es una macro de tres líneas con el nombre de la función de Spark, así los
modelos no cambian:

| Spark | Athena (Trino) |
| --- | --- |
| `md5(x)` | `lower(to_hex(md5(to_utf8(cast(x as varchar)))))` |
| `make_date(a, m, d)` | `from_iso8601_date(format('%04d-%02d-%02d', a, m, d))` |
| `date_format(f, 'yyyy-MM')` | `format_datetime(cast(f as timestamp), 'yyyy-MM')` |
| `last_day(f)` | `last_day_of_month(f)` |
| `datediff(b, a)` | `date_diff('day', a, b)` |
| `int(months_between(b, a))` | `date_diff('month', a, b)` |
| `select explode(sequence(...))` | `select x from unnest(sequence(...)) as t(x)` |
| `cast(x as string)` | `cast(x as varchar)` |
| `'\\s+'` | `'\s+'` |

Las dos últimas no son funciones. `string` no existe como tipo en Trino, y la expresión
regular se escribe distinta porque Spark interpreta las secuencias de escape adentro de la
comilla simple y Trino no: el mismo literal `'\\s+'` que en Spark es «uno o más espacios», en
Athena busca una barra invertida seguida de eses.

Tres macros no se llaman como la función de Spark —`fin_de_mes`, `dias_entre` y
`meses_entre`— porque dbt-core ya publica `last_day` y `datediff` propias y definirlas en el
proyecto se las cambiaría también a dbt.

## Consecuencias

- Gold es la cuarta capa que corre en los dos destinos con el mismo código. Lo único
  específico de un motor sigue siendo un solo archivo, ahora `macros/dialecto.sql` en vez de
  `macros/claves.sql`.
- Los modelos quedan como tablas Iceberg también en Athena (`table_type: iceberg`,
  `format: parquet` en `dbt_project.yml`): las lee Athena, las lee Spark y las lee `check_lake`.
- Los datos de las tablas de gold van a `warehouse/gold/` (`s3_data_dir`) y no debajo de
  `athena-results/`, que es donde dbt-athena los pondría por defecto y donde la regla de ciclo
  de vida del bucket los borraría a los siete días.
- El job de gold instala unos cincuenta paquetes en cada corrida (dbt y el wheel con sus
  dependencias). Medido el 2026-09-06: 197 s de job, de los cuales 81 son el `dbt build` —
  8 modelos y 73 tests, todos en verde— y el resto arranque del clúster e instalación. Es el
  precio de no mantener una imagen propia.
- Los dos destinos dan exactamente las mismas filas y las mismas claves: `md5` produce el
  mismo hexadecimal en Spark y en Trino, así que `pozo_key` y `empresa_key` son comparables
  entre local y aws. Verificado tabla por tabla (611.304 en `dim_pozo`, 4.635 en el mart).
- `dbt docs` no se genera en AWS. El catálogo y el manifiesto salen de la corrida local, que
  es la que alguien mira.
