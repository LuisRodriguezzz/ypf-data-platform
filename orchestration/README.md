# Orquestación

Airflow 3.3 en un solo contenedor (`airflow standalone`). No ejecuta código nuestro: cada tarea
lanza el runner de Spark con un comando y espera a que muera (ADR 0006). `runner.py` arma ese
`DockerOperator`; los DAGs solo declaran tareas y dependencias.

| DAG | Schedule | Tareas |
| --- | --- | --- |
| `produccion_pozo_mensual` | `@monthly` | ingesta → bronze → silver `produccion_pozo` → silver `pozo_primera_produccion` |
| `fractura_diaria` | `@daily` | ingesta → bronze → silver `fractura` |
| `reservas_mensual` | `@monthly` | ingesta → bronze → silver `reservas` |
| `gold_mensual` | día 1 a las 6 | `dbt build` (modelos y tests de gold, ADR 0009) |
| `ml_mensual` | día 2 a las 7 | `entrenar` → `predecir` (ADR 0012) |
| `monitoreo_diario` | `@daily` | `salud` (`dbt build --select monitoreo`) → `frescura` (`dbt source freshness`) |

`gold_mensual` espera a las fuentes por calendario y no con un `ExternalTaskSensor`: los tres
DAGs de origen no comparten schedule, así que un sensor pediría un `execution_date_fn` por cada
uno para alinear intervalos que no coinciden. Corre seis horas después; el motivo está escrito
en el `doc_md` del DAG.

## Observabilidad y alertas

`monitoreo_diario` es el único DAG que no mueve datos de negocio: reconstruye
`gold.salud_pipeline` y `gold.calidad_por_corrida` —una fila por tabla del lakehouse con filas,
última carga y estado de calidad— y después chequea la frescura de las fuentes contra los
umbrales de `pipelines/dbt/models/sources.yml`. `dbt source freshness` termina con código
distinto de cero si alguna fuente pasó su `error_after`, así que una fuente que dejó de
publicarse hace fallar el DAG. No hay ningún servicio nuevo: son dos `dbt` en el runner de
siempre.

**Dónde se conecta un canal de alertas real.** En `orchestration/dags/alertas.py`. La función
`avisar_falla` está enganchada en el `default_args` de los seis DAGs, así que cualquier tarea
que falle pasa por ella, y hoy escribe una línea de log con el DAG, la tarea, la corrida, el
intento y la URL de la UI:

```text
ALERTA | dag=monitoreo_diario | tarea=frescura | corrida=scheduled__2026-09-06T03:00:00+00:00 | intento=1 | http://localhost:8080/dags/...
```

Mandar eso a un correo, a Slack o a PagerDuty es cambiar esas cinco líneas y nada más: no hay
un segundo lugar donde el proyecto decida qué hacer con una falla. Queda como log a propósito —
un webhook de Slack en un repo público es un secreto en el repo, y un SMTP en la máquina de
desarrollo es infraestructura que no se puede demostrar.

## Levantar

```powershell
cd infra\docker
podman-compose --profile core --profile airflow up -d
```

Hace falta nombrar los dos perfiles: `podman-compose` resuelve el `depends_on` del servicio
`airflow` sobre `postgres`, que vive en el perfil `core`. UI en <http://localhost:8080>, sin
login (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`, solo para local). Los DAGs se leen de
`orchestration/dags` montado en solo lectura: editar un archivo alcanza, no hay que reiniciar.

La ruta del repo dentro de la máquina de Podman va en `infra/docker/.env` (`RUNNER_REPO_DIR`):
los montajes del runner los resuelve el motor en WSL, no Windows.

## Disparar y seguir una corrida

Desde la UI: el botón ▶ del DAG. Por CLI:

```powershell
podman exec ypf-lakehouse_airflow_1 airflow dags unpause fractura_diaria
podman exec ypf-lakehouse_airflow_1 airflow dags trigger fractura_diaria
podman exec ypf-lakehouse_airflow_1 airflow dags list-import-errors   # tiene que estar vacío
```

Los logs de cada tarea (incluida la salida del runner) se ven en la UI en *Grid → Logs*, o en
el volumen `airflow-logs`:

```powershell
podman exec ypf-lakehouse_airflow_1 ls /opt/airflow/logs/dag_id=fractura_diaria
podman logs -f ypf-lakehouse_airflow_1        # scheduler y api-server
```

El estado por tarea también sale de la base de Airflow:

```powershell
podman exec ypf-lakehouse_postgres_1 psql -U lakehouse -d airflow -c "select dag_id, task_id, state, start_date, end_date from task_instance order by start_date desc limit 10;"
```
