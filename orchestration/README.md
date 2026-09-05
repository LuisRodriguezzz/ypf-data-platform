# Orquestación

Airflow 3.3 en un solo contenedor (`airflow standalone`). No ejecuta código nuestro: cada tarea
lanza el runner de Spark con un comando y espera a que muera (ADR 0006). `runner.py` arma ese
`DockerOperator`; los DAGs solo declaran tareas y dependencias.

| DAG | Schedule | Tareas |
| --- | --- | --- |
| `produccion_pozo_mensual` | `@monthly` | ingesta → bronze → silver `produccion_pozo` → silver `pozo_primera_produccion` |
| `fractura_diaria` | `@daily` | ingesta → bronze → silver `fractura` |
| `reservas_mensual` | `@monthly` | ingesta |

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
