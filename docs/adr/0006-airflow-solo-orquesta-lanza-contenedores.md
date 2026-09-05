# ADR 0006 — Airflow solo orquesta: cada tarea lanza un contenedor

**Estado:** aceptada · 2026-09-05

## Contexto

La ingesta es una CLI de Python con dependencias del repo y los jobs de transformación son
`spark-submit` en un contenedor efímero (ADR 0004). Airflow podría ejecutarlos de tres formas:
con `PythonOperator`/`BashOperator` dentro del propio contenedor de Airflow, con un
`KubernetesPodOperator`, o lanzando el runner que ya existe.

La primera obligaría a instalar en la imagen de Airflow las dependencias de la ingesta, Java y
Spark, y a mantenerlas sincronizadas con el runner: dos entornos con el mismo código y distinto
resultado posible. La segunda pide un clúster que en local no existe.

## Decisión

Airflow no ejecuta código de negocio. Cada tarea es un `DockerOperator` que arranca el runner
(`apache/spark:4.0.4-...`) con el repo montado y un comando, y muere al terminar. Es el mismo
patrón que `scripts/spark-submit.ps1`, y el mismo que en AWS: un DAG que dispara un job de Glue.

La única pieza compartida es `orchestration/dags/runner.py`, con la función `runner_task`. Los
DAGs son listas de tareas: no hay fábricas de DAGs ni operadores propios.

El contenedor de Airflow habla con el motor por la API de Podman montada como volumen. Las
credenciales y endpoints llegan al contenedor de Airflow desde el compose y se reenvían al
runner, así no hay secretos escritos en los DAGs.

## Consecuencias

- El runner es el único lugar donde corre nuestro código: lo que anda con `spark-submit.ps1`
  anda igual desde Airflow, y el volumen `ivy-cache` sirve los jars y los paquetes de pip a los
  dos. Una tarea arranca en segundos.
- La ingesta pasa a correr también en el runner, que trae Python 3.10: `manifest.py` usa
  `timezone.utc` y no `datetime.UTC`, y `requirements-runner.txt` suma las dependencias de
  `pipelines.ingest` pineadas a las versiones de `uv.lock`.
- El montaje de la API de Podman es un volumen con `driver_opts` y no un bind mount: el cliente
  de Podman para Windows traduce las rutas absolutas de los bind mounts a rutas de Windows y
  `/run/user/1000/podman/podman.sock` termina apuntando a `C:\run\...`. Se monta la carpeta del
  socket porque `mount --bind` de un socket sobre un directorio falla.
- `airflow standalone` en un solo contenedor: alcanza para local y evita cuatro servicios. Sin
  login (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`), lo que solo es aceptable en la máquina de dev.
- Airflow queda atado a un motor de contenedores. En AWS el `DockerOperator` se reemplaza por
  el operador de Glue; los DAGs cambian de una línea por tarea y su forma no.
