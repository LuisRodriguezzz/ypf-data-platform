# ADR 0004 — Spark corre en un contenedor efímero, no en el host

**Estado:** aceptada · 2026-09-05

## Contexto

Los jobs de transformación son Spark en modo `local[*]` (ADR 0001). El host es Windows 11 sin Java: instalarlo implica además `winutils.exe` y `HADOOP_HOME`, una fuente conocida de fallas que no aporta nada al proyecto. Un clúster Spark (master + workers) tampoco aporta: con un solo nodo de 16 GB, `local[*]` usa los mismos núcleos con menos partes móviles, y en AWS el runtime lo pone Glue.

## Decisión

Spark vive en un contenedor `apache/spark:4.0.4-scala2.13-java17-python3-ubuntu` bajo el perfil `spark` del compose. No es un servicio: se lanza con `podman-compose --profile spark run --rm spark ...`, corre un `spark-submit` sobre el código montado del repo y muere. `scripts/spark-submit.ps1` y `scripts/spark-submit.sh` envuelven ese comando.

Los jars de Iceberg, hadoop-aws y el JDBC de Postgres se declaran en `infra/docker/spark-defaults.conf` y se cachean en el volumen `ivy-cache`.

## Consecuencias

- El host no necesita Java, Spark ni winutils; el runner tampoco necesita dependencias Python del proyecto, así que los jobs solo pueden usar la stdlib y PySpark (por eso `pipelines/spark_jobs/config.py` lee el entorno sin pydantic, y el manifiesto se lee por JDBC y no con SQLAlchemy).
- `spark.jars.packages` no puede fijarse desde el código: cuando el job crea la `SparkSession` la JVM de `spark-submit` ya arrancó. Vive en `spark-defaults.conf`, que es además donde iría el equivalente en Glue (`--extra-jars`).
- La primera corrida baja unos 700 MB de jars de Maven; las siguientes arrancan en segundos.
- El código del job no sabe que está en un contenedor: los endpoints llegan por variables de entorno, así que el mismo `bronze_load.py` corre en Glue cambiando solo la configuración.

## Actualización 2026-09-05

La imagen no trae PyYAML, y los contratos y el mapeo de tablas se leían con un parser de YAML propio (`yaml_lite.py`) para evitar sumar la dependencia. Mantener un parser casero va contra el criterio del proyecto (código simple, sin abstracciones que no hagan falta), así que se reemplazó por la solución directa: `scripts/spark-submit.ps1` y `scripts/spark-submit.sh` corren `pip install --user` con `pipelines/spark_jobs/requirements-runner.txt` (pineado) antes de cada `spark-submit`. El volumen `ivy-cache`, que ya persiste los jars de Maven, persiste también el paquete instalado en `/home/spark`: la primera corrida lo descarga y las siguientes lo saltean. No hace falta construir una imagen propia ni tocar `spark-defaults.conf`. `bronze_rules.py` y `silver_rules.py` ahora usan `yaml.safe_load` directamente.
