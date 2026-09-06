-- Bases separadas por responsabilidad. Se crean una sola vez al inicializar el volumen.
CREATE DATABASE airflow;
CREATE DATABASE iceberg;
-- Backend del tracking server y del model registry de MLflow (ADR 0012).
-- Este script solo corre cuando el volumen `pg-data` está vacío. En un entorno que ya lo tiene
-- creado (el caso de esta máquina), la base se agrega a mano una sola vez:
--   podman exec ypf-lakehouse_postgres_1 psql -U lakehouse -d lakehouse -c 'CREATE DATABASE mlflow'
CREATE DATABASE mlflow;
