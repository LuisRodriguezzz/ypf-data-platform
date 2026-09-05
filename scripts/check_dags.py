"""Verifica que los DAGs importan sin errores, sin levantar Airflow (CI, job `dags-importan`).

Se corre en un venv aparte con Airflow instalado (no vive en uv.lock: no se instala en
Windows). `DagBag` importa cada archivo de `orchestration/dags/` como lo haría el scheduler;
si algún import falla (typo, dependencia faltante, variable de entorno sin setear) queda en
`import_errors` en vez de tirar una excepción.
"""

from __future__ import annotations

import sys
from pathlib import Path

from airflow.models import DagBag

DAG_FOLDER = Path(__file__).resolve().parent.parent / "orchestration" / "dags"


def main() -> int:
    # Airflow 3.3 quitó `include_examples`: los ejemplos se apagan por env (ver ci.yml).
    dagbag = DagBag(dag_folder=str(DAG_FOLDER))
    if dagbag.import_errors:
        print(f"Fallaron {len(dagbag.import_errors)} DAG(s) al importar:")
        for path, error in dagbag.import_errors.items():
            print(f"\n--- {path} ---\n{error}")
        return 1
    print(f"{len(dagbag.dags)} DAG(s) importaron sin errores: {sorted(dagbag.dags)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
