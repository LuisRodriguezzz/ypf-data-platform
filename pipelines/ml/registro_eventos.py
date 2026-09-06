"""Coordenadas en MLflow del clasificador de eventos de pozo.

Mismo papel que `registro.py` para el modelo de completación: son los únicos datos que el
entrenamiento y la inferencia tienen que tener idénticos para que `detectar_eventos` cargue el
modelo que dejó `entrenar_eventos`.

Es un módulo aparte y no tres constantes más en `registro.py` porque son dos modelos con ciclos
de vida distintos: uno se reentrena por mes con el mart de gold, el otro por día con la
telemetría. El server, el bucket de artefactos y el alias sí se comparten y se importan de allá.
"""

from __future__ import annotations

from pipelines.ml.registro import ALIAS, configurar_artefactos, tracking_uri

__all__ = [
    "ALIAS",
    "EXPERIMENTO",
    "MODELO",
    "configurar_artefactos",
    "tracking_uri",
    "uri_champion",
]

EXPERIMENTO = "eventos_pozo"
MODELO = "clasificador_eventos_pozo"


def uri_champion() -> str:
    """URI del modelo en producción, tal como lo pide `mlflow.sklearn.load_model`."""
    return f"models:/{MODELO}@{ALIAS}"
