"""Entrena el clasificador de eventos de pozo sobre la telemetría 3W y lo registra en MLflow.

Pregunta: mirando 180 segundos de telemetría de un pozo, ¿el pozo está **normal**, está en el
**transitorio** que precede a un evento no deseado, o el **evento** ya está ocurriendo?

Las dos clases de evento que hay en landing son las de `docs/fuentes/telemetria_3w.md`: cierre
espurio del DHSV (clase 2, se ve en las presiones en segundos) y scaling en el choke de
producción (clase 7, una degradación de horas o días). El modelo no distingue entre las dos:
distingue en qué **etapa** está el pozo, que es lo que decide si hay que avisar.

Cómo se valida: `GroupKFold` sobre `instancia_id` (un archivo de 3W = un pozo registrado de
corrido). Dos ventanas consecutivas de la misma instancia comparten 165 de sus 180 segundos;
con un split aleatorio casi toda ventana de test tiene su gemela en train y el modelo aprueba
por haber visto ese mismo minuto, no por reconocer el fenómeno. Se reporta además el split por
`well_3w`, que es más duro todavía: ni siquiera el pozo se repite entre train y test.

Uso: `uv run python -m pipelines.ml.entrenar_eventos`
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import boto3
import mlflow
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.metrics import confusion_matrix, f1_score, precision_recall_fscore_support
from sklearn.model_selection import GroupKFold
from sklearn.pipeline import Pipeline
from sklearn.tree import DecisionTreeClassifier

from pipelines.ingest.manifest import Manifest
from pipelines.ingest.settings import load_settings
from pipelines.ml import registro_eventos as registro
from pipelines.ml import telemetria_features as tf
from pipelines.spark_jobs.config import LakehouseConfig, load_config
from pipelines.streaming.landing_3w import ArchivoLanding, archivos_en_landing

logger = logging.getLogger("ml.entrenar_eventos")

SEMILLA = 0

# Una sola configuración y no una grilla. Con 88 instancias el split por instancia deja folds
# de 17 grupos: elegir hiperparámetros sobre esos mismos folds infla el número que después se
# publica, y el modelo ya le gana al baseline por márgenes amplios. Es el mismo razonamiento
# que el ADR 0012 aplica a la validación anidada, llevado un paso más lejos.
HIPERPARAMETROS = {
    "max_iter": 300,
    "learning_rate": 0.1,
    "max_leaf_nodes": 31,
    "min_samples_leaf": 50,
    # Con 100.000 ventanas el early stopping de scikit-learn se activa solo; se deja explícito
    # para que la corrida sea reproducible y no dependa del tamaño del dataset.
    "early_stopping": True,
    "validation_fraction": 0.1,
    "n_iter_no_change": 20,
}

# Feature del baseline de umbral: el desvío de la presión del TPT dentro de la ventana. Es la
# regla que pondría un operador sin modelo —"si la presión se movió más que X, avisá"— y es
# contra eso que hay que justificar 30 features y un gradient boosting.
FEATURE_UMBRAL = "p_tpt_desvio"

# Momentos en los que puede caer la primera alarma de una instancia con evento.
ANTES = "antes del transitorio"
A_TIEMPO = "en el transitorio"
TARDE = "despues del evento"
SIN_ALARMA = "sin alarma"


@dataclass(frozen=True)
class Corte:
    """Un split de validación: cómo se agrupa y con cuántos folds."""

    nombre: str
    columna: str
    folds: int


def abrir_s3(config: LakehouseConfig):
    """Cliente S3 apuntado a landing (MinIO en local), igual que el productor de replay."""
    return boto3.client(
        "s3",
        endpoint_url=config.s3_endpoint_url or None,
        aws_access_key_id=config.s3_access_key_id or None,
        aws_secret_access_key=config.s3_secret_access_key or None,
        region_name=config.s3_region,
    )


def leer_instancia(cliente, bucket: str, archivo: ArchivoLanding) -> pd.DataFrame:
    """Un Parquet de 3W de landing, con los nombres de columna de bronze.

    `timestamp` viaja como índice en la metadata de pandas del archivo, de ahí el `reset_index`.
    """
    cuerpo = cliente.get_object(Bucket=bucket, Key=archivo.landing_key)["Body"].read()
    crudo = pq.read_table(io.BytesIO(cuerpo)).to_pandas().reset_index()
    return tf.normalizar_3w(crudo)


def hitos_de_instancia(telemetria: pd.DataFrame, archivo: ArchivoLanding) -> dict:
    """Cuándo empieza el transitorio y cuándo el evento en esta instancia.

    Son los dos instantes contra los que se mide la anticipación. Muchas instancias no llegan
    a tener evento: de las 36 de clase 7 en landing, 31 terminan en el transitorio.
    """
    return {
        "instancia_id": archivo.resource_id,
        "well_3w": archivo.well_3w,
        "clase_3w": archivo.clase,
        "filas": len(telemetria),
        "inicio_transitorio": tf.inicio_de_etiqueta(telemetria, tf.TRANSITORIO),
        "inicio_evento": tf.inicio_de_etiqueta(telemetria, tf.EVENTO),
    }


def construir_dataset(
    config: LakehouseConfig, clases: list[int] | None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ventanas etiquetadas de todas las instancias de landing, más los hitos de cada una."""
    settings = load_settings()
    archivos = archivos_en_landing(Manifest(settings.postgres_dsn), clases)
    cliente = abrir_s3(config)
    ventanas, hitos = [], []
    for archivo in archivos:
        telemetria = leer_instancia(cliente, settings.s3_landing_bucket, archivo)
        hitos.append(hitos_de_instancia(telemetria, archivo))
        de_esta = tf.construir_ventanas(telemetria, archivo.resource_id)
        ventanas.append(de_esta.assign(well_3w=archivo.well_3w, clase_3w=archivo.clase))
        logger.info(
            "%s: %d filas -> %d ventanas", archivo.resource_id, len(telemetria), len(de_esta)
        )
    todas = tf.con_datos(tf.solo_etiquetadas(pd.concat(ventanas, ignore_index=True)))
    return todas, pd.DataFrame(hitos)


def modelo_hgb() -> HistGradientBoostingClassifier:
    """El clasificador: gradient boosting sobre histogramas, con las clases balanceadas.

    Sin `Pipeline` porque las 30 features ya son numéricas y el HGB come `NaN` de fábrica: un
    sensor que la instancia no trae entra como faltante y el modelo aprende hacia qué rama
    mandarlo, que es exactamente lo que se quiere (ver ADR 0013).

    `class_weight="balanced"` porque las tres clases están muy desparejas: el transitorio de
    una instancia de clase 7 dura días y el evento, minutos.
    """
    return HistGradientBoostingClassifier(
        class_weight="balanced", random_state=SEMILLA, **HIPERPARAMETROS
    )


def modelo_mayoritaria() -> DummyClassifier:
    """Baseline mínimo: predecir siempre la clase más frecuente del train."""
    return DummyClassifier(strategy="most_frequent")


def modelo_umbral() -> Pipeline:
    """Baseline de sala de control: un solo umbral sobre un solo sensor.

    Un árbol de profundidad 1 sobre `p_tpt_desvio` es literalmente eso: busca el corte que
    mejor separa y responde con dos clases. Si el modelo grande no le gana holgado, no hay
    caso para 30 features.
    """
    return Pipeline(
        [
            ("una_columna", ColumnTransformer([("sensor", "passthrough", [FEATURE_UMBRAL])])),
            # El árbol no admite NaN; el HGB sí, y esa es parte de la diferencia entre los dos.
            ("imputar", SimpleImputer(strategy="median")),
            (
                "modelo",
                DecisionTreeClassifier(max_depth=1, class_weight="balanced", random_state=SEMILLA),
            ),
        ]
    )


def predecir_fuera_de_muestra(
    armar_modelo, x: pd.DataFrame, y: pd.Series, grupos: pd.Series, folds: int
) -> tuple[np.ndarray, np.ndarray]:
    """Etiqueta y probabilidad que cada ventana recibió cuando estuvo en test.

    Es la única predicción honesta que se puede mirar: la ventana nunca vio a su instancia en
    el entrenamiento del modelo que la clasificó.
    """
    prediccion = np.empty(len(y), dtype=object)
    probabilidad = np.zeros(len(y))
    for train, test in GroupKFold(n_splits=folds).split(x, y, groups=grupos):
        modelo = armar_modelo().fit(x.iloc[train], y.iloc[train])
        prediccion[test] = modelo.predict(x.iloc[test])
        probabilidad[test] = modelo.predict_proba(x.iloc[test]).max(axis=1)
    return prediccion, probabilidad


def metricas_por_clase(y: pd.Series, prediccion: np.ndarray) -> pd.DataFrame:
    """Precisión, recall, F1 y soporte de cada una de las tres etiquetas."""
    precision, recall, f1, soporte = precision_recall_fscore_support(
        y, prediccion, labels=list(tf.ETIQUETAS), zero_division=0
    )
    return pd.DataFrame(
        {
            "etiqueta": list(tf.ETIQUETAS),
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "soporte": soporte,
        }
    )


def f1_macro(y: pd.Series, prediccion: np.ndarray) -> float:
    """F1 macro: las tres clases pesan igual aunque el transitorio tenga 10 veces más ventanas.

    Es la métrica de decisión del alias `champion`: un modelo que acierte todo el transitorio
    y nada del evento no sirve para avisar.
    """
    return float(
        f1_score(y, prediccion, labels=list(tf.ETIQUETAS), average="macro", zero_division=0)
    )


def matriz_confusion(y: pd.Series, prediccion: np.ndarray) -> pd.DataFrame:
    """Matriz de confusión con las etiquetas como nombres de fila y columna."""
    matriz = confusion_matrix(y, prediccion, labels=list(tf.ETIQUETAS))
    return pd.DataFrame(
        matriz,
        index=[f"real_{etiqueta}" for etiqueta in tf.ETIQUETAS],
        columns=[f"pred_{etiqueta}" for etiqueta in tf.ETIQUETAS],
    )


def _momento_de_alarma(alarma, inicio_transitorio, inicio_evento) -> str:
    """En qué etapa de la instancia cayó la primera alarma."""
    if alarma is None or pd.isna(alarma):
        return SIN_ALARMA
    if pd.notna(inicio_transitorio) and alarma < inicio_transitorio:
        return ANTES
    if alarma > inicio_evento:
        return TARDE
    return A_TIEMPO


def anticipacion(
    ventanas: pd.DataFrame, prediccion: np.ndarray, hitos: pd.DataFrame
) -> pd.DataFrame:
    """Cuántos segundos antes del evento se levantó la primera alarma, instancia por instancia.

    La alarma es la primera ventana que el modelo no clasificó como `normal`, y se mide contra
    el **fin** de esa ventana: es el primer instante en que un consumidor en línea la habría
    tenido. Solo entran las instancias que efectivamente tienen evento.

    Se reporta también dónde cayó la alarma. Una que aparece antes de que arranque el
    transitorio da una anticipación enorme y no es mérito: es un falso positivo que por
    casualidad quedó del lado correcto, y contarlo como anticipación sería mentir.
    """
    con_evento = hitos[hitos["inicio_evento"].notna()]
    alarmas = ventanas.assign(prediccion=prediccion)
    alarmas = alarmas[alarmas["prediccion"] != tf.NORMAL]
    primera = alarmas.groupby("instancia_id")["ventana_fin"].min()

    filas = []
    for hito in con_evento.itertuples():
        alarma = primera.get(hito.instancia_id)
        momento = _momento_de_alarma(alarma, hito.inicio_transitorio, hito.inicio_evento)
        filas.append(
            {
                "instancia_id": hito.instancia_id,
                "clase_3w": hito.clase_3w,
                "well_3w": hito.well_3w,
                "inicio_transitorio": hito.inicio_transitorio,
                "inicio_evento": hito.inicio_evento,
                "primera_alarma": alarma,
                "anticipacion_s": (
                    np.nan
                    if momento == SIN_ALARMA
                    else (hito.inicio_evento - alarma).total_seconds()
                ),
                "momento": momento,
            }
        )
    return pd.DataFrame(filas)


def anticipacion_por_clase(tabla: pd.DataFrame) -> pd.DataFrame:
    """Anticipación media y mediana por clase de 3W, contando solo las alarmas a tiempo.

    El promedio global no dice mucho: un cierre de DHSV (clase 2) se anuncia con un transitorio
    de una o dos horas y el scaling del choke (clase 7) con uno de días. Mezclarlos da una
    media que no describe a ninguno de los dos.
    """
    a_tiempo = tabla[tabla["momento"] == A_TIEMPO]
    if a_tiempo.empty:
        return pd.DataFrame(columns=["clase_3w", "instancias", "media_s", "mediana_s"])
    return (
        a_tiempo.groupby("clase_3w")["anticipacion_s"]
        .agg(instancias="size", media_s="mean", mediana_s="median")
        .reset_index()
    )


def resumen_anticipacion(tabla: pd.DataFrame) -> dict:
    """Los números que resumen la anticipación, contando solo las alarmas a tiempo."""
    a_tiempo = tabla[tabla["momento"] == A_TIEMPO]
    return {
        "instancias_con_evento": len(tabla),
        "alarmas_a_tiempo": len(a_tiempo),
        "alarmas_antes_del_transitorio": int((tabla["momento"] == ANTES).sum()),
        "alarmas_tarde": int((tabla["momento"] == TARDE).sum()),
        "instancias_sin_alarma": int((tabla["momento"] == SIN_ALARMA).sum()),
        "anticipacion_media_s": float(a_tiempo["anticipacion_s"].mean()) if len(a_tiempo) else 0.0,
        "anticipacion_mediana_s": (
            float(a_tiempo["anticipacion_s"].median()) if len(a_tiempo) else 0.0
        ),
    }


def importancia_por_permutacion(
    modelo, x: pd.DataFrame, y: pd.Series, repeticiones: int = 5
) -> pd.DataFrame:
    """Cuánto cae el F1 macro al desordenar cada feature. Ordenada de mayor a menor.

    Se mide sobre el F1 macro y no sobre la exactitud porque es la métrica con la que se decide
    promover el modelo: una feature que solo ayuda a la clase mayoritaria no vale nada acá.
    """
    resultado = permutation_importance(
        modelo, x, y, n_repeats=repeticiones, random_state=SEMILLA, scoring="f1_macro", n_jobs=1
    )
    return pd.DataFrame(
        {
            "feature": x.columns,
            "caida_f1_macro": resultado.importances_mean,
            "desvio": resultado.importances_std,
        }
    ).sort_values("caida_f1_macro", ascending=False, ignore_index=True)


def registrar_champion(uri_modelo: str, mejora: bool) -> str | None:
    """Registra la versión y le pone el alias `champion` solo si le ganó a los baselines.

    Mismo criterio que `entrenar.py`: sin la mejora queda el historial pero no se promueve, y
    `detectar_eventos.py` —que carga por alias— sigue sirviendo el modelo anterior.
    """
    version = mlflow.register_model(uri_modelo, registro.MODELO).version
    if not mejora:
        logger.warning("el modelo no supera al baseline: se registra v%s sin alias", version)
        return version
    mlflow.MlflowClient().set_registered_model_alias(registro.MODELO, registro.ALIAS, version)
    logger.info("registrado %s v%s con alias %s", registro.MODELO, version, registro.ALIAS)
    return version


def _loguear_por_clase(prefijo: str, por_clase: pd.DataFrame) -> None:
    """Las tres métricas de cada clase, con el nombre que se ve en la UI de MLflow."""
    for fila in por_clase.itertuples():
        for metrica in ("precision", "recall", "f1"):
            mlflow.log_metric(f"{prefijo}_{metrica}_{fila.etiqueta}", getattr(fila, metrica))


def evaluar_corte(
    ventanas: pd.DataFrame, x: pd.DataFrame, y: pd.Series, corte: Corte
) -> tuple[np.ndarray, float]:
    """Predicción out-of-fold del modelo con un criterio de agrupamiento, y su F1 macro."""
    prediccion, _ = predecir_fuera_de_muestra(
        modelo_hgb, x, y, ventanas[corte.columna], corte.folds
    )
    macro = f1_macro(y, prediccion)
    logger.info("split por %s: F1 macro oof %.3f", corte.nombre, macro)
    return prediccion, macro


def entrenar(ventanas: pd.DataFrame, hitos: pd.DataFrame, folds: int, salida: Path) -> dict:
    """El experimento completo: baselines, los dos splits, anticipación y modelo final."""
    x = ventanas[tf.nombres_features()]
    y = ventanas["etiqueta"]

    mlflow.log_params(
        {
            "ventanas": len(ventanas),
            "instancias": ventanas["instancia_id"].nunique(),
            "pozos_3w": ventanas["well_3w"].nunique(),
            "ventana_s": tf.VENTANA_S,
            "paso_s": tf.PASO_S,
            "max_ventanas_por_instancia": tf.MAX_VENTANAS_POR_INSTANCIA,
            "sensores": ",".join(tf.SENSORES),
            "features": len(x.columns),
            "folds": folds,
            **{f"hgb_{clave}": valor for clave, valor in HIPERPARAMETROS.items()},
        }
    )

    macros: dict[str, float] = {}
    for nombre, armar in (("mayoritaria", modelo_mayoritaria), ("umbral", modelo_umbral)):
        prediccion, _ = predecir_fuera_de_muestra(armar, x, y, ventanas["instancia_id"], folds)
        macros[nombre] = f1_macro(y, prediccion)
        mlflow.log_metric(f"baseline_{nombre}_f1_macro", macros[nombre])
        _loguear_por_clase(f"baseline_{nombre}", metricas_por_clase(y, prediccion))
        logger.info("baseline %s: F1 macro oof %.3f", nombre, macros[nombre])

    por_instancia = Corte("instancia", "instancia_id", folds)
    # Cinco folds sobre 18 pozos: cada fold deja 3 o 4 pozos enteros fuera del entrenamiento.
    por_pozo = Corte("pozo", "well_3w", min(folds, ventanas["well_3w"].nunique()))
    prediccion, macros["hgb"] = evaluar_corte(ventanas, x, y, por_instancia)
    _, macros["hgb_por_pozo"] = evaluar_corte(ventanas, x, y, por_pozo)
    for nombre, valor in macros.items():
        mlflow.log_metric(f"f1_macro_{nombre}", valor)

    por_clase = metricas_por_clase(y, prediccion)
    _loguear_por_clase("hgb_oof", por_clase)
    confusion = matriz_confusion(y, prediccion)
    tabla_anticipacion = anticipacion(ventanas, prediccion, hitos)
    resumen = resumen_anticipacion(tabla_anticipacion)
    por_clase_anticipacion = anticipacion_por_clase(tabla_anticipacion)
    mlflow.log_metrics(resumen)

    mejor_baseline = max(macros["mayoritaria"], macros["umbral"])
    mejora = macros["hgb"] > mejor_baseline
    mlflow.log_metric("mejora_sobre_baseline_f1_macro", macros["hgb"] - mejor_baseline)

    # Modelo final: los mismos hiperparámetros sobre todas las ventanas. Las métricas que se
    # reportan son las out-of-fold de arriba, no las de este ajuste.
    final = modelo_hgb().fit(x, y)
    permutacion = importancia_por_permutacion(final, x, y)

    salida.mkdir(parents=True, exist_ok=True)
    por_clase.to_csv(salida / "metricas_por_clase.csv", index=False)
    confusion.to_csv(salida / "matriz_confusion.csv")
    tabla_anticipacion.to_csv(salida / "anticipacion_por_instancia.csv", index=False)
    por_clase_anticipacion.to_csv(salida / "anticipacion_por_clase.csv", index=False)
    permutacion.to_csv(salida / "importancia_permutacion.csv", index=False)
    hitos.to_csv(salida / "instancias.csv", index=False)
    pd.DataFrame([{"modelo": k, "f1_macro": v} for k, v in macros.items()]).to_csv(
        salida / "comparacion_modelos.csv", index=False
    )
    mlflow.log_artifacts(str(salida), artifact_path="evaluacion")

    info = mlflow.sklearn.log_model(
        final,
        name="modelo",
        signature=infer_signature(x, final.predict(x)),
        input_example=x.head(3),
    )
    version = registrar_champion(info.model_uri, mejora)

    return {
        "macros": macros,
        "por_clase": por_clase,
        "confusion": confusion,
        "anticipacion": tabla_anticipacion,
        "anticipacion_por_clase": por_clase_anticipacion,
        "resumen_anticipacion": resumen,
        "permutacion": permutacion,
        "mejora": mejora,
        "version": version,
    }


def imprimir_resumen(resumen: dict, ventanas: pd.DataFrame) -> None:
    """Lo mínimo para juzgar la corrida sin abrir la UI de MLflow."""
    print("\nventanas por etiqueta:")
    for etiqueta, cuenta in ventanas["etiqueta"].value_counts().items():
        print(f"  {etiqueta:<14}{cuenta:>8,}")
    print("\nF1 macro out-of-fold:")
    for nombre, valor in resumen["macros"].items():
        print(f"  {nombre:<18}{valor:>8.3f}")
    print("\npor clase (HistGradientBoosting, split por instancia):")
    print(resumen["por_clase"].to_string(index=False))
    print("\nmatriz de confusion:")
    print(resumen["confusion"].to_string())
    print("\nanticipacion:")
    for clave, valor in resumen["resumen_anticipacion"].items():
        print(f"  {clave:<32}{valor:>10.1f}")
    print("\nanticipacion por clase de 3W (solo alarmas a tiempo):")
    print(resumen["anticipacion_por_clase"].to_string(index=False))
    print("\ntop 10 features por caida de F1 macro:")
    for fila in resumen["permutacion"].head(10).itertuples():
        print(f"  {fila.feature:<24}{fila.caida_f1_macro:>8.4f}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Entrena el clasificador de eventos de pozo")
    parser.add_argument("--experimento", default=registro.EXPERIMENTO, help="experimento de MLflow")
    parser.add_argument("--folds", type=int, default=5, help="folds del GroupKFold por instancia")
    parser.add_argument("--clases", default="", help="clases de 3W a usar (por defecto, todas)")
    parser.add_argument("--salida", help="carpeta de artefactos (por defecto, una temporal)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    config = load_config()
    registro.configurar_artefactos(config)

    comenzo = time.monotonic()
    clases = [int(parte) for parte in args.clases.split(",") if parte.strip()] or None
    ventanas, hitos = construir_dataset(config, clases)
    logger.info(
        "%d ventanas de %d instancias en %.1f s",
        len(ventanas),
        ventanas["instancia_id"].nunique(),
        time.monotonic() - comenzo,
    )
    if ventanas.empty:
        logger.error("no hay ventanas para entrenar")
        return 1

    mlflow.set_tracking_uri(registro.tracking_uri())
    mlflow.set_experiment(args.experimento)
    with tempfile.TemporaryDirectory() as temporal:
        salida = Path(args.salida or temporal)
        with mlflow.start_run() as corrida:
            logger.info("run %s en %s", corrida.info.run_id, registro.tracking_uri())
            resumen = entrenar(ventanas, hitos, args.folds, salida)
    imprimir_resumen(resumen, ventanas)
    return 0 if resumen["mejora"] else 1


if __name__ == "__main__":
    sys.exit(main())
