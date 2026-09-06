"""Entrena el modelo de producción de petróleo a 12 meses y lo registra en MLflow.

Pregunta: dado el diseño de completación de un pozo no convencional (etapas, arena, agua,
rama horizontal, presión) y su contexto geológico, ¿cuánto petróleo va a acumular en sus
primeros 12 meses?

Cómo se valida: `GroupKFold` sobre el yacimiento. Dos pozos del mismo yacimiento comparten la
roca, y la roca explica buena parte de la producción; si uno queda en train y su vecino en
test, el modelo aprueba por saber dónde está el pozo y no por entender la completación. Con
split aleatorio el R² sube de 0,38 a 0,74: esa diferencia es exactamente la fuga que el split
por grupo evita medir de más.

Uso: `uv run python -m pipelines.ml.entrenar`
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
import shap
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GroupKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

from pipelines.ml import datos, registro
from pipelines.spark_jobs.config import load_config

# Backend sin ventana: el gráfico de SHAP se guarda a archivo y esto puede correr en el runner.
matplotlib.use("Agg")

logger = logging.getLogger("ml.entrenar")

SEMILLA = 0

# Búsqueda chica y a mano: con 351 pozos una grilla grande solo sirve para sobreajustar la
# validación. Estas seis combinaciones cubren el rango útil de profundidad y regularización.
GRILLA = (
    {"max_iter": 200, "learning_rate": 0.05, "max_leaf_nodes": 15, "min_samples_leaf": 10},
    {"max_iter": 300, "learning_rate": 0.05, "max_leaf_nodes": 31, "min_samples_leaf": 20},
    {"max_iter": 400, "learning_rate": 0.03, "max_leaf_nodes": 15, "min_samples_leaf": 20},
    {"max_iter": 200, "learning_rate": 0.10, "max_leaf_nodes": 8, "min_samples_leaf": 20},
    {"max_iter": 500, "learning_rate": 0.03, "max_leaf_nodes": 8, "min_samples_leaf": 10},
    {"max_iter": 150, "learning_rate": 0.10, "max_leaf_nodes": 31, "min_samples_leaf": 5},
)


@dataclass(frozen=True)
class Metricas:
    """Error de un modelo sobre un conjunto, en las dos escalas que importan."""

    mae_log: float
    rmse_log: float
    r2_log: float
    mae_m3: float
    rmse_m3: float
    r2_m3: float


def _codificador() -> ColumnTransformer:
    """Categóricas a enteros; el resto pasa de largo.

    `OrdinalEncoder` y no one-hot porque los árboles parten por umbral sobre el código y no
    necesitan la expansión: con `formacion` en 8 valores, one-hot solo agrega columnas ralas.
    `unknown_value=-1` para que una formación que aparezca después de entrenar no rompa la
    inferencia batch.
    """
    return ColumnTransformer(
        [
            (
                "categoricas",
                OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1),
                list(datos.COLUMNAS_CATEGORICAS),
            )
        ],
        remainder="passthrough",
    )


def modelo_hgb(**hiperparametros) -> Pipeline:
    """Pipeline del modelo: codificar categóricas y un HistGradientBoosting encima.

    Va todo en un solo objeto sklearn para que MLflow guarde un artefacto que ya sabe comer
    el DataFrame crudo: la inferencia no tiene que repetir la codificación.
    """
    return Pipeline(
        [
            ("codificar", _codificador()),
            (
                "modelo",
                HistGradientBoostingRegressor(
                    # Las categóricas quedan primeras en la salida del ColumnTransformer.
                    categorical_features=list(range(len(datos.COLUMNAS_CATEGORICAS))),
                    random_state=SEMILLA,
                    **hiperparametros,
                ),
            ),
        ]
    )


def modelo_mediana() -> Pipeline:
    """Baseline honesto: predecir siempre la mediana del train."""
    return Pipeline([("modelo", DummyRegressor(strategy="median"))])


def modelo_lineal() -> Pipeline:
    """Segundo baseline: regresión lineal sobre las mismas features.

    Necesita imputar porque las intensidades derivadas son nulas en los pozos verticales,
    algo que el HistGradientBoosting resuelve solo.
    """
    return Pipeline(
        [
            ("codificar", _codificador()),
            ("imputar", SimpleImputer(strategy="median")),
            ("modelo", LinearRegression()),
        ]
    )


def medir(y_log: pd.Series, prediccion_log: np.ndarray) -> Metricas:
    """MAE, RMSE y R² en log (la escala de entrenamiento) y en m3 (la que se interpreta)."""
    y_m3 = datos.a_escala_original(y_log.to_numpy())
    pred_m3 = datos.a_escala_original(prediccion_log)
    return Metricas(
        mae_log=float(mean_absolute_error(y_log, prediccion_log)),
        rmse_log=float(np.sqrt(mean_squared_error(y_log, prediccion_log))),
        r2_log=float(r2_score(y_log, prediccion_log)),
        mae_m3=float(mean_absolute_error(y_m3, pred_m3)),
        rmse_m3=float(np.sqrt(mean_squared_error(y_m3, pred_m3))),
        r2_m3=float(r2_score(y_m3, pred_m3)),
    )


def predecir_fuera_de_muestra(
    armar_modelo, x: pd.DataFrame, y: pd.Series, grupos: pd.Series, folds: int
) -> tuple[np.ndarray, list[Metricas]]:
    """Predicción out-of-fold de cada pozo y las métricas de cada fold.

    Devuelve las dos cosas porque miden distinto: el R² por fold usa la varianza del fold, que
    con yacimientos chicos es poco representativa; el R² sobre las predicciones out-of-fold
    juntas usa la varianza del dataset entero y es el número comparable entre modelos.
    """
    fuera_de_muestra = np.zeros(len(y))
    por_fold: list[Metricas] = []
    for train, test in GroupKFold(n_splits=folds).split(x, y, groups=grupos):
        modelo = armar_modelo().fit(x.iloc[train], y.iloc[train])
        prediccion = modelo.predict(x.iloc[test])
        fuera_de_muestra[test] = prediccion
        por_fold.append(medir(y.iloc[test], prediccion))
    return fuera_de_muestra, por_fold


def buscar_hiperparametros(
    x: pd.DataFrame, y: pd.Series, grupos: pd.Series, folds: int
) -> tuple[dict, float]:
    """La combinación de la grilla con mejor R² out-of-fold.

    La selección usa los mismos folds que después se reportan, así que el número publicado es
    algo optimista. Con seis candidatos el sesgo es chico y una validación anidada sobre 351
    pozos dejaría folds de test de 15 pozos: se prefiere el sesgo conocido al ruido.
    """
    mejor, mejor_r2 = GRILLA[0], -np.inf
    for candidatos in GRILLA:
        prediccion, _ = predecir_fuera_de_muestra(
            lambda p=candidatos: modelo_hgb(**p), x, y, grupos, folds
        )
        r2 = r2_score(y, prediccion)
        logger.info("grilla %s -> R2 oof %.3f", candidatos, r2)
        if r2 > mejor_r2:
            mejor, mejor_r2 = candidatos, r2
    return dict(mejor), float(mejor_r2)


def importancia_por_permutacion(
    modelo: Pipeline, x: pd.DataFrame, y: pd.Series, repeticiones: int = 10
) -> pd.DataFrame:
    """Cuánto empeora el R² al desordenar cada columna. Ordenada de mayor a menor."""
    resultado = permutation_importance(
        modelo, x, y, n_repeats=repeticiones, random_state=SEMILLA, scoring="r2"
    )
    return pd.DataFrame(
        {
            "feature": x.columns,
            "caida_r2": resultado.importances_mean,
            "desvio": resultado.importances_std,
        }
    ).sort_values("caida_r2", ascending=False, ignore_index=True)


def valores_shap(modelo: Pipeline, x: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """SHAP exacto sobre el HistGradientBoosting.

    Se explica el último paso del pipeline sobre la matriz ya codificada: TreeExplainer
    recorre los árboles, así que necesita las columnas en el mismo orden en que las ve el
    modelo (categóricas primero, por el `ColumnTransformer`).
    """
    codificado = modelo.named_steps["codificar"].transform(x)
    explicador = shap.TreeExplainer(modelo.named_steps["modelo"])
    nombres = [
        *datos.COLUMNAS_CATEGORICAS,
        *datos.COLUMNAS_NUMERICAS,
        *datos.COLUMNAS_DERIVADAS,
        datos.COLUMNA_ANIO,
    ]
    return explicador.shap_values(codificado), codificado, nombres


def guardar_shap(
    valores: np.ndarray, codificado: np.ndarray, nombres: list[str], destino: Path
) -> None:
    """Summary plot de SHAP a PNG: una fila por feature, un punto por pozo."""
    plt.figure()
    shap.summary_plot(valores, codificado, feature_names=nombres, show=False, max_display=15)
    plt.tight_layout()
    plt.savefig(destino, dpi=120)
    plt.close()


def resumen_shap(valores: np.ndarray, nombres: list[str]) -> pd.DataFrame:
    """|SHAP| promedio por feature: cuánto mueve cada una la predicción, en log(m3)."""
    return pd.DataFrame(
        {"feature": nombres, "shap_medio_abs": np.abs(valores).mean(axis=0)}
    ).sort_values("shap_medio_abs", ascending=False, ignore_index=True)


def tabla_folds(por_fold: list[Metricas]) -> pd.DataFrame:
    """Las métricas de cada fold como tabla, para loguearla como artefacto."""
    return pd.DataFrame(
        [{"fold": indice, **vars(metricas)} for indice, metricas in enumerate(por_fold, start=1)]
    )


def _loguear_metricas(prefijo: str, metricas: Metricas, paso: int | None = None) -> None:
    """Las seis métricas de un modelo, con el nombre que se ve en la UI de MLflow."""
    for nombre, valor in vars(metricas).items():
        mlflow.log_metric(f"{prefijo}_{nombre}", valor, step=paso)


def _promedio(por_fold: list[Metricas]) -> Metricas:
    """Promedio simple de los folds. Es lo que se reporta como 'error por fold'."""
    campos = vars(por_fold[0]).keys()
    return Metricas(
        **{campo: float(np.mean([vars(m)[campo] for m in por_fold])) for campo in campos}
    )


def registrar_champion(uri_modelo: str, mejora: bool) -> str | None:
    """Registra la versión y le pone el alias `champion` solo si le ganó al baseline.

    Sin la mejora se registra igual (queda el historial) pero no se promueve: `predecir.py`
    carga por alias, así que un modelo peor nunca llega a producción por el solo hecho de
    haberse entrenado.
    """
    version = mlflow.register_model(uri_modelo, registro.MODELO).version
    if not mejora:
        logger.warning("el modelo no supera al baseline: se registra v%s sin alias", version)
        return version
    mlflow.MlflowClient().set_registered_model_alias(registro.MODELO, registro.ALIAS, version)
    logger.info("registrado %s v%s con alias %s", registro.MODELO, version, registro.ALIAS)
    return version


def entrenar(pozos: pd.DataFrame, folds: int, salida: Path) -> dict:
    """El experimento completo: baselines, búsqueda, modelo final y artefactos.

    Devuelve un resumen con lo que hay que mirar para decidir si el modelo sirve.
    """
    x = datos.matriz_features(pozos)
    y = datos.objetivo_log(pozos)
    grupos = pozos[datos.GRUPO]

    mlflow.log_params(
        {
            "pozos": len(pozos),
            "yacimientos": grupos.nunique(),
            "folds": folds,
            "objetivo": f"log1p({datos.OBJETIVO})",
            "grupo_split": datos.GRUPO,
            "features": len(x.columns),
        }
    )

    resultados: dict[str, Metricas] = {}
    for nombre, armar in (("mediana", modelo_mediana), ("lineal", modelo_lineal)):
        prediccion, por_fold = predecir_fuera_de_muestra(armar, x, y, grupos, folds)
        resultados[nombre] = medir(y, prediccion)
        _loguear_metricas(f"baseline_{nombre}", resultados[nombre])
        logger.info("baseline %s: R2 oof %.3f", nombre, resultados[nombre].r2_log)

    hiperparametros, _ = buscar_hiperparametros(x, y, grupos, folds)
    mlflow.log_params({f"hgb_{k}": v for k, v in hiperparametros.items()})

    prediccion, por_fold = predecir_fuera_de_muestra(
        lambda: modelo_hgb(**hiperparametros), x, y, grupos, folds
    )
    resultados["hgb"] = medir(y, prediccion)
    _loguear_metricas("hgb_oof", resultados["hgb"])
    for indice, metricas in enumerate(por_fold, start=1):
        _loguear_metricas("hgb_fold", metricas, paso=indice)
    _loguear_metricas("hgb_fold_promedio", _promedio(por_fold))

    mejor_baseline = max(resultados["mediana"].r2_log, resultados["lineal"].r2_log)
    mejora = resultados["hgb"].r2_log > mejor_baseline
    mlflow.log_metric("mejora_sobre_baseline_r2_log", resultados["hgb"].r2_log - mejor_baseline)

    # Modelo final: los mismos hiperparámetros sobre todos los pozos. Las métricas que se
    # reportan son las out-of-fold de arriba, no las de este ajuste.
    final = modelo_hgb(**hiperparametros).fit(x, y)

    salida.mkdir(parents=True, exist_ok=True)
    tabla_folds(por_fold).to_csv(salida / "metricas_por_fold.csv", index=False)
    permutacion = importancia_por_permutacion(final, x, y)
    permutacion.to_csv(salida / "importancia_permutacion.csv", index=False)
    valores, codificado, nombres = valores_shap(final, x)
    resumen = resumen_shap(valores, nombres)
    resumen.to_csv(salida / "shap_por_feature.csv", index=False)
    guardar_shap(valores, codificado, nombres, salida / "shap_summary.png")
    pd.DataFrame(
        [{"modelo": nombre, **vars(metricas)} for nombre, metricas in resultados.items()]
    ).to_csv(salida / "comparacion_modelos.csv", index=False)
    mlflow.log_artifacts(str(salida), artifact_path="evaluacion")

    info = mlflow.sklearn.log_model(
        final,
        name="modelo",
        signature=infer_signature(x, final.predict(x)),
        input_example=x.head(3),
        # MLflow 3 serializa con skops, que exige declarar los tipos no triviales del objeto.
        # Estos dos los pone el `remainder="passthrough"` del ColumnTransformer: es código de
        # scikit-learn, no algo nuestro, y sin declararlos el guardado falla.
        skops_trusted_types=["functools.partial", "sklearn.utils.validation.check_array"],
    )
    version = registrar_champion(info.model_uri, mejora)

    return {
        "resultados": resultados,
        "por_fold": por_fold,
        "hiperparametros": hiperparametros,
        "shap": resumen,
        "mejora": mejora,
        "version": version,
    }


def imprimir_resumen(resumen: dict) -> None:
    """Lo mínimo para juzgar la corrida sin abrir la UI."""
    print("\nmodelo                 R2 log    R2 m3    MAE log    MAE m3")
    for nombre, metricas in resumen["resultados"].items():
        print(
            f"  {nombre:<18}{metricas.r2_log:>8.3f}{metricas.r2_m3:>9.3f}"
            f"{metricas.mae_log:>11.3f}{metricas.mae_m3:>10,.0f}"
        )
    print("\nR2 log por fold (out-of-fold):")
    for indice, metricas in enumerate(resumen["por_fold"], start=1):
        print(f"  fold {indice}: {metricas.r2_log:>7.3f}   MAE m3 {metricas.mae_m3:>9,.0f}")
    print("\ntop 5 features por |SHAP| medio:")
    for fila in resumen["shap"].head(5).itertuples():
        print(f"  {fila.feature:<32}{fila.shap_medio_abs:.3f}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Entrena el modelo de producción a 12 meses")
    parser.add_argument("--experimento", default=registro.EXPERIMENTO, help="experimento de MLflow")
    parser.add_argument("--folds", type=int, default=5, help="folds del GroupKFold por yacimiento")
    parser.add_argument("--salida", help="carpeta de artefactos (por defecto, una temporal)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    config = load_config()
    registro.configurar_artefactos(config)

    pozos = datos.con_objetivo_completo(
        datos.solo_no_convencionales(datos.leer_mart(datos.abrir_catalogo(config)))
    )
    logger.info("pozos con 12 meses declarados: %d", len(pozos))
    if pozos.empty:
        logger.error("no hay pozos para entrenar")
        return 1
    preparados = datos.preparar(pozos)

    mlflow.set_tracking_uri(registro.tracking_uri())
    mlflow.set_experiment(args.experimento)
    with tempfile.TemporaryDirectory() as temporal:
        salida = Path(args.salida or temporal)
        with mlflow.start_run() as corrida:
            logger.info("run %s en %s", corrida.info.run_id, registro.tracking_uri())
            resumen = entrenar(preparados, args.folds, salida)
    imprimir_resumen(resumen)
    return 0 if resumen["mejora"] else 1


if __name__ == "__main__":
    sys.exit(main())
