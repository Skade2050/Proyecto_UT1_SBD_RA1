"""
Funciones de ayuda para métricas de calidad de datos.

Se usa desde integrate_pipeline.py para generar quality_metrics.json.
"""

from typing import Dict

import pandas as pd


def compute_quality_metrics(df_all: pd.DataFrame) -> Dict:
    """
    Calcula métricas de calidad sencillas para guardarlas en JSON.
    Es la misma lógica que tenías en integrate_pipeline.py, movida a un módulo aparte.
    """
    total = len(df_all)
    total_goodreads = (df_all["source"] == "goodreads").sum()
    total_google = (df_all["source"] == "google_books").sum()

    metrics = {
        "total_registros": int(total),
        "total_goodreads": int(total_goodreads),
        "total_google_books": int(total_google),
    }

    # Porcentajes de validez
    for col_flag in [
        "valid_isbn13",
        "valid_fecha_publicacion",
        "valid_idioma",
        "valid_moneda",
    ]:
        if col_flag in df_all.columns:
            fraction = df_all[col_flag].mean() if total > 0 else 0.0
            metrics[f"porcentaje_{col_flag}"] = round(float(fraction) * 100, 2)

    # Duplicados por clave candidata
    dup_counts = df_all["book_id_candidato"].value_counts()
    duplicated_keys = int((dup_counts > 1).sum())
    metrics["claves_candidatas_duplicadas"] = duplicated_keys

    return metrics
