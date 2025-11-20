import json
import hashlib
import pandas as pd
import numpy as np
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

from pathlib import Path
from typing import List, Dict

from utils_isbn import is_valid_isbn13
from utils_quality import compute_quality_metrics

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LANDING_DIR = PROJECT_ROOT / "landing"
STANDARD_DIR = PROJECT_ROOT / "standard"
DOCS_DIR = PROJECT_ROOT / "docs"

GOODREADS_JSON = LANDING_DIR / "goodreads_books.json"
GOOGLEBOOKS_CSV = LANDING_DIR / "googlebooks_books.csv"

DIM_BOOK_PARQUET = STANDARD_DIR / "dim_book.parquet"
BOOK_SOURCE_DETAIL_PARQUET = STANDARD_DIR / "book_source_detail.parquet"
QUALITY_METRICS_JSON = DOCS_DIR / "quality_metrics.json"
SCHEMA_MD = DOCS_DIR / "schema.md"


def simple_normalize_title(title: str) -> str:
    if not isinstance(title, str):
        return ""
    return title.split(":")[0].strip().lower()


def normalize_language_bcp47(lang: str) -> str:
    if not isinstance(lang, str) or not lang:
        return None

    lang = lang.strip().lower()
    if len(lang) == 2:
        return f"{lang}-{lang.upper()}"
    return lang


def normalize_currency(code: str) -> str:
    if not isinstance(code, str) or not code:
        return None
    return code.strip().upper()


def normalize_and_add_fields(df: pd.DataFrame) -> pd.DataFrame:
    df["title_normalized"] = df["title"].apply(simple_normalize_title)
    df["idioma"] = df["idioma_raw"].apply(normalize_language_bcp47)
    df["moneda"] = df["moneda"].apply(normalize_currency)
    df["anio_publicacion"] = df["fecha_publicacion_raw"].str.extract(
        r"(\d{4})", expand=False
    )
    df["anio_publicacion"] = pd.to_numeric(
        df["anio_publicacion"], errors="coerce"
    )
    df["fecha_publicacion_iso"] = None
    df["valid_fecha_publicacion"] = df["anio_publicacion"].notna()
    df["isbn13"] = df["isbn13"].astype("string")
    df["isbn10"] = df["isbn10"].astype("string")
    df["valid_isbn13"] = df["isbn13"].apply(is_valid_isbn13)
    df["valid_idioma"] = df["idioma"].notna()
    df["valid_moneda"] = df["moneda"].notna()

    def build_key(row):
        isbn = row.get("isbn13")
        if pd.notna(isbn) and str(isbn).strip():
            return str(isbn).strip()
        title = row.get("title_normalized", "")
        year = row.get("anio_publicacion")
        year_str = str(int(year)) if pd.notna(year) else ""
        return f"{title}|{year_str}"

    df["book_id_candidato"] = df.apply(build_key, axis=1)
    return df


def load_goodreads() -> pd.DataFrame:
    if not GOODREADS_JSON.exists():
        raise FileNotFoundError(f"No se encuentra {GOODREADS_JSON}")
    df = pd.read_json(GOODREADS_JSON)
    df["row_number"] = df.index + 1
    return pd.DataFrame({
        "source": "goodreads",
        "source_id": df["book_id_source"].astype("string"),
        "source_file": "goodreads_books.json",
        "row_number": df["row_number"],
        "title": df["title"],
        "autor_principal": df["authors"],
        "autores": df["authors"],
        "editorial": None,
        "fecha_publicacion_raw": df["publication_info_raw"],
        "paginas": df["pages"],
        "idioma_raw": None,
        "isbn10": df["isbn10"],
        "isbn13": df["isbn13"],
        "categoria_raw": None,
        "precio": None,
        "moneda": None,
        "rating_value": df["rating_value"],
        "ratings_count": df["ratings_count"],
    })


def load_googlebooks() -> pd.DataFrame:
    if not GOOGLEBOOKS_CSV.exists():
        raise FileNotFoundError(f"No se encuentra {GOOGLEBOOKS_CSV}")
    df = pd.read_csv(GOOGLEBOOKS_CSV)
    df["row_number"] = df.index + 1
    return pd.DataFrame({
        "source": "google_books",
        "source_id": df["google_volume_id"].astype("string"),
        "source_file": "googlebooks_books.csv",
        "row_number": df["row_number"],
        "title": df["title_gb"],
        "autor_principal": df["authors_gb"],
        "autores": df["authors_gb"],
        "editorial": df["publisher"],
        "fecha_publicacion_raw": df["published_date_raw"],
        "paginas": df["page_count"],
        "idioma_raw": df["language_raw"],
        "isbn10": df["isbn10_gb"],
        "isbn13": df["isbn13_gb"],
        "categoria_raw": df["categories_raw"],
        "precio": df["price_amount"],
        "moneda": df["price_currency"],
        "rating_value": None,
        "ratings_count": None,
    })


def assert_quality_constraints(dim_book: pd.DataFrame) -> None:
    total = len(dim_book)
    assert total > 0, "dim_book está vacío."
    assert dim_book["book_id"].is_unique, "book_id no es único en dim_book."
    assert dim_book["titulo"].notna().mean() >= 0.8, "Menos del 80% de libros tienen título."
    if dim_book["anio_publicacion"].notna().any():
        assert dim_book["anio_publicacion"].min() >= 1800, "Año de publicación sospechoso."


def build_dim_book(df_goodreads: pd.DataFrame, df_google: pd.DataFrame) -> pd.DataFrame:
    df_google_unique = df_google.sort_values(
        by="anio_publicacion", ascending=False
    ).drop_duplicates(subset="title_normalized", keep="first")
    df_merged = pd.merge(
        df_goodreads,
        df_google_unique,
        on="title_normalized",
        how="left",
        suffixes=('_gr', '_gg'),
    )
    for col in ["anio_publicacion", "paginas", "precio"]:
        df_merged[f"{col}_gg"] = pd.to_numeric(df_merged[f"{col}_gg"], errors="coerce")
        df_merged[f"{col}_gr"] = pd.to_numeric(df_merged[f"{col}_gr"], errors="coerce")
    gg_cols = {
        "autor_principal": df_merged["autor_principal_gg"].fillna(df_merged["autor_principal_gr"]),
        "editorial": df_merged["editorial_gg"].fillna(df_merged["editorial_gr"]),
        "anio_publicacion": df_merged["anio_publicacion_gg"].fillna(df_merged["anio_publicacion_gr"]),
        "paginas": df_merged["paginas_gg"].fillna(df_merged["paginas_gr"]),
        "idioma": df_merged["idioma_gg"].fillna(df_merged["idioma_gr"]),
        "isbn10": df_merged["isbn10_gg"].fillna(df_merged["isbn10_gr"]),
        "isbn13": df_merged["isbn13_gg"].fillna(df_merged["isbn13_gr"]),
        "categoria": df_merged["categoria_raw_gg"].fillna(df_merged["categoria_raw_gr"]),
        "precio": df_merged["precio_gg"].fillna(df_merged["precio_gr"]),
        "moneda": df_merged["moneda_gg"].fillna(df_merged["moneda_gr"]),
    }
    dim_book = pd.DataFrame(gg_cols)
    dim_book["titulo"] = df_merged["title_gr"]
    dim_book["titulo_normalizado"] = df_merged["title_normalized"]
    dim_book["autores"] = dim_book["autor_principal"]
    dim_book["rating"] = df_merged["rating_value_gr"]
    dim_book["ratings_count"] = df_merged["ratings_count_gr"]
    dim_book["book_id"] = dim_book["isbn13"]
    no_isbn_mask = dim_book["isbn13"].isna()
    hash_ids = df_merged[no_isbn_mask]["title_normalized"].apply(lambda x: hashlib.md5(x.encode("utf-8")).hexdigest()[:12])
    dim_book.loc[no_isbn_mask, "book_id"] = hash_ids
    dim_book["fuente_ganadora"] = np.where(df_merged["source_gg"].notna(), "google_books", "goodreads")
    dim_book["ts_ultima_actualizacion"] = pd.Timestamp.now("UTC").isoformat()
    final_cols = [
        "book_id", "titulo", "titulo_normalizado", "autor_principal", "autores",
        "editorial", "anio_publicacion", "fecha_publicacion", "idioma", "isbn10",
        "isbn13", "paginas", "formato", "categoria", "precio", "moneda",
        "rating", "ratings_count", "fuente_ganadora", "ts_ultima_actualizacion"
    ]
    for col in final_cols:
        if col not in dim_book.columns:
            dim_book[col] = None
    dim_book = dim_book[final_cols]
    dim_book["anio_publicacion"] = dim_book["anio_publicacion"].astype("Int64")
    return dim_book


def build_book_source_detail(df_all: pd.DataFrame) -> pd.DataFrame:
    ts_ingesta = pd.Timestamp.now("UTC").isoformat()
    return pd.DataFrame({
        "source": df_all["source"],
        "source_id": df_all["source_id"],
        "source_file": df_all["source_file"],
        "row_number": df_all["row_number"],
        "book_id_candidato": df_all["book_id_candidato"],
        "titulo": df_all["title"],
        "autor_principal": df_all["autor_principal"],
        "autores": df_all["autores"],
        "editorial": df_all["editorial"],
        "fecha_publicacion_raw": df_all["fecha_publicacion_raw"],
        "fecha_publicacion": df_all["fecha_publicacion_iso"],
        "idioma_raw": df_all["idioma_raw"],
        "idioma": df_all["idioma"],
        "isbn10": df_all["isbn10"],
        "isbn13": df_all["isbn13"],
        "categoria_raw": df_all["categoria_raw"],
        "precio": df_all["precio"],
        "moneda": df_all["moneda"],
        "rating_value": df_all["rating_value"],
        "ratings_count": df_all["ratings_count"],
        "valid_isbn13": df_all["valid_isbn13"],
        "valid_fecha_publicacion": df_all["valid_fecha_publicacion"],
        "valid_idioma": df_all["valid_idioma"],
        "valid_moneda": df_all["valid_moneda"],
        "ts_ingesta": ts_ingesta,
    })


def generate_schema_md(dim_book: pd.DataFrame) -> str:
    header = """
# Documentación del Esquema: `dim_book`

Este documento detalla la estructura, campos y reglas de negocio de la tabla canónica `dim_book`,
que consolida información de libros provenientes de Goodreads y Google Books.

## Descripción General

`dim_book` es una tabla dimensional que contiene una fila única por cada libro, identificada por `book_id`.
Los datos son el resultado de un proceso de integración que incluye normalización, validación de calidad y deduplicación.

## Fuentes de Datos

El modelo se construye a partir de las siguientes fuentes, en orden de prioridad:

1. **Google Books (`google_books`)**:  
   Fuente principal, preferida por la riqueza de sus metadatos (ISBN, detalles de publicación, categorías, precios).

2. **Goodreads (`goodreads`)**:  
   Fuente secundaria, utilizada para complementar información como ratings y conteos de valoraciones,
   o como base para libros no presentes en Google Books.

## Reglas de Deduplicación y Supervivencia

El objetivo es tener un registro único y de alta calidad por cada libro.

### **Clave de Deduplicación**

1. **Primaria**: `isbn13`.  
2. **Fallback**: Si no existe `isbn13`, se genera un `book_id` mediante un hash estable derivado de:
   - `titulo_normalizado`
   - `autor_principal`
   - `anio_publicacion`

### **Reglas de Supervivencia** (qué datos se conservan cuando hay duplicados)

- **Registro Ganador**: Se elige Google Books primero (mayor riqueza y estructura).  
- **Títulos**: Se prefiere Google Books.  
- **Autores y Categorías**: Se combinan sin perder información.  
- **ISBN**: Se utiliza cualquier valor válido, priorizando Google Books.  

---

"""
    descriptions = {
        "book_id": "Identificador único del libro en el modelo canónico (isbn13 o clave derivada).",
        "titulo": "Título principal del libro.",
        "titulo_normalizado": "Título normalizado en minúsculas y sin espacios sobrantes.",
        "autor_principal": "Autor principal del libro (texto plano).",
        "autores": "Lista de autores en texto plano (ej: 'Autor1, Autor2').",
        "editorial": "Editorial principal del libro.",
        "anio_publicacion": "Año de publicación (entero).",
        "fecha_publicacion": "Fecha de publicación en formato ISO (YYYY-MM-DD).",
        "idioma": "Idioma normalizado en formato similar a BCP-47 (ej: 'es-ES', 'en-EN').",
        "isbn10": "ISBN-10 del libro, si está disponible.",
        "isbn13": "ISBN-13 del libro, si está disponible.",
        "paginas": "Número de páginas.",
        "formato": "Formato del libro (tapa dura, bolsillo, ebook, etc.).",
        "categoria": "Categorías o géneros en texto plano.",
        "precio": "Precio numérico si viene informado por la fuente.",
        "moneda": "ISO-4217 (ej: EUR, USD).",
        "rating": "Valoración media del libro (Goodreads).",
        "ratings_count": "Número total de valoraciones.",
        "fuente_ganadora": "Fuente seleccionada tras deduplicación.",
        "ts_ultima_actualizacion": "Timestamp de la última actualización.",
    }

    lines: List[str] = []
    lines.append(header)
    lines.append("## Esquema de Columnas\n")
    lines.append("| Campo | Tipo pandas | Descripción |")
    lines.append("|-------|-------------|-------------|")

    dtypes = dim_book.dtypes

    for col in dim_book.columns:
        dtype_str = str(dtypes[col])
        desc = descriptions.get(col, "")
        lines.append(f"| `{col}` | `{dtype_str}` | {desc} |")

    return "\n".join(lines) + "\n"


def main():
    print("Inicio de la integración multifuente.")
    df_goodreads = load_goodreads()
    df_google = load_googlebooks()
    print(f"Registros Goodreads: {len(df_goodreads)}")
    print(f"Registros Google Books: {len(df_google)}")

    df_goodreads = normalize_and_add_fields(df_goodreads)
    df_google = normalize_and_add_fields(df_google)

    dim_book = build_dim_book(df_goodreads, df_google)
    assert_quality_constraints(dim_book)
    STANDARD_DIR.mkdir(parents=True, exist_ok=True)
    dim_book.to_parquet(DIM_BOOK_PARQUET, index=False)
    print(f"dim_book.parquet generado en {DIM_BOOK_PARQUET}")

    # Para los artefactos secundarios, volvemos a unir los dataframes
    all_cols = df_goodreads.columns.union(df_google.columns)
    df_goodreads = df_goodreads.reindex(columns=all_cols)
    df_google = df_google.reindex(columns=all_cols)
    df_all = pd.concat([df_goodreads, df_google], ignore_index=True)
    
    metrics = compute_quality_metrics(df_all)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    with open(QUALITY_METRICS_JSON, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    print(f"quality_metrics.json generado en {QUALITY_METRICS_JSON}")

    book_source_detail = build_book_source_detail(df_all)
    book_source_detail.to_parquet(BOOK_SOURCE_DETAIL_PARQUET, index=False)
    print(f"book_source_detail.parquet generado en {BOOK_SOURCE_DETAIL_PARQUET}")

    schema_text = generate_schema_md(dim_book)
    with open(SCHEMA_MD, "w", encoding="utf-8") as f:
        f.write(schema_text)
    print(f"schema.md generado en {SCHEMA_MD}")

    print("Proceso de integración completado correctamente.")


if __name__ == "__main__":
    main()
