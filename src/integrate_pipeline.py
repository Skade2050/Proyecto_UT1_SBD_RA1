import json
import hashlib
import pandas as pd

from pathlib import Path
from typing import List

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
    return title.strip().lower()


def normalize_language_bcp47(lang: str) -> str:
    if not isinstance(lang, str) or not lang:
        return None

    lang = lang.strip().lower()
    if len(lang) == 2:
        # Regla simple: xx -> xx-XX
        return f"{lang}-{lang.upper()}"
    return lang


def normalize_currency(code: str) -> str:
    if not isinstance(code, str) or not code:
        return None
    return code.strip().upper()

def build_candidate_key(row: pd.Series) -> str:
    isbn13 = row.get("isbn13")
    if isinstance(isbn13, str) and isbn13.strip():
        return isbn13.strip()

    title_norm = row.get("title_normalized") or ""
    author_main = row.get("autor_principal") or ""
    year = row.get("anio_publicacion") or ""
    key = f"{title_norm}|{author_main}|{year}"
    return key

def load_goodreads() -> pd.DataFrame:
    if not GOODREADS_JSON.exists():
        raise FileNotFoundError(f"No se encuentra {GOODREADS_JSON}")

    df = pd.read_json(GOODREADS_JSON)

    # Añadimos número de fila de la fuente (1..N) para trazabilidad
    df["row_number"] = df.index + 1

    # Aseguramos que algunas columnas existen, aunque vengan como None
    for col in [
        "source",
        "book_id_source",
        "url",
        "title",
        "authors",
        "rating_value",
        "ratings_count",
        "pages",
        "publication_info_raw",
        "isbn10",
        "isbn13",
    ]:
        if col not in df.columns:
            df[col] = None

    # Mapeamos al modelo común (campos con nombres canónicos)
    df_common = pd.DataFrame(
        {
            "source": "goodreads",
            "source_id": df["book_id_source"],
            "source_file": "goodreads_books.json",
            "row_number": df["row_number"],
            "title": df["title"],
            "autor_principal": df["authors"],
            "autores": df["authors"],
            "editorial": None,
            "fecha_publicacion_raw": df["publication_info_raw"],
            "anio_publicacion": None,
            "paginas": df["pages"],
            "idioma_raw": None,
            "isbn10": df["isbn10"],
            "isbn13": df["isbn13"],
            "categoria_raw": None,
            "precio": None,
            "moneda": None,
            "rating_value": df["rating_value"],
            "ratings_count": df["ratings_count"],
            "url_detalle": df["url"],
            "fuente_registro": "goodreads",
        }
    )

    return df_common


def load_googlebooks() -> pd.DataFrame:
    if not GOOGLEBOOKS_CSV.exists():
        raise FileNotFoundError(f"No se encuentra {GOOGLEBOOKS_CSV}")

    df = pd.read_csv(GOOGLEBOOKS_CSV)

    # Añadimos número de fila de la fuente (1..N) para trazabilidad
    df["row_number"] = df.index + 1

    # Aseguramos que algunas columnas existen
    for col in [
        "google_volume_id",
        "title_gb",
        "subtitle_gb",
        "authors_gb",
        "publisher",
        "published_date_raw",
        "page_count",
        "language_raw",
        "categories_raw",
        "isbn10_gb",
        "isbn13_gb",
        "price_amount",
        "price_currency",
        "info_link",
        "self_link",
    ]:
        if col not in df.columns:
            df[col] = None

    df_common = pd.DataFrame(
        {
            "source": "google_books",
            "source_id": df["google_volume_id"],
            "source_file": "googlebooks_books.csv",
            "row_number": df["row_number"],
            "title": df["title_gb"],
            "autor_principal": df["authors_gb"],
            "autores": df["authors_gb"],
            "editorial": df["publisher"],
            "fecha_publicacion_raw": df["published_date_raw"],
            "anio_publicacion": None,
            "paginas": df["page_count"],
            "idioma_raw": df["language_raw"],
            "isbn10": df["isbn10_gb"],
            "isbn13": df["isbn13_gb"],
            "categoria_raw": df["categories_raw"],
            "precio": df["price_amount"],
            "moneda": df["price_currency"],
            "rating_value": None,
            "ratings_count": None,
            "url_detalle": df["info_link"],
            "fuente_registro": "google_books",
        }
    )

    return df_common

def normalize_and_add_fields(df_all: pd.DataFrame) -> pd.DataFrame:
    # Título normalizado
    df_all["title_normalized"] = df_all["title"].apply(simple_normalize_title)

    # Idioma normalizado (a partir de idioma_raw)
    df_all["idioma"] = df_all["idioma_raw"].apply(normalize_language_bcp47)

    # Moneda normalizada (a partir de la columna moneda ya existente)
    df_all["moneda"] = df_all["moneda"].apply(normalize_currency)

    # Fecha de publicación → usamos pandas.to_datetime con errors='coerce'
    df_all["fecha_publicacion"] = pd.to_datetime(
        df_all["fecha_publicacion_raw"], errors="coerce"
    )

    # Año de publicación (si hay fecha válida)
    df_all["anio_publicacion"] = df_all["fecha_publicacion"].dt.year

    # Convertimos la fecha a string ISO (YYYY-MM-DD) para la tabla final
    df_all["fecha_publicacion_iso"] = df_all["fecha_publicacion"].dt.date.astype(
        "string"
    )

    # Flags de calidad
    df_all["valid_isbn13"] = df_all["isbn13"].apply(is_valid_isbn13)
    df_all["valid_fecha_publicacion"] = df_all["fecha_publicacion"].notna()
    df_all["valid_idioma"] = df_all["idioma"].notna()
    df_all["valid_moneda"] = df_all["moneda"].notna()

    # Clave candidata de libro (para deduplicar)
    df_all["book_id_candidato"] = df_all.apply(build_candidate_key, axis=1)

    # Aseguramos que los ISBN se manejen como texto (evitar floats tipo 9.78123e+12)
    df_all["isbn13"] = df_all["isbn13"].astype("string")
    df_all["isbn10"] = df_all["isbn10"].astype("string")

    return df_all


def assert_quality_constraints(dim_book: pd.DataFrame) -> None:
    total = len(dim_book)
    assert total > 0, "dim_book está vacío: no se ha podido construir ningún libro."

    # 1) book_id debe ser único
    assert dim_book["book_id"].is_unique, "book_id no es único en dim_book."

    # 2) Al menos el 80% de los libros deben tener título no nulo
    frac_titulo = dim_book["titulo"].notna().mean()
    assert (
        frac_titulo >= 0.8
    ), f"Solo el {frac_titulo*100:.1f}% de los registros tienen 'titulo' no nulo."

    # 3) Si hay años de publicación, deben ser razonables (>= 1800)
    if dim_book["anio_publicacion"].notna().any():
        min_year = dim_book["anio_publicacion"].min()
        assert (
            min_year >= 1800
        ), f"Se ha encontrado un año de publicación sospechoso: {min_year}."

    # 4) Si hay precios, deben ser > 0
    if dim_book["precio"].notna().any():
        min_price = dim_book["precio"].min()
        assert (
            min_price > 0
        ), f"Se ha encontrado un precio no positivo en dim_book: {min_price}."

def build_dim_book(df_all: pd.DataFrame) -> pd.DataFrame:
    # Definimos prioridad de fuente (más alto = mejor)
    source_priority = {
        "google_books": 2,
        "goodreads": 1,
    }
    df_all["source_priority"] = df_all["source"].map(source_priority).fillna(0)

    # Ordenamos por clave candidata y prioridad de fuente (descendente)
    df_sorted = df_all.sort_values(
        by=["book_id_candidato", "source_priority"], ascending=[True, False]
    )

    # Nos quedamos con la primera fila de cada book_id_candidato
    df_winner = df_sorted.drop_duplicates(subset=["book_id_candidato"], keep="first")

    # Definimos el book_id final:
    #  - si hay isbn13, se usa directamente
    #  - si no, generamos un ID estable a partir de la clave candidata (hash corto)
    def make_book_id(row: pd.Series) -> str:
        isbn13 = row.get("isbn13")
        if isinstance(isbn13, str) and isbn13.strip():
            return isbn13.strip()

        key = str(row.get("book_id_candidato", ""))

        # Por seguridad, si la clave está vacía construimos otra vez con título+autor+año
        if not key:
            key = f"{row.get('title_normalized','')}|{row.get('autor_principal','')}|{row.get('anio_publicacion','')}"

        # Hash MD5 y nos quedamos con 12 caracteres (suficiente y legible)
        hash_id = hashlib.md5(key.encode("utf-8")).hexdigest()[:12]
        return hash_id

    df_winner["book_id"] = df_winner.apply(make_book_id, axis=1)

    # Campos mínimos sugeridos para dim_book
    dim = pd.DataFrame(
        {
            "book_id": df_winner["book_id"],
            "titulo": df_winner["title"],
            "titulo_normalizado": df_winner["title_normalized"],
            "autor_principal": df_winner["autor_principal"],
            "autores": df_winner["autores"],
            "editorial": df_winner["editorial"],
            "anio_publicacion": df_winner["anio_publicacion"],
            "fecha_publicacion": df_winner["fecha_publicacion_iso"],
            "idioma": df_winner["idioma"],
            "isbn10": df_winner["isbn10"],
            "isbn13": df_winner["isbn13"],
            "paginas": df_winner["paginas"],
            "formato": None,  
            "categoria": df_winner["categoria_raw"],
            "precio": df_winner["precio"],
            "moneda": df_winner["moneda"],
            "rating": df_winner["rating_value"],
            "ratings_count": df_winner["ratings_count"],
            "fuente_ganadora": df_winner["source"],
            "ts_ultima_actualizacion": pd.Timestamp.utcnow().isoformat(),
        }
    )

    return dim

def build_book_source_detail(df_all: pd.DataFrame) -> pd.DataFrame:
    # Timestamp de ingesta (mismo para todos los registros de esta ejecución)
    ts_ingesta = pd.Timestamp.utcnow().isoformat()

    detail = pd.DataFrame(
        {
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
        }
    )

    return detail

def generate_schema_md(dim_book: pd.DataFrame) -> str:
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
        "formato": "Formato del libro (tapa dura, bolsillo, ebook, etc.) — no siempre disponible.",
        "categoria": "Categorías o géneros en texto plano.",
        "precio": "Precio numérico si viene informado por la fuente.",
        "moneda": "Moneda de precio en ISO-4217 (ej: EUR, USD).",
        "rating": "Valoración media del libro (Goodreads).",
        "ratings_count": "Número de valoraciones (usuarios) en Goodreads.",
        "fuente_ganadora": "Fuente que ha ganado en la deduplicación (google_books o goodreads).",
        "ts_ultima_actualizacion": "Marca de tiempo UTC de la última actualización del registro.",
    }

    lines: List[str] = []
    lines.append("# Esquema de la tabla dim_book\n")
    lines.append("Tabla canónica de libros, una fila por libro, tras integrar todas las fuentes.\n")
    lines.append("| Campo | Tipo pandas | Descripción |")
    lines.append("|-------|-------------|-------------|")

    dtypes = dim_book.dtypes

    for col in dim_book.columns:
        dtype_str = str(dtypes[col])
        desc = descriptions.get(col, "")
        lines.append(f"| `{col}` | `{dtype_str}` | {desc} |")

    return "\n".join(lines) + "\n"

def main():
    print("[INFO] Inicio de la integración multifuente.")

    # 1) Cargamos las dos fuentes en un modelo común
    df_goodreads = load_goodreads()
    df_google = load_googlebooks()

    print(f"[INFO] Registros Goodreads: {len(df_goodreads)}")
    print(f"[INFO] Registros Google Books: {len(df_google)}")

    # 2) Unimos ambas fuentes
    df_all = pd.concat([df_goodreads, df_google], ignore_index=True)

    # Evitar problemas de tipos mixtos (int + str) al exportar a parquet
    df_all["source_id"] = df_all["source_id"].astype("string")

    # 3) Normalizaciones y campos auxiliares
    df_all = normalize_and_add_fields(df_all)

    # 4) Métricas de calidad
    metrics = compute_quality_metrics(df_all)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    with open(QUALITY_METRICS_JSON, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    print(f"[INFO] quality_metrics.json generado en {QUALITY_METRICS_JSON}")

    # 5) Construimos dim_book (tabla canónica)
    dim_book = build_dim_book(df_all)
    assert_quality_constraints(dim_book)
    STANDARD_DIR.mkdir(parents=True, exist_ok=True)
    dim_book.to_parquet(DIM_BOOK_PARQUET, index=False)
    print(f"[INFO] dim_book.parquet generado en {DIM_BOOK_PARQUET}")

    # 6) Construimos book_source_detail (detalle de fuentes)
    book_source_detail = build_book_source_detail(df_all)
    book_source_detail.to_parquet(BOOK_SOURCE_DETAIL_PARQUET, index=False)
    print(f"[INFO] book_source_detail.parquet generado en {BOOK_SOURCE_DETAIL_PARQUET}")

    # 7) Generamos schema.md
    schema_text = generate_schema_md(dim_book)
    with open(SCHEMA_MD, "w", encoding="utf-8") as f:
        f.write(schema_text)
    print(f"[INFO] schema.md generado en {SCHEMA_MD}")

    print("[INFO] Proceso de integración completado correctamente.")


if __name__ == "__main__":
    main()
