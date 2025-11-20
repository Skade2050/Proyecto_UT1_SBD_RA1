import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests
import pandas as pd

try:
    # No es obligatorio, pero si tienes python-dotenv lo usamos
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LANDING_DIR = PROJECT_ROOT / "landing"

INPUT_JSON = LANDING_DIR / "goodreads_books.json"
OUTPUT_CSV = LANDING_DIR / "googlebooks_books.csv"

GOOGLE_BOOKS_API_URL = "https://www.googleapis.com/books/v1/volumes"

# Cargamos variables de entorno desde .env si existe
if load_dotenv is not None:
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path)

API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")  # puede ser None, la API funciona sin clave con límite

def load_goodreads_records() -> List[Dict]:
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"No se encuentra el archivo: {INPUT_JSON}")

    df = pd.read_json(INPUT_JSON)
    # Lo convertimos a lista de dict por simplicidad
    return df.to_dict(orient="records")


def build_query(record: Dict) -> Optional[str]:

    def clean_isbn(value) -> Optional[str]:
        """Devuelve el ISBN con solo dígitos, o None si no es válido."""
        if value is None:
            return None
        s = str(value).strip()
        if s == "" or s.lower() in {"none", "null", "nan"}:
            return None
        # Nos quedamos solo con los dígitos
        digits = "".join(ch for ch in s if ch.isdigit())
        if len(digits) in (10, 13):
            return digits
        return None

    raw_isbn13 = record.get("isbn13")
    raw_isbn10 = record.get("isbn10")

    isbn13 = clean_isbn(raw_isbn13)
    isbn10 = clean_isbn(raw_isbn10)

    title = record.get("title")
    authors = record.get("authors")

    # 1) Si tenemos un ISBN válido, lo usamos
    if isbn13:
        return f"isbn:{isbn13}"
    if isbn10:
        return f"isbn:{isbn10}"

    # 2) Si no, usamos título + primer autor como fallback
    parts: List[str] = []
    if title:
        parts.append(str(title))
    if authors:
        first_author = str(authors).split(",")[0]
        parts.append(first_author)

    if not parts:
        return None

    return " ".join(parts)



def call_google_books_api(query: str) -> Optional[Dict]:

    params = {
        "q": query,
        "maxResults": 1,
    }
    if API_KEY:
        params["key"] = API_KEY

    try:
        resp = requests.get(GOOGLE_BOOKS_API_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"Fallo en la llamada a Google Books para query='{query}': {e}")
        return None

    items = data.get("items")
    if not items:
        return None

    return items[0]  # primer resultado


def parse_google_volume(volume: Dict, query: str) -> Dict:

    volume_id = volume.get("id")
    volume_info = volume.get("volumeInfo", {}) or {}
    sale_info = volume.get("saleInfo", {}) or {}

    title = volume_info.get("title")
    subtitle = volume_info.get("subtitle")
    authors = volume_info.get("authors") or []
    authors_str = ", ".join(authors) if authors else None

    publisher = volume_info.get("publisher")
    published_date = volume_info.get("publishedDate")  # tal cual, luego normalizaremos
    page_count = volume_info.get("pageCount")
    language = volume_info.get("language")  # ej: "en", "es"

    categories = volume_info.get("categories") or []
    categories_str = " | ".join(categories) if categories else None

    # ISBNs
    isbn10 = None
    isbn13 = None
    for ident in volume_info.get("industryIdentifiers", []) or []:
        t = ident.get("type")
        v = ident.get("identifier")
        if t == "ISBN_10":
            isbn10 = v
        elif t == "ISBN_13":
            isbn13 = v

    # Información de precio (si existe)
    price_amount = None
    price_currency = None

    # Probamos primero con listPrice, luego con retailPrice
    price_obj = sale_info.get("listPrice") or sale_info.get("retailPrice")
    if isinstance(price_obj, dict):
        price_amount = price_obj.get("amount")
        price_currency = price_obj.get("currencyCode")

    # Enlaces útiles
    info_link = volume_info.get("infoLink")
    self_link = volume.get("selfLink")

    record: Dict = {
        "source": "google_books",
        "query_used": query,
        "google_volume_id": volume_id,
        "title_gb": title,
        "subtitle_gb": subtitle,
        "authors_gb": authors_str,
        "publisher": publisher,
        "published_date_raw": published_date,
        "page_count": page_count,
        "language_raw": language,
        "categories_raw": categories_str,
        "isbn10_gb": isbn10,
        "isbn13_gb": isbn13,
        "price_amount": price_amount,
        "price_currency": price_currency,
        "info_link": info_link,
        "self_link": self_link,
    }
    return record


def enrich_with_google_books(records: List[Dict]) -> List[Dict]:

    enriched: List[Dict] = []

    for idx, rec in enumerate(records, start=1):
        # Primer intento: query “normal”
        query = build_query(rec)
        print(f" ({idx}/{len(records)}) Query para Google Books: {query!r}")

        volume = None

        if query:
            volume = call_google_books_api(query)

        # Segundo intento: solo con el título si el primero falla
        if volume is None:
            title_only = rec.get("title")
            if title_only:
                print(f" Sin resultados, reintentando solo con título: {title_only!r}")
                volume = call_google_books_api(title_only)

        if volume is None:
            print(" Sin resultados en Google Books para este libro, se omite.")
            continue

        gb_record = parse_google_volume(volume, query or rec.get("title") or "")
        enriched.append(gb_record)

        # Pausa para ser "amables" con la API
        time.sleep(0.5)

    print(f" Enriquecimiento terminado. Libros enriquecidos: {len(enriched)}")
    return enriched



def save_to_csv(records: List[Dict], output_path: Path) -> None:

    if not records:
        print("No hay registros para guardar en CSV.")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(records)
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Archivo CSV guardado en: {output_path}")

def main():
    print("Inicio del enriquecimiento con Google Books.")

    try:
        goodreads_records = load_goodreads_records()
    except FileNotFoundError as e:
        print(f"{e}")
        return

    print(f"Registros leídos desde goodreads_books.json: {len(goodreads_records)}")

    enriched_records = enrich_with_google_books(goodreads_records)
    save_to_csv(enriched_records, OUTPUT_CSV)

    print("Proceso completado correctamente.")

if __name__ == "__main__":
    main()
