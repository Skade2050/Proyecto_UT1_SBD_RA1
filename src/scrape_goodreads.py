import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

# URL base de Goodreads para un libro concreto
BASE_BOOK_URL = "https://www.goodreads.com/book/show/{}"

# URL base de búsqueda
BASE_SEARCH_URL = "https://www.goodreads.com/search?q={query}"

# User-Agent "realista" para que Goodreads no sospeche que somos un script
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

# Parámetros de la búsqueda automática
SEARCH_QUERY = "data science"   # cámbialo si quieres otra temática
MAX_BOOKS = 12                  # número máximo de libros a scrapear

# Ruta al archivo de salida dentro de landing/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LANDING_DIR = PROJECT_ROOT / "landing"
OUTPUT_JSON = LANDING_DIR / "goodreads_books.json"


def fetch_html(url: str) -> Optional[str]:
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"[ERROR] No se pudo descargar la página {url}: {e}")
        return None


def extract_book_id_from_href(href: str) -> Optional[str]:
    if not href:
        return None
    m = re.search(r"/book/show/(\d+)", href)
    if m:
        return m.group(1)
    return None


def search_book_ids(query: str, max_books: int, max_pages: int = 5) -> List[str]:
    print(f"[INFO] Buscando libros en Goodreads con la query: {query!r}")
    book_ids: List[str] = []

    for page in range(1, max_pages + 1):
        url = f"https://www.goodreads.com/search?page={page}&q={quote_plus(query)}"
        print(f"[INFO]   Descargando página de búsqueda {page}: {url}")
        html = fetch_html(url)
        if html is None:
            print("[WARN]   No se pudo obtener esta página de búsqueda, se corta el bucle.")
            break

        soup = BeautifulSoup(html, "lxml")
        rows = soup.select("table.tableList tr")
        if not rows:
            print("[INFO]   No hay más resultados en esta página, fin de la búsqueda.")
            break

        for row in rows:
            link = row.select_one("a.bookTitle")
            if not link:
                continue
            href = link.get("href", "")
            book_id = extract_book_id_from_href(href)
            if book_id and book_id not in book_ids:
                book_ids.append(book_id)
                print(f"[INFO]     Encontrado libro ID={book_id}")
            if len(book_ids) >= max_books:
                break

        if len(book_ids) >= max_books:
            break

        # Pequeña pausa por cortesía
        time.sleep(0.5)

    print(f"[INFO] Total de IDs encontrados: {len(book_ids)}")
    return book_ids


def extract_isbns_from_soup(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    text = soup.get_text(" ", strip=True)

    # ISBN-13: secuencia de exactamente 13 dígitos
    match_13 = re.search(r"\b\d{13}\b", text)
    isbn13 = match_13.group(0) if match_13 else None

    # ISBN-10: secuencia de exactamente 10 dígitos
    match_10 = re.search(r"\b\d{10}\b", text)
    isbn10 = match_10.group(0) if match_10 else None

    return {"isbn10": isbn10, "isbn13": isbn13}


def parse_book(html: str, book_id: str) -> Dict:
    soup = BeautifulSoup(html, "lxml")

    # Título del libro
    title_tag = soup.select_one("h1[data-testid='bookTitle']")
    title = title_tag.get_text(strip=True) if title_tag else None

    # Autores (puede haber varios enlaces bajo 'data-testid=authorName')
    author_tags = soup.select("span[data-testid='authorName'] a")
    authors = [a.get_text(strip=True) for a in author_tags] if author_tags else []
    authors_str = ", ".join(authors) if authors else None

    # Valoración media (rating)
    rating_tag = soup.select_one("div[data-testid='rating'] span[data-testid='ratingValue']")
    rating_value = None
    if rating_tag:
        try:
            rating_value = float(rating_tag.get_text(strip=True))
        except ValueError:
            rating_value = None

    # Número de valoraciones (ratings_count)
    ratings_count_tag = soup.select_one("div[data-testid='rating'] span[data-testid='ratingsCount']")
    ratings_count = None
    if ratings_count_tag:
        text = ratings_count_tag.get_text(strip=True).replace(",", "")
        parts = text.split()
        if parts and parts[0].isdigit():
            ratings_count = int(parts[0])

    # Número de páginas (pages)
    pages_tag = soup.find("p", attrs={"data-testid": "pagesFormat"})
    pages = None
    if pages_tag:
        text = pages_tag.get_text(strip=True).lower()
        for token in text.split():
            if token.isdigit():
                pages = int(token)
                break

    # Info de publicación (texto bruto)
    details_section = soup.find("p", attrs={"data-testid": "publicationInfo"})
    publication_info = details_section.get_text(strip=True) if details_section else None

    # Intentamos extraer ISBNs de forma sencilla (si no hay, se quedan en None)
    isbn_data = extract_isbns_from_soup(soup)
    isbn10 = isbn_data["isbn10"]
    isbn13 = isbn_data["isbn13"]

    # Metadatos útiles para el pipeline y la documentación
    scraped_at = datetime.utcnow().isoformat()

    book_dict = {
        "source": "goodreads",
        "book_id_source": book_id,
        "url": BASE_BOOK_URL.format(book_id),
        "title": title,
        "authors": authors_str,
        "rating_value": rating_value,
        "ratings_count": ratings_count,
        "pages": pages,
        "publication_info_raw": publication_info,
        "isbn10": isbn10,
        "isbn13": isbn13,
        "search_query": SEARCH_QUERY,
        "scraped_at": scraped_at,
    }

    return book_dict


def scrape_goodreads(book_ids: List[str]) -> List[Dict]:
    results: List[Dict] = []

    for idx, book_id in enumerate(book_ids, start=1):
        print(f"[INFO] ({idx}/{len(book_ids)}) Procesando libro ID={book_id}...")

        url = BASE_BOOK_URL.format(book_id)
        html = fetch_html(url)
        if html is None:
            print(f"[WARN] Saltando libro {book_id} por error de descarga.")
            continue

        book_data = parse_book(html, book_id)
        results.append(book_data)

        # Pausa para no saturar Goodreads (entre 0.5 y 1.5 segundos)
        time.sleep(1.0)

    print(f"[INFO] Scraping terminado. Libros válidos: {len(results)}")
    return results


def save_to_json(records: List[Dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"[INFO] Archivo JSON guardado en: {output_path}")

def main():
    print("[INFO] Inicio del scraping de Goodreads.")
    print(f"[INFO] Búsqueda configurada: {SEARCH_QUERY!r} (máx. {MAX_BOOKS} libros)")

    # 1) Buscar IDs automáticamente
    book_ids = search_book_ids(SEARCH_QUERY, MAX_BOOKS)
    if not book_ids:
        print("[ERROR] No se ha encontrado ningún ID de libro en la búsqueda.")
        return

    print(f"[INFO] IDs que se van a scrapear: {book_ids}")

    # 2) Scraping de cada ficha de libro
    books = scrape_goodreads(book_ids)
    if not books:
        print("[ERROR] No se ha podido obtener ningún libro. Revisa la conexión o los selectores.")
        return

    # 3) Guardar JSON en landing/
    save_to_json(books, OUTPUT_JSON)
    print("[INFO] Proceso completado correctamente.")


if __name__ == "__main__":
    main()
