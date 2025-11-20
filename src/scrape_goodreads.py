import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

BASE_BOOK_URL = "https://www.goodreads.com/book/show/{}"
BASE_SEARCH_URL = "https://www.goodreads.com/search?q={query}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

CONSULTA_DEFAULT = "data science"
NUM_BOOKS_DEFAULT = 10
MAX_PAGINAS_BUSQUEDA = 5

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LANDING_DIR = PROJECT_ROOT / "landing"
OUTPUT_JSON = LANDING_DIR / "goodreads_books.json"

def fetch_html(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        print(f"[ERROR] No se pudo descargar {url}: {e}")
        return None

def extract_book_id_from_href(href: str) -> Optional[str]:
    if not href:
        return None
    m = re.search(r"/book/show/(\d+)", href)
    return m.group(1) if m else None

def clean_isbn(candidate: str) -> Optional[str]:
    if not candidate:
        return None
    digits = re.sub(r"\D", "", candidate)
    if len(digits) in (10, 13):
        return digits
    return None

def extract_isbns_from_ld_json(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    isbn10 = None
    isbn13 = None

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (TypeError, json.JSONDecodeError):
            continue

        # A veces hay un solo objeto, a veces una lista
        candidates = data if isinstance(data, list) else [data]

        for obj in candidates:
            if not isinstance(obj, dict):
                continue
            raw_isbn = obj.get("isbn")
            cleaned = clean_isbn(str(raw_isbn)) if raw_isbn else None
            if not cleaned:
                continue

            if len(cleaned) == 13 and isbn13 is None:
                isbn13 = cleaned
            elif len(cleaned) == 10 and isbn10 is None:
                isbn10 = cleaned

        if isbn10 or isbn13:
            break

    return {"isbn10": isbn10, "isbn13": isbn13}

def extract_isbns_from_text(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    text = soup.get_text(" ", strip=True)
    candidates = re.findall(r"ISBN[^\dXx]*([\dXx\- ]+)", text)

    isbn10 = None
    isbn13 = None

    for c in candidates:
        cleaned = clean_isbn(c)
        if not cleaned:
            continue
        if len(cleaned) == 13 and isbn13 is None:
            isbn13 = cleaned
        elif len(cleaned) == 10 and isbn10 is None:
            isbn10 = cleaned

    return {"isbn10": isbn10, "isbn13": isbn13}


def extract_isbns(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    data = extract_isbns_from_ld_json(soup)
    if data["isbn10"] or data["isbn13"]:
        return data
    return extract_isbns_from_text(soup)

def search_book_ids(query: str, max_books: int) -> List[str]:
    print(f"Buscando libros con la consulta: {query!r}")
    book_ids: List[str] = []

    for page in range(1, MAX_PAGINAS_BUSQUEDA + 1):
        url = f"{BASE_SEARCH_URL.format(query=quote_plus(query))}&page={page}"
        print(f" Página {page}: {url}")

        html = fetch_html(url)
        if html is None:
            print(" No se pudo obtener la página de búsqueda, se detiene la búsqueda.")
            break

        soup = BeautifulSoup(html, "lxml")

        # contenedor de libro = tr[itemtype='http://schema.org/Book']
        rows = soup.select("tr[itemtype='http://schema.org/Book']")
        if not rows:
            print("No hay más resultados en esta página.")
            break

        for row in rows:
            link = row.select_one("a.bookTitle")
            if not link:
                continue

            href = link.get("href", "")
            book_id = extract_book_id_from_href(href)
            if book_id and book_id not in book_ids:
                book_ids.append(book_id)
                print(f" Encontrado ID={book_id}")

            if len(book_ids) >= max_books:
                break

        if len(book_ids) >= max_books:
            break

        time.sleep(0.5)

    print(f"Total de IDs encontrados: {len(book_ids)}")
    return book_ids

def extract_data_from_ld_json(soup: BeautifulSoup) -> Dict:
    """Extrae datos estructurados de un script JSON-LD en la página."""
    ld_json_data = {}
    script = soup.find("script", type="application/ld+json")
    if not script:
        return ld_json_data

    try:
        data = json.loads(script.string or "")
    except (TypeError, json.JSONDecodeError):
        return ld_json_data

    # A veces hay un solo objeto, a veces una lista
    candidates = data if isinstance(data, list) else [data]

    for obj in candidates:
        if not isinstance(obj, dict) or obj.get("@type") != "Book":
            continue

        # Título
        if "name" in obj:
            ld_json_data["title"] = obj["name"]

        # Autores
        if "author" in obj and isinstance(obj["author"], list):
            authors = [a.get("name") for a in obj["author"] if a.get("name")]
            if authors:
                ld_json_data["authors"] = ", ".join(authors)

        # Rating y número de valoraciones
        if "aggregateRating" in obj and isinstance(obj["aggregateRating"], dict):
            rating_info = obj["aggregateRating"]
            if "ratingValue" in rating_info:
                try:
                    ld_json_data["rating_value"] = float(rating_info["ratingValue"])
                except (ValueError, TypeError):
                    pass
            if "ratingCount" in rating_info:
                try:
                    ld_json_data["ratings_count"] = int(rating_info["ratingCount"])
                except (ValueError, TypeError):
                    pass
        
        # ISBN (ya que estamos aquí)
        if "isbn" in obj:
            cleaned_isbn = clean_isbn(str(obj["isbn"]))
            if cleaned_isbn:
                if len(cleaned_isbn) == 13:
                    ld_json_data["isbn13"] = cleaned_isbn
                elif len(cleaned_isbn) == 10:
                    ld_json_data["isbn10"] = cleaned_isbn
        
        # Si hemos encontrado todo lo principal, podemos parar
        if "title" in ld_json_data and "authors" in ld_json_data:
            break
            
    return ld_json_data

def parse_book(html: str, book_id: str) -> Dict:
    soup = BeautifulSoup(html, "lxml")

    # Estrategia 1: Extraer datos de JSON-LD (preferido)
    ld_data = extract_data_from_ld_json(soup)

    # Estrategia 2: Fallback a selectores CSS si JSON-LD falla
    
    # Título
    title = ld_data.get("title")
    if not title:
        title_tag = soup.select_one("h1[data-testid='bookTitle']")
        title = title_tag.get_text(strip=True) if title_tag else None

    # Autor(es)
    authors_str = ld_data.get("authors")
    if not authors_str:
        author_tags = soup.select("span[data-testid='authorName'] a")
        authors = [a.get_text(strip=True) for a in author_tags] if author_tags else []
        authors_str = ", ".join(authors) if authors else None

    # Rating medio
    rating_value = ld_data.get("rating_value")
    if rating_value is None:
        rating_tag = soup.select_one("div[data-testid='rating'] span[data-testid='ratingValue']")
        if rating_tag:
            try:
                rating_value = float(rating_tag.get_text(strip=True))
            except (ValueError, TypeError):
                rating_value = None

    # Número de valoraciones
    ratings_count = ld_data.get("ratings_count")
    if ratings_count is None:
        ratings_count_tag = soup.select_one("div[data-testid='rating'] span[data-testid='ratingsCount']")
        if ratings_count_tag:
            txt = ratings_count_tag.get_text(strip=True).replace(",", "")
            num_match = re.search(r"^\d+", txt)
            if num_match:
                ratings_count = int(num_match.group(0))

    # Páginas
    pages = None
    pages_tag = soup.find("p", attrs={"data-testid": "pagesFormat"})
    if pages_tag:
        txt = pages_tag.get_text(strip=True).lower()
        for token in txt.split():
            if token.isdigit():
                pages = int(token)
                break

    # Información de publicación en bruto
    publication_info_raw = None
    pub_tag = soup.find("p", attrs={"data-testid": "publicationInfo"})
    if pub_tag:
        publication_info_raw = pub_tag.get_text(strip=True)

    # ISBNs
    isbn10 = ld_data.get("isbn10")
    isbn13 = ld_data.get("isbn13")
    if not isbn10 and not isbn13:
        # Fallback a los métodos de extracción de ISBN si no están en JSON-LD
        isbn_data = extract_isbns(soup)
        isbn10 = isbn_data["isbn10"]
        isbn13 = isbn_data["isbn13"]


    scraped_at = datetime.now(timezone.utc).isoformat()

    return {
        "source": "goodreads",
        "book_id_source": book_id,
        "url": BASE_BOOK_URL.format(book_id),
        "title": title,
        "authors": authors_str,
        "rating_value": rating_value,
        "ratings_count": ratings_count,
        "pages": pages,
        "publication_info_raw": publication_info_raw,
        "isbn10": isbn10,
        "isbn13": isbn13,
        "search_query": CONSULTA_DEFAULT,
        "scraped_at": scraped_at,
    }

def scrape_goodreads(book_ids: List[str]) -> List[Dict]:
    results: List[Dict] = []

    for idx, book_id in enumerate(book_ids, start=1):
        print(f"({idx}/{len(book_ids)}) Libro ID={book_id}")
        html = fetch_html(BASE_BOOK_URL.format(book_id))
        if html is None:
            print(f" Saltando libro {book_id} por error de descarga.")
            continue

        try:
            book_data = parse_book(html, book_id)
            results.append(book_data)
        except Exception as e:
            print(f" Error parseando libro {book_id}: {e}")

        time.sleep(1.0)

    print(f"Scraping terminado. Libros válidos: {len(results)}")
    return results

def save_to_json(records: List[Dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"Archivo guardado en: {output_path}")

def main() -> None:
    print("Inicio del scraping de Goodreads.")
    print(f"Consulta: {CONSULTA_DEFAULT!r}")

    ids = search_book_ids(CONSULTA_DEFAULT, NUM_BOOKS_DEFAULT)
    if not ids:
        print("No se ha encontrado ningún ID de libro.")
        return

    books = scrape_goodreads(ids)
    if not books:
        print("No se ha podido scrapear ningún libro.")
        return

    save_to_json(books, OUTPUT_JSON)
    print("Proceso completado correctamente.")

if __name__ == "__main__":
    main()
