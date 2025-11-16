# 📚 Mini-pipeline de libros (Goodreads + Google Books)

Este repositorio implementa un mini-pipeline de extracción → enriquecimiento → integración para un conjunto pequeño de libros.

El objetivo es obtener datos desde Goodreads (scraping), enriquecerlos usando la API de Google Books y consolidar ambas fuentes en un modelo canónico, asegurando trazabilidad, normalización y métricas de calidad.

## 📁 Estructura del repositorio

```
books-pipeline/
├── landing/
│   ├── goodreads_books.json
│   └── googlebooks_books.csv
├── standard/
│   ├── dim_book.parquet
│   └── book_source_detail.parquet
├── docs/
│   ├── quality_metrics.json
│   └── schema.md
├── src/
│   ├── scrape_goodreads.py
│   ├── enrich_googlebooks.py
│   ├── integrate_pipeline.py
│   ├── utils_isbn.py
│   └── utils_quality.py
├── .env.example
├── requirements.txt
└── README.md
```

## 🔧 Requisitos

- Python 3.10+
- Conexión a Internet
- Dependencias:
  - requests, beautifulsoup4, lxml
  - pandas, numpy, pyarrow
  - python-dotenv (opcional)

Instalación:

```bash
pip install -r requirements.txt
```

## 🕸️ Scraping de Goodreads

```bash
python src/scrape_goodreads.py
```

### Qué hace

- Busca “data science” en Goodreads.
- Extrae título, autores, rating, páginas, fecha de publicación y más.
- Usa estos selectores:

```
h1[data-testid='bookTitle']
span[data-testid='authorName'] a
span[data-testid='ratingValue']
span[data-testid='ratingsCount']
p[data-testid='pagesFormat']
p[data-testid='publicationInfo']
```

### Ética del scraping

- Pausas de 0.5–1.5s
- User-Agent realista
- Sin acciones agresivas

Salida:

```
landing/goodreads_books.json
```

## 📚 Enriquecimiento con Google Books

```bash
python src/enrich_googlebooks.py
```

- Construye queries usando isbn13 → isbn10 → título+autor.
- Llama a la API de Google Books.
- Extrae título, autores, editorial, fecha, idioma, categorías, precio, ISBNs.

Configurar `.env` opcional:

```
GOOGLE_BOOKS_API_KEY=TU_API_KEY
```

Salida:

```
landing/googlebooks_books.csv
```

## 🔄 Integración + Normalización + Deduplicación

```bash
python src/integrate_pipeline.py
```

### Normalización

- Título normalizado
- Idioma BCP-47 aproximado
- Moneda ISO-4217
- Fechas ISO-8601
- ISBN como texto

### Clave canónica (`book_id`)

```
Si existe isbn13 válido → book_id = isbn13
Si no → título_normalizado + autor_principal + año
```

### Deduplicación

Prioridad:

1. google_books
2. goodreads

Reglas:

- Se escoge el registro con más prioridad por fuente.
- Se genera `book_source_detail` con provenance.

## 📦 Artefactos generados

### standard/

| Archivo | Descripción |
|--------|-------------|
| dim_book.parquet | Tabla final, 1 fila por libro |
| book_source_detail.parquet | Detalle por fuente y registro original |

### docs/

| Archivo | Descripción |
|--------|-------------|
| quality_metrics.json | Métricas de calidad |
| schema.md | Documentación del modelo |

## 📊 Métricas de calidad (ejemplo real)

```json
{
  "total_registros": 24,
  "total_goodreads": 12,
  "total_google_books": 12,
  "porcentaje_valid_isbn13": 0.0,
  "porcentaje_valid_fecha_publicacion": 50.0,
  "porcentaje_valid_idioma": 50.0,
  "porcentaje_valid_moneda": 29.17,
  "claves_candidatas_duplicadas": 0
}
```

## 🛡️ Aserciones de calidad

El pipeline falla si:

- book_id no es único  
- <80% de títulos  
- años sospechosos (<1800)  
- precios ≤ 0  

## ▶️ Ejecución completa

```bash
python src/scrape_goodreads.py
python src/enrich_googlebooks.py
python src/integrate_pipeline.py
```

## ✔️ Estado final del proyecto

- Pipeline funcional completo  
- Normalización y deduplicación correctas  
- Provenance detallado  
- Métricas y aserciones  
- Artefactos en Parquet + documentación  
