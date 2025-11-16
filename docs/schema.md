
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


## Esquema de Columnas

| Campo | Tipo pandas | Descripción |
|-------|-------------|-------------|
| `book_id` | `object` | Identificador único del libro en el modelo canónico (isbn13 o clave derivada). |
| `titulo` | `object` | Título principal del libro. |
| `titulo_normalizado` | `object` | Título normalizado en minúsculas y sin espacios sobrantes. |
| `autor_principal` | `object` | Autor principal del libro (texto plano). |
| `autores` | `object` | Lista de autores en texto plano (ej: 'Autor1, Autor2'). |
| `editorial` | `object` | Editorial principal del libro. |
| `anio_publicacion` | `float64` | Año de publicación (entero). |
| `fecha_publicacion` | `string` | Fecha de publicación en formato ISO (YYYY-MM-DD). |
| `idioma` | `object` | Idioma normalizado en formato similar a BCP-47 (ej: 'es-ES', 'en-EN'). |
| `isbn10` | `string` | ISBN-10 del libro, si está disponible. |
| `isbn13` | `string` | ISBN-13 del libro, si está disponible. |
| `paginas` | `float64` | Número de páginas. |
| `formato` | `object` | Formato del libro (tapa dura, bolsillo, ebook, etc.). |
| `categoria` | `object` | Categorías o géneros en texto plano. |
| `precio` | `float64` | Precio numérico si viene informado por la fuente. |
| `moneda` | `object` | ISO-4217 (ej: EUR, USD). |
| `rating` | `object` | Valoración media del libro (Goodreads). |
| `ratings_count` | `object` | Número total de valoraciones. |
| `fuente_ganadora` | `object` | Fuente seleccionada tras deduplicación. |
| `ts_ultima_actualizacion` | `object` | Timestamp de la última actualización. |
