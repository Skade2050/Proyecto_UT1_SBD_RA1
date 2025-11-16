# Esquema de la tabla dim_book

Tabla canónica de libros, una fila por libro, tras integrar todas las fuentes.

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
| `formato` | `object` | Formato del libro (tapa dura, bolsillo, ebook, etc.) — no siempre disponible. |
| `categoria` | `object` | Categorías o géneros en texto plano. |
| `precio` | `float64` | Precio numérico si viene informado por la fuente. |
| `moneda` | `object` | Moneda de precio en ISO-4217 (ej: EUR, USD). |
| `rating` | `object` | Valoración media del libro (Goodreads). |
| `ratings_count` | `object` | Número de valoraciones (usuarios) en Goodreads. |
| `fuente_ganadora` | `object` | Fuente que ha ganado en la deduplicación (google_books o goodreads). |
| `ts_ultima_actualizacion` | `object` | Marca de tiempo UTC de la última actualización del registro. |
