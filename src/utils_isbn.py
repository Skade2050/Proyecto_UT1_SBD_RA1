"""
Funciones de ayuda para trabajar con ISBN.

Este módulo se usa desde integrate_pipeline.py para:
- Validar ISBN-13.
"""

from typing import Optional


def is_valid_isbn13(isbn: str) -> bool:
    """
    Validador sencillo de ISBN-13:

    - Debe tener 13 dígitos (se ignoran guiones).
    - Se comprueba el dígito de control.
    """
    if not isinstance(isbn, str):
        return False

    digits = isbn.replace("-", "").strip()
    if len(digits) != 13 or not digits.isdigit():
        return False

    total = 0
    for i, d in enumerate(digits[:12]):
        n = int(d)
        if i % 2 == 0:
            total += n
        else:
            total += 3 * n

    check_digit = (10 - (total % 10)) % 10
    return check_digit == int(digits[-1])
