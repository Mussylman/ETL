"""
Transform module - функции трансформации данных.

Модули:
  - binary: работа с binary-данными из 1С
  - dates:  работа с датами (исправление года 4025→2025)
  - cast:   приведение типов
"""

from .transform_utils import TransformUtils

# Binary functions
from .binary import (
    binary_to_uuid,
    binary_to_int,
    binary_to_int_le,
    binary_to_int_be,
    binary_to_bool,
    process_binary_auto,
    binary_to_hex,
)

# Date functions
from .dates import (
    fix_year,
    fix_year_string,
    parse_1c_date,
    to_date_only,
    is_valid_date,
)

# Cast functions
from .cast import (
    to_int,
    to_float,
    to_decimal,
    to_str,
    to_bool,
    cast,
)


__all__ = [
    # Main class
    "TransformUtils",

    # Binary
    "binary_to_uuid",
    "binary_to_int",
    "binary_to_int_le",
    "binary_to_int_be",
    "binary_to_bool",
    "process_binary_auto",
    "binary_to_hex",

    # Dates
    "fix_year",
    "fix_year_string",
    "parse_1c_date",
    "to_date_only",
    "is_valid_date",

    # Cast
    "to_int",
    "to_float",
    "to_decimal",
    "to_str",
    "to_bool",
    "cast",
]
