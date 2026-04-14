"""
Функции приведения типов.
"""

from typing import Any, Optional
from decimal import Decimal, InvalidOperation


def to_int(value: Any) -> Optional[int]:
    """Приведение к int."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def to_float(value: Any) -> Optional[float]:
    """Приведение к float."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def to_decimal(value: Any, precision: int = 2) -> Optional[Decimal]:
    """Приведение к Decimal с заданной точностью."""
    if value is None:
        return None
    try:
        d = Decimal(str(value))
        return round(d, precision)
    except (InvalidOperation, ValueError, TypeError):
        return None


def to_str(value: Any) -> Optional[str]:
    """Приведение к строке."""
    if value is None:
        return None
    try:
        return str(value)
    except (ValueError, TypeError):
        return None


def to_bool(value: Any) -> Optional[bool]:
    """
    Приведение к bool.

    Понимает:
      - True/False
      - 1/0
      - 'true'/'false', 'yes'/'no', 'да'/'нет'
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    if isinstance(value, str):
        lower = value.lower().strip()
        if lower in ('true', 'yes', '1', 'да', 't', 'y'):
            return True
        if lower in ('false', 'no', '0', 'нет', 'f', 'n'):
            return False

    return None


def cast(value: Any, target_type: str) -> Any:
    """
    Универсальное приведение типа.

    Args:
        value: значение
        target_type: 'int', 'float', 'decimal', 'str', 'bool'

    Returns:
        Приведённое значение или None
    """
    casters = {
        'int': to_int,
        'float': to_float,
        'decimal': to_decimal,
        'str': to_str,
        'bool': to_bool,
    }

    caster = casters.get(target_type)
    if caster:
        return caster(value)

    return value
