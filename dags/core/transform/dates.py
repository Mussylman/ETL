"""
Функции для работы с датами из 1С.

В 1С даты часто хранятся с годом 4000+ (например 4025 вместо 2025).
Эти функции исправляют такие даты.
"""

from typing import Optional, Any, Union
from datetime import datetime, date
import pandas as pd


def fix_year(value: Any, offset: int = 2000) -> Any:
    """
    Исправление года: 4025 → 2025.

    Args:
        value: дата (datetime, date, str)
        offset: сколько вычитать из года (по умолчанию 2000)

    Returns:
        Исправленная дата или исходное значение
    """
    if value is None:
        return None

    # datetime объект
    if isinstance(value, datetime):
        if value.year > 3000:
            try:
                return value.replace(year=value.year - offset)
            except ValueError:
                # Для 29 февраля в невисокосном году
                return datetime(value.year - offset, 3, 1)
        return value

    # date объект
    if isinstance(value, date):
        if value.year > 3000:
            try:
                return value.replace(year=value.year - offset)
            except ValueError:
                return date(value.year - offset, 3, 1)
        return value

    # Строка
    if isinstance(value, str):
        return fix_year_string(value, offset)

    return value


def fix_year_string(value: str, offset: int = 2000) -> str:
    """
    Исправление года в строковой дате.

    Поддерживает форматы:
      - 4025-10-15
      - 4025-10-15T10:30:00
    """
    try:
        parts = value.split("-")
        year = int(parts[0])
        if year > 3000:
            parts[0] = str(year - offset)
            return "-".join(parts)
        return value
    except Exception:
        return value


def parse_1c_date(value: Any) -> Optional[datetime]:
    """
    Парсинг даты из 1С с автоматическим исправлением года.

    Returns:
        datetime или None
    """
    if value is None:
        return None

    # Сначала исправляем год
    fixed = fix_year(value)

    # Если уже datetime — возвращаем
    if isinstance(fixed, datetime):
        return fixed

    # Пытаемся распарсить строку
    if isinstance(fixed, str):
        try:
            return pd.to_datetime(fixed)
        except Exception:
            return None

    return None


def to_date_only(value: Any) -> Optional[date]:
    """Извлекает только дату (без времени)."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    parsed = parse_1c_date(value)
    return parsed.date() if parsed else None


def is_valid_date(value: Any) -> bool:
    """Проверяет, является ли значение валидной датой."""
    try:
        if value is None:
            return False
        if isinstance(value, (datetime, date)):
            return True
        pd.to_datetime(value)
        return True
    except Exception:
        return False
