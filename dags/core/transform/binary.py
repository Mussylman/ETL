"""
Функции для работы с binary-данными из 1С.

binary(16) → UUID (формат 1С)
binary(4)  → int
binary(1)  → bool
"""

from typing import Optional, Any
from uuid import UUID
import binascii


def binary_to_uuid(b: bytes) -> Optional[UUID]:
    """
    Конвертация binary(16) → UUID (формат 1С).

    1С хранит UUID в специфическом порядке байтов,
    эта функция корректно преобразует его в стандартный UUID.
    """
    try:
        if not isinstance(b, (bytes, bytearray, memoryview)) or len(b) != 16:
            return None

        b = bytes(b)
        part1 = b[8:16][::-1]
        full_bytes = part1 + b[0:8]

        return UUID(bytes=(
            full_bytes[0:4][::-1] +
            full_bytes[4:6][::-1] +
            full_bytes[6:8][::-1] +
            full_bytes[8:]
        ))
    except Exception:
        return None


def binary_to_int(b: bytes, byteorder: str = "big") -> Optional[int]:
    """
    Конвертация binary(4) → int.

    Args:
        b: байты
        byteorder: 'big' (для 1С TRef) или 'little'

    Note:
        1С использует big-endian для _RecorderTRef, _RTRef и т.д.
    """
    try:
        if isinstance(b, (bytes, bytearray, memoryview)):
            b = bytes(b)
            if len(b) == 4:
                return int.from_bytes(b, byteorder)
        return b if not isinstance(b, bytes) else None
    except Exception:
        return None


def binary_to_int_le(b: bytes) -> Optional[int]:
    """Конвертация binary(4) → int (little-endian)."""
    return binary_to_int(b, "little")


def binary_to_int_be(b: bytes) -> Optional[int]:
    """Конвертация binary(4) → int (big-endian, для 1С TRef)."""
    return binary_to_int(b, "big")


def binary_to_bool(b: bytes) -> Optional[bool]:
    """Конвертация binary(1) → bool."""
    try:
        if isinstance(b, (bytes, bytearray, memoryview)):
            b = bytes(b)
            if len(b) == 1:
                return b != b"\x00"
        return b if not isinstance(b, bytes) else None
    except Exception:
        return None


def process_binary_auto(value: Any) -> Any:
    """
    Автоматическая конвертация binary по длине:
      - 16 байт → UUID
      - 4 байта → int
      - 1 байт  → bool
    """
    if not isinstance(value, (bytes, bytearray, memoryview)):
        return value

    b = bytes(value)

    if len(b) == 16:
        return binary_to_uuid(b)
    elif len(b) == 4:
        return binary_to_int(b)
    elif len(b) == 1:
        return binary_to_bool(b)

    return value


def binary_to_hex(value: Any) -> Optional[str]:
    """Конвертация binary → hex-строка (0xABCD...)."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "0x" + binascii.hexlify(bytes(value)).decode().upper()
    return None
