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


def _parse_uuid_lenient(value: Any) -> Optional[UUID]:
    """
    Толерантный парсер UUID:
      - принимает UUID, bytes(16), str
      - убирает дефисы, '0x' префикс, пробелы, lowercase
      - возвращает None для невалидных входов (а не исключение)
    """
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        b = bytes(value)
        if len(b) == 16:
            try:
                return UUID(bytes=b)
            except Exception:
                return None
        return None
    if isinstance(value, str):
        s = value.strip().lower()
        if s.startswith("0x"):
            s = s[2:]
        s = s.replace("-", "").replace(" ", "")
        if len(s) != 32:
            return None
        try:
            return UUID(hex=s)
        except Exception:
            return None
    return None


def uuid_to_binary_1c(value: Any) -> Optional[bytes]:
    """
    Конвертация UUID → binary(16) в формате 1С.

    Это ТОЧНАЯ ИНВЕРСИЯ binary_to_uuid:
        bytes b -> UUID u  ==  binary_to_uuid(b) -> u
        UUID  u -> bytes b' == uuid_to_binary_1c(u) -> b'
        round-trip: binary_to_uuid(uuid_to_binary_1c(u)) == u
                    uuid_to_binary_1c(binary_to_uuid(b)) == b

    Используется для построения WHERE-фильтров по списку UUID в MSSQL:
        WHERE _RecorderRRef IN (0x..., 0x..., ...)

    Args:
        value: UUID, bytes(16), либо строка (с дефисами/0x/без — толерантно)

    Returns:
        16 байт в внутреннем порядке 1С, либо None если вход невалиден.
    """
    u = _parse_uuid_lenient(value)
    if u is None:
        return None

    # Логика инверсии binary_to_uuid:
    # Прямая: full_bytes_step = b[8:16][::-1] + b[0:8]
    #         uuid_bytes = full[0:4][::-1] + full[4:6][::-1] + full[6:8][::-1] + full[8:]
    # Обратная:
    #   full[0:4] = uuid_bytes[0:4][::-1]
    #   full[4:6] = uuid_bytes[4:6][::-1]
    #   full[6:8] = uuid_bytes[6:8][::-1]
    #   full[8:]  = uuid_bytes[8:]
    # потом:
    #   b[0:8]  = full[8:16]   (вторая половина full → первая половина b)
    #   b[8:16] = full[0:8][::-1]  (первая половина full реверсированно → вторая половина b)
    ub = u.bytes
    full = (
        ub[0:4][::-1] +
        ub[4:6][::-1] +
        ub[6:8][::-1] +
        ub[8:16]
    )
    b = full[8:16] + full[0:8][::-1]
    return b


def uuid_to_mssql_hex_1c(value: Any) -> Optional[str]:
    """
    UUID → '0x...' литерал для MSSQL binary-сравнения.

    Удобный шорткат для построения WHERE _RecorderRRef IN (0x..., 0x...).
    """
    b = uuid_to_binary_1c(value)
    return binary_to_hex(b) if b is not None else None
