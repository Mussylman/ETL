"""
Вычисляемые колонки (Custom Python Transforms).

Каждая функция принимает row (dict) и возвращает значение.
Новые функции добавляются в CUSTOM_TRANSFORMS.

Стандарт имён колонок (целевая схема):
    dim_sales:
        date, document_number, document, operation_type_id,
        document_basis, orders_id, division_id, partner_id,
        coupon_number, price_type_id, document_responsible

    fact_sales_products:
        sales_id, product_id, quality_id, quantity,
        sales_with_vat, sales_without_vat, sales_without_discounts,
        earned_bonuses, used_bonuses, warehouse_id, seller_id

    Промежуточные (для вычислений, не хранятся):
        cost (Стоимость), vat (НДС),
        bonuses (Эврика_Бонусы), used_bonuses_raw (Эврика_Списанные),
        send_to_abm (ОтправитьВАБМ), bonus_sum (ЭВРИКА_СуммаБонусами)
"""

from typing import Any, Dict


# ═══════════════════════════════════════════════════════════
#  Хелперы
# ═══════════════════════════════════════════════════════════

def _doc_sign(row: dict) -> int:
    """Знак по типу документа: возврат → -1, продажа → +1, иначе 0."""
    rec_type = str(row.get("recorder_type", "") or "")
    op_type = str(row.get("operation_type", "") or "")

    if "Возврат" in rec_type:
        return -1
    elif "Реализация" in rec_type:
        return 1
    elif "ЧекККМ" in rec_type:
        return -1 if "Возврат" in op_type else 1
    return 0


# ═══════════════════════════════════════════════════════════
#  Бизнес-функции — fact_sales_products
# ═══════════════════════════════════════════════════════════

def sales_with_vat(row: dict) -> Any:
    """Стоимость с НДС 12%. Если НДС > 0 — берём как есть, иначе × 1.12."""
    cost = float(row.get("cost", 0) or 0)
    vat = float(row.get("vat", 0) or 0)
    return cost if vat > 0 else cost * 1.12


def sales_without_vat(row: dict) -> Any:
    """
    Стоимость без НДС.
    После 2022-10-12 при ОтправитьВАБМ и наличии бонусов —
    корректировка через списанные бонусы и пересчёт НДС.
    """
    from datetime import datetime

    cost = float(row.get("cost", 0) or 0)
    vat = float(row.get("vat", 0) or 0)
    period = row.get("period")
    send_to_abm = row.get("send_to_abm")
    bonus_sum = float(row.get("bonus_sum", 0) or 0)

    cutoff = datetime(2022, 10, 12)
    is_after_cutoff = False
    if period:
        if isinstance(period, datetime):
            is_after_cutoff = period >= cutoff
        elif isinstance(period, str):
            try:
                is_after_cutoff = datetime.fromisoformat(str(period)) >= cutoff
            except (ValueError, TypeError):
                pass

    if is_after_cutoff and send_to_abm and bonus_sum > 0:
        sign = _doc_sign(row)
        used = float(row.get("used_bonuses_raw", 0) or 0)
        swd = float(row.get("sales_without_discounts", 0) or 0)
        vat_calc = round(swd / 1.12 * 0.12, 2)
        return sign * used + cost - vat_calc

    return cost - vat


def earned_bonuses(row: dict) -> Any:
    """Начисленные бонусы с учётом типа документа."""
    return _doc_sign(row) * float(row.get("bonuses", 0) or 0)


def used_bonuses(row: dict) -> Any:
    """Списанные бонусы с учётом типа документа."""
    return _doc_sign(row) * float(row.get("used_bonuses_raw", 0) or 0)


# ═══════════════════════════════════════════════════════════
#  Дополнительные функции
# ═══════════════════════════════════════════════════════════

def margin(row: dict) -> Any:
    """Маржа = Стоимость - СтоимостьБезСкидок."""
    cost = float(row.get("cost", 0) or 0)
    swd = float(row.get("sales_without_discounts", 0) or 0)
    return cost - swd


def unit_price(row: dict) -> Any:
    """Цена за единицу = Стоимость / Количество."""
    cost = float(row.get("cost", 0) or 0)
    qty = float(row.get("quantity", 0) or 0)
    return cost / qty if qty != 0 else 0


def discount_percent(row: dict) -> Any:
    """Процент скидки = (СтоимостьБезСкидок - Стоимость) / СтоимостьБезСкидок × 100."""
    cost = float(row.get("cost", 0) or 0)
    swd = float(row.get("sales_without_discounts", 0) or 0)
    return ((swd - cost) / swd * 100) if swd != 0 else 0


# ═══════════════════════════════════════════════════════════
#  Реестр
# ═══════════════════════════════════════════════════════════

CUSTOM_TRANSFORMS: Dict[str, dict] = {
    "sales_with_vat": {
        "function": sales_with_vat,
        "description": "Стоимость с НДС 12%",
        "target_type": "float",
        "uses_columns": ["cost", "vat"],
    },
    "sales_without_vat": {
        "function": sales_without_vat,
        "description": "Стоимость без НДС (с корректировкой бонусов после 12.10.2022)",
        "target_type": "float",
        "uses_columns": ["cost", "vat", "sales_without_discounts", "used_bonuses_raw",
                         "period", "send_to_abm", "bonus_sum",
                         "recorder_type", "operation_type"],
    },
    "earned_bonuses": {
        "function": earned_bonuses,
        "description": "Начисленные бонусы (возврат → минус, продажа → плюс)",
        "target_type": "float",
        "uses_columns": ["recorder_type", "bonuses", "operation_type"],
    },
    "used_bonuses": {
        "function": used_bonuses,
        "description": "Списанные бонусы (возврат → минус, продажа → плюс)",
        "target_type": "float",
        "uses_columns": ["recorder_type", "used_bonuses_raw", "operation_type"],
    },
    "margin": {
        "function": margin,
        "description": "Маржа (стоимость - стоимость без скидок)",
        "target_type": "float",
        "uses_columns": ["cost", "sales_without_discounts"],
    },
    "unit_price": {
        "function": unit_price,
        "description": "Цена за единицу",
        "target_type": "float",
        "uses_columns": ["cost", "quantity"],
    },
    "discount_percent": {
        "function": discount_percent,
        "description": "Процент скидки",
        "target_type": "float",
        "uses_columns": ["cost", "sales_without_discounts"],
    },
}


def apply_custom(func_name: str, row: dict) -> Any:
    """Вызов custom transform по имени."""
    entry = CUSTOM_TRANSFORMS.get(func_name)
    if not entry:
        raise ValueError(f"Unknown custom transform: {func_name}")
    return entry["function"](row)


def get_registry() -> list:
    """Список доступных функций для API/UI."""
    return [
        {
            "name": name,
            "description": info["description"],
            "target_type": info["target_type"],
            "uses_columns": info["uses_columns"],
        }
        for name, info in CUSTOM_TRANSFORMS.items()
    ]
