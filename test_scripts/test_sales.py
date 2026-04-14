from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd
from uuid import UUID
from decimal import Decimal
from datetime import datetime

def prepare_value(value):
    if isinstance(value, UUID):
        return str(value)
    elif isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, datetime):
        return value  # datetime нормально адаптируется
    else:
        return value
# Функция для конвертации binary(16) → UUID
def convert_idrref_1c_to_guid(idrref_bytes: bytes) -> UUID:
    if len(idrref_bytes) != 16:
        raise ValueError("Expected 16 bytes")

    part1 = idrref_bytes[8:16][::-1]
    full_bytes = part1 + idrref_bytes[0:8]

    time_low = full_bytes[0:4][::-1]
    time_mid = full_bytes[4:6][::-1]
    time_hi_version = full_bytes[6:8][::-1]
    rest = full_bytes[8:]

    final = time_low + time_mid + time_hi_version + rest
    return UUID(bytes=final)


def bytes_to_hex_string(b: bytes) -> str:
    return '0x' + b.hex().upper()



def process_binary_value(col_name: str, value: bytes) -> any:
    """
    Обрабатывает бинарные значения в зависимости от их длины:
    - 16 байт: UUID
    - 4 байта: int
    - 1 байт: bool
    Остальные возвращаются без изменений.
    """
    try:
        if len(value) == 16:
            return convert_idrref_1c_to_guid(value)
        elif len(value) == 4:
            return int.from_bytes(value, byteorder='little')
        elif len(value) == 1:
            return value != b'\x00'
        else:
            return value
    except Exception:
        return None

def insert_to_postgres_from_dicts(rows: list[dict], table_name: str, pg_conn_id: str):
    """
    Вставляет список словарей в существующую таблицу PostgreSQL.
    UUID -> str, Decimal -> float.
    """
    if not rows:
        print(f"⚠️ Данные пустые, вставка в {table_name} отменена.")
        return

    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    column_names = list(rows[0].keys())

    # Преобразование значений
    values = [
        [prepare_value(row.get(col)) for col in column_names]
        for row in rows
    ]

    # Вставка
    pg_hook.insert_rows(
        table=table_name,
        rows=values,
        target_fields=column_names,
        commit_every=3000
    )

    print(f"✅ Вставлено {len(rows)} строк в таблицу `{table_name}`.")

# Читаемые имена полей
column_map = {
    "_Period": "Период",
    "_RecorderTRef": "Регистратор_ТипСсылки",
    "_RecorderRRef": "Регистратор",
    "_LineNo": "НомерСтроки",
    "_Active": "Активность",
    "_Fld17845RRef": "Номенклатура",
    "_Fld17846RRef": "ХарактеристикаНоменклатуры",
    "_Fld17847_TYPE": "ЗаказПокупателя_ТипЗначения",
    "_Fld17847_RTRef": "ЗаказПокупателя_ТипСсылки",
    "_Fld17847_RRRef": "ЗаказПокупателя",
    "_Fld17848RRef": "ДоговорКонтрагента",
    "_Fld17849_TYPE": "ДокументПродажи_ТипЗначения",
    "_Fld17849_RTRef": "ДокументПродажи_ТипСсылки",
    "_Fld17849_RRRef": "ДокументПродажи",
    "_Fld17850RRef": "Подразделение",
    "_Fld17851RRef": "Проект",
    "_Fld17852RRef": "Организация",
    "_Fld17853RRef": "Контрагент",
    "_Fld24672RRef": "Ответственный",
    "_Fld17854": "Количество",
    "_Fld17855": "Стоимость",
    "_Fld17856": "СтоимостьБезСкидок",
    "_Fld17857": "НДС",
    "_Fld17858": "Акциз"
}

# Подключение к MSSQL
hook = MsSqlHook(mssql_conn_id="mssql_1c_conn")

# 1. Получение типов данных из схемы
type_query = """
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '_AccumRg17844'
"""

type_df = hook.get_pandas_df(type_query)

binary_columns = type_df[type_df["DATA_TYPE"].isin(["binary", "varbinary"])]["COLUMN_NAME"].tolist()

# 2. Получение данных
sql = """
SELECT  *
FROM [UPP_JAN].[dbo]._AccumRg17844
WHERE _Period >= '4025-11-01'
"""

start_time = datetime.now()

with hook.get_conn() as conn:
    cursor = conn.cursor()
    cursor.execute(sql)

    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

duration = (datetime.now() - start_time).total_seconds()
print(f"⏱ Запрос выполнен за {duration:.2f} сек, строк: {len(rows)}")




# 3. Преобразование строк
loaded_at = datetime.utcnow()

result = []
hex_columns = set()

for row in rows:

    row_dict = {}
    binary_hex_list = [] 

    for col_name, value in zip(columns, row):
        readable_name = column_map.get(col_name, col_name)

        # Обработка бинарных колонок
        if col_name in binary_columns and isinstance(value, bytes):
            row_dict[readable_name] = process_binary_value(col_name, value)

            # Добавляем hex-версию
            hex_value = bytes_to_hex_string(value)
            binary_hex_list.append(f"{readable_name}_hex: {hex_value}")

        else:
            # Обработка "Периода"
            if readable_name == "Период" and isinstance(value, datetime):
                try:
                    value = value.replace(year=value.year - 2000)
                except ValueError:
                    value = value.replace(month=3, day=1, year=value.year - 2000)

            row_dict[readable_name] = value

    row_dict["etl_loaded_at"] = loaded_at
     # Добавление hex-значений в одно поле
    row_dict["binary_fields_raw"] = binary_hex_list

    result.append(row_dict)


# for key, value in result[0].items():
#     print(f"{key}: {type(value)}")

# 4. Финальная таблица
# df = pd.DataFrame(result)
# print(df.dtypes)
# Сохраняем в файл
#df.to_csv("output.csv", index=False, encoding="utf-8-sig")


insert_to_postgres_from_dicts(result, table_name="sales_register", pg_conn_id="postgre_test_base")

# print(df.head())
