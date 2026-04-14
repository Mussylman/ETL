from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class SchemaChecker:
    """
    Проверяет структуру таблицы между MSSQL (1С) и Postgres (ETL-слой).
    Работает ТОЛЬКО как проверка — никакого авто-создания столбцов.
    
    Логика:
      - получает имена колонок из MSSQL
      - получает имена колонок из Postgres
      - выводит список новых / отсутствующих полей
    """

    def __init__(self, table_name, src_conn_id="mssql_1c_conn", dst_conn_id="postgre_test_base"):
        self.table_name = table_name
        self.src_conn_id = src_conn_id
        self.dst_conn_id = dst_conn_id

    # --------------------------------------------------------------------
    # 📌 Получение колонок из MSSQL (1С)
    # --------------------------------------------------------------------
    def get_mssql_columns(self):
        hook = MsSqlHook(mssql_conn_id=self.src_conn_id)
        sql = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{self.table_name}'
        """
        df = hook.get_pandas_df(sql)
        return set(df["COLUMN_NAME"].tolist())

    # --------------------------------------------------------------------
    # 📌 Получение колонок из Postgres (целевой таблицы)
    # --------------------------------------------------------------------
    def get_postgres_columns(self):
        hook = PostgresHook(postgres_conn_id=self.dst_conn_id)
        sql = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{self.table_name}'
              AND table_schema = 'public';
        """
        df = hook.get_pandas_df(sql)
        return set(df["column_name"].tolist())

    # --------------------------------------------------------------------
    # 📌 Основная проверка
    # --------------------------------------------------------------------
    def validate_structure(self):
        print(f"📐 Проверка структуры таблицы {self.table_name}...")

        try:
            mssql_cols = self.get_mssql_columns()
            pg_cols = self.get_postgres_columns()
        except Exception as e:
            print(f"❌ Ошибка проверки структуры: {e}")
            return

        # 🔍 Поля, которых нет в Postgres
        missing_in_pg = mssql_cols - pg_cols

        # 🔍 Поля, которых нет в MSSQL (обычно этого не бывает)
        extra_in_pg = pg_cols - mssql_cols

        print(f"📊 MSSQL: {len(mssql_cols)} колонок")
        print(f"📊 Postgres: {len(pg_cols)} колонок")

        if missing_in_pg:
            print("⚠️ ВНИМАНИЕ: В Postgres отсутствуют поля:")
            for col in missing_in_pg:
                print(f"    ➤ {col}")

        if extra_in_pg:
            print("⚠️ ВНИМАНИЕ: В MSSQL нет колонок, которые есть в Postgres:")
            for col in extra_in_pg:
                print(f"    ➤ {col}")

        if not missing_in_pg and not extra_in_pg:
            print("✅ Структура совпадает. Всё хорошо!")
