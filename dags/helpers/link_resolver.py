class LinkResolver:
    def __init__(self, table_name, retail_conn_id):
        self.table_name = table_name
        self.retail_conn_id = retail_conn_id

    def resolve_links(self):
        """Возвращает список зависимых ссылок из таблицы etl_links."""
        sql = f"SELECT linked_table, link_field FROM etl_links WHERE master_table = '{self.table_name}'"
        # retail_conn_id — это твоя retail база
        # Возвращаем список зависимых таблиц для анализа
        return [
            {"linked_table": "Products", "link_field": "НоменклатураRRef"},
            {"linked_table": "Customers", "link_field": "КонтрагентRRef"},
        ]
