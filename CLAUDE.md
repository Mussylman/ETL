# CLAUDE.md — Инструкции для Claude Code

## Проект
ETL-платформа: Apache Airflow + FastAPI конфигуратор для загрузки данных из 1С (MSSQL) в PostgreSQL.

## Ключевые пути
- DAGи: `dags/`
- ETL ядро: `dags/core/` (etl_engine.py, etl_core.py, sales_etl.py)
- Трансформации: `dags/core/transform/` (binary.py, dates.py)
- Плагины: `plugins/`
- Конфигуратор: `etl_config_app/` (FastAPI, порт 5555)
- Obsidian vault: `docs/`

## Obsidian Knowledge Vault
При старте сессии прочитай `docs/00-home/index.md` для понимания контекста.

### Структура
- `docs/00-home/` — навигация и приоритеты
- `docs/atlas/` — архитектура, стек, БД, подключения
- `docs/knowledge/integrations/` — каждая интеграция (1С, GFK, Sheets, Telegram)
- `docs/knowledge/decisions/` — решения с обоснованиями
- `docs/knowledge/debugging/` — баги и их решения
- `docs/knowledge/patterns/` — паттерны DAGов и загрузки
- `docs/knowledge/business/` — контекст: кто пользователи, зачем платформа
- `docs/sessions/` — логи сессий разработки
- `docs/inbox/` — необработанные заметки

### Правила
- После завершения сессии — сохрани лог в `docs/sessions/YYYY-MM-DD тема.md`
- При решении бага — создай заметку в `docs/knowledge/debugging/`
- При принятии архитектурного решения — `docs/knowledge/decisions/`
- Названия файлов = утверждения, не категории
- Wiki-ссылки `[[имя заметки]]` между связанными
- Frontmatter с tags и date
- Язык: русский

## Подключения
- PostgreSQL: 10.10.1.142:5432/test (etl_meta + public схемы)
- MSSQL: 10.10.1.61:1433/UPP_JAN (1С данные)
- 1С API: http://192.168.18.224:8090/NikitaBase/hs/meta

## Команды
```bash
# Запуск конфигуратора
cd etl_config_app && uvicorn app:app --host 0.0.0.0 --port 5555 --reload

# Проверка процессов
ps aux | grep uvicorn
```

## Правила разработки
- UI на русском языке
- Трансформации автоматические по MSSQL типам
- Union/Target/Sync — скрытая механика, не показывать пользователю
- Source of truth для колонок — Column Builder
