"""
Универсальный ETL-движок на основе конфигурации из PostgreSQL.

Использование:
    from core.etl_engine import ETLEngine

    # Полная загрузка
    etl = ETLEngine(
        register_code="sales_register",
        mode="full_period",
        start_date="4025-10-01",
        end_date="4025-11-01"
    )
    etl.run()

    # Инкрементальная загрузка
    etl = ETLEngine(register_code="sales_register", mode="incremental")
    etl.run()
"""

from typing import Any, Optional, List, Dict
from datetime import datetime
from zoneinfo import ZoneInfo

# TZ всех ETL audit-полей. Бизнес-дата `period` уже в Asia/Almaty (offset из 1С),
# держим всё в одной TZ для согласованности (см. docs/sales_load_modes.md).
ETL_TZ = ZoneInfo("Asia/Almaty")

def _format_error(e: Exception, limit: int = 2000) -> str:
    """
    Компактное сообщение об ошибке для load_history.error_message.

    Проблема: исключения драйвера выглядят как
    "Execution failed on sql '<2 КБ SQL-текста>': <настоящая причина>" —
    при простом обрезании до limit причина терялась и падения приходилось
    диагностировать по логам Airflow (кейс 2026-07-30: 19 deadlock-ов,
    причина не сохранилась ни в одной строке load_history).

    Решение: тип исключения и ХВОСТ сообщения (где причина от драйвера)
    ставим в начало, голову оставляем как контекст в остатке лимита.
    """
    msg = str(e) or e.__class__.__name__
    head = f"{e.__class__.__name__}: "

    # Вырезаем тело SQL: "Execution failed on sql '<SQL>': <причина>".
    # Берём ПОСЛЕДНЕЕ "': " — после него у драйвера идёт настоящая причина.
    marker = "': "
    if msg.startswith("Execution failed on sql") and marker in msg:
        cut = msg.rfind(marker)
        cause = msg[cut + len(marker):].strip()
        sql_head = msg[len("Execution failed on sql '"):][:120].replace("\n", " ")
        msg = f"{cause}  [sql ~{cut} симв., начало: {sql_head}…]"

    if len(msg) <= limit - len(head):
        return head + msg
    return head + msg[: limit - len(head) - 1] + "…"


def _now_local():
    """now() в Asia/Almaty, без tzinfo — для записи в timestamp WITHOUT time zone."""
    return datetime.now(ETL_TZ).replace(tzinfo=None)

from .config import ConfigLoader, RegisterConfig, TargetConfig
from .builder import QueryBuilder
from .extract.storage_connector import StorageConnector
from .extract.data_checker import DataChecker
from .transform.transform_utils import TransformUtils
from .load.loaders import Loaders



RAW_REFS_PREFIX = "raw_refs."
_EMPTY_REF = "00000000-0000-0000-0000-000000000000"


def _pack_raw_refs(df: "pd.DataFrame") -> "pd.DataFrame":
    """
    Колонки мэппингов вида raw_refs.<key>[.<sub>] → одна JSONB-колонка raw_refs.

    Стандарт ссылок (2026-09-03): исходные GUID/UID из 1С не живут отдельными
    физическими колонками — только в raw_refs для диагностики и повторного
    resolve. Справочник: raw_refs.kontragent → {"kontragent": "<guid>"};
    полиморфная/документная ссылка: raw_refs.doc_sale.uid + raw_refs.doc_sale.type
    → {"doc_sale": {"uid": ..., "type": 476}}. Пустая ссылка 1С (0000…), NULL и
    NaN в JSON не пишутся. Если таких колонок нет — df не меняется (TEST-контур
    со старыми мэппингами работает как раньше).
    """
    cols = [c for c in df.columns if isinstance(c, str) and c.startswith(RAW_REFS_PREFIX)]
    if not cols:
        return df
    import math
    from uuid import UUID

    def _clean(v):
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        if isinstance(v, UUID):
            v = str(v)
        if isinstance(v, str):
            v = v.strip()
            if not v or v == _EMPTY_REF:
                return None
            return v
        if hasattr(v, "item"):          # numpy scalar → python
            v = v.item()
        return v

    paths = [c[len(RAW_REFS_PREFIX):].split(".") for c in cols]
    packed = []
    for row in df[cols].itertuples(index=False, name=None):
        obj: Dict[str, Any] = {}
        for path, val in zip(paths, row):
            val = _clean(val)
            if val is None:
                continue
            node = obj
            for k in path[:-1]:
                node = node.setdefault(k, {})
            node[path[-1]] = val
        # полиморфная ссылка без uid (только type) — мусор, не пишем
        obj = {k: v for k, v in obj.items() if not (isinstance(v, dict) and "uid" not in v)}
        packed.append(obj)
    df = df.drop(columns=cols)
    df["raw_refs"] = packed
    return df


class ETLEngine:
    """
    Универсальный ETL-движок.

    Работает на основе конфигурации из PostgreSQL (схема etl_meta):
      1. Загружает конфигурацию регистра
      2. Генерирует SQL-запросы (с JOIN и UNION)
      3. Извлекает данные из MSSQL
      4. Трансформирует (binary->UUID, даты, маппинг)
      5. Загружает в одну или несколько целевых таблиц

    Поддерживаемые режимы:
      - full_period  : полная загрузка по периоду
      - incremental  : инкрементальная загрузка по изменениям
      - consistency  : проверка консистентности
    """

    def __init__(
        self,
        register_code: str,
        mode: Optional[str] = None,

        # Connections
        config_conn_id: str = "postgre_test_base",
        src_conn_id: str = "mssql_1c_conn",
        dst_conn_id: str = "postgre_test_base",
        retail_conn_id: str = "bd_retail",

        # Database
        database: str = "UPP_JAN",

        # For full_period mode
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,

        # For specific targets
        target_tables: Optional[List[str]] = None,
    ):
        self.register_code = register_code
        self.start_date = start_date
        self.end_date = end_date
        self.target_tables = target_tables

        # Connection IDs
        self.config_conn_id = config_conn_id
        self.src_conn_id = src_conn_id
        self.dst_conn_id = dst_conn_id
        self.retail_conn_id = retail_conn_id

        # Load config
        self.config_loader = ConfigLoader(config_conn_id)
        self.config: RegisterConfig = self.config_loader.load_register(register_code)

        # Override mode if specified
        self.mode = mode or self.config.default_mode

        # Initialize components
        self.query_builder = QueryBuilder(database=database)
        self.storage = StorageConnector(src_conn_id=src_conn_id, database=database)
        self.transform = TransformUtils(pg_conn_id=dst_conn_id)
        self.loader = Loaders(dst_conn_id=dst_conn_id)

    def run(self) -> Dict[str, int]:
        """
        Главная точка входа.

        Returns:
            Словарь {target_table: rows_loaded}
        """
        print(f"ETL START: register={self.register_code}, mode={self.mode}")
        started_at = datetime.now()

        results = {}

        if self.mode in ("full_period", "full"):
            # full — полная выгрузка БЕЗ периода (справочники и т.п.);
            # full_period — как раньше, период обязателен
            results = self._run_full_period()
        elif self.mode in ("incremental", "consistency"):
            results = self._run_incremental()
        else:
            raise ValueError(f"Unknown mode: {self.mode}")

        elapsed = (datetime.now() - started_at).total_seconds()
        total_rows = sum(results.values())
        print(f"ETL DONE: {self.register_code} | {total_rows} rows | {elapsed:.1f}s")

        return results

    def _get_active_targets(self) -> List[TargetConfig]:
        """Возвращает список активных целевых таблиц, отсортированных по приоритету."""
        targets = [t for t in self.config.targets if t.is_active]

        if self.target_tables:
            targets = [t for t in targets if t.target_table in self.target_tables]

        targets.sort(key=lambda t: t.priority)
        return targets

    # ======================================================================
    #  FULL PERIOD MODE
    # ======================================================================
    def _run_full_period(self) -> Dict[str, int]:
        """
        Полная загрузка по периоду + load_history + post-load validation.

        Контур:
          1. INSERT load_history(run_mode=full_period, status=running)
          2. retail snapshot → updated_at для dim
          3. Прогон targets, считаем total rows
          4. _validate_full_period_load() — counts/null/duplicates/FK
          5. UPDATE load_history(success/failed, error_message)
          6. На любой ошибке — status=failed + raise (DAG увидит failure)
        """
        if self.mode == "full_period" and (not self.start_date or not self.end_date):
            raise ValueError("start_date and end_date required for full_period mode")

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_meta = PostgresHook(postgres_conn_id=self.config_conn_id)

        run_id = self._open_history_run(pg_meta, run_mode="full_period")
        results: Dict[str, int] = {}
        total_rows = 0
        # Базовый checkpoint — заполнится позже точным snapshot_ts.
        checkpoint = f"{self.start_date or 'NULL'}..{self.end_date or 'NULL'}"

        try:
            # Снимок retail ДО запроса в MSSQL.
            # Это retail_snapshot_at для всех строк этого full_period —
            # см. docs/sales_load_modes.md.
            snapshot_ts = self._get_retail_snapshot()
            print(f"Full-period retail snapshot: {snapshot_ts}")
            checkpoint = (
                f"{self.start_date or 'NULL'}..{self.end_date or 'NULL'}; "
                f"retail_snapshot_at={snapshot_ts}"
            )

            for target in self._get_active_targets():
                rows = self._process_target(
                    target=target,
                    period_start=self.start_date,
                    period_end=self.end_date,
                    snapshot_ts=snapshot_ts,
                )
                results[target.target_table] = rows
                total_rows += rows

            # Post-load validation
            self._validate_full_period_load()

            self._close_history_run(
                pg_meta, run_id, status="success",
                checkpoint=checkpoint,
                rows_loaded=total_rows,
            )
            return results

        except Exception as e:
            err = _format_error(e)
            try:
                self._close_history_run(
                    pg_meta, run_id, status="failed",
                    checkpoint=checkpoint,
                    rows_loaded=total_rows,
                    error=err,
                )
            except Exception as close_err:
                print(f"WARN: не смогли записать status=failed в load_history: {close_err}")
            raise

    def _validate_full_period_load(self) -> None:
        """
        Минимальный набор проверок целостности после загрузки.
        Бросает RuntimeError при любом нарушении — caller пишет failed в history.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg = PostgresHook(postgres_conn_id=self.dst_conn_id)

        def _scalar(sql: str) -> int:
            row = pg.get_first(sql)
            return int(row[0]) if row and row[0] is not None else 0

        targets = self._get_active_targets()
        dim = next((t for t in targets if t.target_role == "dimension"), None)
        fact = next((t for t in targets if t.target_role == "fact"), None)

        errors = []

        # 1. counts > 0
        for t in targets:
            n = _scalar(f"SELECT COUNT(*) FROM {t.full_table_name}")
            print(f"  validation: {t.full_table_name} rows={n}")
            if n == 0:
                errors.append(f"{t.full_table_name}: 0 rows (ожидаем > 0)")

        # 2. NULL checks
        def _check_not_null(tbl: str, col: str, include: List[str]):
            if col not in (include or []):
                return
            n = _scalar(f"SELECT COUNT(*) FROM public.{tbl} WHERE {col} IS NULL")
            if n > 0:
                errors.append(f"{tbl}.{col} IS NULL = {n}")

        if dim:
            _check_not_null(dim.target_table, "recorder_type", dim.include_columns)
        if fact:
            _check_not_null(fact.target_table, "recorder_type", fact.include_columns)
            _check_not_null(fact.target_table, "line_no", fact.include_columns)
            # FK column на шапку — заполняется post_load_sql. Имя по конвенции
            # {dim_table}_id (sales → sales_id); для таблиц во множественном числе
            # допускаем единственное (orders → order_id). Берём первую существующую.
            if dim:
                candidates = [f"{dim.target_table}_id"]
                if dim.target_table.endswith("s"):
                    candidates.append(f"{dim.target_table[:-1]}_id")
                fk_col = next(
                    (c for c in candidates if _scalar(
                        f"SELECT COUNT(*) FROM information_schema.columns "
                        f"WHERE table_schema='public' AND table_name='{fact.target_table}' "
                        f"AND column_name='{c}'")),
                    None,
                )
                n = _scalar(
                    f"SELECT COUNT(*) FROM public.{fact.target_table} "
                    f"WHERE {fk_col} IS NULL"
                ) if fk_col else 0
                if fk_col is None:
                    errors.append(
                        f"{fact.target_table}: нет FK-колонки на {dim.target_table} "
                        f"(ожидалась одна из {candidates})"
                    )
                if n > 0:
                    errors.append(
                        f"{fact.target_table}.{fk_col} IS NULL = {n} "
                        f"(post_load_sql FK resolve не отработал?)"
                    )

        # dim-FK колонки (guid→id): каждая ссылка на справочник, пришедшая из 1С,
        # должна быть разрезолвлена post_load-ом в <x>_id (stub гарантирует id).
        # Стандарт ссылок (2026-09-03): исходный guid лежит в raw_refs->>'<x>',
        # отдельной uuid-колонки нет. Проверяем только СПРАВОЧНИКИ — пары, у
        # которых существует public.dim_<x>; ссылка на регистр/документ
        # (doc_sale_id, позже zakaz_id) stub'ом не закрывается, её NULL законен.
        # Таблица без raw_refs (старый контур TEST) — прежнее правило по парам
        # (<x>_uid uuid, <x>_id).
        for t in targets:
            has_raw = _scalar(
                f"SELECT COUNT(*) FROM information_schema.columns "
                f"WHERE table_schema='public' AND table_name='{t.target_table}' "
                f"AND column_name='raw_refs'"
            )
            if has_raw:
                fk_cols = pg.get_records(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name=%s "
                    "  AND column_name LIKE '%%\\_id' AND column_name NOT IN ('id','sales_id') "
                    "  AND to_regclass('public.dim_' || left(column_name, -3)) IS NOT NULL",
                    parameters=(t.target_table,),
                )
                for (fk_col,) in fk_cols:
                    key = fk_col[:-3]
                    n = _scalar(
                        f"SELECT COUNT(*) FROM public.{t.target_table} "
                        f"WHERE {fk_col} IS NULL AND raw_refs ? '{key}'"
                    )
                    if n > 0:
                        errors.append(
                            f"{t.target_table}.{fk_col} IS NULL = {n} "
                            f"при raw_refs->>'{key}' (stub-резолв в post_load_sql не отработал?)"
                        )
                continue
            fk_pairs = pg.get_records(
                "SELECT a.column_name, "
                "       regexp_replace(a.column_name, '_uid$', '') || '_id' AS fk "
                "FROM information_schema.columns a "
                "JOIN information_schema.columns b "
                "  ON b.table_schema = a.table_schema "
                " AND b.table_name  = a.table_name "
                " AND b.column_name = regexp_replace(a.column_name, '_uid$', '') || '_id' "
                "WHERE a.table_schema = 'public' "
                "  AND a.table_name = %s AND a.data_type = 'uuid' "
                "  AND to_regclass('public.dim_' || regexp_replace(a.column_name, '_uid$', '')) IS NOT NULL",
                parameters=(t.target_table,),
            )
            for (guid_col, fk_col) in fk_pairs:
                n = _scalar(
                    f"SELECT COUNT(*) FROM public.{t.target_table} "
                    f"WHERE {fk_col} IS NULL "
                    f"  AND {guid_col} IS NOT NULL "
                    f"  AND {guid_col} <> '00000000-0000-0000-0000-000000000000'::uuid"
                )
                if n > 0:
                    errors.append(
                        f"{t.target_table}.{fk_col} IS NULL = {n} "
                        f"при валидном {guid_col} (stub-резолв в post_load_sql не отработал?)"
                    )

        # ETL audit-поля (см. docs/sales_load_modes.md):
        #   retail_snapshot_at — обязателен после full_period
        #   etl_updated_at     — обязателен после любой загрузки
        #   retail_updated_at  — может быть NULL после full_period (OK), но
        #                         после incremental все обработанные строки
        #                         должны иметь значение (это проверяется в
        #                         _run_incremental отдельно)
        for t in targets:
            for col in ("retail_snapshot_at", "etl_updated_at"):
                # Колонки могут отсутствовать в DDL у старых register-ов — мягко
                exists = _scalar(
                    f"SELECT COUNT(*) FROM information_schema.columns "
                    f"WHERE table_schema='public' AND table_name='{t.target_table}' "
                    f"AND column_name='{col}'"
                )
                if not exists:
                    continue
                n = _scalar(f"SELECT COUNT(*) FROM {t.full_table_name} WHERE {col} IS NULL")
                if n > 0:
                    errors.append(f"{t.full_table_name}.{col} IS NULL = {n}")

        # 3. Duplicates по upsert_keys
        for t in targets:
            if not t.upsert_keys:
                continue
            cols = ", ".join(t.upsert_keys)
            n = _scalar(
                f"SELECT COUNT(*) FROM ("
                f"  SELECT 1 FROM {t.full_table_name} GROUP BY {cols} HAVING COUNT(*) > 1"
                f") d"
            )
            if n > 0:
                errors.append(f"{t.full_table_name}: дубли по ({cols}) = {n} групп")

        if errors:
            msg = "POST-LOAD VALIDATION FAILED:\n  - " + "\n  - ".join(errors)
            print(msg)
            raise RuntimeError(msg)

        print(f"POST-LOAD VALIDATION PASSED ({len(targets)} targets)")

    def _get_retail_snapshot(self) -> "datetime":
        """
        SELECT COALESCE(MAX(updated_at), '1970-01-01'::timestamp) FROM bd_retail.{table}.

        Если retail_table не настроен в регистре — fallback на 1970-01-01,
        чтобы NULL не ломал инкремент.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from datetime import datetime
        if not self.config.retail_table:
            print("retail_table не задан в регистре — snapshot=1970-01-01")
            return datetime(1970, 1, 1)
        pg = PostgresHook(postgres_conn_id=self.retail_conn_id)
        # retail.updated_at в UTC — конвертируем в Almaty чтобы retail_snapshot_at
        # был в одной TZ с остальными нашими аудит-полями.
        rows = pg.get_records(
            f"SELECT COALESCE("
            f"  MAX((updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp), "
            f"  '1970-01-01'::timestamp"
            f") FROM public.{self.config.retail_table}"
        )
        ts = rows[0][0] if rows and rows[0][0] else datetime(1970, 1, 1)
        return ts

    # ======================================================================
    #  INCREMENTAL MODE
    # ======================================================================
    def _run_incremental(self) -> Dict[str, int]:
        """
        Инкрементальная загрузка по изменениям.

        Транзакционный контур (на уровне load_history):
          • открытие: INSERT load_history(status='running', started_at=NOW())
          • захват pg_advisory_lock per-register (защита от параллельных прогонов)
          • DataChecker: окно [from_ts, to_ts) — read-skew guard внутри
          • для каждого target: extract → transform → split → load
          • missing-обработка: changed_uids − returned_recorders → DELETE из dim/fact
          • watermark двигается ДО to_ts (не до MAX) — окно закрывается даже на пустой пачке
          • закрытие: UPDATE load_history(status='success'|'failed', checkpoint_value=to_ts)
        """
        if not self.config.retail_table or not self.config.retail_uid_column:
            raise ValueError(
                "retail_table and retail_uid_column required for incremental mode"
            )

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_meta = PostgresHook(postgres_conn_id=self.config_conn_id)

        # 1. Открыть load_history
        run_id = self._open_history_run(pg_meta, run_mode="incremental")
        run_started_at = _now_local()   # для очистки неполных шапок на пути ошибки
        changed_uids: List[str] = []

        try:
            with self._advisory_lock(pg_meta, self.config.id):
                # 2. Окно изменений (DataChecker сам делает COALESCE на 1970)
                checker = DataChecker(
                    retail_table=self.config.retail_table,
                    retail_conn_id=self.retail_conn_id,
                    config_conn_id=self.config_conn_id,
                    register_id=self.config.id,
                    key_column=self.config.retail_uid_column,
                    # Источник watermark — dim-таблица ЭТОГО регистра
                    # (MAX(updated_at)); fallback на public.sales — поведение
                    # до этапа 0.2, когда имя было захардкожено.
                    etl_table=self._get_watermark_table(),
                    etl_conn_id=self.dst_conn_id,
                )
                changed_df, from_ts, to_ts = checker.get_changed_uids()

                if changed_df.empty:
                    print(f"No changes in window [{from_ts}, {to_ts}) — advance watermark only")
                    self._close_history_run(
                        pg_meta, run_id,
                        status="success",
                        checkpoint=(
                            f"watermark_from={from_ts}; to={to_ts}; "
                            f"overlap={checker.WATERMARK_OVERLAP}; new_changes=0"
                        ),
                        rows_extracted=0, rows_loaded=0,
                    )
                    return {}

                # Карта recorder(uid, lower-case UUID-строка) → retail.updated_at
                uid_to_updated_at = {
                    str(self._normalize_uid(u)): ts
                    for u, ts in zip(changed_df["uid"].tolist(), changed_df["updated_at"].tolist())
                    if u and self._normalize_uid(u)
                }
                changed_uids = list(uid_to_updated_at.keys())
                print(f"Found {len(changed_uids)} changed documents in window [{from_ts}, {to_ts})")

                # 3. Прогон по target-ам (sorted by priority)
                results: Dict[str, int] = {}
                total_extracted = 0
                returned_recorders: set = set()  # заполнится первым target-ом

                targets = self._get_active_targets()
                for idx, target in enumerate(targets):
                    rows = self._process_target_incremental(
                        target=target,
                        changed_uids=changed_uids,
                        uid_to_updated_at=uid_to_updated_at,
                    )
                    results[target.target_table] = rows.get("loaded", 0)
                    total_extracted += rows.get("extracted", 0)
                    if rows.get("recorders"):
                        returned_recorders |= set(rows["recorders"])

                # 4. Missing — кого ретейл изменил, а MSSQL не отдал (удалённые)
                missing = [u for u in changed_uids if u not in returned_recorders]
                if missing:
                    print(f"Missing recorders (deleted in 1C): {len(missing)}")
                    self._delete_missing(targets, missing)

                # 5. Закрыть load_history
                max_retail_updated = changed_df["updated_at"].max() if not changed_df.empty else None
                self._close_history_run(
                    pg_meta, run_id,
                    status="success",
                    checkpoint=(
                        f"watermark_from={from_ts}; to={to_ts}; "
                        f"overlap={checker.WATERMARK_OVERLAP}; "
                        f"max_retail_updated_at={max_retail_updated}; "
                        f"changes={len(changed_uids)}"
                    ),
                    rows_extracted=total_extracted,
                    rows_loaded=sum(results.values()),
                )
                return results

        except Exception as e:
            self._close_history_run(
                pg_meta, run_id,
                status="failed",
                error=_format_error(e),
            )
            # Частичный коммит: dim-target уже записан (и с ним retail_updated_at →
            # watermark уехал), а fact упал. Без очистки эти документы теряются
            # навсегда: retry берёт окно после их сигналов, а добор хвоста ищет только
            # uid, которых нет в dim. См. docs/knowledge/debugging (deadlock 2026-09).
            try:
                self._cleanup_incomplete_headers(changed_uids, run_started_at)
            except Exception as ce:  # noqa: BLE001 — очистка не должна маскировать исходную ошибку
                print(f"cleanup incomplete headers failed: {_format_error(ce, 300)}")
            raise

    def _cleanup_incomplete_headers(self, changed_uids: List[str], run_started_at, dry_run: bool = False) -> int:
        """
        Откат неполного документа на пути ошибки: удаляет ШАПКУ и ЕЁ ПОЗИЦИИ одной транзакцией.

        Почему обе таблицы. Таргеты грузятся по очереди и каждый в своей транзакции:
        шапки → post_load(шапок) → позиции → post_load(позиций, он и ставит <fk>).
        Если падает последний шаг (deadlock 14.09 19:20), позиции уже вставлены, но
        <fk> у них NULL. Прошлая версия удаляла только шапку — позиции оставались
        сиротами, а retry вставлял шапку заново с НОВЫМ id, и <fk> старых позиций
        указывал на удалённый id (33 dangling-строки, аудит 2026-09-15).

        Scope строго по прогону: recorder ∈ changed_uids И etl_updated_at ≥ старт прогона.
        Никаких DELETE по периоду. Документ становится «отсутствующим в DWH» целиком,
        и добор хвоста перечитает его следующим тиком.
        """
        targets = self._get_active_targets()
        dim = next((t for t in targets if t.target_role == "dimension"), None)
        fact = next((t for t in targets if t.target_role == "fact"), None)
        if not (dim and fact and changed_uids):
            return 0
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg = PostgresHook(postgres_conn_id=self.dst_conn_id)
        # FK positions → header: {dim}_id, для множественного числа — без «s» (orders → order_id)
        candidates = [f"{dim.target_table}_id"]
        if dim.target_table.endswith("s"):
            candidates.append(f"{dim.target_table[:-1]}_id")
        fk = next((c for c in candidates if pg.get_first(
            "SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=%s",
            parameters=(fact.target_schema, fact.target_table, c))), None)
        if not fk:
            print(f"cleanup incomplete headers: FK {candidates} не найдена в {fact.full_table_name} — пропуск")
            return 0
        uids = [str(self._normalize_uid(u)) for u in changed_uids if self._normalize_uid(u)]
        if not uids:
            return 0
        # Кандидаты: шапки этого прогона, у которых нет ни одной позиции с корректным <fk>.
        # (позиции, вставленные упавшим прогоном, имеют <fk> IS NULL — поэтому шапка сюда попадает)
        pick = (f"SELECT d.id, d.recorder, d.recorder_type FROM {dim.full_table_name} d "
                f"WHERE d.recorder = ANY(%s::uuid[]) AND d.etl_updated_at >= %s "
                f"AND NOT EXISTS (SELECT 1 FROM {fact.full_table_name} p WHERE p.{fk} = d.id)")
        if dry_run:
            rows = pg.get_records(pick, parameters=(uids, run_started_at))
            print(f"cleanup incomplete document [{dim.target_table}]: найдено {len(rows)} шапок без позиций "
                  f"(прогон с {run_started_at}) — dry-run, ничего не удалено")
            return len(rows)
        deleted_h = deleted_p = 0
        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(pick, (uids, run_started_at))
                rows = cur.fetchall()
                if rows:
                    recs = [r[0] for r in rows]          # id шапок
                    nks = [(str(r[1]), r[2]) for r in rows]  # (recorder, recorder_type)
                    # 1) позиции этих документов, тронутые ЭТИМ прогоном (в т.ч. с <fk> IS NULL)
                    cur.execute(
                        f"DELETE FROM {fact.full_table_name} p "
                        f"WHERE (p.recorder, p.recorder_type) IN %s AND p.etl_updated_at >= %s",
                        (tuple(nks), run_started_at),
                    )
                    deleted_p = cur.rowcount
                    # 2) сами шапки
                    cur.execute(f"DELETE FROM {dim.full_table_name} d WHERE d.id = ANY(%s)", (recs,))
                    deleted_h = cur.rowcount
            conn.commit()
        print(f"cleanup incomplete document [{dim.target_table}]: удалено {deleted_h} шапок и "
              f"{deleted_p} позиций прогона с {run_started_at} (документы перечитает добор хвоста)")
        return deleted_h

    # ------------------------------------------------------------------
    # incremental helpers
    # ------------------------------------------------------------------
    def _get_watermark_table(self) -> str:
        """
        Таблица-источник watermark для инкремента: dim-target регистра
        (target_role='dimension', минимальный priority). У dim есть updated_at,
        который движок проставляет из retail — это и есть точка отсечки.
        Fallback: 'public.sales' (легаси-хардкод до этапа 0.2) — для конфигов
        без target_role поведение не меняется.
        """
        dim = next(
            (t for t in self._get_active_targets() if t.target_role == "dimension"),
            None,
        )
        if dim:
            return dim.full_table_name
        return "public.sales"

    def _open_history_run(self, pg_meta, run_mode: str) -> int:
        sql = """
            INSERT INTO etl_meta.load_history
                (register_id, run_mode, status, started_at)
            VALUES (%s, %s, 'running', NOW())
            RETURNING id
        """
        rows = pg_meta.get_records(sql, parameters=(self.config.id, run_mode))
        run_id = rows[0][0]
        print(f"load_history.id={run_id} status=running")
        return run_id

    def _close_history_run(
        self, pg_meta, run_id: int, status: str,
        checkpoint: Optional[str] = None,
        rows_extracted: Optional[int] = None,
        rows_loaded: Optional[int] = None,
        error: Optional[str] = None,
    ):
        sql = """
            UPDATE etl_meta.load_history
            SET status = %s,
                finished_at = NOW(),
                checkpoint_value = COALESCE(%s, checkpoint_value),
                rows_extracted = COALESCE(%s, rows_extracted),
                rows_loaded = COALESCE(%s, rows_loaded),
                error_message = COALESCE(%s, error_message)
            WHERE id = %s
        """
        pg_meta.run(sql, parameters=(status, checkpoint, rows_extracted, rows_loaded, error, run_id))
        print(f"load_history.id={run_id} status={status} checkpoint={checkpoint}")

    def _advisory_lock(self, pg_meta, register_id: int):
        """
        pg_advisory_lock per-register — блокирует параллельные прогоны того же
        регистра. Возвращает context manager.
        """
        from contextlib import contextmanager
        @contextmanager
        def _lock():
            conn = pg_meta.get_conn()
            cur = conn.cursor()
            try:
                # неблокирующая попытка — если уже взят, падаем сразу
                cur.execute("SELECT pg_try_advisory_lock(%s)", (register_id,))
                got = cur.fetchone()[0]
                if not got:
                    raise RuntimeError(
                        f"Another incremental run is in progress for register_id={register_id}"
                    )
                conn.commit()
                print(f"advisory_lock({register_id}) acquired")
                yield
            finally:
                try:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (register_id,))
                    conn.commit()
                    print(f"advisory_lock({register_id}) released")
                except Exception:
                    pass
                cur.close()
                conn.close()
        return _lock()

    @staticmethod
    def _normalize_uid(u):
        """Толерантный парсинг uid → стандартная UUID-строка с дефисами (lowercase)."""
        from .transform.binary import _parse_uuid_lenient
        parsed = _parse_uuid_lenient(u)
        return str(parsed) if parsed else None

    def _process_target_incremental(
        self,
        target: "TargetConfig",
        changed_uids: List[str],
        uid_to_updated_at: Optional[Dict[str, "datetime"]] = None,
    ) -> dict:
        """
        Извлечение/трансформация/загрузка одного target в инкременте.

        Args:
            uid_to_updated_at: карта recorder(UUID-строка) → retail.updated_at.
                               Для dim-target проставляется как updated_at у строки.

        Возвращает {loaded, extracted, recorders} для статистики и missing-логики.
        """
        print(f"Processing target [incremental]: {target.full_table_name}")

        # 1–4b. Извлечение — общее с full_period и прямым путём в ClickHouse
        df, extracted, returned_recorders = self.extract_frame(target, key_values=changed_uids)
        if df.empty:
            print(f"No data for {target.target_table} in incremental")
            return {"loaded": 0, "extracted": 0, "recorders": set()}

        # 4c-new. ETL audit-поля для incremental (см. docs/sales_load_modes.md):
        #     retail_updated_at  — per-row retail.updated_at (через map recorder→ts)
        #     retail_snapshot_at — snapshot самого запуска (MAX(retail_updated_at) batch'а)
        #     etl_updated_at     — now()
        #     updated_at (legacy)— оставляем синхронным с retail_updated_at у dim
        # ETL audit-поля в Asia/Almaty TZ (см. docs/sales_load_modes.md)
        # etl_loaded_at не трогаем — он DEFAULT now() в DDL, обновляется только при INSERT
        # TZ сессии Postgres задаётся в Loader (SET TIME ZONE 'Asia/Almaty')
        df["etl_updated_at"] = _now_local()
        if uid_to_updated_at and "recorder" in df.columns:
            mapped = df["recorder"].astype(str).map(uid_to_updated_at)
            df["retail_updated_at"] = mapped
            if mapped.notna().any():
                df["retail_snapshot_at"] = mapped.max()
            # legacy updated_at — только если колонка есть в include_columns (старый контур)
            if target.target_role == "dimension" and "updated_at" in (target.include_columns or []):
                df["updated_at"] = mapped

        # 4c. updated_at для dim — берём из uid_to_updated_at по recorder каждой строки.
        #     Не из now()! Время хранится в часах retail, чтобы watermark двигался корректно.
        if (
            target.target_role == "dimension"
            and uid_to_updated_at
            and "updated_at" in (target.include_columns or [])
            and "recorder" in df.columns
        ):
            df["updated_at"] = df["recorder"].astype(str).map(uid_to_updated_at)
            n_mapped = df["updated_at"].notna().sum()
            print(f"Dim updated_at: {n_mapped}/{len(df)} строк замаплено из retail")

        # 5. Pre-load SQL
        if target.pre_load_sql:
            df = self._apply_pre_load_sql(df, target.pre_load_sql)

        # 6. Load — для fact используем delete_insert_by_recorder, если так настроено;
        #    для dim — обычный upsert. Режим читаем из target.load_mode.
        loaded = self.loader.load(
            df=df,
            table_name=target.full_table_name,
            mode=target.load_mode,
            upsert_keys=target.upsert_keys,
        )

        # 7. Post-load SQL (FK resolve)
        if target.post_load_sql:
            self._execute_post_load_sql(target.post_load_sql)

        return {"loaded": loaded, "extracted": extracted, "recorders": returned_recorders}

    def reload_documents(self, uids: List[str], uid_to_updated_at: Optional[Dict[str, "datetime"]] = None) -> Dict[str, int]:
        """
        Точечно перечитать конкретные документы по recorder — без окна и без retail.

        Зачем отдельная точка входа. Инкремент узнаёт «что изменилось» от retail, а retail
        сигналит не обо всём: возвраты без чека в нём отсутствуют, правки B2B-реализаций
        невидимы (см. CLAUDE.md). Страховочная сверка (tools/sales_reconcile) сравнивает
        свежий хвост 1С с витриной, находит такие документы сама и просит движок перечитать
        именно их. Логика загрузки при этом та же самая — _process_target_incremental:
        те же мэппинги, тот же upsert, тот же post_load. Здесь отличается только источник
        списка uid, никакой отдельной бизнес-логики sales тут нет.

        Почему uid_to_updated_at по умолчанию None. Retail-метки у этих документов нет —
        он про них и не знал. Выдумывать её нельзя: MAX(retail_updated_at) это watermark
        инкремента, и любая фальшивая метка сдвинула бы его вперёд, потеряв реальные
        сигналы в пропущенном интервале. Без карты колонка retail_updated_at в df не
        попадает вовсе: upsert сохраняет её у существующих строк и оставляет NULL у новых,
        а NULL в MAX не участвует — watermark не двигается ни вперёд, ни назад.

        Исчезнувшие строки внутри перечитанного документа убирает штатный post_load
        (DELETE по etl_updated_at в окне последних загрузок) — здесь ничего своего нет.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        uids = [u for u in (self._normalize_uid(x) for x in uids) if u]
        if not uids:
            print("reload_documents: пустой список uid — нечего перечитывать")
            return {}

        pg_meta = PostgresHook(postgres_conn_id=self.config_conn_id)
        results: Dict[str, int] = {}
        # тот же advisory_lock, что у инкремента: параллельный тик по этому регистру
        # не должен идти одновременно со сверкой
        with self._advisory_lock(pg_meta, self.config.id):
            run_id = self._open_history_run(pg_meta, "reconcile")
            try:
                for target in self._get_active_targets():
                    res = self._process_target_incremental(
                        target=target, changed_uids=uids, uid_to_updated_at=uid_to_updated_at)
                    results[target.target_table] = res["loaded"]
                self._close_history_run(
                    pg_meta, run_id, "success", rows_loaded=sum(results.values()),
                    checkpoint=f"reconcile; documents={len(uids)}")
            except Exception as e:
                self._close_history_run(pg_meta, run_id, "failed", error=_format_error(e))
                raise
        return results

    def _delete_missing(self, targets: List["TargetConfig"], missing_uids: List[str]):
        """
        Удаление строк по recorder из target-ов для документов, которых нет в MSSQL
        (удалены в 1С). Для fact — DELETE из public.{fact} WHERE recorder IN (missing).
        Для dim  — DELETE из public.{dim}  WHERE recorder IN (missing).
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from .transform.binary import _parse_uuid_lenient

        # Нормализуем uid в стандартные UUID-строки (recorder в PG хранится как uuid)
        normalized = [str(_parse_uuid_lenient(u)) for u in missing_uids if _parse_uuid_lenient(u)]
        if not normalized:
            return

        pg = PostgresHook(postgres_conn_id=self.dst_conn_id)
        placeholders = ", ".join(["%s"] * len(normalized))
        for target in targets:
            # Колонка-ключ удаления — первый upsert-ключ target-а:
            # sales dim/fact → 'recorder', справочник → 'id', VT → 'doc_id'.
            # Fallback 'recorder' — поведение до этапа 0.2 (target без ключей).
            key_col = target.upsert_keys[0] if target.upsert_keys else "recorder"
            sql = f'DELETE FROM {target.full_table_name} WHERE "{key_col}" IN ({placeholders})'
            with pg.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, normalized)
                    deleted = cur.rowcount
                conn.commit()
            print(f"missing DELETE {target.target_table}: {deleted} rows")

    # ======================================================================
    #  PROCESS TARGET
    # ======================================================================
    def extract_frame(
        self,
        target: "TargetConfig",
        *,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        key_values: Optional[List[str]] = None,
    ):
        """
        Извлечение одной цели БЕЗ записи куда-либо: SQL → 1С → преобразования →
        raw_refs → колонки цели → dedup шапки.

        Общая часть full_period, incremental и прямого пути в ClickHouse. До выноса
        эти шаги были продублированы в _process_target и _process_target_incremental
        почти дословно; теперь бизнес-логика извлечения из 1С существует в одном
        месте, и прямой путь получает её, а не копию.

        Возвращает (df, extracted, returned_recorders). returned_recorders снимается
        ДО фильтра include_columns — по нему missing-логика понимает, какие документы
        1С вернула, а какие исчезли.
        """
        sql = self._build_sql_for_target(
            target=target,
            period_start=period_start,
            period_end=period_end,
            key_values=key_values,
        )
        print(f"Generated SQL:\n{sql[:500]}...")

        df, binary_columns = self.storage.execute_query(sql)
        extracted = len(df)
        if df.empty:
            return df, 0, set()
        print(f"Extracted: {extracted} rows, binary columns: {binary_columns}")

        column_transforms = self._build_column_transforms(target)
        df = self.transform.transform_dataframe_by_config(
            df=df, binary_columns=binary_columns, column_transforms=column_transforms,
        )
        df = _pack_raw_refs(df)   # raw_refs.<key> → JSONB raw_refs (стандарт ссылок)

        returned = set()
        if "recorder" in df.columns:
            returned = set(df["recorder"].dropna().astype(str).tolist())

        if target.include_columns:
            available = [c for c in target.include_columns if c in df.columns]
            if "etl_loaded_at" in df.columns and "etl_loaded_at" not in available:
                available.append("etl_loaded_at")
            df = df[available]
            print(f"Filtered to {len(available)} columns for {target.target_table}")

        # Для регистра накопления один документ порождает N строк по позициям;
        # у шапки ключ — документ, поэтому схлопываем явно.
        if target.target_role == "dimension" and target.upsert_keys:
            before = len(df)
            df = df.drop_duplicates(subset=target.upsert_keys, keep="last")
            if before != len(df):
                print(f"Dim dedup: {before} → {len(df)} rows by keys {target.upsert_keys}")

        return df, extracted, returned

    def _process_target(
        self,
        target: TargetConfig,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        key_values: Optional[List[str]] = None,
        snapshot_ts: Optional["datetime"] = None,
    ) -> int:
        """
        Обрабатывает одну целевую таблицу.

        Args:
            snapshot_ts: для full_period — общий retail-снимок MAX(updated_at),
                         проставляется как updated_at у dim-строк.

        Returns:
            Количество загруженных строк
        """
        print(f"Processing target: {target.full_table_name}")

        # 1–4b. Извлечение — общее с incremental и прямым путём в ClickHouse
        df, _, _ = self.extract_frame(
            target,
            period_start=period_start,
            period_end=period_end,
            key_values=key_values,
        )
        if df.empty:
            print(f"No data for {target.target_table}")
            return 0

        # 4c. ETL audit-поля — пишутся ВСЕМ строкам в обоих режимах.
        #     Это служебные колонки (см. docs/sales_load_modes.md), которые
        #     не управляются include_columns / маппингами:
        #       retail_snapshot_at — MAX(updated_at) из retail на старте full_period
        #       retail_updated_at  — заполняется только в incremental (per-row)
        #       etl_updated_at     — момент изменения строки этим прогоном
        #       updated_at (legacy) — для совместимости копируем туда retail_snapshot
        #                             у dim. Новый код должен использовать
        #                             retail_snapshot_at, не updated_at.
        # TZ всех аудит-полей — Asia/Almaty (см. docs/sales_load_modes.md)
        df["etl_updated_at"] = _now_local()

        if snapshot_ts is not None:
            df["retail_snapshot_at"] = snapshot_ts
            # legacy updated_at — только если колонка есть в include_columns таргета
            # (старый контур TEST). В чистой модели (PROD) её нет: дубль retail_snapshot_at.
            if target.target_role == "dimension" and "updated_at" in (target.include_columns or []):
                df["updated_at"] = snapshot_ts
            print(f"retail_snapshot_at={snapshot_ts} (всем строкам {target.target_table})")

        # 5. Pre-load SQL (агрегация, дедупликация)
        if target.pre_load_sql:
            df = self._apply_pre_load_sql(df, target.pre_load_sql)

        # 6. Загружаем
        rows = self.loader.load(
            df=df,
            table_name=target.full_table_name,
            mode=target.load_mode,
            upsert_keys=target.upsert_keys,
        )

        # 7. Post-load SQL (resolve FK, cleanup)
        if target.post_load_sql:
            self._execute_post_load_sql(target.post_load_sql)

        return rows

    def _build_sql_for_target(
        self,
        target: TargetConfig,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        key_values: Optional[List[str]] = None,
    ) -> str:
        """Генерирует SQL для целевой таблицы."""

        # Определяем ключевую колонку для incremental.
        # _AccumRg (standalone)    → _RecorderRRef (UUID документа-регистратора)
        # _Document* (header)      → _IDRRef       (UUID самой шапки)
        # _Document*_VT* (detail)  → _Document*_IDRRef через JOIN — фильтруем по parent
        # Берём первый подходящий источник, у которого настроен этот ключ.
        key_column = None
        if key_values:
            # 1. Если target привязан к конкретному источнику — берём его тип
            primary = None
            if target.source_config:
                primary = target.source_config
            elif target.union_config and target.union_config.members:
                primary = next(
                    (m.source for m in target.union_config.members if m.source),
                    None,
                )
            elif self.config.sources:
                primary = self.config.sources[0]

            if primary:
                if primary.source_type == "standalone":
                    key_column = "_RecorderRRef"
                elif primary.source_type == "header":
                    key_column = "_IDRRef"
                elif primary.source_type == "detail":
                    # detail JOIN-ится на родителя — фильтр через parent
                    parent = primary.parent_source
                    if parent and parent.source_type == "header":
                        key_column = "_IDRRef"
                    else:
                        key_column = "_RecorderRRef"

        # Специальный путь: AccumRg + headers + VTs через LEFT JOIN
        if (self.config.pipeline_type or "").lower() == "accumrg_with_documents":
            return self.query_builder.build_accumrg_with_documents(
                register=self.config,
                target=target,
                period_start=period_start,
                period_end=period_end,
                key_column=key_column,
                key_values=key_values,
            )

        if target.union_config:
            # UNION нескольких источников
            return self.query_builder.build_union_query(
                union=target.union_config,
                register=self.config,
                period_start=period_start,
                period_end=period_end,
                key_column=key_column,
                key_values=key_values,
            )
        elif target.source_config:
            # Один источник
            return self.query_builder.build_source_query(
                source=target.source_config,
                period_start=period_start,
                period_end=period_end,
                key_column=key_column,
                key_values=key_values,
            )
        else:
            raise ValueError(f"Target {target.target_table} has no source or union configured")

    def _build_column_transforms(self, target: TargetConfig) -> Dict[str, Dict]:
        """
        Собирает трансформации колонок из конфигурации.

        Returns:
            {column_name: {"type": transform_type, "params": {...}}}
        """
        transforms = {}

        # Собираем из всех источников, связанных с target
        sources = []
        if target.union_config:
            for member in target.union_config.members:
                if member.source:
                    sources.append(member.source)
                    if member.source.parent_source:
                        sources.append(member.source.parent_source)
        elif target.source_config:
            sources.append(target.source_config)
            if target.source_config.parent_source:
                sources.append(target.source_config.parent_source)

        for source in sources:
            for col in source.columns:
                if col.transform_type:
                    transforms[col.target_column] = {
                        "type": col.transform_type,
                        "params": col.transform_params,
                    }

        return transforms

    def _apply_pre_load_sql(self, df, pre_load_sql: str):
        """
        Применяет pre_load_sql к DataFrame.

        Placeholder __df__ заменяется на временную таблицу.
        """
        import duckdb

        # Используем DuckDB для SQL над DataFrame
        result = duckdb.query(pre_load_sql.replace("__df__", "df")).df()
        print(f"Pre-load SQL applied: {len(df)} -> {len(result)} rows")
        return result

    def _execute_post_load_sql(self, post_load_sql: str):
        """Выполняет SQL после загрузки (resolve FK, cleanup)."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=self.dst_conn_id)
        hook.run(post_load_sql)
        print(f"Post-load SQL executed")
