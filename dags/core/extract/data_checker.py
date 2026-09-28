from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataChecker:
    """
    Определяет список document_uid, которые нужно обновить в ETL.

    Watermark per-register хранится в etl_meta.load_history.checkpoint_value
    у последней успешной строки (status='success'). Если строк нет — fallback
    на `default_since` (по умолчанию 2000-01-01).

    Логика инкремента:
      from_ts = MAX(checkpoint_value) WHERE register_id=R AND status='success'
      to_ts   = now() - READ_SKEW_GUARD       # верхняя граница, чтобы не
                                              # ловить in-flight writes ретейла
      WHERE updated_at >= from_ts AND updated_at < to_ts

    Watermark двигаем до `to_ts` (а не до MAX(updated_at) пачки) —
    тогда даже если пачка пустая, окно всё равно закрывается.
    """

    READ_SKEW_GUARD = timedelta(seconds=5)
    # Overlap: при вычислении from_ts отходим назад на эту дельту, чтобы
    # компенсировать read-skew между retail и нашей dim-таблицей. См.
    # docs/sales_load_modes.md.
    WATERMARK_OVERLAP = timedelta(minutes=5)
    # Добор хвоста: данные появляются в MSSQL позже сигнала retail, поэтому
    # overlap не спасает — uid, чьих данных ещё нет в 1С на момент тика,
    # навсегда выпадали из окна (аудит 2026-07-21: ~23% документов,
    # см. docs/audits/sales_aggregate_recon_2026-07-21.md). Каждый тик
    # дополнительно перепроверяем uid за TAIL_LOOKBACK, которых нет в DWH, —
    # пока документ не появится в 1С или не выйдет из окна.
    #
    # 48 часов оказалось мало: 2026-08-13 потеряно 30 чеков склада Шиели —
    # магазин синхронизировался с 1С на третьи сутки, окно уже закрылось.
    # Наблюдаемые лаги того же склада: 39 ч и 16 ч (14-15 августа, догнали).
    # 15 суток дают ~9x запас к худшему наблюдённому лагу.
    #
    # ПОЧЕМУ НЕ БОЛЬШЕ: хвост берёт только uid, которых нет в DWH, поэтому на
    # здоровой витрине его размер не зависит от глубины окна (замер 2026-08-18:
    # 16 / 22 / 49 uid для 7 / 14 / 30 дней, тик одинаково ~2.8 с). Но потолок
    # растёт линейно — это все сигналы retail за окно: 12 тыс. за 7 дней,
    # 23 тыс. за 15, 50 тыс. за 30. Он выстреливает в аварии (пустая витрина,
    # долгий простой DAG): весь список уходит в MSSQL ОДНИМ `IN (...)` без
    # батчинга и тем же списком в missing-DELETE, а тик ограничен
    # execution_timeout=10 мин. Прежде чем ставить окно больше, нужен батчинг
    # uid по 500-1000 в QueryBuilder — как в tools/load_dim_names.py.
    TAIL_LOOKBACK = timedelta(days=15)
    DEFAULT_SINCE = "1970-01-01 00:00:00"

    def __init__(
        self,
        retail_table: str = "sales",
        retail_conn_id: str = "bd_retail",
        config_conn_id: Optional[str] = None,
        register_id: Optional[int] = None,
        key_column: str = "document_uid",
        # legacy-параметры для обратной совместимости (не используются для watermark)
        etl_table: Optional[str] = None,
        etl_conn_id: Optional[str] = None,
    ):
        from ..conn import require_conn
        require_conn("config_conn_id", config_conn_id)
        self.retail_table = retail_table
        self.retail_conn_id = retail_conn_id
        self.config_conn_id = config_conn_id
        self.register_id = register_id
        self.key_column = key_column
        # legacy — оставляем поля чтобы не сломать существующие вызовы
        self.etl_table = etl_table
        self.etl_conn_id = etl_conn_id

    # ------------------------------------------------------------------
    # 1) Watermark per-register
    # ------------------------------------------------------------------
    def get_last_update(self) -> str:
        """
        Watermark per-register (см. docs/sales_load_modes.md):
          • Сначала MAX(retail_updated_at) — это per-row отметка из retail,
            заполнена строками, которые уже прошли incremental.
          • Если NULL (только full_period работал) — MAX(retail_snapshot_at).
          • Если и его нет (старая БД до миграции 006) — fallback на
            legacy MAX(updated_at).
          • Применяется overlap минус READ_SKEW_GUARD в get_changed_uids,
            чтобы не упустить записи на границе.
          • load_history.checkpoint_value — аудит, не источник правды.
        """
        if not (self.etl_table and self.etl_conn_id):
            return self.DEFAULT_SINCE

        pg = PostgresHook(postgres_conn_id=self.etl_conn_id)

        def _max(col: str) -> Optional[str]:
            # Сначала проверим что колонка существует (etl_table = 'public.sales')
            schema, _, tbl = self.etl_table.partition(".")
            if not tbl:
                schema, tbl = "public", schema
            check = pg.get_records(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name=%s AND column_name=%s",
                parameters=(schema, tbl, col),
            )
            if not check:
                return None
            rows = pg.get_records(
                f"SELECT MAX({col}) FROM {self.etl_table}"
            )
            return str(rows[0][0]) if rows and rows[0][0] else None

        for col in ("retail_updated_at", "retail_snapshot_at", "updated_at"):
            ts = _max(col)
            if ts:
                return ts

        return self.DEFAULT_SINCE

    # ------------------------------------------------------------------
    # 2) Окно [from_ts, to_ts) и список изменившихся
    # ------------------------------------------------------------------
    def get_changed_uids(self) -> Tuple[pd.DataFrame, str, str]:
        """
        Возвращает (df[uid, updated_at], from_ts, to_ts) — всё в Asia/Almaty TZ.

        Retail таблица хранит updated_at в UTC, наша public.sales — в Almaty.
        При запросе к retail добавляем +5h к updated_at чтобы привести к Almaty;
        полученный updated_at в df уже в Almaty и пишется как retail_updated_at.

        from_ts — watermark (включительно) в Almaty
        to_ts   — now(Almaty) - READ_SKEW_GUARD (эксклюзивная верхняя граница)
        """
        from zoneinfo import ZoneInfo
        ALMATY = ZoneInfo("Asia/Almaty")

        from_ts_raw = self.get_last_update()
        # Применяем overlap: -WATERMARK_OVERLAP от watermark. Безопасный пересчёт
        # последних N минут — upsert по (recorder, recorder_type[, line_no]) идемпотентен.
        try:
            from_ts_dt = datetime.fromisoformat(str(from_ts_raw).replace("+00:00", ""))
            from_ts = (from_ts_dt - self.WATERMARK_OVERLAP).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )
        except Exception:
            from_ts = from_ts_raw
        to_ts = (datetime.now(ALMATY).replace(tzinfo=None) - self.READ_SKEW_GUARD).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )

        pg = PostgresHook(postgres_conn_id=self.retail_conn_id)
        # retail.updated_at хранится в UTC — конвертируем в Almaty через AT TIME ZONE
        # (учитывает DST, если когда-нибудь появится).
        # updated_at в df — уже в Almaty, идёт прямо в наш retail_updated_at.
        sql = f"""
            SELECT {self.key_column} AS uid,
                   (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp AS updated_at
            FROM   public.{self.retail_table}
            WHERE  (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp >= %s
              AND  (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp <  %s
            ORDER BY updated_at ASC
        """
        df = pg.get_pandas_df(sql, parameters=(from_ts, to_ts))

        print(
            f"🔵 Incremental window [{from_ts}, {to_ts}): "
            f"{len(df)} changed docs in {self.retail_table}"
        )

        # Добор хвоста: uid за TAIL_LOOKBACK, отсутствующие в DWH
        tail = self._get_tail_uids(window_from=from_ts)
        if tail is not None and not tail.empty:
            df = pd.concat([df, tail], ignore_index=True)
            # один uid мог попасть и в окно, и в хвост — оставляем максимальный updated_at
            df = (
                df.sort_values("updated_at")
                .drop_duplicates(subset=["uid"], keep="last")
                .reset_index(drop=True)
            )

        return df, from_ts, to_ts

    # ------------------------------------------------------------------
    # 2b) Добор хвоста недогруженных uid
    # ------------------------------------------------------------------
    def _get_tail_uids(self, window_from: str) -> Optional[pd.DataFrame]:
        """
        uid, изменённые в retail за TAIL_LOOKBACK ДО начала основного окна
        и отсутствующие в DWH (etl_table). Это документы, чьё проведение в 1С
        отстало от сигнала retail сильнее overlap'а: обычное окно их уже не
        увидит, а retail второй раз про них не сигналит. Перепроверяются
        каждый тик, пока не появятся в регистре 1С (тогда загрузятся штатным
        upsert) или не выйдут за TAIL_LOOKBACK (не-продажи отбрасываются сами).

        Возвращает df[uid, updated_at] (Almaty) или None.
        """
        if not self._presence_available():
            return None

        from zoneinfo import ZoneInfo
        ALMATY = ZoneInfo("Asia/Almaty")
        tail_from = (
            datetime.now(ALMATY).replace(tzinfo=None) - self.TAIL_LOOKBACK
        ).strftime("%Y-%m-%d %H:%M:%S.%f")
        # хвост = [tail_from, window_from): только то, что старше основного окна
        if tail_from >= str(window_from):
            return None

        pg = PostgresHook(postgres_conn_id=self.retail_conn_id)
        sql = f"""
            SELECT {self.key_column} AS uid,
                   MAX((updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp) AS updated_at
            FROM   public.{self.retail_table}
            WHERE  (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp >= %s
              AND  (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp <  %s
            GROUP BY {self.key_column}
        """
        cand = pg.get_pandas_df(sql, parameters=(tail_from, window_from))
        if cand.empty:
            return None

        # нормализуем в валидные uuid (recorder в DWH — тип uuid, lower-case)
        from uuid import UUID

        def _norm(u) -> Optional[str]:
            try:
                return str(UUID(str(u).strip()))
            except Exception:
                return None

        cand["uid_norm"] = cand["uid"].map(_norm)
        cand = cand[cand["uid_norm"].notna()]
        if cand.empty:
            return None

        # анти-джойн: кого из кандидатов уже нет в хранилище
        present = self._present_uids(cand["uid_norm"].tolist())
        missing = cand[~cand["uid_norm"].isin(present)]
        if missing.empty:
            return None

        print(
            f"🟡 Tail pickup: {len(missing)} uid за {self.TAIL_LOOKBACK} "
            f"нет в {self.etl_table} — добираем"
        )
        return missing[["uid", "updated_at"]]

    # ------------------------------------------------------------------
    # Хранилище — единственное, что привязывает проверку к таблице факта.
    # Вынесено в методы, чтобы прямой путь (факты только в ClickHouse)
    # переопределил их, не копируя логику окна и добора хвоста.
    # ------------------------------------------------------------------
    def _presence_available(self) -> bool:
        return bool(self.etl_table and self.etl_conn_id)

    def _present_uids(self, uids: List[str]) -> set:
        """Какие из uid уже есть в хранилище. recorder — конвенция движка для uid документа."""
        dwh = PostgresHook(postgres_conn_id=self.etl_conn_id)
        rows = dwh.get_records(
            f"SELECT DISTINCT recorder::text FROM {self.etl_table} "
            f"WHERE recorder = ANY(%s::uuid[])",
            parameters=(uids,),
        )
        return {r[0] for r in rows}

    # ------------------------------------------------------------------
    # Точка входа (старая сигнатура — для обратной совместимости)
    # ------------------------------------------------------------------
    def detect_changes(self) -> pd.DataFrame:
        df, _, _ = self.get_changed_uids()
        return df
