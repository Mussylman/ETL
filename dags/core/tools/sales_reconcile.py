"""
Страховочная сверка свежего хвоста продаж: 1С → витрина → точечная дозагрузка.

Зачем. Основной инкремент узнаёт «что изменилось» от retail, и это работает не всегда:
возвраты без чека в retail отсутствуют вовсе, правки B2B-реализаций невидимы (CLAUDE.md).
Подтверждённый пример — возврат 5cb2300f-b108-11f1-8115-6032b16c99f7 (тип 476): в 1С есть,
в bd_retail.public.sales его нет ни одной строкой, поэтому обычный тик его не увидит никогда.
Второй случай — документу в 1С поменяли дату: витрина остаётся со старым period.

Что делает. Берёт хвост за SALES_RECONCILE_LOOKBACK_DAYS суток, читает НАПРЯМУЮ регистр 1С
(источник берётся из etl_meta, не зашит), строит по каждому документу минимальный отпечаток
(period, строк, SUM stoimost, SUM nds), сравнивает с витриной и просит движок перечитать
только разошедшиеся документы.

Чего НЕ делает. Не подменяет retail-инкремент и не трогает его watermark. Не содержит своей
загрузочной логики: починка идёт через ETLEngine.reload_documents → _process_target_incremental,
те же мэппинги, upsert и post_load. Исчезнувшие строки внутри перечитанного документа убирает
штатный post_load. Документы, которых в 1С нет вовсе (удалены/распроведены), только считаются
и показываются — удаление остаётся за явным решением, здесь его нет.

Использование:
    PYTHONPATH=/home/dev/airflow/dags python -u -m core.tools.sales_reconcile --conn etl_prod --plan
    PYTHONPATH=/home/dev/airflow/dags python -u -m core.tools.sales_reconcile --conn etl_prod --apply
    ... --lookback-days 7     окно хвоста (по умолчанию SALES_RECONCILE_LOOKBACK_DAYS)
    ... --register sales      код регистра в etl_meta
    ... --limit 500           предохранитель: не чинить больше N документов за прогон
    ... --json                машинный вывод метрик
"""

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

SCHEMA = "etl_meta"

# Окно страховочной сверки. Она работает только со свежим хвостом: старая история
# уже сверена помесячно и пересобирать её этот механизм не должен.
SALES_RECONCILE_LOOKBACK_DAYS = 7

# Порог расхождения сумм. Меньше копейки — это представление float/numeric, а не расхождение.
AMOUNT_TOLERANCE = Decimal("0.01")

# Год в 1С хранится со смещением +2000 (2026 → 4026), см. CLAUDE.md.
YEAR_OFFSET = 2000

# Целевые колонки, из которых собирается отпечаток документа. Имена исходных полей
# берутся из column_mappings — ни _Fld17855, ни _AccumRg17844 здесь не зашиты.
FP_COLUMNS = ("recorder", "recorder_type", "period", "stoimost", "nds")


def _c1(d: date) -> str:
    """Дата витрины → дата в формате 1С (+2000 к году)."""
    return f"{d.year + YEAR_OFFSET:04d}-{d.month:02d}-{d.day:02d}"


def _dec(v) -> Decimal:
    if v is None:
        return Decimal("0")
    try:
        d = Decimal(str(v))
    except Exception:
        return Decimal("0")
    return Decimal("0") if d.is_nan() else d


class SalesReconciler:
    """Сверка «1С → витрина» по документам свежего хвоста и точечная починка."""

    def __init__(self, conn_id: str, register_code: str = "sales",
                 mssql_conn_id: str = "mssql_1c_conn", retail_conn_id: str = "bd_retail",
                 lookback_days: int = SALES_RECONCILE_LOOKBACK_DAYS):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

        self.conn_id = conn_id
        self.register_code = register_code
        self.mssql_conn_id = mssql_conn_id
        self.retail_conn_id = retail_conn_id
        self.lookback_days = int(lookback_days)
        self.pg = PostgresHook(postgres_conn_id=conn_id)
        self.ms = MsSqlHook(mssql_conn_id=mssql_conn_id)
        self.cfg = self._read_config()

    # ------------------------------------------------------------------ конфиг
    def _read_config(self) -> dict:
        """
        Источник отпечатка — из etl_meta: сам регистр 1С и поля его мэппинга.

        Берём standalone-источник регистра (для продаж это накопительный регистр) и
        имена исходных колонок для recorder / recorder_type / period / stoimost / nds.
        Так сверка не знает названий таблиц и реквизитов 1С и переживает их смену в конфиге.
        """
        src = self.pg.get_first(f"""
            SELECT s.id, s.mssql_schema, s.mssql_table
            FROM {SCHEMA}.register_sources s
            JOIN {SCHEMA}.registers r ON r.id = s.register_id
            WHERE r.code = %s AND r.is_active AND s.is_active AND s.source_type = 'standalone'
            ORDER BY s.priority, s.id LIMIT 1""", parameters=(self.register_code,))
        if not src:
            raise RuntimeError(
                f"{self.register_code}: нет активного standalone-источника в register_sources — "
                f"сверять хвост не с чем")

        maps = dict(self.pg.get_records(f"""
            SELECT target_column, source_column FROM {SCHEMA}.column_mappings
            WHERE source_id = %s AND is_active AND NOT is_expression
              AND target_column = ANY(%s)""", parameters=(src[0], list(FP_COLUMNS))))
        missing = [c for c in FP_COLUMNS if c not in maps]
        if missing:
            raise RuntimeError(
                f"{self.register_code}: в мэппинге источника {src[2]} нет колонок {missing} — "
                f"отпечаток документа собрать не из чего")

        dim = self.pg.get_first(f"""
            SELECT COALESCE(t.target_schema,'public'), t.target_table FROM {SCHEMA}.register_targets t
            JOIN {SCHEMA}.registers r ON r.id = t.register_id
            WHERE r.code = %s AND t.is_active AND t.target_role = 'dimension'""",
            parameters=(self.register_code,))
        fact = self.pg.get_first(f"""
            SELECT COALESCE(t.target_schema,'public'), t.target_table FROM {SCHEMA}.register_targets t
            JOIN {SCHEMA}.registers r ON r.id = t.register_id
            WHERE r.code = %s AND t.is_active AND t.target_role = 'fact'""",
            parameters=(self.register_code,))
        if not dim or not fact:
            raise RuntimeError(f"{self.register_code}: нужны активные таргеты dimension и fact")

        return {"table": f"[{src[1]}].[{src[2]}]", "maps": maps,
                "header": f"{dim[0]}.{dim[1]}", "positions": f"{fact[0]}.{fact[1]}"}

    def window(self) -> Tuple[date, date]:
        """[сегодня − lookback, завтра) — правая граница открыта, сегодня включён."""
        today = datetime.now().date()
        return today - timedelta(days=self.lookback_days), today + timedelta(days=1)

    # ------------------------------------------------------------- отпечатки
    def fingerprints_1c(self) -> Dict[Tuple[str, int], tuple]:
        from ..transform.binary import binary_to_uuid, binary_to_int
        m = self.cfg["maps"]
        start, end = self.window()
        rows = self.ms.get_records(
            f"SELECT [{m['recorder']}], [{m['recorder_type']}], MIN([{m['period']}]), COUNT(*), "
            f"       SUM([{m['stoimost']}]), SUM([{m['nds']}]) "
            f"FROM {self.cfg['table']} WITH (NOLOCK) "
            f"WHERE [{m['period']}] >= '{_c1(start)}' AND [{m['period']}] < '{_c1(end)}' "
            f"GROUP BY [{m['recorder']}], [{m['recorder_type']}]")
        return self._pack_1c(rows, binary_to_uuid, binary_to_int)

    def fingerprints_1c_by_uid(self, keys: List[Tuple[str, int]], batch: int = 500) -> Dict[Tuple[str, int], tuple]:
        """
        Отпечатки конкретных документов независимо от их периода.

        Нужны для документов, которые есть в витрине, но не попали в окно 1С: у такого
        документа дата могла уехать за пределы хвоста, и без адресного запроса он выглядел бы
        «удалённым из 1С», хотя просто переехал.
        """
        from ..transform.binary import binary_to_uuid, binary_to_int, uuid_to_mssql_hex_1c
        m = self.cfg["maps"]
        uids = sorted({k[0] for k in keys})
        out: Dict[Tuple[str, int], tuple] = {}
        for i in range(0, len(uids), batch):
            hexes = [h for h in (uuid_to_mssql_hex_1c(u) for u in uids[i:i + batch]) if h]
            if not hexes:
                continue
            rows = self.ms.get_records(
                f"SELECT [{m['recorder']}], [{m['recorder_type']}], MIN([{m['period']}]), COUNT(*), "
                f"       SUM([{m['stoimost']}]), SUM([{m['nds']}]) "
                f"FROM {self.cfg['table']} WITH (NOLOCK) "
                f"WHERE [{m['recorder']}] IN ({','.join(hexes)}) "
                f"GROUP BY [{m['recorder']}], [{m['recorder_type']}]")
            out.update(self._pack_1c(rows, binary_to_uuid, binary_to_int))
        return out

    @staticmethod
    def _pack_1c(rows, to_uuid, to_int) -> Dict[Tuple[str, int], tuple]:
        out = {}
        for rref, tref, period, cnt, stoimost, nds in rows:
            uid = to_uuid(bytes(rref)) if rref is not None else None
            if uid is None:
                continue
            # год 1С хранится со смещением: 4026 → 2026
            per = period.replace(year=period.year - YEAR_OFFSET) if period else None
            out[(str(uid), to_int(bytes(tref)))] = (per, int(cnt), _dec(stoimost), _dec(nds))
        return out

    def fingerprints_dwh(self) -> Dict[Tuple[str, int], tuple]:
        start, end = self.window()
        rows = self.pg.get_records(f"""
            SELECT p.recorder::text, p.recorder_type, min(s.period), count(*),
                   coalesce(sum(NULLIF(p.stoimost,'NaN'::numeric)),0),
                   coalesce(sum(NULLIF(p.nds,'NaN'::numeric)),0)
            FROM {self.cfg['positions']} p
            JOIN {self.cfg['header']} s
              ON s.recorder = p.recorder AND s.recorder_type = p.recorder_type
            WHERE s.period >= %s AND s.period < %s
            GROUP BY 1, 2""", parameters=(start, end))
        return {(r[0], int(r[1])): (r[2], int(r[3]), _dec(r[4]), _dec(r[5])) for r in rows}

    # -------------------------------------------------------------- сравнение
    def compare(self) -> dict:
        one = self.fingerprints_1c()
        dwh = self.fingerprints_dwh()

        # документы витрины вне окна 1С: могли переехать по дате — спрашиваем адресно
        only_dwh = [k for k in dwh if k not in one]
        if only_dwh:
            one.update(self.fingerprints_1c_by_uid(only_dwh))

        missing, period_mm, rows_mm, amount_mm, absent_in_1c = [], [], [], [], []
        for key, fp in one.items():
            got = dwh.get(key)
            if got is None:
                missing.append(key)
                continue
            if fp[0] is not None and got[0] is not None and fp[0] != got[0]:
                period_mm.append(key)
            if fp[1] != got[1]:
                rows_mm.append(key)
            if abs(fp[2] - got[2]) > AMOUNT_TOLERANCE or abs(fp[3] - got[3]) > AMOUNT_TOLERANCE:
                amount_mm.append(key)
        for key in dwh:
            if key not in one:
                absent_in_1c.append(key)

        bad = sorted(set(missing) | set(period_mm) | set(rows_mm) | set(amount_mm))
        return {"one": one, "dwh": dwh, "missing": missing, "period_mismatch": period_mm,
                "rows_mismatch": rows_mm, "amount_mismatch": amount_mm,
                "absent_in_1c": absent_in_1c, "bad": bad}

    # ---------------------------------------------------------------- починка
    def repair(self, keys: List[Tuple[str, int]]) -> Dict[str, int]:
        """Перечитать разошедшиеся документы существующим движком. Своей загрузки здесь нет."""
        from ..etl_engine import ETLEngine
        engine = ETLEngine(
            register_code=self.register_code, mode="incremental",
            config_conn_id=self.conn_id, dst_conn_id=self.conn_id,
            src_conn_id=self.mssql_conn_id, retail_conn_id=self.retail_conn_id)
        return engine.reload_documents(sorted({k[0] for k in keys}))

    # ------------------------------------------------------------------ прогон
    def run(self, apply: bool = False, limit: Optional[int] = None) -> dict:
        start, end = self.window()
        print(f"sales_reconcile: регистр={self.register_code} conn={self.conn_id} "
              f"окно [{start}, {end}) источник 1С {self.cfg['table']}")
        cmp = self.compare()
        metrics = {
            "lookback_days": self.lookback_days,
            "docs_1c": len(cmp["one"]),
            "docs_dwh": len(cmp["dwh"]),
            "missing": len(cmp["missing"]),
            "period_mismatch": len(cmp["period_mismatch"]),
            "rows_mismatch": len(cmp["rows_mismatch"]),
            "amount_mismatch": len(cmp["amount_mismatch"]),
            "reloaded": 0,
            "remaining_delta": len(cmp["bad"]),
        }
        self._show(cmp)

        bad = cmp["bad"]
        if bad and apply:
            todo = bad[:limit] if limit else bad
            if limit and len(bad) > limit:
                print(f"  ⚠ расхождений {len(bad)}, лимит {limit} — чиню первые {limit}, "
                      f"остальные заберёт следующий прогон")
            self.repair(todo)
            metrics["reloaded"] = len({k[0] for k in todo})
            after = self.compare()
            metrics["remaining_delta"] = len(after["bad"])
            print("\n  после перезагрузки:")
            self._show(after)
        elif bad:
            print(f"\n  режим --plan: {len(bad)} документов к перезагрузке, ничего не менял")

        print("\n" + " | ".join(f"{k}={v}" for k, v in metrics.items()))
        if cmp["absent_in_1c"]:
            print(f"  (справочно: {len(cmp['absent_in_1c'])} документов есть в витрине, "
                  f"но отсутствуют в 1С — удаление не моя задача, не трогаю)")
        return metrics

    def _show(self, cmp: dict, sample: int = 5) -> None:
        for name in ("missing", "period_mismatch", "rows_mismatch", "amount_mismatch"):
            keys = cmp[name]
            if not keys:
                continue
            print(f"  {name}: {len(keys)}")
            for key in keys[:sample]:
                a, b = cmp["one"].get(key), cmp["dwh"].get(key)
                print(f"     {key[0]} type={key[1]}")
                print(f"        1С      : period={a[0]} строк={a[1]} stoimost={a[2]} nds={a[3]}"
                      if a else "        1С      : нет")
                print(f"        витрина : period={b[0]} строк={b[1]} stoimost={b[2]} nds={b[3]}"
                      if b else "        витрина : нет")
            if len(keys) > sample:
                print(f"     … ещё {len(keys) - sample}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Страховочная сверка свежего хвоста продаж с 1С")
    ap.add_argument("--conn", default="etl_prod", help="Airflow conn_id витрины и etl_meta")
    ap.add_argument("--register", default="sales", help="код регистра в etl_meta")
    ap.add_argument("--lookback-days", type=int, default=SALES_RECONCILE_LOOKBACK_DAYS)
    ap.add_argument("--mssql-conn", default="mssql_1c_conn")
    ap.add_argument("--retail-conn", default="bd_retail")
    ap.add_argument("--limit", type=int, default=None,
                    help="не перезагружать больше N документов за прогон")
    ap.add_argument("--json", action="store_true", help="машинный вывод метрик")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true", help="только показать расхождения")
    mode.add_argument("--apply", action="store_true", help="перечитать расходящиеся документы")
    args = ap.parse_args()

    r = SalesReconciler(conn_id=args.conn, register_code=args.register,
                        mssql_conn_id=args.mssql_conn, retail_conn_id=args.retail_conn,
                        lookback_days=args.lookback_days)
    metrics = r.run(apply=args.apply, limit=args.limit)
    if args.json:
        print(json.dumps(metrics, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
