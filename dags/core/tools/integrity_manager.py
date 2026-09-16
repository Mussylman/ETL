"""
Generic integrity manager: контракты загрузки из etl_meta, без хардкода регистров.

Две сущности, один отчёт и один режим применения:
  • пары «шапка → позиции» (FK, индексы, резолв в post_load, счётчики целостности);
  • справочники reference_dim (частичный индекс под выборку stub в stub-pass).

Зачем. Каждый регистр с парой «документ → строки» повторяет одни и те же требования:
FK позиций на шапку, индекс под этот FK, индекс под окно последних загрузок у шапки,
резолв FK после загрузки (в т.ч. «висячих» ссылок) и откат неполного документа при аварии.
Раньше это оформлялось персональными миграциями на каждый регистр (023-026 для sales и orders).
Менеджер выводит те же требования из метаданных и приводит к ним любую пару.

Использование:
    PYTHONPATH=/home/dev/airflow/dags python -u -m core.tools.integrity_manager --conn etl_prod --plan
    PYTHONPATH=/home/dev/airflow/dags python -u -m core.tools.integrity_manager --conn etl_prod --apply-safe
    ... --register order        # только один регистр
    ... --json                  # машинный вывод плана

Режимы:
    --plan        ничего не меняет: показывает требования, расхождения и предлагаемые действия
    --apply-safe  применяет только безопасные действия (CREATE INDEX CONCURRENTLY,
                  UPDATE-резолв FK, дополнение post_load в etl_meta).
                  Разрушающих действий менеджер не выполняет никогда: DELETE и DROP
                  остаются за откатом движка и явными миграциями.

Идемпотентность: повторный запуск после --apply-safe даёт NO ACTION.

Откуда берутся правила (всё из etl_meta, ничего не зашито):
    пара          — register_targets.parent_target_id
    FK-колонка    — {parent_table}_id, для множественного числа ещё {parent_table без 's'}_id;
                    выбирается та, что физически есть в child (sales → sales_id, orders → order_id).
                    Та же логика в etl_engine._cleanup_incomplete_headers и в валидаторе движка.
    ключи связи   — upsert_keys родителя, присутствующие в обеих таблицах (recorder, recorder_type)
    NK ребёнка    — upsert_keys child-таргета
"""

import argparse
import json
import re
import sys
from typing import Dict, List, Optional

SCHEMA = "etl_meta"

# Часть post_load, за которую отвечает менеджер. Всё остальное в post_load (stub-резолв
# справочников, удаление исчезнувших строк) — дело конкретного регистра, мы его не трогаем.
FK_BLOCK_HEADER = "-- [integrity_manager] FK на шапку по natural key"


def _fk_block(child: str, parent: str, fk: str, keys: List[str], with_dangling: bool = True) -> str:
    """Стандартный резолв FK: непривязанные строки + «висячие» ссылки в окне последних загрузок.

    with_dangling=False — если у родителя нет etl_updated_at: окно последних загрузок не по чему
    строить, и ветка ссылалась бы на несуществующую колонку. Тогда ставим только резолв NULL.
    """
    cond = " AND ".join(f"p.{k} = h.{k}" for k in keys)
    head = (
        f"{FK_BLOCK_HEADER} ({', '.join(keys)}).\n"
        f"-- 1) не привязанные строки — где угодно (индекс по {child}.{fk})\n"
        f"UPDATE {child} p SET {fk} = h.id FROM {parent} h\n"
        f"WHERE p.{fk} IS NULL AND {cond};"
    )
    if not with_dangling:
        return head + (f"\n-- 2) ветки «висячих» ссылок нет: у {parent} отсутствует etl_updated_at, "
                       f"окно последних загрузок строить не по чему.")
    return head + (
        f"\n-- 2) «висячие» ссылки: шапку пересоздали с новым id после отката упавшего прогона.\n"
        f"--    Скоуп — документы последних загрузок (индекс по {parent}.etl_updated_at).\n"
        f"UPDATE {child} p SET {fk} = h.id FROM {parent} h\n"
        f"WHERE h.etl_updated_at >= timezone('Asia/Almaty', now()) - interval '60 minutes'\n"
        f"  AND {cond} AND p.{fk} IS NOT NULL AND p.{fk} <> h.id;"
    )


class Pair:
    """Пара «родитель → ребёнок» с выведенным из метаданных контрактом целостности."""

    def __init__(self, row: dict):
        self.register = row["register_code"]
        self.child_id = row["child_id"]
        self.child = f'{row["child_schema"]}.{row["child_table"]}'
        self.child_schema, self.child_table = row["child_schema"], row["child_table"]
        self.parent_id = row["parent_id"]
        self.parent = f'{row["parent_schema"]}.{row["parent_table"]}'
        self.parent_schema, self.parent_table = row["parent_schema"], row["parent_table"]
        self.parent_keys = list(row["parent_keys"] or [])
        self.child_keys = list(row["child_keys"] or [])
        self.post_load = row["post_load_sql"] or ""
        self.fk: Optional[str] = None
        self.match_keys: List[str] = []
        self.problems: List[str] = []
        self.actions: List[dict] = []
        self.checks: Dict[str, Optional[int]] = {}

    @property
    def title(self) -> str:
        return f"{self.register}: {self.parent_table} → {self.child_table}"


class DimEntry:
    """
    Справочник reference_dim — вторая сущность менеджера, с тем же контрактом отчёта.

    Требование у неё одно: stub-pass каждые 5 минут спрашивает «есть ли строки
    is_stub» и берёт порцию по id. Без частичного индекса это Seq Scan по всей
    таблице (на PROD dim_kontragent — 661 МБ, 288 раз в сутки ради единиц строк).
    """

    def __init__(self, row: dict):
        self.register = row["register_code"]
        self.child_id = row["target_id"]          # ключ для applied — id таргета, он уникален
        self.schema = row["target_schema"] or "public"
        self.table = row["target_table"]
        self.child = f"{self.schema}.{self.table}"
        self.parent = ""
        self.fk: Optional[str] = None
        self.match_keys: List[str] = []
        self.problems: List[str] = []
        self.actions: List[dict] = []
        self.checks: Dict[str, Optional[int]] = {}

    @property
    def title(self) -> str:
        return f"{self.register}: {self.table} (reference_dim)"


def fk_candidates(parent_table: str) -> List[str]:
    """{parent}_id, затем без «s» — правило движка, одинаковое во всех местах."""
    cands = [f"{parent_table}_id"]
    if parent_table.endswith("s") and len(parent_table) > 1:
        cands.append(f"{parent_table[:-1]}_id")
    return cands


class IntegrityManager:
    def __init__(self, conn_id: str, apply_safe: bool = False):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        self.pg = PostgresHook(postgres_conn_id=conn_id)
        self.conn_id = conn_id
        self.apply_safe = apply_safe

    # ---------------- вспомогательные обращения к каталогу ----------------
    def _columns(self, schema: str, table: str) -> Dict[str, str]:
        return {c: t for c, t in self.pg.get_records(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s", parameters=(schema, table))}

    def _has_index_on(self, schema: str, table: str, column: str) -> Optional[str]:
        """Индекс, у которого column — ПЕРВАЯ колонка (только такой годится для фильтра по ней)."""
        for name, ddl in self.pg.get_records(
            "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname=%s AND tablename=%s",
            parameters=(schema, table),
        ):
            m = re.search(r"\(([^)]*)\)", ddl)
            if m and m.group(1).split(",")[0].strip().strip('"').split(" ")[0] == column:
                return name
        return None

    def _scalar(self, sql: str, params=None) -> int:
        row = self.pg.get_first(sql, parameters=params)
        return int(row[0]) if row and row[0] is not None else 0

    def _table_exists(self, schema: str, table: str) -> bool:
        return self.pg.get_first(
            "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
            parameters=(schema, table)) is not None

    # ---------------- discovery ----------------
    def discover(self, register: Optional[str] = None) -> List[Pair]:
        sql = f"""
            SELECT r.code AS register_code,
                   c.id AS child_id, COALESCE(c.target_schema,'public') AS child_schema,
                   c.target_table AS child_table, c.upsert_keys AS child_keys, c.post_load_sql,
                   p.id AS parent_id, COALESCE(p.target_schema,'public') AS parent_schema,
                   p.target_table AS parent_table, p.upsert_keys AS parent_keys
            FROM {SCHEMA}.register_targets c
            JOIN {SCHEMA}.register_targets p ON p.id = c.parent_target_id
            JOIN {SCHEMA}.registers r ON r.id = c.register_id
            WHERE c.is_active AND p.is_active {{flt}}
            ORDER BY r.code, c.priority, c.id
        """.format(flt="AND r.code = %s" if register else "")
        rows = self.pg.get_records(sql, parameters=([register] if register else None))
        cols = ["register_code", "child_id", "child_schema", "child_table", "child_keys",
                "post_load_sql", "parent_id", "parent_schema", "parent_table", "parent_keys"]
        return [Pair(dict(zip(cols, r))) for r in rows]

    # ---------------- контракт одной пары ----------------
    def analyze(self, pair: Pair) -> Pair:
        if not self._table_exists(pair.child_schema, pair.child_table):
            pair.problems.append(f"нет таблицы {pair.child}")
            return pair
        if not self._table_exists(pair.parent_schema, pair.parent_table):
            pair.problems.append(f"нет таблицы {pair.parent}")
            return pair

        child_cols = self._columns(pair.child_schema, pair.child_table)
        parent_cols = self._columns(pair.parent_schema, pair.parent_table)

        # 1. FK-колонка
        cands = fk_candidates(pair.parent_table)
        pair.fk = next((c for c in cands if c in child_cols), None)
        if not pair.fk:
            pair.problems.append(f"нет FK-колонки: ни одной из {cands} в {pair.child} "
                                 f"(её создаёт DDL/Sync, менеджер таблицы не меняет)")
            return pair
        if child_cols[pair.fk] != "bigint":
            pair.problems.append(f"{pair.child}.{pair.fk} имеет тип {child_cols[pair.fk]}, ожидается bigint")

        # 2. Ключи связи: upsert_keys родителя, физически присутствующие у обоих
        pair.match_keys = [k for k in pair.parent_keys if k in child_cols and k in parent_cols]
        missing_keys = [k for k in pair.parent_keys if k not in pair.match_keys]
        if not pair.match_keys:
            pair.problems.append(f"ключи связи не выводятся: upsert_keys родителя {pair.parent_keys} "
                                 f"отсутствуют в {pair.child}")
            return pair
        if missing_keys:
            pair.problems.append(f"ключи {missing_keys} есть у родителя, но не у ребёнка — "
                                 f"связь строится по {pair.match_keys}")

        # 3. Откат движка применим? (та же логика поиска FK + ключи на месте)
        if "etl_updated_at" not in child_cols or "etl_updated_at" not in parent_cols:
            pair.problems.append("нет etl_updated_at в одной из таблиц — run-scoped откат "
                                 "и окно резолва работать не смогут")

        # 4. Индексы
        idx_fk = self._has_index_on(pair.child_schema, pair.child_table, pair.fk)
        if idx_fk:
            pair.checks["index_child_fk"] = idx_fk
        else:
            name = f"idx_{pair.child_table}_{pair.fk}"
            pair.actions.append({
                "kind": "create_index", "safe": True, "object": f"{pair.child}({pair.fk})",
                "reason": "резолв FK и проверка «позиции без шапки» иначе идут Seq Scan",
                "sql": f'CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} ON {pair.child} ({pair.fk})',
            })
        if "etl_updated_at" in parent_cols:
            idx_upd = self._has_index_on(pair.parent_schema, pair.parent_table, "etl_updated_at")
            if idx_upd:
                pair.checks["index_parent_etl_updated_at"] = idx_upd
            else:
                name = f"idx_{pair.parent_table}_etl_updated_at"
                pair.actions.append({
                    "kind": "create_index", "safe": True, "object": f"{pair.parent}(etl_updated_at)",
                    "reason": "окно последних загрузок в резолве «висячих» FK",
                    "sql": f'CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} ON {pair.parent} (etl_updated_at)',
                })

        # 5. post_load: оба резолва присутствуют?
        #    Ветку «висячих» требуем только там, где у родителя есть etl_updated_at, — иначе
        #    менеджер предлагал бы патч на каждом прогоне и ломал идемпотентность.
        can_dangle = "etl_updated_at" in parent_cols
        block = _fk_block(pair.child, pair.parent, pair.fk, pair.match_keys, with_dangling=can_dangle)
        has_null = re.search(rf"SET\s+{pair.fk}\s*=.*?\.id", pair.post_load, re.S) and \
            re.search(rf"p?\.?{pair.fk}\s+IS\s+NULL", pair.post_load)
        has_dangling = re.search(rf"{pair.fk}\s*<>\s*[a-z]+\.id", pair.post_load) or not can_dangle
        if not has_null or not has_dangling:
            missing = ("резолв FK отсутствует полностью" if not has_null
                       else "нет ветки «висячих» ссылок (FK <> parent.id)")
            pair.actions.append({
                "kind": "patch_post_load", "safe": True, "object": f"target {pair.child_id} ({pair.child})",
                "reason": missing,
                "sql": block,
                "_target_id": pair.child_id, "_block": block,
            })
        else:
            pair.checks["post_load_fk_resolve"] = "NULL + dangling" if can_dangle else "NULL (без окна)"

        # 6. Integrity checks
        cond = " AND ".join(f"p.{k} = h.{k}" for k in pair.match_keys)
        pair.checks["fk_null_with_parent"] = self._scalar(
            f"SELECT count(*) FROM {pair.child} p WHERE p.{pair.fk} IS NULL "
            f"AND EXISTS (SELECT 1 FROM {pair.parent} h WHERE {cond})")
        pair.checks["fk_dangling"] = self._scalar(
            f"SELECT count(*) FROM {pair.child} p WHERE p.{pair.fk} IS NOT NULL "
            f"AND NOT EXISTS (SELECT 1 FROM {pair.parent} h WHERE h.id = p.{pair.fk})")
        if pair.child_keys:
            cols = ", ".join(pair.child_keys)
            pair.checks["dup_child_nk"] = self._scalar(
                f"SELECT count(*) FROM (SELECT 1 FROM {pair.child} GROUP BY {cols} HAVING count(*) > 1) d")
        if pair.parent_keys:
            cols = ", ".join(pair.parent_keys)
            pair.checks["dup_parent_nk"] = self._scalar(
                f"SELECT count(*) FROM (SELECT 1 FROM {pair.parent} GROUP BY {cols} HAVING count(*) > 1) d")

        broken = (pair.checks.get("fk_null_with_parent", 0) or 0) + (pair.checks.get("fk_dangling", 0) or 0)
        if broken:
            stmts = [s.strip() for s in block.split(";") if "UPDATE" in s]
            pair.actions.append({
                "kind": "resolve_fk", "safe": True, "object": pair.child,
                "reason": f"{pair.checks.get('fk_null_with_parent', 0)} строк без FK при живой шапке, "
                          f"{pair.checks.get('fk_dangling', 0)} «висячих»",
                "sql": ";\n".join(stmts) + ";",
                "_stmts": stmts,
            })
        for key in ("dup_child_nk", "dup_parent_nk"):
            if pair.checks.get(key):
                pair.problems.append(f"{key} = {pair.checks[key]} — дубли natural key лечатся только "
                                     f"разбором данных, автоматических действий нет")
        return pair

    # ---------------- справочники: индекс под stub-pass ----------------
    def discover_dims(self, register: Optional[str] = None) -> List[DimEntry]:
        flt = "AND r.code = %s" if register else ""
        rows = self.pg.get_records(
            f"""SELECT r.code AS register_code, t.id AS target_id,
                       COALESCE(t.target_schema,'public') AS target_schema, t.target_table
                FROM {SCHEMA}.register_targets t
                JOIN {SCHEMA}.registers r ON r.id = t.register_id
                WHERE t.is_active AND r.is_active AND r.pipeline_type = 'reference_dim'
                  AND t.target_role = 'dimension' {flt}
                ORDER BY r.code""",
            parameters=(register,) if register else None)
        cols = ["register_code", "target_id", "target_schema", "target_table"]
        return [DimEntry(dict(zip(cols, r))) for r in rows]

    def _stub_index(self, schema: str, table: str) -> Optional[str]:
        """Любой индекс, ограниченный предикатом по is_stub, — эквивалент, дубль не нужен."""
        for name, ddl in self.pg.get_records(
            "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname=%s AND tablename=%s",
            parameters=(schema, table),
        ):
            if "is_stub" in ddl:
                return name
        return None

    def analyze_dim(self, d: DimEntry) -> DimEntry:
        if not self._table_exists(d.schema, d.table):
            d.problems.append(f"нет таблицы {d.child}")
            return d
        cols = self._columns(d.schema, d.table)
        if "is_stub" not in cols:
            d.problems.append(f"{d.child}: нет колонки is_stub — stub-pass для него не работает "
                              f"(колонку создаёт Sync конфигуратора, менеджер таблицы не меняет)")
            return d

        idx = self._stub_index(d.schema, d.table)
        if idx:
            d.checks["index_stub"] = idx
        else:
            name = f"idx_{d.table}_stub"
            d.actions.append({
                "kind": "create_index", "safe": True, "object": f"{d.child}(id) WHERE is_stub",
                "reason": "выборка порции stub каждый тик иначе идёт Seq Scan по всей таблице",
                "sql": f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} "
                       f"ON {d.child} (id) WHERE is_stub",
            })
        d.checks["stub_rows"] = self._scalar(f"SELECT count(*) FROM {d.child} WHERE is_stub")
        d.checks["dup_guid"] = self._scalar(
            f"SELECT count(*) FROM (SELECT guid FROM {d.child} "
            f"GROUP BY guid HAVING count(*) > 1) x") if "guid" in cols else 0
        if d.checks["dup_guid"]:
            d.problems.append(f"dup_guid = {d.checks['dup_guid']} — дубли ключа справочника "
                              f"лечатся только разбором данных")
        return d

    # ---------------- применение ----------------
    def apply(self, pair: Pair) -> List[str]:
        done = []
        for a in pair.actions:
            if not a.get("safe"):
                continue
            if a["kind"] == "create_index":
                self.pg.run(a["sql"], autocommit=True)   # CONCURRENTLY нельзя в транзакции
                done.append(f'{a["kind"]}: {a["object"]}')
            elif a["kind"] == "resolve_fk":
                fixed = []
                with self.pg.get_conn() as conn:
                    with conn.cursor() as cur:
                        for stmt in a["_stmts"]:
                            cur.execute(stmt)
                            fixed.append(cur.rowcount)
                    conn.commit()
                done.append(f'{a["kind"]}: {pair.child} — NULL {fixed[0]}, dangling {fixed[1] if len(fixed) > 1 else 0}')
            elif a["kind"] == "patch_post_load":
                # FK-блок менеджера ставим в начало post_load, остальной текст не трогаем
                cur_sql = self.pg.get_first(
                    f"SELECT post_load_sql FROM {SCHEMA}.register_targets WHERE id=%s",
                    parameters=(a["_target_id"],))[0] or ""
                if FK_BLOCK_HEADER in cur_sql:
                    continue
                self.pg.run(
                    f"UPDATE {SCHEMA}.register_targets SET post_load_sql = %s WHERE id = %s",
                    parameters=(a["_block"] + "\n\n" + cur_sql, a["_target_id"]))
                done.append(f'{a["kind"]}: {a["object"]}')
        return done


# Счётчики, которые показывают состояние, а не дефект: нулю радоваться нечему,
# ненулевое не ошибка (stub-строки — норма, пока объект не приехал из 1С).
INFO_CHECKS = {"stub_rows"}


def _print_report(pairs, applied: Dict[int, List[str]], apply_safe: bool):
    total_actions = 0
    total_applied = 0
    for p in pairs:
        print(f"\n=== {p.title} ===")
        if p.fk:
            print(f"  FK: {p.child}.{p.fk} → {p.parent}.id   ключи связи: {', '.join(p.match_keys) or '—'}")
            print(f"  NK ребёнка: {', '.join(p.child_keys) or '—'}   NK родителя: {', '.join(p.parent_keys) or '—'}")
        for k, v in p.checks.items():
            mark = ("" if (k in INFO_CHECKS or not isinstance(v, int))
                    else (" ✅" if v == 0 else " ❌"))
            print(f"  {k:<28} {v}{mark}")
        for w in p.problems:
            print(f"  ⚠ {w}")
        done = applied.get(p.child_id, [])
        total_applied += len(done)
        for line in done:
            print(f"  ✔ применено: {line}")
        if not p.actions:
            print("  → NO ACTION" + (" (после применения)" if done else ""))
        for a in p.actions:
            total_actions += 1
            print(f"  → {a['kind']} ({'safe' if a.get('safe') else 'manual'}): {a['object']} — {a['reason']}")
            for line in a["sql"].splitlines():
                print(f"       {line}")
    if apply_safe:
        print(f"\nИТОГО: пар {len(pairs)}, применено {total_applied}, осталось {total_actions}"
              + (" — NO ACTION" if total_actions == 0 else " (требуют ручного разбора)"))
    else:
        print(f"\nИТОГО: пар {len(pairs)}, действий {total_actions}"
              + (" (режим --plan, ничего не изменено)" if total_actions else " — NO ACTION по всем парам"))
    return total_actions


def main():
    ap = argparse.ArgumentParser(description="Generic parent-child integrity manager (etl_meta)")
    ap.add_argument("--conn", default="etl_prod", help="Airflow conn_id к БД витрины и etl_meta")
    ap.add_argument("--register", default=None, help="только один регистр (код из etl_meta.registers)")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true", help="показать план, ничего не менять")
    mode.add_argument("--apply-safe", action="store_true", help="применить только безопасные действия")
    ap.add_argument("--scope", choices=["all", "pairs", "dims"], default="all",
                    help="что проверять: пары «шапка → позиции», справочники reference_dim или всё")
    ap.add_argument("--json", action="store_true", help="машинный вывод плана")
    args = ap.parse_args()

    mgr = IntegrityManager(args.conn, apply_safe=args.apply_safe)

    def _scan():
        # две сущности с общим контрактом отчёта: пары «шапка → позиции» и справочники
        out = []
        if args.scope in ("all", "pairs"):
            out += [mgr.analyze(p) for p in mgr.discover(args.register)]
        if args.scope in ("all", "dims"):
            out += [mgr.analyze_dim(d) for d in mgr.discover_dims(args.register)]
        return out

    pairs = _scan()
    applied: Dict[int, List[str]] = {}
    if args.apply_safe:
        for p in pairs:
            if p.actions:
                applied[p.child_id] = mgr.apply(p)
        # повторный анализ — проверка идемпотентности и итогового состояния
        pairs = _scan()

    if args.json:
        print(json.dumps([{
            "register": p.register, "parent": p.parent, "child": p.child, "fk": p.fk,
            "match_keys": p.match_keys, "checks": p.checks, "problems": p.problems,
            "actions": [{k: v for k, v in a.items() if not k.startswith("_")} for a in p.actions],
            "applied": applied.get(p.child_id, []),
        } for p in pairs], ensure_ascii=False, indent=2))
        return 0

    print(f"Generic integrity manager — conn={args.conn}, "
          f"режим={'apply-safe' if args.apply_safe else 'plan'}")
    remaining = _print_report(pairs, applied, args.apply_safe)
    return 1 if (remaining and args.apply_safe) else 0


if __name__ == "__main__":
    sys.exit(main())
