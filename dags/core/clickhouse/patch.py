"""
Патч партиций по документам — load_mode = document_patch.

    staging(P) = цель(P) − строки изменившихся документов + свежие строки этих документов
    → проверка сборки → атомарная публикация

Зачем не «пересобрать месяц»: извлечение месяца из боевой 1С стоит в среднем 211 с
(p90 382 с, максимум 599 с), а за цикл меняется в среднем 32 строки. Патч читает
из 1С только изменившиеся документы, а остальное берёт из уже опубликованной партиции.

Затронутые партиции — это и новые (куда документы легли сейчас), и старые (где они
лежали до изменения). Старые находятся в самой цели по ключу документа, поэтому
переезд между месяцами, удаление документа и исчезновение строки ТЧ обрабатываются
одним и тем же механизмом: документ целиком выбрасывается из старой партиции и
целиком возвращается туда, где он есть сейчас. Если его нигде нет — он удалён.

Проверка сборки опирается на аддитивность отпечатка. count, контрольная сумма,
sum и sum квадратов складываются: отпечаток собранной партиции обязан равняться
сумме отпечатков оставленной части и свежих строк. Не сошлось — партиция не
публикуется, цель не меняется.
"""

import time
from decimal import Decimal
from typing import Dict, Iterable, List, Optional

from datetime import datetime

from . import engine as eng
from . import reconcile as rec
from .target import ClickHouse, fmt_value


def _fp(ch: ClickHouse, spec, table: str, where: str = "") -> List[str]:
    return ch.row(rec.fingerprint_sql(spec, "clickhouse") + f" FROM {table}{where}")


def _add(spec, a: List[str], b: List[str]) -> List[str]:
    """Покомпонентная сумма отпечатков: все метрики отпечатка аддитивны."""
    out = []
    for i in range(len(rec.labels(spec))):
        x = Decimal(str(a[i] if i < len(a) and a[i] not in ("", None) else 0))
        y = Decimal(str(b[i] if i < len(b) and b[i] not in ("", None) else 0))
        out.append(str(x + y))
    return out


def _uuid_list(keys: Iterable[str]) -> str:
    return ", ".join(f"toUUID('{k}')" for k in keys)


def publish(pg, ch: ClickHouse, spec, frame, changed_docs: Iterable[str], *,
            rebuild: Optional[List[str]] = None, run_mode: str = "patch") -> List[Dict]:
    """
    frame        — подготовленные свежие строки: колонки строго spec.target_columns
    changed_docs — все документы набора изменений, включая удалённые (их в frame нет)
    rebuild      — список партиций для полной пересборки: оставленной части нет,
                   партиция целиком берётся из frame (ремонт, sweep, backfill)
    """
    doc_key = (spec.source_params.get("doc_key") or ["recorder"])[0]
    changed = sorted({str(d).lower() for d in changed_docs if d})
    pexpr = spec.partition_expr
    results: List[Dict] = []

    # 1. свежие строки — в сырой staging
    ch.execute(f"TRUNCATE TABLE {spec.raw_fqn}")
    if len(frame):
        lines = ("\t".join(fmt_value(v) for v in row) + "\n"
                 for row in frame[spec.target_columns].itertuples(index=False, name=None))
        ch.insert_tsv(spec.raw_fqn, spec.target_columns, lines)

    # 2. затронутые партиции: новые (из свежих строк) и старые (где документы лежали)
    parts = set()
    out = ch.query(f"SELECT DISTINCT {pexpr} FROM {spec.raw_fqn}")
    parts |= {x for x in out.split("\n") if x.strip()}
    if rebuild:
        # Пересборка никогда не трогает партицию, которую не просили: свежие строки
        # вне списка означали бы, что ими заменят чужой месяц целиком.
        outside = parts - set(rebuild)
        if outside:
            raise RuntimeError(f"{spec.code}: свежие строки вне пересобираемых партиций {sorted(outside)} "
                               f"— пересборка остановлена, цель не изменена")
        parts |= set(rebuild)
    elif changed:
        out = ch.query(f"SELECT DISTINCT {pexpr} FROM {spec.fqn} WHERE {doc_key} IN ({_uuid_list(changed)})")
        parts |= {x for x in out.split("\n") if x.strip()}

    not_changed = f" AND {doc_key} NOT IN ({_uuid_list(changed)})" if changed else ""
    for p in sorted(parts):
        t0 = time.monotonic()
        started = datetime.now()
        wp = f" WHERE {pexpr} = {int(p)}"
        cols = ", ".join(spec.target_columns)
        ch.execute(f"TRUNCATE TABLE {spec.stage_fqn}")

        # оставленная часть: всё, кроме изменившихся документов
        if not rebuild:
            ch.execute(f"INSERT INTO {spec.stage_fqn} ({cols}) SELECT {cols} FROM {spec.fqn}{wp}{not_changed}")
        fp_kept = _fp(ch, spec, spec.stage_fqn)
        # свежие строки этой партиции
        ch.execute(f"INSERT INTO {spec.stage_fqn} ({cols}) SELECT {cols} FROM {spec.raw_fqn}{wp}")
        fp_stage = _fp(ch, spec, spec.stage_fqn)
        fp_fresh = _fp(ch, spec, spec.raw_fqn, wp)

        # проверка сборки: отпечатки аддитивны
        diff = rec.compare(spec, _add(spec, fp_kept, fp_fresh), fp_stage)
        dup_sql = rec.duplicates_sql(spec, spec.stage_fqn)
        dup = int(ch.scalar(dup_sql) or 0) if dup_sql else 0
        extra = [(l, int(ch.scalar(s) or 0)) for l, s in rec.extra_checks(spec, spec.stage_fqn)]
        extra = [(l, n) for l, n in extra if n]
        n_stage = int(fp_stage[0]) if fp_stage else 0

        res = {"partition": p, "kept": int(fp_kept[0]) if fp_kept else 0,
               "fresh": int(fp_fresh[0]) if fp_fresh else 0, "rows": n_stage}
        rjson = {"assembly_diff": [{"metric": n, "expected": a, "stage": b} for n, a, b in diff],
                 "duplicates": dup, "checks": dict(extra), "kept": res["kept"], "fresh": res["fresh"]}

        problem = None
        if diff:
            problem = "сборка партиции не сошлась: " + "; ".join(f"{n}: ожидалось {a}, в staging {b}"
                                                             for n, a, b in diff)
        elif dup:
            problem = f"дублей бизнес-ключа {dup}"
        elif extra:
            problem = "; ".join(f"{l}: {n}" for l, n in extra)
        elif n_stage == 0 and spec.empty_partition_policy == "fail":
            problem = "партиция опустела бы, empty_partition_policy=fail"
        if problem:
            res.update(status="failed", note=problem)
            eng._history(pg, spec, run_mode, p, "failed", started, res["fresh"], n_stage, None, rjson, problem)
            eng._save_state(pg, spec, p, "failed", error=problem)
            results.append(res)
            # дальше не идём: плохая партиция не публикуется, цель остаётся прежней
            return results

        existing = ch.partition_rows(spec.fqn, pexpr, p)
        if existing == 0 and n_stage > 0:
            ch.execute(f"ALTER TABLE {spec.stage_fqn} MOVE PARTITION {int(p)} TO TABLE {spec.fqn}")
            method = "MOVE PARTITION TO TABLE"
        else:
            ch.execute(f"ALTER TABLE {spec.fqn} REPLACE PARTITION {int(p)} FROM {spec.stage_fqn}")
            method = "REPLACE PARTITION"

        fp_after = _fp(ch, spec, spec.fqn, wp)
        if rec.compare(spec, fp_stage, fp_after):
            problem = "после публикации цель не совпала со staging"
            res.update(status="failed", note=problem, method=method)
            eng._history(pg, spec, run_mode, p, "failed", started, res["fresh"], n_stage, method, rjson, problem)
            eng._save_state(pg, spec, p, "failed", error=problem)
            results.append(res)
            return results

        res.update(status="ok", method=method, seconds=round(time.monotonic() - t0, 2))
        rjson["metrics"] = rec.to_json(spec, fp_stage, fp_after, [])["metrics"]
        eng._save_state(pg, spec, p, "ok", rows=n_stage, fingerprint=rjson["metrics"])
        eng._history(pg, spec, run_mode, p, "success", started, res["fresh"], n_stage, method, rjson)
        results.append(res)

    ch.execute(f"TRUNCATE TABLE {spec.stage_fqn}")
    ch.execute(f"TRUNCATE TABLE {spec.raw_fqn}")
    return results
