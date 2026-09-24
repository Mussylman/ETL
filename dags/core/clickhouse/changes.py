"""
Классификация набора изменений: сигнал ≠ готовность данных.

Источник сигнала (retail) сообщает «документ изменился» раньше, чем авторитетный
источник (1С) его получил. Поэтому отсутствие документа в источнике само по себе
ничего не значит, и различать нужно явно:

  signal (окно + хвост)  → candidate keys
  отбросить невалидные   → valid
  lookup в источнике     → ready        — есть в источнике, идут в патч
  нет в источнике и нет в текущей витрине → pending — ждут, ничего не трогаем
  нет в источнике, но есть в витрине      → deleted — патч удаляет

Модуль не знает ни одной таблицы: источник готовности и витрина передаются
функциями, их конкретика — в конфигурации источника.
"""

from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Set
from uuid import UUID

import pandas as pd

EMPTY_KEY = "00000000-0000-0000-0000-000000000000"


def norm_key(v) -> str:
    """Ключ документа → каноничный uuid; пустой/невалидный → ''."""
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    try:
        u = str(UUID(s))
    except ValueError:
        return ""
    return "" if u == EMPTY_KEY else u


@dataclass
class ChangeSet:
    from_ts: str
    to_ts: str
    retail_changed: int = 0          # сигналы в основном окне
    tail_candidates: int = 0         # добор хвоста: сигнал был раньше, в витрине документа нет
    invalid: int = 0                 # NULL / пусто / не uuid / пустая ссылка
    signal_ts: Dict[str, object] = field(default_factory=dict)   # valid key → updated_at
    ready: Set[str] = field(default_factory=set)
    pending: Set[str] = field(default_factory=set)
    deleted: Set[str] = field(default_factory=set)

    @property
    def valid(self) -> int:
        return len(self.signal_ts)

    @property
    def patch_keys(self) -> List[str]:
        """Документы, которые патч пересобирает: свежие из источника и удалённые."""
        return sorted(self.ready | self.deleted)

    def summary(self) -> Dict:
        return {"window": (self.from_ts, self.to_ts), "retail_changed": self.retail_changed,
                "tail_candidates": self.tail_candidates, "valid_guid": self.valid,
                "invalid_guid": self.invalid, "ready_in_source": len(self.ready),
                "pending_source": len(self.pending), "actually_deleted": len(self.deleted)}


def classify(signal: pd.DataFrame, from_ts: str, to_ts: str, *,
             ready_lookup: Callable[[List[str]], Iterable[str]],
             present_lookup: Callable[[List[str]], Iterable[str]]) -> ChangeSet:
    """
    signal         — df[uid, updated_at]: окно и хвост вместе (хвост — updated_at < from_ts)
    ready_lookup   — ключи → какие из них есть в авторитетном источнике (точный запрос)
    present_lookup — ключи → какие из них уже лежат в витрине
    """
    cs = ChangeSet(from_ts=str(from_ts), to_ts=str(to_ts))
    if signal is None or signal.empty:
        return cs
    ts = pd.to_datetime(signal["updated_at"])
    in_tail = ts < pd.Timestamp(from_ts)
    cs.tail_candidates = int(in_tail.sum())
    cs.retail_changed = int((~in_tail).sum())

    for u, t in zip(signal["uid"].tolist(), signal["updated_at"].tolist()):
        k = norm_key(u)
        if not k:
            cs.invalid += 1
            continue
        if k not in cs.signal_ts or t > cs.signal_ts[k]:
            cs.signal_ts[k] = t

    keys = sorted(cs.signal_ts)
    if not keys:
        return cs
    cs.ready = {norm_key(x) for x in ready_lookup(keys)} & set(keys)
    absent = [k for k in keys if k not in cs.ready]
    if absent:
        cs.deleted = {norm_key(x) for x in present_lookup(absent)} & set(absent)
    cs.pending = set(absent) - cs.deleted
    return cs
