---
tags: [session, dwh, dim, nomenklatura, pilot, retail]
date: 2026-07-28
---

# Сессия 2026-07-27/28: retail-доказательство, аудит int-FK, пилот dim_nomenklatura

## Сделано
1. **Еженедельная сверка** нашла 4 разъехавшихся дня → по-документная диагностика → все виновники
   не-кассовые документы (415 перепроведения, 352, возврат без чека) → дни перезагружены, всё в ноль.
2. **Доказательный пакет для инженера retail** (`docs/audits/retail_signal_gaps_2026-07-27.md`):
   правки 1С не двигают retail.updated_at (created=updated у всех 5 кейсов); 352/возвраты в retail
   отсутствуют полностью. Инженер подтвердил, обещал фикс до ~01.08 (см. заметку интеграции).
3. **Storage-аудит** (`reports/storage_audit_2026-07-24.md`): PG 14 не Docker; sales БОЛЬШЕ positions
   (10 uuid = 43% heap); экономия — мегабайты, мотив int-ключей = Power BI, не диск.
4. **Аудит целевой архитектуры int-FK** (`reports/dwh_int_fk_readiness_2026-07-27.md`):
   реализуемо; NOT NULL FK нельзя; raw_refs ×3.3 (только для полиморфных); isHidden не защищает
   модель PBI — только bi.* views; канон guid подтверждён по всем путям (bytea в DWH ноль).
5. **Пилот guid→id РЕАЛИЗОВАН** (`reports/pilot_dim_nomenklatura_plan_2026-07-27.md` + журнал):
   dao.py не дропает *_id; dim_nomenklatura (id IDENTITY, guid UNIQUE, is_stub); nomenklatura_id
   в факте; backfill 9 905 stub'ов / 121 977 строк / unresolved=0; stub+resolve в post_load_sql
   (обе дороги); валидация 5.1 generic по парам (<x> uuid, <x>_id); тик и full_period зелёные,
   3 stub'а родились на лету.

## Решения пользователя
- Пилот одобрен с шагом 5.1; шаг 7 (наполнение имён из 1С) — отдельное «ок» после наблюдения 2-3 дня.
- Reprocess DAG по-прежнему не делаем — еженедельный sales_recon.py руками.

## Открытое
- Наблюдение пилота до ~30.07 → «ок» на шаг 7 (найти _ReferenceNNN через meta API)
- Проверка фикса retail (~01.08): updated_at при перепроведении; вошли ли 352/возвраты
- Далее: bi.* витрина + роль PBI; остальные 8 dim; бэкап etl_meta
