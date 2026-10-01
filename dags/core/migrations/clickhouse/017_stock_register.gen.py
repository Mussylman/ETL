"""Генератор миграции 017: регистр stock (склад) — конфиг извлечения 1С и shadow-цели ClickHouse."""
import json
q = lambda s: "NULL" if s is None else "'" + str(s).replace("'", "''") + "'"
arr = lambda xs: "ARRAY[" + ", ".join(q(x) for x in xs) + "]::text[]"
R = "_AccumRg17576"
RCV = "[{alias}].[_Fld17581_RRRef]"            # ДокументОприходования
# guid документа 1С уникален во всех таблицах — фильтр по _RTRef не нужен: в _Document390 найдутся только поступления
VT = f"FROM _Document390_VT9678 v WITH (NOLOCK) WHERE v._Document390_IDRRef={RCV} AND v._Fld9690RRef=[{{alias}}].[_Fld17577RRef]"
D390 = f"FROM _Document390 p WITH (NOLOCK) WHERE p._IDRRef={RCV}"
def d390_date(col): return f"(SELECT CASE WHEN p.{col}>'2001-01-02' THEN p.{col} END {D390})"
order_sub = f"(SELECT MAX(v._Fld9683RRef) {VT})"
# (target_column, source_column, is_expression, target_type, transform, onec_name)
main = [
 ("period", "_Period", False, "timestamp", "fix_year", "Период"),
 ("recorder", "_RecorderRRef", False, "uuid", "binary_to_uuid", "Регистратор"),
 ("recorder_type", "_RecorderTRef", False, "integer", "binary_to_int", "Регистратор.Тип"),
 ("line_no", "_LineNo", False, "integer", None, "НомерСтроки"),
 ("movement_type", "_RecordKind", False, "integer", None, "ВидДвижения"),
 ("raw_refs.nomenklatura", "_Fld17577RRef", False, "uuid", "binary_to_uuid", "Номенклатура"),
 ("nomenklatura_guid", "_Fld17577RRef", False, "uuid", "binary_to_uuid", "Номенклатура"),
 ("raw_refs.sklad", "_Fld17580RRef", False, "uuid", "binary_to_uuid", "Склад"),
 ("sklad_guid", "_Fld17580RRef", False, "uuid", "binary_to_uuid", "Склад"),
 ("raw_refs.kachestvo", "_Fld17585RRef", False, "uuid", "binary_to_uuid", "Качество"),
 ("kachestvo_guid", "_Fld17585RRef", False, "uuid", "binary_to_uuid", "Качество"),
 ("quantity", "_Fld17586", False, "numeric", None, "Количество"),
 ("cost_amount", "_Fld17587", False, "numeric", None, "Стоимость"),
 ("unit_cost", "CASE WHEN [{alias}].[_Fld17586]<>0 THEN [{alias}].[_Fld17587]/[{alias}].[_Fld17586] END", True, "numeric", None, "Стоимость/Количество"),
 ("operation_code", "_Fld17594RRef", False, "uuid", "binary_to_uuid", "КодОперации"),
 ("receipt_document_guid", "_Fld17581_RRRef", False, "uuid", "binary_to_uuid", "ДокументОприходования"),
 ("receipt_document_type", "_Fld17581_RTRef", False, "integer", "binary_to_int", "ДокументОприходования.Тип"),
 ("receipt_document_date", d390_date("_Date_Time"), True, "timestamp", "fix_year", "ДокументОприходования.Дата"),
 ("invoice_date", d390_date("_Fld24877"), True, "timestamp", "fix_year", "ДокументОприходования.ДатаИнвойса"),
 ("raw_refs.kontragent", f"(SELECT p._Fld9650RRef {D390})", True, "uuid", "binary_to_uuid", "ДокументОприходования.Контрагент"),
 ("kontragent_guid", f"(SELECT p._Fld9650RRef {D390})", True, "uuid", "binary_to_uuid", "ДокументОприходования.Контрагент"),
 ("supplier_order_guid", order_sub, True, "uuid", "binary_to_uuid", "Поступление.Товары.ЗаказПоставщику"),
 ("supplier_order_date", f"(SELECT o._Date_Time FROM _Document272 o WITH (NOLOCK) WHERE o._IDRRef={order_sub})", True, "timestamp", "fix_year", "ЗаказПоставщику.Дата"),
 ("price", f"(SELECT SUM(v._Fld9695)/NULLIF(SUM(v._Fld9685),0) {VT})", True, "numeric", None, "Поступление.Товары: Σ Сумма / Σ Количество"),
]
basis = {364: "_Fld8248_RRRef", 353: "_Fld7569_RRRef", 318: "_Fld6238_RRRef", 390: "_Fld9648_RRRef", 415: "_Fld11022_RRRef",
         442: "_Fld12249_RRRef", 308: "_Fld5791_RRRef", 254: "_Fld3922_RRRef", 387: "_Fld9310_RRRef", 339: "_Fld7014_RRRef",
         255: "_Fld4081_RRRef", 396: "_Fld10077_RRRef", 260: "_Fld4347_RRRef", 358: "_Fld7966RRef", 250: "_Fld3799RRef",
         443: "_Fld12325_RRRef", 316: None, 385: None, 26908: None}
hdr_inc = ["period", "recorder", "recorder_type", "document_date", "document_basis_guid"]
pos_inc = ["recorder", "recorder_type", "line_no", "period", "movement_type", "nomenklatura_guid", "sklad_guid", "kachestvo_guid",
           "quantity", "cost_amount", "unit_cost", "operation_code", "receipt_document_guid", "receipt_document_type",
           "receipt_document_date", "invoice_date", "kontragent_guid", "supplier_order_guid", "supplier_order_date", "price",
           "raw_refs.nomenklatura", "raw_refs.sklad", "raw_refs.kachestvo", "raw_refs.kontragent", "raw_refs"]
L = []
a = L.append
a("-- 017: склад (РегистрНакопления.ПартииТоваровНаСкладахБухгалтерскийУчет) — конфиг извлечения 1С и shadow-цели ClickHouse.")
a("-- Сгенерирован по аудиту docs/audits/Складские движения 1С … 2026-09-29.md. Факты в PostgreSQL не пишутся (pg_fact_write=false).")
a("-- Идемпотентна: повторный запуск ничего не делает, если регистр stock уже заведён.")
a("BEGIN;")
a("CREATE SEQUENCE IF NOT EXISTS etl_meta.doc_key_stock_seq;")
a("INSERT INTO etl_meta.doc_key_scope (doc_table, sequence_name, issuer) VALUES ('stock', 'etl_meta.doc_key_stock_seq', 'registry') ON CONFLICT (doc_table) DO NOTHING;")
a("DO $$")
a("DECLARE rid int; sid int; hsid int; th int; tp int; cs int; cp int;")
a("BEGIN")
a("  IF EXISTS (SELECT 1 FROM etl_meta.registers WHERE code = 'stock') THEN RAISE NOTICE 'stock уже заведён'; RETURN; END IF;")
a("  INSERT INTO etl_meta.registers (code, name, description, default_mode, retail_table, retail_uid_column, is_active, pipeline_type, pg_fact_write, dim_key_source)")
a("  VALUES ('stock', 'Складские движения', 'РегистрНакопления.ПартииТоваровНаСкладахБухгалтерскийУчет — только прямой путь в ClickHouse', 'incremental', 'stock_documents', 'document_uid', true, 'accumrg_with_documents', false, 'facts') RETURNING id INTO rid;")
a(f"  INSERT INTO etl_meta.register_sources (register_id, source_code, source_type, mssql_schema, mssql_table, where_clause, priority, is_active, onec_name, period_column)")
a(f"  VALUES (rid, 'stock', 'standalone', 'dbo', '{R}', '[{{alias}}].[_Active] = 0x01', 0, true, 'РегистрНакопления.ПартииТоваровНаСкладахБухгалтерскийУчет', '_Period') RETURNING id INTO sid;")
for t, s, ex, tt, tr, on in main:
    assert len(s) <= 255, (t, len(s))
    a(f"  INSERT INTO etl_meta.column_mappings (source_id, register_id, source_column, is_expression, target_column, target_type, transform_type, is_active, onec_name, is_auto, is_required)"
      f" VALUES (sid, rid, {q(s)}, {str(ex).lower()}, {q(t)}, {q(tt)}, {q(tr)}, true, {q(on)}, false, false);")
a(f"  INSERT INTO etl_meta.register_targets (register_id, target_schema, target_table, source_id, load_mode, upsert_keys, include_columns, priority, target_role, is_active)"
  f" VALUES (rid, 'public', 'stock', sid, 'upsert', {arr(['recorder','recorder_type'])}, {arr(hdr_inc)}, 0, 'dimension', true) RETURNING id INTO th;")
a(f"  INSERT INTO etl_meta.register_targets (register_id, target_schema, target_table, source_id, load_mode, upsert_keys, include_columns, priority, target_role, parent_target_id, is_active)"
  f" VALUES (rid, 'public', 'stock_positions', sid, 'upsert', {arr(['recorder','recorder_type','line_no'])}, {arr(pos_inc)}, 1, 'fact', th, true) RETURNING id INTO tp;")
for i, (t, col) in enumerate(sorted(basis.items())):
    a(f"  INSERT INTO etl_meta.register_sources (register_id, source_code, source_type, mssql_schema, mssql_table, priority, is_active) VALUES (rid, 'doc_{t}', 'header', 'dbo', '_Document{t}', {10 + i}, true) RETURNING id INTO hsid;")
    a(f"  INSERT INTO etl_meta.column_mappings (source_id, register_id, source_column, is_expression, target_column, target_type, transform_type, is_active, onec_name, target_id, is_auto, is_required) VALUES (hsid, rid, '_Date_Time', false, 'document_date', 'timestamp', 'fix_year', true, 'Дата', th, false, false);")
    if col:
        a(f"  INSERT INTO etl_meta.column_mappings (source_id, register_id, source_column, is_expression, target_column, target_type, transform_type, is_active, onec_name, target_id, is_auto, is_required) VALUES (hsid, rid, '{col}', false, 'document_basis_guid', 'uuid', 'binary_to_uuid', true, 'ДокументОснование', th, false, false);")
# --- ClickHouse shadow
sig = [{"table": "stock_documents", "key_column": "document_uid", "updated_at_column": "updated_at"},
       {"table": "transfers", "key_column": "guid", "updated_at_column": "updated_at"},
       {"table": "adjustments", "key_column": "guid", "updated_at_column": "updated_at"},
       {"table": "sales", "key_column": "document_uid", "updated_at_column": "updated_at", "filter": "doc_type IN (6, 10)"}]
hp = {"own_id": True, "shadow": True, "target": "stock", "doc_key": ["recorder"], "state_key": "onec_register:stock:shadow", "signal_sources": sig}
pp = {"parent": "stock", "shadow": True, "target": "stock_positions", "doc_key": ["recorder"], "state_key": "onec_register:stock:shadow",
      "parent_key": ["recorder", "recorder_type"], "parent_prefix": "hdr_"}
def ch(code, table, order_by, bk, checksum, measures, rm, params, prio, cols):
    a(f"  INSERT INTO etl_meta.ch_sync (code, description, source_conn_id, source_type, source_object, target_database, target_table, partition_expr, order_by, load_mode, partition_column, partition_granularity, business_key, batch_size, empty_partition_policy, hot_window, sweep_interval_min, checksum_columns, measure_columns, reconcile_metrics, priority, is_active, source_params, sync_group)")
    a(f"  VALUES ({q(code)}, {q('SHADOW складских движений 1С → ClickHouse (' + table + '). Не активировать до PASS.')}, 'mssql_1c_conn', 'onec_register', 'stock', 'analytics_poc', {q(table)}, 'toYYYYMM(period)',"
      f" {arr(order_by)}, 'document_patch', 'period', 'month', {arr(bk)}, 100000, 'fail', 3, 60, {arr(checksum)}, {arr(measures)}, {q(json.dumps(rm))}::jsonb, {prio}, false, {q(json.dumps(params, ensure_ascii=False))}::jsonb, 'shadow_stock') RETURNING id INTO {'cs' if prio == 130 else 'cp'};")
    for i, (src, tgt, typ, codec) in enumerate(cols, 1):
        a(f"  INSERT INTO etl_meta.ch_sync_columns (sync_id, ordinal, source_expr, target_column, target_type, codec) VALUES ({'cs' if prio == 130 else 'cp'}, {i}, {q(src)}, {q(tgt)}, {q(typ)}, {q(codec)});")
D, Z, T64, U = "Delta, ZSTD(1)", "ZSTD(1)", "T64, ZSTD(1)", "UUID"
ch("fact_stock_shadow", "fact_stock_shadow", ["period", "recorder_type", "recorder"], ["recorder", "recorder_type"],
   ["recorder", "recorder_type", "document_date", "document_basis_guid"], [], {"unique": [["recorder", "recorder_type"]], "not_zero": ["id"]}, hp, 130,
   [("id", "id", "UInt64", T64), ("recorder", "recorder", U, Z), ("recorder_type", "recorder_type", "UInt16", T64),
    ("period", "period", "DateTime", D), ("document_date", "document_date", "DateTime", D),
    ("document_basis_guid", "document_basis_guid", U, Z),
    ("retail_updated_at", "source_updated_at", "DateTime", D), ("etl_updated_at", "loaded_at", "DateTime", D)])
ch("fact_stock_positions_shadow", "fact_stock_positions_shadow", ["period", "sklad_id", "nomenklatura_id", "recorder", "line_no"], ["recorder", "recorder_type", "line_no"],
   ["recorder", "recorder_type", "line_no", "nomenklatura_id", "sklad_id", "kachestvo_id", "kontragent_id", "movement_type", "operation_code",
    "receipt_document_guid", "supplier_order_guid"], ["quantity", "cost_amount"],
   {"unique": [["recorder", "recorder_type", "line_no"]], "not_zero": ["stock_id", "nomenklatura_id", "sklad_id"]}, pp, 131,
   [("hdr_id", "stock_id", "UInt64", T64), ("recorder", "recorder", U, Z), ("recorder_type", "recorder_type", "UInt16", T64),
    ("line_no", "line_no", "UInt32", T64), ("period", "period", "DateTime", D),
    ("nomenklatura_id", "nomenklatura_id", "UInt32", T64), ("nomenklatura_guid", "nomenklatura_guid", U, Z),
    ("sklad_id", "sklad_id", "UInt16", T64), ("sklad_guid", "sklad_guid", U, Z),
    ("kachestvo_id", "kachestvo_id", "UInt8", T64), ("kachestvo_guid", "kachestvo_guid", U, Z),
    ("movement_type", "movement_type", "UInt8", T64), ("quantity", "quantity", "Decimal(18,4)", Z),
    ("cost_amount", "cost_amount", "Decimal(18,4)", Z), ("unit_cost", "unit_cost", "Decimal(18,4)", Z),
    ("operation_code", "operation_code", U, Z), ("hdr_document_basis_guid", "document_basis_guid", U, Z),
    ("receipt_document_guid", "receipt_document_guid", U, Z), ("receipt_document_type", "receipt_document_type", "UInt16", T64),
    ("receipt_document_date", "receipt_document_date", "DateTime", D), ("invoice_date", "invoice_date", "DateTime", D),
    ("supplier_order_guid", "supplier_order_guid", U, Z), ("supplier_order_date", "supplier_order_date", "DateTime", D),
    ("kontragent_id", "kontragent_id", "UInt32", T64), ("kontragent_guid", "kontragent_guid", U, Z),
    ("price", "price", "Decimal(18,4)", Z), ("hdr_retail_updated_at", "source_updated_at", "DateTime", D),
    ("etl_updated_at", "loaded_at", "DateTime", D)])
a("END $$;")
a("INSERT INTO etl_meta.ch_sync_group (sync_group, dag_id, position, is_active, runner, description)")
a("VALUES ('shadow_stock', 'analytics_sync', 16, false, 'ch_sync', 'shadow складских движений 1С → ClickHouse; включается отдельно') ON CONFLICT (sync_group) DO NOTHING;")
a("COMMIT;")
open("/home/dev/airflow/dags/core/migrations/clickhouse/017_stock_register.sql", "w").write("\n".join(L) + "\n")
print("строк SQL:", len(L))
