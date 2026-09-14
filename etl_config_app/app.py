"""
ETL Config — standalone web application.
FastAPI + Jinja2, port 5555.

Run:  python app.py
  or: uvicorn app:app --host 0.0.0.0 --port 5555 --reload
"""

import json
from pathlib import Path
from fastapi import FastAPI, Request, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn

import dao
import onec_client
import mssql_client


# MSSQL type → (target_type, transform_type)
_MSSQL_TYPE_RULES = {
    ("binary", 16):  ("uuid", "binary_auto"),
    ("binary", 1):   ("boolean", "binary_auto"),
    ("binary", 4):   ("integer", "binary_auto"),
    ("datetime", None): ("timestamp", "fix_year"),
    ("numeric", None):  ("numeric", None),
    ("int", None):      ("integer", None),
    ("bigint", None):   ("bigint", None),
    ("float", None):    ("numeric", None),
    ("real", None):     ("numeric", None),
    ("nchar", None):    ("varchar", None),
    ("nvarchar", None): ("varchar", None),
    ("ntext", None):    ("text", None),
    ("text", None):     ("text", None),
    ("image", None):    ("bytea", None),
    ("bit", None):      ("boolean", None),
    ("date", None):     ("date", None),
    ("varbinary", None): ("bytea", None),
}


def _resolve_mssql_type(data_type: str, max_length=None) -> tuple:
    """Resolve MSSQL data type to (target_type, transform_type)."""
    # Try exact match with length first (for binary variants)
    result = _MSSQL_TYPE_RULES.get((data_type, max_length))
    if result:
        return result
    # Fallback to type-only match
    result = _MSSQL_TYPE_RULES.get((data_type, None))
    if result:
        return result
    return ("text", None)


def _enrich_fields_with_mssql_types(fields: list, col_types: dict):
    """Add mssql_type, target_type, transform_type to each field based on MSSQL column info."""
    for f in fields:
        sql_name = f.get("field_name_sql", "")
        # MSSQL column has _ prefix: _Fld13608RRef for API's Fld13608
        # Also try with RRef suffix for reference fields
        mssql_col = f"_{sql_name}"
        info = col_types.get(mssql_col)
        if not info:
            # Try with RRef suffix (reference fields)
            info = col_types.get(f"{mssql_col}RRef")
            if info:
                mssql_col = f"{mssql_col}RRef"
        if info:
            dt = info["data_type"]
            ml = info.get("max_length")
            target_type, transform = _resolve_mssql_type(dt, ml)
            f["mssql_column"] = mssql_col
            f["mssql_type"] = dt
            f["mssql_length"] = ml
            f["target_type"] = target_type
            f["transform_type"] = transform


def _resolve_onec_field_name(onec_table: str, source_column: str) -> str | None:
    """Lookup Russian field name from 1C API by MSSQL column name."""
    structs = onec_client.get_structure([onec_table])
    if not structs or not structs[0].get("fields"):
        return None
    col = source_column.lstrip("_").lower()
    for f in structs[0]["fields"]:
        key = f["field_name_sql"].lower()
        if key == col or (key == "recorder" and col == "recorderrref") or (key == "id" and col == "idrref"):
            return f["field_name"]
    return None


def _sync_targets_for_source(src_id: int):
    """Find all targets linked to this source (directly or via union) and sync them."""
    source = dao.get_source(src_id)
    if not source:
        return
    targets = dao.list_targets_for_register(source["register_id"])
    for t in targets:
        if t.get("source_id") == src_id:
            dao.sync_target_table(t["id"])
        elif t.get("union_id"):
            members = dao.list_members_for_union(t["union_id"])
            if any(m["source_id"] == src_id for m in members):
                dao.sync_target_table(t["id"])

app = FastAPI(title="ETL Config")
BASE = Path(__file__).parent

app.mount("/static", StaticFiles(directory=BASE / "static"), name="static")
templates = Jinja2Templates(directory=BASE / "templates")
# Метка окружения во всех шаблонах: «TEST / test» или «PROD / etl_prod» (см. dao.DB_CONFIG)
templates.env.globals.update(
    ENV_LABEL=dao.ENV_LABEL, IS_PROD=dao.IS_PROD, ALLOW_DESTRUCTIVE=dao.ALLOW_DESTRUCTIVE,
    DB_NAME=dao.DB_CONFIG["dbname"],
)


# ────────────────────────────────────────────
#  Template helpers
# ────────────────────────────────────────────
def _tpl(name: str, request: Request, **ctx):
    return templates.TemplateResponse(name, {"request": request, **ctx})


# ────────────────────────────────────────────
#  PAGES: Registers
# ────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return RedirectResponse("/registers", status_code=302)


@app.get("/registers", response_class=HTMLResponse)
async def register_list(request: Request):
    regs = dao.list_registers(include_inactive=True)
    return _tpl("registers/list.html", request, registers=regs)


@app.get("/registers/new", response_class=HTMLResponse)
async def register_new(request: Request):
    all_regs = dao.list_registers()
    return _tpl("registers/form.html", request, register=None, all_registers=all_regs)


@app.post("/registers/new")
async def register_create(
    request: Request,
    code: str = Form(...),
    name: str = Form(""),
    description: str = Form(""),
    default_mode: str = Form("incremental"),
    retail_table: str = Form(""),
    retail_uid_column: str = Form(""),
    parent_id: str = Form(""),
    pipeline_type: str = Form(""),
):
    dao.create_register({
        "code": code, "name": name, "description": description,
        "default_mode": default_mode,
        "retail_table": retail_table or None,
        "retail_uid_column": retail_uid_column or None,
        "parent_id": int(parent_id) if parent_id else None,
        "pipeline_type": pipeline_type or None,
    })
    return RedirectResponse("/registers", status_code=303)


# Wizard страницы — обязательно ВЫШЕ /registers/{reg_id}, иначе FastAPI
# попытается распарсить «wizard» как int и вернёт 422.
@app.get("/registers/wizard", response_class=HTMLResponse)
async def wizard_index(request: Request):
    """Главный экран — список шаблонов 'Создать витрину'."""
    return _tpl("registers/wizard_index.html", request)


@app.get("/registers/wizard/accumrg", response_class=HTMLResponse)
async def wizard_accumrg(request: Request):
    """Шаблон 'Регистр накопления + документы + VT'."""
    return _tpl("registers/wizard.html", request)


@app.get("/registers/{reg_id}", response_class=HTMLResponse)
async def register_detail(request: Request, reg_id: int):
    reg = dao.get_register(reg_id)
    if not reg:
        return RedirectResponse("/registers", status_code=302)
    sources = dao.list_sources_for_register(reg_id)
    unions = dao.list_unions_for_register(reg_id)
    targets = dao.list_targets_for_register(reg_id)
    history = dao.list_load_history(reg_id)
    parent_reg = dao.get_register(reg.get("parent_id")) if reg.get("parent_id") else None
    # Union — не скрытая механика: показываем членов, output_columns и таргет, который
    # им питается (document_with_vt: _VT4708 + _VT4746 → order_positions_u → order_positions).
    for u in unions:
        u["members"] = dao.list_members_for_union(u["id"])
        u["fed_targets"] = [t["target_table"] for t in targets if t.get("union_id") == u["id"]]
    target_names = {t["id"]: t["target_table"] for t in targets}
    for t in targets:
        t["parent_target_table"] = target_names.get(t.get("parent_target_id"))
        t["has_post_load"] = bool(t.get("post_load_sql"))
        t["include_count"] = len(t.get("include_columns") or [])
    return _tpl("registers/detail.html", request,
                register=reg, sources=sources, unions=unions,
                targets=targets, history=history, parent_register=parent_reg)


@app.get("/registers/{reg_id}/edit", response_class=HTMLResponse)
async def register_edit(request: Request, reg_id: int):
    reg = dao.get_register(reg_id)
    all_regs = dao.list_registers()
    return _tpl("registers/form.html", request, register=reg, all_registers=all_regs)


@app.post("/registers/{reg_id}/edit")
async def register_update(
    request: Request, reg_id: int,
    code: str = Form(...),
    name: str = Form(""),
    description: str = Form(""),
    default_mode: str = Form("incremental"),
    retail_table: str = Form(""),
    retail_uid_column: str = Form(""),
    parent_id: str = Form(""),
    pipeline_type: str = Form(""),
):
    # PATCH: join-ключи (ставит Discover) и recorder_type_map не присылаются формой — не трогаем
    dao.update_register(reg_id, {
        "code": code, "name": name, "description": description,
        "default_mode": default_mode,
        "retail_table": retail_table or None,
        "retail_uid_column": retail_uid_column or None,
        "parent_id": int(parent_id) if parent_id else None,
        "pipeline_type": pipeline_type or None,
    })
    return RedirectResponse(f"/registers/{reg_id}", status_code=303)


def _refused(title: str, err: Exception, back: str):
    """Отказ защитной проверки: ничего не изменено, объясняем и даём ссылку назад."""
    return HTMLResponse(
        f"<div style='font-family:sans-serif;padding:24px'><h3>{title}</h3>"
        f"<p>{err}</p><p><a href='{back}'>← назад</a></p></div>",
        status_code=409,
    )


@app.post("/registers/{reg_id}/delete")
async def register_delete(reg_id: int):
    try:
        dao.delete_register(reg_id)
    except ValueError as e:
        return _refused("Регистр не удалён", e, f"/registers/{reg_id}")
    return RedirectResponse("/registers", status_code=303)


@app.post("/registers/{reg_id}/toggle")
async def register_toggle(reg_id: int):
    reg = dao.get_register(reg_id)
    if reg:
        dao.toggle_register(reg_id, not reg["is_active"])
    return RedirectResponse(f"/registers/{reg_id}", status_code=303)


def _period_column_from_form(value):
    """Селект формы: '__auto__' → None (дефолт по типу таблицы в dao), '__none__' → '' (без фильтра)."""
    if value is None or value == "__auto__":
        return None
    if value == "__none__":
        return ""
    return value


# ────────────────────────────────────────────
#  PAGES: Sources
# ────────────────────────────────────────────
@app.get("/registers/{reg_id}/sources/new", response_class=HTMLResponse)
async def source_new(request: Request, reg_id: int):
    reg = dao.get_register(reg_id)
    sources = dao.list_sources_for_register(reg_id)
    return _tpl("sources/form.html", request,
                register=reg, source=None, all_sources=sources)


@app.post("/registers/{reg_id}/sources/new")
async def source_create(
    request: Request, reg_id: int,
    source_code: str = Form(...),
    source_type: str = Form("header"),
    mssql_schema: str = Form("dbo"),
    mssql_table: str = Form(...),
    onec_name: str = Form(""),
    parent_source_id: str = Form(""),
    join_type: str = Form(""),
    join_key_source: str = Form(""),
    join_key_parent: str = Form(""),
    where_clause: str = Form(""),
    priority: int = Form(0),
    period_column: str = Form("__auto__"),
):
    payload = {
        "register_id": reg_id, "source_code": source_code,
        "source_type": source_type, "mssql_schema": mssql_schema,
        "mssql_table": mssql_table, "onec_name": onec_name or None,
        "parent_source_id": int(parent_source_id) if parent_source_id else None,
        "join_type": join_type or None,
        "join_key_source": join_key_source or None,
        "join_key_parent": join_key_parent or None,
        "where_clause": where_clause or None,
        "priority": priority,
    }
    pc = _period_column_from_form(period_column)
    if pc is not None:
        payload["period_column"] = pc      # иначе dao.create_source подставит дефолт по типу таблицы
    sid = dao.create_source(payload)
    # Auto-create mappings for system fields from 1C
    if onec_name:
        fields = onec_client.get_structure([onec_name])
        if fields and fields[0].get("fields"):
            dao.auto_create_mappings(sid, fields[0]["fields"])
    return RedirectResponse(f"/sources/{sid}", status_code=303)


@app.get("/sources/{src_id}", response_class=HTMLResponse)
async def source_detail(request: Request, src_id: int):
    source = dao.get_source(src_id)
    if not source:
        return RedirectResponse("/registers", status_code=302)
    mappings = dao.list_mappings_for_source(src_id)
    targets = dao.list_targets_for_register(source["register_id"])
    return _tpl("sources/detail.html", request,
                source=source, mappings=mappings, targets=targets)


@app.get("/sources/{src_id}/edit", response_class=HTMLResponse)
async def source_edit(request: Request, src_id: int):
    source = dao.get_source(src_id)
    reg = dao.get_register(source["register_id"])
    sources = dao.list_sources_for_register(source["register_id"])
    return _tpl("sources/form.html", request,
                register=reg, source=source, all_sources=sources)


@app.post("/sources/{src_id}/edit")
async def source_update(
    request: Request, src_id: int,
    source_code: str = Form(...),
    source_type: str = Form("header"),
    mssql_schema: str = Form("dbo"),
    mssql_table: str = Form(...),
    onec_name: str = Form(""),
    parent_source_id: str = Form(""),
    join_type: str = Form(""),
    join_key_source: str = Form(""),
    join_key_parent: str = Form(""),
    where_clause: str = Form(""),
    priority: int = Form(0),
    period_column: str = Form("__auto__"),
):
    source = dao.get_source(src_id)
    payload = {
        "source_code": source_code, "source_type": source_type,
        "mssql_schema": mssql_schema, "mssql_table": mssql_table,
        "onec_name": onec_name or None,
        "parent_source_id": int(parent_source_id) if parent_source_id else None,
        "join_type": join_type or None,
        "join_key_source": join_key_source or None,
        "join_key_parent": join_key_parent or None,
        "where_clause": where_clause or None,
        "priority": priority,
    }
    pc = _period_column_from_form(period_column)
    if pc is not None:
        payload["period_column"] = pc
    elif source and source.get("period_column") is None:
        payload["period_column"] = dao.default_period_column(source_type, mssql_table)
    dao.update_source(src_id, payload)
    return RedirectResponse(f"/sources/{src_id}", status_code=303)


@app.post("/sources/{src_id}/delete")
async def source_delete(src_id: int):
    source = dao.get_source(src_id)
    reg_id = source["register_id"] if source else None
    try:
        dao.delete_source(src_id)
    except ValueError as e:
        return _refused("Источник не удалён", e, f"/sources/{src_id}")
    return RedirectResponse(f"/registers/{reg_id}" if reg_id else "/registers", status_code=303)


# ────────────────────────────────────────────
#  PAGES: Column Mappings
# ────────────────────────────────────────────
@app.get("/sources/{src_id}/mappings/new", response_class=HTMLResponse)
async def mapping_new(request: Request, src_id: int):
    source = dao.get_source(src_id)
    return _tpl("mappings/form.html", request, source=source, mapping=None)


@app.post("/sources/{src_id}/mappings/new")
async def mapping_create(
    request: Request, src_id: int,
    source_column: str = Form(...),
    target_column: str = Form(...),
    is_expression: bool = Form(False),
    target_type: str = Form(""),
    transform_type: str = Form(""),
    transform_params: str = Form(""),
    default_value: str = Form(""),
    is_nullable: bool = Form(True),
):
    source = dao.get_source(src_id)
    # Физическая истина — MSSQL: мэппинг в несуществующую колонку падает на первом SELECT.
    if source and not is_expression:
        try:
            col_types = mssql_client.get_column_types(source.get("mssql_table", ""))
        except Exception:
            col_types = None
        if col_types is not None and source_column not in col_types:
            return _refused("Мэппинг не создан",
                            ValueError(f"колонки «{source_column}» нет в {source.get('mssql_table')} (UPP_JAN). "
                                       f"Имя из meta API — только подсказка; выберите поле со статусом «MSSQL ✓»."),
                            f"/sources/{src_id}/mappings/new")
    # Resolve 1C Russian name for this column
    onec_name = None
    if source and source.get("onec_name"):
        try:
            onec_name = _resolve_onec_field_name(source["onec_name"], source_column)
        except Exception:
            pass
    dao.create_mapping({
        "source_id": src_id,
        "source_column": source_column,
        "target_column": target_column,
        "is_expression": is_expression,
        "target_type": target_type or None,
        "transform_type": transform_type or None,
        "transform_params": transform_params or None,
        "default_value": default_value or None,
        "is_nullable": is_nullable,
        "onec_name": onec_name,
    })
    _sync_targets_for_source(src_id)
    return RedirectResponse(f"/sources/{src_id}", status_code=303)


@app.get("/mappings/{map_id}/edit", response_class=HTMLResponse)
async def mapping_edit(request: Request, map_id: int):
    mapping = dao.get_mapping(map_id)
    source = dao.get_source(mapping["source_id"])
    onec_field_name = mapping.get("onec_name")
    return _tpl("mappings/form.html", request, source=source, mapping=mapping, onec_field_name=onec_field_name)


@app.post("/mappings/{map_id}/edit")
async def mapping_update(
    request: Request, map_id: int,
    source_column: str = Form(...),
    target_column: str = Form(...),
    is_expression: bool = Form(False),
    target_type: str = Form(""),
    transform_type: str = Form(""),
    transform_params: str = Form(""),
    default_value: str = Form(""),
    is_nullable: bool = Form(True),
):
    mapping = dao.get_mapping(map_id)
    dao.update_mapping(map_id, {
        "source_column": source_column,
        "target_column": target_column,
        "is_expression": is_expression,
        "target_type": target_type or None,
        "transform_type": transform_type or None,
        "transform_params": transform_params or None,
        "default_value": default_value or None,
        "is_nullable": is_nullable,
    })
    _sync_targets_for_source(mapping['source_id'])
    return RedirectResponse(f"/sources/{mapping['source_id']}", status_code=303)


@app.post("/mappings/{map_id}/delete")
async def mapping_delete(map_id: int):
    mapping = dao.get_mapping(map_id)
    src_id = mapping["source_id"] if mapping else None
    dao.delete_mapping(map_id)
    return RedirectResponse(f"/sources/{src_id}" if src_id else "/registers", status_code=303)


@app.post("/mappings/{map_id}/toggle")
async def mapping_toggle(map_id: int):
    mapping = dao.get_mapping(map_id)
    if mapping:
        dao.toggle_mapping(map_id, not mapping.get("is_active", True))
    src_id = mapping["source_id"] if mapping else None
    return RedirectResponse(f"/sources/{src_id}" if src_id else "/registers", status_code=303)


# Batch save mappings from 1C API columns
@app.post("/sources/{src_id}/mappings/batch")
async def mapping_batch(request: Request, src_id: int):
    """
    Добавляет колонки в источник и доводит их до реальной таблицы.

    Без привязки к register_id/target_id мэппинг остаётся «сиротой»: Sync его
    не видит (собирает по target.include_columns), движок не читает (читает по
    register_id) — колонка молча не появляется в БД. Поэтому здесь мы:
      1) проставляем register_id/target_id,
      2) дописываем target_column в include_columns таргета,
      3) только потом запускаем Sync.
    """
    body = await request.json()
    mappings = body.get("mappings", [])
    if not mappings:
        return JSONResponse({"ok": False, "error": "не передано ни одной колонки"},
                            status_code=400)

    source = dao.get_source(src_id)
    if not source:
        return JSONResponse({"ok": False, "error": f"источник {src_id} не найден"},
                            status_code=404)
    reg_id = source["register_id"]

    # Колонки, которых нет в источнике, добавлять нельзя: SELECT к 1С упадёт
    # на первом же прогоне. Такое приезжает из meta API, когда реквизит есть в
    # конфигурации, но физической колонки в этой базе нет (см. fields_cache
    # с пустым mssql_column).
    known = set()
    try:
        cache = source.get("fields_cache") or []
        if isinstance(cache, str):
            cache = json.loads(cache)
        known = {(f.get("mssql_column") or "").lower() for f in cache if f.get("mssql_column")}
    except Exception:
        known = set()
    if known:
        unknown = [m["source_column"] for m in mappings
                   if (m.get("source_column") or "").lower() not in known]
        if unknown:
            return JSONResponse({
                "ok": False,
                "error": "нет таких колонок в источнике: " + ", ".join(unknown)
                         + ". Реквизит есть в конфигурации 1С, но колонки в этой базе нет.",
            }, status_code=400)

    targets = dao.list_targets_for_register(reg_id)
    created = 0
    for m in mappings:
        m["source_id"] = src_id
        m["register_id"] = reg_id
        tgt = next((t for t in targets if t.get("source_id") == src_id), None)
        if tgt is None and len(targets) == 1:
            tgt = targets[0]
        if tgt:
            m["target_id"] = tgt["id"]
        dao.create_mapping(m)
        created += 1
        if tgt:
            dao.add_include_column(tgt["id"], m["target_column"])

    _sync_targets_for_source(src_id)
    return JSONResponse({"ok": True, "count": created})


# ────────────────────────────────────────────
#  PAGES: Unions
# ────────────────────────────────────────────
@app.get("/registers/{reg_id}/unions/new", response_class=HTMLResponse)
async def union_new(request: Request, reg_id: int):
    reg = dao.get_register(reg_id)
    return _tpl("unions/form.html", request, register=reg, union=None)


@app.post("/registers/{reg_id}/unions/new")
async def union_create(
    request: Request, reg_id: int,
    union_code: str = Form(...),
    description: str = Form(""),
    output_columns: str = Form(""),
):
    uid = dao.create_union({
        "register_id": reg_id, "union_code": union_code,
        "description": description or None,
        "output_columns": output_columns,
    })
    return RedirectResponse(f"/unions/{uid}", status_code=303)


@app.get("/unions/{union_id}", response_class=HTMLResponse)
async def union_detail(request: Request, union_id: int):
    union = dao.get_union(union_id)
    if not union:
        return RedirectResponse("/registers", status_code=302)
    members = dao.list_members_for_union(union_id)
    sources = dao.list_sources_for_register(union["register_id"])
    return _tpl("unions/detail.html", request,
                union=union, members=members, sources=sources)


@app.get("/unions/{union_id}/edit", response_class=HTMLResponse)
async def union_edit(request: Request, union_id: int):
    union = dao.get_union(union_id)
    reg = dao.get_register(union["register_id"])
    return _tpl("unions/form.html", request, register=reg, union=union)


@app.post("/unions/{union_id}/edit")
async def union_update(
    request: Request, union_id: int,
    union_code: str = Form(...),
    description: str = Form(""),
    output_columns: str = Form(""),
):
    dao.update_union(union_id, {
        "union_code": union_code,
        "description": description or None,
        "output_columns": output_columns,
    })
    return RedirectResponse(f"/unions/{union_id}", status_code=303)


@app.post("/unions/{union_id}/delete")
async def union_delete(union_id: int):
    union = dao.get_union(union_id)
    reg_id = union["register_id"] if union else None
    try:
        dao.delete_union(union_id)
    except ValueError as e:
        # union питает таргет — не удаляем ничего, объясняем и возвращаем на страницу union
        back = f"/unions/{union_id}"
        return HTMLResponse(
            f"<div style='font-family:sans-serif;padding:24px'><h3>Union не удалён</h3>"
            f"<p>{e}</p><p><a href='{back}'>← назад к union</a></p></div>",
            status_code=409,
        )
    return RedirectResponse(f"/registers/{reg_id}" if reg_id else "/registers", status_code=303)


# ────────────────────────────────────────────
#  PAGES: Union Members
# ────────────────────────────────────────────
@app.get("/unions/{union_id}/members/new", response_class=HTMLResponse)
async def member_new(request: Request, union_id: int):
    union = dao.get_union(union_id)
    sources = dao.list_sources_for_register(union["register_id"])
    return _tpl("members/form.html", request,
                union=union, member=None, sources=sources)


@app.post("/unions/{union_id}/members/new")
async def member_create(
    request: Request, union_id: int,
    source_id: int = Form(...),
    priority: int = Form(0),
    where_clause: str = Form(""),
):
    dao.create_member({
        "union_id": union_id, "source_id": source_id,
        "priority": priority, "where_clause": where_clause or None,
    })
    return RedirectResponse(f"/unions/{union_id}", status_code=303)


@app.get("/members/{mem_id}/edit", response_class=HTMLResponse)
async def member_edit(request: Request, mem_id: int):
    member = dao.get_member(mem_id)
    union = dao.get_union(member["union_id"])
    sources = dao.list_sources_for_register(union["register_id"])
    return _tpl("members/form.html", request,
                union=union, member=member, sources=sources)


@app.post("/members/{mem_id}/edit")
async def member_update(
    request: Request, mem_id: int,
    source_id: int = Form(...),
    priority: int = Form(0),
    where_clause: str = Form(""),
):
    member = dao.get_member(mem_id)
    dao.update_member(mem_id, {
        "source_id": source_id, "priority": priority,
        "where_clause": where_clause or None,
    })
    return RedirectResponse(f"/unions/{member['union_id']}", status_code=303)


@app.post("/members/{mem_id}/delete")
async def member_delete(mem_id: int):
    member = dao.get_member(mem_id)
    uid = member["union_id"] if member else None
    dao.delete_member(mem_id)
    return RedirectResponse(f"/unions/{uid}" if uid else "/registers", status_code=303)


# ────────────────────────────────────────────
#  PAGES: Targets
# ────────────────────────────────────────────
@app.get("/registers/{reg_id}/targets/new", response_class=HTMLResponse)
async def target_new(request: Request, reg_id: int):
    reg = dao.get_register(reg_id)
    sources = dao.list_sources_for_register(reg_id)
    unions = dao.list_unions_for_register(reg_id)
    return _tpl("targets/form.html", request,
                register=reg, target=None, sources=sources, unions=unions,
                parent_targets=dao.list_targets_for_register(reg_id))


def _target_form_payload(target_schema, target_table, load_mode, upsert_keys, source_id, union_id,
                         pre_load_sql, target_role, parent_target_id, priority, include_columns, post_load_sql):
    """Общая сборка payload формы таргета + инвариант: ровно один из source_id / union_id."""
    sid = int(source_id) if source_id else None
    uid = int(union_id) if union_id else None
    if bool(sid) == bool(uid):
        raise ValueError(
            "у таргета должен быть ровно один источник данных: либо Source (direct), либо Union. "
            + ("Оба пусты — связь с union/источником была бы потеряна." if not sid else "Заданы оба.")
        )
    return {
        "target_schema": target_schema, "target_table": target_table,
        "load_mode": load_mode, "upsert_keys": upsert_keys,
        "source_id": sid, "union_id": uid,
        "pre_load_sql": pre_load_sql or None,
        "target_role": target_role or None,
        "parent_target_id": int(parent_target_id) if parent_target_id else None,
        "priority": priority,
        "include_columns": include_columns,
        "post_load_sql": post_load_sql or None,
    }


@app.post("/registers/{reg_id}/targets/new")
async def target_create(
    request: Request, reg_id: int,
    target_schema: str = Form("public"),
    target_table: str = Form(...),
    load_mode: str = Form("upsert"),
    upsert_keys: str = Form(""),
    source_id: str = Form(""),
    union_id: str = Form(""),
    pre_load_sql: str = Form(""),
    target_role: str = Form(""),
    parent_target_id: str = Form(""),
    priority: int = Form(0),
    include_columns: str = Form(""),
    post_load_sql: str = Form(""),
):
    try:
        payload = _target_form_payload(target_schema, target_table, load_mode, upsert_keys, source_id, union_id,
                                       pre_load_sql, target_role, parent_target_id, priority, include_columns, post_load_sql)
    except ValueError as e:
        return _refused("Таргет не создан", e, f"/registers/{reg_id}/targets/new")
    payload["register_id"] = reg_id
    tid = dao.create_target(payload)
    dao.sync_target_table(tid)
    return RedirectResponse(f"/registers/{reg_id}", status_code=303)


@app.get("/targets/{tgt_id}/edit", response_class=HTMLResponse)
async def target_edit(request: Request, tgt_id: int):
    target = dao.get_target(tgt_id)
    reg = dao.get_register(target["register_id"])
    sources = dao.list_sources_for_register(target["register_id"])
    unions = dao.list_unions_for_register(target["register_id"])
    parent_targets = [t for t in dao.list_targets_for_register(target["register_id"]) if t["id"] != tgt_id]
    return _tpl("targets/form.html", request,
                register=reg, target=target, sources=sources, unions=unions,
                parent_targets=parent_targets)


@app.post("/targets/{tgt_id}/edit")
async def target_update(
    request: Request, tgt_id: int,
    target_schema: str = Form("public"),
    target_table: str = Form(...),
    load_mode: str = Form("upsert"),
    upsert_keys: str = Form(""),
    source_id: str = Form(""),
    union_id: str = Form(""),
    pre_load_sql: str = Form(""),
    target_role: str = Form(""),
    parent_target_id: str = Form(""),
    priority: int = Form(0),
    include_columns: str = Form(""),
    post_load_sql: str = Form(""),
):
    target = dao.get_target(tgt_id)
    try:
        payload = _target_form_payload(target_schema, target_table, load_mode, upsert_keys, source_id, union_id,
                                       pre_load_sql, target_role, parent_target_id, priority, include_columns, post_load_sql)
        dao.update_target(tgt_id, payload)
    except ValueError as e:
        return _refused("Таргет не сохранён", e, f"/targets/{tgt_id}/edit")
    dao.sync_target_table(tgt_id)
    return RedirectResponse(f"/registers/{target['register_id']}", status_code=303)


@app.post("/targets/{tgt_id}/delete")
async def target_delete(tgt_id: int):
    target = dao.get_target(tgt_id)
    reg_id = target["register_id"] if target else None
    dao.delete_target(tgt_id)
    return RedirectResponse(f"/registers/{reg_id}" if reg_id else "/registers", status_code=303)


# ────────────────────────────────────────────
#  API: Sync & 1C Meta (AJAX)
# ────────────────────────────────────────────
@app.post("/api/registers/{reg_id}/sync")
async def api_sync_register(reg_id: int, confirm: bool = False):
    """
    Синхронизация config↔таблицы для всех target-ов регистра.

    Поведение:
      • Считаем план изменений для каждого target.
      • Safe-actions применяются всегда.
      • Если есть destructive (drop col / сужение типа / NOT NULL на ненулевую) —
        и confirm=false → ничего не меняем, возвращаем plans с requires_confirm=true
        и UI показывает модалку с предупреждением.
      • При confirm=true — destructive применяются (recreate таблицы через
        DROP CASCADE + CREATE). После recreate данные надо грузить заново (full_period).
    """
    try:
        targets = dao.list_targets_for_register(reg_id)
        if not targets:
            return JSONResponse({"ok": True, "plans": [], "message": "no targets"})

        plans = [dao.compute_sync_plan(t["id"]) for t in targets]
        has_destructive = any((p.get("destructive_count") or 0) > 0 for p in plans)

        # Destructive DDL (DROP/сужение типа/recreate) через UI — только при
        # ETL_CONFIG_ALLOW_DESTRUCTIVE=1. Иначе план показываем, но не применяем.
        if has_destructive and confirm and not dao.ALLOW_DESTRUCTIVE:
            return JSONResponse({
                "ok": False,
                "refused": True,
                "plans": plans,
                "message": f"Destructive DDL в окружении «{dao.ENV_LABEL}» через UI запрещён — "
                           f"выполняйте миграцией. Safe-действия не применены, чтобы план остался целостным.",
            }, status_code=403)

        # Если хотя бы один план требует confirm — собираем общий план для UI
        if has_destructive and not confirm:
            return JSONResponse({
                "ok": False,
                "requires_confirm": True,
                "destructive_allowed": dao.ALLOW_DESTRUCTIVE,
                "plans": plans,
            })

        # Применяем
        applied = []
        for plan in plans:
            result = dao.apply_sync_plan(plan, confirm=confirm)
            applied.append(result)

        any_error = any(p.get("errors") for p in applied)
        return JSONResponse({
            "ok": not any_error,
            "applied": applied,
            "recreated": any(p.get("recreated") for p in applied),
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
@app.get("/api/targets/{tgt_id}/post-load-template")
async def api_post_load_template(tgt_id: int):
    """READ-ONLY: шаблон post_load_sql из metadata (ничего не сохраняет — вставляется в форму таргета)."""
    try:
        return JSONResponse(dao.generate_post_load_sql(tgt_id))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/registers/{reg_id}/sync-plan")
async def api_sync_plan(reg_id: int):
    """READ-ONLY: план Sync по всем таргетам регистра без применения (dry-run)."""
    try:
        targets = dao.list_targets_for_register(reg_id)
        plans = [dao.compute_sync_plan(t["id"]) for t in targets]
        return JSONResponse({
            "ok": True, "env": dao.ENV_LABEL, "destructive_allowed": dao.ALLOW_DESTRUCTIVE,
            "plans": plans,
            "summary": [
                {"target": p.get("full_table_name"), "exists": p.get("exists"), "rows": p.get("rows_in_table"),
                 "actions": len(p.get("actions") or []), "destructive": p.get("destructive_count", 0),
                 "error": p.get("error")}
                for p in plans
            ],
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/registers/{reg_id}/discover-recorder-types")
async def api_discover_recorder_types(reg_id: int):
    """
    Auto-discover document types from parent register's MSSQL table.
    Queries DISTINCT _RecorderTRef, resolves 1C names via API.
    """
    try:
        reg = dao.get_register(reg_id)
        if not reg or not reg.get("parent_id"):
            return JSONResponse({"error": "Register has no parent"}, status_code=400)

        # Find parent register's source table
        parent_sources = dao.list_sources_for_register(reg["parent_id"])
        if not parent_sources:
            return JSONResponse({"error": "Parent register has no sources"}, status_code=400)

        mssql_table = parent_sources[0]["mssql_table"]  # e.g. _AccumRg17844

        # Query MSSQL for distinct recorder types
        type_numbers = mssql_client.query_distinct_recorder_types(mssql_table)

        # Resolve 1C names + discover VT tables via search API
        doc_info = onec_client.discover_document_with_vt(type_numbers)

        doc_types = []
        for n in type_numbers:
            info = doc_info.get(n, {})
            doc_types.append({
                "type_int": n,
                "mssql_table": f"_Document{n}",
                "onec_name": info.get("onec_name"),
                "vt_tables": info.get("vt_tables", []),
            })

        # Auto-set join keys: parent.recorder (_RecorderRRef) = child.id_ref (_IDRRef)
        dao.update_register_join_keys(reg_id, parent_join_key="recorder", child_join_key="id_ref")

        # Auto-save recorder_type → document name mapping
        type_map = {}
        for dt in doc_types:
            type_map[str(dt["type_int"])] = {
                "mssql_table": dt["mssql_table"],
                "onec_name": dt.get("onec_name"),
            }
        # Save on child register (sales_positions)
        dao.update_register_type_map(reg_id, type_map)
        # Save on parent register too (sales) — for recorder_type_lookup transform
        if reg.get("parent_id"):
            dao.update_register_type_map(reg["parent_id"], type_map)

        parent_reg = dao.get_register(reg["parent_id"])
        parent_code = parent_reg["code"] if parent_reg else "parent"

        return JSONResponse({
            "ok": True,
            "parent_table": mssql_table,
            "doc_types": doc_types,
            "join": {
                "parent_join_key": "recorder",
                "child_join_key": "id_ref",
                "parent_code": parent_code,
                "child_code": reg["code"],
            },
            "type_map": type_map,
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/registers/{reg_id}/batch-document-sources")
async def api_batch_document_sources(request: Request, reg_id: int):
    """Batch-create document sources from a list of document types."""
    try:
        body = await request.json()
        doc_types = body.get("doc_types", [])

        if not doc_types:
            return JSONResponse({"error": "doc_types required"}, status_code=400)

        # Resolve field structures from 1C API for ALL tables (documents + VT)
        all_api_names = []
        for dt in doc_types:
            all_api_names.append(f"Document{dt['type_int']}")
            for vt in dt.get("vt_tables", []):
                api_name = vt["mssql_table"].lstrip("_").replace("_VT", ".VT")  # Document476.VT13626
                all_api_names.append(api_name)

        field_map = {}
        if all_api_names:
            structures = onec_client.get_structure(all_api_names)
            for s in structures:
                field_map[s.get("table_name_sql", "")] = s.get("fields", [])

        # Attach fields to doc_types and enrich with MSSQL column types
        for dt in doc_types:
            doc_api_name = f"Document{dt['type_int']}"
            dt["fields"] = field_map.get(doc_api_name, [])
            # Get MSSQL types for this document table
            try:
                col_types = mssql_client.get_column_types(dt["mssql_table"])
                _enrich_fields_with_mssql_types(dt["fields"], col_types)
            except Exception:
                pass
            for vt in dt.get("vt_tables", []):
                api_name = vt["mssql_table"].lstrip("_").replace("_VT", ".VT")
                vt["fields"] = field_map.get(api_name, [])
                try:
                    vt_col_types = mssql_client.get_column_types(vt["mssql_table"])
                    _enrich_fields_with_mssql_types(vt["fields"], vt_col_types)
                except Exception:
                    pass

        result = dao.batch_create_document_sources(reg_id, doc_types)
        return JSONResponse({"ok": True, **result})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ────────────────────────────────────────────
#  API: Retail (incremental change detection)
# ────────────────────────────────────────────
@app.get("/api/retail/tables")
async def api_retail_tables():
    """List tables in retail DB (for register form dropdown)."""
    try:
        tables = dao.get_retail_tables()
        return JSONResponse({"tables": tables})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/retail/columns/{table}")
async def api_retail_columns(table: str):
    """Get columns for a retail table."""
    try:
        cols = dao.get_retail_columns(table)
        return JSONResponse({"columns": cols})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/registers/{reg_id}/retail-changes")
async def api_retail_changes(reg_id: int, since: str = Query("")):
    """Preview changed records from retail DB for a register."""
    try:
        reg = dao.get_register(reg_id)
        if not reg:
            return JSONResponse({"error": "Register not found"}, status_code=404)
        if not reg.get("retail_table") or not reg.get("retail_uid_column"):
            return JSONResponse({"error": "retail_table/retail_uid_column not configured"}, status_code=400)

        changes = dao.get_retail_changes(
            reg["retail_table"],
            reg["retail_uid_column"],
            since if since else None,
        )
        return JSONResponse({
            "ok": True,
            "count": len(changes),
            "retail_table": reg["retail_table"],
            "uid_column": reg["retail_uid_column"],
            "changes": [{"uid": str(c["uid"]), "updated_at": str(c["updated_at"])} for c in changes],
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ────────────────────────────────────────────
#  API: 1C Meta (AJAX)
# ────────────────────────────────────────────
@app.get("/api/1c/search")
async def api_1c_search(q: str = Query("")):
    results = onec_client.search_1c(q)
    return JSONResponse(results)


@app.get("/api/1c/structure")
async def api_1c_structure(table: str = Query("")):
    tables = [t.strip() for t in table.split(",") if t.strip()]
    results = onec_client.get_structure(tables)
    return JSONResponse(results)


# Quick-add source from 1C (AJAX, from register detail page)
@app.post("/api/registers/{reg_id}/quick-source")
async def api_quick_source(request: Request, reg_id: int):
    body = await request.json()
    onec_name = body.get("onec_name", "")
    table_name_sql = body.get("table_name_sql", "")
    source_type = body.get("source_type", "header")
    source_code = body.get("source_code", "")
    parent_source_id = body.get("parent_source_id") or None
    join_type = body.get("join_type") or None
    join_key_source = body.get("join_key_source") or None
    join_key_parent = body.get("join_key_parent") or None

    if not table_name_sql:
        return JSONResponse({"error": "table_name_sql required"}, status_code=400)

    # Auto-generate source_code from onec_name if not provided
    if not source_code:
        source_code = onec_name.replace(".", "_").replace(" ", "_").lower() if onec_name else table_name_sql.lower()

    # Fetch fields from 1C API
    fields_list = []
    if onec_name:
        structs = onec_client.get_structure([onec_name])
        if structs and structs[0].get("fields"):
            fields_list = structs[0]["fields"]

    sid = dao.create_source({
        "register_id": reg_id,
        "source_code": source_code,
        "source_type": source_type,
        "mssql_schema": "dbo",
        "mssql_table": table_name_sql,
        "onec_name": onec_name or None,
        "parent_source_id": int(parent_source_id) if parent_source_id else None,
        "join_type": join_type,
        "join_key_source": join_key_source,
        "join_key_parent": join_key_parent,
        "fields_cache": fields_list or None,
    })
    # Auto-create mappings for system fields from 1C
    auto_count = 0
    if fields_list:
        auto_count = dao.auto_create_mappings(sid, fields_list)
    return JSONResponse({"ok": True, "id": sid, "auto_mappings": auto_count})


# ================================================================
#  COLUMN BUILDER — unified target schema mapping
# ================================================================

@app.get("/api/registers/{reg_id}/column-builder")
async def api_column_builder(reg_id: int):
    """
    Return all sources for this register with their fields_cache
    and existing mappings, for the Column Builder UI.
    """
    sources = dao.list_sources_for_register(reg_id)
    result = []
    for s in sources:
        fields_cache = s.get("fields_cache") or []
        if isinstance(fields_cache, str):
            fields_cache = json.loads(fields_cache)
        # Get existing mappings for this source
        mappings = dao.list_mappings_for_source(s["id"])
        result.append({
            "id": s["id"],
            "source_code": s["source_code"],
            "source_type": s["source_type"],
            "mssql_table": s["mssql_table"],
            "onec_name": s.get("onec_name"),
            "fields": fields_cache,
            "mappings": [
                {
                    "id": m["id"],
                    "source_column": m["source_column"],
                    "target_column": m["target_column"],
                    "target_type": m.get("target_type"),
                    "transform_type": m.get("transform_type"),
                    "transform_params": m.get("transform_params"),
                    "is_expression": m.get("is_expression", False),
                    "onec_name": m.get("onec_name"),
                }
                for m in mappings
            ],
        })

    # Also return existing target columns (union output_columns)
    unions = dao.list_unions_for_register(reg_id)
    existing_target_cols = []
    if unions:
        u = dao.get_union(unions[0]["id"])
        if u and u.get("output_columns"):
            existing_target_cols = u["output_columns"]

    return JSONResponse({
        "sources": result,
        "existing_target_columns": existing_target_cols,
    })


@app.get("/api/registers/{reg_id}/common-fields")
async def api_common_fields(reg_id: int):
    """
    Aggregate fields by 1C name across all sources.
    Returns fields sorted by how many sources have them.
    """
    sources = dao.list_sources_for_register(reg_id)
    # Collect existing mappings to mark already-mapped fields
    existing_mapped = set()
    for s in sources:
        for m in dao.list_mappings_for_source(s["id"]):
            if not m.get("is_expression"):
                existing_mapped.add((s["id"], m["source_column"].lower()))

    # Aggregate by 1C field name
    field_map = {}  # onec_name → {sources: [...], target_type, transform}
    for s in sources:
        fields_cache = s.get("fields_cache") or []
        if isinstance(fields_cache, str):
            fields_cache = json.loads(fields_cache)
        for f in fields_cache:
            onec_name = f.get("field_name", "")
            if not onec_name:
                continue
            mssql_col = f.get("mssql_column") or ("_" + f.get("field_name_sql", ""))
            is_mapped = (s["id"], mssql_col.lower()) in existing_mapped

            if onec_name not in field_map:
                field_map[onec_name] = {
                    "onec_name": onec_name,
                    "target_type": f.get("target_type", "text"),
                    "transform_type": f.get("transform_type"),
                    "sources": [],
                    "mapped_count": 0,
                }
            field_map[onec_name]["sources"].append({
                "source_id": s["id"],
                "source_code": s["source_code"],
                "onec_name_src": s.get("onec_name", ""),
                "source_type": s["source_type"],
                "mssql_column": mssql_col,
                "field_name_sql": f.get("field_name_sql", ""),
                "is_mapped": is_mapped,
            })
            if is_mapped:
                field_map[onec_name]["mapped_count"] += 1

    # Sort: most common first, then alphabetical
    fields = sorted(field_map.values(), key=lambda x: (-len(x["sources"]), x["onec_name"]))

    return JSONResponse({
        "fields": fields,
        "total_sources": len(sources),
    })


@app.post("/api/registers/{reg_id}/batch-add-columns")
async def api_batch_add_columns(request: Request, reg_id: int):
    """
    Batch-add multiple target columns at once.
    Body: { "columns": [{"onec_name": "Номенклатура", "target_column": "nomenclature",
            "target_type": "uuid", "transform_type": "binary_to_uuid",
            "sources": [{"source_id": 1, "mssql_column": "_Fld123"}] }, ...] }
    """
    try:
        body = await request.json()
        columns = body.get("columns", [])
        created = 0
        for col in columns:
            target_col = col["target_column"]
            target_type = col.get("target_type", "text")
            transform = col.get("transform_type")
            for src in col.get("sources", []):
                dao.create_mapping({
                    "source_id": src["source_id"],
                    "source_column": src["mssql_column"],
                    "target_column": target_col,
                    "target_type": target_type,
                    "transform_type": transform,
                    "onec_name": col.get("onec_name"),
                })
                created += 1

        # Sync union output_columns and target
        _sync_union_and_target(reg_id)

        return JSONResponse({"ok": True, "created": created})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


def _sync_union_and_target(reg_id: int):
    """After adding columns, sync union output_columns, target tables, and include_columns."""
    sources = dao.list_sources_for_register(reg_id)
    targets = dao.list_targets_for_register(reg_id)

    # Update union output_columns — ТОЛЬКО колонками его членов (и не теряя уже заданные).
    # Раньше сюда сливались колонки ВСЕХ источников регистра: у document_with_vt
    # в output_columns позиций попадали бы поля шапки (summa_documenta, raw_refs.kontragent).
    unions = dao.list_unions_for_register(reg_id)
    for u in unions:
        out_cols = set(u.get("output_columns") or [])
        for mem in dao.list_members_for_union(u["id"]):
            for m in dao.list_mappings_for_source(mem["source_id"]):
                if m.get("is_active", True) and m.get("transform_type") != "custom_python":
                    out_cols.add(m["target_column"])
        dao.update_union(u["id"], {
            "union_code": u["union_code"],
            "description": u.get("description"),
            "output_columns": sorted(out_cols),
        })

    # Auto-fill include_columns by source_type → target_role
    dim_target = next((t for t in targets if t.get("target_role") == "dimension"), None)
    fact_target = next((t for t in targets if t.get("target_role") == "fact"), None)

    if dim_target or fact_target:
        dim_cols = set(dim_target.get("include_columns") or []) if dim_target else set()
        fact_cols = set(fact_target.get("include_columns") or []) if fact_target else set()

        for s in sources:
            mappings = dao.list_mappings_for_source(s["id"])
            for m in mappings:
                col = m["target_column"]
                if m.get("transform_type") == "custom_python":
                    continue  # computed → manual
                if s["source_type"] == "header" and dim_target:
                    dim_cols.add(col)
                elif s["source_type"] == "detail" and fact_target:
                    fact_cols.add(col)
                # standalone → keep existing manual assignment

        if dim_target:
            dao.execute(
                f"UPDATE {dao.SCHEMA}.register_targets SET include_columns=%s WHERE id=%s",
                [sorted(dim_cols) if dim_cols else None, dim_target["id"]]
            )
        if fact_target:
            dao.execute(
                f"UPDATE {dao.SCHEMA}.register_targets SET include_columns=%s WHERE id=%s",
                [sorted(fact_cols) if fact_cols else None, fact_target["id"]]
            )

    # Sync target tables
    for t in targets:
        dao.sync_target_table(t["id"])


@app.get("/api/sources/{source_id}/fields")
async def api_source_fields(source_id: int, refresh: bool = False):
    """
    Поля источника со статусом относительно РЕАЛЬНОЙ базы MSSQL (UPP_JAN):
      confirmed     — есть в meta API 1С и физически есть в таблице;
      api_only      — только подсказка meta API (NikitaBase), колонки в этой базе НЕТ — мэппинг запрещён;
      physical_only — колонка есть в MSSQL, но meta API её не знает (новые реквизиты с другой нумерацией).
    Имена/номера из meta API — подсказка, физическая схема — истина (ловушка Fld24506 vs _Fld24518RRef).
    """
    src = dao.get_source(source_id)
    if not src:
        return JSONResponse({"error": "Source not found"}, status_code=404)

    fields = src.get("fields_cache") or []
    if isinstance(fields, str):
        fields = json.loads(fields)
    fields = [f for f in fields if f.get("status") != "physical_only"]  # пересобираем из MSSQL ниже

    if not fields or refresh:
        api_name = (src.get("mssql_table") or "").lstrip("_").replace("_VT", ".VT")
        if api_name:
            structs = onec_client.get_structure([api_name])
            if structs and structs[0].get("fields"):
                fields = structs[0]["fields"]

    col_types = {}
    mssql_error = None
    try:
        col_types = mssql_client.get_column_types(src.get("mssql_table", ""))
    except Exception as e:  # noqa: BLE001
        mssql_error = str(e)[:200]

    if col_types:
        for f in fields:
            f.pop("mssql_column", None)
        _enrich_fields_with_mssql_types(fields, col_types)
        seen = set()
        for f in fields:
            mc = f.get("mssql_column")
            if mc and mc in col_types:
                f["status"] = "confirmed"; seen.add(mc)
            else:
                f["status"] = "api_only"; f["mssql_column"] = None  # имя не достраиваем — SELECT по нему падал
        for col, info in col_types.items():
            if col in seen:
                continue
            tt, tr = _resolve_mssql_type(info["data_type"], info.get("max_length"))
            fields.append({"field_name": None, "field_name_sql": col.lstrip("_"), "mssql_column": col,
                           "mssql_type": info["data_type"], "mssql_length": info.get("max_length"),
                           "target_type": tt, "transform_type": tr, "status": "physical_only"})
        order = {"confirmed": 0, "physical_only": 1, "api_only": 2}
        fields.sort(key=lambda f: order.get(f.get("status"), 3))
        dao.update_source_fields_cache(source_id, fields)
    else:
        for f in fields:
            f.setdefault("status", "confirmed" if f.get("mssql_column") else "unverified")

    counts = {}
    for f in fields:
        counts[f.get("status", "unverified")] = counts.get(f.get("status", "unverified"), 0) + 1
    return JSONResponse({"source_id": source_id, "fields": fields, "counts": counts,
                         "mssql_verified": bool(col_types), "mssql_error": mssql_error})


@app.post("/api/registers/{reg_id}/add-target-column")
async def api_add_target_column(request: Request, reg_id: int):
    """
    Add a target column across multiple sources.
    Body: {
        "target_column": "nomenclature",
        "target_type": "uuid",
        "transform_type": "binary_to_uuid",
        "mappings": [
            {"source_id": 10, "source_column": "_Fld17845", "onec_name": "Номенклатура"},
            {"source_id": 11, "source_column": "_Fld20001", "onec_name": "Номенклатура"},
        ]
    }
    """
    try:
        body = await request.json()
        target_column = body.get("target_column", "").strip()
        target_type = body.get("target_type") or None
        transform_type = body.get("transform_type") or None
        source_mappings = body.get("mappings", [])

        if not target_column:
            return JSONResponse({"error": "target_column required"}, status_code=400)

        # Pre-fetch MSSQL types per source for auto-detection
        source_col_types = {}
        for sm in source_mappings:
            sid = sm["source_id"]
            if sid not in source_col_types:
                src = dao.get_source(sid)
                if src and src.get("mssql_table"):
                    try:
                        source_col_types[sid] = mssql_client.get_column_types(src["mssql_table"])
                    except Exception:
                        source_col_types[sid] = {}
                else:
                    source_col_types[sid] = {}

        # Физическая проверка: колонка должна быть в MSSQL (типы уже получены выше); force=true — осознанный обход
        if not body.get("force"):
            unknown = [f'{sm["source_id"]}:{sm.get("source_column")}' for sm in source_mappings
                       if sm.get("source_column", "").strip() and source_col_types.get(sm["source_id"])
                       and sm["source_column"].strip() not in source_col_types[sm["source_id"]]]
            if unknown:
                return JSONResponse({"error": "нет таких колонок в MSSQL (UPP_JAN): " + ", ".join(unknown)
                                     + ". Имя из meta API — подсказка, не истина."}, status_code=400)

        created = []
        for sm in source_mappings:
            src_col = sm.get("source_column", "").strip()
            if not src_col:
                continue
            # Auto-detect type and transform from MSSQL if not explicitly set
            m_target_type = target_type
            m_transform = transform_type
            col_info = source_col_types.get(sm["source_id"], {}).get(src_col)
            if col_info:
                auto_type, auto_transform = _resolve_mssql_type(col_info["data_type"], col_info.get("max_length"))
                if not m_target_type or m_target_type == "varchar":
                    m_target_type = auto_type
                if not m_transform:
                    m_transform = auto_transform

            mid = dao.create_mapping({
                "source_id": sm["source_id"],
                "source_column": src_col,
                "target_column": target_column,
                "target_type": m_target_type,
                "transform_type": m_transform,
                "onec_name": sm.get("onec_name") or None,
            })
            created.append({"mapping_id": mid, "source_id": sm["source_id"]})

        # Куда идёт новая колонка — только в свои таргеты (раньше колонка добавлялась
        # во ВСЕ таргеты регистра и в output_columns первого попавшегося union).
        source_ids = [sm["source_id"] for sm in source_mappings if sm.get("source_column", "").strip()]
        target_ids = _targets_for_new_column(reg_id, target_column, source_ids, body.get("target_ids"))

        # output_columns — только у union'ов, членами которых являются источники колонки
        for u in dao.list_unions_for_register(reg_id):
            member_ids = {m["source_id"] for m in dao.list_members_for_union(u["id"])}
            if not member_ids & set(source_ids):
                continue
            full = dao.get_union(u["id"]) or u
            oc = list(full.get("output_columns") or [])
            if target_column not in oc:
                oc.append(target_column)
                dao.update_union(u["id"], {
                    "union_code": full["union_code"],
                    "description": full.get("description"),
                    "output_columns": oc,
                })

        for tid in target_ids:
            dao.add_include_column(tid, target_column)   # idempotent; NULL include = «все колонки источника»
            dao.sync_target_table(tid)
            dao.ensure_target_column(tid, target_column, target_type)

        return JSONResponse({
            "ok": True, "created": created, "target_ids": target_ids,
            **({} if target_ids else {"note": "колонка не назначена ни одному таргету — таблицы не изменены"}),
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


def _targets_for_new_column(reg_id: int, target_column: str, source_ids: list, explicit_ids=None) -> list:
    """
    Таргеты, которым принадлежит новая колонка, в порядке приоритета:
      1. явный список target_ids из запроса;
      2. таргеты, у которых колонка уже есть в include_columns;
      3. по типу источников мэппингов: header → dimension, detail → fact
         (standalone/computed — назначаются вручную, таблицы не трогаем).
    Никогда не «все таргеты регистра».
    """
    targets = dao.list_targets_for_register(reg_id)
    ids = {t["id"] for t in targets}
    if explicit_ids:
        return [int(i) for i in explicit_ids if int(i) in ids]
    by_include = [t["id"] for t in targets if target_column in (t.get("include_columns") or [])]
    if by_include:
        return by_include
    kinds = set()
    for sid in source_ids:
        src = dao.get_source(sid)
        if src:
            kinds.add(src.get("source_type"))
    out = []
    if "header" in kinds:
        out += [t["id"] for t in targets if t.get("target_role") == "dimension"]
    if "detail" in kinds:
        out += [t["id"] for t in targets if t.get("target_role") == "fact"]
    return sorted(set(out))


@app.delete("/api/registers/{reg_id}/target-column/{col_name}")
async def api_delete_target_column(reg_id: int, col_name: str):
    """Remove a target column from all sources of this register."""
    try:
        sources = dao.list_sources_for_register(reg_id)
        deleted = 0
        for s in sources:
            mappings = dao.list_mappings_for_source(s["id"])
            for m in mappings:
                if m["target_column"] == col_name:
                    dao.delete_mapping(m["id"])
                    deleted += 1

        # Remove from output_columns of every union that has it
        for u0 in dao.list_unions_for_register(reg_id):
            u = dao.get_union(u0["id"])
            oc = (u or {}).get("output_columns") or []
            if u and col_name in oc:
                dao.update_union(u["id"], {
                    "union_code": u["union_code"],
                    "description": u.get("description"),
                    "output_columns": [c for c in oc if c != col_name],
                })

        return JSONResponse({"ok": True, "deleted": deleted})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/registers/{reg_id}/rename-column")
async def api_rename_column(request: Request, reg_id: int):
    """Rename a target column across all sources, union output_columns, and target table."""
    try:
        body = await request.json()
        old_name = body.get("old_name", "").strip()
        new_name = body.get("new_name", "").strip()
        if not old_name or not new_name:
            return JSONResponse({"error": "old_name and new_name required"}, status_code=400)
        if old_name == new_name:
            return JSONResponse({"ok": True, "renamed": 0})

        # Технический хребет и системные колонки не переименовываются никогда
        # (upsert, добор хвоста, missing-delete, post_load, BI смотрят на эти имена).
        protected = {"id", "recorder", "recorder_type", "line_no", "vt_kind", "period", "raw_refs"}
        if old_name in protected or old_name.endswith("_id") or old_name.startswith("raw_refs."):
            return JSONResponse({"error": f"колонка «{old_name}» — техническая/системная, переименование запрещено"}, status_code=409)
        # Таблица с данными: физический RENAME COLUMN ломает BI и post_load — только миграцией
        busy = dao._targets_with_data(dao.list_targets_for_register(reg_id))
        if busy:
            return JSONResponse({"error": f"таблицы с данными ({', '.join(busy)}): переименование колонок через UI запрещено, только миграцией"}, status_code=409)

        # Rename in all mappings
        sources = dao.list_sources_for_register(reg_id)
        renamed = 0
        for s in sources:
            mappings = dao.list_mappings_for_source(s["id"])
            for m in mappings:
                if m["target_column"] == old_name:
                    dao.update_mapping(m["id"], {
                        "source_column": m["source_column"],
                        "target_column": new_name,
                        "target_type": m.get("target_type"),
                        "transform_type": m.get("transform_type"),
                        "is_expression": m.get("is_expression", False),
                        "onec_name": m.get("onec_name"),
                    })
                    renamed += 1

        # Rename in output_columns of EVERY union that has the column
        for u0 in dao.list_unions_for_register(reg_id):
            u = dao.get_union(u0["id"])
            oc = (u or {}).get("output_columns") or []
            if u and old_name in oc:
                dao.update_union(u["id"], {
                    "union_code": u["union_code"],
                    "description": u.get("description"),
                    "output_columns": [new_name if c == old_name else c for c in oc],
                })

        # Rename actual column in target table
        targets = dao.list_targets_for_register(reg_id)
        for t in targets:
            tgt = dao.get_target(t["id"])
            if tgt:
                schema = tgt.get("target_schema", "public")
                table = tgt.get("target_table", "")
                if table:
                    try:
                        conn = dao.get_conn()
                        with conn.cursor() as cur:
                            cur.execute(
                                f'ALTER TABLE "{schema}"."{table}" RENAME COLUMN "{old_name}" TO "{new_name}"'
                            )
                        conn.commit()
                        conn.close()
                    except Exception:
                        pass  # Column may not exist yet

        return JSONResponse({"ok": True, "renamed": renamed})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/registers/{reg_id}/set-column-transform")
async def api_set_column_transform(request: Request, reg_id: int):
    """Set transform_type for all mappings of a given target_column.
    Body: {column: str, transform_type: str|null}
    """
    try:
        body = await request.json()
        col = (body.get("column") or "").strip()
        new_transform = body.get("transform_type")
        if new_transform == "":
            new_transform = None
        if not col:
            return JSONResponse({"error": "column required"}, status_code=400)

        updated = 0
        for s in dao.list_sources_for_register(reg_id):
            for m in dao.list_mappings_for_source(s["id"]):
                if m["target_column"] != col:
                    continue
                if m.get("transform_type") == "custom_python":
                    continue
                dao.update_mapping(m["id"], {
                    "source_column": m["source_column"],
                    "target_column": m["target_column"],
                    "target_type": m.get("target_type"),
                    "transform_type": new_transform,
                    "transform_params": m.get("transform_params"),
                    "is_expression": m.get("is_expression", False),
                    "onec_name": m.get("onec_name"),
                })
                updated += 1
        return JSONResponse({"ok": True, "updated": updated})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/registers/{reg_id}/auto-transforms")
async def api_auto_transforms(reg_id: int):
    """Auto-set target_type and transform_type for all mappings based on MSSQL column types."""
    try:
        sources = dao.list_sources_for_register(reg_id)
        updated = 0
        for src in sources:
            # Get MSSQL column types for this source
            mssql_table = src.get("mssql_table", "")
            if not mssql_table:
                continue
            try:
                col_types = mssql_client.get_column_types(mssql_table)
            except Exception:
                continue

            mappings = dao.list_mappings_for_source(src["id"])
            for m in mappings:
                if m.get("is_expression"):
                    continue
                source_col = m.get("source_column", "")
                info = col_types.get(source_col)
                if not info:
                    continue
                target_type, transform = _resolve_mssql_type(info["data_type"], info.get("max_length"))
                # Update if different
                if target_type != m.get("target_type") or transform != m.get("transform_type"):
                    dao.update_mapping(m["id"], {
                        "source_column": m["source_column"],
                        "target_column": m["target_column"],
                        "target_type": target_type,
                        "transform_type": transform,
                        "is_expression": m.get("is_expression", False),
                        "onec_name": m.get("onec_name"),
                    })
                    updated += 1

        # Sync targets
        targets = dao.list_targets_for_register(reg_id)
        for t in targets:
            dao.sync_target_table(t["id"])

        return JSONResponse({"ok": True, "updated": updated})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/registers/{reg_id}/sql-preview")
async def api_sql_preview(reg_id: int):
    """Generate SQL preview for the register's pipeline."""
    try:
        sources = dao.list_sources_for_register(reg_id)
        unions = dao.list_unions_for_register(reg_id)
        targets = dao.list_targets_for_register(reg_id)

        lines = []
        lines.append(f"-- ETL Pipeline for register {reg_id}")
        lines.append("")

        if not sources:
            return JSONResponse({"sql": "-- No sources configured"})

        # Build SELECT per source
        source_sqls = []
        for src in sources:
            mappings = dao.list_mappings_for_source(src["id"])
            if not mappings:
                continue
            active_mappings = [m for m in mappings if m.get("is_active", True)]
            if not active_mappings:
                continue

            cols = []
            for m in active_mappings:
                if m.get("transform_type") == "custom_python":
                    cols.append(f"  -- [computed] {m['target_column']}  (custom_python)")
                    continue
                if m.get("is_expression"):
                    cols.append(f"  {m['source_column']} AS [{m['target_column']}]")
                elif m.get("transform_type"):
                    cols.append(f"  {m['source_column']} AS [{m['target_column']}]  -- {m['transform_type']}")
                else:
                    cols.append(f"  {m['source_column']} AS [{m['target_column']}]")

            table = src["mssql_table"] if src["mssql_table"].startswith("_") else f"_{src['mssql_table']}"
            select = f"SELECT\n" + ",\n".join(cols) + f"\nFROM [dbo].[{table}]"

            if src.get("where_clause"):
                select += f"\nWHERE {src['where_clause']}"

            onec_comment = f"  -- {src.get('onec_name', '')}" if src.get('onec_name') else ""
            source_sqls.append(f"-- Source: {src['source_code']}{onec_comment}\n{select}")

        if unions and len(source_sqls) > 1:
            lines.append("-- UNION ALL query")
            lines.append("\n\nUNION ALL\n\n".join(source_sqls))
        else:
            lines.extend(source_sqls)

        if targets:
            t = targets[0]
            lines.append(f"\n-- Target: {t['target_schema']}.{t['target_table']}")
            lines.append(f"-- Mode: {t['load_mode']}")
            if t.get("upsert_keys"):
                lines.append(f"-- Upsert keys: {', '.join(t['upsert_keys'])}")

        return JSONResponse({"sql": "\n".join(lines)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ═══════════════ Custom Python Transforms ═══════════════

@app.get("/api/custom-transforms")
async def api_custom_transforms():
    """Список доступных custom python функций."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))
    from core.transform.custom import get_registry
    return JSONResponse(get_registry())


@app.post("/api/registers/{reg_id}/add-computed-column")
async def api_add_computed_column(reg_id: int, request: Request):
    """Добавить вычисляемую колонку (custom_python) ко всем источникам."""
    data = await request.json()
    func_name = data.get("function")
    target_column = data.get("target_column", func_name)
    target_type = data.get("target_type", "decimal")

    if not func_name:
        return JSONResponse({"error": "function is required"}, status_code=400)

    # Загружаем реестр функций для проверки uses_columns
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))
    from core.transform.custom import CUSTOM_TRANSFORMS

    func_entry = CUSTOM_TRANSFORMS.get(func_name)
    if not func_entry:
        return JSONResponse({"error": f"Функция '{func_name}' не найдена"}, status_code=400)

    # Проверяем что все uses_columns есть в маппингах регистра
    sources = dao.list_sources_for_register(reg_id)
    if not sources:
        return JSONResponse({"error": "Нет источников"}, status_code=400)

    existing_targets = set()
    for src in sources:
        for m in dao.list_mappings_for_source(src["id"]):
            if m.get("target_column"):
                existing_targets.add(m["target_column"])

    required = func_entry["uses_columns"]
    missing = [col for col in required if col not in existing_targets]

    if missing:
        return JSONResponse({
            "error": "Не хватает колонок: " + ", ".join(missing),
            "missing_columns": missing,
        }, status_code=400)

    # Всё ок — создаём маппинг на первом источнике
    first_source = sources[0]
    transform_params = json.dumps({"function": func_name}, ensure_ascii=False)

    dao.create_mapping({
        "source_id": first_source["id"],
        "source_column": f"__computed__{func_name}",
        "target_column": target_column,
        "target_type": target_type,
        "transform_type": "custom_python",
        "transform_params": transform_params,
        "is_expression": False,
        "is_nullable": True,
    })

    return JSONResponse({"ok": True, "target_column": target_column})


@app.delete("/api/registers/{reg_id}/computed-column/{col_name}")
async def api_delete_computed_column(reg_id: int, col_name: str):
    """Удалить вычисляемую колонку."""
    sources = dao.list_sources_for_register(reg_id)
    deleted = 0
    for src in sources:
        mappings = dao.list_mappings_for_source(src["id"])
        for m in mappings:
            if m["target_column"] == col_name and m.get("transform_type") == "custom_python":
                dao.delete_mapping(m["id"])
                deleted += 1
    return JSONResponse({"ok": True, "deleted": deleted})


@app.post("/api/targets/{tgt_id}/include-columns")
async def api_set_include_columns(tgt_id: int, request: Request):
    """Установить include_columns для target."""
    data = await request.json()
    cols = data.get("include_columns")  # list of column names or None
    target = dao.get_target(tgt_id)
    if not target:
        return JSONResponse({"error": "target not found"}, status_code=404)
    dao.execute(
        f"UPDATE {dao.SCHEMA}.register_targets SET include_columns=%s WHERE id=%s",
        [cols if cols else None, tgt_id]
    )
    return JSONResponse({"ok": True})


@app.post("/api/registers/{reg_id}/assign-column-target")
async def api_assign_column_target(reg_id: int, request: Request):
    """
    Атомарно установить во ВСЕ targets, в каких живёт колонка.
    Body:
      • {"column": "recorder", "target_ids": [80, 81]}  — в обоих
      • {"column": "recorder", "target_ids": [80]}      — только sales
      • {"column": "recorder", "target_ids": []}        — не грузить
      • legacy: {"column": "x", "target_id": 5 | "all" | null} — старый формат
    """
    data = await request.json()
    col_name = data.get("column")
    if not col_name:
        return JSONResponse({"error": "column is required"}, status_code=400)

    targets = dao.list_targets_for_register(reg_id)

    # Resolve final set of target_ids
    if "target_ids" in data:
        raw = data.get("target_ids") or []
        target_ids = {int(x) for x in raw}
    else:
        # legacy single-value
        legacy = data.get("target_id")
        if legacy == "all":
            target_ids = {t["id"] for t in targets}
        elif legacy in (None, ""):
            target_ids = set()
        else:
            target_ids = {int(legacy)}

    for t in targets:
        current = t.get("include_columns") or []
        tid = t["id"]
        if tid in target_ids:
            if col_name not in current:
                current.append(col_name)
        else:
            current = [c for c in current if c != col_name]
        dao.execute(
            f"UPDATE {dao.SCHEMA}.register_targets SET include_columns=%s WHERE id=%s",
            [current if current else None, tid]
        )

    return JSONResponse({"ok": True, "target_ids": sorted(target_ids)})


@app.get("/api/registers/{reg_id}/column-targets")
async def api_column_targets(reg_id: int):
    """
    Карта: какая колонка в какой target.
    Авто-определение по source_type:
      header → dimension target, detail → fact target,
      standalone/computed → из include_columns (ручной выбор).
    Returns: {targets: [...], column_map: {col: {targets: [...], auto: bool}}}
    """
    targets = dao.list_targets_for_register(reg_id)
    sources = dao.list_sources_for_register(reg_id)

    # Find dim/fact targets by role
    dim_target = next((t for t in targets if t.get("target_role") == "dimension"), None)
    fact_target = next((t for t in targets if t.get("target_role") == "fact"), None)

    # Build column map
    col_map = {}

    # Источник истины о принадлежности колонки таргету — include_columns
    # (по ним работают Sync и движок). Эвристика header→dim / detail→fact —
    # только fallback для регистров, где include_columns ещё не заполнены.
    has_include = any(t.get("include_columns") for t in targets)

    # 1. Auto-assign by source_type
    for src in sources:
        mappings = dao.list_mappings_for_source(src["id"])
        for m in mappings:
            col = m["target_column"]
            if m.get("transform_type") == "custom_python":
                continue  # computed → manual below

            if has_include:
                assigned = [t["id"] for t in targets if col in (t.get("include_columns") or [])]
                if assigned:
                    col_map[col] = {"targets": assigned, "auto": False,
                                    "expression": m["source_column"] if m.get("is_expression") else None}
                    continue

            if src["source_type"] == "header" and dim_target:
                col_map[col] = {"targets": [dim_target["id"]], "auto": True}
            elif src["source_type"] == "detail" and fact_target:
                col_map[col] = {"targets": [fact_target["id"]], "auto": True}
            # standalone → check include_columns (manual)
            elif src["source_type"] == "standalone":
                if col not in col_map:
                    # Find from include_columns
                    assigned = []
                    for t in targets:
                        if col in (t.get("include_columns") or []):
                            assigned.append(t["id"])
                    col_map[col] = {"targets": assigned, "auto": False}

    # 2. Computed columns → manual
    for src in sources:
        for m in dao.list_mappings_for_source(src["id"]):
            if m.get("transform_type") == "custom_python":
                col = m["target_column"]
                assigned = []
                for t in targets:
                    if col in (t.get("include_columns") or []):
                        assigned.append(t["id"])
                col_map[col] = {"targets": assigned, "auto": False}

    return JSONResponse({
        "targets": [
            {"id": t["id"], "target_table": t["target_table"],
             "target_schema": t.get("target_schema", "public"),
             "priority": t.get("priority", 0),
             "target_role": t.get("target_role"),
             "include_columns": t.get("include_columns") or [],
             "load_mode": t.get("load_mode", "upsert"),
             "upsert_keys": t.get("upsert_keys") or []}
            for t in targets
        ],
        "column_map": col_map,
    })


# ═══════════════ WIZARD «Создать регистр продажи» ═══════════════
@app.get("/api/registers/wizard/probe-source")
async def api_wizard_probe(mssql_table: str = Query("")):
    """
    Резолвит главный источник + находит документы-регистраторы и их VT-таблицы.

    Args:
        mssql_table: например '_AccumRg17844'

    Returns:
        {
          "main_source": {"mssql_table":"_AccumRg17844",
                          "onec_name":"РегистрНакопления.Продажи",
                          "fields":[{...}]},
          "documents": [
            {"type_int":476, "mssql_table":"_Document476",
             "onec_name":"Документ.ЧекККМ",
             "vt_tables":[{"mssql_table":"_Document476_VT13626",
                           "vt_number":"13626",
                           "onec_name":"Документ.ЧекККМ.Товары"}, ...]},
             ...
          ],
          "documents_without_vt": [443, ...]  # документы без VT.Товары — отметка для UI
        }
    """
    if not mssql_table:
        return JSONResponse({"error": "mssql_table required"}, status_code=400)

    tbl = mssql_table.lstrip("_")
    # Получить структуру главного источника
    main_struct = onec_client.get_structure([tbl])
    main_source = {"mssql_table": mssql_table, "onec_name": None, "fields": []}
    if main_struct and main_struct[0]:
        main_source["onec_name"] = main_struct[0].get("table_name")
        main_source["fields"] = main_struct[0].get("fields") or []

    # Найти документы-регистраторы (только для AccumRg/InfoRg)
    documents = []
    documents_without_vt = []
    if tbl.lower().startswith(("accumrg", "inforg", "accrg")):
        try:
            type_numbers = mssql_client.query_distinct_recorder_types(mssql_table)
            doc_info = onec_client.discover_document_with_vt(type_numbers)
            for n in sorted(type_numbers):
                info = doc_info.get(n, {})
                vt_tables = info.get("vt_tables", [])
                documents.append({
                    "type_int": n,
                    "mssql_table": f"_Document{n}",
                    "onec_name": info.get("onec_name"),
                    "vt_tables": vt_tables,
                    "vt_count": len(vt_tables),
                })
                if not vt_tables:
                    documents_without_vt.append(n)
        except Exception as e:
            return JSONResponse({"main_source": main_source, "documents": [],
                                  "warning": f"recorder discover failed: {e}"})

    return JSONResponse({
        "main_source": main_source,
        "documents": documents,
        "documents_without_vt": documents_without_vt,
    })


@app.post("/api/registers/wizard/create")
async def api_wizard_create(request: Request):
    """
    Создаёт регистр + источники + targets + маппинги одним вызовом.

    Body:
      {
        "code": "sales",
        "name": "Продажа",
        "description": "...",
        "main_mssql_table": "_AccumRg17844",
        "target_dim_name": "sales",
        "target_fact_name": "sales_positions",
        "documents": [
          {"type_int": 476, "vt_tables": ["_Document476_VT13626"]},
          {"type_int": 415, "vt_tables": ["_Document415_VT11053"]},
          ...
        ]
      }

    Семантика:
      • main_mssql_table — главный регистр (AccumRg). Source=standalone.
      • Для каждого выбранного документа создаём header-source (для JOIN).
      • Для каждой выбранной VT-таблицы — detail-source с parent=header.
      • Два target: dim (target_dim_name) + fact (target_fact_name).
      • Auto-маппинги системных полей через _AUTO_MAPPING_RULES.
      • Auto-маппинги бизнес-полей VT — переименование в snake_case транслит.
      • Если регистр с таким code уже есть — 409 Conflict.
    """
    try:
        body = await request.json()
        code = (body.get("code") or "").strip()
        if not code:
            return JSONResponse({"error": "code required"}, status_code=400)

        # Валидация уникальности
        existing = dao.query_one("SELECT id FROM etl_meta.registers WHERE code=%s", [code])
        if existing:
            return JSONResponse({
                "error": f"register with code '{code}' already exists (id={existing['id']})"
            }, status_code=409)

        main_mssql_table = body.get("main_mssql_table") or ""
        target_dim_name = (body.get("target_dim_name") or "").strip() or code
        target_fact_name = (body.get("target_fact_name") or "").strip() or f"{code}_positions"
        documents = body.get("documents") or []

        if not main_mssql_table:
            return JSONResponse({"error": "main_mssql_table required"}, status_code=400)

        # 1. Регистр (витрина) с pipeline_type
        reg_id = dao.create_register({
            "code": code,
            "name": body.get("name") or code,
            "description": body.get("description") or "",
            "default_mode": "full_period",
            "pipeline_type": "accumrg_with_documents",
        })

        # 2. Главный источник AccumRg (standalone) — нужен сначала,
        #    т.к. constraint требует source_id или union_id у target.
        main_struct = onec_client.get_structure([main_mssql_table.lstrip("_")])
        main_onec = main_struct[0].get("table_name") if main_struct else None
        main_fields = main_struct[0].get("fields") if main_struct else []
        try:
            col_types = mssql_client.get_column_types(main_mssql_table)
            _enrich_fields_with_mssql_types(main_fields, col_types)
        except Exception:
            pass

        main_src_id = dao.create_source({
            "register_id": reg_id,
            "source_code": code,
            "source_type": "standalone",
            "mssql_schema": "dbo",
            "mssql_table": main_mssql_table,
            "onec_name": main_onec,
            "fields_cache": main_fields,
        })

        # 3. Два target — dim + fact. parent_target_id у fact = dim (UI группировка).
        dim_target_id = dao.create_target({
            "register_id": reg_id,
            "target_schema": "public",
            "target_table": target_dim_name,
            "source_id": main_src_id,
            "load_mode": "upsert",
            "upsert_keys": ["recorder"],
            "priority": 0,
            "target_role": "dimension",
        })
        post_load_sql = (
            f"UPDATE public.{target_fact_name} AS f\n"
            f"SET    sales_id = d.id\n"
            f"FROM   public.{target_dim_name} AS d\n"
            f"WHERE  f.recorder = d.recorder\n"
            f"  AND  f.sales_id IS NULL;"
        )
        fact_target_id = dao.create_target({
            "register_id": reg_id,
            "target_schema": "public",
            "target_table": target_fact_name,
            "source_id": main_src_id,
            "load_mode": "upsert",
            "upsert_keys": ["recorder", "line_no"],
            "priority": 1,
            "target_role": "fact",
            "post_load_sql": post_load_sql,
            "parent_target_id": dim_target_id,
        })

        # 4. Маппинги главного source создаём ПОСЛЕ targets чтобы знать target_id.
        #    Для AccumRg оставляем target_id=NULL (колонки идут и в dim, и в fact —
        #    раскладываются через include_columns).
        dao.auto_create_mappings(main_src_id, main_fields, target_id=None)

        # 4. Для каждого документа: header-source + VT detail-sources
        #    document_header → target_id = dim_target_id
        #    document_detail (VT) → target_id = fact_target_id
        created_headers = 0
        created_vts = 0
        for doc in documents:
            type_int = doc.get("type_int")
            doc_mssql = f"_Document{type_int}"
            doc_struct = onec_client.get_structure([doc_mssql.lstrip("_")])
            doc_onec = doc_struct[0].get("table_name") if doc_struct else None
            doc_fields = doc_struct[0].get("fields") if doc_struct else []
            try:
                col_types = mssql_client.get_column_types(doc_mssql)
                _enrich_fields_with_mssql_types(doc_fields, col_types)
            except Exception:
                pass

            header_src_id = dao.create_source({
                "register_id": reg_id,
                "source_code": f"doc_{type_int}",
                "source_type": "header",
                "mssql_schema": "dbo",
                "mssql_table": doc_mssql,
                "onec_name": doc_onec,
                "fields_cache": doc_fields,
            })
            dao.auto_create_mappings(header_src_id, doc_fields, target_id=dim_target_id)
            created_headers += 1

            for vt_mssql in doc.get("vt_tables") or []:
                vt_api_name = vt_mssql.lstrip("_").replace("_VT", ".VT")
                vt_struct = onec_client.get_structure([vt_api_name])
                vt_onec = vt_struct[0].get("table_name") if vt_struct else None
                vt_fields = vt_struct[0].get("fields") if vt_struct else []
                try:
                    col_types = mssql_client.get_column_types(vt_mssql)
                    _enrich_fields_with_mssql_types(vt_fields, col_types)
                except Exception:
                    pass

                vt_num = vt_mssql.split("_VT")[-1] if "_VT" in vt_mssql else ""

                vt_src_id = dao.create_source({
                    "register_id": reg_id,
                    "source_code": f"doc_{type_int}_vt_{vt_num}",
                    "source_type": "detail",
                    "mssql_schema": "dbo",
                    "mssql_table": vt_mssql,
                    "onec_name": vt_onec,
                    "parent_source_id": header_src_id,
                    "join_type": "INNER JOIN",
                    "join_key_source": f"_Document{type_int}_IDRRef",
                    "join_key_parent": "_IDRRef",
                    "fields_cache": vt_fields,
                })
                dao.auto_create_mappings(vt_src_id, vt_fields, target_id=fact_target_id)
                created_vts += 1

        # 5. Auto-распределение include_columns по target_role
        #    Legacy путь работает: ETL Engine читает include_columns на target.
        _sync_union_and_target(reg_id)

        return JSONResponse({
            "ok": True,
            "register_id": reg_id,
            "main_source_id": main_src_id,
            "dim_target_id": dim_target_id,
            "fact_target_id": fact_target_id,
            "documents_created": created_headers,
            "vt_sources_created": created_vts,
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=5555, reload=True)
