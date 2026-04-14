"""
Flask-AppBuilder views for ETL Meta configuration.
"""

import os
import json
from flask import request, flash, redirect, url_for, jsonify
from flask_appbuilder import BaseView, expose

from etl_meta_dao import EtlMetaDAO
from onec_api import OneCMetaClient

dao = EtlMetaDAO()


class RegisterView(BaseView):
    route_base = "/etl-meta/registers"
    default_view = "list"
    template_folder = os.path.join(os.path.dirname(__file__), "templates")

    @expose("/")
    def list(self):
        registers = dao.list_registers(include_inactive=True)
        return self.render_template("etl_meta/registers/list.html", registers=registers)

    @expose("/create", methods=["GET", "POST"])
    def create(self):
        if request.method == "POST":
            try:
                dao.create_register(request.form.to_dict())
                flash("Register created", "success")
                return redirect(url_for("RegisterView.list"))
            except Exception as e:
                flash(f"Error: {e}", "danger")
        return self.render_template("etl_meta/registers/form.html", register=None)

    @expose("/<int:id>")
    def detail(self, id):
        register = dao.get_register(id)
        if not register:
            flash("Register not found", "danger")
            return redirect(url_for("RegisterView.list"))

        sources = dao.list_sources_for_register(id)
        unions = dao.list_unions_for_register(id)
        targets = dao.list_targets_for_register(id)
        history = dao.list_load_history(id)

        return self.render_template(
            "etl_meta/registers/detail.html",
            register=register, sources=sources, unions=unions,
            targets=targets, history=history,
        )

    @expose("/<int:id>/edit", methods=["GET", "POST"])
    def edit(self, id):
        register = dao.get_register(id)
        if not register:
            flash("Register not found", "danger")
            return redirect(url_for("RegisterView.list"))

        if request.method == "POST":
            try:
                dao.update_register(id, request.form.to_dict())
                flash("Register updated", "success")
                return redirect(url_for("RegisterView.detail", id=id))
            except Exception as e:
                flash(f"Error: {e}", "danger")

        return self.render_template("etl_meta/registers/form.html", register=register)

    @expose("/<int:id>/delete", methods=["POST"])
    def delete(self, id):
        try:
            dao.delete_register(id)
            flash("Register deleted", "success")
        except Exception as e:
            flash(f"Error: {e}", "danger")
        return redirect(url_for("RegisterView.list"))


class SourceView(BaseView):
    route_base = "/etl-meta/sources"
    default_view = "list"
    template_folder = os.path.join(os.path.dirname(__file__), "templates")

    def _get_parent_sources(self, register_id):
        """List of (id, label) for parent source dropdown."""
        sources = dao.list_sources_for_register(register_id)
        return [(str(s["id"]), f"{s['source_code']} ({s['mssql_table']})") for s in sources]

    @expose("/create/<int:register_id>", methods=["GET", "POST"])
    def create(self, register_id):
        register = dao.get_register(register_id)
        if not register:
            flash("Register not found", "danger")
            return redirect(url_for("RegisterView.list"))

        if request.method == "POST":
            try:
                data = request.form.to_dict()
                data["register_id"] = register_id
                source_id = dao.create_source(data)
                flash("Source created", "success")
                return redirect(url_for("RegisterView.detail", id=register_id))
            except Exception as e:
                flash(f"Error: {e}", "danger")

        return self.render_template(
            "etl_meta/sources/form.html",
            register=register, source=None,
            parent_sources=self._get_parent_sources(register_id),
        )

    @expose("/<int:id>")
    def detail(self, id):
        source = dao.get_source(id)
        if not source:
            flash("Source not found", "danger")
            return redirect(url_for("RegisterView.list"))

        mappings = dao.list_mappings_for_source(id)
        return self.render_template(
            "etl_meta/column_mappings/list.html",
            source=source, mappings=mappings,
        )

    @expose("/<int:id>/edit", methods=["GET", "POST"])
    def edit(self, id):
        source = dao.get_source(id)
        if not source:
            flash("Source not found", "danger")
            return redirect(url_for("RegisterView.list"))

        register = dao.get_register(source["register_id"])

        if request.method == "POST":
            try:
                dao.update_source(id, request.form.to_dict())
                flash("Source updated", "success")
                return redirect(url_for("RegisterView.detail", id=source["register_id"]))
            except Exception as e:
                flash(f"Error: {e}", "danger")

        return self.render_template(
            "etl_meta/sources/form.html",
            register=register, source=source,
            parent_sources=self._get_parent_sources(source["register_id"]),
        )

    @expose("/<int:id>/delete", methods=["POST"])
    def delete(self, id):
        source = dao.get_source(id)
        register_id = source["register_id"] if source else None
        try:
            dao.delete_source(id)
            flash("Source deleted", "success")
        except Exception as e:
            flash(f"Error: {e}", "danger")
        return redirect(url_for("RegisterView.detail", id=register_id) if register_id else url_for("RegisterView.list"))


class ColumnMappingView(BaseView):
    route_base = "/etl-meta/mappings"
    default_view = "list"
    template_folder = os.path.join(os.path.dirname(__file__), "templates")

    @expose("/create/<int:source_id>", methods=["GET", "POST"])
    def create(self, source_id):
        source = dao.get_source(source_id)
        if not source:
            flash("Source not found", "danger")
            return redirect(url_for("RegisterView.list"))

        if request.method == "POST":
            try:
                data = request.form.to_dict()
                data["source_id"] = source_id
                data["is_expression"] = "is_expression" in request.form
                data["is_nullable"] = "is_nullable" in request.form
                dao.create_mapping(data)
                flash("Mapping created", "success")
                return redirect(url_for("SourceView.detail", id=source_id))
            except Exception as e:
                flash(f"Error: {e}", "danger")

        return self.render_template(
            "etl_meta/column_mappings/form.html",
            source=source, mapping=None,
        )

    @expose("/batch/<int:source_id>", methods=["POST"])
    def batch_create(self, source_id):
        """Batch-создание маппингов из колонок 1С."""
        source = dao.get_source(source_id)
        if not source:
            flash("Source not found", "danger")
            return redirect(url_for("RegisterView.list"))

        total = int(request.form.get("total_fields", 0))
        mappings = []
        for i in range(total):
            if request.form.get(f"select_{i}"):
                mappings.append({
                    "source_column": request.form.get(f"source_col_{i}", ""),
                    "target_column": request.form.get(f"target_col_{i}", ""),
                    "transform_type": request.form.get(f"transform_{i}", "") or None,
                    "is_nullable": True,
                })

        if mappings:
            try:
                dao.create_mappings_batch(source_id, mappings)
                flash(f"{len(mappings)} mappings created", "success")
            except Exception as e:
                flash(f"Error: {e}", "danger")
        else:
            flash("No columns selected", "warning")

        return redirect(url_for("SourceView.detail", id=source_id))

    @expose("/<int:id>/edit", methods=["GET", "POST"])
    def edit(self, id):
        mapping = dao.get_mapping(id)
        if not mapping:
            flash("Mapping not found", "danger")
            return redirect(url_for("RegisterView.list"))

        source = dao.get_source(mapping["source_id"])

        if request.method == "POST":
            try:
                data = request.form.to_dict()
                data["is_expression"] = "is_expression" in request.form
                data["is_nullable"] = "is_nullable" in request.form
                dao.update_mapping(id, data)
                flash("Mapping updated", "success")
                return redirect(url_for("SourceView.detail", id=mapping["source_id"]))
            except Exception as e:
                flash(f"Error: {e}", "danger")

        return self.render_template(
            "etl_meta/column_mappings/form.html",
            source=source, mapping=mapping,
        )

    @expose("/<int:id>/delete", methods=["POST"])
    def delete(self, id):
        mapping = dao.get_mapping(id)
        source_id = mapping["source_id"] if mapping else None
        try:
            dao.delete_mapping(id)
            flash("Mapping deleted", "success")
        except Exception as e:
            flash(f"Error: {e}", "danger")
        return redirect(url_for("SourceView.detail", id=source_id) if source_id else url_for("RegisterView.list"))


class UnionView(BaseView):
    route_base = "/etl-meta/unions"
    default_view = "list"
    template_folder = os.path.join(os.path.dirname(__file__), "templates")

    @expose("/create/<int:register_id>", methods=["GET", "POST"])
    def create(self, register_id):
        register = dao.get_register(register_id)
        if not register:
            flash("Register not found", "danger")
            return redirect(url_for("RegisterView.list"))

        if request.method == "POST":
            try:
                data = request.form.to_dict()
                data["register_id"] = register_id
                dao.create_union(data)
                flash("Union created", "success")
                return redirect(url_for("RegisterView.detail", id=register_id))
            except Exception as e:
                flash(f"Error: {e}", "danger")

        return self.render_template(
            "etl_meta/unions/form.html",
            register=register, union=None,
        )

    @expose("/<int:id>")
    def detail(self, id):
        union = dao.get_union(id)
        if not union:
            flash("Union not found", "danger")
            return redirect(url_for("RegisterView.list"))

        members = dao.list_members_for_union(id)
        return self.render_template(
            "etl_meta/unions/detail.html",
            union=union, members=members,
        )

    @expose("/<int:id>/edit", methods=["GET", "POST"])
    def edit(self, id):
        union = dao.get_union(id)
        if not union:
            flash("Union not found", "danger")
            return redirect(url_for("RegisterView.list"))

        register = dao.get_register(union["register_id"])

        if request.method == "POST":
            try:
                dao.update_union(id, request.form.to_dict())
                flash("Union updated", "success")
                return redirect(url_for("UnionView.detail", id=id))
            except Exception as e:
                flash(f"Error: {e}", "danger")

        return self.render_template(
            "etl_meta/unions/form.html",
            register=register, union=union,
        )

    @expose("/<int:id>/delete", methods=["POST"])
    def delete(self, id):
        union = dao.get_union(id)
        register_id = union["register_id"] if union else None
        try:
            dao.delete_union(id)
            flash("Union deleted", "success")
        except Exception as e:
            flash(f"Error: {e}", "danger")
        return redirect(url_for("RegisterView.detail", id=register_id) if register_id else url_for("RegisterView.list"))


class UnionMemberView(BaseView):
    route_base = "/etl-meta/union-members"
    default_view = "list"
    template_folder = os.path.join(os.path.dirname(__file__), "templates")

    def _get_sources(self, register_id):
        sources = dao.list_sources_for_register(register_id)
        return [(str(s["id"]), f"{s['source_code']} ({s['mssql_table']})") for s in sources]

    @expose("/create/<int:union_id>", methods=["GET", "POST"])
    def create(self, union_id):
        union = dao.get_union(union_id)
        if not union:
            flash("Union not found", "danger")
            return redirect(url_for("RegisterView.list"))

        if request.method == "POST":
            try:
                data = request.form.to_dict()
                data["union_id"] = union_id
                dao.create_member(data)
                flash("Member added", "success")
                return redirect(url_for("UnionView.detail", id=union_id))
            except Exception as e:
                flash(f"Error: {e}", "danger")

        return self.render_template(
            "etl_meta/union_members/form.html",
            union=union, member=None,
            sources=self._get_sources(union["register_id"]),
        )

    @expose("/<int:id>/edit", methods=["GET", "POST"])
    def edit(self, id):
        member = dao.get_member(id)
        if not member:
            flash("Member not found", "danger")
            return redirect(url_for("RegisterView.list"))

        union = dao.get_union(member["union_id"])

        if request.method == "POST":
            try:
                dao.update_member(id, request.form.to_dict())
                flash("Member updated", "success")
                return redirect(url_for("UnionView.detail", id=member["union_id"]))
            except Exception as e:
                flash(f"Error: {e}", "danger")

        return self.render_template(
            "etl_meta/union_members/form.html",
            union=union, member=member,
            sources=self._get_sources(union["register_id"]),
        )

    @expose("/<int:id>/delete", methods=["POST"])
    def delete(self, id):
        member = dao.get_member(id)
        union_id = member["union_id"] if member else None
        try:
            dao.delete_member(id)
            flash("Member removed", "success")
        except Exception as e:
            flash(f"Error: {e}", "danger")
        return redirect(url_for("UnionView.detail", id=union_id) if union_id else url_for("RegisterView.list"))


class TargetView(BaseView):
    route_base = "/etl-meta/targets"
    default_view = "list"
    template_folder = os.path.join(os.path.dirname(__file__), "templates")

    def _get_options(self, register_id):
        sources = dao.list_sources_for_register(register_id)
        unions = dao.list_unions_for_register(register_id)
        return (
            [(str(s["id"]), s["source_code"]) for s in sources],
            [(str(u["id"]), u["union_code"]) for u in unions],
        )

    @expose("/create/<int:register_id>", methods=["GET", "POST"])
    def create(self, register_id):
        register = dao.get_register(register_id)
        if not register:
            flash("Register not found", "danger")
            return redirect(url_for("RegisterView.list"))

        if request.method == "POST":
            try:
                data = request.form.to_dict()
                data["register_id"] = register_id
                dao.create_target(data)
                flash("Target created", "success")
                return redirect(url_for("RegisterView.detail", id=register_id))
            except Exception as e:
                flash(f"Error: {e}", "danger")

        sources, unions = self._get_options(register_id)
        return self.render_template(
            "etl_meta/targets/form.html",
            register=register, target=None,
            sources=sources, unions=unions,
        )

    @expose("/<int:id>/edit", methods=["GET", "POST"])
    def edit(self, id):
        target = dao.get_target(id)
        if not target:
            flash("Target not found", "danger")
            return redirect(url_for("RegisterView.list"))

        register = dao.get_register(target["register_id"])

        if request.method == "POST":
            try:
                dao.update_target(id, request.form.to_dict())
                flash("Target updated", "success")
                return redirect(url_for("RegisterView.detail", id=target["register_id"]))
            except Exception as e:
                flash(f"Error: {e}", "danger")

        sources, unions = self._get_options(target["register_id"])
        return self.render_template(
            "etl_meta/targets/form.html",
            register=register, target=target,
            sources=sources, unions=unions,
        )

    @expose("/<int:id>/delete", methods=["POST"])
    def delete(self, id):
        target = dao.get_target(id)
        register_id = target["register_id"] if target else None
        try:
            dao.delete_target(id)
            flash("Target deleted", "success")
        except Exception as e:
            flash(f"Error: {e}", "danger")
        return redirect(url_for("RegisterView.detail", id=register_id) if register_id else url_for("RegisterView.list"))


class EtlMetaApiView(BaseView):
    """AJAX API endpoints for 1C integration."""
    route_base = "/etl-meta/api"
    default_view = "search_1c"
    template_folder = os.path.join(os.path.dirname(__file__), "templates")

    @expose("/search-1c")
    def search_1c(self):
        """Search 1C objects by name (autocomplete proxy)."""
        q = request.args.get("q", "").strip()
        if len(q) < 2:
            return jsonify({"results": []})

        try:
            client = OneCMetaClient()
            names = client.search(q)

            # If search returns just names, fetch structure to get SQL names
            if names and isinstance(names[0], str):
                structures = client.get_structure(names[:10])
                results = [
                    {"table_name": s["table_name"], "table_name_sql": s["table_name_sql"]}
                    for s in structures
                ]
            elif names and isinstance(names[0], dict):
                results = names[:10]
            else:
                results = []

            return jsonify({"results": results})
        except Exception as e:
            return jsonify({"results": [], "error": str(e)})

    @expose("/table-fields")
    def table_fields(self):
        """Get columns for a 1C table."""
        table = request.args.get("table", "").strip()
        onec_name = request.args.get("onec_name", table)
        if not table:
            return jsonify({"fields": []})

        try:
            client = OneCMetaClient()
            structures = client.get_structure([onec_name])
            if structures:
                return jsonify({"fields": structures[0].get("fields", [])})
            return jsonify({"fields": []})
        except Exception as e:
            return jsonify({"fields": [], "error": str(e)})
