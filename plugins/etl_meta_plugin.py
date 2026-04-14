"""
Airflow Plugin: ETL Meta Configuration UI.

Adds 'ETL Config' menu to Airflow web UI for managing
ETL registers, sources, column mappings, unions, and targets.
"""

from airflow.plugins_manager import AirflowPlugin
from airflow.listeners.hookimpl import hookimpl

from etl_meta_views import (
    RegisterView,
    SourceView,
    ColumnMappingView,
    UnionView,
    UnionMemberView,
    TargetView,
    EtlMetaApiView,
)


class EtlMetaPlugin(AirflowPlugin):
    name = "etl_meta_plugin"

    appbuilder_views = [
        {
            "name": "ETL Registers",
            "category": "ETL Config",
            "view": RegisterView(),
        },
        {"view": SourceView()},
        {"view": ColumnMappingView()},
        {"view": UnionView()},
        {"view": UnionMemberView()},
        {"view": TargetView()},
        {"view": EtlMetaApiView()},
    ]

    # Menu link for Airflow 3 navbar (works in FAB-based views)
    appbuilder_menu_items = [
        {
            "name": "ETL Config",
            "href": "/pluginsv2/etl-meta/registers/",
            "category": "Plugins",
        },
    ]


def _patch_extra_menu():
    """
    Patch FabAuthManager.get_extra_menu_items to add ETL Config link
    to the Airflow 3 React navbar (Security dropdown).
    """
    try:
        from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager
        from airflow.api_fastapi.common.types import ExtraMenuItem

        _original = FabAuthManager.get_extra_menu_items

        def _patched(self, *, user):
            items = _original(self, user=user)
            items.append(ExtraMenuItem(text="ETL Config", href="/pluginsv2/etl-meta/registers/"))
            return items

        FabAuthManager.get_extra_menu_items = _patched
    except Exception:
        pass


_patch_extra_menu()
