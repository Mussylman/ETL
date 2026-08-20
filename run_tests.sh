#!/usr/bin/env bash
set -e
uv run python tests/golden_sales_test.py
uv run python tests/sync_ddl_test.py
