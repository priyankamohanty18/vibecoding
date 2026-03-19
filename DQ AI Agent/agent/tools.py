from __future__ import annotations

import json
import traceback
from typing import Callable, List

from langchain_core.tools import tool
from sqlalchemy import text


def build_tools(*, engine, cust_schema_text: str) -> List[Callable]:
    """
    Build read-only tools for the DQ agent.

    Notes:
    - Tools are intentionally read-only (no SQL execution).
    - Currently guarded to only support the `cust` table.
    """

    @tool
    def get_table_schema_text(table: str) -> str:
        """Return the schema text for a table (cust only)."""
        if table != "cust":
            raise ValueError("Only 'cust' table is supported for now.")
        return cust_schema_text.strip()

    @tool
    def get_table_sample_rows(table: str, limit: int = 50) -> str:
        """Return JSON sample rows from the table (cust only)."""
        if table != "cust":
            raise ValueError("Only 'cust' table is supported for now.")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM cust LIMIT :limit"), {"limit": limit})
            rows = [dict(row._mapping) for row in result]
        return json.dumps(rows, default=str)

    @tool
    def run_ge_validation(table: str) -> str:
        """Run Great Expectations validation and return JSON list of normalized DQ issue dicts."""
        if table != "cust":
            raise ValueError("Only 'cust' table is supported for now.")
        try:
            from dq.ge_runner import run_validation_and_normalize

            issues = run_validation_and_normalize(engine, table)
        except Exception as e:
            # Don't silently swallow GE failures; surface them so the API caller can fix config/version issues.
            tb = traceback.format_exc()
            raise RuntimeError(f"Great Expectations validation failed: {e}\n{tb}")
        return json.dumps(issues, default=str)

    return [get_table_schema_text, get_table_sample_rows, run_ge_validation]

