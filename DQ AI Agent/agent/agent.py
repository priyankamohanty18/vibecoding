from __future__ import annotations

import json
from typing import Any, Dict, Optional

from langchain.agents.factory import create_agent
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

from agent.tools import build_tools


def _extract_json_object(text: str) -> Dict[str, Any]:
    """
    Best-effort extraction of a single JSON object from model output.
    We keep this lightweight: first try full parse, then fall back to substring parse.
    """
    text = (text or "").strip()
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    first = text.find("{")
    last = text.rfind("}")
    if first != -1 and last != -1 and last > first:
        try:
            parsed = json.loads(text[first : last + 1])
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

    raise ValueError("Agent did not return a valid JSON object.")


def build_dq_agent(
    *,
    engine,
    openai_api_key: str,
    cust_schema_text: str,
    model: str = "gpt-4.1-mini",
    temperature: float = 0,
) -> Any:
    tools = build_tools(engine=engine, cust_schema_text=cust_schema_text)

    llm = ChatOpenAI(
        model=model,
        temperature=temperature,
        api_key=openai_api_key,
    )

    system_prompt = (
        "You are a senior data engineer and data quality expert for a PostgreSQL database.\n"
        "You have read-only tools to fetch: (1) table schema text, (2) sample rows, and (3) Great Expectations DQ issues.\n\n"
        "Workflow:\n"
        "- Always call run_ge_validation(table) to get dq_issues.\n"
        "- Call get_table_schema_text(table) to get schema.\n"
        "- Call get_table_sample_rows(table, limit) to get sample rows.\n"
        "- Then produce explanations and propose safe SQL fixes.\n\n"
        "Safety:\n"
        "- Do NOT execute SQL.\n"
        "- Prefer conservative UPDATE statements and scoped fixes.\n"
        "- Avoid DELETE unless absolutely necessary; if proposed, make it tightly scoped.\n\n"
        "Output:\n"
        "- Return JSON only (no markdown, no commentary), with keys:\n"
        "  - table: string\n"
        "  - dq_issues: array (the parsed JSON list from run_ge_validation)\n"
        "  - explanations: array of {issue_index, rule_type, summary, detailed_explanation}\n"
        "  - sql_fixes: array of {issue_index, rule_type, description, sql}\n"
        "- sql should be a single SQL statement string.\n"
    )

    # LangChain v1.x provides a built-in tool-calling agent factory.
    return create_agent(llm, tools, system_prompt=system_prompt, debug=True)


def run_dq_agent(
    *,
    engine,
    openai_api_key: str,
    cust_schema_text: str,
    table: str = "cust",
    limit: int = 50,
    model: str = "gpt-4.1-mini",
) -> Dict[str, Any]:
    """
    Run the DQ agent end-to-end:
    - tool calls to gather schema/sample/DQ issues
    - LLM generates explanations + SQL fix suggestions
    Returns a parsed dict (strict JSON object).
    """
    executor = build_dq_agent(
        engine=engine,
        openai_api_key=openai_api_key,
        cust_schema_text=cust_schema_text,
        model=model,
    )

    result = executor.invoke(
        {
            "messages": [
                HumanMessage(
                    content=(
                        f"Run the DQ workflow for table '{table}'. "
                        f"Use sample row limit={limit}. "
                        "Return the final JSON object with table, dq_issues, explanations, sql_fixes."
                    )
                )
            ]
        }
    )

    messages = result.get("messages", []) if isinstance(result, dict) else []
    if not messages:
        raise ValueError("Agent did not return any messages.")

    output_text = getattr(messages[-1], "content", None) or ""
    return _extract_json_object(output_text)

