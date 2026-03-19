from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
import os

from dotenv import load_dotenv

from sqlalchemy import create_engine, text

from agent.agent import run_dq_agent


load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is not set")

engine = create_engine(DATABASE_URL)


class DQIssue(BaseModel):
    rule_type: str
    expectation: str
    description: str
    column: Optional[str] = None
    columns: Optional[List[str]] = None
    failed_count: int
    sample_failed_values: Optional[List[Any]] = None
    sample_failed_ids: Optional[List[Any]] = None
    severity: str


class DQRunResponse(BaseModel):
    table: str
    dq_issues: List[DQIssue]
    explanations: List[Dict[str, Any]]
    sql_fixes: List[Dict[str, Any]]


class ApplyFixRequest(BaseModel):
    table: str
    sql: str
    description: Optional[str] = None
    issue_index: Optional[int] = None


class ApplyFixResponse(BaseModel):
    table: str
    sql: str
    description: Optional[str]
    issue_index: Optional[int]
    rows_affected: Optional[int]


app = FastAPI(title="DQ AI Agent", version="0.1.0")


CUST_SCHEMA = """
cust (
  customerid INTEGER PRIMARY KEY,
  firstname TEXT,
  lastname TEXT,
  email TEXT,
  phone TEXT,
  dateofbirth TEXT,
  address TEXT,
  city TEXT,
  state TEXT,
  zipcode TEXT,
  country TEXT,
  registrationdate TEXT,
  lastpurchasedate TEXT,
  totalspent NUMERIC(12,2),
  status TEXT
)
"""


def get_cust_schema_text() -> str:
    return CUST_SCHEMA.strip()


@app.post("/dq/run", response_model=DQRunResponse)
def run_dq_for_cust() -> DQRunResponse:
    """
    Run data quality checks for the `cust` table and use the LLM agent
    to explain issues and propose SQL fixes. This endpoint NEVER applies any SQL.
    """
    table = "cust"

    try:
        agent_output = run_dq_agent(
            engine=engine,
            openai_api_key=OPENAI_API_KEY,
            cust_schema_text=get_cust_schema_text(),
            table=table,
            limit=50,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    dq_issues_raw = agent_output.get("dq_issues", []) or []
    dq_issues = [DQIssue(**x) for x in dq_issues_raw]
    explanations = agent_output.get("explanations", []) or []
    sql_fixes = agent_output.get("sql_fixes", []) or []

    return DQRunResponse(
        table=table,
        dq_issues=dq_issues,
        explanations=explanations,
        sql_fixes=sql_fixes,
    )


@app.post("/dq/apply_fix", response_model=ApplyFixResponse)
def apply_sql_fix(payload: ApplyFixRequest) -> ApplyFixResponse:
    """
    Apply a specific SQL fix ONLY after explicit user confirmation on the frontend.
    This endpoint blindly executes the provided SQL within a transaction and returns
    the affected row count. It does not generate SQL itself.
    """
    # Basic guardrails: ensure the table mentioned matches the expected table.
    if payload.table != "cust":
        raise HTTPException(status_code=400, detail="Only 'cust' table is supported for now.")

    # Minimal safety check: disallow DDL for now.
    forbidden = ["DROP ", "TRUNCATE ", "ALTER ", "CREATE "]
    upper_sql = payload.sql.upper()
    if any(word in upper_sql for word in forbidden):
        raise HTTPException(
            status_code=400,
            detail="DDL statements are not allowed via this endpoint.",
        )

    # Execute within a transaction.
    with engine.begin() as conn:
        result = conn.execute(text(payload.sql))
        try:
            rows_affected = result.rowcount
        except Exception:
            rows_affected = None

    return ApplyFixResponse(
        table=payload.table,
        sql=payload.sql,
        description=payload.description,
        issue_index=payload.issue_index,
        rows_affected=rows_affected,
    )


@app.get("/")
def health_check():
    return {"status": "ok"}

