# DQ AI Agent

PostgreSQL + Great Expectations + LangChain agent: detect data quality issues, explain them, and suggest SQL fixes. Fixes are applied **only after explicit user confirmation**.

## Setup

1. **Environment**
   - Copy `config.example.env` to `.env` and set `OPENAI_API_KEY` and `DATABASE_URL`.
   - Example: `DATABASE_URL=postgresql+psycopg2://user:pwd@localhost:5432/mydb`

2. **Install**
   ```bash
   pip install -r requirements.txt
   ```

3. **Database**
   - Ensure the `cust` table exists and is loaded (see project notes for schema and sample data).

## Run

```bash
uvicorn app:app --reload
```

- **Health:** `GET http://localhost:8000/`
- **Run DQ check (detect + explain + suggest SQL):** `POST http://localhost:8000/dq/run`
- **Apply a fix (only after user confirms):** `POST http://localhost:8000/dq/apply_fix` with body:
  ```json
  { "table": "cust", "sql": "UPDATE cust SET ...", "description": "...", "issue_index": 0 }
  ```

The agent never executes suggested SQL; the UI (or your client) must call `/dq/apply_fix` only after the user confirms.

## DQ rules (cust table)

Duplicate records, missing/null values, invalid email format, inconsistent phone format, invalid dates, referential integrity (country), standardization (state 2-letter).
