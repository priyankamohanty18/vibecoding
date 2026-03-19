"""
Run Great Expectations validation on the cust table and normalize results to DQIssue list.
Uses GE fluent API (SQL datasource + table asset + expectation suite).
"""
from __future__ import annotations

from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


def run_validation_and_normalize(engine: "Engine", table_name: str) -> List[dict]:
    """
    Run GE validation on the given table and return a list of issue dicts
    compatible with app.DQIssue (rule_type, expectation, description, column/columns,
    failed_count, sample_failed_values, sample_failed_ids, severity).
    """
    import great_expectations as gx
    from great_expectations.core import ExpectationSuite

    connection_string = str(engine.url)
    context = gx.get_context(mode="ephemeral")

    # GE v1.x uses `context.data_sources` (not `context.sources`).
    datasource = context.data_sources.add_postgres(
        name="pg_ds",
        connection_string=connection_string,
    )
    asset = datasource.add_table_asset(name=f"{table_name}_asset", table_name=table_name)
    batch_request = asset.build_batch_request()

    suite_name = f"{table_name}_suite"
    suite = ExpectationSuite(name=suite_name)
    context.suites.add(suite)
    validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)
    _add_cust_expectations(validator)

    validation_result = validator.validate()
    return _normalize_validation_result(validation_result, table_name)


def _add_cust_expectations(validator) -> None:
    """Add the 8 DQ rules to the validator (duplicates, nulls, email, phone, dates, referential, standardization)."""
    # 1. Duplicate records
    validator.expect_column_values_to_be_unique(column="customerid")
    validator.expect_compound_columns_to_be_unique(column_list=["email", "dateofbirth"])

    # 2. Missing / null values
    for col in ["customerid", "email", "firstname", "lastname", "registrationdate", "status"]:
        validator.expect_column_values_to_not_be_null(column=col)

    # 3. Invalid email format
    validator.expect_column_values_to_match_regex(
        column="email",
        regex=r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
    )

    # 4. Inconsistent phone format (relaxed: digits and common separators)
    validator.expect_column_values_to_match_regex(
        column="phone",
        regex=r"^[\d\s\-\.\+\(\)]*\d[\d\s\-\.\+\(\)]*$",
    )

    # 5/6. Invalid dates + inconsistent date formats
    # The table stores dates as TEXT. In GE v1.x, `expect_column_values_to_be_between`
    # does not support `parse_strings_as_datetimes`, so we enforce a single standard
    # format and let non-matching values surface as DQ issues.
    validator.expect_column_values_to_match_regex(
        column="dateofbirth",
        regex=r"^\d{4}-\d{2}-\d{2}$",
    )

    # 7. Referential / allowed values (country)
    validator.expect_column_values_to_be_in_set(
        column="country",
        value_set=["USA", "United States", "US"],
    )

    # 8. Standardization (state 2-letter)
    validator.expect_column_values_to_match_regex(column="state", regex=r"^[A-Za-z]{2}$")


def _normalize_validation_result(validation_result, table_name: str) -> List[dict]:
    """Convert GE ExpectationSuiteValidationResult to list of DQIssue-like dicts."""
    issues = []
    for r in getattr(validation_result, "results", []):
        if getattr(r, "success", True):
            continue
        config = getattr(r, "expectation_config", None) or {}
        if isinstance(config, dict):
            kwargs = config.get("kwargs", {})
            expectation_type = config.get("expectation_type") or config.get("type") or "unknown"
        else:
            kwargs = getattr(config, "kwargs", {}) or {}
            expectation_type = getattr(config, "expectation_type", None) or getattr(config, "type", None) or "unknown"
        raw_result = getattr(r, "result", None) or {}
        result = raw_result if isinstance(raw_result, dict) else getattr(raw_result, "__dict__", {}) or {}

        # Map expectation_type to rule_type and description
        rule_type, description = _expectation_to_rule(expectation_type, kwargs)
        column = kwargs.get("column")
        columns = kwargs.get("column_list")
        failed_count = result.get("unexpected_count") or result.get("element_count") or 0
        partial = result.get("partial_unexpected_list") or result.get("unexpected_list") or []
        sample = list(partial)[:10]
        # optional: unexpected_index_list for sample_failed_ids
        sample_ids = result.get("unexpected_index_list") or []

        issues.append({
            "rule_type": rule_type,
            "expectation": expectation_type,
            "description": description,
            "column": column,
            "columns": columns if columns else None,
            "failed_count": failed_count,
            "sample_failed_values": sample,
            "sample_failed_ids": sample_ids[:10] if sample_ids else None,
            "severity": "high" if rule_type in ("duplicate_records", "missing_values", "referential_integrity") else "medium",
        })
    return issues


def _expectation_to_rule(expectation_type: str, kwargs: dict) -> tuple[str, str]:
    """Map GE expectation type to (rule_type, description)."""
    column = kwargs.get("column", "")
    column_list = kwargs.get("column_list", [])
    if "compound_columns_to_be_unique" in expectation_type or "multicolumn_values_to_be_unique" in expectation_type:
        return "duplicate_records", f"Duplicate values in {column_list}"
    if "values_to_be_unique" in expectation_type:
        return "duplicate_records", f"Duplicate values in {column}"
    if "not_be_null" in expectation_type:
        return "missing_values", f"Missing/null values in {column}"
    if "match_regex" in expectation_type:
        if column == "email":
            return "invalid_email_format", "Invalid email format"
        if column == "phone":
            return "inconsistent_phone_format", "Inconsistent phone format"
        if column == "state":
            return "standardization", "State should be 2-letter code"
        return "standardization", f"Format violation in {column}"
    if "be_between" in expectation_type:
        return "invalid_dates", f"Invalid or out-of-range date in {column}"
    if "be_in_set" in expectation_type:
        return "referential_integrity", f"Invalid or non-standard value in {column}"
    return "data_quality", f"Expectation failed: {expectation_type}"
