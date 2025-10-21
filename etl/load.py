# ─────────────────────────────────────────────────────────────
# etl/load.py — Data Loading and Cleaning Stage
# ─────────────────────────────────────────────────────────────
# This module implements the **Load** stage of the ETL pipeline.
# It receives raw DataFrames extracted from source CSVs and:
#   1. Standardizes schema and column names
#   2. Validates required columns
#   3. Cleans and converts data types
#   4. Persists cleaned outputs to Parquet
#   5. Runs automated Data Quality (DQ) checks
#
# The result is a consistent, validated, and analysis-ready dataset.
# ─────────────────────────────────────────────────────────────

import pandas as pd
from .paths import CLEAN
from .dq import dq_checks, write_dq_logs

# ─────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────
def rename_columns(df: pd.DataFrame, expected_cols: dict[str, str]) -> pd.DataFrame:
    """
    Standardize column names to match expected schema (case-insensitive).

    Args:
        df: Input DataFrame to rename.
        expected_cols: Dict mapping lowercase field names → proper case names.

    Returns:
        DataFrame with renamed columns for consistent downstream usage.
    """
    # Build lowercase lookup for actual column names
    current_cols = {col.lower(): col for col in df.columns}

    # Map actual → target names where matches are found
    rename_map = {
        current_cols[expected_lower]: proper_name
        for expected_lower, proper_name in expected_cols.items()
        if expected_lower in current_cols
    }

    return df.rename(columns=rename_map)


def validate_required_columns(df: pd.DataFrame, required_cols: set[str], table_name: str):
    """
    Enforce required schema integrity by checking for missing columns.

    Args:
        df: DataFrame to validate.
        required_cols: Set of column names that must exist.
        table_name: Logical name of the table (for error context).

    Raises:
        KeyError if one or more required columns are missing.
    """
    missing = required_cols - set(df.columns)
    if missing:
        raise KeyError(f"{table_name}: missing required columns: {sorted(missing)}")


# ─────────────────────────────────────────────────────────────
# MAIN LOAD FUNCTION
# ─────────────────────────────────────────────────────────────
def load(dfs: dict[str, pd.DataFrame | None]) -> dict[str, pd.DataFrame | None]:
    """
    Load and clean extracted data, then persist results to Parquet.

    Steps:
      1. Standardize column names (case-insensitive)
      2. Validate required columns exist
      3. Convert data types and handle missing values
      4. Save cleaned data as Parquet files
      5. Run data quality checks and log issues

    Args:
        dfs: Dictionary containing extracted DataFrames:
             - customers, orders, od (order details), products (optional)

    Returns:
        dict containing cleaned DataFrames and DQ issue list.
    """

    # ─────────────────────────────────────────────────────────────
    # STEP 1: Standardize column names
    # ─────────────────────────────────────────────────────────────
    # Normalize capitalization and align schemas to expected naming convention.
    customers = rename_columns(dfs["customers"].copy(), {
        "customerid": "CustomerID",
        "companyname": "CompanyName",
        "country": "Country",
    })

    orders = rename_columns(dfs["orders"].copy(), {
        "orderid": "OrderID",
        "customerid": "CustomerID",
        "orderdate": "OrderDate",
    })

    od = rename_columns(dfs["od"].copy(), {
        "orderid": "OrderID",
        "productid": "ProductID",
        "unitprice": "UnitPrice",
        "quantity": "Quantity",
        "discount": "Discount",
    })

    products = None
    if dfs["products"] is not None:
        products = rename_columns(dfs["products"].copy(), {
            "productid": "ProductID",
            "productname": "ProductName",
            "categoryid": "CategoryID",
        })

    # ─────────────────────────────────────────────────────────────
    # STEP 2: Validate required columns exist
    # ─────────────────────────────────────────────────────────────
    # Ensures structural completeness of all tables before cleaning.
    validate_required_columns(
        customers, {"CustomerID", "CompanyName", "Country"}, "customers"
    )
    validate_required_columns(
        orders, {"OrderID", "CustomerID", "OrderDate"}, "orders"
    )
    validate_required_columns(
        od, {"OrderID", "ProductID", "UnitPrice", "Quantity", "Discount"}, "order_details"
    )

    # ─────────────────────────────────────────────────────────────
    # STEP 3: Clean and convert data types
    # ─────────────────────────────────────────────────────────────
    # Convert text-based fields into consistent data types and remove invalid records.
    # Example: parse dates, coerce numeric columns, and handle null values.
    orders["OrderDate"] = pd.to_datetime(orders["OrderDate"], errors="coerce")
    orders = orders.dropna(subset=["OrderDate"])  # remove invalid or missing dates

    for col in ["Quantity", "UnitPrice", "Discount"]:
        od[col] = pd.to_numeric(od[col], errors="coerce")

    od["Discount"] = od["Discount"].fillna(0.0)  # treat missing discounts as 0
    od = od.dropna(subset=["Quantity", "UnitPrice"])  # enforce numeric completeness

    # ─────────────────────────────────────────────────────────────
    # STEP 4: Save cleaned data as Parquet files
    # ─────────────────────────────────────────────────────────────
    # Persist all cleaned tables into the standard /01-clean layer for downstream use.
    customers.to_parquet(CLEAN / "customers.parquet", index=False)
    orders.to_parquet(CLEAN / "orders.parquet", index=False)
    od.to_parquet(CLEAN / "order_details.parquet", index=False)

    if products is not None:
        products.to_parquet(CLEAN / "products.parquet", index=False)

    # ─────────────────────────────────────────────────────────────
    # STEP 5: Run Data Quality (DQ) checks and log issues
    # ─────────────────────────────────────────────────────────────
    # Validate referential integrity, numeric ranges, and missing data.
    # Logs both summary and detailed issues in /01-clean/_dq for audit.
    issues = dq_checks(customers, orders, od)
    write_dq_logs(issues)

    # Return cleaned tables and DQ results for downstream use
    return {
        "customers": customers,
        "orders": orders,
        "od": od,
        "products": products,
        "dq_issues": issues,
    }
