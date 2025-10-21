# ─────────────────────────────────────────────────────────────
# dq.py — Data Quality (DQ) Validation and Logging
# ─────────────────────────────────────────────────────────────
# This module provides lightweight data quality checks and logging
# mechanisms for the ETL pipeline. It ensures that cleaned datasets
# conform to expected constraints before transformation or modeling.
#
# Responsibilities:
# 1. Validate referential integrity (FKs)
# 2. Detect schema or value-level anomalies (e.g., nulls, negatives)
# 3. Record validation results for audit and monitoring
# ─────────────────────────────────────────────────────────────

from __future__ import annotations
from datetime import datetime
import pandas as pd
from .paths import CLEAN

# ─────────────────────────────────────────────────────────────
# dq_checks — Core validation logic
# ─────────────────────────────────────────────────────────────
def dq_checks(customers: pd.DataFrame, orders: pd.DataFrame, od: pd.DataFrame) -> list[str]:
    """
    Perform rule-based data quality validation.

    Args:
        customers: Cleaned customer DataFrame
        orders: Cleaned orders DataFrame
        od: Cleaned order details DataFrame

    Returns:
        A list of issue messages describing DQ rule violations.
        Returns an empty list if all checks pass.
    """

    issues: list[str] = []

    # ─────────────────────────────────────────────────────────────
    # STEP 1: Basic schema validation (required columns)
    # ─────────────────────────────────────────────────────────────
    required_orders = {"OrderID", "CustomerID", "OrderDate"}
    missing_orders = required_orders - set(orders.columns)
    if missing_orders:
        # Early exit if schema mismatch prevents deeper checks
        issues.append(f"orders: missing columns {sorted(missing_orders)}")
        return issues

    # ─────────────────────────────────────────────────────────────
    # STEP 2: Order-level field validation
    # ─────────────────────────────────────────────────────────────
    if orders["OrderID"].isna().any():
        issues.append("orders: OrderID has nulls")
    if (orders["OrderID"] <= 0).any():
        issues.append("orders: OrderID must be > 0")

    # ─────────────────────────────────────────────────────────────
    # STEP 3: Numeric coercion for robust validation
    # ─────────────────────────────────────────────────────────────
    # Ensures type consistency even if upstream values were strings
    for col in ["Quantity", "UnitPrice", "Discount"]:
        if col in od.columns:
            od[col] = pd.to_numeric(od[col], errors="coerce")

    # ─────────────────────────────────────────────────────────────
    # STEP 4: Value constraints for order details
    # ─────────────────────────────────────────────────────────────
    if (od["Quantity"] <= 0).any():
        issues.append("order_details: Quantity must be > 0")
    if (od["UnitPrice"] < 0).any():
        issues.append("order_details: UnitPrice must be >= 0")

    # Discount must be between 0 and 1 inclusive
    bad_disc = od["Discount"].lt(0) | od["Discount"].gt(1)
    if bad_disc.any():
        issues.append(f"order_details: Discount must be 0..1 (bad={int(bad_disc.sum())})")

    # ─────────────────────────────────────────────────────────────
    # STEP 5: Referential integrity (Customer FK)
    # ─────────────────────────────────────────────────────────────
    if "CustomerID" in customers and "CustomerID" in orders:
        missing_fk = (~orders["CustomerID"].isin(customers["CustomerID"])).sum()
        if missing_fk > 0:
            issues.append(f"orders: missing customer FK ({int(missing_fk)})")

    return issues


# ─────────────────────────────────────────────────────────────
# write_dq_logs — Logging and audit trail
# ─────────────────────────────────────────────────────────────
def write_dq_logs(issues: list[str]) -> None:
    """
    Persist DQ run summary and detailed issue logs.

    Creates two parquet outputs under data/01-clean/_dq:
      - dq_runs.parquet: summary of each run (timestamp, pass/fail, issue count)
      - dq_issues.parquet: detailed list of rule violations per run

    Args:
        issues: List of data quality issue strings
    """

    # Current UTC timestamp for consistent audit trail
    ts = datetime.utcnow().isoformat()

    # Ensure DQ output folder exists
    (CLEAN / "_dq").mkdir(parents=True, exist_ok=True)

    # ─────────────────────────────────────────────────────────────
    # Summary log: one record per DQ run
    # ─────────────────────────────────────────────────────────────
    pd.DataFrame([{
        "run_ts": ts,
        "status": "PASS" if not issues else "FAIL",
        "issues_count": len(issues),
    }]).to_parquet(CLEAN / "_dq" / "dq_runs.parquet", index=False)

    # ─────────────────────────────────────────────────────────────
    # Detailed log: one row per detected issue
    # ─────────────────────────────────────────────────────────────
    pd.DataFrame({
        "run_ts": [ts] * len(issues),
        "detail": issues
    }).to_parquet(CLEAN / "_dq" / "dq_issues.parquet", index=False)
