from __future__ import annotations
from datetime import datetime
import pandas as pd
from .paths import CLEAN

def dq_checks(customers: pd.DataFrame, orders: pd.DataFrame, od: pd.DataFrame) -> list[str]:
    issues: list[str] = []

    required_orders = {"OrderID", "CustomerID", "OrderDate"}
    missing_orders = required_orders - set(orders.columns)
    if missing_orders:
        issues.append(f"orders: missing columns {sorted(missing_orders)}")
        return issues

    if orders["OrderID"].isna().any(): issues.append("orders: OrderID has nulls")
    if (orders["OrderID"] <= 0).any(): issues.append("orders: OrderID must be > 0")

    # Coerce numerics for robust checks
    for col in ["Quantity", "UnitPrice", "Discount"]:
        if col in od.columns:
            od[col] = pd.to_numeric(od[col], errors="coerce")

    if (od["Quantity"] <= 0).any(): issues.append("order_details: Quantity must be > 0")
    if (od["UnitPrice"] < 0).any(): issues.append("order_details: UnitPrice must be >= 0")
    bad_disc = od["Discount"].lt(0) | od["Discount"].gt(1)
    if bad_disc.any(): issues.append(f"order_details: Discount must be 0..1 (bad={int(bad_disc.sum())})")

    if "CustomerID" in customers and "CustomerID" in orders:
        missing_fk = (~orders["CustomerID"].isin(customers["CustomerID"])).sum()
        if missing_fk > 0:
            issues.append(f"orders: missing customer FK ({int(missing_fk)})")

    return issues

def write_dq_logs(issues: list[str]) -> None:
    ts = datetime.utcnow().isoformat()
    (CLEAN / "_dq").mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{
        "run_ts": ts,
        "status": "PASS" if not issues else "FAIL",
        "issues_count": len(issues),
    }]).to_parquet(CLEAN / "_dq" / "dq_runs.parquet", index=False)
    pd.DataFrame({"run_ts": [ts]*len(issues), "detail": issues}) \
      .to_parquet(CLEAN / "_dq" / "dq_issues.parquet", index=False)
