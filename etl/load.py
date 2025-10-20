from __future__ import annotations
import pandas as pd
from .paths import CLEAN
from .dq import dq_checks, write_dq_logs

def _rename_columns(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    cols = {c: c for c in df.columns}  # identity
    lower_map = {c.lower(): c for c in df.columns}
    # build a rename dict using lowercase lookup
    rename_dict = {}
    for src_lower, dst in mapping.items():
        if src_lower in lower_map:
            rename_dict[lower_map[src_lower]] = dst
    return df.rename(columns=rename_dict)

def _standardize_schemas(dfs: dict[str, pd.DataFrame | None]) -> dict[str, pd.DataFrame | None]:
    customers = dfs["customers"].copy()
    orders    = dfs["orders"].copy()
    od        = dfs["od"].copy()
    products  = dfs["products"]

    # Normalize headers: camelCase -> TitleCase expected by pipeline
    customers = _rename_columns(customers, {
        "customerid": "CustomerID",
        "companyname": "CompanyName",
        "country": "Country",
    })

    orders = _rename_columns(orders, {
        "orderid": "OrderID",
        "customerid": "CustomerID",
        "orderdate": "OrderDate",
        # (others are optional for this pipeline: employeeid, shippeddate, etc.)
    })

    od = _rename_columns(od, {
        "orderid": "OrderID",
        "productid": "ProductID",
        "unitprice": "UnitPrice",
        "quantity": "Quantity",
        "discount": "Discount",
    })

    if products is not None:
        products = _rename_columns(products, {
            "productid": "ProductID",
            "productname": "ProductName",
            "categoryid": "CategoryID",
        })

    # Sanity: required columns after rename
    required_orders = {"OrderID", "CustomerID", "OrderDate"}
    missing = required_orders - set(orders.columns)
    if missing:
        raise KeyError(f"orders: missing required columns after rename: {sorted(missing)}")

    required_od = {"OrderID", "ProductID", "UnitPrice", "Quantity", "Discount"}
    missing_od = required_od - set(od.columns)
    if missing_od:
        raise KeyError(f"order_details: missing required columns after rename: {sorted(missing_od)}")

    required_customers = {"CustomerID", "CompanyName", "Country"}
    missing_c = required_customers - set(customers.columns)
    if missing_c:
        raise KeyError(f"customers: missing required columns after rename: {sorted(missing_c)}")

    return {"customers": customers, "orders": orders, "od": od, "products": products}

def load(dfs: dict[str, pd.DataFrame | None]) -> dict[str, pd.DataFrame | None]:
    # 1) normalize schemas to expected names
    dfs = _standardize_schemas(dfs)

    customers = dfs["customers"]
    orders    = dfs["orders"].copy()
    od        = dfs["od"].copy()
    products  = dfs["products"]

    # 2) typing & cleaning
    orders["OrderDate"] = pd.to_datetime(orders["OrderDate"], errors="coerce")

    for col in ["Quantity", "UnitPrice", "Discount"]:
        if col in od.columns:
            od[col] = pd.to_numeric(od[col], errors="coerce")

    orders = orders.dropna(subset=["OrderDate"]).copy()
    od["Discount"] = od["Discount"].fillna(0.0)
    od = od.dropna(subset=["Quantity", "UnitPrice"]).copy()

    # 3) persist clean layer
    customers.to_parquet(CLEAN / "customers.parquet", index=False)
    orders.to_parquet(CLEAN / "orders.parquet", index=False)
    od.to_parquet(CLEAN / "order_details.parquet", index=False)
    if products is not None:
        products.to_parquet(CLEAN / "products.parquet", index=False)

    # 4) data quality logs
    issues = dq_checks(customers, orders, od)
    write_dq_logs(issues)

    return {
        "customers": customers,
        "orders": orders,
        "od": od,
        "products": products,
        "dq_issues": issues,
    }
