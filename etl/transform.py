# ─────────────────────────────────────────────────────────────
# etl/transform.py — Data Transformation Layer
# ─────────────────────────────────────────────────────────────
# This module performs the **Transformation** stage of the ETL pipeline.
# It converts cleaned staging data into analytical model tables:
# - Dimension tables (e.g., customers)
# - Fact tables (e.g., sales transactions)
#
# Responsibilities:
# 1. Derive dimensions and facts from cleaned inputs
# 2. Compute calculated fields (e.g., line amount)
# 3. Preserve referential integrity across keys
# ─────────────────────────────────────────────────────────────

from __future__ import annotations
import pandas as pd

def transform(dfs: dict[str, pd.DataFrame | None]) -> dict[str, pd.DataFrame | None]:
    """
    Transform cleaned data into analytical models.

    Args:
        dfs: Dictionary containing cleaned DataFrames
             - customers, orders, od (order details), products (optional)

    Returns:
        dict with transformed DataFrames:
            - dim_customer: dimension table for customer attributes
            - fact_sales: fact table capturing transaction-level sales
            - products: passthrough for product dimension (if available)
    """
    
    # ─────────────────────────────────────────────────────────────
    # STEP 1: Extract individual source DataFrames
    # ─────────────────────────────────────────────────────────────
    customers = dfs["customers"]
    orders    = dfs["orders"]
    od        = dfs["od"]
    products  = dfs["products"]  # may be None (not all datasets include this)
    
    # ─────────────────────────────────────────────────────────────
    # STEP 2: Build Dimension Table → dim_customer
    # ─────────────────────────────────────────────────────────────
    # Select only key attributes (ensures a clean, de-duplicated lookup table)
    # Result: One row per unique CustomerID
    dim_customer = (
        customers
        .loc[:, ["CustomerID", "CompanyName", "Country"]]
        .drop_duplicates()
    )
    
    # ─────────────────────────────────────────────────────────────
    # STEP 3: Build Fact Table → fact_sales
    # ─────────────────────────────────────────────────────────────
    # Join order details with order headers to include customer and date info.
    # Compute derived metrics:
    #   - order_date: normalized (date-only) version of OrderDate
    #   - line_amount: revenue at line-item level (price × qty × (1 - discount))
    fact_sales = (
        od.merge(
            orders[["OrderID", "CustomerID", "OrderDate"]],
            on="OrderID",
            how="inner"
        )
        .assign(
            order_date=lambda d: d["OrderDate"].dt.date,
            line_amount=lambda d: d["UnitPrice"] * d["Quantity"] * (1.0 - d["Discount"])
        )[[
            "OrderID",
            "CustomerID",
            "ProductID",
            "order_date",
            "UnitPrice",
            "Quantity",
            "Discount",
            "line_amount"
        ]]
    )
    
    # ─────────────────────────────────────────────────────────────
    # STEP 4: Return Transformed Tables
    # ─────────────────────────────────────────────────────────────
    # These outputs can be persisted or used directly by modeling or BI tools.
    return {
        "dim_customer": dim_customer,
        "fact_sales": fact_sales,
        "products": products,
    }
