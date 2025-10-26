# ─────────────────────────────────────────────────────────────
# etl/build.py — Data Modeling and Aggregation Stage
# ─────────────────────────────────────────────────────────────
# This module implements the **Build** stage of the ETL pipeline.
# It aggregates cleaned and transformed data into the modeled
# (“gold”) layer, producing summary tables for analytics and dashboards.
#
# Responsibilities:
# 1. Join dimension and fact tables
# 2. Compute key aggregations (sales by customer, country, product)
# 3. Persist final outputs as Parquet files under /02-model
#
# Output:
# Modeled datasets ready for visualization or reporting.
# ─────────────────────────────────────────────────────────────

from __future__ import annotations
import pandas as pd
from .paths import MODEL

# ─────────────────────────────────────────────────────────────
# MAIN BUILD FUNCTION
# ─────────────────────────────────────────────────────────────
def build(dfs: dict[str, pd.DataFrame | None]) -> dict[str, pd.DataFrame | None]:
    """
    Aggregate fact and dimension tables to produce modeled datasets.

    Steps:
      1. Join sales fact data with customers to compute totals per customer
      2. Aggregate sales by geographic region (country)
      3. Optionally, include product-level sales summary
      4. Save all results to the /02-model directory

    Args:
        dfs: Dictionary of transformed DataFrames:
             - dim_customer, fact_sales, and optional products

    Returns:
        dict containing dimension tables, fact tables,
        and aggregated summary DataFrames.
    """

    # ─────────────────────────────────────────────────────────────
    # STEP 1: Unpack transformed inputs
    # ─────────────────────────────────────────────────────────────
    # These represent the “silver” layer produced by the transform stage.
    dim_customer = dfs["dim_customer"]
    fact_sales   = dfs["fact_sales"]
    products     = dfs.get("products")

    # ─────────────────────────────────────────────────────────────
    # STEP 2: Aggregate sales by customer
    # ─────────────────────────────────────────────────────────────
    # Join fact_sales with customer dimension and compute total line_amount per customer.
    # Result → Ranked list of top customers by total sales volume.
    sales_by_customer = (
        fact_sales.merge(dim_customer, on="CustomerID", how="left")
                  .groupby("CompanyName", as_index=False, observed=True)["line_amount"].sum()
                  .sort_values("line_amount", ascending=False)
    )

    # ─────────────────────────────────────────────────────────────
    # STEP 3: Aggregate sales by country
    # ─────────────────────────────────────────────────────────────
    # Roll up the same metric (line_amount) but at a geographic level.
    # Useful for regional or market segmentation insights.
    sales_by_country = (
        fact_sales.merge(dim_customer, on="CustomerID", how="left")
                  .groupby("Country", as_index=False, observed=True)["line_amount"].sum()
                  .sort_values("line_amount", ascending=False)
    )

    # ─────────────────────────────────────────────────────────────
    # STEP 4: Optional — Aggregate sales by product
    # ─────────────────────────────────────────────────────────────
    # If product data is available, derive a similar aggregation for product-level insights.
    sales_by_product = None
    if (products is not None) and ("ProductID" in products.columns):
        # Build a lightweight product dimension to ensure unique keys
        dim_product = products.loc[:, ["ProductID", "ProductName", "CategoryID"]].drop_duplicates()

        sales_by_product = (
            fact_sales.merge(dim_product, on="ProductID", how="left")
                      .groupby("ProductName", as_index=False, observed=True)["line_amount"].sum()
                      .sort_values("line_amount", ascending=False)
        )

        # Persist product-level output
        sales_by_product.to_parquet(MODEL / "sales_by_product.parquet", index=False)

    # ─────────────────────────────────────────────────────────────
    # STEP 5: Persist modeled outputs to /02-model
    # ─────────────────────────────────────────────────────────────
    # These parquet files form the final analytical layer.
    dim_customer.to_parquet(MODEL / "dim_customer.parquet", index=False)
    fact_sales.to_parquet(MODEL / "fact_sales.parquet", index=False)
    sales_by_customer.to_parquet(MODEL / "sales_by_customer.parquet", index=False)
    sales_by_country.to_parquet(MODEL / "sales_by_country.parquet", index=False)

    # ─────────────────────────────────────────────────────────────
    # STEP 6: Return modeled DataFrames
    # ─────────────────────────────────────────────────────────────
    # Enables downstream reuse (e.g., dashboards or automated reports).
    return {
        "dim_customer": dim_customer,
        "fact_sales": fact_sales,
        "sales_by_customer": sales_by_customer,
        "sales_by_country": sales_by_country,
        "sales_by_product": sales_by_product,
    }
