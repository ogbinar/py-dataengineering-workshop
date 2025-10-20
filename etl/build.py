# etl/build.py
from __future__ import annotations
import pandas as pd
from .paths import MODEL

def build(dfs: dict[str, pd.DataFrame | None]) -> dict[str, pd.DataFrame | None]:
    dim_customer = dfs["dim_customer"]
    fact_sales   = dfs["fact_sales"]
    products     = dfs.get("products")

    sales_by_customer = (
        fact_sales.merge(dim_customer, on="CustomerID", how="left")
                  .groupby("CompanyName", as_index=False, observed=True)["line_amount"].sum()
                  .sort_values("line_amount", ascending=False)
    )

    sales_by_country = (
        fact_sales.merge(dim_customer, on="CustomerID", how="left")
                  .groupby("Country", as_index=False, observed=True)["line_amount"].sum()
                  .sort_values("line_amount", ascending=False)
    )

    sales_by_product = None
    if (products is not None) and ("ProductID" in products.columns):
        dim_product = products.loc[:, ["ProductID","ProductName","CategoryID"]].drop_duplicates()
        sales_by_product = (
            fact_sales.merge(dim_product, on="ProductID", how="left")
                      .groupby("ProductName", as_index=False, observed=True)["line_amount"].sum()
                      .sort_values("line_amount", ascending=False)
        )
        sales_by_product.to_parquet(MODEL / "sales_by_product.parquet", index=False)

    # Persist modeled/aggregated layer
    dim_customer.to_parquet(MODEL / "dim_customer.parquet", index=False)
    fact_sales.to_parquet(MODEL / "fact_sales.parquet", index=False)
    sales_by_customer.to_parquet(MODEL / "sales_by_customer.parquet", index=False)
    sales_by_country.to_parquet(MODEL / "sales_by_country.parquet", index=False)

    return {
        "dim_customer": dim_customer,
        "fact_sales": fact_sales,
        "sales_by_customer": sales_by_customer,
        "sales_by_country": sales_by_country,
        "sales_by_product": sales_by_product,
    }
