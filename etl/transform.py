# etl/transform.py
from __future__ import annotations
import pandas as pd

def transform(dfs: dict[str, pd.DataFrame | None]) -> dict[str, pd.DataFrame | None]:
    customers = dfs["customers"]
    orders    = dfs["orders"]
    od        = dfs["od"]
    products  = dfs["products"]  # may be None

    dim_customer = customers.loc[:, ["CustomerID", "CompanyName", "Country"]].drop_duplicates()

    fact_sales = (
        od.merge(orders[["OrderID", "CustomerID", "OrderDate"]], on="OrderID", how="inner")
          .assign(
              order_date=lambda d: d["OrderDate"].dt.date,
              line_amount=lambda d: d["UnitPrice"] * d["Quantity"] * (1.0 - d["Discount"])
          )[["OrderID","CustomerID","ProductID","order_date","UnitPrice","Quantity","Discount","line_amount"]]
    )

    return {
        "dim_customer": dim_customer,
        "fact_sales": fact_sales,
        "products": products,
    }
