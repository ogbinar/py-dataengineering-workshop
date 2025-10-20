# etl/run.py
from __future__ import annotations
import argparse

from .extract import extract
from .load import load as load_stage
from .transform import transform
from .build import build as build_stage

def run_extract(use_products: bool = True):
    dfs = extract(use_products=use_products)
    print("[EXTRACT] Loaded:", ", ".join([k for k, v in dfs.items() if v is not None]))
    return dfs

def run_load(use_products: bool = True):
    raw_dfs = run_extract(use_products=use_products)
    clean_dfs = load_stage(raw_dfs)
    print("[LOAD] Written clean parquet + DQ. Issues:", clean_dfs.get("dq_issues", []))
    return clean_dfs

def run_transform(use_products: bool = True):
    clean_dfs = run_load(use_products=use_products)
    modeled = transform(clean_dfs)
    print("[TRANSFORM] Produced dim_customer + fact_sales.")
    return modeled

def run_build(use_products: bool = True):
    modeled = run_transform(use_products=use_products)
    gold = build_stage(modelled := modeled)  # just to name it “modeled” in logs if you want
    print("[BUILD] Written: dim_customer, fact_sales, sales_by_customer, sales_by_country",
          "(+ sales_by_product if products present)")
    return gold

def main():
    p = argparse.ArgumentParser(description="Run pipeline stages (extract → load → transform → build)")
    p.add_argument("--stage", choices=["extract","load","transform","build","all"], default="all")
    p.add_argument("--no-products", action="store_true")
    args = p.parse_args()
    use_products = not args.no_products

    if args.stage == "extract":
        run_extract(use_products=use_products)
    elif args.stage == "load":
        run_load(use_products=use_products)
    elif args.stage == "transform":
        run_transform(use_products=use_products)
    else:
        run_build(use_products=use_products)  # build or all

if __name__ == "__main__":
    main()
