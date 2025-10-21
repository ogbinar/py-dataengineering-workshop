# ─────────────────────────────────────────────────────────────
# etl/run.py — Orchestrator for the Full ETL Pipeline
# ─────────────────────────────────────────────────────────────
# This script provides a unified entry point to execute the ETL workflow.
# It wires together the modular stages:
#     extract → load → transform → build
#
# Each stage can be run independently or in sequence (via CLI).
# The design enables:
#   - Reproducible, stepwise debugging
#   - Flexible reruns of partial stages
#   - Compatibility with CLI and programmatic use
# ─────────────────────────────────────────────────────────────

from __future__ import annotations
import argparse

# Import pipeline stages from local ETL modules
from .extract import extract
from .load import load as load_stage
from .transform import transform
from .build import build as build_stage


# ─────────────────────────────────────────────────────────────
# STAGE 1: Extract
# ─────────────────────────────────────────────────────────────
def run_extract(use_products: bool = True):
    """
    Run the extraction stage:
    - Reads raw CSVs from data/00-raw
    - Returns DataFrames for customers, orders, order_details, and optionally products
    """
    dfs = extract(use_products=use_products)
    print("[EXTRACT] Loaded:", ", ".join([k for k, v in dfs.items() if v is not None]))
    return dfs


# ─────────────────────────────────────────────────────────────
# STAGE 2: Load (includes cleaning + DQ)
# ─────────────────────────────────────────────────────────────
def run_load(use_products: bool = True):
    """
    Run the loading stage:
    - Extracts raw data
    - Cleans and validates datasets
    - Writes cleaned Parquet files and runs DQ checks
    """
    raw_dfs = run_extract(use_products=use_products)
    clean_dfs = load_stage(raw_dfs)
    print("[LOAD] Written clean parquet + DQ. Issues:", clean_dfs.get("dq_issues", []))
    return clean_dfs


# ─────────────────────────────────────────────────────────────
# STAGE 3: Transform
# ─────────────────────────────────────────────────────────────
def run_transform(use_products: bool = True):
    """
    Run the transformation stage:
    - Loads and cleans data via load()
    - Produces analytical model tables (dim_customer, fact_sales)
    - Returns transformed DataFrames ready for modeling or aggregation
    """
    clean_dfs = run_load(use_products=use_products)
    modeled = transform(clean_dfs)
    print("[TRANSFORM] Produced dim_customer + fact_sales.")
    return modeled


# ─────────────────────────────────────────────────────────────
# STAGE 4: Build (aggregations and exports)
# ─────────────────────────────────────────────────────────────
def run_build(use_products: bool = True):
    """
    Run the build stage:
    - Transforms the data
    - Produces and writes aggregated model outputs such as:
        * sales_by_customer
        * sales_by_country
        * sales_by_product (if applicable)
    - Represents the 'gold' layer of the pipeline
    """
    modeled = run_transform(use_products=use_products)
    gold = build_stage(modelled := modeled)  # internal alias for readability in logs
    print(
        "[BUILD] Written: dim_customer, fact_sales, sales_by_customer, sales_by_country",
        "(+ sales_by_product if products present)"
    )
    return gold


# ─────────────────────────────────────────────────────────────
# MAIN ENTRY POINT — Command-Line Interface (CLI)
# ─────────────────────────────────────────────────────────────
def main():
    """
    Command-line controller for the ETL pipeline.

    Usage examples:
        uv run python -m etl.run                  → runs full pipeline
        uv run python -m etl.run --stage load     → runs extract + load only
        uv run python -m etl.run --no-products    → excludes product table
    """
    p = argparse.ArgumentParser(
        description="Run pipeline stages (extract → load → transform → build)"
    )
    p.add_argument("--stage", choices=["extract", "load", "transform", "build", "all"], default="all")
    p.add_argument("--no-products", action="store_true")
    args = p.parse_args()

    # Toggle product inclusion
    use_products = not args.no_products

    # Map stage argument to corresponding run function
    if args.stage == "extract":
        run_extract(use_products=use_products)
    elif args.stage == "load":
        run_load(use_products=use_products)
    elif args.stage == "transform":
        run_transform(use_products=use_products)
    else:
        run_build(use_products=use_products)  # default or "all"

# ─────────────────────────────────────────────────────────────
# Script Entrypoint
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
