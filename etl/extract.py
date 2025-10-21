# ─────────────────────────────────────────────────────────────
# etl/extract.py — Data Extraction Stage
# ─────────────────────────────────────────────────────────────
# This module implements the **Extract** phase of the ETL pipeline.
# It retrieves the Northwind dataset from a public GitHub source
# and loads it into pandas DataFrames for further processing.
#
# Responsibilities:
# 1. Download source CSVs (if not present locally)
# 2. Validate required inputs exist
# 3. Read CSVs into memory with robust error handling
#
# Output:
# A dictionary of raw DataFrames for customers, orders, order details,
# and optionally products — forming the pipeline’s starting point.
# ─────────────────────────────────────────────────────────────

from pathlib import Path
import urllib.request
import pandas as pd

# ─────────────────────────────────────────────────────────────
# CONFIGURATION — Paths and Data Source
# ─────────────────────────────────────────────────────────────

# Define base data directories following the ETL convention
DATA = Path("data")
RAW = DATA / "00-raw"
RAW.mkdir(parents=True, exist_ok=True)  # Ensure folder exists before writing

# GitHub repository hosting the sample Northwind data
BASE_URL = "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data"

# Mapping between local and remote filenames (standardized for clarity)
FILES = {
    "Customers.csv": "customers.csv",           # Customer master data
    "Orders.csv": "orders.csv",                 # Order header data
    "Order_Details.csv": "order-details.csv",   # Order line items
    "Products.csv": "products.csv"              # Product catalog (optional)
}

# ─────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────
def download_files(use_products: bool = True):
    """
    Download CSV files from GitHub if they are not yet present locally.

    Args:
        use_products: If False, skip the Products.csv download (optional table)
    """
    for local_name, remote_name in FILES.items():
        # Skip products file if not required by current run
        if not use_products and local_name == "Products.csv":
            continue

        filepath = RAW / local_name

        # Avoid redundant downloads (idempotent extraction)
        if not filepath.exists():
            url = f"{BASE_URL}/{remote_name}"
            print(f"[EXTRACT] Downloading {url} → {filepath}")
            urllib.request.urlretrieve(url, filepath)


def read_csv_robust(path: Path) -> pd.DataFrame:
    """
    Safely read CSV into a DataFrame with fallback error handling.

    Strategy:
      1. Attempt standard read_csv (fast and reliable for clean files)
      2. On failure, retry with on_bad_lines='skip' to ignore corrupt rows

    Args:
        path: Local path to CSV file

    Returns:
        DataFrame with parsed contents
    """
    try:
        # Primary read — handles UTF-8 BOM automatically
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        # Fallback — skip malformed lines while logging warnings
        return pd.read_csv(path, encoding="utf-8-sig", on_bad_lines="skip")


# ─────────────────────────────────────────────────────────────
# MAIN EXTRACTION FUNCTION
# ─────────────────────────────────────────────────────────────
def extract(use_products: bool = True, auto_download: bool = True) -> dict[str, pd.DataFrame | None]:
    """
    Extract Northwind CSV data and load into pandas DataFrames.

    Steps:
      1. Download missing files (if auto_download=True)
      2. Validate that required files exist
      3. Load each file into a DataFrame
      4. Optionally include Products table

    Args:
        use_products: Whether to include Products.csv
        auto_download: Automatically download any missing CSVs

    Returns:
        dict containing:
            - customers → customer master data
            - orders → order header records
            - od → order detail (line items)
            - products → product list (or None if excluded)

    Raises:
        FileNotFoundError: If mandatory files are missing
    """

    # ─────────────────────────────────────────────────────────────
    # STEP 1: Download missing files (if enabled)
    # ─────────────────────────────────────────────────────────────
    if auto_download:
        download_files(use_products)

    # ─────────────────────────────────────────────────────────────
    # STEP 2: Validate presence of required files
    # ─────────────────────────────────────────────────────────────
    required = ["Customers.csv", "Orders.csv", "Order_Details.csv"]
    missing = [f for f in required if not (RAW / f).exists()]
    if missing:
        raise FileNotFoundError(f"Missing required files: {', '.join(missing)}")

    # ─────────────────────────────────────────────────────────────
    # STEP 3: Load mandatory CSVs into DataFrames
    # ─────────────────────────────────────────────────────────────
    result = {
        "customers": read_csv_robust(RAW / "Customers.csv"),
        "orders": read_csv_robust(RAW / "Orders.csv"),
        "od": read_csv_robust(RAW / "Order_Details.csv"),  # "od" = order details
        "products": None,  # placeholder (loaded next if applicable)
    }

    # ─────────────────────────────────────────────────────────────
    # STEP 4: Optionally load product catalog
    # ─────────────────────────────────────────────────────────────
    # Products are optional — included for richer modeling, but not mandatory
    if use_products and (RAW / "Products.csv").exists():
        result["products"] = read_csv_robust(RAW / "Products.csv")

    return result
