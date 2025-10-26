# ─────────────────────────────────────────────────────────────
# sandbox_explore_parquet.py — Quick Parquet File Explorer
# ─────────────────────────────────────────────────────────────
# Lightweight inspection tool using pandas + pyarrow.
# Allows you to preview any Parquet file produced by the ETL pipeline.
# Works across Windows, macOS, and Linux.
# ─────────────────────────────────────────────────────────────

import pandas as pd
from pathlib import Path

# Base directory for modeled data
MODEL = Path("data/02-model")

# Choose which Parquet file to inspect
target = MODEL / "sales_by_customer.parquet"   # change as needed

if not target.exists():
    raise FileNotFoundError(f"❌ Parquet file not found: {target}")

print(f"📂 Reading: {target}")

# Load file (pyarrow backend is used automatically)
df = pd.read_parquet(target)

# Display basic information
print("\n=== HEAD (first 5 rows) ===")
print(df.head(), "\n")

print("=== INFO ===")
print(df.info(), "\n")

print("=== SUMMARY STATISTICS ===")
print(df.describe(include='all'), "\n")

print(f"✅ Loaded {len(df):,} rows × {len(df.columns)} columns")
