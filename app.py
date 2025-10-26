# ─────────────────────────────────────────────────────────────
# app.py — Streamlit Dashboard for Northwind Data Pipeline
# ─────────────────────────────────────────────────────────────
# This app provides a simple interactive interface to visualize
# outputs from the Py Data Engineering Workshop pipeline.
#
# Responsibilities:
# 1. Display aggregated sales metrics (customers, products, countries)
# 2. Show recent Data Quality (DQ) validation runs
# 3. Provide pipeline run guidance if outputs are missing
#
# The dashboard reads data directly from the /data folder,
# enabling a fully local and self-contained analytics workflow.
# ─────────────────────────────────────────────────────────────

import streamlit as st
import pandas as pd
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# Directory Configuration (aligned with ETL paths.py)
# ─────────────────────────────────────────────────────────────
MODEL = Path("data/02-model")     # modeled/aggregated outputs
CLEAN = Path("data/01-clean")     # cleaned tables
DQDIR = CLEAN / "_dq"             # data quality logs and reports

# ─────────────────────────────────────────────────────────────
# Streamlit App Configuration
# ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="Py Data Engineering — Northwind", layout="wide")
st.title("🏪 Northwind — Py Dashboard")

# Create tabbed layout for key analytics sections
tab_sales, tab_country, tab_dq = st.tabs([
    "📊 Sales (Customers)",
    "🌍 Sales by Country",
    "🧪 Data Quality"
])

# ─────────────────────────────────────────────────────────────
# TAB 1: Sales by Customer and Product
# ─────────────────────────────────────────────────────────────
with tab_sales:
    p = MODEL / "sales_by_customer.parquet"
    
    # Validate that the model file exists before visualization
    if not p.exists():
        st.warning("Run the pipeline first: `uv run etl` (or `etl-model`).")
    else:
        # Load pre-aggregated sales by customer
        df = pd.read_parquet(p)
        st.subheader("Top Customers")
        st.bar_chart(df.set_index("CompanyName"))  # visual ranking by total sales

        # Optional: Sales by product (if model step included it)
        p_prod = MODEL / "sales_by_product.parquet"
        if p_prod.exists():
            prod = pd.read_parquet(p_prod)
            st.subheader("Top Products")
            st.bar_chart(prod.set_index("ProductName"))

# ─────────────────────────────────────────────────────────────
# TAB 2: Sales by Country
# ─────────────────────────────────────────────────────────────
with tab_country:
    p = MODEL / "sales_by_country.parquet"
    
    if p.exists():
        # Load aggregated data and render bar chart
        country = pd.read_parquet(p)
        st.subheader("Sales by Country")
        st.bar_chart(country.set_index("Country"))
    else:
        # Provide instructional message if no model data available
        st.info("Generate via pipeline → model stage")

# ─────────────────────────────────────────────────────────────
# TAB 3: Data Quality Monitoring
# ─────────────────────────────────────────────────────────────
with tab_dq:
    runs_pq = DQDIR / "dq_runs.parquet"
    issues_pq = DQDIR / "dq_issues.parquet"
    
    if runs_pq.exists():
        # Summary log: recent DQ runs (status + issue count)
        runs = pd.read_parquet(runs_pq)
        st.subheader("DQ Runs (latest 10)")
        st.dataframe(runs.tail(10), use_container_width=True)
        
        # Detailed log: issue-level reporting for latest run
        if issues_pq.exists():
            issues = pd.read_parquet(issues_pq)
            if not issues.empty:
                # Identify the most recent timestamp
                last_ts = runs.sort_values("run_ts")["run_ts"].iloc[-1]
                latest = issues[issues["run_ts"] == last_ts]

                if latest.empty:
                    st.success("All checks passed 🎉")
                else:
                    st.warning(f"{len(latest)} issue(s) in latest run")
                    st.dataframe(latest, use_container_width=True)
            else:
                st.success("All checks passed 🎉")
    else:
        # Fallback message if DQ checks haven’t been executed
        st.info("No DQ runs yet.")
