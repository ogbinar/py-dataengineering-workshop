import streamlit as st
import pandas as pd
from pathlib import Path

MODEL = Path("data/02-model")
CLEAN = Path("data/01-clean")
DQDIR = CLEAN / "_dq"

st.set_page_config(page_title="Py Data Engineering — Northwind", layout="wide")
st.title("🏪 Northwind — pandas + pyarrow + Streamlit (uv)")

tab_sales, tab_country, tab_dq = st.tabs(["📊 Sales (Customers)", "🌍 Sales by Country", "🧪 Data Quality"])

with tab_sales:
    p = MODEL / "sales_by_customer.parquet"
    if not p.exists():
        st.warning("Run the pipeline first: `uv run etl` (or `etl-model`).")
    else:
        df = pd.read_parquet(p)
        st.subheader("Top Customers")
        st.bar_chart(df.set_index("CompanyName"))

        p_prod = MODEL / "sales_by_product.parquet"
        if p_prod.exists():
            prod = pd.read_parquet(p_prod)
            st.subheader("Top Products")
            st.bar_chart(prod.set_index("ProductName"))

with tab_country:
    p = MODEL / "sales_by_country.parquet"
    if p.exists():
        country = pd.read_parquet(p)
        st.subheader("Sales by Country")
        st.bar_chart(country.set_index("Country"))
    else:
        st.info("Generate via pipeline → model stage")

with tab_dq:
    runs_pq = DQDIR / "dq_runs.parquet"
    issues_pq = DQDIR / "dq_issues.parquet"
    if runs_pq.exists():
        runs = pd.read_parquet(runs_pq)
        st.subheader("DQ Runs (latest 10)")
        st.dataframe(runs.tail(10), use_container_width=True)
        if issues_pq.exists():
            issues = pd.read_parquet(issues_pq)
            if not issues.empty:
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
        st.info("No DQ runs yet.")
