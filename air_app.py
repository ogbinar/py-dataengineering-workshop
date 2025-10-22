# ─────────────────────────────────────────────────────────────
# app.py — Air Dashboard (Seaborn version)
# ─────────────────────────────────────────────────────────────
import io
from pathlib import Path

import air
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from fastapi.responses import StreamingResponse

# ─────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────
MODEL = Path("data/02-model")
CLEAN = Path("data/01-clean")
DQDIR = CLEAN / "_dq"

# ─────────────────────────────────────────────────────────────
# App init
# ─────────────────────────────────────────────────────────────
app = air.Air()
sns.set_theme(style="whitegrid")

# ─────────────────────────────────────────────────────────────
# Utility to render Seaborn chart to PNG stream
# ─────────────────────────────────────────────────────────────
def render_chart(fig) -> StreamingResponse:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return StreamingResponse(buf, media_type="image/png")

# ─────────────────────────────────────────────────────────────
# Chart endpoints (return PNG)
# ─────────────────────────────────────────────────────────────
@app.get("/chart/customers")
def chart_customers():
    p = MODEL / "sales_by_customer.parquet"
    if not p.exists():
        return StreamingResponse(io.BytesIO(), media_type="image/png")

    df = pd.read_parquet(p)
    if "CompanyName" not in df.columns:
        return StreamingResponse(io.BytesIO(), media_type="image/png")

    val_cols = [c for c in df.columns if c != "CompanyName" and pd.api.types.is_numeric_dtype(df[c])]
    if not val_cols:
        return StreamingResponse(io.BytesIO(), media_type="image/png")

    ycol = val_cols[0]
    # ✅ Select top 10 by metric value
    df = df.sort_values(ycol, ascending=False).head(10)

    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(df, x="CompanyName", y=ycol, ax=ax, color="steelblue")
    ax.set_title("Top 10 Customers")
    ax.tick_params(axis="x", rotation=40)
    return render_chart(fig)


@app.get("/chart/products")
def chart_products():
    p = MODEL / "sales_by_product.parquet"
    if not p.exists():
        return StreamingResponse(io.BytesIO(), media_type="image/png")

    df = pd.read_parquet(p)
    if "ProductName" not in df.columns:
        return StreamingResponse(io.BytesIO(), media_type="image/png")

    val_cols = [c for c in df.columns if c != "ProductName" and pd.api.types.is_numeric_dtype(df[c])]
    if not val_cols:
        return StreamingResponse(io.BytesIO(), media_type="image/png")

    ycol = val_cols[0]
    # ✅ Select top 10 by metric value
    df = df.sort_values(ycol, ascending=False).head(10)

    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(df, x="ProductName", y=ycol, ax=ax, color="seagreen")
    ax.set_title("Top 10 Products")
    ax.tick_params(axis="x", rotation=40)
    return render_chart(fig)


@app.get("/chart/countries")
def chart_countries():
    p = MODEL / "sales_by_country.parquet"
    if not p.exists():
        return StreamingResponse(io.BytesIO(), media_type="image/png")

    df = pd.read_parquet(p)
    if "Country" not in df.columns:
        return StreamingResponse(io.BytesIO(), media_type="image/png")

    val_cols = [c for c in df.columns if c != "Country" and pd.api.types.is_numeric_dtype(df[c])]
    if not val_cols:
        return StreamingResponse(io.BytesIO(), media_type="image/png")

    ycol = val_cols[0]
    # ✅ Select top 10 by metric value
    df = df.sort_values(ycol, ascending=False).head(10)

    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(df, x="Country", y=ycol, ax=ax, color="orange")
    ax.set_title("Top 10 Countries")
    ax.tick_params(axis="x", rotation=20)
    return render_chart(fig)


# ─────────────────────────────────────────────────────────────
# Page routes
# ─────────────────────────────────────────────────────────────
@app.get("/")
def index():
    dq_runs = DQDIR / "dq_runs.parquet"
    dq_issues = DQDIR / "dq_issues.parquet"

    dq_block = []
    if dq_runs.exists():
        runs = pd.read_parquet(dq_runs).tail(10)
        dq_block.append(
                # after
                air.Div(
                    air.H3("DQ Runs (latest 10)"),
                    air.Table(
                        air.Thead(air.Tr(*[air.Th(c) for c in runs.columns])),
                        air.Tbody(*[
                            air.Tr(*[air.Td(str(row[c])) for c in runs.columns])
                            for _, row in runs.iterrows()
                        ])
                    ),
                    class_="card"
                )

        )

        if dq_issues.exists():
            issues = pd.read_parquet(dq_issues)
            if not issues.empty:
                last_ts = runs.sort_values("run_ts")["run_ts"].iloc[-1]
                latest = issues[issues["run_ts"] == last_ts]
                if latest.empty:
                    dq_block.append(air.P("All checks passed 🎉"))
                else:
                    dq_block.append(air.P(f"{len(latest)} issue(s) in latest run"))
    else:
        dq_block.append(air.P("No DQ runs yet."))

    return air.Html(
        air.Head(air.Title("Northwind Dashboard (Seaborn)")),
        air.Body(
            air.H1("🏪 Northwind — Py Dashboard (Seaborn)"),
            air.Section(
                air.H2("📊 Sales by Customer"),
                air.Img(src="/chart/customers"),
                air.H3("Top Products"),
                air.Img(src="/chart/products"),
            ),
            air.Section(
                air.H2("🌍 Sales by Country"),
                air.Img(src="/chart/countries"),
            ),
            air.Section(
                air.H2("🧪 Data Quality"),
                *dq_block
            ),
        )
    )
