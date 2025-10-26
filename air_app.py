# ─────────────────────────────────────────────────────────────
# app.py — Air Dashboard (Seaborn, final)
# ─────────────────────────────────────────────────────────────
import io
from pathlib import Path
from functools import lru_cache

import air
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from fastapi import Response
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
# Utilities
# ─────────────────────────────────────────────────────────────

def _cache_key_for(path: Path) -> tuple[str, float]:
    """Return a stable cache key that changes when the file changes."""
    try:
        stat = path.stat()
        return (str(path.resolve()), stat.st_mtime)
    except FileNotFoundError:
        return (str(path.resolve()), -1.0)

@lru_cache(maxsize=32)
def _load_top10_by_cached(path_key: tuple[str, float], label_col: str):
    """Cache layer keyed by (path, mtime)."""
    path = Path(path_key[0])
    if not path.exists():
        return None, None
    df = pd.read_parquet(path)
    if label_col not in df.columns:
        return None, None
    val_cols = [
        c for c in df.columns
        if c != label_col and pd.api.types.is_numeric_dtype(df[c])
    ]
    if not val_cols:
        return None, None
    ycol = val_cols[0]
    df = df.sort_values(ycol, ascending=False).head(10)
    return ycol, df


def load_top10_by(path: Path, label_col: str):
    """Public loader that uses the cached function with a file-mtime key."""
    key = _cache_key_for(path)
    return _load_top10_by_cached(key, label_col)


def render_chart(fig) -> StreamingResponse:
    """Render a Matplotlib figure to a PNG StreamingResponse."""
    buf = io.BytesIO()
    try:
        fig.savefig(buf, format="png", bbox_inches="tight")
        buf.seek(0)
        return StreamingResponse(
            buf,
            media_type="image/png",
            headers={"Cache-Control": "no-store"},
        )
    finally:
        plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Chart endpoints (return PNG)
# ─────────────────────────────────────────────────────────────
@app.get("/chart/customers")
def chart_customers():
    p = MODEL / "sales_by_customer.parquet"
    ycol, df = load_top10_by(p, "CompanyName")
    if df is None:
        return Response(status_code=204)

    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(data=df, x="CompanyName", y=ycol, ax=ax)
    ax.set_title("Top 10 Customers")
    ax.set_xlabel("Customer")
    ax.set_ylabel(ycol)
    ax.tick_params(axis="x", rotation=40)
    fig.tight_layout()
    return render_chart(fig)


@app.get("/chart/products")
def chart_products():
    p = MODEL / "sales_by_product.parquet"
    ycol, df = load_top10_by(p, "ProductName")
    if df is None:
        return Response(status_code=204)

    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(data=df, x="ProductName", y=ycol, ax=ax)
    ax.set_title("Top 10 Products")
    ax.set_xlabel("Product")
    ax.set_ylabel(ycol)
    ax.tick_params(axis="x", rotation=40)
    fig.tight_layout()
    return render_chart(fig)


@app.get("/chart/countries")
def chart_countries():
    p = MODEL / "sales_by_country.parquet"
    ycol, df = load_top10_by(p, "Country")
    if df is None:
        return Response(status_code=204)

    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(data=df, x="Country", y=ycol, ax=ax)
    ax.set_title("Top 10 Countries")
    ax.set_xlabel("Country")
    ax.set_ylabel(ycol)
    ax.tick_params(axis="x", rotation=20)
    fig.tight_layout()
    return render_chart(fig)


# ─────────────────────────────────────────────────────────────
# Page route (Air layout + images + DQ summary)
# ─────────────────────────────────────────────────────────────
@app.get("/")
def index():
    dq_runs = DQDIR / "dq_runs.parquet"
    dq_issues = DQDIR / "dq_issues.parquet"
# ─────────────────────────────────────────────────────────────
# app.py — Air Dashboard (Seaborn, final)
# ─────────────────────────────────────────────────────────────
import io
from pathlib import Path
from functools import lru_cache

import air
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from fastapi import Response
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
# Utilities
# ─────────────────────────────────────────────────────────────

def _cache_key_for(path: Path) -> tuple[str, float]:
    """Return a stable cache key that changes when the file changes."""
    try:
        stat = path.stat()
        return (str(path.resolve()), stat.st_mtime)
    except FileNotFoundError:
        return (str(path.resolve()), -1.0)

@lru_cache(maxsize=32)
def _load_top10_by_cached(path_key: tuple[str, float], label_col: str):
    """Cache layer keyed by (path, mtime)."""
    path = Path(path_key[0])
    if not path.exists():
        return None, None
    df = pd.read_parquet(path)
    if label_col not in df.columns:
        return None, None
    val_cols = [
        c for c in df.columns
        if c != label_col and pd.api.types.is_numeric_dtype(df[c])
    ]
    if not val_cols:
        return None, None
    ycol = val_cols[0]
    df = df.sort_values(ycol, ascending=False).head(10)
    return ycol, df


def load_top10_by(path: Path, label_col: str):
    """Public loader that uses the cached function with a file-mtime key."""
    key = _cache_key_for(path)
    return _load_top10_by_cached(key, label_col)


def render_chart(fig) -> StreamingResponse:
    """Render a Matplotlib figure to a PNG StreamingResponse."""
    buf = io.BytesIO()
    try:
        fig.savefig(buf, format="png", bbox_inches="tight")
        buf.seek(0)
        return StreamingResponse(
            buf,
            media_type="image/png",
            headers={"Cache-Control": "no-store"},
        )
    finally:
        plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Chart endpoints (return PNG)
# ─────────────────────────────────────────────────────────────
@app.get("/chart/customers")
def chart_customers():
    p = MODEL / "sales_by_customer.parquet"
    ycol, df = load_top10_by(p, "CompanyName")
    if df is None:
        return Response(status_code=204)

    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(data=df, x="CompanyName", y=ycol, ax=ax)
    ax.set_title("Top 10 Customers")
    ax.set_xlabel("Customer")
    ax.set_ylabel(ycol)
    ax.tick_params(axis="x", rotation=40)
    fig.tight_layout()
    return render_chart(fig)


@app.get("/chart/products")
def chart_products():
    p = MODEL / "sales_by_product.parquet"
    ycol, df = load_top10_by(p, "ProductName")
    if df is None:
        return Response(status_code=204)

    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(data=df, x="ProductName", y=ycol, ax=ax)
    ax.set_title("Top 10 Products")
    ax.set_xlabel("Product")
    ax.set_ylabel(ycol)
    ax.tick_params(axis="x", rotation=40)
    fig.tight_layout()
    return render_chart(fig)


@app.get("/chart/countries")
def chart_countries():
    p = MODEL / "sales_by_country.parquet"
    ycol, df = load_top10_by(p, "Country")
    if df is None:
        return Response(status_code=204)

    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(data=df, x="Country", y=ycol, ax=ax)
    ax.set_title("Top 10 Countries")
    ax.set_xlabel("Country")
    ax.set_ylabel(ycol)
    ax.tick_params(axis="x", rotation=20)
    fig.tight_layout()
    return render_chart(fig)


# ─────────────────────────────────────────────────────────────
# Page route (Air layout + images + DQ summary)
# ─────────────────────────────────────────────────────────────
@app.get("/")
def index():
    dq_runs = DQDIR / "dq_runs.parquet"
    dq_issues = DQDIR / "dq_issues.parquet"

    dq_block = []
    if dq_runs.exists():
        runs = pd.read_parquet(dq_runs).tail(10)
        dq_table = air.Table(
            air.Thead(air.Tr(*[air.Th(str(c)) for c in runs.columns])),
            air.Tbody(
                *[
                    air.Tr(*[air.Td(str(row[c])) for c in runs.columns])
                    for _, row in runs.iterrows()
                ]
            ),
        )
        dq_block.append(
            air.Div(
                air.H3("DQ Runs (latest 10)"),
                air.Div(dq_table, style="overflow:auto; max-width:100%")
            )
        )

        if dq_issues.exists():
            issues = pd.read_parquet(dq_issues)
            if not issues.empty and "run_ts" in runs.columns and "run_ts" in issues.columns:
                last_ts = runs.sort_values("run_ts")["run_ts"].iloc[-1]
                latest = issues[issues["run_ts"] == last_ts]
                if latest.empty:
                    dq_block.append(air.P("All checks passed 🎉"))
                else:
                    dq_block.append(air.P(f"{len(latest)} issue(s) in latest run"))
            else:
                dq_block.append(air.P("No issues logged for the latest run."))
    else:
        dq_block.append(air.P("No DQ runs yet."))

    return air.layouts.picocss(
        air.Title("Northwind Dashboard (Seaborn)"),
        air.H1("🏪 Northwind — Py Dashboard (Seaborn)"),
        air.Section(
            air.H2("📊 Sales by Customer"),
            air.Img(src="/chart/customers", alt="Top 10 Customers by sales"),
            air.H3("Top Products"),
            air.Img(src="/chart/products", alt="Top 10 Products by sales"),
        ),
        air.Section(
            air.H2("🌍 Sales by Country"),
            air.Img(src="/chart/countries", alt="Top 10 Countries by sales"),
        ),
        air.Section(
            air.H2("🧪 Data Quality"),
            *dq_block,
        ),
    )

    dq_block = []
    if dq_runs.exists():
        runs = pd.read_parquet(dq_runs).tail(10)
        dq_table = air.Table(
            air.Thead(air.Tr(*[air.Th(str(c)) for c in runs.columns])),
            air.Tbody(
                *[
                    air.Tr(*[air.Td(str(row[c])) for c in runs.columns])
                    for _, row in runs.iterrows()
                ]
            ),
        )
        dq_block.append(
            air.Div(
                air.H3("DQ Runs (latest 10)"),
                air.Div(dq_table, style="overflow:auto; max-width:100%")
            )
        )

        if dq_issues.exists():
            issues = pd.read_parquet(dq_issues)
            if not issues.empty and "run_ts" in runs.columns and "run_ts" in issues.columns:
                last_ts = runs.sort_values("run_ts")["run_ts"].iloc[-1]
                latest = issues[issues["run_ts"] == last_ts]
                if latest.empty:
                    dq_block.append(air.P("All checks passed 🎉"))
                else:
                    dq_block.append(air.P(f"{len(latest)} issue(s) in latest run"))
            else:
                dq_block.append(air.P("No issues logged for the latest run."))
    else:
        dq_block.append(air.P("No DQ runs yet."))

    return air.layouts.picocss(
        air.Title("Northwind Dashboard (Seaborn)"),
        air.H1("🏪 Northwind — Py Dashboard (Seaborn)"),
        air.Section(
            air.H2("📊 Sales by Customer"),
            air.Img(src="/chart/customers", alt="Top 10 Customers by sales"),
            air.H3("Top Products"),
            air.Img(src="/chart/products", alt="Top 10 Products by sales"),
        ),
        air.Section(
            air.H2("🌍 Sales by Country"),
            air.Img(src="/chart/countries", alt="Top 10 Countries by sales"),
        ),
        air.Section(
            air.H2("🧪 Data Quality"),
            *dq_block,
        ),
    )
