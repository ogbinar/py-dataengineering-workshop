# 💻 Py-DataEngineering Workshop — Hands-On Guide

> **Objective:**
> Build a working end-to-end **data engineering pipeline** using pure Python.
> You’ll extract, clean, transform, model, and visualize data — mirroring how real data engineers work.

---

## 🗺️ Session Overview

| Stage         | Folder             | Core Concept                | Output                             |
| ------------- | ------------------ | --------------------------- | ---------------------------------- |
| 1️⃣ Extract   | `etl/extract.py`   | Getting data from source    | `data/00-raw/*.csv`                |
| 2️⃣ Load      | `etl/load.py`      | Cleaning + validating       | `data/01-clean/*.parquet`          |
| 3️⃣ Transform | `etl/transform.py` | Modeling (fact & dimension) | `data/02-model/fact_sales.parquet` |
| 4️⃣ Build     | `etl/build.py`     | Aggregation + gold layer    | `data/02-model/sales_by_*.parquet` |
| 5️⃣ Visualize | `app.py`           | Dashboard + DQ view         | Streamlit web app                  |

---

## ⚙️ Step 0 — Setup Recap

If you haven’t already:

```bash
git clone https://github.com/YOURNAME/py-dataengineering-workshop.git
cd py-dataengineering-workshop
uv venv && uv sync
```

Create the folder structure automatically by running the base pipeline once:

```bash
uv run python -m etl.run --stage extract
```

---

## 1️⃣ EXTRACT — Getting the Data

📄 File: `etl/extract.py`

### 🧠 Concept

Extraction is the **first step** of every data pipeline:

* Fetches raw files (CSV, JSON, etc.)
* Validates they exist
* Stores them in a **raw layer** (`data/00-raw/`)

### 🔍 Learnings

* **File I/O** using `urllib.request`
* **Idempotency:** avoids re-downloading if file already exists
* **Standardized directory layout** (Bronze / Raw Layer)

### ▶️ Try It

```bash
uv run python -m etl.run --stage extract
```

Expected output:

```
[EXTRACT] Downloading .../customers.csv → data/00-raw/Customers.csv
[EXTRACT] Loaded: customers, orders, od, products
```

Check the raw data:

```bash
ls data/00-raw
```

Each file corresponds to a Northwind table.

---

## 2️⃣ LOAD — Cleaning and Validating

📄 File: `etl/load.py`
📄 Linked helper: `etl/dq.py`

### 🧠 Concept

**Data loading** is about preparing data for reliable downstream use:

* Renames columns to consistent schema
* Validates required fields
* Converts data types
* Logs **Data Quality (DQ)** checks

### ⚙️ DQ Checks Include

* Null or invalid `OrderID`
* Negative `Quantity` or `UnitPrice`
* `Discount` out of range (0–1)
* Missing foreign keys (`CustomerID` mismatch)

### ▶️ Try It

```bash
uv run python -m etl.run --stage load
```

Output:

```
[LOAD] Written clean parquet + DQ. Issues: []
```

### 🔍 Inspect Results

```bash
ls data/01-clean
```

Look for:

* `customers.parquet`
* `orders.parquet`
* `order_details.parquet`
* `_dq/dq_runs.parquet`
* `_dq/dq_issues.parquet`

### 🧩 Discussion

* Why use **Parquet** instead of CSV? (columnar, compressed)
* Why have **data quality logs**?
* What does a “clean” dataset look like?

---

## 3️⃣ TRANSFORM — Modeling the Data

📄 File: `etl/transform.py`

### 🧠 Concept

Transformation is where we **combine and enrich** data:

* Joins cleaned tables
* Creates **facts** (sales) and **dimensions** (customers)
* Adds calculated columns

### 🧩 Common Modeling Patterns

| Model Type              | Example                             | Purpose                               |
| ----------------------- | ----------------------------------- | ------------------------------------- |
| **Fact Table**          | `fact_sales`                        | transactional metrics                 |
| **Dimension Table**     | `dim_customer`                      | attributes like company name, country |
| **OBT (One Big Table)** | `fact_sales` joined with dim tables | simpler analytics                     |

### ▶️ Try It

```bash
uv run python -m etl.run --stage transform
```

This runs:

* Load → Clean data
* Transform → Join + Compute `line_amount`

Check new outputs in `data/02-model/`:

```
dim_customer.parquet
fact_sales.parquet
```

### 💬 Discussion

* What’s the role of a **Customer dimension**?
* Why compute `line_amount = UnitPrice * Quantity * (1 - Discount)`?
* How does this mirror “star schema” modeling?

---

## 4️⃣ BUILD — Aggregating and Serving the Gold Layer

📄 File: `etl/build.py`

### 🧠 Concept

The **Build** step aggregates your fact table into **analytical outputs**, often called **gold tables** or **data marts**.

This mimics real business metrics:

* Total sales per customer
* Total sales per country
* Total sales per product

### ▶️ Try It

```bash
uv run python -m etl.run --stage build
```

### 🧾 Expected Outputs

```bash
ls data/02-model
```

You’ll now have:

* `sales_by_customer.parquet`
* `sales_by_country.parquet`
* `sales_by_product.parquet`

### 💬 Discussion

* Why aggregate instead of querying raw data directly?
* How does this improve performance and reuse?

---

## 5️⃣ VISUALIZE — Streamlit Dashboard

📄 File: `app.py`

### 🧠 Concept

Visualization completes the loop: **data → insight.**

The dashboard reads the modeled outputs (`/02-model`) and DQ logs (`/01-clean/_dq`).

Tabs:

1. **Sales (Customers)** – top customers & products
2. **Sales by Country** – regional view
3. **Data Quality** – validation history

### ▶️ Run It

```bash
uv run streamlit run app.py
```

Then open [http://localhost:8501](http://localhost:8501).

### 💬 Discussion

* Why visualize locally before BI deployment?
* How does this reflect a real “serving layer”?
* How would you scale this (Metabase, Superset, etc.)?

---

## 🧠 Extra Learning Topics (Optional Live Demos)

### 🧱 1. Inspecting Parquet Files

```bash
uv run python view_data.py
```

Shows:

* Head of data
* Schema and datatypes
* Summary stats

💡 *Compare a Parquet vs CSV read time.*

---

### 🧪 2. Adding a New DQ Rule

Edit `dq.py`:

```python
if (od["Quantity"] > 1000).any():
    issues.append("order_details: unusually high quantity (>1000)")
```

Re-run:

```bash
uv run python -m etl.run --stage load
```

Inspect `dq_issues.parquet` for new entries.

---

### 🧩 3. Extend the Pipeline

Ideas for experimentation:

* Add `Employees.csv` and create `dim_employee`
* Aggregate `sales_by_year`
* Add a time-based chart to Streamlit
* Export results as CSV in `/03-sandbox/`

---

## ⚙️ Common Pitfalls and Tips

| Mistake                          | Why it Happens                      | Fix                                     |
| -------------------------------- | ----------------------------------- | --------------------------------------- |
| Forgetting to re-run after edits | Cached data                         | Delete `/data/01-clean` and `/02-model` |
| DQ logs overwritten              | `write_dq_logs()` replaces old file | append or version logs                  |
| Missing dependencies             | env mismatch                        | use `uv sync`                           |
| “Streamlit not found”            | ran outside venv                    | `uv run streamlit run app.py`           |
| File not found                   | missing extract step                | run `--stage extract` first             |

---

## 🚀 Beyond the Workshop

### What You Just Built

✅ Local ETL pipeline
✅ Cleaned + modeled data
✅ Reproducible structure
✅ Basic governance (DQ + logs)
✅ Working dashboard

### Next Steps

| Goal                   | Tool / Concept        |
| ---------------------- | --------------------- |
| Add orchestration      | Prefect or Airflow    |
| Move data to a DB      | DuckDB or Postgres    |
| Incremental transforms | dbt                   |
| Add monitoring         | Grafana, Slack alerts |
| Query across layers    | Ibis                  |
| Serve features to ML   | Feature stores        |

---

## 🏁 Wrap-Up

**Key Takeaways**

* Data engineering = building trust in data.
* Good DE practice = *clean, validated, modular pipelines.*
* Even pure Python can mirror modern data stacks.
* You’ve built a complete mini stack: **CSV → Parquet → Dashboard.**

> 💬 “You don’t need big data to learn data engineering — you just need structure, consistency, and curiosity.”

---

**End of Workshop — Happy Engineering! 🧩**
