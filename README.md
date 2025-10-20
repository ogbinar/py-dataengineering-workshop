# 🧠 Py-DataEngineering Workshop

**From CSV to Dashboard — Building a Mini Data Pipeline in Pure Python**

This workshop demonstrates how to build a full **data engineering workflow** using only open-source tools:  
`pandas • pyarrow • uv • streamlit`.

You’ll ingest, clean, model, and visualize the classic **Northwind** dataset — all locally, no cloud or database required.

---

## 📦 Project Structure

```

py-dataengineering-workshop/
├─ README.md
├─ pyproject.toml
├─ .gitignore
├─ data/
│  ├─ 00-raw/         # raw source CSVs (downloaded automatically)
│  ├─ 01-clean/       # cleaned + validated Parquet data
│  │  └─ _dq/         # data quality logs
│  ├─ 02-model/       # modeled & aggregated Parquet tables
│  └─ 03-sandbox/     # optional scratch area
├─ etl/
│  ├─ extract.py      # downloads + reads raw CSVs
│  ├─ load.py         # cleans data + runs DQ checks
│  ├─ transform.py    # creates modeled tables (fact/dim)
│  ├─ build.py        # aggregates & writes gold layer
│  ├─ dq.py           # reusable DQ rules + logging
│  ├─ paths.py        # centralized folder definitions
│  └─ run.py          # stage orchestrator (CLI)
└─ app.py             # Streamlit dashboard

````

---

## 🧰 Requirements

- Python ≥ 3.10  
- [uv](https://github.com/astral-sh/uv) (modern fast package/environment manager)  
- Internet connection (for first-time Northwind CSV download)

---

## ⚙️ Setup

```bash
# 1. Clone this repo
git clone https://github.com/YOURNAME/py-dataengineering-workshop.git
cd py-dataengineering-workshop

# 2. Create environment + install deps
uv venv && uv sync

# 3. (Optional) fetch data manually
uv run python -m etl.extract

# 4. Run the entire pipeline
uv run python -m etl.run
````

On first run, the `extract` stage will automatically download:

```
Customers.csv, Orders.csv, Order_Details.csv, Products.csv
```

and store them in `data/00-raw/`.

---

## 🧩 Pipeline Overview

| Stage         | Script             | Purpose                                                       |
| ------------- | ------------------ | ------------------------------------------------------------- |
| **Extract**   | `etl/extract.py`   | Download + load raw CSVs into memory                          |
| **Load**      | `etl/load.py`      | Clean data, standardize columns, and log data-quality issues  |
| **Transform** | `etl/transform.py` | Create fact/dimension tables                                  |
| **Build**     | `etl/build.py`     | Aggregate to gold layer (sales by customer, country, product) |

Run any stage individually:

```bash
uv run python -m etl.run --stage extract
uv run python -m etl.run --stage load
uv run python -m etl.run --stage transform
uv run python -m etl.run --stage build
```

---

## 🧪 Data Quality

All validation results are stored in:

```
data/01-clean/_dq/
├─ dq_runs.parquet    # summary of each run
└─ dq_issues.parquet  # detailed issue list
```

Rules include:

* Non-null & positive IDs
* Valid price, quantity, discount ranges
* Foreign-key consistency (Orders ↔ Customers)

---

## 📊 Streamlit Dashboard

After running the pipeline:

```bash
uv run streamlit run app.py
```

Then open [http://localhost:8501](http://localhost:8501).

**Tabs:**

1. 📊 **Sales (Customers)** – Top customers & products
2. 🌍 **Sales by Country** – Aggregated view by region
3. 🧪 **Data Quality** – Latest DQ run and issue details

---

## 🧱 Teaching Flow

This repo is structured for live code-along sessions:

1. **Inspect raw CSVs** → `extract.py`
2. **Clean & validate** → `load.py`
3. **Model & aggregate** → `transform.py` + `build.py`
4. **Visualize** → `app.py` (Streamlit)

Each layer is self-contained, readable, and directly runnable.

---

## 💡 Extensions

* Replace Northwind with your own dataset
* Add new DQ rules in `dq.py`
* Create extra dashboards (e.g., by category or year)
* Swap pandas → Polars or DuckDB to compare performance

---

## 🧾 License

MIT © 2025 Myk Ogbinar / Data Engineering Pilipinas

---

## 🙌 Acknowledgments

* [Neo4j Northwind Dataset](https://github.com/neo4j-contrib/northwind-neo4j)
* [pandas](https://pandas.pydata.org/)
* [Streamlit](https://streamlit.io/)
* [DurianPy](https://durianpy.org/)
* [PyCon Davao 2025](https://pycon-davao.durianpy.org/)
* [Data Engineering Pilipinas](https://dataengineering.ph/)


