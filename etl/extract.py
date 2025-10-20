# etl/extract.py (patched robust reader)
from __future__ import annotations
from pathlib import Path
import io, csv
import urllib.request
import pandas as pd

DATA = Path("data")
RAW = DATA / "00-raw"
RAW.mkdir(parents=True, exist_ok=True)

BASE = "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data"
REMOTE_MAP = {
    "customers.csv":       "Customers.csv",
    "orders.csv":          "Orders.csv",
    "order-details.csv":   "Order_Details.csv",
    "products.csv":        "Products.csv",
}
REQUIRED_LOCAL = ["Customers.csv", "Orders.csv", "Order_Details.csv"]  # Products optional

def _download_raw_files(target: Path, use_products: bool = True) -> None:
    target.mkdir(parents=True, exist_ok=True)
    for remote, local in REMOTE_MAP.items():
        if not use_products and local == "Products.csv":
            continue
        out = target / local
        if out.exists():
            continue
        url = f"{BASE}/{remote}"
        print(f"[extract] Downloading {url} -> {out}")
        urllib.request.urlretrieve(url, out)
    print(f"[extract] Raw CSVs ready in {target}")

def _assert_required_present(target: Path, use_products: bool) -> list[str]:
    missing: list[str] = []
    for name in REQUIRED_LOCAL:
        if not (target / name).exists():
            missing.append(name)
    if use_products and not (target / "Products.csv").exists():
        # optional; we won’t fail on this
        pass
    return missing

def _read_csv_auto(path: Path) -> pd.DataFrame:
    """
    Robust CSV reader:
      1) try default pandas parser (engine='c')
      2) sniff delimiter + retry with engine='python' (no low_memory)
      3) last resort: on_bad_lines='skip'
    """
    # First attempt: fast C engine
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except pd.errors.ParserError:
        pass  # fall through

    # Sniff delimiter
    import csv
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        sample = fh.read(4096)
    try:
        sniff = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        delim = sniff.delimiter
    except Exception:
        delim = ","

    # Second attempt: python engine (NOTE: no low_memory with python engine)
    try:
        return pd.read_csv(
            path,
            engine="python",
            sep=delim,
            quotechar='"',
            escapechar="\\",
            encoding="utf-8-sig",
        )
    except pd.errors.ParserError:
        # Final attempt: tolerate malformed lines
        return pd.read_csv(
            path,
            engine="python",
            sep=delim,
            quotechar='"',
            escapechar="\\",
            encoding="utf-8-sig",
            on_bad_lines="skip",
        )


def extract(use_products: bool = True, auto_download: bool = True) -> dict[str, pd.DataFrame | None]:
    if auto_download:
        _download_raw_files(RAW, use_products=use_products)

    still_missing = _assert_required_present(RAW, use_products=use_products)
    hard_missing = [m for m in still_missing if m != "Products.csv"]
    if hard_missing:
        raise FileNotFoundError(
            "Missing required raw CSVs in data/00-raw/: " + ", ".join(hard_missing)
        )

    customers = _read_csv_auto(RAW / "Customers.csv")
    orders    = _read_csv_auto(RAW / "Orders.csv")
    od        = _read_csv_auto(RAW / "Order_Details.csv")

    products = None
    prod_path = RAW / "Products.csv"
    if use_products and prod_path.exists():
        products = _read_csv_auto(prod_path)

    return {"customers": customers, "orders": orders, "od": od, "products": products}
