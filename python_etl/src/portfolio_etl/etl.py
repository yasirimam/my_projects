import logging
import sqlite3
from pathlib import Path
import pandas as pd
import yaml

def load_config():
    cfg_path = Path(__file__).parent / "config.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logger(log_path):
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(filename=log_path, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def run_pipeline():
    cfg = load_config()
    setup_logger(cfg["log_path"])
    logging.info("Pipeline started")

    base = Path(__file__).resolve().parents[2]
    raw_dir = base / "data" / "raw"
    db_path = base / "data" / "warehouse.sqlite"

    customers = pd.read_csv(raw_dir / "customers_scd2.csv", parse_dates=["valid_from", "valid_to"])
    orders = pd.read_csv(raw_dir / "orders.csv", parse_dates=["order_date"])
    products = pd.read_csv(raw_dir / "products.csv")
    lines = pd.read_csv(raw_dir / "order_lines.csv")

    dim_customers = customers[customers["is_current"] == 1].copy()

    fact = (lines
            .merge(orders, on="order_id")
            .merge(products, on="product_id")
            .merge(dim_customers, on="customer_id"))

    fact["extended_amount"] = fact["quantity"] * fact["unit_price"]
    fact["order_day"] = fact["order_date"].dt.date

    con = sqlite3.connect(db_path)
    dim_customers.to_sql("dim_customers", con, if_exists="replace", index=False)
    products.to_sql("dim_products", con, if_exists="replace", index=False)
    orders.to_sql("stg_orders", con, if_exists="replace", index=False)
    fact.to_sql("fact_order_lines", con, if_exists="replace", index=False)
    con.close()

    logging.info("Pipeline finished")
