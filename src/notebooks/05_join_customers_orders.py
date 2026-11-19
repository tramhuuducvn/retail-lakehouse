# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Join customers + orders
# MAGIC bronze.customers + bronze.orders -> silver.customer_orders.

# COMMAND ----------
from databricks.sdk.runtime import dbutils, spark
import os
import sys

# Make src/retail_dynamic importable (notebook CWD is src/notebooks in the bundle).
sys.path.insert(0, os.path.abspath(".."))

from retail_dynamic.config import load_job_config

cfg = load_job_config(dbutils)
print(cfg)

# COMMAND ----------

from retail_dynamic import quality
from retail_dynamic import transforms as T

customers = spark.table(cfg.table("bronze", "customers"))
orders = spark.table(cfg.table("bronze", "orders"))

orphans = quality.count_orphans(orders, customers, "customer_id")
print(f"orders without customer: {orphans}")

T.overwrite_table(
    T.join_customers_orders(customers, orders, cfg.run_id), cfg.table("silver", "customer_orders")
)
