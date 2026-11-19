# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Join customers + sales
# MAGIC bronze.customers + bronze.sales -> silver.customer_sales (net_amount recomputed; duplicates
# MAGIC still present).

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
sales = spark.table(cfg.table("bronze", "sales"))

orphans = quality.count_orphans(sales, customers, "customer_id")
print(f"sales without customer: {orphans}")

T.overwrite_table(T.join_customer_sales(customers, sales), cfg.table("silver", "customer_sales"))
