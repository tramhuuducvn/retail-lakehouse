# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Deduplicate sales (If branch: true)
# MAGIC Keeps the latest record per sale_id -> silver.clean_sales.

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

from retail_dynamic import transforms as T
from retail_dynamic.task_values import set_task_value

clean = T.deduplicate_sales(spark.table(cfg.table("silver", "customer_sales")), cfg.run_id)
T.overwrite_table(clean, cfg.table("silver", "clean_sales"))
set_task_value(dbutils, "clean_row_count", spark.table(cfg.table("silver", "clean_sales")).count())
