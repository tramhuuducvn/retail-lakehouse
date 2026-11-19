# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Ingest orders
# MAGIC JSON Lines -> bronze.orders (full refresh).

# COMMAND ----------

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

df = T.add_ingestion_metadata(T.read_orders(spark, cfg.source_dir("orders")), cfg.run_id)
T.overwrite_table(df, cfg.table("bronze", "orders"))
set_task_value(dbutils, "row_count", spark.table(cfg.table("bronze", "orders")).count())
