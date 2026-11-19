# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Process region (For Each iteration)
# MAGIC Nested task of `for_each_region`. Receives `region` = `{{input}}` and replaces only that
# MAGIC region's partition.

# COMMAND ----------
from databricks.sdk.runtime import dbutils, spark, display
import os
import sys

# Make src/retail_dynamic importable (notebook CWD is src/notebooks in the bundle).
sys.path.insert(0, os.path.abspath(".."))

from retail_dynamic.config import load_job_config

cfg = load_job_config(dbutils)
print(cfg)

# COMMAND ----------

from retail_dynamic import transforms as T
from retail_dynamic.config import get_widget, validate_region

region = validate_region(get_widget(dbutils, "region", "north"))

customer_orders = spark.table(cfg.table("silver", "customer_orders"))
metrics = T.summarize_region(customer_orders, region, cfg.run_id)
T.replace_region(metrics, cfg.table("silver", "region_order_metrics"), region)
display(metrics)
