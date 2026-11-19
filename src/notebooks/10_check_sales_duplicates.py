# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Check sales duplicates
# MAGIC Publishes the `duplicate_count` task value used by the `has_sales_duplicates` If/Else condition.

# COMMAND ----------

import os
import sys

# Make src/retail_dynamic importable (notebook CWD is src/notebooks in the bundle).
sys.path.insert(0, os.path.abspath(".."))

from retail_dynamic.config import load_job_config

cfg = load_job_config(dbutils)
print(cfg)

# COMMAND ----------

from retail_dynamic import quality
from retail_dynamic.task_values import DUPLICATE_COUNT_KEY, set_task_value

df = spark.table(cfg.table("silver", "customer_sales"))
total_rows = df.count()
duplicate_count = quality.count_duplicates(df, "sale_id")

quality.write_audit_records(
    spark,
    cfg.table("audit", "task_audit_log"),
    [quality.duplicate_record(cfg.run_id, duplicate_count, total_rows)],
)
set_task_value(dbutils, DUPLICATE_COUNT_KEY, int(duplicate_count))
print(f"duplicate_count={duplicate_count} of {total_rows}")
