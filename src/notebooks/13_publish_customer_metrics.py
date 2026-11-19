# Databricks notebook source
# MAGIC %md
# MAGIC # 13 · Publish customer metrics
# MAGIC run_if AT_LEAST_ONE_SUCCESS (only one If/Else branch runs). -> gold.customer_metrics,
# MAGIC gold.category_revenue.

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

from pyspark.sql import functions as F

from retail_dynamic import quality
from retail_dynamic import transforms as T

clean = spark.table(cfg.table("silver", "clean_sales"))
customer_metrics = T.build_customer_metrics(clean)
category_revenue = T.build_category_revenue(clean)
T.overwrite_table(customer_metrics, cfg.table("gold", "customer_metrics"))
T.overwrite_table(category_revenue, cfg.table("gold", "category_revenue"))

branch = clean.select("processing_branch").distinct().first()
total = category_revenue.agg(F.sum("revenue")).first()[0] or 0.0
record = quality.AuditRecord(
    cfg.run_id,
    "customer_metrics",
    quality.SUCCESS,
    float(total),
    f"{customer_metrics.count()} customers; branch={branch[0] if branch else 'n/a'}",
)
quality.write_audit_records(spark, cfg.table("audit", "task_audit_log"), [record])
display(category_revenue)
