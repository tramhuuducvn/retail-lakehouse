# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Publish region metrics
# MAGIC Depends on the For Each container. silver.region_order_metrics -> gold.region_metrics.

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

source = spark.table(cfg.table("silver", "region_order_metrics"))
stale = source.filter(F.col("run_id") != cfg.run_id).count()
gold = T.build_region_metrics(spark, source)
T.overwrite_table(gold, cfg.table("gold", "region_metrics"))

total = gold.agg(F.sum("revenue")).first()[0] or 0.0
record = quality.AuditRecord(
    cfg.run_id,
    "region_metrics",
    quality.WARNING if stale else quality.SUCCESS,
    float(total),
    f"{gold.count()} regions, {stale} stale region rows",
)
quality.write_audit_records(spark, cfg.table("audit", "task_audit_log"), [record])
display(gold)
