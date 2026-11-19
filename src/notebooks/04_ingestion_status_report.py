# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Ingestion status report
# MAGIC Runs if AT_LEAST_ONE ingestion succeeded. Records per-dataset status for this run_id.

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

from pyspark.sql import functions as F

from retail_dynamic import quality
from retail_dynamic.config import DATASETS


def rows_for_this_run(dataset):
    try:
        df = spark.table(cfg.table("bronze", dataset)).filter(F.col("_run_id") == cfg.run_id)
        count = df.count()
    except Exception as exc:  # table missing because the ingest task failed
        print(f"{dataset}: {exc}")
        return None
    return count or None


counts = {dataset: rows_for_this_run(dataset) for dataset in DATASETS}
records = quality.ingestion_records(cfg.run_id, counts)
quality.write_audit_records(spark, cfg.table("audit", "task_audit_log"), records)
for record in records:
    print(record)
