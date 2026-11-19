# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Generate source files
# MAGIC Deterministic synthetic customers (CSV), orders (JSON) and sales (CSV) in the UC Volume.
# MAGIC Overwrites previous files.

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
from retail_dynamic.generators import generate_source_data, write_source_files
from retail_dynamic.task_values import set_task_value

data = generate_source_data(
    record_scale=cfg.record_scale, inject_duplicates=cfg.inject_duplicates, seed=cfg.seed
)
written = write_source_files(data, cfg.volume_path)

# Shared audit objects are created once here, before parallel tasks write to them.
quality.ensure_audit_objects(
    spark,
    cfg.table("audit", "task_audit_log"),
    cfg.table("audit", "workflow_run_log"),
    cfg.table("gold", "latest_audit_status"),
)

counts = {
    "customers": len(data.customers),
    "orders": len(data.orders),
    "sales": len(data.sales),
    "injected_duplicates": data.duplicate_count,
}
print(written, counts)
set_task_value(dbutils, "generated_counts", counts)
