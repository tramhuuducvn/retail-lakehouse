# Databricks notebook source
# MAGIC %md
# MAGIC # 14 · Dynamic workflow summary (ALL_DONE)
# MAGIC Always runs. Reconciles revenue, writes the run audit row and fails the job if any expected
# MAGIC check is missing/failed.

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

audit_log = cfg.table("audit", "task_audit_log")


def table_total(layer, name):
    try:
        return spark.table(cfg.table(layer, name)).agg(F.sum("revenue")).first()[0]
    except Exception as exc:  # upstream task failed before creating the table
        print(f"{name}: {exc}")
        return None


region_revenue = table_total("gold", "region_metrics")
category_revenue = table_total("gold", "category_revenue")
status, detail = quality.reconcile_revenue(region_revenue, category_revenue)
quality.write_audit_records(
    spark, audit_log, [quality.AuditRecord(cfg.run_id, "revenue_reconciliation", status, None, detail)]
)

rows = spark.table(audit_log).filter(F.col("run_id") == cfg.run_id).collect()
latest = {row["check_name"]: row["status"] for row in rows}
dup_rows = [row["metric_value"] for row in rows if row["check_name"] == "sales_duplicates"]
overall, failed = quality.overall_status(latest)

run_row = spark.createDataFrame(
    [
        (
            cfg.run_id,
            overall,
            cfg.inject_duplicates,
            cfg.record_scale,
            int(dup_rows[0]) if dup_rows and dup_rows[0] is not None else None,
            region_revenue,
            category_revenue,
            ",".join(failed),
        )
    ],
    "run_id STRING, overall_status STRING, inject_duplicates BOOLEAN, record_scale INT, "
    "duplicate_count BIGINT, region_revenue DOUBLE, category_revenue DOUBLE, failed_checks STRING",
).withColumn("recorded_at", F.current_timestamp())
run_row.createOrReplaceTempView("_run_summary_src")
spark.sql(
    f"""
    MERGE INTO {cfg.table("audit", "workflow_run_log")} t
    USING _run_summary_src s ON t.run_id = s.run_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)
quality.write_audit_records(
    spark, audit_log, [quality.AuditRecord(cfg.run_id, "workflow_run", overall, None, ",".join(failed))]
)
display(run_row)

if overall != quality.SUCCESS:
    # This is the leaf task: failing here makes the job run show as failed.
    raise RuntimeError(f"Workflow run {cfg.run_id} finished with failed checks: {failed}")
