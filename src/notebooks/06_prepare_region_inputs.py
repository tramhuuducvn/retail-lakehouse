# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Prepare region inputs
# MAGIC Creates the partitioned For Each target table and publishes the `regions` task value.

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
from retail_dynamic.task_values import (
    REGION_ORDER_COUNTS_KEY,
    REGIONS_KEY,
    build_region_inputs,
    set_task_value,
)

# Create the table BEFORE parallel iterations start (avoids concurrent CREATE conflicts).
T.ensure_region_metrics_table(spark, cfg.table("silver", "region_order_metrics"))

regions = build_region_inputs()  # all regions, including any that have no orders
counts = {
    row["region"]: row["count"]
    for row in spark.table(cfg.table("silver", "customer_orders")).groupBy("region").count().collect()
}
set_task_value(dbutils, REGION_ORDER_COUNTS_KEY, {r: int(counts.get(r, 0)) for r in regions})
set_task_value(dbutils, REGIONS_KEY, regions)
print(regions, counts)
