"""Spark transformations and Delta writers for the workflow."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from typing import TypeVar

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from retail_dynamic.config import REGIONS, validate_region

T = TypeVar("T")

# ----------------------------------------------------------------------------- schemas
CUSTOMERS_DDL = (
    "customer_id STRING, customer_name STRING, email STRING, region STRING, "
    "segment STRING, created_at TIMESTAMP"
)
ORDERS_DDL = (
    "order_id STRING, customer_id STRING, order_ts TIMESTAMP, status STRING, "
    "channel STRING, order_total DOUBLE"
)
SALES_DDL = (
    "sale_id STRING, order_id STRING, customer_id STRING, product_id STRING, "
    "category STRING, quantity INT, unit_price DOUBLE, discount DOUBLE, "
    "net_amount DOUBLE, updated_at TIMESTAMP"
)
REGION_ORDER_METRICS_DDL = (
    "region STRING, order_count BIGINT, customer_count BIGINT, revenue DOUBLE, "
    "avg_order_value DOUBLE, first_order_ts TIMESTAMP, last_order_ts TIMESTAMP, "
    "run_id STRING, processed_at TIMESTAMP"
)

# Output contract shared by BOTH If/Else branches (deduplicate_sales / use_original_sales).
CLEAN_SALES_COLUMNS: list[tuple[str, str]] = [
    ("sale_id", "string"),
    ("order_id", "string"),
    ("customer_id", "string"),
    ("customer_name", "string"),
    ("region", "string"),
    ("segment", "string"),
    ("product_id", "string"),
    ("category", "string"),
    ("quantity", "int"),
    ("unit_price", "double"),
    ("discount", "double"),
    ("net_amount", "double"),
    ("updated_at", "timestamp"),
    ("processing_branch", "string"),
    ("run_id", "string"),
]


# ----------------------------------------------------------------------------- bronze
def read_customers(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", "true").schema(CUSTOMERS_DDL).csv(path)


def read_orders(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(ORDERS_DDL).json(path)


def read_sales(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", "true").schema(SALES_DDL).csv(path)


def add_ingestion_metadata(df: DataFrame, run_id: str) -> DataFrame:
    """Add lineage columns. Uses _metadata (input_file_name is unsupported on UC/serverless)."""
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
    )


# ----------------------------------------------------------------------------- revenue
def net_amount_col(
    quantity: str = "quantity", unit_price: str = "unit_price", discount: str = "discount"
) -> Column:
    """net_amount = quantity * unit_price * (1 - discount), rounded to cents."""
    return F.round(F.col(quantity) * F.col(unit_price) * (F.lit(1) - F.col(discount)), 2)


def with_net_amount(df: DataFrame) -> DataFrame:
    """Recompute net_amount from its components so the business rule is authoritative."""
    return df.withColumn("net_amount", net_amount_col())


# ----------------------------------------------------------------------------- region branch
def join_customers_orders(customers: DataFrame, orders: DataFrame, run_id: str) -> DataFrame:
    dims = customers.select("customer_id", "customer_name", "region", "segment")
    return orders.join(dims, "customer_id", "inner").select(
        "order_id",
        "customer_id",
        "customer_name",
        "region",
        "segment",
        "order_ts",
        "status",
        "channel",
        "order_total",
        F.lit(run_id).alias("run_id"),
    )


def filter_region(df: DataFrame, region: str) -> DataFrame:
    return df.filter(F.col("region") == F.lit(validate_region(region)))


def summarize_region(customer_orders: DataFrame, region: str, run_id: str) -> DataFrame:
    """One metrics row for a region. An empty region still yields a zero row."""
    region = validate_region(region)
    subset = filter_region(customer_orders, region)
    agg = subset.agg(
        F.count("order_id").alias("order_count"),
        F.countDistinct("customer_id").alias("customer_count"),
        F.coalesce(F.round(F.sum("order_total"), 2), F.lit(0.0)).alias("revenue"),
        F.min("order_ts").alias("first_order_ts"),
        F.max("order_ts").alias("last_order_ts"),
    )
    return agg.select(
        F.lit(region).alias("region"),
        F.col("order_count").cast("bigint"),
        F.col("customer_count").cast("bigint"),
        F.col("revenue").cast("double"),
        F.when(F.col("order_count") > 0, F.round(F.col("revenue") / F.col("order_count"), 2))
        .otherwise(F.lit(0.0))
        .alias("avg_order_value"),
        "first_order_ts",
        "last_order_ts",
        F.lit(run_id).alias("run_id"),
        F.current_timestamp().alias("processed_at"),
    )


def build_region_metrics(
    spark: SparkSession, region_order_metrics: DataFrame, regions: Iterable[str] = REGIONS
) -> DataFrame:
    """Gold region metrics; every expected region is present (zeros if missing)."""
    expected = spark.createDataFrame([(r,) for r in regions], "region STRING")
    joined = expected.join(region_order_metrics, "region", "left")
    return joined.select(
        "region",
        F.coalesce("order_count", F.lit(0)).cast("bigint").alias("order_count"),
        F.coalesce("customer_count", F.lit(0)).cast("bigint").alias("customer_count"),
        F.coalesce("revenue", F.lit(0.0)).cast("double").alias("revenue"),
        F.coalesce("avg_order_value", F.lit(0.0)).cast("double").alias("avg_order_value"),
        "last_order_ts",
        F.current_timestamp().alias("published_at"),
    ).orderBy("region")


# ----------------------------------------------------------------------------- sales branch
def join_customer_sales(customers: DataFrame, sales: DataFrame) -> DataFrame:
    dims = customers.select("customer_id", "customer_name", "region", "segment")
    keep = [c for c in sales.columns if c not in {"customer_name", "region", "segment"}]
    return with_net_amount(sales.select(*keep).join(dims, "customer_id", "inner"))


def deduplicate_latest(
    df: DataFrame, key: str = "sale_id", order_by: tuple[str, ...] = ("updated_at",)
) -> DataFrame:
    """Keep the most recent row per key (latest updated_at; ties broken by _ingested_at)."""
    order_cols = [F.col(c).desc_nulls_last() for c in order_by]
    if "_ingested_at" in df.columns:
        order_cols.append(F.col("_ingested_at").desc_nulls_last())
    window = Window.partitionBy(key).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


def to_clean_sales(df: DataFrame, branch: str, run_id: str) -> DataFrame:
    """Project onto CLEAN_SALES_COLUMNS so both If/Else branches share one schema."""
    extra = {"processing_branch": F.lit(branch), "run_id": F.lit(run_id)}
    return df.select(
        *[
            (extra[name] if name in extra else F.col(name)).cast(dtype).alias(name)
            for name, dtype in CLEAN_SALES_COLUMNS
        ]
    )


def deduplicate_sales(df: DataFrame, run_id: str) -> DataFrame:
    return to_clean_sales(deduplicate_latest(df), "deduplicated", run_id)


def use_original_sales(df: DataFrame, run_id: str) -> DataFrame:
    return to_clean_sales(df, "original", run_id)


def build_customer_metrics(clean_sales: DataFrame) -> DataFrame:
    metrics = clean_sales.groupBy("customer_id", "customer_name", "region", "segment").agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").cast("bigint").alias("units"),
        F.round(F.sum("net_amount"), 2).alias("revenue"),
        F.max("updated_at").alias("last_purchase_at"),
    )
    rank_window = Window.orderBy(F.col("revenue").desc(), F.col("customer_id"))
    return metrics.withColumn("revenue_rank", F.row_number().over(rank_window)).withColumn(
        "published_at", F.current_timestamp()
    )


def build_category_revenue(clean_sales: DataFrame) -> DataFrame:
    return clean_sales.groupBy("category").agg(
        F.round(F.sum("net_amount"), 2).alias("revenue"),
        F.sum("quantity").cast("bigint").alias("units"),
        F.countDistinct("order_id").alias("order_count"),
        F.count("*").alias("line_count"),
    ).withColumn("published_at", F.current_timestamp())


# ----------------------------------------------------------------------------- writers
def with_conflict_retry(action: Callable[[], T], attempts: int = 4, base_sleep: float = 2.0) -> T:
    """Retry Delta concurrent-modification conflicts (parallel tasks writing one table)."""
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as exc:  # noqa: BLE001 - Delta raises several conflict classes
            is_conflict = "Concurrent" in type(exc).__name__ or "DELTA_CONCURRENT" in str(exc)
            if not is_conflict or attempt == attempts:
                raise
            time.sleep(base_sleep * attempt)
    raise RuntimeError("unreachable")


def overwrite_table(df: DataFrame, table: str) -> None:
    """Full refresh: reruns replace the table instead of appending duplicates."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )


def ensure_region_metrics_table(spark: SparkSession, table: str) -> None:
    """Create the For Each target up-front, partitioned by region, before iterations start."""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({REGION_ORDER_METRICS_DDL}) "
        "USING DELTA PARTITIONED BY (region)"
    )


def replace_region(df: DataFrame, table: str, region: str) -> None:
    """Overwrite only one region's partition so parallel iterations never clobber each other."""
    region = validate_region(region)
    with_conflict_retry(
        lambda: df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"region = '{region}'")
        .saveAsTable(table)
    )
