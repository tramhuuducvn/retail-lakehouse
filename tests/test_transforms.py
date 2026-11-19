"""Spark transformations (local Spark). Skipped automatically if pyspark is missing."""

from __future__ import annotations

from datetime import datetime

import pytest

from retail_dynamic import quality
from retail_dynamic.generators import generate_source_data, write_source_files

SALES_ROWS_DDL = (
    "sale_id STRING, order_id STRING, customer_id STRING, customer_name STRING, region STRING, "
    "segment STRING, product_id STRING, category STRING, quantity INT, unit_price DOUBLE, "
    "discount DOUBLE, net_amount DOUBLE, updated_at TIMESTAMP"
)


def _sale(sale_id, quantity, updated_at, region="north", price=10.0, discount=0.0, category="home"):
    return (sale_id, sale_id.split("-")[0], "C1", "Nguyen An", region, "consumer", "P1", category,
            quantity, price, discount, round(quantity * price * (1 - discount), 2), updated_at)


@pytest.fixture(scope="module")
def bronze(spark, T, tmp_path_factory):
    root = tmp_path_factory.mktemp("volume")
    data = generate_source_data(record_scale=1, inject_duplicates=True, seed=42)
    write_source_files(data, str(root))
    customers = T.add_ingestion_metadata(T.read_customers(spark, str(root / "customers")), "r1")
    orders = T.add_ingestion_metadata(T.read_orders(spark, str(root / "orders")), "r1")
    sales = T.add_ingestion_metadata(T.read_sales(spark, str(root / "sales")), "r1")
    return data, customers.cache(), orders.cache(), sales.cache()


def test_bronze_schema_and_counts(bronze):
    data, customers, orders, sales = bronze
    assert customers.count() == len(data.customers)
    assert orders.count() == len(data.orders)
    assert sales.count() == len(data.sales)
    assert dict(sales.dtypes)["quantity"] == "int"
    assert dict(orders.dtypes)["order_ts"] == "timestamp"
    assert {"_source_file", "_ingested_at", "_run_id"} <= set(sales.columns)
    assert sales.filter("updated_at IS NULL").count() == 0


def test_referential_integrity_in_spark(bronze):
    _, customers, orders, sales = bronze
    assert quality.count_orphans(orders, customers, "customer_id") == 0
    assert quality.count_orphans(sales, orders, "order_id") == 0
    mismatched = sales.join(orders.select("order_id", orders.customer_id.alias("oc")), "order_id")
    assert mismatched.filter("customer_id <> oc").count() == 0


def test_duplicate_count_matches_injection(bronze):
    data, _, _, sales = bronze
    assert quality.count_duplicates(sales, "sale_id") == data.duplicate_count > 0


def test_deduplication_keeps_latest_record(spark, T):
    df = spark.createDataFrame(
        [
            _sale("O1-01", 1, datetime(2026, 1, 1, 8)),
            _sale("O1-01", 5, datetime(2026, 1, 2, 8)),  # latest version
            _sale("O1-01", 3, datetime(2026, 1, 1, 12)),
            _sale("O2-01", 2, datetime(2026, 1, 1, 9)),
        ],
        SALES_ROWS_DDL,
    )
    result = {r["sale_id"]: r for r in T.deduplicate_sales(df, "r1").collect()}
    assert len(result) == 2
    assert result["O1-01"]["quantity"] == 5
    assert result["O1-01"]["processing_branch"] == "deduplicated"


def test_revenue_calculation(spark, T):
    df = spark.createDataFrame(
        [(2, 100.0, 0.1), (3, 9.99, 0.0), (1, 50.0, 0.2)], "quantity INT, unit_price DOUBLE, discount DOUBLE"
    )
    values = [r["net_amount"] for r in T.with_net_amount(df).collect()]
    assert values == [180.0, 29.97, 40.0]


def test_region_revenue_reconciles_with_category_revenue(spark, T, bronze):
    _, customers, orders, sales = bronze
    customer_orders = T.join_customers_orders(customers, orders, "r1")
    region_rows = T.summarize_region(customer_orders, "north", "r1")
    for region in ("central", "south"):
        region_rows = region_rows.unionByName(T.summarize_region(customer_orders, region, "r1"))
    region_total = sum(r["revenue"] for r in T.build_region_metrics(spark, region_rows).collect())

    clean = T.deduplicate_sales(T.join_customer_sales(customers, sales), "r1")
    category_total = sum(r["revenue"] for r in T.build_category_revenue(clean).collect())
    assert region_total == pytest.approx(category_total, abs=1.0)


def test_region_filtering_including_empty_region(spark, T):
    orders = spark.createDataFrame(
        [
            ("O1", "C1", "A", "north", "consumer", datetime(2026, 1, 1), "completed", "web", 100.0, "r1"),
            ("O2", "C2", "B", "north", "consumer", datetime(2026, 1, 2), "completed", "web", 50.0, "r1"),
            ("O3", "C3", "C", "central", "corporate", datetime(2026, 1, 3), "shipped", "store", 25.0, "r1"),
        ],
        "order_id STRING, customer_id STRING, customer_name STRING, region STRING, segment STRING, "
        "order_ts TIMESTAMP, status STRING, channel STRING, order_total DOUBLE, run_id STRING",
    )
    assert T.filter_region(orders, "north").count() == 2
    assert T.filter_region(orders, "south").count() == 0

    south = T.summarize_region(orders, "south", "r1").collect()
    assert len(south) == 1
    assert (south[0]["order_count"], south[0]["revenue"], south[0]["avg_order_value"]) == (0, 0.0, 0.0)

    north = T.summarize_region(orders, "north", "r1").first()
    assert (north["order_count"], north["customer_count"], north["revenue"]) == (2, 2, 150.0)

    # Gold still lists every region even if an iteration produced no row.
    partial = T.summarize_region(orders, "north", "r1")
    gold = {r["region"]: r for r in T.build_region_metrics(spark, partial).collect()}
    assert set(gold) == {"north", "central", "south"}
    assert gold["south"]["revenue"] == 0.0

    with pytest.raises(ValueError):
        T.filter_region(orders, "west")


def test_if_else_branches_have_identical_schema(spark, T):
    from pyspark.sql import functions as F

    df = spark.createDataFrame(
        [_sale("O1-01", 1, datetime(2026, 1, 1)), _sale("O1-01", 2, datetime(2026, 1, 2))],
        SALES_ROWS_DDL,
    ).withColumn("_ingested_at", F.current_timestamp())
    true_branch = T.deduplicate_sales(df, "r1")
    false_branch = T.use_original_sales(df, "r1")
    assert true_branch.schema == false_branch.schema
    assert [f.name for f in true_branch.schema] == [name for name, _ in T.CLEAN_SALES_COLUMNS]
    assert true_branch.count() == 1 and false_branch.count() == 2


def test_customer_metrics_rank(spark, T):
    df = spark.createDataFrame(
        [_sale("O1-01", 1, datetime(2026, 1, 1)), _sale("O2-01", 9, datetime(2026, 1, 2))],
        SALES_ROWS_DDL,
    )
    clean = T.use_original_sales(df, "r1")
    rows = T.build_customer_metrics(clean).collect()
    assert len(rows) == 1 and rows[0]["revenue"] == 100.0 and rows[0]["revenue_rank"] == 1
