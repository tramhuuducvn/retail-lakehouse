"""Data quality checks and the audit log.

Pure-Python helpers (status logic) are separated from Spark helpers so the
rules can be unit tested without a cluster.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass

SUCCESS = "SUCCESS"
WARNING = "WARNING"
FAILED = "FAILED"
MISSING = "MISSING"

AUDIT_LOG_DDL = (
    "run_id STRING, check_name STRING, status STRING, metric_value DOUBLE, "
    "detail STRING, recorded_at TIMESTAMP"
)
RUN_LOG_DDL = (
    "run_id STRING, overall_status STRING, inject_duplicates BOOLEAN, record_scale INT, "
    "duplicate_count BIGINT, region_revenue DOUBLE, category_revenue DOUBLE, "
    "failed_checks STRING, recorded_at TIMESTAMP"
)

# Checks the summary expects to find for a fully successful run.
EXPECTED_CHECKS: tuple[str, ...] = (
    "ingest_customers",
    "ingest_orders",
    "ingest_sales",
    "sales_duplicates",
    "region_metrics",
    "customer_metrics",
    "revenue_reconciliation",
)


@dataclass(frozen=True)
class AuditRecord:
    run_id: str
    check_name: str
    status: str
    metric_value: float | None = None
    detail: str = ""

    def as_row(self) -> dict:
        return asdict(self)


# ----------------------------------------------------------------------------- pure logic
def ingestion_records(run_id: str, counts: Mapping[str, int | None]) -> list[AuditRecord]:
    """counts: dataset -> rows ingested for THIS run (None if the table is missing/stale)."""
    records = []
    for dataset, count in counts.items():
        if count is None:
            records.append(
                AuditRecord(
                    run_id,
                    f"ingest_{dataset}",
                    MISSING,
                    None,
                    "no rows for this run_id (ingestion failed or skipped)",
                )
            )
        elif count == 0:
            records.append(AuditRecord(run_id, f"ingest_{dataset}", FAILED, 0.0, "empty table"))
        else:
            records.append(AuditRecord(run_id, f"ingest_{dataset}", SUCCESS, float(count), f"{count} rows"))
    return records


def duplicate_record(run_id: str, duplicate_count: int, total_rows: int) -> AuditRecord:
    status = WARNING if duplicate_count > 0 else SUCCESS
    branch = "deduplicate_sales" if duplicate_count > 0 else "use_original_sales"
    return AuditRecord(
        run_id,
        "sales_duplicates",
        status,
        float(duplicate_count),
        f"{duplicate_count} duplicate rows of {total_rows}; branch={branch}",
    )


def reconcile_revenue(
    region_revenue: float | None, category_revenue: float | None, tolerance: float = 1.0
) -> tuple[str, str]:
    """Region revenue (orders) must match category revenue (clean sales)."""
    if region_revenue is None or category_revenue is None:
        return MISSING, "metrics table not available"
    diff = abs(region_revenue - category_revenue)
    status = SUCCESS if diff <= tolerance else FAILED
    return status, f"region={region_revenue:.2f} category={category_revenue:.2f} diff={diff:.2f}"


def overall_status(
    latest: Mapping[str, str], expected: Iterable[str] = EXPECTED_CHECKS
) -> tuple[str, list[str]]:
    """Return (overall_status, failed_checks). WARNING counts as healthy."""
    failed = [name for name in expected if latest.get(name) not in (SUCCESS, WARNING)]
    return (SUCCESS if not failed else FAILED), failed


# ----------------------------------------------------------------------------- spark helpers
def count_duplicates(df, key: str = "sale_id") -> int:
    """Rows beyond the first occurrence of each key."""
    return df.count() - df.select(key).distinct().count()


def count_orphans(child, parent, key: str) -> int:
    """Child rows whose key has no match in parent."""
    return child.join(parent.select(key).distinct(), key, "left_anti").count()


def ensure_audit_objects(spark, audit_log: str, run_log: str, latest_view: str) -> None:
    """Create audit tables and the gold view the dashboard reads."""
    props = "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {audit_log} ({AUDIT_LOG_DDL}) USING DELTA {props}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {run_log} ({RUN_LOG_DDL}) USING DELTA {props}")
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {latest_view} AS
        SELECT run_id, check_name, status, metric_value, detail, recorded_at
        FROM {audit_log}
        QUALIFY row_number() OVER (PARTITION BY check_name ORDER BY recorded_at DESC) = 1
        """
    )


def write_audit_records(spark, audit_log: str, records: Iterable[AuditRecord]) -> None:
    """MERGE on (run_id, check_name): reruns/repairs update rows instead of duplicating them."""
    from pyspark.sql import functions as F

    from retail_dynamic.transforms import with_conflict_retry

    rows = [r.as_row() for r in records]
    if not rows:
        return
    source = spark.createDataFrame(
        rows, "run_id STRING, check_name STRING, status STRING, metric_value DOUBLE, detail STRING"
    ).withColumn("recorded_at", F.current_timestamp())
    view = f"_audit_src_{uuid.uuid4().hex}"
    source.createOrReplaceTempView(view)
    names = ", ".join(f"'{r['check_name']}'" for r in rows)
    with_conflict_retry(
        lambda: spark.sql(
            f"""
            MERGE INTO {audit_log} t
            USING {view} s
            ON t.run_id = s.run_id AND t.check_name = s.check_name
               AND t.check_name IN ({names})
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    )
