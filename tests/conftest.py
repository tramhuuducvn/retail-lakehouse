from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession. Spark tests are skipped if pyspark is not installed."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("retail-dynamic-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def T():  # noqa: N802 - mirrors the notebook import alias
    pytest.importorskip("pyspark")
    from retail_dynamic import transforms

    return transforms
