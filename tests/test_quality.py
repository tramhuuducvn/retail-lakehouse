"""Pure-Python quality/audit rules and task value helpers."""

from __future__ import annotations

import pytest

from retail_dynamic import quality
from retail_dynamic.config import JobConfig, parse_bool, parse_scale, validate_region
from retail_dynamic.task_values import build_region_inputs


@pytest.mark.parametrize("value,expected", [("true", True), ("FALSE", False), ("1", True), ("", False)])
def test_parse_bool(value, expected):
    assert parse_bool(value) is expected


def test_parse_bool_rejects_garbage():
    with pytest.raises(ValueError):
        parse_bool("maybe")


def test_parse_scale_bounds():
    assert parse_scale("3") == 3
    with pytest.raises(ValueError):
        parse_scale("0")


def test_validate_region():
    assert validate_region(" North ") == "north"
    with pytest.raises(ValueError):
        validate_region("west")


def test_region_inputs_cover_all_regions():
    assert build_region_inputs() == ["north", "central", "south"]
    assert build_region_inputs(["south", "south"]) == ["south"]


def test_job_config_table_names():
    cfg = JobConfig.from_params({"catalog": "workspace", "gold_schema": "dev_x_retail_dyn_gold",
                                 "inject_duplicates": "true", "record_scale": "2"})
    assert cfg.table("gold", "region_metrics") == "workspace.dev_x_retail_dyn_gold.region_metrics"
    assert cfg.inject_duplicates is True and cfg.record_scale == 2


def test_ingestion_records_statuses():
    records = quality.ingestion_records("r1", {"customers": 10, "orders": None, "sales": 0})
    statuses = {r.check_name: r.status for r in records}
    assert statuses == {"ingest_customers": "SUCCESS", "ingest_orders": "MISSING",
                        "ingest_sales": "FAILED"}


def test_duplicate_record_status():
    assert quality.duplicate_record("r", 3, 100).status == quality.WARNING
    assert quality.duplicate_record("r", 0, 100).status == quality.SUCCESS


def test_reconcile_revenue():
    assert quality.reconcile_revenue(100.0, 100.4)[0] == quality.SUCCESS
    assert quality.reconcile_revenue(100.0, 150.0)[0] == quality.FAILED
    assert quality.reconcile_revenue(None, 1.0)[0] == quality.MISSING


def test_overall_status():
    healthy = {name: quality.SUCCESS for name in quality.EXPECTED_CHECKS}
    healthy["sales_duplicates"] = quality.WARNING
    assert quality.overall_status(healthy) == (quality.SUCCESS, [])
    broken = dict(healthy)
    broken.pop("region_metrics")
    assert quality.overall_status(broken) == (quality.FAILED, ["region_metrics"])
