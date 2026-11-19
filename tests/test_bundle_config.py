"""Static checks of the bundle YAML and dashboard (no workspace access needed)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[1]
VALID_RUN_IF = {
    "ALL_SUCCESS",
    "AT_LEAST_ONE_SUCCESS",
    "NONE_FAILED",
    "ALL_DONE",
    "AT_LEAST_ONE_FAILED",
    "ALL_FAILED",
}


def _load(path: str) -> dict:
    return yaml.safe_load((ROOT / path).read_text())


@pytest.fixture(scope="module")
def job():
    return _load("resources/dynamic_retail_job.yml")["resources"]["jobs"]["dynamic_retail_job"]


@pytest.fixture(scope="module")
def tasks(job):
    return {t["task_key"]: t for t in job["tasks"]}


def deps(task):
    return {d["task_key"] for d in task.get("depends_on", [])}


def test_bundle_targets_and_variables():
    bundle = _load("databricks.yml")
    assert bundle["bundle"]["name"] == "retail-dynamic-workflow"
    assert {"catalog", "schema_prefix", "warehouse_id"} <= set(bundle["variables"])
    assert bundle["targets"]["dev"]["default"] is True
    assert set(bundle["targets"]) == {"dev", "prod"}


def test_job_settings(job):
    assert job["name"] == "retail-dynamic-workflow-job"
    assert job["max_concurrent_runs"] == 1
    assert job["schedule"] == {
        "quartz_cron_expression": "0 0/30 * * * ?",
        "timezone_id": "Asia/Ho_Chi_Minh",
        "pause_status": "${var.schedule_pause_status}",
    }
    assert _load("databricks.yml")["variables"]["schedule_pause_status"]["default"] == "PAUSED"
    assert "job_clusters" not in job
    params = {p["name"] for p in job["parameters"]}
    assert {"record_scale", "inject_duplicates"} <= params


def test_serverless_only(tasks):
    for task in tasks.values():
        for key in ("new_cluster", "existing_cluster_id", "job_cluster_key"):
            assert key not in task
            assert key not in task.get("for_each_task", {}).get("task", {})


def test_dependencies_reference_existing_tasks(tasks):
    for task in tasks.values():
        assert deps(task) <= set(tasks), task["task_key"]
        assert task.get("run_if", "ALL_SUCCESS") in VALID_RUN_IF


def test_dag_shape(tasks):
    for name in ("ingest_customers", "ingest_orders", "ingest_sales"):
        assert deps(tasks[name]) == {"generate_source_files"}
    assert deps(tasks["join_customers_orders"]) == {"ingest_customers", "ingest_orders"}
    assert deps(tasks["join_customer_sales"]) == {"ingest_customers", "ingest_sales"}
    assert deps(tasks["publish_region_metrics"]) == {"for_each_region"}
    assert deps(tasks["refresh_dashboard"]) == {"publish_region_metrics", "publish_customer_metrics"}
    assert "refresh_dashboard" in deps(tasks["dynamic_workflow_summary"])


def test_run_if_semantics(tasks):
    assert tasks["ingestion_status_report"]["run_if"] == "AT_LEAST_ONE_SUCCESS"
    assert deps(tasks["ingestion_status_report"]) == {"ingest_customers", "ingest_orders", "ingest_sales"}
    assert tasks["publish_customer_metrics"]["run_if"] == "AT_LEAST_ONE_SUCCESS"
    assert tasks["refresh_dashboard"]["run_if"] == "AT_LEAST_ONE_SUCCESS"
    assert tasks["dynamic_workflow_summary"]["run_if"] == "ALL_DONE"


def test_if_else_condition(tasks):
    cond = tasks["has_sales_duplicates"]["condition_task"]
    assert cond["op"] == "GREATER_THAN" and cond["right"] == "0"
    assert cond["left"] == "{{tasks.check_sales_duplicates.values.duplicate_count}}"
    outcomes = {
        d.get("outcome") for t in ("deduplicate_sales", "use_original_sales") for d in tasks[t]["depends_on"]
    }
    assert outcomes == {"true", "false"}


def test_for_each(tasks):
    fe = tasks["for_each_region"]["for_each_task"]
    assert fe["inputs"] == "{{tasks.prepare_region_inputs.values.regions}}"
    assert fe["task"]["task_key"] == "process_region"
    assert fe["task"]["notebook_task"]["base_parameters"]["region"] == "{{input}}"
    assert "process_region" not in {d for t in tasks.values() for d in deps(t)}


def test_dashboard_task(tasks):
    dash = tasks["refresh_dashboard"]["dashboard_task"]
    assert dash["dashboard_id"] == "${resources.dashboards.retail_dashboard.id}"


def test_notebooks_exist_and_numbered(tasks):
    paths = []
    for task in tasks.values():
        inner = task.get("for_each_task", {}).get("task", task)
        if "notebook_task" in inner:
            paths.append(inner["notebook_task"]["notebook_path"])
    notebooks = sorted(p.name for p in (ROOT / "src/notebooks").glob("*.py"))
    assert [n[:2] for n in notebooks] == [f"{i:02d}" for i in range(15)]
    for path in paths:
        assert (ROOT / "resources" / path).resolve().exists(), path
    assert len(paths) == 15


def test_dashboard_definition():
    resources = _load("resources/retail_dashboard.yml")["resources"]
    assert set(resources["schemas"]) == {"raw", "bronze", "silver", "gold", "audit"}
    assert resources["volumes"]["source_files"]["volume_type"] == "MANAGED"
    dash = json.loads((ROOT / "src/dashboards/retail_dashboard.lvdash.json").read_text())
    sql = " ".join(" ".join(d["queryLines"]) for d in dash["datasets"])
    for table in ("region_metrics", "category_revenue", "customer_metrics", "latest_audit_status"):
        assert table in sql
    names = {d["name"] for d in dash["datasets"]}
    for item in dash["pages"][0]["layout"]:
        for query in item["widget"].get("queries", []):
            assert query["query"]["datasetName"] in names
