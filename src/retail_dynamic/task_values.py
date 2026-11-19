"""Thin wrappers around dbutils.jobs.taskValues (task values must be JSON-serialisable)."""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from retail_dynamic.config import REGIONS, validate_region

# Keys shared between YAML ({{tasks.<task>.values.<key>}}) and notebooks.
REGIONS_KEY = "regions"
DUPLICATE_COUNT_KEY = "duplicate_count"
REGION_ORDER_COUNTS_KEY = "region_order_counts"


def build_region_inputs(regions: Iterable[str] = REGIONS) -> list[str]:
    """Validated, de-duplicated region list used as the For Each input array."""
    result: list[str] = []
    for region in regions:
        normalised = validate_region(region)
        if normalised not in result:
            result.append(normalised)
    if not result:
        raise ValueError("For Each needs at least one region")
    return result


def set_task_value(dbutils: Any, key: str, value: Any) -> Any:
    """Publish a task value. Outside a job run this is a no-op in Databricks."""
    json.dumps(value)  # fail early on non-serialisable values
    dbutils.jobs.taskValues.set(key=key, value=value)
    return value


def get_task_value(dbutils: Any, task_key: str, key: str, default: Any, debug_value: Any) -> Any:
    """Read an upstream task value; debug_value is used when running interactively."""
    return dbutils.jobs.taskValues.get(taskKey=task_key, key=key, default=default, debugValue=debug_value)
