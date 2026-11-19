"""Job configuration: parameter parsing and fully qualified table names."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

REGIONS: tuple[str, ...] = ("north", "central", "south")
DATASETS: tuple[str, ...] = ("customers", "orders", "sales")
MAX_RECORD_SCALE = 50

# Defaults used only when a notebook is run interactively (outside a job).
DEFAULT_PARAMETERS: dict[str, str] = {
    "catalog": "workspace",
    "raw_schema": "retail_dyn_raw",
    "bronze_schema": "retail_dyn_bronze",
    "silver_schema": "retail_dyn_silver",
    "gold_schema": "retail_dyn_gold",
    "audit_schema": "retail_dyn_audit",
    "volume_path": "/Volumes/workspace/retail_dyn_raw/source_files",
    "record_scale": "1",
    "inject_duplicates": "false",
    "seed": "42",
    "run_id": "interactive",
}

# Table names per layer.
TABLES: dict[str, dict[str, str]] = {
    "bronze": {
        "customers": "customers",
        "orders": "orders",
        "sales": "sales",
    },
    "silver": {
        "customer_orders": "customer_orders",
        "region_order_metrics": "region_order_metrics",
        "customer_sales": "customer_sales",
        "clean_sales": "clean_sales",
    },
    "gold": {
        "region_metrics": "region_metrics",
        "customer_metrics": "customer_metrics",
        "category_revenue": "category_revenue",
        "latest_audit_status": "latest_audit_status",
    },
    "audit": {
        "task_audit_log": "task_audit_log",
        "workflow_run_log": "workflow_run_log",
    },
}

_TRUE = {"true", "1", "yes", "y", "on"}
_FALSE = {"false", "0", "no", "n", "off", ""}


def parse_bool(value: Any) -> bool:
    """Parse a job parameter string such as "true"/"false" into a bool."""
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    raise ValueError(f"Cannot interpret {value!r} as a boolean")


def parse_scale(value: Any) -> int:
    """Parse record_scale and keep it inside a Free Edition friendly range."""
    scale = int(str(value).strip())
    if not 1 <= scale <= MAX_RECORD_SCALE:
        raise ValueError(f"record_scale must be between 1 and {MAX_RECORD_SCALE}, got {scale}")
    return scale


def validate_region(region: str) -> str:
    """Normalise and validate a region name coming from the For Each input."""
    normalised = str(region).strip().lower()
    if normalised not in REGIONS:
        raise ValueError(f"Unknown region {region!r}; expected one of {REGIONS}")
    return normalised


@dataclass(frozen=True)
class JobConfig:
    catalog: str
    raw_schema: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    audit_schema: str
    volume_path: str
    record_scale: int = 1
    inject_duplicates: bool = False
    seed: int = 42
    run_id: str = "interactive"

    @classmethod
    def from_params(cls, params: Mapping[str, Any]) -> JobConfig:
        merged = {**DEFAULT_PARAMETERS, **{k: v for k, v in params.items() if v is not None}}
        return cls(
            catalog=merged["catalog"],
            raw_schema=merged["raw_schema"],
            bronze_schema=merged["bronze_schema"],
            silver_schema=merged["silver_schema"],
            gold_schema=merged["gold_schema"],
            audit_schema=merged["audit_schema"],
            volume_path=str(merged["volume_path"]).rstrip("/"),
            record_scale=parse_scale(merged["record_scale"]),
            inject_duplicates=parse_bool(merged["inject_duplicates"]),
            seed=int(merged["seed"]),
            run_id=str(merged["run_id"]),
        )

    def schema(self, layer: str) -> str:
        try:
            return getattr(self, f"{layer}_schema")
        except AttributeError as exc:
            raise ValueError(f"Unknown layer {layer!r}") from exc

    def table(self, layer: str, name: str) -> str:
        """Return ``catalog.schema.table`` for a logical table name."""
        physical = TABLES[layer][name]
        return f"{self.catalog}.{self.schema(layer)}.{physical}"

    def source_dir(self, dataset: str) -> str:
        if dataset not in DATASETS:
            raise ValueError(f"Unknown dataset {dataset!r}")
        return f"{self.volume_path}/{dataset}"


def load_job_config(dbutils: Any, extra: Mapping[str, str] | None = None) -> JobConfig:
    """Declare widgets (for interactive runs) and build a JobConfig from them.

    In a job run the widgets are populated from job parameters.
    """
    values: dict[str, str] = {}
    for name, default in {**DEFAULT_PARAMETERS, **(extra or {})}.items():
        dbutils.widgets.text(name, default)
        values[name] = dbutils.widgets.get(name)
    return JobConfig.from_params(values)


def get_widget(dbutils: Any, name: str, default: str = "") -> str:
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name)
