"""Deterministic synthetic retail data (pure Python, no Spark required).

* customers -> CSV, orders -> JSON Lines, sales -> CSV
* orders.customer_id exists in customers
* sales.order_id exists in orders and sales.customer_id equals the order's customer
* net_amount = quantity * unit_price * (1 - discount)
* optional 2-5 % duplicate sales (same sale_id, later updated_at)
"""

from __future__ import annotations

import csv
import json
import random
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from retail_dynamic.config import REGIONS

BASE_CUSTOMERS = 200
BASE_ORDERS = 800
MIN_DUPLICATE_RATE = 0.02
MAX_DUPLICATE_RATE = 0.05
BASE_DATE = datetime(2026, 1, 1, 8, 0, 0)
TS_FORMAT = "%Y-%m-%dT%H:%M:%S"

CUSTOMER_COLUMNS = ["customer_id", "customer_name", "email", "region", "segment", "created_at"]
ORDER_COLUMNS = ["order_id", "customer_id", "order_ts", "status", "channel", "order_total"]
SALES_COLUMNS = [
    "sale_id",
    "order_id",
    "customer_id",
    "product_id",
    "category",
    "quantity",
    "unit_price",
    "discount",
    "net_amount",
    "updated_at",
]

FILE_NAMES = {
    "customers": "customers.csv",
    "orders": "orders.json",
    "sales": "sales.csv",
}

_FIRST_NAMES = [
    "An",
    "Binh",
    "Chi",
    "Dung",
    "Giang",
    "Hai",
    "Khanh",
    "Linh",
    "Minh",
    "Nam",
    "Phuong",
    "Quan",
    "Thao",
    "Trung",
    "Uyen",
    "Vy",
]
_LAST_NAMES = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Vo", "Dang", "Bui", "Do"]
_SEGMENTS = ["consumer", "corporate", "small_business"]
_STATUSES = ["completed", "completed", "completed", "shipped", "processing"]
_CHANNELS = ["web", "mobile", "store"]
_DISCOUNTS = [0.0, 0.0, 0.05, 0.1, 0.15, 0.2]

# product_id, category, list price
PRODUCTS: list[tuple[str, str, float]] = [
    ("P001", "electronics", 249.0),
    ("P002", "electronics", 89.5),
    ("P003", "electronics", 15.99),
    ("P004", "home", 45.0),
    ("P005", "home", 120.0),
    ("P006", "fashion", 35.0),
    ("P007", "fashion", 59.9),
    ("P008", "grocery", 4.5),
    ("P009", "grocery", 12.25),
    ("P010", "sports", 75.0),
]


def net_amount(quantity: int, unit_price: float, discount: float) -> float:
    """Business rule: quantity * unit_price * (1 - discount), rounded to cents."""
    return round(quantity * unit_price * (1 - discount), 2)


def _ts(value: datetime) -> str:
    return value.strftime(TS_FORMAT)


@dataclass
class SourceData:
    customers: list[dict] = field(default_factory=list)
    orders: list[dict] = field(default_factory=list)
    sales: list[dict] = field(default_factory=list)

    @property
    def duplicate_count(self) -> int:
        return len(self.sales) - len({row["sale_id"] for row in self.sales})


def _generate_customers(rng: random.Random, count: int) -> list[dict]:
    customers = []
    for i in range(1, count + 1):
        first, last = rng.choice(_FIRST_NAMES), rng.choice(_LAST_NAMES)
        # Round-robin region keeps every region populated; segment is random.
        customers.append(
            {
                "customer_id": f"C{i:05d}",
                "customer_name": f"{last} {first}",
                "email": f"{first.lower()}.{last.lower()}{i}@example.com",
                "region": REGIONS[(i - 1) % len(REGIONS)],
                "segment": rng.choice(_SEGMENTS),
                "created_at": _ts(BASE_DATE - timedelta(days=rng.randint(30, 720))),
            }
        )
    return customers


def _generate_orders_and_sales(
    rng: random.Random, customers: list[dict], count: int
) -> tuple[list[dict], list[dict]]:
    orders, sales = [], []
    for i in range(1, count + 1):
        customer = rng.choice(customers)
        order_id = f"O{i:07d}"
        order_ts = BASE_DATE + timedelta(minutes=rng.randint(0, 90 * 24 * 60))
        lines = []
        for line_no in range(1, rng.randint(1, 4) + 1):
            product_id, category, price = rng.choice(PRODUCTS)
            quantity = rng.randint(1, 5)
            discount = rng.choice(_DISCOUNTS)
            lines.append(
                {
                    "sale_id": f"{order_id}-{line_no:02d}",
                    "order_id": order_id,
                    "customer_id": customer["customer_id"],
                    "product_id": product_id,
                    "category": category,
                    "quantity": quantity,
                    "unit_price": price,
                    "discount": discount,
                    "net_amount": net_amount(quantity, price, discount),
                    "updated_at": _ts(order_ts + timedelta(minutes=rng.randint(1, 30))),
                }
            )
        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "order_ts": _ts(order_ts),
                "status": rng.choice(_STATUSES),
                "channel": rng.choice(_CHANNELS),
                "order_total": round(sum(line["net_amount"] for line in lines), 2),
            }
        )
        sales.extend(lines)
    return orders, sales


def _inject_duplicates(rng: random.Random, sales: list[dict]) -> list[dict]:
    """Re-send 2-5 % of sales rows with the same values and a later updated_at."""
    if not sales:
        return []
    rate = rng.uniform(MIN_DUPLICATE_RATE, MAX_DUPLICATE_RATE)
    count = max(1, round(len(sales) * rate))
    duplicates = []
    for idx in sorted(rng.sample(range(len(sales)), count)):
        dup = dict(sales[idx])
        original_ts = datetime.strptime(dup["updated_at"], TS_FORMAT)
        dup["updated_at"] = _ts(original_ts + timedelta(hours=rng.randint(1, 48)))
        duplicates.append(dup)
    return duplicates


def generate_source_data(
    record_scale: int = 1, inject_duplicates: bool = False, seed: int = 42
) -> SourceData:
    """Generate customers, orders and sales deterministically for a given seed.

    The base data does not depend on ``inject_duplicates``: duplicates use a separate
    random stream, so the true/false runs differ only by the duplicate rows.
    """
    if record_scale < 1:
        raise ValueError("record_scale must be >= 1")
    rng = random.Random(seed)
    customers = _generate_customers(rng, BASE_CUSTOMERS * record_scale)
    orders, sales = _generate_orders_and_sales(rng, customers, BASE_ORDERS * record_scale)
    if inject_duplicates:
        sales = sales + _inject_duplicates(random.Random(seed * 7919 + 1), sales)
    return SourceData(customers=customers, orders=orders, sales=sales)


def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _write_json_lines(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps({c: row[c] for c in columns}) + "\n")


def write_source_files(data: SourceData, base_path: str) -> dict[str, str]:
    """Write the three datasets into ``<base_path>/<dataset>/``.

    Each dataset directory is deleted and recreated first, so reruns overwrite
    previous files instead of accumulating them. Works for local paths and for
    Unity Catalog Volume paths (``/Volumes/...``) on serverless compute.
    """
    root = Path(base_path)
    written: dict[str, str] = {}
    targets = {
        "customers": (CUSTOMER_COLUMNS, data.customers, _write_csv),
        "orders": (ORDER_COLUMNS, data.orders, _write_json_lines),
        "sales": (SALES_COLUMNS, data.sales, _write_csv),
    }
    for dataset, (columns, rows, writer) in targets.items():
        directory = root / dataset
        if directory.exists():
            shutil.rmtree(directory)
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / FILE_NAMES[dataset]
        writer(file_path, columns, rows)
        written[dataset] = str(file_path)
    return written
