"""Generation: schema, row counts, determinism, referential integrity, duplicates, rerun."""

from __future__ import annotations

import csv
import json

import pytest

from retail_dynamic.config import REGIONS
from retail_dynamic.generators import (
    BASE_CUSTOMERS,
    BASE_ORDERS,
    CUSTOMER_COLUMNS,
    ORDER_COLUMNS,
    SALES_COLUMNS,
    generate_source_data,
    net_amount,
    write_source_files,
)


@pytest.fixture(scope="module")
def clean():
    return generate_source_data(record_scale=1, inject_duplicates=False, seed=42)


@pytest.fixture(scope="module")
def dirty():
    return generate_source_data(record_scale=1, inject_duplicates=True, seed=42)


def test_schema_columns(clean):
    assert list(clean.customers[0]) == CUSTOMER_COLUMNS
    assert list(clean.orders[0]) == ORDER_COLUMNS
    assert list(clean.sales[0]) == SALES_COLUMNS


@pytest.mark.parametrize("scale", [1, 2])
def test_row_counts_scale(scale):
    data = generate_source_data(record_scale=scale, seed=42)
    assert len(data.customers) == BASE_CUSTOMERS * scale
    assert len(data.orders) == BASE_ORDERS * scale
    assert len(data.orders) <= len(data.sales) <= 4 * len(data.orders)


def test_deterministic_with_fixed_seed(clean):
    again = generate_source_data(record_scale=1, inject_duplicates=False, seed=42)
    assert again.customers == clean.customers
    assert again.orders == clean.orders
    assert again.sales == clean.sales
    assert generate_source_data(seed=7).sales != clean.sales


def test_regions_are_valid_and_all_populated(clean):
    assert {c["region"] for c in clean.customers} == set(REGIONS)


def test_referential_integrity(dirty):
    customer_ids = {c["customer_id"] for c in dirty.customers}
    order_customer = {o["order_id"]: o["customer_id"] for o in dirty.orders}
    assert all(o["customer_id"] in customer_ids for o in dirty.orders)
    assert all(s["order_id"] in order_customer for s in dirty.sales)
    assert all(s["customer_id"] == order_customer[s["order_id"]] for s in dirty.sales)


def test_net_amount_formula(clean):
    assert net_amount(3, 10.0, 0.1) == 27.0
    for row in clean.sales:
        expected = round(row["quantity"] * row["unit_price"] * (1 - row["discount"]), 2)
        assert row["net_amount"] == expected


def test_order_total_matches_sales_lines(clean):
    totals: dict[str, float] = {}
    for s in clean.sales:
        totals[s["order_id"]] = totals.get(s["order_id"], 0.0) + s["net_amount"]
    for o in clean.orders:
        assert o["order_total"] == pytest.approx(totals[o["order_id"]], abs=0.01)


def test_duplicate_injection_off(clean):
    assert clean.duplicate_count == 0


def test_duplicate_injection_on_is_2_to_5_percent(clean, dirty):
    base = len(clean.sales)
    assert 0.02 * base - 1 <= dirty.duplicate_count <= 0.05 * base + 1
    assert dirty.duplicate_count > 0
    # base data is identical; duplicates are only appended
    assert dirty.sales[:base] == clean.sales


def test_duplicates_are_later_versions_of_existing_sales(clean, dirty):
    originals = {s["sale_id"]: s for s in clean.sales}
    for dup in dirty.sales[len(clean.sales):]:
        original = originals[dup["sale_id"]]
        assert dup["updated_at"] > original["updated_at"]
        assert dup["net_amount"] == original["net_amount"]


def test_write_files_and_rerun_overwrites(tmp_path, clean, dirty):
    write_source_files(dirty, str(tmp_path))
    written = write_source_files(clean, str(tmp_path))  # rerun with different data

    for dataset in ("customers", "orders", "sales"):
        assert len(list((tmp_path / dataset).iterdir())) == 1

    with open(written["sales"], newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == len(clean.sales)
    assert list(rows[0]) == SALES_COLUMNS

    with open(written["orders"]) as handle:
        orders = [json.loads(line) for line in handle]
    assert len(orders) == len(clean.orders)
