"""Add the sales lines and local Neuco/Metropac history missing from Parts1.

Appends to the live vendor_intelligence tables. Does not rebuild the tables
already loaded.

Neuco on disk is about 29 GB of full daily catalogs. Each day is stored, but
after the first day only rows whose price or quantity changed are inserted.
The latest day is already a full catalog and is left as it is. Metropac is
small, so every distinct file day is stored in full.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from pathlib import Path

import os

from pipeline.parts1.extract_parts import (
    FE_SALES_GAPS,
    SALES_TABLES,
    fe_sales_gap_sql,
    neuco_qty_changes_sql,
    sales_extract_sql,
)
from pipeline.parts1.load import (
    LOCK_RESOURCE,
    Target,
    ensure_offer_stage,
    qualified,
    source_name,
    stage_offer_slice,
)
from pipeline.parts1.parse_local import (
    SelectedFile,
    _later,
    _neuco_file,
    iter_offer_rows,
    neuco_snapshot_date,
    utc_file_date,
)

SALES_DEST = qualified("sales_line")
VENDOR_DEST = qualified("vendor_change")
NEUCO_ROOT = Path(os.getenv("NEUCO_FILES_DIR", r"E:\NeucoFiles"))
METRO_ROOT = Path(os.getenv("METROPAC_FILES_DIR", r"E:\Metropac"))


def insert_sales(target: Target) -> None:
    for store in ("PM", "HT"):
        table = SALES_TABLES[store]
        sql = (
            f"DELETE FROM {SALES_DEST} WHERE store_code = '{store}';\n"
            f"INSERT INTO {SALES_DEST} (store_code, order_id, sku, sold_on, units, unit_price) "
            + sales_extract_sql(store, source_name(table))
        )
        target.execute(sql)
        print(f"sales {store} loaded")
    for gap in FE_SALES_GAPS:
        select = fe_sales_gap_sql(source_name(gap), source_name("dbo.FeSalesData"))
        sql = (
            f"INSERT INTO {SALES_DEST} (store_code, order_id, sku, sold_on, units, unit_price) "
            f"SELECT store_code, order_id, sku, sold_on, units, unit_price FROM ({select}) gap "
            f"WHERE NOT EXISTS ("
            f"SELECT 1 FROM {SALES_DEST} s "
            f"WHERE s.store_code = gap.store_code AND s.order_id = gap.order_id "
            f"AND s.sku = gap.sku AND s.sold_on = gap.sold_on)"
        )
        target.execute(sql)
        print(f"sales gap {gap}: {target.cursor.rowcount:,}")


def insert_neuco_qty(target: Target) -> None:
    select = neuco_qty_changes_sql().replace(
        "FROM dbo.NeucoQtyChanges",
        f"FROM {source_name('dbo.NeucoQtyChanges')}",
    )
    sql = (
        f"DELETE FROM {VENDOR_DEST} WHERE vendor_code = 'Neuco' AND change_date > '2024-04-17';\n"
        f"INSERT INTO {VENDOR_DEST} (vendor_code, sku, change_date, qty_before, qty_after) "
        + select
    )
    target.execute(sql)
    print("Neuco quantity changes after 2024-04-17 loaded")


def metropac_days(root: Path) -> list[tuple[date, Path]]:
    dated: dict[date, list[Path]] = defaultdict(list)
    for path in root.glob("*.txt"):
        if path.name.startswith("~$"):
            continue
        dated[utc_file_date(path)].append(path)
    return [(day, _later(paths)) for day, paths in sorted(dated.items())]


def neuco_days(root: Path) -> list[tuple[date, Path]]:
    dated: dict[date, list[Path]] = defaultdict(list)
    for path in root.glob("*.txt"):
        snapshot = neuco_snapshot_date(path.name)
        if snapshot is not None:
            dated[snapshot].append(path)
    return [(day, _later(paths)) for day, paths in sorted(dated.items())]


def replace_offers(target: Target, vendor_code: str, snapshot: date, offers: list[tuple]) -> None:
    stage_offer_slice(target, vendor_code, snapshot.isoformat(), offers)


def load_metropac(target: Target) -> None:
    days = metropac_days(METRO_ROOT)
    print(f"Metropac days to store: {len(days)}")
    stored = 0
    for snapshot, path in days:
        item = SelectedFile(
            path=path,
            vendor_code="Metropac",
            snapshot_date=snapshot,
            price_kind="NET",
            manufacturer_column="manufacturer",
            part_column="metropac part number",
            qty_column="onhand",
            price_column="net_price",
        )
        offers, _counts = iter_offer_rows(item)
        replace_offers(target, "Metropac", snapshot, offers)
        stored += len(offers)
        print(f"Metropac {snapshot} {path.name} rows={len(offers):,} stored={stored:,}")


def changed_offers(vendor_code: str, snapshot: date, price_kind: str, current: dict, previous: dict | None):
    offers = []
    for key, (qty, price) in current.items():
        if previous is not None and previous.get(key) == (qty, price):
            continue
        manufacturer, part_code = key
        offers.append((vendor_code, snapshot.isoformat(), manufacturer, part_code, qty, price, price_kind))
    if previous is None or len(current) < int(0.8 * len(previous)):
        return offers
    for key, (qty, price) in previous.items():
        if key in current:
            continue
        manufacturer, part_code = key
        offers.append((vendor_code, snapshot.isoformat(), manufacturer, part_code, "", "", price_kind))
    return offers


def load_neuco_history(target: Target) -> None:
    days = neuco_days(NEUCO_ROOT)
    print(f"Neuco days on disk: {len(days)}")
    latest = days[-1][0]
    previous: dict | None = None
    stored = 0
    for snapshot, path in days:
        item = _neuco_file(path, snapshot)
        offers, _counts = iter_offer_rows(item)
        current = {(row[2], row[3]): (row[4], row[5]) for row in offers}
        if snapshot == latest:
            print(f"Neuco {snapshot} kept as the full catalog already loaded ({len(offers):,} rows)")
            previous = current
            continue
        delta = changed_offers("Neuco", snapshot, item.price_kind, current, previous)
        replace_offers(target, "Neuco", snapshot, delta)
        stored += len(delta)
        print(
            f"Neuco {snapshot} {path.name} catalog={len(offers):,} "
            f"kept={len(delta):,} stored={stored:,}"
        )
        previous = current


def main() -> None:
    target = Target(LOCK_RESOURCE + "_history")
    try:
        insert_sales(target)
        insert_neuco_qty(target)
        ensure_offer_stage(target)
        load_metropac(target)
        load_neuco_history(target)
        for table in ("sales_line", "vendor_change", "local_offer"):
            count = target.execute(f"SELECT COUNT(*) FROM {qualified(table)}", fetch=True)[0]
            print(f"{table} {int(count):,}")
    finally:
        target.close()


if __name__ == "__main__":
    main()
