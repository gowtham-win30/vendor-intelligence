import csv
import sys
from datetime import date
from pathlib import Path

import openpyxl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "pipeline"))

from pipeline.parts1.extract_parts import (  # noqa: E402
    fe_sales_gap_sql,
    neuco_qty_changes_sql,
    sales_queries,
    vendor_queries,
)
from pipeline.parts1.parse_local import (  # noqa: E402
    iter_offer_rows,
    select_local_snapshots,
    ued_snapshot_date,
    write_offers,
)

TODAY = date(2026, 10, 5)


def test_ued_dates_and_ambiguous_names():
    assert ued_snapshot_date("HN0003Inventory031626.xlsx", TODAY) == ("OK", date(2026, 3, 16))
    assert ued_snapshot_date("HN0003IInventory082026.xlsx", TODAY) == ("OK", date(2026, 8, 20))
    assert ued_snapshot_date("HN0003InventoryFile072926.xlsx", TODAY) == ("OK", date(2026, 7, 29))
    assert ued_snapshot_date("HN0003Inventory050426B.xlsx", TODAY) == ("OK", date(2026, 5, 4))
    assert ued_snapshot_date("HN0003Inventory061226 (1).xlsx", TODAY) == ("OK", date(2026, 6, 12))
    assert ued_snapshot_date("HN0003Inventory0224266.csv", TODAY)[0] == "OK_TRIMMED"
    assert ued_snapshot_date("HN0003Inventory0224266.csv", TODAY)[1] == date(2026, 2, 24)
    assert ued_snapshot_date("HN0003Inventory0406226.xlsx", TODAY)[0] == "AMBIGUOUS_DATE"


def test_tab_csv_and_padded_metropac_and_neuco_extra_columns(tmp_path: Path):
    ued = tmp_path / "ued"
    neuco = tmp_path / "neuco"
    metro = tmp_path / "metro"
    ued.mkdir()
    neuco.mkdir()
    metro.mkdir()
    (ued / "HN0003Inventory010526.csv").write_text(
        "Mfg\tMfgItemCode\tDescription\tCustCorePrice\tCustExchPrice\tDealerExchPrice\tListExchPrice\tQtyAvail\n"
        "ACC\t110AC6\tcord\t0\t5.35\t5.35\t7.95\t26\n"
        "ACC\t3804FFSS2\tpack\t0\t10.57\t10.57\t14.8\t50+\n"
        "\t\tcompressor\t0\t1\t2\t3\t4\n",
        encoding="utf-8",
    )
    (neuco / "AAA.daily.20260918.txt").write_text(
        "Manu code\tManu name\tItem\tminimum_qty_order\tQty Available\tsell_price\tdescription\n"
        "AA\tAurora\t116-0513-010\t1\t0\t450.84\tbracket\n",
        encoding="utf-8",
    )
    (metro / "DOC#1.txt").write_text(
        "METROPAC PART NUMBER,MANUFACTURER,MANUFACTURER PART NUMBER,DESCRIPTION,ONHAND,NET_PRICE,UPC,WEIGHT,IMAGE URL\n"
        "143227,YORK,S1-02435329000,Draft Inducer,8,266.46972000,688714295280,7.0000,https://example.test/a.jpg\n",
        encoding="utf-8",
    )
    selected, rejects = select_local_snapshots(
        {"Neuco": neuco, "Metropac": metro, "UED": ued},
        TODAY,
    )
    by_vendor = {item.vendor_code: item for item in selected}
    offers, counts = iter_offer_rows(by_vendor["UED"])
    assert counts["MISSING_PART_KEY"] == 1
    assert [(row[2], row[3], row[4], row[5], row[6]) for row in offers] == [
        ("ACC", "110AC6", "26", "5.35", "DEALER_EXCHANGE"),
        ("ACC", "3804FFSS2", "50+", "10.57", "DEALER_EXCHANGE"),
    ]
    neuco_offers, _ = iter_offer_rows(by_vendor["Neuco"])
    assert neuco_offers[0][2:] == ("AA", "116-0513-010", "0", "450.84", "SELL")
    metro_offers, _ = iter_offer_rows(by_vendor["Metropac"])
    assert metro_offers[0][3] == "143227"
    assert metro_offers[0][6] == "NET"
    assert rejects == []


def test_server_extract_omits_customer_fields_and_vendor_price():
    sales_sql = "\n".join(sales_queries().values()).lower()
    vendor_sql = "\n".join(vendor_queries().values()).lower()
    assert "customeremail" not in sales_sql
    assert "phone" not in sales_sql
    assert "old_price" not in vendor_sql
    assert "new_price" not in vendor_sql
    gap_sql = fe_sales_gap_sql("dbo.FESalesData2026").lower()
    qty_sql = neuco_qty_changes_sql().lower()
    assert "customeremail" not in gap_sql
    assert "2024-04-17" in qty_sql
    assert "old_price" not in qty_sql


def test_negative_price_is_dropped(tmp_path: Path):
    folder = tmp_path / "ued"
    folder.mkdir()
    path = folder / "HN0003Inventory010526.csv"
    path.write_text(
        "Mfg,MfgItemCode,DealerExchPrice,QtyAvail\nACC,GOOD,5.00,1\nACC,BAD,-1,1\n",
        encoding="utf-8",
    )
    selected, _ = select_local_snapshots({"UED": folder}, TODAY)
    destination = tmp_path / "out.csv"
    rejects = write_offers(selected, destination)
    assert rejects[0].reason_code == "NEGATIVE_PRICE"
    rows = list(csv.reader(destination.open(encoding="utf-8")))
    assert len(rows) == 1
    assert rows[0][3] == "GOOD"


def _write_xlsx(path: Path, rows: list[list[object]]) -> None:
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    for row in rows:
        sheet.append(row)
    workbook.save(path)
    workbook.close()


def test_duplicate_ued_date_keeps_newer_file(tmp_path: Path):
    ued = tmp_path / "ued"
    ued.mkdir()
    older = ued / "HN0003Inventory050426.xlsx"
    newer = ued / "HN0003Inventory050426B.xlsx"
    _write_xlsx(older, [["Mfg", "MfgItemCode", "DealerExchPrice", "QtyAvail"], ["OLD", "1", 1, 1]])
    _write_xlsx(newer, [["Mfg", "MfgItemCode", "DealerExchPrice", "QtyAvail"], ["NEW", "1", 2, 2]])
    import os

    os.utime(older, (1_700_000_000, 1_700_000_000))
    os.utime(newer, (1_800_000_000, 1_800_000_000))
    selected, rejects = select_local_snapshots({"UED": ued}, TODAY)
    assert [item.path.name for item in selected] == ["HN0003Inventory050426B.xlsx"]
    assert {item.reason_code for item in rejects} == {"DUPLICATE_DATE"}
    offers, _ = iter_offer_rows(selected[0])
    assert offers[0][2] == "NEW"
