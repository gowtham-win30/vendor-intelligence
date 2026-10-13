"""Few-column extracts from the Parts SQL Server.

Sales and vendor changes only. Customer name, email, phone, and address
are not selected.
"""
from __future__ import annotations

SALES_TABLES = {
    "PartsFE": "dbo.FESalesData",
    "PartsHNC": "dbo.HNCSalesData",
    "APZ": "dbo.APZSalesData",
    "WTB": "dbo.WTBSalesData",
    "PM": "dbo.PMSalesData",
    "HT": "dbo.HTSalesData",
}
# Year slices that contain a few PartsFE lines absent from FeSalesData.
FE_SALES_GAPS = ("dbo.FESalesData2025", "dbo.FESalesData2026")
FE_SALES_PARENT = "dbo.FeSalesData"

VENDOR_SOURCES = (
    (
        "Neuco",
        "dbo.NeucoDelta",
        "Date2",
        "UPPER(LTRIM(RTRIM(CAST(Item AS varchar(100)))))",
        "CAST(Old_Qty AS decimal(18, 4))",
        "CAST(New_Qty AS decimal(18, 4))",
        "Item",
    ),
    (
        "Metropac",
        "dbo.MetropacDelta",
        "Day2",
        "UPPER(LTRIM(RTRIM(CAST(SKU AS varchar(100)))))",
        "CAST(OldQty AS decimal(18, 4))",
        "CAST(NewQty AS decimal(18, 4))",
        "SKU",
    ),
    (
        "EC",
        "dbo.ECQtyChanges",
        "Day2",
        "UPPER(LTRIM(RTRIM(CAST(SKU AS varchar(100)))))",
        "CAST(OldQty AS decimal(18, 4))",
        "CAST(NewQty AS decimal(18, 4))",
        "SKU",
    ),
    (
        "Msupply",
        "dbo.MsupplyQTYChanges",
        "Day2",
        "UPPER(LTRIM(RTRIM(CAST(SKU AS varchar(100)))))",
        "CAST(OldQty AS decimal(18, 4))",
        "CAST(NewQty AS decimal(18, 4))",
        "SKU",
    ),
    (
        "ReliableParts",
        "dbo.RPDailyQTYChanges",
        "Day2",
        "UPPER(LTRIM(RTRIM(CAST(SKU AS varchar(100)))))",
        "CAST(OldQty AS decimal(18, 4))",
        "CAST(NewQty AS decimal(18, 4))",
        "SKU",
    ),
)


def sales_extract_sql(store_code: str, table: str) -> str:
    return f"""
        SELECT
            '{store_code}' AS store_code,
            LTRIM(RTRIM(CAST(increment_id AS varchar(100)))) AS order_id,
            UPPER(LTRIM(RTRIM(CAST(SKU AS varchar(100))))) AS sku,
            CAST(Created_at AS date) AS sold_on,
            CAST(qty_ordered AS decimal(18, 4)) AS units,
            CASE
                WHEN price IS NULL OR price < 0 THEN NULL
                ELSE CAST(price AS decimal(18, 4))
            END AS unit_price
        FROM {table}
        WHERE Created_at IS NOT NULL
          AND Created_at < DATEADD(day, 1, CAST(GETDATE() AS date))
          AND SKU IS NOT NULL
          AND LTRIM(RTRIM(CAST(SKU AS varchar(100)))) <> ''
          AND increment_id IS NOT NULL
          AND LTRIM(RTRIM(CAST(increment_id AS varchar(100)))) <> ''
          AND qty_ordered > 0
    """


def sales_reject_sql(table: str) -> str:
    return f"""
        SELECT
            SUM(CASE
                WHEN SKU IS NOT NULL
                 AND LTRIM(RTRIM(CAST(SKU AS varchar(100)))) <> ''
                 AND (increment_id IS NULL OR LTRIM(RTRIM(CAST(increment_id AS varchar(100)))) = '')
                THEN 1 ELSE 0 END) AS missing_order_id,
            SUM(CASE
                WHEN SKU IS NOT NULL
                 AND LTRIM(RTRIM(CAST(SKU AS varchar(100)))) <> ''
                 AND (qty_ordered IS NULL OR qty_ordered <= 0)
                THEN 1 ELSE 0 END) AS invalid_units,
            SUM(CASE
                WHEN Created_at >= DATEADD(day, 1, CAST(GETDATE() AS date))
                THEN 1 ELSE 0 END) AS future_sold_on
        FROM {table}
    """


def vendor_extract_sql(
    vendor_code: str,
    table: str,
    change_date: str,
    sku_expr: str,
    qty_before: str,
    qty_after: str,
    sku_column: str,
) -> str:
    return f"""
        SELECT
            '{vendor_code}' AS vendor_code,
            {sku_expr} AS sku,
            CAST({change_date} AS date) AS change_date,
            {qty_before} AS qty_before,
            {qty_after} AS qty_after
        FROM {table}
        WHERE {change_date} IS NOT NULL
          AND {change_date} <= CAST(GETDATE() AS date)
          AND {sku_column} IS NOT NULL
          AND LTRIM(RTRIM(CAST({sku_column} AS varchar(100)))) <> ''
    """


def vendor_queries() -> dict[str, str]:
    return {spec[0]: vendor_extract_sql(*spec) for spec in VENDOR_SOURCES}


def fe_sales_gap_sql(table: str, parent: str = FE_SALES_PARENT) -> str:
    """PartsFE lines that exist only in a year slice, not in the full sales table."""
    return f"""
        SELECT
            'PartsFE' AS store_code,
            LTRIM(RTRIM(CAST(c.increment_id AS varchar(100)))) AS order_id,
            UPPER(LTRIM(RTRIM(CAST(c.SKU AS varchar(100))))) AS sku,
            CAST(c.Created_at AS date) AS sold_on,
            CAST(c.qty_ordered AS decimal(18, 4)) AS units,
            CASE
                WHEN c.price IS NULL OR c.price < 0 THEN NULL
                ELSE CAST(c.price AS decimal(18, 4))
            END AS unit_price
        FROM {table} c
        WHERE c.Created_at IS NOT NULL
          AND c.Created_at < DATEADD(day, 1, CAST(GETDATE() AS date))
          AND c.SKU IS NOT NULL
          AND LTRIM(RTRIM(CAST(c.SKU AS varchar(100)))) <> ''
          AND c.increment_id IS NOT NULL
          AND LTRIM(RTRIM(CAST(c.increment_id AS varchar(100)))) <> ''
          AND c.qty_ordered > 0
          AND NOT EXISTS (
                SELECT 1
                FROM {parent} p
                WHERE LTRIM(RTRIM(CAST(p.increment_id AS varchar(100))))
                      = LTRIM(RTRIM(CAST(c.increment_id AS varchar(100))))
                  AND UPPER(LTRIM(RTRIM(CAST(p.SKU AS varchar(100)))))
                      = UPPER(LTRIM(RTRIM(CAST(c.SKU AS varchar(100)))))
                  AND CAST(p.Created_at AS date) = CAST(c.Created_at AS date)
          )
    """


def neuco_qty_changes_sql() -> str:
    """Neuco quantity changes after NeucoDelta stops. This table has no price."""
    return """
        SELECT
            'Neuco' AS vendor_code,
            UPPER(LTRIM(RTRIM(CAST(SKU AS varchar(100))))) AS sku,
            CAST(Day2 AS date) AS change_date,
            CAST(OldQty AS decimal(18, 4)) AS qty_before,
            CAST(NewQty AS decimal(18, 4)) AS qty_after
        FROM dbo.NeucoQtyChanges
        WHERE Day2 > '2024-04-17'
          AND Day2 <= CAST(GETDATE() AS date)
          AND SKU IS NOT NULL
          AND LTRIM(RTRIM(CAST(SKU AS varchar(100)))) <> ''
    """


def sales_queries() -> dict[str, str]:
    return {
        store_code: sales_extract_sql(store_code, table)
        for store_code, table in SALES_TABLES.items()
    }
