import csv
from decimal import Decimal
from pathlib import Path

from generate_phase1_reports import get_pg_conn
from ingest_parts_to_postgres import sql_server_conn

REPORT_PATH = (
    Path(__file__).resolve().parent.parent
    / "reports"
    / "validation"
    / "phase1_source_reconciliation.csv"
)
FULL_REPORT_PATH = (
    Path(__file__).resolve().parent.parent
    / "reports"
    / "validation"
    / "full_source_reconciliation.csv"
)

VENDOR_SOURCES = {
    "Neuco": {
        "table": "dbo.NeucoDelta",
        "sku": "Item",
        "snapshot": "Date2",
        "old_qty": "Old_Qty",
        "new_qty": "New_Qty",
        "old_price": "Old_Price",
        "new_price": "New_Price",
    },
    "Metropac": {
        "table": "dbo.MetropacDelta",
        "sku": "SKU",
        "legacy_prior": "Day1",
        "snapshot": "Day2",
        "old_qty": "OldQty",
        "new_qty": "NewQty",
    },
    "EC": {
        "table": "dbo.ECQtyChanges",
        "sku": "SKU",
        "legacy_prior": "Day1",
        "snapshot": "Day2",
        "old_qty": "OldQty",
        "new_qty": "NewQty",
    },
    "Msupply": {
        "table": "dbo.MsupplyQTYChanges",
        "sku": "SKU",
        "legacy_prior": "Day1",
        "snapshot": "Day2",
        "old_qty": "OldQty",
        "new_qty": "NewQty",
    },
    "ReliableParts": {
        "table": "dbo.RPDailyQTYChanges",
        "sku": "SKU",
        "legacy_prior": "Day1",
        "snapshot": "Day2",
        "old_qty": "OldQty",
        "new_qty": "NewQty",
    },
}

SALES_SOURCES = {
    "PartsFE": "dbo.FESalesData",
    "PartsHNC": "dbo.HNCSalesData",
    "APZ": "dbo.APZSalesData",
    "WTB": "dbo.WTBSalesData",
}

INVENTORY_SOURCES = {
    "APZ": "dbo.APZLatestData",
    "PartsHNC": "dbo.HNCLatestData",
    "PartsFE": "dbo.FELatestData",
    "WTB": "dbo.WTBLatestData",
}

MERCHANT_CATALOG_SOURCES = {
    "APZ": "dbo.APZMcFeed",
    "PartsHNC": "dbo.HNCMCFeed",
    "PartsFE": "dbo.FEMCDataFeed",
    "WTB": "dbo.WTBMCFeed",
}


def as_number(value):
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def as_date(value):
    if value is None:
        return None
    if hasattr(value, "date"):
        value = value.date()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)[:10]


def fetch_sample_skus(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            WITH vendor_ranked AS (
                SELECT
                    source_vendor AS source_name,
                    sku,
                    ROW_NUMBER() OVER (
                        PARTITION BY source_vendor
                        ORDER BY MD5(source_vendor || ':' || sku)
                    ) AS source_rank
                FROM (
                    SELECT DISTINCT source_vendor, sku
                    FROM clean.vendor_deltas
                ) candidates
            ),
            sales_ranked AS (
                SELECT
                    source_store AS source_name,
                    sku,
                    ROW_NUMBER() OVER (
                        PARTITION BY source_store
                        ORDER BY MD5(source_store || ':' || sku)
                    ) AS source_rank
                FROM (
                    SELECT DISTINCT source_store, sku
                    FROM clean.sales_orders
                ) candidates
            )
            SELECT DISTINCT sku
            FROM (
                SELECT sku FROM analytics.phase1_validation_sample
                UNION ALL
                SELECT sku FROM vendor_ranked WHERE source_rank <= 3
                UNION ALL
                SELECT sku FROM sales_ranked WHERE source_rank <= 3
                UNION ALL
                SELECT sku
                FROM analytics.preliminary_top_200_focus_parts
                WHERE rank <= 100
            ) validation_skus
            ORDER BY sku
            """
        )
        return [row[0] for row in cur.fetchall()]


def fetch_postgres_vendor_baseline(pg_conn, skus):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                r.source_vendor,
                UPPER(TRIM(r.sku)) AS sku,
                COUNT(*) AS row_count,
                COALESCE(SUM(
                    CASE
                        WHEN r.old_qty >= 0 AND r.new_qty >= 0 AND r.old_qty > r.new_qty
                        THEN r.old_qty - r.new_qty
                        ELSE 0
                    END
                ), 0) AS valid_depleted_units,
                COUNT(*) FILTER (
                    WHERE r.old_qty >= 0 AND r.new_qty >= 0 AND r.old_qty > r.new_qty
                ) AS depletion_events,
                COUNT(*) FILTER (
                    WHERE r.old_qty > 0 AND r.new_qty = 0
                ) AS stockout_transitions
            FROM raw.vendor_deltas r
            WHERE UPPER(TRIM(r.sku)) = ANY(%s)
            GROUP BY r.source_vendor, UPPER(TRIM(r.sku))
            """,
            (skus,),
        )
        baseline = {
            (row[0], row[1]): {
                "row_count": int(row[2]),
                "valid_depleted_units": as_number(row[3]),
                "depletion_events": int(row[4]),
                "stockout_transitions": int(row[5]),
            }
            for row in cur.fetchall()
        }

        cur.execute(
            """
            SELECT source_vendor, MAX(snapshot_date)
            FROM raw.vendor_deltas
            GROUP BY source_vendor
            """
        )
        cutoffs = dict(cur.fetchall())

    return baseline, cutoffs


def fetch_postgres_sales_baseline(pg_conn, skus):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                r.source_store,
                UPPER(TRIM(r.sku)) AS sku,
                COUNT(*) AS row_count,
                COALESCE(SUM(CASE WHEN r.qty_ordered > 0 THEN r.qty_ordered ELSE 0 END), 0)
                    AS valid_units_sold,
                COALESCE(SUM(
                    CASE
                        WHEN r.qty_ordered > 0 AND r.price >= 0
                        THEN r.qty_ordered * r.price
                        ELSE 0
                    END
                ), 0) AS valid_revenue
            FROM raw.sales_orders r
            WHERE UPPER(TRIM(r.sku)) = ANY(%s)
            GROUP BY r.source_store, UPPER(TRIM(r.sku))
            """,
            (skus,),
        )
        baseline = {
            (row[0], row[1]): {
                "row_count": int(row[2]),
                "valid_units_sold": as_number(row[3]),
                "valid_revenue": as_number(row[4]),
            }
            for row in cur.fetchall()
        }

        cur.execute(
            """
            SELECT source_store, MAX(created_at)
            FROM raw.sales_orders
            GROUP BY source_store
            """
        )
        cutoffs = dict(cur.fetchall())

    return baseline, cutoffs


def fetch_postgres_full_summaries(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                source_vendor,
                COUNT(*) AS row_count,
                COUNT(DISTINCT UPPER(TRIM(sku))) AS distinct_skus,
                MIN(snapshot_date) AS first_record_date,
                MAX(snapshot_date) AS last_record_date,
                COUNT(*) FILTER (
                    WHERE source_vendor = 'Neuco'
                      AND old_price IS NOT NULL
                      AND new_price IS NOT NULL
                      AND old_price <> new_price
                ) AS price_change_rows
            FROM raw.vendor_deltas
            WHERE sku IS NOT NULL
              AND TRIM(sku) <> ''
              AND snapshot_date IS NOT NULL
              AND snapshot_date <= CURRENT_DATE
            GROUP BY source_vendor
            """
        )
        vendor = {
            row[0]: {
                "row_count": int(row[1]),
                "distinct_skus": int(row[2]),
                "first_record_date": as_date(row[3]),
                "last_record_date": as_date(row[4]),
                "price_change_rows": int(row[5]),
            }
            for row in cur.fetchall()
        }

        cur.execute(
            """
            SELECT
                source_store,
                COUNT(*) AS row_count,
                COUNT(DISTINCT UPPER(TRIM(sku))) AS distinct_skus,
                MIN(created_at)::date AS first_record_date,
                MAX(created_at)::date AS last_record_date
            FROM raw.sales_orders
            WHERE sku IS NOT NULL
              AND TRIM(sku) <> ''
              AND created_at IS NOT NULL
            GROUP BY source_store
            """
        )
        sales = {
            row[0]: {
                "row_count": int(row[1]),
                "distinct_skus": int(row[2]),
                "first_record_date": as_date(row[3]),
                "last_record_date": as_date(row[4]),
                "price_change_rows": 0,
            }
            for row in cur.fetchall()
        }
    return vendor, sales


def fetch_postgres_catalog_summaries(pg_conn):
    summaries = {}
    with pg_conn.cursor() as cur:
        for data_area, relation in (
            ("current_inventory", "raw.current_inventory"),
            ("merchant_catalog", "raw.merchant_catalog"),
        ):
            cur.execute(
                f"""
                SELECT
                    source_store,
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT UPPER(TRIM(sku))) AS distinct_skus
                FROM {relation}
                WHERE sku IS NOT NULL
                  AND TRIM(sku) <> ''
                GROUP BY source_store
                """
            )
            summaries[data_area] = {
                row[0]: {
                    "row_count": int(row[1]),
                    "distinct_skus": int(row[2]),
                    "first_record_date": None,
                    "last_record_date": None,
                    "price_change_rows": 0,
                }
                for row in cur.fetchall()
            }
    return summaries


def fetch_sql_server_catalog_summary(conn, table, sku_column):
    query = f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT UPPER(LTRIM(RTRIM(
                CAST({sku_column} AS varchar(100))
            )))) AS distinct_skus
        FROM {table}
        WHERE {sku_column} IS NOT NULL
          AND LTRIM(RTRIM(CAST({sku_column} AS varchar(100)))) <> ''
    """
    cursor = conn.cursor()
    cursor.execute(query)
    row = cursor.fetchone()
    return {
        "row_count": int(row[0]),
        "distinct_skus": int(row[1]),
        "first_record_date": None,
        "last_record_date": None,
        "price_change_rows": 0,
    }


def fetch_sql_server_full_vendor(conn, source_name, config, cutoff):
    price_change_expression = "CAST(0 AS bigint)"
    if config.get("old_price") and config.get("new_price"):
        price_change_expression = f"""
            SUM(CASE
                WHEN {config['old_price']} IS NOT NULL
                 AND {config['new_price']} IS NOT NULL
                 AND CAST({config['old_price']} AS float)
                     <> CAST({config['new_price']} AS float)
                THEN 1 ELSE 0
            END)
        """
    query = f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT UPPER(LTRIM(RTRIM(
                CAST({config['sku']} AS varchar(100))
            )))) AS distinct_skus,
            MIN(CAST({config['snapshot']} AS date)) AS first_record_date,
            MAX(CAST({config['snapshot']} AS date)) AS last_record_date,
            {price_change_expression} AS price_change_rows
        FROM {config['table']}
        WHERE {config['snapshot']} IS NOT NULL
          AND CAST({config['snapshot']} AS date) <= CAST(? AS date)
          AND {config['sku']} IS NOT NULL
          AND LTRIM(RTRIM(CAST({config['sku']} AS varchar(100)))) <> ''
    """
    cursor = conn.cursor()
    cursor.execute(query, str(cutoff))
    row = cursor.fetchone()
    return {
        "row_count": int(row[0]),
        "distinct_skus": int(row[1]),
        "first_record_date": as_date(row[2]),
        "last_record_date": as_date(row[3]),
        "price_change_rows": int(row[4]),
    }


def fetch_sql_server_full_sales(conn, table, cutoff):
    query = f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT UPPER(LTRIM(RTRIM(
                CAST(SKU AS varchar(100))
            )))) AS distinct_skus,
            MIN(CAST(Created_at AS date)) AS first_record_date,
            MAX(CAST(Created_at AS date)) AS last_record_date
        FROM {table}
        WHERE Created_at IS NOT NULL
          AND Created_at <= CAST(? AS datetime)
          AND SKU IS NOT NULL
          AND LTRIM(RTRIM(CAST(SKU AS varchar(100)))) <> ''
    """
    cursor = conn.cursor()
    cursor.execute(query, str(cutoff))
    row = cursor.fetchone()
    return {
        "row_count": int(row[0]),
        "distinct_skus": int(row[1]),
        "first_record_date": as_date(row[2]),
        "last_record_date": as_date(row[3]),
        "price_change_rows": 0,
    }


def build_full_comparison_rows(data_areas):
    rows = []
    for data_area, postgres_values, source_values in data_areas:
        for source_name in sorted(set(postgres_values) | set(source_values)):
            pg_row = postgres_values.get(source_name, {})
            source_row = source_values.get(source_name, {})
            checks = {
                field: pg_row.get(field) == source_row.get(field)
                for field in (
                    "row_count",
                    "distinct_skus",
                    "first_record_date",
                    "last_record_date",
                    "price_change_rows",
                )
            }
            rows.append(
                {
                    "data_area": data_area,
                    "source_name": source_name,
                    "postgres_row_count": pg_row.get("row_count", 0),
                    "source_row_count": source_row.get("row_count", 0),
                    "postgres_distinct_skus": pg_row.get("distinct_skus", 0),
                    "source_distinct_skus": source_row.get("distinct_skus", 0),
                    "postgres_first_record_date": pg_row.get(
                        "first_record_date", ""
                    ),
                    "source_first_record_date": source_row.get(
                        "first_record_date", ""
                    ),
                    "postgres_last_record_date": pg_row.get(
                        "last_record_date", ""
                    ),
                    "source_last_record_date": source_row.get(
                        "last_record_date", ""
                    ),
                    "postgres_price_change_rows": pg_row.get(
                        "price_change_rows", 0
                    ),
                    "source_price_change_rows": source_row.get(
                        "price_change_rows", 0
                    ),
                    "validation_pass": all(checks.values()),
                    "failed_checks": ",".join(
                        field for field, passed in checks.items() if not passed
                    ),
                }
            )
    return rows


def fetch_sql_server_vendor(conn, source_name, config, skus, cutoff):
    placeholders = ", ".join("?" for _ in skus)
    sku_expression = f"UPPER(LTRIM(RTRIM(CAST({config['sku']} AS varchar(100)))))"
    old_qty = f"CAST({config['old_qty']} AS float)"
    new_qty = f"CAST({config['new_qty']} AS float)"
    legacy_prior = config.get("legacy_prior")
    legacy_exclusion_expression = (
        f"""
            SUM(CASE
                WHEN {legacy_prior} IS NULL
                  OR CAST({legacy_prior} AS date) > CAST(GETDATE() AS date)
                THEN 1 ELSE 0
            END)
        """
        if legacy_prior
        else "CAST(0 AS int)"
    )
    query = f"""
        SELECT
            {sku_expression} AS sku,
            COUNT(*) AS row_count,
            SUM(CASE
                WHEN {old_qty} >= 0 AND {new_qty} >= 0 AND {old_qty} > {new_qty}
                THEN {old_qty} - {new_qty}
                ELSE 0
            END) AS valid_depleted_units,
            SUM(CASE
                WHEN {old_qty} >= 0 AND {new_qty} >= 0 AND {old_qty} > {new_qty}
                THEN 1 ELSE 0
            END) AS depletion_events,
            SUM(CASE
                WHEN {old_qty} > 0 AND {new_qty} = 0
                THEN 1 ELSE 0
            END) AS stockout_transitions,
            {legacy_exclusion_expression} AS legacy_filter_excluded_rows
        FROM {config['table']}
        WHERE {config['sku']} IS NOT NULL
          AND LTRIM(RTRIM(CAST({config['sku']} AS varchar(100)))) <> ''
          AND {config['snapshot']} IS NOT NULL
          AND CAST({config['snapshot']} AS date) <= CAST(? AS date)
          AND {sku_expression} IN ({placeholders})
        GROUP BY {sku_expression}
    """
    cursor = conn.cursor()
    cursor.execute(query, str(cutoff), *skus)
    return {
        (source_name, row[0]): {
            "row_count": int(row[1]),
            "valid_depleted_units": as_number(row[2]),
            "depletion_events": int(row[3]),
            "stockout_transitions": int(row[4]),
            "legacy_filter_excluded_rows": int(row[5]),
        }
        for row in cursor.fetchall()
    }


def fetch_sql_server_sales(conn, source_name, table, skus, cutoff):
    placeholders = ", ".join("?" for _ in skus)
    sku_expression = "UPPER(LTRIM(RTRIM(CAST(SKU AS varchar(100)))))"
    query = f"""
        SELECT
            {sku_expression} AS sku,
            COUNT(*) AS row_count,
            SUM(CASE WHEN CAST(qty_ordered AS float) > 0
                THEN CAST(qty_ordered AS float) ELSE 0 END) AS valid_units_sold,
            SUM(CASE
                WHEN CAST(qty_ordered AS float) > 0 AND CAST(price AS float) >= 0
                THEN CAST(qty_ordered AS float) * CAST(price AS float)
                ELSE 0
            END) AS valid_revenue
        FROM {table}
        WHERE SKU IS NOT NULL
          AND LTRIM(RTRIM(CAST(SKU AS varchar(100)))) <> ''
          AND Created_at IS NOT NULL
          AND Created_at <= CAST(? AS datetime)
          AND {sku_expression} IN ({placeholders})
        GROUP BY {sku_expression}
    """
    cursor = conn.cursor()
    cursor.execute(query, str(cutoff), *skus)
    return {
        (source_name, row[0]): {
            "row_count": int(row[1]),
            "valid_units_sold": as_number(row[2]),
            "valid_revenue": as_number(row[3]),
        }
        for row in cursor.fetchall()
    }


def compare_metric(left, right, tolerance=0.0001):
    return abs(as_number(left) - as_number(right)) <= tolerance


def build_comparison_rows(pg_vendor, source_vendor, pg_sales, source_sales):
    rows = []

    for key in sorted(set(pg_vendor) | set(source_vendor)):
        pg_values = pg_vendor.get(key, {})
        source_values = source_vendor.get(key, {})
        checks = {
            "row_count": compare_metric(
                pg_values.get("row_count", 0), source_values.get("row_count", 0)
            ),
            "valid_depleted_units": compare_metric(
                pg_values.get("valid_depleted_units", 0),
                source_values.get("valid_depleted_units", 0),
            ),
            "depletion_events": compare_metric(
                pg_values.get("depletion_events", 0),
                source_values.get("depletion_events", 0),
            ),
            "stockout_transitions": compare_metric(
                pg_values.get("stockout_transitions", 0),
                source_values.get("stockout_transitions", 0),
            ),
        }
        row_difference = (
            source_values.get("row_count", 0) - pg_values.get("row_count", 0)
        )
        legacy_filter_excluded_rows = source_values.get(
            "legacy_filter_excluded_rows",
            0,
        )
        is_match = all(checks.values())
        legacy_omission_confirmed = (
            not is_match
            and row_difference > 0
            and row_difference == legacy_filter_excluded_rows
        )
        status = (
            "MATCH"
            if is_match
            else (
                "LEGACY_FILTER_OMISSION_CONFIRMED"
                if legacy_omission_confirmed
                else "SOURCE_MISMATCH_UNEXPLAINED"
            )
        )
        rows.append(
            {
                "data_area": "vendor",
                "source_name": key[0],
                "sku": key[1],
                "postgres_row_count": pg_values.get("row_count", 0),
                "source_row_count": source_values.get("row_count", 0),
                "postgres_primary_value": pg_values.get("valid_depleted_units", 0),
                "source_primary_value": source_values.get("valid_depleted_units", 0),
                "postgres_secondary_value": pg_values.get("depletion_events", 0),
                "source_secondary_value": source_values.get("depletion_events", 0),
                "postgres_tertiary_value": pg_values.get("stockout_transitions", 0),
                "source_tertiary_value": source_values.get("stockout_transitions", 0),
                "row_count_difference": row_difference,
                "legacy_filter_excluded_rows": legacy_filter_excluded_rows,
                "validation_pass": is_match,
                "status": status,
            }
        )

    for key in sorted(set(pg_sales) | set(source_sales)):
        pg_values = pg_sales.get(key, {})
        source_values = source_sales.get(key, {})
        checks = {
            "row_count": compare_metric(
                pg_values.get("row_count", 0), source_values.get("row_count", 0)
            ),
            "valid_units_sold": compare_metric(
                pg_values.get("valid_units_sold", 0),
                source_values.get("valid_units_sold", 0),
            ),
            "valid_revenue": compare_metric(
                pg_values.get("valid_revenue", 0),
                source_values.get("valid_revenue", 0),
                tolerance=0.01,
            ),
        }
        rows.append(
            {
                "data_area": "sales",
                "source_name": key[0],
                "sku": key[1],
                "postgres_row_count": pg_values.get("row_count", 0),
                "source_row_count": source_values.get("row_count", 0),
                "postgres_primary_value": pg_values.get("valid_units_sold", 0),
                "source_primary_value": source_values.get("valid_units_sold", 0),
                "postgres_secondary_value": pg_values.get("valid_revenue", 0),
                "source_secondary_value": source_values.get("valid_revenue", 0),
                "postgres_tertiary_value": "",
                "source_tertiary_value": "",
                "row_count_difference": (
                    source_values.get("row_count", 0) - pg_values.get("row_count", 0)
                ),
                "legacy_filter_excluded_rows": 0,
                "validation_pass": all(checks.values()),
                "status": (
                    "MATCH"
                    if all(checks.values())
                    else "SOURCE_MISMATCH_UNEXPLAINED"
                ),
            }
        )

    return rows


def write_report(rows):
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "data_area",
        "source_name",
        "sku",
        "postgres_row_count",
        "source_row_count",
        "postgres_primary_value",
        "source_primary_value",
        "postgres_secondary_value",
        "source_secondary_value",
        "postgres_tertiary_value",
        "source_tertiary_value",
        "row_count_difference",
        "legacy_filter_excluded_rows",
        "validation_pass",
        "status",
    ]
    with REPORT_PATH.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_full_report(rows):
    FULL_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "data_area",
        "source_name",
        "postgres_row_count",
        "source_row_count",
        "postgres_distinct_skus",
        "source_distinct_skus",
        "postgres_first_record_date",
        "source_first_record_date",
        "postgres_last_record_date",
        "source_last_record_date",
        "postgres_price_change_rows",
        "source_price_change_rows",
        "validation_pass",
        "failed_checks",
    ]
    with FULL_REPORT_PATH.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    pg_conn = get_pg_conn()
    source_conn = sql_server_conn()
    try:
        skus = fetch_sample_skus(pg_conn)
        pg_vendor, vendor_cutoffs = fetch_postgres_vendor_baseline(pg_conn, skus)
        pg_sales, sales_cutoffs = fetch_postgres_sales_baseline(pg_conn, skus)
        pg_full_vendor, pg_full_sales = fetch_postgres_full_summaries(pg_conn)
        pg_catalog = fetch_postgres_catalog_summaries(pg_conn)
        pg_conn.rollback()

        source_vendor = {}
        source_full_vendor = {}
        for source_name, config in VENDOR_SOURCES.items():
            source_vendor.update(
                fetch_sql_server_vendor(
                    source_conn,
                    source_name,
                    config,
                    skus,
                    vendor_cutoffs[source_name],
                )
            )
            source_full_vendor[source_name] = fetch_sql_server_full_vendor(
                source_conn,
                source_name,
                config,
                vendor_cutoffs[source_name],
            )

        source_sales = {}
        source_full_sales = {}
        for source_name, table in SALES_SOURCES.items():
            source_sales.update(
                fetch_sql_server_sales(
                    source_conn,
                    source_name,
                    table,
                    skus,
                    sales_cutoffs[source_name],
                )
            )
            source_full_sales[source_name] = fetch_sql_server_full_sales(
                source_conn,
                table,
                sales_cutoffs[source_name],
            )

        source_full_inventory = {
            source_name: fetch_sql_server_catalog_summary(
                source_conn,
                table,
                "SKU",
            )
            for source_name, table in INVENTORY_SOURCES.items()
        }
        source_full_catalog = {
            source_name: fetch_sql_server_catalog_summary(
                source_conn,
                table,
                "id",
            )
            for source_name, table in MERCHANT_CATALOG_SOURCES.items()
        }

        rows = build_comparison_rows(
            pg_vendor,
            source_vendor,
            pg_sales,
            source_sales,
        )
        write_report(rows)
        full_rows = build_full_comparison_rows(
            (
                ("vendor", pg_full_vendor, source_full_vendor),
                ("sales", pg_full_sales, source_full_sales),
                (
                    "current_inventory",
                    pg_catalog["current_inventory"],
                    source_full_inventory,
                ),
                (
                    "merchant_catalog",
                    pg_catalog["merchant_catalog"],
                    source_full_catalog,
                ),
            )
        )
        write_full_report(full_rows)
    finally:
        source_conn.close()
        pg_conn.close()

    matched = sum(1 for row in rows if row["validation_pass"])
    mismatched = len(rows) - matched
    full_matched = sum(1 for row in full_rows if row["validation_pass"])
    full_mismatched = len(full_rows) - full_matched
    print("=" * 60)
    print("PHASE 1 SQL SERVER SOURCE RECONCILIATION")
    print("=" * 60)
    print(f"Source/SKU comparisons:  {len(rows)}")
    print(f"Matched:                 {matched}")
    print(f"Mismatched:              {mismatched}")
    print(f"Report:                  {REPORT_PATH}")
    print(f"Full source comparisons: {len(full_rows)}")
    print(f"Full source matched:     {full_matched}")
    print(f"Full source mismatched:  {full_mismatched}")
    print(f"Full report:             {FULL_REPORT_PATH}")
    print("=" * 60)

    if mismatched or full_mismatched:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
