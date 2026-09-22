import argparse
import io
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import pyodbc
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
SQL_SERVER = {
    "host": os.getenv("SQL_SERVER_HOST"),
    "port": os.getenv("SQL_SERVER_PORT", "1433"),
    "database": os.getenv("SQL_SERVER_DB", "Parts"),
    "user": os.getenv("SQL_SERVER_USER"),
    "password": os.getenv("SQL_SERVER_PASSWORD"),
    "driver": os.getenv("SQL_SERVER_DRIVER", "ODBC Driver 18 for SQL Server"),
}

POSTGRES = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5433")),
    "database": os.getenv("PGDATABASE", "vendor_intelligence"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD"),
}

VENDOR_COLUMNS = [
    "source_vendor",
    "source_table",
    "prior_date",
    "snapshot_date",
    "sku",
    "manufacturer",
    "old_qty",
    "new_qty",
    "old_price",
    "new_price",
    "month_label",
]

SALES_COLUMNS = [
    "source_store",
    "source_table",
    "order_id",
    "customer_email",
    "sku",
    "created_at",
    "qty_ordered",
    "price",
    "region",
    "postcode",
    "company",
    "customer_first_name",
    "customer_last_name",
    "phone",
    "product_name",
]

INVENTORY_COLUMNS = [
    "source_store",
    "source_table",
    "sku",
    "manufacturer",
    "quantity",
    "price",
    "list_price",
    "weight",
]

MERCHANT_CATALOG_COLUMNS = [
    "source_store",
    "source_table",
    "sku",
    "mpn",
    "product_name",
    "description",
    "product_link",
    "image_link",
    "price",
    "sale_price",
    "brand",
    "availability",
    "condition",
    "google_product_category",
    "product_type",
    "shipment_weight",
    "status_value",
    "product_detail",
    "custom_label_0",
    "shipping",
]

INGESTION_LOCK_ID = 8429157301


def sql_server_driver():
    available = pyodbc.drivers()
    preferred = SQL_SERVER["driver"]
    if preferred in available:
        return preferred
    for candidate in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server Native Client 11.0", "SQL Server"]:
        if candidate in available:
            return candidate
    raise RuntimeError("No SQL Server ODBC driver found.")


def sql_server_conn():
    missing = [
        name
        for name, value in {
            "SQL_SERVER_HOST": SQL_SERVER["host"],
            "SQL_SERVER_USER": SQL_SERVER["user"],
            "SQL_SERVER_PASSWORD": SQL_SERVER["password"],
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(
            f"Missing required SQL Server environment variables: {', '.join(missing)}"
        )

    driver = sql_server_driver()
    base = (
        f"DRIVER={{{driver}}};"
        f"SERVER={SQL_SERVER['host']},{SQL_SERVER['port']};"
        f"DATABASE={SQL_SERVER['database']};"
        f"UID={SQL_SERVER['user']};"
        f"PWD={SQL_SERVER['password']};"
    )
    if "18" in driver:
        base += "TrustServerCertificate=yes;Encrypt=yes;MARS_Connection=yes;"
    elif "17" in driver:
        base += "TrustServerCertificate=yes;MARS_Connection=yes;"
    return pyodbc.connect(base, timeout=30)


def postgres_conn():
    if not POSTGRES["password"]:
        raise RuntimeError("Missing required environment variable: PGPASSWORD")

    return psycopg2.connect(
        host=POSTGRES["host"],
        port=POSTGRES["port"],
        dbname=POSTGRES["database"],
        user=POSTGRES["user"],
        password=POSTGRES["password"],
    )


def acquire_ingestion_lock(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (INGESTION_LOCK_ID,))
        acquired = cur.fetchone()[0]
    pg_conn.commit()
    if not acquired:
        raise RuntimeError("Another Vendor Intelligence ingestion run is active.")


def release_ingestion_lock(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(%s)", (INGESTION_LOCK_ID,))
    pg_conn.commit()


def ensure_schema(pg_conn):
    schema_path = Path(__file__).resolve().parent / "sql" / "init_schema.sql"
    with pg_conn.cursor() as cur:
        cur.execute(schema_path.read_text(encoding="utf-8"))
    pg_conn.commit()


def top_clause(limit):
    return f"TOP ({int(limit)}) " if limit else ""


def vendor_queries(limit=None):
    top = top_clause(limit)
    today_filter = "CAST(GETDATE() AS date)"
    return {
        "Neuco": f"""
            SELECT {top}
                'Neuco' AS source_vendor,
                'dbo.NeucoDelta' AS source_table,
                CAST(Date1 AS date) AS prior_date,
                CAST(Date2 AS date) AS snapshot_date,
                UPPER(LTRIM(RTRIM(CAST(Item AS varchar(100))))) AS sku,
                UPPER(LTRIM(RTRIM(CAST(Manu_name AS varchar(255))))) AS manufacturer,
                CAST(Old_Qty AS float) AS old_qty,
                CAST(New_Qty AS float) AS new_qty,
                CAST(Old_Price AS float) AS old_price,
                CAST(New_Price AS float) AS new_price,
                CAST(Month AS varchar(50)) AS month_label
            FROM dbo.NeucoDelta
            WHERE Date2 IS NOT NULL
              AND Date2 <= {today_filter}
              AND Item IS NOT NULL
              AND LTRIM(RTRIM(CAST(Item AS varchar(100)))) != ''
        """,
        "Metropac": f"""
            SELECT {top}
                'Metropac' AS source_vendor,
                'dbo.MetropacDelta' AS source_table,
                CAST(Day1 AS date) AS prior_date,
                CAST(Day2 AS date) AS snapshot_date,
                UPPER(LTRIM(RTRIM(CAST(SKU AS varchar(100))))) AS sku,
                CAST(NULL AS varchar(255)) AS manufacturer,
                CAST(OldQty AS float) AS old_qty,
                CAST(NewQty AS float) AS new_qty,
                CAST(NULL AS float) AS old_price,
                CAST(NULL AS float) AS new_price,
                CAST(NULL AS varchar(50)) AS month_label
            FROM dbo.MetropacDelta
            WHERE Day2 IS NOT NULL
              AND Day2 <= {today_filter}
              AND SKU IS NOT NULL
              AND LTRIM(RTRIM(CAST(SKU AS varchar(100)))) != ''
        """,
        "EC": f"""
            SELECT {top}
                'EC' AS source_vendor,
                'dbo.ECQtyChanges' AS source_table,
                CAST(Day1 AS date) AS prior_date,
                CAST(Day2 AS date) AS snapshot_date,
                UPPER(LTRIM(RTRIM(CAST(SKU AS varchar(100))))) AS sku,
                UPPER(LTRIM(RTRIM(CAST(Manufacturer AS varchar(255))))) AS manufacturer,
                CAST(OldQty AS float) AS old_qty,
                CAST(NewQty AS float) AS new_qty,
                CAST(NULL AS float) AS old_price,
                CAST(NULL AS float) AS new_price,
                CAST(NULL AS varchar(50)) AS month_label
            FROM dbo.ECQtyChanges
            WHERE Day2 IS NOT NULL
              AND Day2 <= {today_filter}
              AND SKU IS NOT NULL
              AND LTRIM(RTRIM(CAST(SKU AS varchar(100)))) != ''
        """,
        "Msupply": f"""
            SELECT {top}
                'Msupply' AS source_vendor,
                'dbo.MsupplyQTYChanges' AS source_table,
                CAST(Day1 AS date) AS prior_date,
                CAST(Day2 AS date) AS snapshot_date,
                UPPER(LTRIM(RTRIM(CAST(SKU AS varchar(100))))) AS sku,
                UPPER(LTRIM(RTRIM(CAST(Manufacturer AS varchar(255))))) AS manufacturer,
                CAST(OldQty AS float) AS old_qty,
                CAST(NewQty AS float) AS new_qty,
                CAST(NULL AS float) AS old_price,
                CAST(NULL AS float) AS new_price,
                CAST(NULL AS varchar(50)) AS month_label
            FROM dbo.MsupplyQTYChanges
            WHERE Day2 IS NOT NULL
              AND Day2 <= {today_filter}
              AND SKU IS NOT NULL
              AND LTRIM(RTRIM(CAST(SKU AS varchar(100)))) != ''
        """,
        "ReliableParts": f"""
            SELECT {top}
                'ReliableParts' AS source_vendor,
                'dbo.RPDailyQTYChanges' AS source_table,
                CAST(Day1 AS date) AS prior_date,
                CAST(Day2 AS date) AS snapshot_date,
                UPPER(LTRIM(RTRIM(CAST(SKU AS varchar(100))))) AS sku,
                UPPER(LTRIM(RTRIM(CAST(Manufacturer AS varchar(255))))) AS manufacturer,
                CAST(OldQty AS float) AS old_qty,
                CAST(NewQty AS float) AS new_qty,
                CAST(NULL AS float) AS old_price,
                CAST(NULL AS float) AS new_price,
                CAST(NULL AS varchar(50)) AS month_label
            FROM dbo.RPDailyQTYChanges
            WHERE Day2 IS NOT NULL
              AND Day2 <= {today_filter}
              AND SKU IS NOT NULL
              AND LTRIM(RTRIM(CAST(SKU AS varchar(100)))) != ''
        """,
    }


def sales_queries(limit=None):
    top = top_clause(limit)
    tables = {
        "PartsFE": "dbo.FESalesData",
        "PartsHNC": "dbo.HNCSalesData",
        "APZ": "dbo.APZSalesData",
        "WTB": "dbo.WTBSalesData",
    }
    queries = {}
    for store, table in tables.items():
        queries[store] = f"""
            SELECT {top}
                '{store}' AS source_store,
                '{table}' AS source_table,
                CAST(increment_id AS varchar(100)) AS order_id,
                CAST(CustomerEmail AS varchar(255)) AS customer_email,
                UPPER(LTRIM(RTRIM(CAST(SKU AS varchar(100))))) AS sku,
                CAST(Created_at AS datetime) AS created_at,
                CAST(qty_ordered AS float) AS qty_ordered,
                CAST(price AS float) AS price,
                CAST(region AS varchar(255)) AS region,
                CAST(postcode AS varchar(100)) AS postcode,
                CAST(company AS varchar(255)) AS company,
                CAST(CustomerFirstName AS varchar(255)) AS customer_first_name,
                CAST(CustomerLastName AS varchar(255)) AS customer_last_name,
                CAST(Phone AS varchar(100)) AS phone,
                CAST(Name AS varchar(1000)) AS product_name
            FROM {table}
            WHERE Created_at IS NOT NULL
              AND SKU IS NOT NULL
              AND LTRIM(RTRIM(CAST(SKU AS varchar(100)))) != ''
        """
    return queries


def inventory_queries(limit=None):
    top = top_clause(limit)
    sources = {
        "APZ": ("dbo.APZLatestData", False),
        "PartsHNC": ("dbo.HNCLatestData", True),
        "PartsFE": ("dbo.FELatestData", False),
        "WTB": ("dbo.WTBLatestData", True),
    }
    queries = {}
    for store, (table, has_weight) in sources.items():
        weight = "CAST(Weight AS float)" if has_weight else "CAST(NULL AS float)"
        queries[store] = f"""
            SELECT {top}
                '{store}' AS source_store,
                '{table}' AS source_table,
                UPPER(LTRIM(RTRIM(CAST(SKU AS varchar(100))))) AS sku,
                UPPER(LTRIM(RTRIM(CAST(Manufacturer AS varchar(255)))))
                    AS manufacturer,
                CAST(Quantity AS float) AS quantity,
                CAST(Price AS float) AS price,
                CAST(ListPrice AS float) AS list_price,
                {weight} AS weight
            FROM {table}
            WHERE SKU IS NOT NULL
              AND LTRIM(RTRIM(CAST(SKU AS varchar(100)))) != ''
        """
    return queries


def merchant_catalog_queries(limit=None):
    top = top_clause(limit)
    sources = {
        "APZ": ("dbo.APZMcFeed", "name", False),
        "PartsHNC": ("dbo.HNCMCFeed", "name", False),
        "PartsFE": ("dbo.FEMCDataFeed", "name", False),
        "WTB": ("dbo.WTBMCFeed", "title", True),
    }
    queries = {}
    for store, (table, product_name, is_wtb) in sources.items():
        sale_price = "CAST(sale_price AS float)" if is_wtb else "CAST(NULL AS float)"
        shipment_weight = (
            "CAST(shipment_weight AS float)" if is_wtb else "CAST(NULL AS float)"
        )
        status_value = (
            "CAST(status_value AS int)"
            if table == "dbo.FEMCDataFeed"
            else "CAST(NULL AS int)"
        )
        product_detail = (
            "CAST(product_detail AS varchar(max))"
            if is_wtb
            else "CAST(NULL AS varchar(max))"
        )
        custom_label = (
            "CAST(custom_label_0 AS varchar(255))"
            if is_wtb
            else "CAST(NULL AS varchar(255))"
        )
        shipping = (
            "CAST(shipping AS varchar(max))"
            if is_wtb
            else "CAST(NULL AS varchar(max))"
        )
        queries[store] = f"""
            SELECT {top}
                '{store}' AS source_store,
                '{table}' AS source_table,
                UPPER(LTRIM(RTRIM(CAST(id AS varchar(100))))) AS sku,
                UPPER(LTRIM(RTRIM(CAST(mpn AS varchar(100))))) AS mpn,
                CAST({product_name} AS varchar(1000)) AS product_name,
                CAST(description AS varchar(max)) AS description,
                CAST(link AS varchar(2000)) AS product_link,
                CAST(image_link AS varchar(2000)) AS image_link,
                CAST(price AS float) AS price,
                {sale_price} AS sale_price,
                UPPER(LTRIM(RTRIM(CAST(brand AS varchar(255))))) AS brand,
                UPPER(LTRIM(RTRIM(CAST(availability AS varchar(100)))))
                    AS availability,
                UPPER(LTRIM(RTRIM(CAST(condition AS varchar(100))))) AS condition,
                CAST(google_product_category AS varchar(255))
                    AS google_product_category,
                CAST(product_type AS varchar(1000)) AS product_type,
                {shipment_weight} AS shipment_weight,
                {status_value} AS status_value,
                {product_detail} AS product_detail,
                {custom_label} AS custom_label_0,
                {shipping} AS shipping
            FROM {table}
            WHERE id IS NOT NULL
              AND LTRIM(RTRIM(CAST(id AS varchar(100)))) != ''
        """
    return queries


def write_copy_buffer(df, columns):
    buffer = io.StringIO()
    df[columns].to_csv(buffer, index=False, header=False, na_rep="", lineterminator="\n")
    buffer.seek(0)
    return buffer


def read_query_chunks(sql_conn, query, chunk_size):
    cursor = sql_conn.cursor()
    try:
        cursor.execute(query)
        column_names = [column[0] for column in cursor.description]
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            yield pd.DataFrame.from_records(rows, columns=column_names)
    finally:
        cursor.close()


def copy_dataframe(pg_conn, df, target_table, columns):
    if df.empty:
        return 0
    copy_sql = f"""
        COPY {target_table} ({", ".join(columns)})
        FROM STDIN WITH (FORMAT CSV, NULL '')
    """
    with pg_conn.cursor() as cur:
        cur.copy_expert(copy_sql, write_copy_buffer(df, columns))
    return len(df)


def log_run(pg_conn, source_name, source_table, target_table, rows_loaded, started_at, status, error_message=None):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit.ingestion_runs
                (source_name, source_table, target_table, rows_loaded, started_at, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (source_name, source_table, target_table, rows_loaded, started_at, status, error_message),
        )


def ingest_query(
    sql_conn,
    pg_conn,
    source_name,
    query,
    target_table,
    staging_table,
    source_column,
    columns,
    chunk_size,
    publish,
):
    started_at = datetime.now(timezone.utc)
    total = 0
    source_table = ""
    try:
        pg_conn.rollback()
        with pg_conn.cursor() as cur:
            cur.execute(f"TRUNCATE {staging_table};")

        for chunk in read_query_chunks(sql_conn, query, chunk_size):
            if chunk.empty:
                continue
            source_table = str(chunk["source_table"].iloc[0])
            total += copy_dataframe(pg_conn, chunk, staging_table, columns)
            print(f"{source_name}: staged {total:,} rows")

        if not publish:
            pg_conn.rollback()
            log_run(
                pg_conn,
                source_name,
                source_table or source_name,
                staging_table,
                total,
                started_at,
                "smoke_success",
            )
            pg_conn.commit()
            return total

        if total == 0:
            raise RuntimeError(
                f"{source_name} returned zero rows; existing production data was preserved."
            )

        column_list = ", ".join(columns)
        with pg_conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {target_table} WHERE {source_column} = %s",
                (source_name,),
            )
            cur.execute(
                f"""
                INSERT INTO {target_table} ({column_list})
                SELECT {column_list}
                FROM {staging_table}
                """
            )
            cur.execute(f"TRUNCATE {staging_table};")

        log_run(
            pg_conn,
            source_name,
            source_table,
            target_table,
            total,
            started_at,
            "success",
        )
        pg_conn.commit()
        print(f"{source_name}: atomically published {total:,} rows")
        return total
    except Exception as exc:
        pg_conn.rollback()
        log_run(
            pg_conn,
            source_name,
            source_table or source_name,
            target_table,
            total,
            started_at,
            "failed",
            str(exc),
        )
        pg_conn.commit()
        raise


def verify_counts(pg_conn):
    queries = {
        "raw.vendor_deltas": "SELECT source_vendor, COUNT(*) FROM raw.vendor_deltas GROUP BY source_vendor ORDER BY source_vendor",
        "raw.sales_orders": "SELECT source_store, COUNT(*) FROM raw.sales_orders GROUP BY source_store ORDER BY source_store",
        "raw.current_inventory": "SELECT source_store, COUNT(*) FROM raw.current_inventory GROUP BY source_store ORDER BY source_store",
        "raw.merchant_catalog": "SELECT source_store, COUNT(*) FROM raw.merchant_catalog GROUP BY source_store ORDER BY source_store",
        "audit.ingestion_runs": "SELECT source_name, rows_loaded, status FROM audit.ingestion_runs ORDER BY run_id",
    }
    with pg_conn.cursor() as cur:
        for label, query in queries.items():
            print(f"\n{label}")
            cur.execute(query)
            for row in cur.fetchall():
                print(row)


def main():
    parser = argparse.ArgumentParser(description="Ingest Parts SQL Server data into local PostgreSQL.")
    parser.add_argument("--mode", choices=["smoke", "full"], default="smoke")
    parser.add_argument(
        "--only",
        choices=["vendors", "sales", "catalog", "all"],
        default="all",
    )
    parser.add_argument("--limit", type=int, default=None, help="Rows per source. Defaults to 1000 in smoke mode.")
    parser.add_argument("--chunk-size", type=int, default=50000)
    args = parser.parse_args()

    limit = args.limit
    if args.mode == "smoke" and limit is None:
        limit = 1000

    publish = args.mode == "full"
    sql_conn = sql_server_conn()
    pg_conn = postgres_conn()
    lock_acquired = False
    try:
        ensure_schema(pg_conn)
        acquire_ingestion_lock(pg_conn)
        lock_acquired = True

        total_rows = 0
        if args.only in ("vendors", "all"):
            for source, query in vendor_queries(limit).items():
                total_rows += ingest_query(
                    sql_conn,
                    pg_conn,
                    source,
                    query,
                    "raw.vendor_deltas",
                    "staging.vendor_deltas",
                    "source_vendor",
                    VENDOR_COLUMNS,
                    args.chunk_size,
                    publish,
                )

        if args.only in ("sales", "all"):
            for source, query in sales_queries(limit).items():
                total_rows += ingest_query(
                    sql_conn,
                    pg_conn,
                    source,
                    query,
                    "raw.sales_orders",
                    "staging.sales_orders",
                    "source_store",
                    SALES_COLUMNS,
                    args.chunk_size,
                    publish,
                )

        if args.only in ("catalog", "all"):
            for source, query in inventory_queries(limit).items():
                total_rows += ingest_query(
                    sql_conn,
                    pg_conn,
                    source,
                    query,
                    "raw.current_inventory",
                    "staging.current_inventory",
                    "source_store",
                    INVENTORY_COLUMNS,
                    args.chunk_size,
                    publish,
                )
            for source, query in merchant_catalog_queries(limit).items():
                total_rows += ingest_query(
                    sql_conn,
                    pg_conn,
                    source,
                    query,
                    "raw.merchant_catalog",
                    "staging.merchant_catalog",
                    "source_store",
                    MERCHANT_CATALOG_COLUMNS,
                    args.chunk_size,
                    publish,
                )

        action = "published" if publish else "validated in smoke mode"
        print(f"\nTotal rows {action}: {total_rows:,}")
        verify_counts(pg_conn)
    finally:
        if lock_acquired:
            release_ingestion_lock(pg_conn)
        sql_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
