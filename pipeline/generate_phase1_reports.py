import os
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
POSTGRES = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5433")),
    "database": os.getenv("PGDATABASE", "vendor_intelligence"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD"),
}

REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"
PIPELINE_VERSION = "1.0.0"
KPI_SPEC_VERSION = "1.0"
TRANSFORMATION_LOCK_ID = 8429157302


def get_pg_conn():
    if not POSTGRES["password"]:
        raise RuntimeError("Missing required environment variable: PGPASSWORD")

    return psycopg2.connect(
        host=POSTGRES["host"],
        port=POSTGRES["port"],
        dbname=POSTGRES["database"],
        user=POSTGRES["user"],
        password=POSTGRES["password"],
    )


def acquire_transformation_lock(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_try_advisory_lock(%s)",
            (TRANSFORMATION_LOCK_ID,),
        )
        acquired = cur.fetchone()[0]
    conn.commit()
    if not acquired:
        raise RuntimeError("Another analytics transformation run is active.")


def release_transformation_lock(conn):
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_unlock(%s)",
            (TRANSFORMATION_LOCK_ID,),
        )
    conn.commit()


def ensure_transformation_audit_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE SCHEMA IF NOT EXISTS audit;
            CREATE TABLE IF NOT EXISTS audit.transformation_runs (
                run_id UUID PRIMARY KEY,
                pipeline_version TEXT NOT NULL,
                kpi_spec_version TEXT NOT NULL,
                data_cutoff_date DATE NOT NULL,
                source_loaded_at TIMESTAMPTZ,
                transformation_started_at TIMESTAMPTZ NOT NULL,
                transformation_completed_at TIMESTAMPTZ,
                row_counts JSONB,
                status TEXT NOT NULL CHECK (
                    status IN ('RUNNING', 'COMPLETED', 'FAILED')
                ),
                error_message TEXT
            )
            """
        )
    conn.commit()


def start_transformation_run(conn):
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT GREATEST(
                (SELECT MAX(ingested_at) FROM raw.vendor_deltas),
                (SELECT MAX(ingested_at) FROM raw.sales_orders),
                (SELECT MAX(ingested_at) FROM raw.current_inventory),
                (SELECT MAX(ingested_at) FROM raw.merchant_catalog)
            )
            """
        )
        source_loaded_at = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO audit.transformation_runs (
                run_id,
                pipeline_version,
                kpi_spec_version,
                data_cutoff_date,
                source_loaded_at,
                transformation_started_at,
                status
            ) VALUES (%s, %s, %s, %s, %s, %s, 'RUNNING')
            """,
            (
                run_id,
                PIPELINE_VERSION,
                KPI_SPEC_VERSION,
                date.today(),
                source_loaded_at,
                started_at,
            ),
        )
    conn.commit()
    return run_id


def complete_transformation_run(conn, run_id):
    row_counts = {}
    relations = (
        "clean.vendor_deltas",
        "clean.sales_orders",
        "clean.current_inventory",
        "clean.merchant_catalog",
        "analytics.sku_phase1_kpis",
        "analytics.sku_catalog_current",
        "analytics.sku_monthly_metrics",
        "analytics.monthly_trend_summary",
    )
    with conn.cursor() as cur:
        for relation in relations:
            cur.execute(f"SELECT COUNT(*) FROM {relation}")
            row_counts[relation] = cur.fetchone()[0]
        cur.execute(
            """
            UPDATE audit.transformation_runs
            SET transformation_completed_at = %s,
                row_counts = %s,
                status = 'COMPLETED'
            WHERE run_id = %s
            """,
            (datetime.now(timezone.utc), Json(row_counts), run_id),
        )
    conn.commit()


def fail_transformation_run(conn, run_id, error):
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE audit.transformation_runs
            SET transformation_completed_at = %s,
                status = 'FAILED',
                error_message = %s
            WHERE run_id = %s
            """,
            (datetime.now(timezone.utc), str(error), run_id),
        )
    conn.commit()


def run_sql_files(conn, sql_paths):
    with conn.cursor() as cur:
        for sql_path in sql_paths:
            print(f"Executing {sql_path.name}...")
            cur.execute(sql_path.read_text(encoding="utf-8"))
    conn.commit()
    print("Analytics tables and Hecta views published atomically.")


def export_reports(conn):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    queries = {
        "preliminary_top_200_focus_parts.csv": """
            SELECT * FROM analytics.preliminary_top_200_focus_parts
            ORDER BY rank ASC;
        """,
        "preliminary_movement_classification.csv": """
            SELECT 
                preliminary_movement_class,
                COUNT(*) AS sku_count,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS sku_pct,
                SUM(valid_depleted_units) AS valid_depleted_units,
                ROUND(
                    100.0 * SUM(valid_depleted_units)
                    / NULLIF(SUM(SUM(valid_depleted_units)) OVER (), 0),
                    2
                ) AS volume_pct,
                ROUND(AVG(depletion_events), 1) AS avg_depletion_events,
                ROUND(AVG(preliminary_daily_depletion_rate), 4) AS avg_preliminary_daily_depletion_rate,
                MAX(kpi_spec_version) AS kpi_spec_version,
                MAX(data_cutoff_date) AS data_cutoff_date
            FROM analytics.sku_vendor_summary
            GROUP BY preliminary_movement_class
            ORDER BY valid_depleted_units DESC;
        """,
        "preliminary_movement_vs_sales_gap.csv": """
            SELECT *
            FROM analytics.preliminary_movement_vs_sales_gap
            ORDER BY valid_depleted_units DESC, sku
            LIMIT 500;
        """,
        "no_observed_movement_candidates.csv": """
            SELECT *
            FROM analytics.no_observed_movement_candidates
            ORDER BY historical_vendor_count DESC, sku
            LIMIT 1000;
        """,
        "preliminary_category_summary.csv": """
            SELECT * FROM analytics.preliminary_category_summary;
        """,
        "data_quality_summary.csv": """
            SELECT *
            FROM analytics.data_quality_summary
            ORDER BY data_area, metric;
        """,
    }

    print("\nExporting Phase 1 Deliverables:")
    for filename, query in queries.items():
        out_path = REPORTS_DIR / filename
        copy_query = query.strip().rstrip(";")
        with out_path.open("w", encoding="utf-8", newline="") as output:
            with conn.cursor() as cur:
                cur.copy_expert(
                    f"COPY ({copy_query}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)",
                    output,
                )
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM ({copy_query}) AS report_rows")
            row_count = cur.fetchone()[0]
        print(f" -> Exported {row_count:,} rows to {out_path.name}")

    obsolete_reports = [
        "top_200_focus_parts.csv",
        "fsn_classification_matrix.csv",
        "movement_vs_sales_gap_opportunities.csv",
        "dead_stock_demotion_list.csv",
        "category_segment_summary.csv",
    ]
    for filename in obsolete_reports:
        path = REPORTS_DIR / filename
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            print(f" -> Warning: could not remove obsolete report {filename}: {exc}")


def print_summary(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM analytics.sku_master_intelligence;")
        total_skus = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*)
            FROM analytics.sku_master_intelligence
            WHERE preliminary_priority_score >= 90
            """
        )
        preliminary_focus = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*)
            FROM analytics.sku_master_intelligence
            WHERE preliminary_gap_class = 'HIGH_GAP_CANDIDATE'
            """
        )
        high_gap_candidates = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM analytics.no_observed_movement_candidates;")
        no_movement_candidates = cur.fetchone()[0]

    print("\n" + "=" * 60)
    print("PHASE 1 ANALYTICS SUMMARY (SPEC v1.0)")
    print("=" * 60)
    print(f"Total unique SKUs analyzed:            {total_skus:,}")
    print(f"Preliminary score >= 90:               {preliminary_focus:,}")
    print(f"High gap candidates:                   {high_gap_candidates:,}")
    print(f"No observed movement candidates:       {no_movement_candidates:,}")
    print("Business approval status:              APPROVED FOR REVIEWED DECISIONS")
    print("=" * 60)


def main():
    conn = get_pg_conn()
    run_id = None
    lock_acquired = False
    try:
        acquire_transformation_lock(conn)
        lock_acquired = True
        ensure_transformation_audit_table(conn)
        run_id = start_transformation_run(conn)
        sql_dir = Path(__file__).resolve().parent / "sql"
        run_sql_files(
            conn,
            (
                sql_dir / "clean_and_kpis.sql",
                sql_dir / "hecta_ready_views.sql",
            ),
        )
        export_reports(conn)
        print_summary(conn)
        complete_transformation_run(conn, run_id)
        print(f"Transformation run ID:                {run_id}")
    except Exception as exc:
        if run_id:
            fail_transformation_run(conn, run_id, exc)
        raise
    finally:
        if lock_acquired:
            release_transformation_lock(conn)
        conn.close()


if __name__ == "__main__":
    main()
