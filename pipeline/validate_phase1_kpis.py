from pathlib import Path

from generate_phase1_reports import get_pg_conn
from psycopg2 import sql

PIPELINE_DIR = Path(__file__).resolve().parent
VALIDATION_SQL = PIPELINE_DIR / "sql" / "validate_phase1_sample.sql"
VALIDATION_REPORTS_DIR = PIPELINE_DIR.parent / "reports" / "validation"

HECTA_REQUIRED_FIELDS = {
    "daily_command_center": (
        "data_cutoff_date",
        "total_skus",
        "kpi_spec_version",
    ),
    "sku_intelligence": (
        "sku",
        "category_segment",
        "category_is_authoritative",
        "current_inventory_status",
        "current_catalog_status",
        "kpi_spec_version",
        "data_cutoff_date",
    ),
    "ad_priority_candidates": (
        "rank",
        "sku",
        "category_segment",
        "current_inventory_status",
        "kpi_spec_version",
        "data_cutoff_date",
    ),
    "movement_vs_sales_gap": (
        "sku",
        "category_segment",
        "gap_class",
        "kpi_spec_version",
        "data_cutoff_date",
    ),
    "catalog_visibility_candidates": (
        "sku",
        "category_segment",
        "current_inventory_status",
        "current_catalog_status",
        "kpi_spec_version",
        "data_cutoff_date",
    ),
    "monthly_trends": (
        "month_start",
        "category_segment",
        "category_is_authoritative",
        "kpi_spec_version",
    ),
    "sales_daily": (
        "sale_date",
        "source_store",
        "kpi_spec_version",
        "data_cutoff_date",
        "usage_limitation",
    ),
    "sales_weekly": (
        "week_start",
        "source_store",
        "kpi_spec_version",
        "data_cutoff_date",
        "usage_limitation",
    ),
    "sales_monthly": (
        "month_start",
        "source_store",
        "kpi_spec_version",
        "data_cutoff_date",
        "usage_limitation",
    ),
    "source_health": (
        "source_type",
        "source_name",
        "latest_record_at",
        "freshness_basis",
        "freshness_days",
        "freshness_status",
        "source_freshness_score",
        "kpi_spec_version",
        "data_cutoff_date",
    ),
    "data_quality_health": (
        "source_vendor",
        "raw_rows",
        "clean_rows",
        "data_quality_risk_score",
        "kpi_spec_version",
        "data_cutoff_date",
    ),
    "current_catalog": (
        "sku",
        "current_inventory_status",
        "current_catalog_status",
        "inventory_quantity_basis",
        "source_loaded_at",
        "data_cutoff_date",
    ),
    "pricing_review_candidates": (
        "sku",
        "current_inventory_status",
        "data_cutoff_date",
        "usage_limitation",
    ),
    "column_quality_profile": (
        "schema_name",
        "table_name",
        "source_name",
        "column_name",
        "total_rows",
        "missing_rows",
        "missing_pct",
        "null_policy",
        "kpi_impact",
        "trust_status",
    ),
}


def find_hecta_required_field_failures(cur):
    failures = []
    for view_name, columns in HECTA_REQUIRED_FIELDS.items():
        expressions = [
            sql.SQL(
                "COUNT(*) FILTER (WHERE {column} IS NULL "
                "OR BTRIM({column}::text) = '')"
            ).format(column=sql.Identifier(column_name))
            for column_name in columns
        ]
        cur.execute(
            sql.SQL("SELECT {} FROM analytics_hecta.{}").format(
                sql.SQL(", ").join(expressions),
                sql.Identifier(view_name),
            )
        )
        counts = cur.fetchone()
        failures.extend(
            (view_name, column_name, null_count)
            for column_name, null_count in zip(columns, counts)
            if null_count
        )
    return failures


def run_validation_sql(conn):
    sql = VALIDATION_SQL.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def export_query(conn, query, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    copy_query = query.strip().rstrip(";")
    with output_path.open("w", encoding="utf-8", newline="") as output:
        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY ({copy_query}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)",
                output,
            )


def validate_results(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM analytics.phase1_validation_sample")
        sample_count = cur.fetchone()[0]

        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE internal_reconciliation_pass),
                COUNT(*) FILTER (WHERE NOT internal_reconciliation_pass)
            FROM analytics.phase1_sample_reconciliation
            """
        )
        passed, failed = cur.fetchone()

        cur.execute(
            """
            WITH expected AS (
                SELECT
                    r.source_vendor,
                    COUNT(*) FILTER (
                        WHERE r.old_qty IS NULL
                           OR r.new_qty IS NULL
                           OR r.old_qty < 0
                           OR r.new_qty < 0
                    ) AS invalid_quantity_rows,
                    COUNT(*) FILTER (
                        WHERE r.old_qty IS NULL
                           OR r.new_qty IS NULL
                           OR r.old_qty < 0
                           OR r.new_qty < 0
                           OR r.prior_date > CURRENT_DATE
                           OR r.prior_date > r.snapshot_date
                    ) AS invalid_quantity_date_rows,
                    ROUND(
                        100.0 * COUNT(*) FILTER (
                            WHERE r.old_qty IS NOT NULL
                              AND r.new_qty IS NOT NULL
                              AND r.old_qty >= 0
                              AND r.new_qty >= 0
                        ) / NULLIF(COUNT(*), 0),
                        4
                    ) AS quantity_change_consistency
                FROM raw.vendor_deltas r
                GROUP BY r.source_vendor
            )
            SELECT COUNT(*)
            FROM expected e
            JOIN analytics.source_quality_kpis q USING (source_vendor)
            WHERE e.invalid_quantity_rows <> q.invalid_quantity_rows
               OR e.invalid_quantity_date_rows <> q.invalid_quantity_date_rows
               OR e.quantity_change_consistency <> q.quantity_change_consistency
               OR q.duplicate_sku_record_rate NOT BETWEEN 0 AND 100
               OR q.null_manufacturer_rate NOT BETWEEN 0 AND 100
               OR q.invalid_quantity_date_rate NOT BETWEEN 0 AND 100
               OR q.quantity_change_consistency NOT BETWEEN 0 AND 100
               OR q.data_quality_risk_score NOT BETWEEN 0 AND 100
            """
        )
        quality_failures = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*)
            FROM (
                VALUES
                    ('daily_command_center'),
                    ('sku_intelligence'),
                    ('ad_priority_candidates'),
                    ('movement_vs_sales_gap'),
                    ('catalog_visibility_candidates'),
                    ('monthly_trends'),
                    ('source_health'),
                    ('data_quality_health'),
                    ('current_catalog'),
                    ('pricing_review_candidates'),
                    ('column_quality_profile'),
                    ('sales_daily'),
                    ('sales_weekly'),
                    ('sales_monthly')
            ) AS expected(table_name)
            LEFT JOIN information_schema.views v
              ON v.table_schema = 'analytics_hecta'
             AND v.table_name = expected.table_name
            WHERE v.table_name IS NULL
            """
        )
        missing_hecta_views = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = 'analytics_hecta'
              AND column_name IN (
                  'customer_email',
                  'postcode',
                  'company',
                  'customer_first_name',
                  'customer_last_name',
                  'phone'
              )
            """
        )
        hecta_pii_columns = cur.fetchone()[0]

        cur.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM analytics_hecta.sku_intelligence)
                    = (SELECT COUNT(*) FROM analytics.sku_phase1_kpis),
                NOT EXISTS (
                    SELECT sku
                    FROM analytics_hecta.sku_intelligence
                    GROUP BY sku
                    HAVING COUNT(*) > 1
                ),
                NOT EXISTS (
                    SELECT 1
                    FROM analytics_hecta.sku_intelligence
                    WHERE sku IS NULL
                       OR BTRIM(sku) = ''
                       OR data_cutoff_date IS NULL
                       OR kpi_spec_version IS NULL
                ),
                (SELECT COUNT(*) FROM analytics_hecta.daily_command_center) = 1,
                (SELECT COUNT(*) FROM analytics_hecta.monthly_trends)
                    = (
                        SELECT COUNT(*)
                        FROM analytics.monthly_trend_summary_catalog
                    ),
                (SELECT COUNT(*) FROM analytics_hecta.sales_daily)
                    = (
                        SELECT COUNT(*)
                        FROM analytics.sales_period_summary
                        WHERE period_grain = 'DAY'
                    ),
                (SELECT COUNT(*) FROM analytics_hecta.sales_weekly)
                    = (
                        SELECT COUNT(*)
                        FROM analytics.sales_period_summary
                        WHERE period_grain = 'WEEK'
                    ),
                (SELECT COUNT(*) FROM analytics_hecta.sales_monthly)
                    = (
                        SELECT COUNT(*)
                        FROM analytics.sales_period_summary
                        WHERE period_grain = 'MONTH'
                    ),
                (SELECT COUNT(*) FROM analytics_hecta.current_catalog)
                    = (SELECT COUNT(*) FROM analytics.sku_catalog_current),
                NOT EXISTS (
                    SELECT sku
                    FROM analytics_hecta.current_catalog
                    GROUP BY sku
                    HAVING COUNT(*) > 1
                ),
                NOT EXISTS (
                    SELECT 1
                    FROM analytics_hecta.column_quality_profile
                    WHERE column_name IN (
                        'customer_email',
                        'postcode',
                        'company',
                        'customer_first_name',
                        'customer_last_name',
                        'phone'
                    )
                ),
                NOT EXISTS (
                    SELECT 1
                    FROM analytics_hecta.sku_intelligence
                    WHERE historical_valid_depleted_units > 0
                      AND historical_valid_depleted_units
                          > historical_peak_observed_quantity
                      AND depletion_credibility_flag
                          NOT IN ('LIKELY_RECOUNTED', 'SEVERE_RECOUNT')
                ),
                NOT EXISTS (
                    SELECT 1
                    FROM analytics_hecta.sku_intelligence
                    WHERE historical_valid_depleted_units > 0
                      AND depletion_credibility_flag IS NULL
                )
            """
        )
        hecta_contract_checks = cur.fetchone()
        hecta_contract_failures = sum(not check for check in hecta_contract_checks)
        required_field_failures = (
            find_hecta_required_field_failures(cur)
            if missing_hecta_views == 0
            else []
        )

    if sample_count != 20:
        raise RuntimeError(f"Expected 20 validation SKUs, found {sample_count}.")
    if failed:
        raise RuntimeError(
            f"Internal reconciliation failed for {failed} of {sample_count} SKUs."
        )
    if quality_failures:
        raise RuntimeError(
            f"Data-quality formula validation failed for {quality_failures} sources."
        )
    if missing_hecta_views:
        raise RuntimeError(f"Missing {missing_hecta_views} required Hecta views.")
    if hecta_pii_columns:
        raise RuntimeError(
            f"Hecta schema exposes {hecta_pii_columns} prohibited PII columns."
        )
    if hecta_contract_failures:
        raise RuntimeError(
            f"Hecta view-contract validation failed {hecta_contract_failures} checks."
        )
    if required_field_failures:
        details = ", ".join(
            f"{view}.{column}={null_count}"
            for view, column, null_count in required_field_failures
        )
        raise RuntimeError(f"Hecta required-field validation failed: {details}")

    return (
        sample_count,
        passed,
        failed,
        quality_failures,
        missing_hecta_views,
        hecta_pii_columns,
        hecta_contract_failures,
        len(required_field_failures),
    )


def main():
    conn = get_pg_conn()
    try:
        run_validation_sql(conn)
        (
            sample_count,
            passed,
            failed,
            quality_failures,
            missing_hecta_views,
            hecta_pii_columns,
            hecta_contract_failures,
            required_field_failures,
        ) = validate_results(conn)

        export_query(
            conn,
            """
            SELECT *
            FROM analytics.phase1_validation_sample
            ORDER BY sample_priority, sku
            """,
            VALIDATION_REPORTS_DIR / "phase1_validation_sample.csv",
        )
        export_query(
            conn,
            """
            SELECT *
            FROM analytics.phase1_sample_reconciliation
            ORDER BY sample_priority, sku
            """,
            VALIDATION_REPORTS_DIR / "phase1_internal_reconciliation.csv",
        )
    finally:
        conn.close()

    print("=" * 60)
    print("PHASE 1 KPI INTERNAL VALIDATION")
    print("=" * 60)
    print(f"Validation sample:              {sample_count}")
    print(f"Internal reconciliation pass:  {passed}")
    print(f"Internal reconciliation fail:  {failed}")
    print(f"Quality formula failures:      {quality_failures}")
    print(f"Missing Hecta views:           {missing_hecta_views}")
    print(f"Hecta PII columns:             {hecta_pii_columns}")
    print(f"Hecta contract failures:       {hecta_contract_failures}")
    print(f"Hecta required-field failures: {required_field_failures}")
    print("SQL Server source review:       SEE SOURCE RECONCILIATION REPORT")
    print("=" * 60)


if __name__ == "__main__":
    main()
