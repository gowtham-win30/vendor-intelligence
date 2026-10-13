from pathlib import Path

from generate_phase1_reports import get_pg_conn

OUTPUT_PATH = (
    Path(__file__).resolve().parent.parent / "docs" / "hecta_data_catalog_import.sql"
)

TABLE_DESCRIPTIONS = {
    "daily_command_center": "One-row executive summary of the latest validated Vendor Intelligence analytical state.",
    "sku_intelligence": "One row per normalized SKU with historical movement, sales, current catalog context, and review scores.",
    "ad_priority_candidates": "Ranked SKU candidates for analyst review when planning advertising or merchandising activity.",
    "movement_vs_sales_gap": "SKUs where historical vendor movement differs materially from observed storefront sales.",
    "catalog_visibility_candidates": "SKUs requiring analyst review for catalog presence, content, or merchandising visibility.",
    "monthly_trends": "Monthly category-level history of vendor movement and observed storefront sales signals.",
    "sales_daily": "Daily observed storefront sales by store or NETWORK. Sales only; not vendor depletion.",
    "sales_weekly": "Weekly observed storefront sales by store or NETWORK. Sales only; not vendor depletion.",
    "sales_monthly": "Monthly observed storefront sales by store or NETWORK. Sales only; not vendor depletion.",
    "source_health": "Freshness status and freshness score for each integrated source feed.",
    "data_quality_health": "Vendor-level data-quality counts, rates, freshness, and weighted risk indicators.",
    "current_catalog": "Current inventory and merchant-catalog context consolidated to one row per normalized SKU.",
    "pricing_review_candidates": "SKUs with observed retail-price spread or list-price differences requiring manual review.",
    "column_quality_profile": "Latest missing-value and trust profile by source table and column, excluding PII columns.",
}

COLUMN_DESCRIPTIONS = {
    "behavioral_classification": "Rule-based SKU behavior segment derived from historical movement and sales signals.",
    "brand": "Merchant-feed brand after normalization; may be absent when the source does not provide it.",
    "bulk_shock_flag": "True when a large isolated movement may distort recurring-demand interpretation.",
    "catalog_source_loaded_at": "Latest ingestion timestamp among current catalog or inventory records mapped to the SKU.",
    "catalog_store_count": "Number of integrated storefront catalogs currently containing the SKU.",
    "catalog_stores": "Delimited list of integrated storefront catalogs currently containing the SKU.",
    "category_is_authoritative": "True when category_segment is based on Google Product Category rather than inference.",
    "category_segment": "Business-facing product category, preferring Google Product Category when available.",
    "clean_rows": "Rows retained in the clean layer after validation and deduplication.",
    "column_name": "Profiled source or analytical column name.",
    "current_catalog_skus": "Number of normalized SKUs represented in the integrated current catalog.",
    "current_catalog_status": "Current merchant-catalog mapping status across integrated storefront feeds.",
    "current_inventory_quantity": "Largest non-negative on-hand quantity at one integrated storefront. Not network on-hand and not vendor QOH.",
    "network_inventory_quantity": "Sum of non-negative storefront quantities across integrated stores. Not a single warehouse balance.",
    "inventory_quantity_basis": "Explains that current_inventory_quantity is the max integrated store quantity, not a network on-hand total.",
    "is_non_product_sku": "True when the key is a fee, cancellation, or other non-product token excluded from action queues.",
    "current_inventory_status": "Current mapped inventory state such as IN_STOCK, OUT_OF_STOCK, or UNKNOWN_NOT_INTEGRATED.",
    "currently_in_stock_skus": "Number of mapped SKUs with positive current inventory.",
    "currently_out_of_stock_skus": "Number of mapped SKUs with zero current inventory.",
    "data_cutoff_date": "Business date through which the published analytical state is valid.",
    "data_quality_risk_score": "Weighted 0-to-100 source-quality risk indicator; higher values indicate greater review risk.",
    "data_type": "PostgreSQL data type of the profiled column.",
    "demand_consistency_score": "Normalized score representing how consistently demand signals recur over time.",
    "demand_pattern": "Rule-based demand pattern derived from recurrence, volume, and sales observations.",
    "depletion_credibility_flag": "Credibility label for cumulative depletion (PLAUSIBLE, LIKELY_RECOUNTED, SEVERE_RECOUNT, NO_DEPLETION, UNKNOWN); values above PLAUSIBLE indicate depletion re-counts a persistent standing gap and overstates true consumption.",
    "depletion_events": "Count of valid vendor quantity-reduction events in the month and category.",
    "depletion_to_peak_ratio": "Cumulative valid depleted units divided by the SKU's peak observed on-hand quantity; ratios above 1 indicate depletion re-counts a standing gap rather than true consumption.",
    "duplicate_sku_record_rate": "Percentage of raw records participating in repeated source-SKU observations.",
    "eligible_rows": "Raw rows eligible for the relevant quality calculation.",
    "exact_duplicate_rows_removed": "Exact duplicate source rows removed during cleaning.",
    "fresh_vendor_count": "Number of vendor sources for the SKU that meet the configured freshness threshold.",
    "freshness_basis": "Source timestamp or business date used to calculate freshness.",
    "freshness_days": "Calendar days between the source's latest record and the analytical cutoff date.",
    "freshness_status": "Rule-based source state such as FRESH, AGING, or STALE.",
    "gap_class": "Movement-versus-sales review category assigned to the SKU.",
    "google_product_category": "Google Product Category supplied by the merchant feed when available.",
    "has_catalog_conflict": "True when mapped current catalog records disagree on key attributes.",
    "has_conflicting_records": "True when current records for the SKU contain conflicting values.",
    "has_description": "True when at least one current merchant record contains a product description.",
    "has_image": "True when at least one current merchant record contains an image reference.",
    "has_product_link": "True when at least one current merchant record contains a product URL.",
    "high_gap_candidates": "Number of SKUs classified as high movement-versus-sales gaps.",
    "high_priority_skus": "Number of SKUs currently included in the high-priority analyst review group.",
    "historical_depletion_events": "Count of valid historical vendor quantity-reduction events for the SKU.",
    "historical_order_count": "Count of observed historical storefront orders containing the SKU.",
    "historical_stockout_transitions": "Count of valid historical vendor transitions from positive quantity to zero.",
    "historical_valid_depleted_units": "Cumulative valid vendor quantity reductions; a movement signal, not confirmed sales.",
    "historical_valid_revenue": "Cumulative observed storefront revenue in source-record currency, currently USD.",
    "historical_valid_units_sold": "Cumulative valid units sold through integrated storefront sales sources.",
    "historical_vendor_count": "Number of historical vendor sources with observations for the SKU.",
    "historical_peak_observed_quantity": "Highest single old or new quantity ever observed for the SKU across vendor snapshots; used as the plausibility ceiling for cumulative depletion.",
    "historical_restock_events": "Count of vendor snapshot rows where new quantity exceeds old quantity; near zero because replenishment occurs between snapshots and is not recorded at row level.",
    "invalid_prior_date_rows": "Rows where the prior date is in the future or later than the snapshot date.",
    "invalid_quantity_date_rate": "Percentage of rows with invalid quantity or date conditions, counted once per row.",
    "invalid_quantity_date_rows": "Distinct rows containing an invalid quantity or date condition.",
    "invalid_quantity_rows": "Distinct rows with missing or negative old or new quantity.",
    "inventory_store_count": "Number of integrated storefront inventory sources currently containing the SKU.",
    "kpi_impact": "KPI area potentially affected when the profiled field is missing.",
    "kpi_spec_version": "Version of the KPI specification used to produce the record.",
    "latest_record_at": "Latest available business timestamp or date for the source.",
    "manufacturer": "Normalized manufacturer name where supplied by the source.",
    "maximum_current_retail_price": "Highest current retail price observed across integrated merchant feeds.",
    "maximum_list_price": "Highest current list price observed across integrated merchant feeds.",
    "maximum_weight": "Highest product weight observed across mapped current merchant records.",
    "merchant_product_type": "Merchant-defined product type; not treated as authoritative business taxonomy.",
    "minimum_current_retail_price": "Lowest current retail price observed across integrated merchant feeds.",
    "missing_pct": "Percentage of profiled rows that are null or blank under the column policy.",
    "missing_rows": "Number of profiled rows that are null or blank under the column policy.",
    "month_start": "First calendar date of the aggregated month.",
    "movement_frequency_index": "Normalized score representing how frequently valid movement events recur.",
    "movement_sales_gap_class": "SKU-level category comparing historical vendor movement with observed storefront sales.",
    "multi_vendor_strength_score": "Normalized score representing breadth and freshness of vendor coverage.",
    "negative_quantity_rows": "Rows containing a negative old or new quantity.",
    "no_observed_movement_skus": "Number of SKUs with no qualifying movement in the observed historical window.",
    "null_manufacturer_rate": "Percentage of rows with a missing manufacturer value.",
    "null_manufacturer_rows": "Rows with a missing manufacturer value.",
    "null_policy": "Declared handling policy for missing values in the profiled column.",
    "null_quantity_rows": "Rows with a missing old or new quantity.",
    "observed_store_price_spread": "Difference between the maximum and minimum current retail prices across stores.",
    "distinct_skus_sold": "Count of distinct SKUs with a valid sale in the period and store.",
    "month_end": "Last calendar date of the aggregated month.",
    "orders": "Count of observed storefront orders in the period or category.",
    "period_end": "Inclusive last calendar date of the aggregated period.",
    "period_is_complete": "True when the period ended before the analytical cutoff date.",
    "sale_date": "Calendar date of observed storefront sales.",
    "source_store": "Storefront identifier or NETWORK for the cross-store sales rollup.",
    "week_end": "Sunday ending the ISO week.",
    "week_start": "Monday starting the ISO week.",
    "priority_score": "Composite historical review score; it is not an automated action recommendation.",
    "product_name": "Current merchant product title after normalization, where available.",
    "quantity_change_consistency": "Percentage of source rows with complete, non-negative old and new quantities.",
    "rank": "Current ordering of candidates by priority score within the published refresh.",
    "raw_rows": "Number of source records preserved in the raw layer.",
    "requires_analyst_review": "True because the record supports reviewed decision-making rather than autonomous action.",
    "sales_freshness_days": "Days since the SKU's latest observed storefront sale.",
    "schema_name": "PostgreSQL schema containing the profiled relation.",
    "sku": "Upper-cased, trimmed product stock-keeping unit used as the analytical key.",
    "source_freshness_score": "Normalized freshness score where higher values indicate a more current source.",
    "source_loaded_at": "Latest ingestion timestamp among current source records mapped to the SKU.",
    "source_name": "Integrated vendor, storefront, or logical source identifier.",
    "source_type": "Source grouping such as vendor movement, sales, inventory, or merchant catalog.",
    "source_vendor": "Normalized vendor source identifier.",
    "stale_sources": "Number of integrated sources currently classified as stale.",
    "stockout_transitions": "Count of valid positive-to-zero vendor quantity transitions in the month and category.",
    "store_count": "Number of storefront sales sources with historical observations for the SKU.",
    "table_name": "Profiled relation name.",
    "total_rows": "Total rows evaluated for the column profile.",
    "total_skus": "Number of normalized SKUs in the published analytical population.",
    "trust_status": "Derived trust classification based on missing values and the declared null policy.",
    "usage_limitation": "Human-readable restriction describing how the record must not be interpreted or automated.",
    "valid_depleted_units": "Valid vendor quantity reductions in the month and category; not confirmed sales.",
    "valid_revenue": "Observed storefront revenue in the month and category, currently interpreted as USD.",
    "valid_units_sold": "Valid storefront units sold in the month and category.",
    "vendor_dependency_risk": "Rule-based risk category indicating dependence on limited vendor coverage.",
    "vendor_freshness_days": "Days since the SKU or source's latest valid vendor observation.",
    "vendor_freshness_index": "Normalized vendor freshness score where higher values indicate fresher data.",
}

TYPE_MAP = {
    "bool": "BOOLEAN",
    "date": "DATE",
    "float4": "REAL",
    "float8": "DOUBLE PRECISION",
    "int2": "SMALLINT",
    "int4": "INTEGER",
    "int8": "BIGINT",
    "numeric": "NUMERIC",
    "text": "TEXT",
    "timestamp": "TIMESTAMP",
    "timestamptz": "TIMESTAMPTZ",
    "varchar": "VARCHAR",
}

RELATIONSHIPS = {
    ("ad_priority_candidates", "sku"): ("sku_intelligence", "sku"),
    ("movement_vs_sales_gap", "sku"): ("sku_intelligence", "sku"),
    ("catalog_visibility_candidates", "sku"): ("sku_intelligence", "sku"),
    ("pricing_review_candidates", "sku"): ("current_catalog", "sku"),
}


def sql_literal(value):
    return "'" + value.replace("'", "''") + "'"


def export_catalog_ddl():
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name, column_name, udt_name
                FROM information_schema.columns
                WHERE table_schema = 'analytics_hecta'
                ORDER BY table_name, ordinal_position
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    columns_by_table = {}
    for table_name, column_name, udt_name in rows:
        columns_by_table.setdefault(table_name, []).append((column_name, udt_name))

    missing_tables = set(columns_by_table) - set(TABLE_DESCRIPTIONS)
    missing_columns = {
        column_name
        for columns in columns_by_table.values()
        for column_name, _ in columns
        if column_name not in COLUMN_DESCRIPTIONS
    }
    if missing_tables or missing_columns:
        raise RuntimeError(
            f"Missing catalog descriptions: tables={sorted(missing_tables)}, "
            f"columns={sorted(missing_columns)}"
        )

    statements = [
        "-- Metadata-only DDL for Hecta Data Catalog import.",
        "-- Do not execute this file against PostgreSQL.",
        "",
    ]
    for table_name in TABLE_DESCRIPTIONS:
        columns = columns_by_table.get(table_name)
        if not columns:
            raise RuntimeError(f"Required Hecta view is missing: {table_name}")

        statements.append(f"CREATE TABLE analytics_hecta.{table_name} (")
        for index, (column_name, udt_name) in enumerate(columns):
            data_type = TYPE_MAP.get(udt_name, udt_name.upper())
            reference = RELATIONSHIPS.get((table_name, column_name))
            reference_clause = (
                f" REFERENCES analytics_hecta.{reference[0]}({reference[1]})"
                if reference
                else ""
            )
            comma = "," if index < len(columns) - 1 else ""
            statements.append(
                f"    {column_name} {data_type}{reference_clause}{comma}"
            )
        statements.extend(
            [
                ");",
                "",
                f"COMMENT ON TABLE analytics_hecta.{table_name} IS "
                f"{sql_literal(TABLE_DESCRIPTIONS[table_name])};",
            ]
        )
        for column_name, _ in columns:
            statements.append(
                f"COMMENT ON COLUMN analytics_hecta.{table_name}.{column_name} IS "
                f"{sql_literal(COLUMN_DESCRIPTIONS[column_name])};"
            )
        statements.append("")

    OUTPUT_PATH.write_text("\n".join(statements), encoding="utf-8")
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    export_catalog_ddl()
