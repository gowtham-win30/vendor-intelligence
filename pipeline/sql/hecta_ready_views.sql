CREATE SCHEMA IF NOT EXISTS analytics_hecta;

CREATE OR REPLACE VIEW analytics_hecta.daily_command_center AS
SELECT
    MAX(k.data_cutoff_date) AS data_cutoff_date,
    COUNT(*) AS total_skus,
    COUNT(*) FILTER (WHERE k.preliminary_priority_score >= 90) AS high_priority_skus,
    COUNT(*) FILTER (
        WHERE k.preliminary_gap_class = 'HIGH_GAP_CANDIDATE'
    ) AS high_gap_candidates,
    COUNT(*) FILTER (
        WHERE k.behavioral_classification = 'NO_OBSERVED_MOVEMENT'
    ) AS no_observed_movement_skus,
    COUNT(*) FILTER (
        WHERE c.current_inventory_status = 'IN_STOCK'
    ) AS currently_in_stock_skus,
    COUNT(*) FILTER (
        WHERE c.current_inventory_status = 'OUT_OF_STOCK'
    ) AS currently_out_of_stock_skus,
    COUNT(*) FILTER (
        WHERE c.catalog_store_count > 0
    ) AS current_catalog_skus,
    SUM(k.valid_depleted_units) AS historical_valid_depleted_units,
    SUM(k.valid_units_sold) AS historical_valid_units_sold,
    SUM(k.valid_revenue) AS historical_valid_revenue,
    (
        SELECT COUNT(*)
        FROM analytics.source_freshness_summary f
        WHERE f.freshness_status = 'STALE'
    ) AS stale_sources,
    MAX(k.kpi_spec_version) AS kpi_spec_version,
    TRUE AS requires_analyst_review
FROM analytics.sku_phase1_kpis k
LEFT JOIN analytics.sku_catalog_current c USING (sku);

CREATE OR REPLACE VIEW analytics_hecta.sku_intelligence AS
SELECT
    k.sku,
    COALESCE(c.product_name, k.product_name) AS product_name,
    COALESCE(c.canonical_manufacturer, k.manufacturer) AS manufacturer,
    COALESCE(c.google_product_category, k.inferred_category_segment) AS category_segment,
    c.google_product_category IS NOT NULL AS category_is_authoritative,
    c.google_product_category,
    k.historical_vendor_count,
    k.fresh_vendor_count,
    k.store_count,
    k.valid_depleted_units AS historical_valid_depleted_units,
    k.depletion_events AS historical_depletion_events,
    k.stockout_transitions AS historical_stockout_transitions,
    k.valid_units_sold AS historical_valid_units_sold,
    k.valid_revenue AS historical_valid_revenue,
    k.order_count AS historical_order_count,
    k.demand_consistency_score,
    k.movement_frequency_index,
    k.bulk_shock_flag,
    k.demand_pattern,
    k.behavioral_classification,
    k.multi_vendor_strength_score,
    k.vendor_dependency_risk,
    k.preliminary_gap_class AS movement_sales_gap_class,
    k.preliminary_priority_score AS priority_score,
    k.vendor_freshness_days,
    k.sales_freshness_days,
    COALESCE(
        c.current_inventory_status,
        'UNKNOWN_NOT_INTEGRATED'
    ) AS current_inventory_status,
    COALESCE(
        c.current_catalog_status,
        'NOT_IN_INTEGRATED_FEEDS'
    ) AS current_catalog_status,
    c.current_inventory_quantity,
    c.network_inventory_quantity,
    c.inventory_quantity_basis,
    k.is_non_product_sku,
    c.inventory_store_count,
    c.catalog_store_count,
    c.minimum_current_retail_price,
    c.maximum_current_retail_price,
    c.maximum_list_price,
    c.has_description,
    c.has_product_link,
    c.has_image,
    c.has_conflicting_records AS has_catalog_conflict,
    c.source_loaded_at AS catalog_source_loaded_at,
    k.kpi_spec_version,
    k.data_cutoff_date,
    TRUE AS requires_analyst_review,
    k.peak_observed_quantity AS historical_peak_observed_quantity,
    k.restock_events AS historical_restock_events,
    k.depletion_to_peak_ratio,
    k.depletion_credibility_flag
FROM analytics.sku_phase1_kpis k
LEFT JOIN analytics.sku_catalog_current c USING (sku);

CREATE OR REPLACE VIEW analytics_hecta.ad_priority_candidates AS
SELECT
    t.rank,
    t.sku,
    COALESCE(c.product_name, t.product_name) AS product_name,
    COALESCE(c.canonical_manufacturer, t.manufacturer) AS manufacturer,
    COALESCE(c.google_product_category, t.inferred_category_segment) AS category_segment,
    c.google_product_category IS NOT NULL AS category_is_authoritative,
    t.historical_vendor_count,
    t.fresh_vendor_count,
    t.store_count,
    t.valid_depleted_units AS historical_valid_depleted_units,
    t.valid_units_sold AS historical_valid_units_sold,
    t.valid_revenue AS historical_valid_revenue,
    t.preliminary_priority_score AS priority_score,
    t.demand_consistency_score,
    t.movement_frequency_index,
    t.demand_pattern,
    t.behavioral_classification,
    t.multi_vendor_strength_score,
    t.vendor_dependency_risk,
    t.vendor_freshness_days,
    t.sales_freshness_days,
    COALESCE(
        c.current_inventory_status,
        'UNKNOWN_NOT_INTEGRATED'
    ) AS current_inventory_status,
    COALESCE(
        c.current_catalog_status,
        'NOT_IN_INTEGRATED_FEEDS'
    ) AS current_catalog_status,
    c.current_inventory_quantity,
    c.minimum_current_retail_price,
    c.maximum_current_retail_price,
    t.kpi_spec_version,
    t.data_cutoff_date,
    'Analyst review only; ad-platform state and expected return are not integrated.'
        ::text AS usage_limitation,
    t.peak_observed_quantity AS historical_peak_observed_quantity,
    t.depletion_to_peak_ratio,
    t.depletion_credibility_flag
FROM analytics.preliminary_top_200_focus_parts t
LEFT JOIN analytics.sku_catalog_current c USING (sku);

CREATE OR REPLACE VIEW analytics_hecta.movement_vs_sales_gap AS
SELECT
    g.sku,
    COALESCE(c.product_name, g.product_name) AS product_name,
    COALESCE(c.canonical_manufacturer, g.manufacturer) AS manufacturer,
    COALESCE(c.google_product_category, g.inferred_category_segment) AS category_segment,
    c.google_product_category IS NOT NULL AS category_is_authoritative,
    g.historical_vendor_count,
    g.valid_depleted_units AS historical_valid_depleted_units,
    g.depletion_events AS historical_depletion_events,
    g.valid_units_sold AS historical_valid_units_sold,
    g.valid_revenue AS historical_valid_revenue,
    g.preliminary_gap_class AS gap_class,
    g.vendor_freshness_days,
    COALESCE(
        c.current_inventory_status,
        'UNKNOWN_NOT_INTEGRATED'
    ) AS current_inventory_status,
    COALESCE(
        c.current_catalog_status,
        'NOT_IN_INTEGRATED_FEEDS'
    ) AS current_catalog_status,
    c.current_inventory_quantity,
    g.kpi_spec_version,
    g.data_cutoff_date,
    'Historical gap candidate; current catalog and inventory context is included when mapped.'
        ::text AS usage_limitation,
    g.peak_observed_quantity AS historical_peak_observed_quantity,
    g.restock_events AS historical_restock_events,
    g.depletion_to_peak_ratio,
    g.depletion_credibility_flag
FROM analytics.preliminary_movement_vs_sales_gap g
LEFT JOIN analytics.sku_catalog_current c USING (sku);

CREATE OR REPLACE VIEW analytics_hecta.catalog_visibility_candidates AS
SELECT
    n.sku,
    COALESCE(c.canonical_manufacturer, n.manufacturer) AS manufacturer,
    COALESCE(c.google_product_category, n.inferred_category_segment) AS category_segment,
    c.google_product_category IS NOT NULL AS category_is_authoritative,
    n.historical_vendor_count,
    n.valid_depleted_units AS historical_valid_depleted_units,
    n.valid_units_sold AS historical_valid_units_sold,
    n.vendor_freshness_days,
    COALESCE(
        c.current_inventory_status,
        'UNKNOWN_NOT_INTEGRATED'
    ) AS current_inventory_status,
    COALESCE(
        c.current_catalog_status,
        'NOT_IN_INTEGRATED_FEEDS'
    ) AS current_catalog_status,
    c.current_inventory_quantity,
    c.catalog_store_count,
    c.has_description,
    c.has_image,
    n.kpi_spec_version,
    n.data_cutoff_date,
    n.limitation AS usage_limitation
FROM analytics.no_observed_movement_candidates n
LEFT JOIN analytics.sku_catalog_current c USING (sku);

CREATE OR REPLACE VIEW analytics_hecta.sales_daily AS
SELECT
    period_start AS sale_date,
    period_end,
    source_store,
    valid_units_sold,
    valid_revenue,
    orders,
    distinct_skus_sold,
    period_is_complete,
    kpi_spec_version,
    data_cutoff_date,
    usage_limitation
FROM analytics.sales_period_summary
WHERE period_grain = 'DAY';

CREATE OR REPLACE VIEW analytics_hecta.sales_weekly AS
SELECT
    period_start AS week_start,
    period_end AS week_end,
    source_store,
    valid_units_sold,
    valid_revenue,
    orders,
    distinct_skus_sold,
    period_is_complete,
    kpi_spec_version,
    data_cutoff_date,
    usage_limitation
FROM analytics.sales_period_summary
WHERE period_grain = 'WEEK';

CREATE OR REPLACE VIEW analytics_hecta.sales_monthly AS
SELECT
    period_start AS month_start,
    period_end AS month_end,
    source_store,
    valid_units_sold,
    valid_revenue,
    orders,
    distinct_skus_sold,
    period_is_complete,
    kpi_spec_version,
    data_cutoff_date,
    usage_limitation
FROM analytics.sales_period_summary
WHERE period_grain = 'MONTH';

CREATE OR REPLACE VIEW analytics_hecta.monthly_trends AS
SELECT
    m.month_start,
    m.category_segment,
    m.category_is_authoritative,
    m.valid_depleted_units,
    m.depletion_events,
    m.stockout_transitions,
    m.valid_units_sold,
    m.valid_revenue,
    m.orders,
    m.kpi_spec_version
FROM analytics.monthly_trend_summary_catalog m;

CREATE OR REPLACE VIEW analytics_hecta.source_health AS
SELECT
    f.source_type,
    f.source_name,
    f.latest_record_at,
    f.freshness_days,
    f.freshness_status,
    f.source_freshness_score,
    f.kpi_spec_version,
    f.data_cutoff_date,
    f.freshness_basis
FROM analytics.source_freshness_summary f;

CREATE OR REPLACE VIEW analytics_hecta.data_quality_health AS
SELECT
    q.source_vendor,
    q.raw_rows,
    q.eligible_rows,
    q.clean_rows,
    q.exact_duplicate_rows_removed,
    q.null_quantity_rows,
    q.negative_quantity_rows,
    q.invalid_quantity_rows,
    q.invalid_prior_date_rows,
    q.invalid_quantity_date_rows,
    q.null_manufacturer_rows,
    q.duplicate_sku_record_rate,
    q.null_manufacturer_rate,
    q.quantity_change_consistency,
    q.invalid_quantity_date_rate,
    q.vendor_freshness_days,
    q.vendor_freshness_index,
    q.data_quality_risk_score,
    q.kpi_spec_version,
    q.data_cutoff_date
FROM analytics.source_quality_kpis q;

CREATE OR REPLACE VIEW analytics_hecta.current_catalog AS
SELECT
    c.sku,
    c.product_name,
    c.canonical_manufacturer AS manufacturer,
    c.canonical_brand AS brand,
    c.google_product_category AS category_segment,
    c.google_product_category,
    c.inventory_store_count,
    c.catalog_store_count,
    c.catalog_stores,
    c.current_inventory_quantity,
    c.network_inventory_quantity,
    c.inventory_quantity_basis,
    c.current_inventory_status,
    c.current_catalog_status,
    c.minimum_current_retail_price,
    c.maximum_current_retail_price,
    c.maximum_list_price,
    c.maximum_weight,
    c.has_description,
    c.has_product_link,
    c.has_image,
    c.has_conflicting_records,
    c.source_loaded_at,
    c.data_cutoff_date,
    c.merchant_product_type
FROM analytics.sku_catalog_current c;

CREATE OR REPLACE VIEW analytics_hecta.pricing_review_candidates AS
SELECT
    c.sku,
    c.product_name,
    c.canonical_manufacturer AS manufacturer,
    c.google_product_category AS category_segment,
    c.inventory_store_count,
    c.catalog_store_count,
    c.current_inventory_quantity,
    c.current_inventory_status,
    c.minimum_current_retail_price,
    c.maximum_current_retail_price,
    c.maximum_list_price,
    c.maximum_current_retail_price - c.minimum_current_retail_price
        AS observed_store_price_spread,
    c.source_loaded_at,
    c.data_cutoff_date,
    'Pricing review only; vendor cost, MAP, freight, returns, and margin are not integrated.'
        ::text AS usage_limitation
FROM analytics.sku_catalog_current c
WHERE c.minimum_current_retail_price IS NOT NULL
  AND (
      c.maximum_current_retail_price > c.minimum_current_retail_price
      OR c.maximum_list_price IS DISTINCT FROM c.maximum_current_retail_price
  );

CREATE OR REPLACE VIEW analytics_hecta.column_quality_profile AS
WITH latest_run AS (
    SELECT run_id
    FROM audit.data_quality_profile_runs
    WHERE status = 'COMPLETED'
    ORDER BY completed_at DESC
    LIMIT 1
)
SELECT
    p.schema_name,
    p.table_name,
    p.source_name,
    p.column_name,
    p.data_type,
    p.total_rows,
    p.missing_rows,
    p.missing_pct,
    p.null_policy,
    p.kpi_impact,
    p.trust_status
FROM audit.data_quality_column_profiles p
JOIN latest_run r USING (run_id)
WHERE p.kpi_impact <> 'NONE_PII_EXCLUDED';

COMMENT ON SCHEMA analytics_hecta IS
    'Curated, read-only Vendor Intelligence contract for Hecta Data Catalog.';
COMMENT ON VIEW analytics_hecta.daily_command_center IS
    'One row per analytics refresh cutoff; historical cumulative measures only.';
COMMENT ON VIEW analytics_hecta.sku_intelligence IS
    'One row per normalized SKU with historical KPIs plus mapped current catalog and inventory context.';
COMMENT ON VIEW analytics_hecta.ad_priority_candidates IS
    'Up to 200 historical ad-review candidates; no automated budget action.';
COMMENT ON VIEW analytics_hecta.movement_vs_sales_gap IS
    'One row per historical movement-sales gap candidate.';
COMMENT ON VIEW analytics_hecta.catalog_visibility_candidates IS
    'One row per historical no-observed-movement candidate; not a demotion list.';
COMMENT ON VIEW analytics_hecta.monthly_trends IS
    'One row per inferred category segment and calendar month.';
COMMENT ON VIEW analytics_hecta.sales_daily IS
    'One row per storefront or NETWORK and calendar day of observed storefront sales.';
COMMENT ON VIEW analytics_hecta.sales_weekly IS
    'One row per storefront or NETWORK and ISO week of observed storefront sales.';
COMMENT ON VIEW analytics_hecta.sales_monthly IS
    'One row per storefront or NETWORK and calendar month of observed storefront sales.';
COMMENT ON VIEW analytics_hecta.source_health IS
    'One row per vendor or storefront source.';
COMMENT ON VIEW analytics_hecta.data_quality_health IS
    'One row per vendor source with distinct-row data-quality rates.';
COMMENT ON VIEW analytics_hecta.current_catalog IS
    'One row per normalized SKU from integrated current inventory and merchant feeds.';
COMMENT ON VIEW analytics_hecta.pricing_review_candidates IS
    'One row per SKU with observed retail-price differences; not margin optimization.';
COMMENT ON VIEW analytics_hecta.column_quality_profile IS
    'Latest completed missing-value profile by source and column; PII columns excluded.';

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hecta_catalog_reader') THEN
        CREATE ROLE hecta_catalog_reader NOLOGIN;
    END IF;
END
$$;

REVOKE ALL ON SCHEMA analytics_hecta FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA analytics_hecta FROM PUBLIC;
GRANT USAGE ON SCHEMA analytics_hecta TO hecta_catalog_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics_hecta TO hecta_catalog_reader;
