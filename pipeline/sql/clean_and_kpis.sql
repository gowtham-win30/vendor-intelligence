CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS clean.sales_orders CASCADE;
CREATE TABLE clean.sales_orders AS
WITH ranked AS (
    SELECT
        source_store,
        source_table,
        order_id,
        UPPER(TRIM(sku)) AS normalized_sku,
        created_at,
        qty_ordered,
        price,
        product_name,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY
    source_store,
    source_table,
    order_id,
    customer_email,
                UPPER(TRIM(sku)),
    created_at,
                qty_ordered,
                price,
    region,
    postcode,
    company,
    customer_first_name,
    customer_last_name,
    phone,
    product_name
            ORDER BY ingested_at
        ) AS exact_duplicate_rank
FROM raw.sales_orders
WHERE sku IS NOT NULL 
      AND TRIM(sku) <> ''
      AND created_at IS NOT NULL
      AND created_at < CURRENT_DATE + INTERVAL '1 day'
)
SELECT
    source_store,
    source_table,
    order_id,
    normalized_sku AS sku,
    created_at,
    qty_ordered,
    price,
    CASE WHEN qty_ordered > 0 THEN qty_ordered END AS valid_units_sold,
    CASE
        WHEN qty_ordered > 0 AND price IS NOT NULL AND price >= 0
        THEN qty_ordered * price
    END AS valid_revenue,
    (qty_ordered IS NULL OR qty_ordered <= 0) AS has_invalid_sale_quantity,
    (price IS NULL OR price < 0) AS has_invalid_sale_price,
    product_name,
    ingested_at AS source_ingested_at
FROM ranked
WHERE exact_duplicate_rank = 1;

CREATE INDEX idx_clean_so_sku ON clean.sales_orders(sku);
CREATE INDEX idx_clean_so_store_date ON clean.sales_orders(source_store, created_at);

DROP TABLE IF EXISTS clean.vendor_deltas CASCADE;
CREATE TABLE clean.vendor_deltas AS
WITH ranked AS (
    SELECT
        source_vendor,
        source_table,
        prior_date,
        snapshot_date,
        UPPER(TRIM(sku)) AS normalized_sku,
        NULLIF(UPPER(TRIM(manufacturer)), '') AS normalized_manufacturer,
        old_qty,
        new_qty,
        old_price,
        new_price,
        month_label,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY
    source_vendor,
    source_table,
                prior_date,
                snapshot_date,
                UPPER(TRIM(sku)),
                NULLIF(UPPER(TRIM(manufacturer)), ''),
                old_qty,
                new_qty,
                old_price,
                new_price,
                month_label
            ORDER BY ingested_at
        ) AS exact_duplicate_rank
    FROM raw.vendor_deltas
    WHERE sku IS NOT NULL
      AND TRIM(sku) <> ''
      AND snapshot_date IS NOT NULL
      AND snapshot_date <= CURRENT_DATE
),
deduplicated AS (
    SELECT *
    FROM ranked
    WHERE exact_duplicate_rank = 1
),
same_day_transitions AS (
    SELECT
        source_vendor,
        normalized_sku,
        snapshot_date,
        COUNT(*) AS transition_count
    FROM deduplicated
    GROUP BY source_vendor, normalized_sku, snapshot_date
)
SELECT
    d.source_vendor,
    d.source_table,
    CASE
        WHEN d.prior_date > CURRENT_DATE OR d.prior_date > d.snapshot_date THEN NULL
        ELSE d.prior_date
    END AS prior_date,
    d.snapshot_date,
    d.normalized_sku AS sku,
    d.normalized_manufacturer AS manufacturer,
    d.old_qty,
    d.new_qty,
    CASE
        WHEN d.old_qty IS NULL OR d.new_qty IS NULL OR d.old_qty < 0 OR d.new_qty < 0 THEN NULL
        WHEN d.old_qty > d.new_qty THEN d.old_qty - d.new_qty
        ELSE 0
    END AS valid_units_depleted,
    CASE
        WHEN d.old_qty IS NULL OR d.new_qty IS NULL OR d.old_qty < 0 OR d.new_qty < 0 THEN NULL
        ELSE d.old_qty > d.new_qty
    END AS is_valid_depletion_event,
    CASE
        WHEN d.old_qty IS NULL OR d.new_qty IS NULL OR d.old_qty < 0 OR d.new_qty < 0 THEN NULL
        ELSE d.old_qty > 0 AND d.new_qty = 0
    END AS is_valid_stockout_transition,
    (d.old_qty IS NULL OR d.new_qty IS NULL) AS has_null_quantity,
    (d.old_qty < 0 OR d.new_qty < 0) AS has_negative_quantity,
    (d.prior_date > CURRENT_DATE OR d.prior_date > d.snapshot_date) AS has_invalid_prior_date,
    (t.transition_count > 1) AS has_multiple_same_day_transitions,
    d.old_price,
    d.new_price,
    d.month_label,
    d.ingested_at AS source_ingested_at
FROM deduplicated d
JOIN same_day_transitions t
  ON t.source_vendor = d.source_vendor
 AND t.normalized_sku = d.normalized_sku
 AND t.snapshot_date = d.snapshot_date;

CREATE INDEX idx_clean_vd_sku ON clean.vendor_deltas(sku);
CREATE INDEX idx_clean_vd_vendor_date ON clean.vendor_deltas(source_vendor, snapshot_date);

DROP TABLE IF EXISTS clean.current_inventory CASCADE;
CREATE TABLE clean.current_inventory AS
SELECT
    source_store,
    UPPER(TRIM(sku)) AS sku,
    MODE() WITHIN GROUP (
        ORDER BY NULLIF(UPPER(TRIM(manufacturer)), '')
    ) AS manufacturer,
    MAX(quantity) AS quantity,
    MAX(price) AS price,
    MAX(list_price) AS list_price,
    MAX(weight) AS weight,
    COUNT(*) AS source_record_count,
    COUNT(*) > 1 AS has_conflicting_records,
    MAX(ingested_at) AS source_loaded_at
FROM raw.current_inventory
WHERE sku IS NOT NULL
  AND TRIM(sku) <> ''
GROUP BY source_store, UPPER(TRIM(sku));

CREATE UNIQUE INDEX idx_clean_current_inventory_store_sku
    ON clean.current_inventory(source_store, sku);
CREATE INDEX idx_clean_current_inventory_sku
    ON clean.current_inventory(sku);

DROP TABLE IF EXISTS clean.merchant_catalog CASCADE;
CREATE TABLE clean.merchant_catalog AS
SELECT
    source_store,
    UPPER(TRIM(sku)) AS sku,
    MODE() WITHIN GROUP (ORDER BY NULLIF(UPPER(TRIM(mpn)), '')) AS mpn,
    MODE() WITHIN GROUP (ORDER BY NULLIF(TRIM(product_name), '')) AS product_name,
    MODE() WITHIN GROUP (ORDER BY NULLIF(UPPER(TRIM(brand)), '')) AS brand,
    MODE() WITHIN GROUP (
        ORDER BY NULLIF(UPPER(TRIM(availability)), '')
    ) AS availability,
    MODE() WITHIN GROUP (ORDER BY NULLIF(UPPER(TRIM(condition)), '')) AS condition,
    MODE() WITHIN GROUP (
        ORDER BY NULLIF(TRIM(google_product_category), '')
    ) AS google_product_category,
    MODE() WITHIN GROUP (ORDER BY NULLIF(TRIM(product_type), '')) AS product_type,
    MAX(price) AS price,
    MAX(sale_price) AS sale_price,
    MAX(shipment_weight) AS shipment_weight,
    BOOL_OR(NULLIF(TRIM(description), '') IS NOT NULL) AS has_description,
    BOOL_OR(NULLIF(TRIM(product_link), '') IS NOT NULL) AS has_product_link,
    BOOL_OR(NULLIF(TRIM(image_link), '') IS NOT NULL) AS has_image,
    COUNT(*) AS source_record_count,
    COUNT(*) > 1 AS has_conflicting_records,
    MAX(ingested_at) AS source_loaded_at
FROM raw.merchant_catalog
WHERE sku IS NOT NULL
  AND TRIM(sku) <> ''
GROUP BY source_store, UPPER(TRIM(sku));

CREATE UNIQUE INDEX idx_clean_merchant_catalog_store_sku
    ON clean.merchant_catalog(source_store, sku);
CREATE INDEX idx_clean_merchant_catalog_sku
    ON clean.merchant_catalog(sku);

DROP TABLE IF EXISTS analytics.sku_catalog_current CASCADE;
CREATE TABLE analytics.sku_catalog_current AS
WITH store_records AS (
    SELECT
        COALESCE(i.source_store, m.source_store) AS source_store,
        COALESCE(i.sku, m.sku) AS sku,
        i.manufacturer,
        m.brand,
        m.product_name,
        m.google_product_category,
        m.product_type,
        i.quantity,
        COALESCE(m.sale_price, m.price, i.price) AS current_retail_price,
        i.list_price,
        i.weight,
        m.availability,
        m.has_description,
        m.has_product_link,
        m.has_image,
        COALESCE(i.has_conflicting_records, FALSE)
            OR COALESCE(m.has_conflicting_records, FALSE) AS has_conflicting_records,
        GREATEST(i.source_loaded_at, m.source_loaded_at) AS source_loaded_at,
        i.sku IS NOT NULL AS has_inventory_record,
        m.sku IS NOT NULL AS has_catalog_record
    FROM clean.current_inventory i
    FULL OUTER JOIN clean.merchant_catalog m
      ON m.source_store = i.source_store
     AND m.sku = i.sku
)
SELECT
    sku,
    MODE() WITHIN GROUP (
        ORDER BY COALESCE(manufacturer, brand)
    ) AS canonical_manufacturer,
    MODE() WITHIN GROUP (ORDER BY brand) AS canonical_brand,
    MODE() WITHIN GROUP (ORDER BY product_name) AS product_name,
    MODE() WITHIN GROUP (ORDER BY product_type) AS merchant_product_type,
    MODE() WITHIN GROUP (
        ORDER BY google_product_category
    ) AS google_product_category,
    COUNT(DISTINCT source_store) FILTER (
        WHERE has_inventory_record
    ) AS inventory_store_count,
    COUNT(DISTINCT source_store) FILTER (
        WHERE has_catalog_record
    ) AS catalog_store_count,
    STRING_AGG(
        DISTINCT source_store,
        ', ' ORDER BY source_store
    ) FILTER (WHERE has_catalog_record) AS catalog_stores,
    COALESCE(
        SUM(quantity) FILTER (WHERE quantity >= 0),
        0
    ) AS current_inventory_quantity,
    CASE
        WHEN NOT BOOL_OR(has_inventory_record) THEN 'UNKNOWN_NOT_INTEGRATED'
        WHEN BOOL_OR(quantity > 0) THEN 'IN_STOCK'
        WHEN BOOL_AND(quantity IS NULL) THEN 'UNKNOWN'
        ELSE 'OUT_OF_STOCK'
    END AS current_inventory_status,
    CASE
        WHEN NOT BOOL_OR(has_catalog_record) THEN 'NOT_IN_INTEGRATED_FEEDS'
        WHEN BOOL_OR(availability IN ('IN STOCK', 'IN_STOCK')) THEN 'ACTIVE_IN_STOCK'
        WHEN BOOL_OR(availability IS NOT NULL) THEN 'ACTIVE_OTHER_AVAILABILITY'
        ELSE 'ACTIVE_STATUS_UNKNOWN'
    END AS current_catalog_status,
    MIN(current_retail_price) FILTER (
        WHERE current_retail_price >= 0
    ) AS minimum_current_retail_price,
    MAX(current_retail_price) FILTER (
        WHERE current_retail_price >= 0
    ) AS maximum_current_retail_price,
    MAX(list_price) FILTER (WHERE list_price >= 0) AS maximum_list_price,
    MAX(weight) FILTER (WHERE weight >= 0) AS maximum_weight,
    BOOL_OR(COALESCE(has_description, FALSE)) AS has_description,
    BOOL_OR(COALESCE(has_product_link, FALSE)) AS has_product_link,
    BOOL_OR(COALESCE(has_image, FALSE)) AS has_image,
    BOOL_OR(has_conflicting_records) AS has_conflicting_records,
    MAX(source_loaded_at) AS source_loaded_at,
    CURRENT_DATE AS data_cutoff_date
FROM store_records
GROUP BY sku;

CREATE UNIQUE INDEX idx_sku_catalog_current_sku
    ON analytics.sku_catalog_current(sku);
CREATE INDEX idx_sku_catalog_current_product_type
    ON analytics.sku_catalog_current(merchant_product_type);

DROP TABLE IF EXISTS analytics.sku_vendor_summary CASCADE;
CREATE TABLE analytics.sku_vendor_summary AS
WITH vendor_coverage AS (
    SELECT
        source_vendor,
        MIN(snapshot_date) AS vendor_first_snapshot_date,
        MAX(snapshot_date) AS vendor_last_snapshot_date,
        (MAX(snapshot_date) - MIN(snapshot_date) + 1) AS vendor_coverage_days
    FROM clean.vendor_deltas
    GROUP BY source_vendor
),
vendor_sku AS (
    SELECT
        d.source_vendor,
        d.sku,
        MODE() WITHIN GROUP (ORDER BY d.manufacturer) AS manufacturer,
        COALESCE(SUM(d.valid_units_depleted), 0) AS valid_depleted_units,
        COUNT(*) FILTER (WHERE d.is_valid_depletion_event) AS depletion_events,
        COUNT(DISTINCT d.snapshot_date) FILTER (WHERE d.is_valid_depletion_event) AS depletion_active_days,
        COUNT(*) FILTER (WHERE d.is_valid_stockout_transition) AS stockout_transitions,
        COUNT(*) FILTER (
            WHERE d.old_qty IS NOT NULL AND d.new_qty IS NOT NULL
              AND d.old_qty >= 0 AND d.new_qty >= 0
              AND d.new_qty > d.old_qty
        ) AS restock_events,
        GREATEST(MAX(d.old_qty), MAX(d.new_qty)) AS peak_observed_quantity,
        COUNT(*) FILTER (WHERE d.has_null_quantity) AS null_quantity_records,
        COUNT(*) FILTER (WHERE d.has_negative_quantity) AS negative_quantity_records,
        COUNT(*) FILTER (WHERE d.has_multiple_same_day_transitions) AS multiple_same_day_transition_records,
        MIN(d.snapshot_date) AS first_sku_snapshot_date,
        MAX(d.snapshot_date) AS last_sku_snapshot_date,
        c.vendor_first_snapshot_date,
        c.vendor_last_snapshot_date,
        c.vendor_coverage_days,
        COALESCE(SUM(d.valid_units_depleted), 0) / NULLIF(c.vendor_coverage_days, 0)::numeric
            AS preliminary_vendor_daily_depletion_rate
    FROM clean.vendor_deltas d
    JOIN vendor_coverage c USING (source_vendor)
    GROUP BY
        d.source_vendor,
        d.sku,
        c.vendor_first_snapshot_date,
        c.vendor_last_snapshot_date,
        c.vendor_coverage_days
),
network AS (
    SELECT
        sku,
        MODE() WITHIN GROUP (ORDER BY manufacturer) AS manufacturer,
        COUNT(*) AS historical_vendor_count,
        STRING_AGG(source_vendor, ', ' ORDER BY source_vendor) AS historical_vendors,
        SUM(valid_depleted_units) AS valid_depleted_units,
        SUM(depletion_events) AS depletion_events,
        SUM(depletion_active_days) AS depletion_active_days,
        SUM(stockout_transitions) AS stockout_transitions,
        SUM(restock_events) AS restock_events,
        MAX(peak_observed_quantity) AS peak_observed_quantity,
        SUM(null_quantity_records) AS null_quantity_records,
        SUM(negative_quantity_records) AS negative_quantity_records,
        SUM(multiple_same_day_transition_records) AS multiple_same_day_transition_records,
        MIN(first_sku_snapshot_date) AS first_snapshot_date,
        MAX(last_sku_snapshot_date) AS latest_snapshot_date,
        SUM(preliminary_vendor_daily_depletion_rate) AS preliminary_daily_depletion_rate
    FROM vendor_sku
    GROUP BY sku
)
SELECT
    *,
    CURRENT_DATE - latest_snapshot_date AS vendor_freshness_days,
    CASE
        WHEN depletion_events >= 10 AND valid_depleted_units >= 50 THEN 'FAST_CANDIDATE'
        WHEN depletion_events > 0 THEN 'SLOW_CANDIDATE'
        ELSE 'NO_OBSERVED_DEPLETION'
    END AS preliminary_movement_class,
    '1.0'::text AS kpi_spec_version,
    CURRENT_DATE AS data_cutoff_date
FROM network;

CREATE UNIQUE INDEX idx_sku_vendor_summary_sku ON analytics.sku_vendor_summary(sku);

DROP TABLE IF EXISTS analytics.sku_sales_summary CASCADE;
CREATE TABLE analytics.sku_sales_summary AS
WITH store_coverage AS (
    SELECT
        source_store,
        MIN(created_at::date) AS store_first_sale_date,
        MAX(created_at::date) AS store_latest_sale_date,
        (MAX(created_at::date) - MIN(created_at::date) + 1) AS store_coverage_days
    FROM clean.sales_orders
    GROUP BY source_store
),
store_sku AS (
    SELECT
        s.source_store,
        s.sku,
        MODE() WITHIN GROUP (ORDER BY s.product_name) AS product_name,
        COALESCE(SUM(s.valid_units_sold), 0) AS valid_units_sold,
        COALESCE(SUM(s.valid_revenue), 0) AS valid_revenue,
        COUNT(DISTINCT s.order_id) FILTER (WHERE s.valid_units_sold IS NOT NULL) AS order_count,
        COUNT(*) FILTER (WHERE s.has_invalid_sale_quantity) AS invalid_quantity_records,
        COUNT(*) FILTER (WHERE s.has_invalid_sale_price) AS invalid_price_records,
        MIN(s.created_at::date) AS first_sku_sale_date,
        MAX(s.created_at::date) AS latest_sku_sale_date,
        c.store_coverage_days,
        COALESCE(SUM(s.valid_units_sold), 0) / NULLIF(c.store_coverage_days, 0)::numeric
            AS store_daily_sales_velocity
    FROM clean.sales_orders s
    JOIN store_coverage c USING (source_store)
    GROUP BY s.source_store, s.sku, c.store_coverage_days
),
network AS (
    SELECT
        sku,
        MODE() WITHIN GROUP (ORDER BY product_name) AS product_name,
        COUNT(*) AS store_count,
        STRING_AGG(source_store, ', ' ORDER BY source_store) AS stores,
        SUM(valid_units_sold) AS valid_units_sold,
        SUM(valid_revenue) AS valid_revenue,
        SUM(order_count) AS order_count,
        SUM(invalid_quantity_records) AS invalid_quantity_records,
        SUM(invalid_price_records) AS invalid_price_records,
        MIN(first_sku_sale_date) AS first_sale_date,
        MAX(latest_sku_sale_date) AS latest_sale_date,
        SUM(store_daily_sales_velocity) AS preliminary_daily_sales_velocity
    FROM store_sku
    GROUP BY sku
)
SELECT
    *,
    CASE
        WHEN valid_units_sold > 0 THEN ROUND(valid_revenue / valid_units_sold, 2)
    END AS average_selling_price,
    CURRENT_DATE - latest_sale_date AS sales_freshness_days,
    '1.0'::text AS kpi_spec_version,
    CURRENT_DATE AS data_cutoff_date
FROM network;

CREATE UNIQUE INDEX idx_sku_sales_summary_sku ON analytics.sku_sales_summary(sku);

DROP TABLE IF EXISTS analytics.sku_master_intelligence CASCADE;
CREATE TABLE analytics.sku_master_intelligence AS
WITH combined_skus AS (
    SELECT sku FROM analytics.sku_vendor_summary
    UNION
    SELECT sku FROM analytics.sku_sales_summary
),
base AS (
SELECT
    c.sku,
    s.product_name,
    v.manufacturer,
    CASE
            WHEN v.historical_vendors LIKE '%Neuco%'
              OR v.historical_vendors LIKE '%Metropac%'
              OR s.stores LIKE '%PartsHNC%' THEN 'HVAC_INFERRED'
            WHEN v.historical_vendors LIKE '%EC%'
              OR v.historical_vendors LIKE '%Msupply%'
              OR v.historical_vendors LIKE '%ReliableParts%'
              OR s.stores LIKE '%APZ%' THEN 'APPLIANCE_INFERRED'
            WHEN s.stores LIKE '%PartsFE%' THEN 'COMMERCIAL_KITCHEN_INFERRED'
            ELSE 'OTHER_INFERRED'
        END AS inferred_category_segment,
        COALESCE(v.historical_vendor_count, 0) AS historical_vendor_count,
        v.historical_vendors,
    COALESCE(s.store_count, 0) AS store_count,
        s.stores,
        COALESCE(v.valid_depleted_units, 0) AS valid_depleted_units,
        COALESCE(v.preliminary_daily_depletion_rate, 0) AS preliminary_daily_depletion_rate,
    COALESCE(v.depletion_events, 0) AS depletion_events,
        COALESCE(v.stockout_transitions, 0) AS stockout_transitions,
        COALESCE(v.restock_events, 0) AS restock_events,
        v.peak_observed_quantity,
        COALESCE(v.preliminary_movement_class, 'NO_VENDOR_HISTORY') AS preliminary_movement_class,
        COALESCE(s.valid_units_sold, 0) AS valid_units_sold,
        COALESCE(s.valid_revenue, 0) AS valid_revenue,
    COALESCE(s.order_count, 0) AS order_count,
        s.average_selling_price,
        v.latest_snapshot_date,
        v.vendor_freshness_days,
        s.latest_sale_date,
        s.sales_freshness_days,
        (v.sku IS NOT NULL) AS has_vendor_history,
        (s.sku IS NOT NULL) AS has_store_sales_history,
        CASE
            WHEN COALESCE(v.valid_depleted_units, 0) >= 100
              AND COALESCE(s.valid_units_sold, 0) = 0
              AND COALESCE(v.valid_depleted_units, 0)
                  <= COALESCE(v.peak_observed_quantity, 0) THEN 'HIGH_GAP_CANDIDATE'
            WHEN COALESCE(v.valid_depleted_units, 0) >= 20
              AND COALESCE(s.valid_units_sold, 0) <= 2
              AND COALESCE(v.valid_depleted_units, 0)
                  <= COALESCE(v.peak_observed_quantity, 0) THEN 'MODERATE_GAP_CANDIDATE'
        ELSE 'NONE'
        END AS preliminary_gap_class
    FROM combined_skus c
    LEFT JOIN analytics.sku_vendor_summary v USING (sku)
    LEFT JOIN analytics.sku_sales_summary s USING (sku)
)
SELECT
    *,
    ROUND(
        (
            PERCENT_RANK() OVER (ORDER BY valid_revenue) * 35.0
          + PERCENT_RANK() OVER (ORDER BY valid_units_sold) * 25.0
          + PERCENT_RANK() OVER (
                ORDER BY LEAST(
                    valid_depleted_units,
                    COALESCE(peak_observed_quantity, valid_depleted_units)
                )
            ) * 25.0
          + PERCENT_RANK() OVER (ORDER BY historical_vendor_count) * 15.0
        )::numeric,
        2
    ) AS preliminary_priority_score,
    CASE
        WHEN COALESCE(valid_depleted_units, 0) = 0 THEN NULL
        ELSE ROUND(
            valid_depleted_units::numeric / NULLIF(peak_observed_quantity, 0),
            2
        )
    END AS depletion_to_peak_ratio,
    CASE
        WHEN COALESCE(valid_depleted_units, 0) = 0 THEN 'NO_DEPLETION'
        WHEN peak_observed_quantity IS NULL OR peak_observed_quantity <= 0
            THEN 'UNKNOWN'
        WHEN valid_depleted_units <= peak_observed_quantity THEN 'PLAUSIBLE'
        WHEN valid_depleted_units <= 10 * peak_observed_quantity
            THEN 'LIKELY_RECOUNTED'
        ELSE 'SEVERE_RECOUNT'
    END AS depletion_credibility_flag,
    '1.0'::text AS kpi_spec_version,
    CURRENT_DATE AS data_cutoff_date
FROM base;

CREATE UNIQUE INDEX idx_sku_master_intelligence_sku ON analytics.sku_master_intelligence(sku);
CREATE INDEX idx_sku_master_intelligence_score
    ON analytics.sku_master_intelligence(preliminary_priority_score DESC);
CREATE INDEX idx_sku_master_intelligence_segment
    ON analytics.sku_master_intelligence(inferred_category_segment);

CREATE OR REPLACE VIEW analytics.preliminary_top_200_focus_parts AS
SELECT
    ROW_NUMBER() OVER (ORDER BY preliminary_priority_score DESC, sku) AS rank,
    sku,
    product_name,
    manufacturer,
    inferred_category_segment,
    historical_vendor_count,
    historical_vendors,
    store_count,
    stores,
    valid_depleted_units,
    ROUND(preliminary_daily_depletion_rate, 4) AS preliminary_daily_depletion_rate,
    valid_units_sold,
    valid_revenue,
    order_count,
    average_selling_price,
    latest_snapshot_date,
    vendor_freshness_days,
    latest_sale_date,
    sales_freshness_days,
    preliminary_priority_score,
    kpi_spec_version,
    data_cutoff_date
FROM analytics.sku_master_intelligence
ORDER BY preliminary_priority_score DESC, sku
LIMIT 200;

CREATE OR REPLACE VIEW analytics.preliminary_movement_vs_sales_gap AS
SELECT
    sku,
    product_name,
    manufacturer,
    inferred_category_segment,
    historical_vendor_count,
    historical_vendors,
    valid_depleted_units,
    ROUND(preliminary_daily_depletion_rate, 4) AS preliminary_daily_depletion_rate,
    depletion_events,
    valid_units_sold,
    valid_revenue,
    latest_snapshot_date,
    vendor_freshness_days,
    preliminary_gap_class,
    peak_observed_quantity,
    restock_events,
    depletion_to_peak_ratio,
    depletion_credibility_flag,
    kpi_spec_version,
    data_cutoff_date
FROM analytics.sku_master_intelligence
WHERE preliminary_gap_class IN ('HIGH_GAP_CANDIDATE', 'MODERATE_GAP_CANDIDATE')
ORDER BY valid_depleted_units DESC, sku;

CREATE OR REPLACE VIEW analytics.no_observed_movement_candidates AS
SELECT
    sku,
    manufacturer,
    inferred_category_segment,
    historical_vendor_count,
    historical_vendors,
    valid_depleted_units,
    valid_units_sold,
    latest_snapshot_date,
    vendor_freshness_days,
    'Historical candidate only; current catalog and inventory status not available.'::text AS limitation,
    kpi_spec_version,
    data_cutoff_date
FROM analytics.sku_master_intelligence
WHERE has_vendor_history
  AND valid_depleted_units = 0
  AND valid_units_sold = 0
ORDER BY historical_vendor_count DESC, sku;

CREATE OR REPLACE VIEW analytics.preliminary_category_summary AS
SELECT
    inferred_category_segment,
    COUNT(*) AS total_skus,
    COUNT(*) FILTER (WHERE has_vendor_history) AS skus_with_vendor_history,
    COUNT(*) FILTER (WHERE has_store_sales_history) AS skus_with_sales_history,
    SUM(valid_depleted_units) AS valid_vendor_depleted_units,
    SUM(valid_units_sold) AS valid_store_units_sold,
    SUM(valid_revenue) AS valid_store_revenue,
    MAX(data_cutoff_date) AS data_cutoff_date
FROM analytics.sku_master_intelligence
GROUP BY inferred_category_segment
ORDER BY valid_store_revenue DESC;

DROP TABLE IF EXISTS analytics.data_quality_summary;
CREATE TABLE analytics.data_quality_summary AS
SELECT 'vendor'::text AS data_area, 'raw_rows'::text AS metric, COUNT(*)::numeric AS metric_value
FROM raw.vendor_deltas
UNION ALL
SELECT 'vendor', 'clean_rows', COUNT(*)::numeric
FROM clean.vendor_deltas
UNION ALL
SELECT 'vendor', 'rejected_rows_before_dedup', (
    SELECT COUNT(*) FROM raw.vendor_deltas
) - (
    SELECT COUNT(*) FROM raw.vendor_deltas
    WHERE sku IS NOT NULL
      AND TRIM(sku) <> ''
      AND snapshot_date IS NOT NULL
      AND snapshot_date <= CURRENT_DATE
)
UNION ALL
SELECT 'vendor', 'exact_duplicate_rows_removed', (
    SELECT COUNT(*) FROM raw.vendor_deltas
    WHERE sku IS NOT NULL
      AND TRIM(sku) <> ''
      AND snapshot_date IS NOT NULL
      AND snapshot_date <= CURRENT_DATE
) - COUNT(*)
FROM clean.vendor_deltas
UNION ALL
SELECT 'vendor', 'null_quantity_records', COUNT(*)::numeric
FROM clean.vendor_deltas
WHERE has_null_quantity
UNION ALL
SELECT 'vendor', 'negative_quantity_records', COUNT(*)::numeric
FROM clean.vendor_deltas
WHERE has_negative_quantity
UNION ALL
SELECT 'vendor', 'invalid_prior_date_records', COUNT(*)::numeric
FROM clean.vendor_deltas
WHERE has_invalid_prior_date
UNION ALL
SELECT 'vendor', 'multiple_same_day_transition_records', COUNT(*)::numeric
FROM clean.vendor_deltas
WHERE has_multiple_same_day_transitions
UNION ALL
SELECT 'sales', 'raw_rows', COUNT(*)::numeric
FROM raw.sales_orders
UNION ALL
SELECT 'sales', 'clean_rows', COUNT(*)::numeric
FROM clean.sales_orders
UNION ALL
SELECT 'sales', 'rejected_rows_before_dedup', (
    SELECT COUNT(*) FROM raw.sales_orders
) - (
    SELECT COUNT(*) FROM raw.sales_orders
    WHERE sku IS NOT NULL
      AND TRIM(sku) <> ''
      AND created_at IS NOT NULL
      AND created_at < CURRENT_DATE + INTERVAL '1 day'
)
UNION ALL
SELECT 'sales', 'exact_duplicate_rows_removed', (
    SELECT COUNT(*) FROM raw.sales_orders
    WHERE sku IS NOT NULL
      AND TRIM(sku) <> ''
      AND created_at IS NOT NULL
      AND created_at < CURRENT_DATE + INTERVAL '1 day'
) - COUNT(*)
FROM clean.sales_orders
UNION ALL
SELECT 'sales', 'invalid_quantity_records', COUNT(*)::numeric
FROM clean.sales_orders
WHERE has_invalid_sale_quantity
UNION ALL
SELECT 'sales', 'invalid_price_records', COUNT(*)::numeric
FROM clean.sales_orders
WHERE has_invalid_sale_price;

DROP TABLE IF EXISTS analytics.source_freshness_summary CASCADE;
CREATE TABLE analytics.source_freshness_summary AS
WITH source_dates AS (
    SELECT
        'vendor'::text AS source_type,
        source_vendor AS source_name,
        MAX(snapshot_date)::timestamp AS latest_record_at,
        'SOURCE_RECORD_DATE'::text AS freshness_basis
    FROM clean.vendor_deltas
    GROUP BY source_vendor
    UNION ALL
    SELECT
        'sales'::text AS source_type,
        source_store AS source_name,
        MAX(created_at) AS latest_record_at,
        'SOURCE_RECORD_TIMESTAMP'::text AS freshness_basis
    FROM clean.sales_orders
    GROUP BY source_store
    UNION ALL
    SELECT
        'current_inventory'::text AS source_type,
        source_store AS source_name,
        MAX(source_loaded_at) AS latest_record_at,
        'PIPELINE_LOAD_TIMESTAMP_NO_SOURCE_DATE'::text AS freshness_basis
    FROM clean.current_inventory
    GROUP BY source_store
    UNION ALL
    SELECT
        'merchant_catalog'::text AS source_type,
        source_store AS source_name,
        MAX(source_loaded_at) AS latest_record_at,
        'PIPELINE_LOAD_TIMESTAMP_NO_SOURCE_DATE'::text AS freshness_basis
    FROM clean.merchant_catalog
    GROUP BY source_store
),
scored AS (
    SELECT
        source_type,
        source_name,
        latest_record_at,
        freshness_basis,
        CURRENT_DATE - latest_record_at::date AS freshness_days
    FROM source_dates
)
SELECT
    source_type,
    source_name,
    latest_record_at,
    freshness_basis,
    freshness_days,
    CASE
        WHEN freshness_days <= 7 THEN 'CURRENT'
        WHEN freshness_days <= 30 THEN 'WATCH'
        ELSE 'STALE'
    END AS freshness_status,
    ROUND(
        CASE
            WHEN freshness_days <= 7 THEN 100
            WHEN freshness_days <= 30 THEN 100 - ((freshness_days - 7) * 50.0 / 23.0)
            WHEN freshness_days <= 90 THEN 50 - ((freshness_days - 30) * 50.0 / 60.0)
            ELSE 0
        END,
        2
    ) AS source_freshness_score,
    '1.0'::text AS kpi_spec_version,
    CURRENT_DATE AS data_cutoff_date
FROM scored;

CREATE UNIQUE INDEX idx_source_freshness_summary
    ON analytics.source_freshness_summary(source_type, source_name);

DROP TABLE IF EXISTS analytics.source_quality_kpis CASCADE;
CREATE TABLE analytics.source_quality_kpis AS
WITH raw_vendor AS (
    SELECT
        source_vendor,
        COUNT(*) AS raw_rows,
        COUNT(*) FILTER (
            WHERE sku IS NOT NULL
              AND TRIM(sku) <> ''
              AND snapshot_date IS NOT NULL
              AND snapshot_date <= CURRENT_DATE
        ) AS eligible_rows,
        COUNT(*) FILTER (WHERE old_qty IS NULL OR new_qty IS NULL) AS null_quantity_rows,
        COUNT(*) FILTER (WHERE old_qty < 0 OR new_qty < 0) AS negative_quantity_rows,
        COUNT(*) FILTER (
            WHERE old_qty IS NULL
               OR new_qty IS NULL
               OR old_qty < 0
               OR new_qty < 0
        ) AS invalid_quantity_rows,
        COUNT(*) FILTER (
            WHERE prior_date > CURRENT_DATE OR prior_date > snapshot_date
        ) AS invalid_prior_date_rows,
        COUNT(*) FILTER (
            WHERE old_qty IS NULL
               OR new_qty IS NULL
               OR old_qty < 0
               OR new_qty < 0
               OR prior_date > CURRENT_DATE
               OR prior_date > snapshot_date
        ) AS invalid_quantity_date_rows,
        COUNT(*) FILTER (
            WHERE manufacturer IS NULL OR TRIM(manufacturer) = ''
        ) AS null_manufacturer_rows
    FROM raw.vendor_deltas
    GROUP BY source_vendor
),
clean_vendor AS (
    SELECT source_vendor, COUNT(*) AS clean_rows
    FROM clean.vendor_deltas
    GROUP BY source_vendor
),
rates AS (
    SELECT
        r.source_vendor,
        r.raw_rows,
        r.eligible_rows,
        c.clean_rows,
        r.eligible_rows - c.clean_rows AS exact_duplicate_rows_removed,
        r.null_quantity_rows,
        r.negative_quantity_rows,
        r.invalid_quantity_rows,
        r.invalid_prior_date_rows,
        r.invalid_quantity_date_rows,
        r.null_manufacturer_rows,
        100.0 * (r.eligible_rows - c.clean_rows) / NULLIF(r.raw_rows, 0)
            AS duplicate_sku_record_rate,
        100.0 * r.null_manufacturer_rows / NULLIF(r.raw_rows, 0)
            AS null_manufacturer_rate,
        100.0 * r.invalid_quantity_date_rows / NULLIF(r.raw_rows, 0)
            AS invalid_quantity_date_rate
    FROM raw_vendor r
    JOIN clean_vendor c USING (source_vendor)
)
SELECT
    r.source_vendor,
    r.raw_rows,
    r.eligible_rows,
    r.clean_rows,
    r.exact_duplicate_rows_removed,
    r.null_quantity_rows,
    r.negative_quantity_rows,
    r.invalid_quantity_rows,
    r.invalid_prior_date_rows,
    r.invalid_quantity_date_rows,
    r.null_manufacturer_rows,
    ROUND(r.duplicate_sku_record_rate, 4) AS duplicate_sku_record_rate,
    ROUND(r.null_manufacturer_rate, 4) AS null_manufacturer_rate,
    ROUND(
        100.0 * (r.raw_rows - r.invalid_quantity_rows) / NULLIF(r.raw_rows, 0),
        4
    ) AS quantity_change_consistency,
    ROUND(r.invalid_quantity_date_rate, 4) AS invalid_quantity_date_rate,
    f.freshness_days AS vendor_freshness_days,
    f.source_freshness_score AS vendor_freshness_index,
    LEAST(
        100,
        ROUND(
            r.duplicate_sku_record_rate * 0.25
          + r.null_manufacturer_rate * 0.20
          + r.invalid_quantity_date_rate * 0.35
          + (100 - f.source_freshness_score) * 0.20,
            2
        )
    ) AS data_quality_risk_score,
    '1.0'::text AS kpi_spec_version,
    CURRENT_DATE AS data_cutoff_date
FROM rates r
JOIN analytics.source_freshness_summary f
  ON f.source_type = 'vendor'
 AND f.source_name = r.source_vendor;

CREATE UNIQUE INDEX idx_source_quality_kpis_vendor
    ON analytics.source_quality_kpis(source_vendor);

DROP TABLE IF EXISTS analytics.sku_monthly_metrics CASCADE;
CREATE TABLE analytics.sku_monthly_metrics AS
WITH vendor_monthly AS (
    SELECT
        sku,
        DATE_TRUNC('month', snapshot_date)::date AS month_start,
        COALESCE(SUM(valid_units_depleted), 0) AS valid_depleted_units,
        COUNT(*) FILTER (WHERE is_valid_depletion_event) AS depletion_events,
        COUNT(*) FILTER (WHERE is_valid_stockout_transition) AS stockout_transitions
    FROM clean.vendor_deltas
    GROUP BY sku, DATE_TRUNC('month', snapshot_date)::date
),
sales_monthly AS (
    SELECT
        sku,
        DATE_TRUNC('month', created_at)::date AS month_start,
        COALESCE(SUM(valid_units_sold), 0) AS valid_units_sold,
        COALESCE(SUM(valid_revenue), 0) AS valid_revenue,
        COUNT(DISTINCT order_id) FILTER (WHERE valid_units_sold IS NOT NULL) AS orders
    FROM clean.sales_orders
    GROUP BY sku, DATE_TRUNC('month', created_at)::date
)
SELECT
    COALESCE(v.sku, s.sku) AS sku,
    COALESCE(v.month_start, s.month_start) AS month_start,
    COALESCE(v.valid_depleted_units, 0) AS valid_depleted_units,
    COALESCE(v.depletion_events, 0) AS depletion_events,
    COALESCE(v.stockout_transitions, 0) AS stockout_transitions,
    COALESCE(s.valid_units_sold, 0) AS valid_units_sold,
    COALESCE(s.valid_revenue, 0) AS valid_revenue,
    COALESCE(s.orders, 0) AS orders
FROM vendor_monthly v
FULL OUTER JOIN sales_monthly s USING (sku, month_start);

CREATE UNIQUE INDEX idx_sku_monthly_metrics
    ON analytics.sku_monthly_metrics(sku, month_start);
CREATE INDEX idx_sku_monthly_metrics_month
    ON analytics.sku_monthly_metrics(month_start);

DROP TABLE IF EXISTS analytics.sku_phase1_kpis CASCADE;
CREATE TABLE analytics.sku_phase1_kpis AS
WITH month_anchor AS (
    SELECT (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month')::date
        AS last_complete_month
),
temporal AS (
    SELECT
        m.sku,
        MIN(m.month_start) AS first_observed_month,
        MAX(m.month_start) AS latest_observed_month,
        (
            EXTRACT(YEAR FROM AGE(MAX(m.month_start), MIN(m.month_start))) * 12
          + EXTRACT(MONTH FROM AGE(MAX(m.month_start), MIN(m.month_start)))
          + 1
        )::integer AS observation_months,
        COUNT(*) FILTER (
            WHERE m.valid_depleted_units > 0 OR m.valid_units_sold > 0
        ) AS active_signal_months,
        SUM(m.valid_depleted_units) AS lifetime_depleted_units,
        SUM(m.valid_units_sold) AS lifetime_units_sold,
        SUM(m.valid_units_sold) FILTER (
            WHERE m.month_start BETWEEN a.last_complete_month - INTERVAL '5 months'
                                    AND a.last_complete_month
        ) AS recent_6m_sales_units,
        SUM(m.valid_units_sold) FILTER (
            WHERE m.month_start BETWEEN a.last_complete_month - INTERVAL '11 months'
                                    AND a.last_complete_month - INTERVAL '6 months'
        ) AS previous_6m_sales_units,
        SUM(m.valid_depleted_units) FILTER (
            WHERE m.month_start BETWEEN a.last_complete_month - INTERVAL '5 months'
                                    AND a.last_complete_month
        ) AS recent_6m_depleted_units,
        SUM(m.valid_depleted_units) FILTER (
            WHERE m.month_start BETWEEN a.last_complete_month - INTERVAL '11 months'
                                    AND a.last_complete_month - INTERVAL '6 months'
        ) AS previous_6m_depleted_units
    FROM analytics.sku_monthly_metrics m
    CROSS JOIN month_anchor a
    GROUP BY m.sku
),
event_stats AS (
    SELECT
        sku,
        MAX(valid_units_depleted) AS largest_depletion_event,
        COALESCE(SUM(valid_units_depleted), 0) AS total_depleted_units
    FROM clean.vendor_deltas
    GROUP BY sku
),
supplier_stats AS (
    SELECT
        d.sku,
        COUNT(DISTINCT d.source_vendor) AS historical_vendor_count,
        COUNT(DISTINCT d.source_vendor) FILTER (
            WHERE f.freshness_days <= 30
        ) AS fresh_vendor_count
    FROM clean.vendor_deltas d
    JOIN analytics.source_freshness_summary f
      ON f.source_type = 'vendor'
     AND f.source_name = d.source_vendor
    GROUP BY d.sku
),
features AS (
    SELECT
        m.*,
        t.first_observed_month,
        t.latest_observed_month,
        COALESCE(t.observation_months, 0) AS observation_months,
        COALESCE(t.active_signal_months, 0) AS active_signal_months,
        ROUND(
            100.0 * COALESCE(t.active_signal_months, 0)
            / NULLIF(t.observation_months, 0),
            2
        ) AS demand_consistency_score,
        ROUND(
            COALESCE(m.depletion_events, 0)::numeric
            / NULLIF(t.observation_months, 0),
            4
        ) AS movement_frequency_index,
        COALESCE(e.largest_depletion_event, 0) AS largest_depletion_event,
        ROUND(
            100.0 * COALESCE(e.largest_depletion_event, 0)
            / NULLIF(e.total_depleted_units, 0),
            2
        ) AS largest_event_share_pct,
        (
            COALESCE(e.largest_depletion_event, 0) >= 50
            AND COALESCE(e.largest_depletion_event, 0)
                / NULLIF(e.total_depleted_units, 0) >= 0.50
        ) AS bulk_shock_flag,
        COALESCE(s.fresh_vendor_count, 0) AS fresh_vendor_count,
        ROUND(
            100.0 * (
                0.70 * COALESCE(s.fresh_vendor_count, 0)
                    / NULLIF(s.historical_vendor_count, 0)
              + 0.30 * LEAST(COALESCE(s.historical_vendor_count, 0) / 3.0, 1.0)
            ),
            2
        ) AS multi_vendor_strength_score,
        CASE
            WHEN m.has_store_sales_history AND m.historical_vendor_count = 0
                THEN 'NO_VENDOR_HISTORY'
            WHEN m.has_store_sales_history AND m.historical_vendor_count = 1
                THEN 'SINGLE_VENDOR_DEPENDENCY'
            WHEN m.has_store_sales_history AND m.historical_vendor_count >= 2
                THEN 'MULTI_VENDOR'
            ELSE 'NO_STORE_SALES_HISTORY'
        END AS vendor_dependency_risk,
        CASE
            WHEN COALESCE(t.lifetime_units_sold, 0) > 0
                THEN COALESCE(t.recent_6m_sales_units, 0)
            ELSE COALESCE(t.recent_6m_depleted_units, 0)
        END AS recent_6m_demand_signal,
        CASE
            WHEN COALESCE(t.lifetime_units_sold, 0) > 0
                THEN COALESCE(t.previous_6m_sales_units, 0)
            ELSE COALESCE(t.previous_6m_depleted_units, 0)
        END AS previous_6m_demand_signal,
        COALESCE(t.lifetime_units_sold, 0) + COALESCE(t.lifetime_depleted_units, 0)
            AS lifetime_demand_signal
    FROM analytics.sku_master_intelligence m
    LEFT JOIN temporal t USING (sku)
    LEFT JOIN event_stats e USING (sku)
    LEFT JOIN supplier_stats s USING (sku)
),
patterns AS (
    SELECT
        f.*,
        CASE
            WHEN lifetime_demand_signal = 0 THEN 'NO_OBSERVED_DEMAND'
            WHEN recent_6m_demand_signal = 0 AND previous_6m_demand_signal = 0
                THEN 'DORMANT'
            WHEN previous_6m_demand_signal = 0 AND recent_6m_demand_signal > 0
                THEN 'NEW_OR_REACTIVATED'
            WHEN recent_6m_demand_signal >= previous_6m_demand_signal * 1.25
                THEN 'GROWING'
            WHEN recent_6m_demand_signal <= previous_6m_demand_signal * 0.75
                THEN 'DECLINING'
            ELSE 'STABLE'
        END AS demand_pattern
    FROM features f
)
SELECT
    p.*,
    CASE
        WHEN valid_units_sold > 0 AND valid_depleted_units = 0 THEN 'SALES_ONLY'
        WHEN valid_units_sold = 0 AND valid_depleted_units = 0 THEN 'NO_OBSERVED_MOVEMENT'
        WHEN bulk_shock_flag THEN 'BULK_SHOCK_DOMINATED'
        WHEN demand_consistency_score >= 60 AND depletion_events >= 10
            AND depletion_credibility_flag = 'PLAUSIBLE'
            THEN 'CONSISTENT_RECURRING'
        WHEN demand_pattern = 'GROWING' THEN 'GROWING'
        WHEN demand_pattern = 'DECLINING' THEN 'DECLINING'
        ELSE 'INTERMITTENT'
    END AS behavioral_classification
FROM patterns p;

CREATE UNIQUE INDEX idx_sku_phase1_kpis_sku
    ON analytics.sku_phase1_kpis(sku);
CREATE INDEX idx_sku_phase1_kpis_behavior
    ON analytics.sku_phase1_kpis(behavioral_classification);
CREATE INDEX idx_sku_phase1_kpis_demand_pattern
    ON analytics.sku_phase1_kpis(demand_pattern);

DROP TABLE IF EXISTS analytics.monthly_trend_summary CASCADE;
CREATE TABLE analytics.monthly_trend_summary AS
SELECT
    k.inferred_category_segment,
    m.month_start,
    SUM(m.valid_depleted_units) AS valid_depleted_units,
    SUM(m.depletion_events) AS depletion_events,
    SUM(m.stockout_transitions) AS stockout_transitions,
    SUM(m.valid_units_sold) AS valid_units_sold,
    SUM(m.valid_revenue) AS valid_revenue,
    SUM(m.orders) AS orders,
    '1.0'::text AS kpi_spec_version
FROM analytics.sku_monthly_metrics m
JOIN analytics.sku_master_intelligence k USING (sku)
GROUP BY k.inferred_category_segment, m.month_start;

CREATE UNIQUE INDEX idx_monthly_trend_summary
    ON analytics.monthly_trend_summary(inferred_category_segment, month_start);

SET LOCAL max_parallel_workers_per_gather = 0;
SET LOCAL work_mem = '16MB';

DROP TABLE IF EXISTS analytics.monthly_trend_summary_catalog CASCADE;
CREATE TABLE analytics.monthly_trend_summary_catalog AS
SELECT
    COALESCE(
        'GOOGLE_PRODUCT_CATEGORY_' || c.google_product_category,
        k.inferred_category_segment
    ) AS category_segment,
    c.google_product_category IS NOT NULL AS category_is_authoritative,
    m.month_start,
    SUM(m.valid_depleted_units) AS valid_depleted_units,
    SUM(m.depletion_events) AS depletion_events,
    SUM(m.stockout_transitions) AS stockout_transitions,
    SUM(m.valid_units_sold) AS valid_units_sold,
    SUM(m.valid_revenue) AS valid_revenue,
    SUM(m.orders) AS orders,
    '1.0'::text AS kpi_spec_version
FROM analytics.sku_monthly_metrics m
JOIN analytics.sku_master_intelligence k USING (sku)
LEFT JOIN analytics.sku_catalog_current c USING (sku)
GROUP BY
    COALESCE(
        'GOOGLE_PRODUCT_CATEGORY_' || c.google_product_category,
        k.inferred_category_segment
    ),
    c.google_product_category IS NOT NULL,
    m.month_start;

CREATE UNIQUE INDEX idx_monthly_trend_summary_catalog
    ON analytics.monthly_trend_summary_catalog(
        category_segment,
        category_is_authoritative,
        month_start
    );

SET LOCAL max_parallel_workers_per_gather = DEFAULT;
SET LOCAL work_mem = DEFAULT;

DROP TABLE IF EXISTS analytics.capital_concentration_summary CASCADE;
CREATE TABLE analytics.capital_concentration_summary AS
WITH metric_values AS (
    SELECT sku, 'STORE_REVENUE'::text AS metric, valid_revenue AS metric_value
    FROM analytics.sku_master_intelligence
    WHERE valid_revenue > 0
    UNION ALL
    SELECT sku, 'VENDOR_DEPLETION_UNITS'::text, valid_depleted_units
    FROM analytics.sku_master_intelligence
    WHERE valid_depleted_units > 0
),
ranked AS (
    SELECT
        metric,
        sku,
        metric_value,
        ROW_NUMBER() OVER (PARTITION BY metric ORDER BY metric_value DESC, sku) AS rank
    FROM metric_values
),
totals AS (
    SELECT metric, SUM(metric_value) AS total_value
    FROM ranked
    GROUP BY metric
),
cutoffs(top_n) AS (
    VALUES (10), (50), (200)
)
SELECT
    r.metric,
    c.top_n,
    t.total_value,
    SUM(r.metric_value) FILTER (WHERE r.rank <= c.top_n) AS top_value,
    ROUND(
        100.0 * SUM(r.metric_value) FILTER (WHERE r.rank <= c.top_n)
        / NULLIF(t.total_value, 0),
        2
    ) AS concentration_pct,
    '1.0'::text AS kpi_spec_version,
    CURRENT_DATE AS data_cutoff_date
FROM ranked r
JOIN totals t USING (metric)
CROSS JOIN cutoffs c
GROUP BY r.metric, c.top_n, t.total_value;

CREATE UNIQUE INDEX idx_capital_concentration_summary
    ON analytics.capital_concentration_summary(metric, top_n);

CREATE OR REPLACE VIEW analytics.preliminary_top_200_focus_parts AS
SELECT
    ROW_NUMBER() OVER (ORDER BY k.preliminary_priority_score DESC, k.sku) AS rank,
    k.sku,
    k.product_name,
    k.manufacturer,
    k.inferred_category_segment,
    k.historical_vendor_count,
    k.historical_vendors,
    k.store_count,
    k.stores,
    k.valid_depleted_units,
    ROUND(k.preliminary_daily_depletion_rate, 4) AS preliminary_daily_depletion_rate,
    k.valid_units_sold,
    k.valid_revenue,
    k.order_count,
    k.average_selling_price,
    k.latest_snapshot_date,
    k.vendor_freshness_days,
    k.latest_sale_date,
    k.sales_freshness_days,
    k.preliminary_priority_score,
    k.kpi_spec_version,
    k.data_cutoff_date,
    p.demand_consistency_score,
    p.movement_frequency_index,
    p.bulk_shock_flag,
    p.demand_pattern,
    p.behavioral_classification,
    p.fresh_vendor_count,
    p.multi_vendor_strength_score,
    p.vendor_dependency_risk,
    p.peak_observed_quantity,
    p.depletion_to_peak_ratio,
    p.depletion_credibility_flag
FROM analytics.sku_master_intelligence k
JOIN analytics.sku_phase1_kpis p USING (sku)
ORDER BY k.preliminary_priority_score DESC, k.sku
LIMIT 200;
