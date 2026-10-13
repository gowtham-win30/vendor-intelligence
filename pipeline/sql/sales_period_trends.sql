CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.sales_period_summary CASCADE;
CREATE TABLE analytics.sales_period_summary AS
WITH valid_sales AS (
    SELECT
        source_store,
        created_at::date AS sale_date,
        valid_units_sold,
        valid_revenue,
        order_id,
        sku
    FROM clean.sales_orders
    WHERE valid_units_sold IS NOT NULL
),
cutoff AS (
    SELECT CURRENT_DATE AS data_cutoff_date
),
store_bounds AS (
    SELECT
        source_store,
        MIN(sale_date) AS first_date,
        MAX(sale_date) AS last_date
    FROM valid_sales
    GROUP BY source_store
),
network_bounds AS (
    SELECT
        'NETWORK'::text AS source_store,
        MIN(sale_date) AS first_date,
        MAX(sale_date) AS last_date
    FROM valid_sales
),
day_spine AS (
    SELECT
        b.source_store,
        gs::date AS period_start,
        gs::date AS period_end,
        'DAY'::text AS period_grain
    FROM store_bounds b
    CROSS JOIN LATERAL generate_series(b.first_date, b.last_date, INTERVAL '1 day') gs
    UNION ALL
    SELECT
        n.source_store,
        gs::date,
        gs::date,
        'DAY'
    FROM network_bounds n
    CROSS JOIN LATERAL generate_series(n.first_date, n.last_date, INTERVAL '1 day') gs
),
week_spine AS (
    SELECT
        b.source_store,
        gs::date AS period_start,
        (gs + INTERVAL '6 days')::date AS period_end,
        'WEEK'::text AS period_grain
    FROM store_bounds b
    CROSS JOIN LATERAL generate_series(
        DATE_TRUNC('week', b.first_date)::date,
        DATE_TRUNC('week', b.last_date)::date,
        INTERVAL '1 week'
    ) gs
    UNION ALL
    SELECT
        n.source_store,
        gs::date,
        (gs + INTERVAL '6 days')::date,
        'WEEK'
    FROM network_bounds n
    CROSS JOIN LATERAL generate_series(
        DATE_TRUNC('week', n.first_date)::date,
        DATE_TRUNC('week', n.last_date)::date,
        INTERVAL '1 week'
    ) gs
),
month_spine AS (
    SELECT
        b.source_store,
        gs::date AS period_start,
        (DATE_TRUNC('month', gs) + INTERVAL '1 month - 1 day')::date AS period_end,
        'MONTH'::text AS period_grain
    FROM store_bounds b
    CROSS JOIN LATERAL generate_series(
        DATE_TRUNC('month', b.first_date)::date,
        DATE_TRUNC('month', b.last_date)::date,
        INTERVAL '1 month'
    ) gs
    UNION ALL
    SELECT
        n.source_store,
        gs::date,
        (DATE_TRUNC('month', gs) + INTERVAL '1 month - 1 day')::date,
        'MONTH'
    FROM network_bounds n
    CROSS JOIN LATERAL generate_series(
        DATE_TRUNC('month', n.first_date)::date,
        DATE_TRUNC('month', n.last_date)::date,
        INTERVAL '1 month'
    ) gs
),
spine AS (
    SELECT * FROM day_spine
    UNION ALL
    SELECT * FROM week_spine
    UNION ALL
    SELECT * FROM month_spine
),
fact AS (
    SELECT
        source_store,
        sale_date AS period_start,
        'DAY'::text AS period_grain,
        SUM(valid_units_sold) AS valid_units_sold,
        SUM(valid_revenue) AS valid_revenue,
        COUNT(DISTINCT order_id) AS orders,
        COUNT(DISTINCT sku) AS distinct_skus_sold
    FROM valid_sales
    GROUP BY source_store, sale_date
    UNION ALL
    SELECT
        'NETWORK',
        sale_date,
        'DAY',
        SUM(valid_units_sold),
        SUM(valid_revenue),
        COUNT(DISTINCT (source_store || ':' || COALESCE(order_id, sku))),
        COUNT(DISTINCT sku)
    FROM valid_sales
    GROUP BY sale_date
    UNION ALL
    SELECT
        source_store,
        DATE_TRUNC('week', sale_date)::date,
        'WEEK',
        SUM(valid_units_sold),
        SUM(valid_revenue),
        COUNT(DISTINCT order_id),
        COUNT(DISTINCT sku)
    FROM valid_sales
    GROUP BY source_store, DATE_TRUNC('week', sale_date)::date
    UNION ALL
    SELECT
        'NETWORK',
        DATE_TRUNC('week', sale_date)::date,
        'WEEK',
        SUM(valid_units_sold),
        SUM(valid_revenue),
        COUNT(DISTINCT (source_store || ':' || COALESCE(order_id, sku))),
        COUNT(DISTINCT sku)
    FROM valid_sales
    GROUP BY DATE_TRUNC('week', sale_date)::date
    UNION ALL
    SELECT
        source_store,
        DATE_TRUNC('month', sale_date)::date,
        'MONTH',
        SUM(valid_units_sold),
        SUM(valid_revenue),
        COUNT(DISTINCT order_id),
        COUNT(DISTINCT sku)
    FROM valid_sales
    GROUP BY source_store, DATE_TRUNC('month', sale_date)::date
    UNION ALL
    SELECT
        'NETWORK',
        DATE_TRUNC('month', sale_date)::date,
        'MONTH',
        SUM(valid_units_sold),
        SUM(valid_revenue),
        COUNT(DISTINCT (source_store || ':' || COALESCE(order_id, sku))),
        COUNT(DISTINCT sku)
    FROM valid_sales
    GROUP BY DATE_TRUNC('month', sale_date)::date
)
SELECT
    s.period_grain,
    s.period_start,
    s.period_end,
    s.source_store,
    COALESCE(f.valid_units_sold, 0) AS valid_units_sold,
    COALESCE(f.valid_revenue, 0) AS valid_revenue,
    COALESCE(f.orders, 0) AS orders,
    COALESCE(f.distinct_skus_sold, 0) AS distinct_skus_sold,
    (
        s.period_end < c.data_cutoff_date
    ) AS period_is_complete,
    '1.2'::text AS kpi_spec_version,
    c.data_cutoff_date,
    'Observed storefront demand only. Not vendor depletion, margin, or forecast.'::text
        AS usage_limitation
FROM spine s
CROSS JOIN cutoff c
LEFT JOIN fact f
  ON f.source_store = s.source_store
 AND f.period_grain = s.period_grain
 AND f.period_start = s.period_start;

CREATE UNIQUE INDEX idx_sales_period_summary_grain
    ON analytics.sales_period_summary(period_grain, period_start, source_store);
CREATE INDEX idx_sales_period_summary_store
    ON analytics.sales_period_summary(source_store, period_grain, period_start);
