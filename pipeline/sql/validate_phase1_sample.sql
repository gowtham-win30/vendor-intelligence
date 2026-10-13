CREATE SCHEMA IF NOT EXISTS analytics;

DROP VIEW IF EXISTS analytics.phase1_sample_reconciliation;
DROP TABLE IF EXISTS analytics.phase1_validation_sample;
CREATE TABLE analytics.phase1_validation_sample AS
WITH top_score AS (
    SELECT sku, 'TOP_SCORE'::text AS sample_reason, 1 AS sample_priority
    FROM analytics.sku_master_intelligence
    ORDER BY preliminary_priority_score DESC, sku
    LIMIT 5
),
high_gap AS (
    SELECT sku, 'HIGH_GAP'::text AS sample_reason, 2 AS sample_priority
    FROM analytics.sku_master_intelligence
    WHERE preliminary_gap_class = 'HIGH_GAP_CANDIDATE'
      AND sku NOT IN (SELECT sku FROM top_score)
    ORDER BY valid_depleted_units DESC, sku
    LIMIT 5
),
no_movement AS (
    SELECT sku, 'NO_OBSERVED_MOVEMENT'::text AS sample_reason, 3 AS sample_priority
    FROM analytics.no_observed_movement_candidates
    WHERE sku NOT IN (
        SELECT sku FROM top_score
        UNION ALL
        SELECT sku FROM high_gap
    )
    ORDER BY historical_vendor_count DESC, sku
    LIMIT 4
),
negative_quantity AS (
    SELECT d.sku, 'NEGATIVE_QUANTITY'::text AS sample_reason, 4 AS sample_priority
    FROM clean.vendor_deltas d
    WHERE d.has_negative_quantity
      AND d.sku NOT IN (
          SELECT sku FROM top_score
          UNION ALL
          SELECT sku FROM high_gap
          UNION ALL
          SELECT sku FROM no_movement
      )
    GROUP BY d.sku
    ORDER BY COUNT(*) DESC, d.sku
    LIMIT 2
),
multiple_same_day AS (
    SELECT d.sku, 'MULTIPLE_SAME_DAY_TRANSITIONS'::text AS sample_reason, 5 AS sample_priority
    FROM clean.vendor_deltas d
    WHERE d.has_multiple_same_day_transitions
      AND d.sku NOT IN (
          SELECT sku FROM top_score
          UNION ALL
          SELECT sku FROM high_gap
          UNION ALL
          SELECT sku FROM no_movement
          UNION ALL
          SELECT sku FROM negative_quantity
      )
    GROUP BY d.sku
    ORDER BY COUNT(*) DESC, d.sku
    LIMIT 2
),
stockout AS (
    SELECT d.sku, 'STOCKOUT_TRANSITION'::text AS sample_reason, 6 AS sample_priority
    FROM clean.vendor_deltas d
    WHERE d.is_valid_stockout_transition
      AND d.sku NOT IN (
          SELECT sku FROM top_score
          UNION ALL
          SELECT sku FROM high_gap
          UNION ALL
          SELECT sku FROM no_movement
          UNION ALL
          SELECT sku FROM negative_quantity
          UNION ALL
          SELECT sku FROM multiple_same_day
      )
    GROUP BY d.sku
    ORDER BY COUNT(*) DESC, d.sku
    LIMIT 2
)
SELECT
    sample_priority,
    sample_reason,
    sku,
    '1.1'::text AS kpi_spec_version,
    CURRENT_DATE AS selected_at
FROM (
    SELECT * FROM top_score
    UNION ALL
    SELECT * FROM high_gap
    UNION ALL
    SELECT * FROM no_movement
    UNION ALL
    SELECT * FROM negative_quantity
    UNION ALL
    SELECT * FROM multiple_same_day
    UNION ALL
    SELECT * FROM stockout
) sample;

ALTER TABLE analytics.phase1_validation_sample
    ADD CONSTRAINT phase1_validation_sample_pk PRIMARY KEY (sku);

CREATE OR REPLACE VIEW analytics.phase1_sample_reconciliation AS
WITH raw_vendor AS (
    SELECT
        UPPER(TRIM(r.sku)) AS sku,
        COUNT(*) AS raw_vendor_rows
    FROM raw.vendor_deltas r
    JOIN analytics.phase1_validation_sample s
      ON s.sku = UPPER(TRIM(r.sku))
    GROUP BY UPPER(TRIM(r.sku))
),
clean_vendor AS (
    SELECT
        d.sku,
        COUNT(*) AS clean_vendor_rows,
        COALESCE(SUM(d.valid_units_depleted), 0) AS calculated_depleted_units,
        COUNT(*) FILTER (WHERE d.is_valid_depletion_event) AS calculated_depletion_events,
        COUNT(*) FILTER (WHERE d.is_valid_stockout_transition) AS calculated_stockout_transitions,
        COUNT(*) FILTER (WHERE d.has_negative_quantity) AS negative_quantity_records,
        COUNT(*) FILTER (WHERE d.has_null_quantity) AS null_quantity_records,
        COUNT(*) FILTER (WHERE d.has_invalid_prior_date) AS invalid_prior_date_records,
        COUNT(*) FILTER (WHERE d.has_multiple_same_day_transitions)
            AS multiple_same_day_transition_records
    FROM clean.vendor_deltas d
    JOIN analytics.phase1_validation_sample s USING (sku)
    GROUP BY d.sku
),
raw_sales AS (
    SELECT
        UPPER(TRIM(r.sku)) AS sku,
        COUNT(*) AS raw_sales_rows
    FROM raw.sales_orders r
    JOIN analytics.phase1_validation_sample s
      ON s.sku = UPPER(TRIM(r.sku))
    GROUP BY UPPER(TRIM(r.sku))
),
clean_sales AS (
    SELECT
        d.sku,
        COUNT(*) AS clean_sales_rows,
        COALESCE(SUM(d.valid_units_sold), 0) AS calculated_units_sold,
        COALESCE(SUM(d.valid_revenue), 0) AS calculated_revenue,
        COUNT(DISTINCT d.order_id) FILTER (WHERE d.valid_units_sold IS NOT NULL)
            AS calculated_order_count
    FROM clean.sales_orders d
    JOIN analytics.phase1_validation_sample s USING (sku)
    GROUP BY d.sku
),
comparison AS (
    SELECT
        s.sample_priority,
        s.sample_reason,
        s.sku,
        COALESCE(rv.raw_vendor_rows, 0) AS raw_vendor_rows,
        COALESCE(cv.clean_vendor_rows, 0) AS clean_vendor_rows,
        COALESCE(cv.calculated_depleted_units, 0) AS calculated_depleted_units,
        COALESCE(m.valid_depleted_units, 0) AS published_depleted_units,
        COALESCE(cv.calculated_depletion_events, 0) AS calculated_depletion_events,
        COALESCE(m.depletion_events, 0) AS published_depletion_events,
        COALESCE(cv.calculated_stockout_transitions, 0) AS calculated_stockout_transitions,
        COALESCE(m.stockout_transitions, 0) AS published_stockout_transitions,
        COALESCE(cv.negative_quantity_records, 0) AS negative_quantity_records,
        COALESCE(cv.null_quantity_records, 0) AS null_quantity_records,
        COALESCE(cv.invalid_prior_date_records, 0) AS invalid_prior_date_records,
        COALESCE(cv.multiple_same_day_transition_records, 0)
            AS multiple_same_day_transition_records,
        COALESCE(rs.raw_sales_rows, 0) AS raw_sales_rows,
        COALESCE(cs.clean_sales_rows, 0) AS clean_sales_rows,
        COALESCE(cs.calculated_units_sold, 0) AS calculated_units_sold,
        COALESCE(m.valid_units_sold, 0) AS published_units_sold,
        COALESCE(cs.calculated_revenue, 0) AS calculated_revenue,
        COALESCE(m.valid_revenue, 0) AS published_revenue,
        COALESCE(cs.calculated_order_count, 0) AS calculated_order_count,
        COALESCE(m.order_count, 0) AS published_order_count,
        s.kpi_spec_version,
        s.selected_at
    FROM analytics.phase1_validation_sample s
    JOIN analytics.sku_master_intelligence m USING (sku)
    LEFT JOIN raw_vendor rv USING (sku)
    LEFT JOIN clean_vendor cv USING (sku)
    LEFT JOIN raw_sales rs USING (sku)
    LEFT JOIN clean_sales cs USING (sku)
)
SELECT
    *,
    calculated_depleted_units = published_depleted_units AS depletion_units_match,
    calculated_depletion_events = published_depletion_events AS depletion_events_match,
    calculated_stockout_transitions = published_stockout_transitions AS stockout_transitions_match,
    calculated_units_sold = published_units_sold AS units_sold_match,
    ABS(calculated_revenue - published_revenue) < 0.01 AS revenue_match,
    calculated_order_count = published_order_count AS order_count_match,
    (
        calculated_depleted_units = published_depleted_units
        AND calculated_depletion_events = published_depletion_events
        AND calculated_stockout_transitions = published_stockout_transitions
        AND calculated_units_sold = published_units_sold
        AND ABS(calculated_revenue - published_revenue) < 0.01
        AND calculated_order_count = published_order_count
    ) AS internal_reconciliation_pass,
    'SEE_SOURCE_RECONCILIATION_REPORT'::text AS source_validation_status
FROM comparison;
