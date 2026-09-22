CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS raw.vendor_deltas (
    source_vendor TEXT NOT NULL,
    source_table TEXT NOT NULL,
    prior_date DATE,
    snapshot_date DATE,
    sku TEXT NOT NULL,
    manufacturer TEXT,
    old_qty NUMERIC,
    new_qty NUMERIC,
    old_price NUMERIC(18, 4),
    new_price NUMERIC(18, 4),
    month_label TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.sales_orders (
    source_store TEXT NOT NULL,
    source_table TEXT NOT NULL,
    order_id TEXT,
    customer_email TEXT,
    sku TEXT,
    created_at TIMESTAMP,
    qty_ordered NUMERIC,
    price NUMERIC(18, 4),
    region TEXT,
    postcode TEXT,
    company TEXT,
    customer_first_name TEXT,
    customer_last_name TEXT,
    phone TEXT,
    product_name TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.current_inventory (
    source_store TEXT NOT NULL,
    source_table TEXT NOT NULL,
    sku TEXT NOT NULL,
    manufacturer TEXT,
    quantity NUMERIC,
    price NUMERIC(18, 4),
    list_price NUMERIC(18, 4),
    weight NUMERIC,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.merchant_catalog (
    source_store TEXT NOT NULL,
    source_table TEXT NOT NULL,
    sku TEXT NOT NULL,
    mpn TEXT,
    product_name TEXT,
    description TEXT,
    product_link TEXT,
    image_link TEXT,
    price NUMERIC(18, 4),
    sale_price NUMERIC(18, 4),
    brand TEXT,
    availability TEXT,
    condition TEXT,
    google_product_category TEXT,
    product_type TEXT,
    shipment_weight NUMERIC,
    status_value INTEGER,
    product_detail TEXT,
    custom_label_0 TEXT,
    shipping TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit.ingestion_runs (
    run_id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_table TEXT NOT NULL,
    target_table TEXT NOT NULL,
    rows_loaded BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS audit.transformation_runs (
    run_id UUID PRIMARY KEY,
    pipeline_version TEXT NOT NULL,
    kpi_spec_version TEXT NOT NULL,
    data_cutoff_date DATE NOT NULL,
    source_loaded_at TIMESTAMPTZ,
    transformation_started_at TIMESTAMPTZ NOT NULL,
    transformation_completed_at TIMESTAMPTZ,
    row_counts JSONB,
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS audit.data_quality_profile_runs (
    run_id UUID PRIMARY KEY,
    profile_version TEXT NOT NULL,
    data_cutoff_date DATE NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    profiled_tables INTEGER,
    profiled_columns INTEGER,
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS audit.data_quality_column_profiles (
    run_id UUID NOT NULL REFERENCES audit.data_quality_profile_runs(run_id),
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    source_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    data_type TEXT NOT NULL,
    total_rows BIGINT NOT NULL,
    null_rows BIGINT NOT NULL,
    blank_rows BIGINT NOT NULL,
    missing_rows BIGINT NOT NULL,
    missing_pct NUMERIC(9, 4) NOT NULL,
    null_policy TEXT NOT NULL,
    kpi_impact TEXT NOT NULL,
    trust_status TEXT NOT NULL,
    PRIMARY KEY (run_id, schema_name, table_name, source_name, column_name)
);

CREATE UNLOGGED TABLE IF NOT EXISTS staging.vendor_deltas
    (LIKE raw.vendor_deltas INCLUDING DEFAULTS);

CREATE UNLOGGED TABLE IF NOT EXISTS staging.sales_orders
    (LIKE raw.sales_orders INCLUDING DEFAULTS);
CREATE UNLOGGED TABLE IF NOT EXISTS staging.current_inventory
    (LIKE raw.current_inventory INCLUDING DEFAULTS);
CREATE UNLOGGED TABLE IF NOT EXISTS staging.merchant_catalog
    (LIKE raw.merchant_catalog INCLUDING DEFAULTS);

CREATE INDEX IF NOT EXISTS idx_vendor_deltas_source_date ON raw.vendor_deltas(source_vendor, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_vendor_deltas_sku_date ON raw.vendor_deltas(sku, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_vendor_deltas_manufacturer ON raw.vendor_deltas(manufacturer);

CREATE INDEX IF NOT EXISTS idx_sales_orders_store_date ON raw.sales_orders(source_store, created_at);
CREATE INDEX IF NOT EXISTS idx_sales_orders_sku_date ON raw.sales_orders(sku, created_at);
CREATE INDEX IF NOT EXISTS idx_sales_orders_order_id ON raw.sales_orders(order_id);

CREATE INDEX IF NOT EXISTS idx_current_inventory_store_sku
    ON raw.current_inventory(source_store, sku);
CREATE INDEX IF NOT EXISTS idx_merchant_catalog_store_sku
    ON raw.merchant_catalog(source_store, sku);
CREATE INDEX IF NOT EXISTS idx_merchant_catalog_product_type
    ON raw.merchant_catalog(product_type);

DROP VIEW IF EXISTS analytics.vendor_daily_movement;
DROP VIEW IF EXISTS analytics.sales_daily_movement;
