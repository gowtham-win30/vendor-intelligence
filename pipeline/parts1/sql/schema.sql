-- Thin tables in the target SQL Server database (default Parts1), schema vendor_intelligence.
-- The schema is owned by the loading login so it has full DML on these objects;
-- other Parts1 readers can be granted SELECT on this schema.
-- pipeline/load_mssql_thin.py switches to Parts1, ensures the schema exists,
-- fills the _next tables, then renames them into place inside one transaction.
-- sales_line and vendor_change are copied server-side from the Parts database.
-- local_offer comes from the E: files pushed by the client.
-- Customer name, email, phone, and address are not copied.

IF OBJECT_ID('vendor_intelligence.load_run') IS NULL
CREATE TABLE vendor_intelligence.load_run (
    run_id BIGINT IDENTITY(1, 1) PRIMARY KEY,
    started_at DATETIME2 NOT NULL,
    completed_at DATETIME2 NULL,
    status VARCHAR(32) NOT NULL,
    sales_rows BIGINT NULL,
    vendor_change_rows BIGINT NULL,
    local_offer_rows BIGINT NULL,
    note NVARCHAR(400) NULL
);
GO

IF OBJECT_ID('vendor_intelligence.load_reject') IS NULL
CREATE TABLE vendor_intelligence.load_reject (
    run_id BIGINT NOT NULL,
    reason_code VARCHAR(64) NOT NULL,
    vendor_code VARCHAR(64) NULL,
    file_name NVARCHAR(260) NULL,
    rejected_count BIGINT NOT NULL,
    detail NVARCHAR(400) NULL
);
GO

IF OBJECT_ID('vendor_intelligence.sales_line_next') IS NOT NULL DROP TABLE vendor_intelligence.sales_line_next;
GO
CREATE TABLE vendor_intelligence.sales_line_next (
    store_code VARCHAR(32) NOT NULL,
    order_id VARCHAR(128) NOT NULL,
    sku VARCHAR(128) NOT NULL,
    sold_on DATE NOT NULL,
    units DECIMAL(18, 4) NOT NULL,
    unit_price DECIMAL(18, 4) NULL
);
GO
CREATE INDEX idx_sales_next_sold_on ON vendor_intelligence.sales_line_next (sold_on);
CREATE INDEX idx_sales_next_sku ON vendor_intelligence.sales_line_next (sku);
GO

IF OBJECT_ID('vendor_intelligence.vendor_change_next') IS NOT NULL DROP TABLE vendor_intelligence.vendor_change_next;
GO
CREATE TABLE vendor_intelligence.vendor_change_next (
    vendor_code VARCHAR(64) NOT NULL,
    sku VARCHAR(128) NOT NULL,
    change_date DATE NOT NULL,
    qty_before DECIMAL(18, 4) NULL,
    qty_after DECIMAL(18, 4) NULL
);
GO
CREATE INDEX idx_vc_next_vendor_date ON vendor_intelligence.vendor_change_next (vendor_code, change_date);
CREATE INDEX idx_vc_next_sku ON vendor_intelligence.vendor_change_next (sku);
GO

IF OBJECT_ID('vendor_intelligence.local_offer_next') IS NOT NULL DROP TABLE vendor_intelligence.local_offer_next;
GO
CREATE TABLE vendor_intelligence.local_offer_next (
    vendor_code VARCHAR(64) NOT NULL,
    snapshot_date DATE NOT NULL,
    manufacturer VARCHAR(255) NOT NULL,
    part_code VARCHAR(128) NOT NULL,
    qty_on_hand VARCHAR(32) NULL,
    price DECIMAL(18, 4) NULL,
    price_kind VARCHAR(32) NOT NULL
);
GO
CREATE INDEX idx_lo_next_key ON vendor_intelligence.local_offer_next (vendor_code, snapshot_date, part_code);
GO
