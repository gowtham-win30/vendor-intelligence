-- Parts1.vendor_intelligence  -  the three data tables the agent reads.
-- Used to compare what we sold, how vendor stock quantity changed, and the price
-- in the vendor files. No customer data. price/unit_price are not confirmed cost.
-- The notes beside the columns are for reading; SQL Server does not store them.
-- load_run, load_reject, local_offer_stage, and the dbo.*_next leftovers are
-- operational or dead and are intentionally left out of this file.

-- What we sold. One row is one order line with units greater than zero.
CREATE TABLE vendor_intelligence.sales_line (
    store_code VARCHAR(32) NOT NULL,       -- store that sold it: PartsFE, PartsHNC, APZ, WTB, PM, or HT
    order_id   VARCHAR(128) NOT NULL,      -- the order number
    sku        VARCHAR(128) NOT NULL,      -- the part that was sold
    sold_on    DATE NOT NULL,              -- the day it was sold
    units      DECIMAL(18, 4) NOT NULL,    -- how many were sold
    unit_price DECIMAL(18, 4) NULL         -- price for one unit; blank when the source price was missing or negative
);
GO
CREATE INDEX idx_sales_next_sold_on ON vendor_intelligence.sales_line (sold_on);
CREATE INDEX idx_sales_next_sku ON vendor_intelligence.sales_line (sku);
GO

-- How a vendor's stock quantity changed. One row is one change. This table has no price.
CREATE TABLE vendor_intelligence.vendor_change (
    vendor_code VARCHAR(64) NOT NULL,      -- vendor: Neuco, Metropac, EC, Msupply, or ReliableParts
    sku         VARCHAR(128) NOT NULL,     -- the vendor's part number
    change_date DATE NOT NULL,             -- the day the quantity changed
    qty_before  DECIMAL(18, 4) NULL,       -- quantity before the change
    qty_after   DECIMAL(18, 4) NULL        -- quantity after the change; 0 means stock became zero that day
);
GO
CREATE INDEX idx_vc_next_vendor_date ON vendor_intelligence.vendor_change (vendor_code, change_date);
CREATE INDEX idx_vc_next_sku ON vendor_intelligence.vendor_change (sku);
GO

-- A vendor file on one day: who makes the part, how many they had, and the price.
CREATE TABLE vendor_intelligence.local_offer (
    vendor_code   VARCHAR(64) NOT NULL,    -- vendor file source: Neuco, Metropac, or UED
    snapshot_date DATE NOT NULL,           -- the day of the file
    manufacturer  VARCHAR(255) NOT NULL,   -- who made the part
    part_code     VARCHAR(128) NOT NULL,   -- the part number in that file
    qty_on_hand   VARCHAR(32) NULL,        -- how many the vendor had; blank if the part dropped off a Neuco day
    price         DECIMAL(18, 4) NULL,     -- the price in the file; blank if the file had no price or the part was removed
    price_kind    VARCHAR(32) NOT NULL     -- SELL for Neuco, NET for Metropac, DEALER_EXCHANGE for UED; not cost
);
GO
CREATE INDEX idx_lo_next_key ON vendor_intelligence.local_offer (vendor_code, snapshot_date, part_code);
GO
