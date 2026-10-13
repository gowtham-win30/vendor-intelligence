# Parts1 data catalog

Database: configured `SQL_SERVER_TARGET_DB` (default `Parts1`) on the same
SQL Server instance as `Parts`.

Read only schema `vendor_intelligence`. Do not use schema `dbo`.

Rules:

- No customer name, email, phone, or address.
- `price` and `unit_price` are source prices, not confirmed cost.
- Money is DECIMAL(18,4). Dates are calendar dates with no timezone.
- No rows for a filter means no data, not zero sales or zero stock.

## vendor_intelligence.sales_line

What we sold. One row is one order line with units greater than zero.

| Column | Type | Null | Meaning |
|---|---|---|---|
| store_code | VARCHAR(32) | no | PartsFE, PartsHNC, APZ, WTB, PM, or HT |
| order_id | VARCHAR(128) | no | Order number |
| sku | VARCHAR(128) | no | Part sold |
| sold_on | DATE | no | Sale day |
| units | DECIMAL(18,4) | no | Units sold |
| unit_price | DECIMAL(18,4) | yes | Unit price; null when source price was missing or negative |

WTB sales stop earlier than the other stores. Do not call WTB current.

## vendor_intelligence.vendor_change

Vendor stock quantity change. Quantity only. No price.

| Column | Type | Null | Meaning |
|---|---|---|---|
| vendor_code | VARCHAR(64) | no | Neuco, Metropac, EC, Msupply, or ReliableParts |
| sku | VARCHAR(128) | no | Vendor part number |
| change_date | DATE | no | Day of the change |
| qty_before | DECIMAL(18,4) | yes | Quantity before |
| qty_after | DECIMAL(18,4) | yes | Quantity after. 0 means stock hit zero that day, not that the part left the catalog |

Neuco through 2024-04-17 comes from the old Neuco delta. Later Neuco rows are quantity only.

## vendor_intelligence.local_offer

One vendor file day: manufacturer, on-hand, and file price.

| Column | Type | Null | Meaning |
|---|---|---|---|
| vendor_code | VARCHAR(64) | no | Neuco, Metropac, or UED |
| snapshot_date | DATE | no | File day |
| manufacturer | VARCHAR(255) | no | Maker |
| part_code | VARCHAR(128) | no | Part in that file |
| qty_on_hand | VARCHAR(32) | yes | Vendor qty. Blank if the part dropped off a Neuco change day |
| price | DECIMAL(18,4) | yes | File price. Not cost |
| price_kind | VARCHAR(32) | no | SELL, NET, or DEALER_EXCHANGE |

Metropac and UED store the full file each day. Neuco keeps a full catalog for the first day and the latest day; other Neuco days keep only parts whose price or quantity changed.

## Operational tables

`load_run` — one row per job.  
`load_reject` — refused rows or files.  
`local_offer_stage` — upload scratch. Do not report from it.
