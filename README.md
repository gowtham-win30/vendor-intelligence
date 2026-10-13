# Vendor Intelligence Pipeline

SQL Server `Parts` is ingested into PostgreSQL. The pipeline cleans those feeds,
computes Phase 1 SKU KPIs, and publishes read-only `analytics_hecta` views for
Hecta. It does not modify Hecta source code.

This repository is the production pipeline only. Local E: file loaders, thin
extracts, Excel workbooks, reports, and internal design notes stay on the
workstation and are gitignored.

## What it does

1. **Ingest** 17 SQL Server tables into `raw` (full replace per source, after
   staging).
2. **Clean** into `clean`: normalize SKUs, drop exact duplicates, flag bad
   dates and quantities, compute `valid_units_depleted` and `valid_revenue`.
   Customer email, name, phone, and postcode are not copied from SQL Server.
3. **Analytics** at SKU grain: vendor history, store sales, current catalog
   and inventory, depletion credibility, priority and gap classes.
4. **Sales periods** at day / week / month by store and `NETWORK`.
5. **Hecta views** in `analytics_hecta` only. Hecta must not read `raw` or
   `clean`.
6. **Audit** ingestion runs, transformation runs, and column-quality profiles.

Vendor depletion is a movement signal, not sales. Cumulative depleted units
often re-count a standing on-hand gap. Use `depletion_credibility_flag`
(`PLAUSIBLE` / `LIKELY_RECOUNTED` / `SEVERE_RECOUNT`). HIGH/MODERATE gap and
`CONSISTENT_RECURRING` require `PLAUSIBLE`. The priority score caps the
depletion term at peak observed quantity.

A second, optional job copies sales and vendor quantity from `Parts` into
SQL Server `Parts1` and uploads local daily vendor files. That job does not
use PostgreSQL. See `pipeline/parts1/README.md`.

## Sources loaded (17)

| Group | SQL Server tables | Postgres |
|---|---|---|
| Vendor qty deltas | `NeucoDelta`, `MetropacDelta`, `ECQtyChanges`, `MsupplyQTYChanges`, `RPDailyQTYChanges` | `raw`/`clean`.vendor_deltas |
| Store sales | `FESalesData`, `HNCSalesData`, `APZSalesData`, `WTBSalesData` | `raw`/`clean`.sales_orders |
| Store inventory | `APZLatestData`, `HNCLatestData`, `FELatestData`, `WTBLatestData` | `raw`/`clean`.current_inventory |
| Merchant catalog | `APZMcFeed`, `HNCMCFeed`, `FEMCDataFeed`, `WTBMCFeed` | `raw`/`clean`.merchant_catalog |

Neuco is the only delta with `old_price` / `new_price`, and that file ends
2024-04-17. Inventory and catalog freshness is the load date, not a business
event date.

## How to run

Secrets come from `.env` (copy `.env.example`). Never commit `.env`.

```powershell
docker compose --env-file .env -f pipeline/docker-compose.yml up -d
pip install -r pipeline/requirements.txt
python pipeline/ingest_parts_to_postgres.py --mode smoke
python pipeline/ingest_parts_to_postgres.py --mode full
python pipeline/generate_phase1_reports.py
python pipeline/validate_phase1_kpis.py
python pipeline/validate_phase1_sources.py
python pipeline/profile_data_quality.py
```

`--only vendors|sales|catalog|all` limits ingest. Smoke does not write `raw`.
Full ingest replaces one source at a time after that source is fully staged.

`generate_phase1_reports.py` runs, in order:

- `pipeline/sql/clean_and_kpis.sql`
- `pipeline/sql/sales_period_trends.sql`
- `pipeline/sql/hecta_ready_views.sql`

Then it writes CSVs under `reports/` (local only).

## Hecta

Grant a login role membership in `hecta_catalog_reader` (NOLOGIN). Set
`search_path=analytics_hecta`.

Connection shape:

`postgresql://hecta_reader:<password>@localhost:5544/vendor_intelligence`

If Hecta runs in Docker, use `host.docker.internal` instead of `localhost`.

Published views:

| View | Grain | Use |
|---|---|---|
| `daily_command_center` | one row | Latest totals. Not a prior-period series. |
| `sku_intelligence` | sku | Historical KPIs plus mapped stock/catalog |
| `ad_priority_candidates` | sku rank | Review queue, not an ad-spend action |
| `movement_vs_sales_gap` | sku | HIGH_GAP only when depletion is PLAUSIBLE |
| `catalog_visibility_candidates` | sku | No observed movement; not dead stock |
| `sales_daily` / `sales_weekly` / `sales_monthly` | store × period | Observed demand. `NETWORK` is a rollup; do not add it to store rows. Exclude WTB for current demand. |
| `monthly_trends` | category × month | Mixed depletion + sales; depletion is ungated |
| `current_catalog` | sku | Listing and stock |
| `pricing_review_candidates` | sku | Our retail spread, not cost |
| `source_health` / `data_quality_health` / `column_quality_profile` | source / column | Freshness and missing-value trust |

Regenerate catalog metadata with `python pipeline/export_hecta_catalog_ddl.py`
(output is local under `docs/`).

## Project layout

```text
pipeline/
  ingest_parts_to_postgres.py
  generate_phase1_reports.py
  validate_phase1_kpis.py
  validate_phase1_sources.py
  profile_data_quality.py
  export_hecta_catalog_ddl.py
  parts1/                  # Parts -> Parts1 + local E: files
  sql/
    init_schema.sql
    clean_and_kpis.sql
    sales_period_trends.sql
    hecta_ready_views.sql
    validate_phase1_sample.sql
  docker-compose.yml
  requirements.txt
tests/
.github/workflows/ci.yml
```

## Tests and CI

```powershell
pip install -r requirements-dev.txt
ruff check .
pytest
```

GitHub Actions runs ruff, compileall on `pipeline` and `tests`, pytest,
Docker Compose config, `pip-audit --strict`, and gitleaks. Source
reconciliation against SQL Server is a staging gate, not CI.

## Rules the agent and the views must keep

- Do not call depletion sales, demand, or lost revenue.
- Do not treat `LIKELY_RECOUNTED` / `SEVERE_RECOUNT` units as consumption.
- Zero sales usually means no matched sales row, not dead stock.
- `current_inventory_quantity` is the largest qty at one store, not network
  on-hand and not vendor QOH.
- Do not automate pricing, ads, purchasing, or catalog removal from these views.
