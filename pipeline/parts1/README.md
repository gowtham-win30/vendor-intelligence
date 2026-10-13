# Parts1 thin load

Second job, separate from the Postgres / Hecta pipeline.

It copies a few columns from SQL Server `Parts` into SQL Server `Parts1`,
and uploads daily vendor files from this workstation. Rows do **not** go
through PostgreSQL.

```text
Parts (sales + vendor qty)  --INSERT...SELECT-->  Parts1.vendor_intelligence
E:\ Neuco / Metropac / UED  --this PC---------->  Parts1.vendor_intelligence
```

Do not read `Parts1.dbo`. That leftover is from a failed first load.

## Layout

```text
pipeline/parts1/
  load.py              Full rebuild into *_next, then swap
  load_missing.py      Append sales / Neuco qty / file history a run missed
  resume_upload.py     Continue a dropped local-file upload
  parse_local.py       Read Neuco / Metropac / UED files
  extract_parts.py     INSERT...SELECT SQL against Parts
  sql/schema.sql       Creates vendor_intelligence objects
  sql/published_tables.sql
  sql/catalog.md       Column meanings for readers
```

## Tables

| Table | Source | Grain |
|---|---|---|
| `sales_line` | Parts sales tables | One sold line. No customer PII. |
| `vendor_change` | Parts qty deltas | One qty change. No price. |
| `local_offer` | Local daily files | One vendor+day+part. Price is SELL / NET / DEALER_EXCHANGE, not cost. |
| `load_run` / `load_reject` | This job | Audit |
| `local_offer_stage` | This job | Scratch. Do not report from it. |

Neuco qty through 2024-04-17 comes from `NeucoDelta`. Later Neuco qty can
come from `NeucoQtyChanges`. Current Neuco **price** is only in `local_offer`.

Neuco file history: first day and the latest day are full catalogs. Other
days keep only price or qty changes. Metropac and UED store each file day
in full.

## Run

Same `.env` as the Postgres ingest (`SQL_SERVER_*`). Optional:

```text
SQL_SERVER_TARGET_DB=Parts1
NEUCO_FILES_DIR=E:\NeucoFiles
METROPAC_FILES_DIR=E:\Metropac
UED_FILES_DIR=E:\UED FIiles
```

From the repo root:

```powershell
python -m pipeline.parts1.load
python -m pipeline.parts1.load_missing
python -m pipeline.parts1.resume_upload
```

Folder flags override env: `--neuco`, `--metropac`, `--ued`.

Customer name, email, phone, and address are never selected.
