# Vendor Intelligence Ingestion & Analytics Pipeline

This pipeline ingests Phase 1 source data from SQL Server `Parts` into a local Dockerized PostgreSQL database, cleans and deduplicates the raw feeds, and computes Phase 1 KPIs and analytics deliverables.

## Architecture

1. **Ingestion Layer (`raw`)**: Extracts vendor deltas, sales, latest inventory,
   and merchant catalog feeds from SQL Server.
2. **Cleaning & Normalization Layer (`clean`)**: Normalizes keys, preserves quality
   flags, deduplicates records, and excludes invalid inputs from affected metrics.
3. **Analytics & Intelligence Layer (`analytics`)**: Pre-computes historical KPIs
   and current catalog/inventory context.
4. **Hecta Contract Layer (`analytics_hecta`)**: Exposes documented, read-only,
   business-facing views without customer PII.
5. **Audit Layer (`audit`)**: Records ingestion, transformation, and data-profile runs.
6. **Deliverable Exports (`reports/`)**: Exports versioned analytical and validation
   evidence.

Production architecture, correctness rules, testing gates, and AI/ML safeguards are maintained
in the internal engineering framework document (kept outside this repository).

---

## Source Database

SQL Server connection details are supplied only through environment variables. Host,
user, and password values must not be committed to this repository.

## Target Database

- PostgreSQL container: `vendor_intelligence_postgres`
- Host: `localhost`
- Port: `5544`
- Database: `vendor_intelligence`
- User: `postgres`

---

## 1. Start PostgreSQL Container

Copy `.env.example` to `.env` or provide the required values through your deployment
secret manager. Never commit `.env`.

```powershell
cd C:\Users\DELL\Desktop\Vendor_intelligence
docker compose --env-file .env -f pipeline/docker-compose.yml up -d
```

---

## 2. Install Dependencies

```powershell
pip install -r pipeline/requirements.txt
```

For development and CI checks:

```powershell
pip install -r requirements-dev.txt
ruff check .
pytest
```

---

## 3. Ingest Data (If Running Fresh Extraction)

```powershell
# Smoke test (1,000 rows per table)
python pipeline/ingest_parts_to_postgres.py --mode smoke

# Full source-by-source atomic ingestion
python pipeline/ingest_parts_to_postgres.py --mode full

# Selective source-group reload
python pipeline/ingest_parts_to_postgres.py --mode full --only sales

# Current inventory and merchant catalog reload
python pipeline/ingest_parts_to_postgres.py --mode full --only catalog
```

Smoke mode validates extraction and staging without modifying raw production tables.
Full mode atomically replaces one source at a time only after its complete extraction
has been staged successfully.

---

## 4. Run Cleaning & Generate Phase 1 Reports

Once raw data exists in PostgreSQL, run:

```powershell
python pipeline/generate_phase1_reports.py
```

This runs `pipeline/sql/clean_and_kpis.sql` and generates preliminary Phase 1 analytical outputs into
`reports/`. Metric definitions and limitations are documented in the internal Phase 1 KPI
specification (kept outside this repository).
These outputs support reviewed analyst decisions and are not approved for autonomous action.

## Project Structure

```text
Vendor_intelligence/
├── README.md
├── pipeline/
│   ├── sql/
│   ├── docker-compose.yml
│   ├── requirements.txt
│   └── *.py
├── tests/
└── .github/workflows/
```

Generated reports:

| Report File | Description |
| :--- | :--- |
| `reports/preliminary_top_200_focus_parts.csv` | Prototype ranking using sales, vendor depletion, and historical vendor presence |
| `reports/preliminary_movement_classification.csv` | Candidate movement classes using unapproved prototype thresholds |
| `reports/preliminary_movement_vs_sales_gap.csv` | Candidate SKUs with high vendor movement signals and low observed store sales |
| `reports/no_observed_movement_candidates.csv` | Historical candidates only; not a dead-stock or catalog-demotion decision |
| `reports/preliminary_category_summary.csv` | Summary using inferred, non-authoritative category assignments |
| `reports/data_quality_summary.csv` | Duplicate, invalid-date, quantity, and reconciliation indicators |

## 5. Validate Phase 1 Outputs

```powershell
python pipeline/validate_phase1_kpis.py
python pipeline/validate_phase1_sources.py
```

Validation evidence is written to `reports/validation/`. Source validation exits
with a non-zero status whenever PostgreSQL does not reconcile with SQL Server.

## 6. Profile Missing Values and Column Trust

```powershell
python pipeline/profile_data_quality.py
```

The profile is recorded under `audit.data_quality_profile_runs` and
`audit.data_quality_column_profiles`. Current-run exports are written to:

- `reports/data_quality_missing_values.csv`
- `reports/data_quality_column_profile.csv`
- `reports/data_quality_profile_summary.md`

## 7. Hecta Data Catalog Contract

Run `pipeline/generate_phase1_reports.py` after each successful ingestion to publish
the `analytics_hecta` views. Grant the separately managed Hecta login membership in
the no-login `hecta_catalog_reader` role. Do not expose internal schemas.

View contracts and interpretation limits are documented in the internal Hecta data catalog
mapping (kept outside this repository).

## Continuous Integration

GitHub Actions runs linting, compilation, unit tests, Docker Compose validation,
dependency vulnerability auditing, and secret scanning on pull requests and pushes
to `main`. Database reconciliation remains an integration gate because it requires
controlled SQL Server and PostgreSQL credentials; it must run in staging before a
release is approved.
