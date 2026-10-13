import re
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DIR = PROJECT_ROOT / "pipeline"
sys.path.insert(0, str(PIPELINE_DIR))

from ingest_parts_to_postgres import (  # noqa: E402
    is_non_product_sku,
    merchant_catalog_queries,
    sales_queries,
    sql_cast_or_null,
    top_clause,
)
from profile_data_quality import column_policy, trust_status  # noqa: E402
from validate_phase1_kpis import HECTA_REQUIRED_FIELDS  # noqa: E402
from validate_phase1_sources import as_date, as_number, compare_metric  # noqa: E402


def test_source_value_normalization():
    assert as_number(None) == 0.0
    assert as_number(Decimal("12.50")) == 12.5
    assert as_date(datetime(2026, 9, 21, 14, 30)) == "2026-09-21"
    assert as_date(date(2026, 9, 21)) == "2026-09-21"
    assert as_date("2026-09-21T14:30:00") == "2026-09-21"


def test_metric_comparison_tolerance():
    assert compare_metric(1.0, 1.00001)
    assert not compare_metric(1.0, 1.1)


def test_data_quality_trust_policy():
    assert trust_status(0, 0, "REQUIRED") == "NO_DATA"
    assert trust_status(10, 0, "REQUIRED") == "TRUSTED"
    assert trust_status(10, 1, "REQUIRED") == "NOT_TRUSTED"
    assert trust_status(10, 1, "EXPECTED_SOURCE_NULL") == "KNOWN_SOURCE_LIMITATION"
    assert column_policy("raw", "sales_orders", "PartsFE", "customer_email") == (
        "PII_PROFILE_COUNTS_ONLY",
        "NONE_PII_EXCLUDED",
    )


def test_smoke_query_limit_clause():
    assert top_clause(None) == ""
    assert top_clause(1000) == "TOP (1000) "


def test_non_product_sku_exclusions():
    assert is_non_product_sku("CANCELLATION")
    assert is_non_product_sku("OVERSIZE FEE1")
    assert is_non_product_sku("oversize fee3")
    assert not is_non_product_sku("WH22X37840")
    assert not is_non_product_sku("FEE-BOARD")


def test_optional_sql_column_projection():
    assert sql_cast_or_null(["Weight", "SKU"], "Weight", "float") == "CAST([Weight] AS float)"
    assert sql_cast_or_null(["SKU"], "Weight", "float") == "CAST(NULL AS float)"


def test_sales_ingest_omits_customer_pii():
    query = sales_queries()["APZ"]
    assert "CAST(NULL AS varchar(255)) AS customer_email" in query
    assert "CAST(NULL AS varchar(100)) AS phone" in query
    assert "CustomerEmail" not in query
    assert "CustomerFirstName" not in query


def test_merchant_sku_prefers_sku_column_when_present():
    query = merchant_catalog_queries(
        column_lookup={"dbo.APZMcFeed": ["id", "sku", "mpn", "sale_price"]}
    )["APZ"]
    assert "NULLIF(CAST([sku] AS varchar(100)), '')" in query
    assert "CAST([sale_price] AS float)" in query
    assert "CAST([mpn] AS varchar(100)) AS mpn" in query


def test_required_field_contract_covers_every_hecta_view():
    view_sql = (PIPELINE_DIR / "sql" / "hecta_ready_views.sql").read_text(
        encoding="utf-8"
    )
    published_views = set(
        re.findall(
            r"CREATE\s+OR\s+REPLACE\s+VIEW\s+analytics_hecta\.([a-z_]+)",
            view_sql,
            flags=re.IGNORECASE,
        )
    )

    assert set(HECTA_REQUIRED_FIELDS) == published_views
    assert all(fields for fields in HECTA_REQUIRED_FIELDS.values())
    assert all(
        len(fields) == len(set(fields))
        for fields in HECTA_REQUIRED_FIELDS.values()
    )
    assert "GOOGLE_PRODUCT_CATEGORY_" not in view_sql
    assert "current inventory and catalog status are unavailable" not in view_sql
    assert "analytics_hecta.sales_daily" in view_sql
    assert "analytics_hecta.sales_weekly" in view_sql
    assert "analytics_hecta.sales_monthly" in view_sql
    assert "MAX_INTEGRATED_STORE_NOT_NETWORK_ON_HAND" in (
        PIPELINE_DIR / "sql" / "clean_and_kpis.sql"
    ).read_text(encoding="utf-8")
