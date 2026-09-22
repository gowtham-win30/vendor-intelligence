import csv
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

from generate_phase1_reports import get_pg_conn
from psycopg2 import sql
from psycopg2.extras import execute_values

PROFILE_VERSION = "1.0"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = PROJECT_ROOT / "reports"

TARGETS = (
    ("raw", "vendor_deltas", "source_vendor"),
    ("raw", "sales_orders", "source_store"),
    ("raw", "current_inventory", "source_store"),
    ("raw", "merchant_catalog", "source_store"),
    ("clean", "vendor_deltas", "source_vendor"),
    ("clean", "sales_orders", "source_store"),
    ("clean", "current_inventory", "source_store"),
    ("clean", "merchant_catalog", "source_store"),
    ("analytics", "sku_phase1_kpis", None),
    ("analytics", "sku_catalog_current", None),
)

PII_COLUMNS = {
    "customer_email",
    "postcode",
    "company",
    "customer_first_name",
    "customer_last_name",
    "phone",
}
REQUIRED_COLUMNS = {
    "source_vendor",
    "source_store",
    "source_table",
    "sku",
    "snapshot_date",
    "created_at",
    "ingested_at",
    "source_ingested_at",
    "kpi_spec_version",
    "data_cutoff_date",
}
KPI_INPUT_COLUMNS = {
    "old_qty",
    "new_qty",
    "qty_ordered",
    "price",
    "valid_units_depleted",
    "valid_units_sold",
    "valid_revenue",
    "quantity",
    "product_type",
    "availability",
    "current_retail_price",
}
CONDITIONAL_COLUMNS = {
    "prior_date",
    "manufacturer",
    "product_name",
    "old_price",
    "new_price",
    "average_selling_price",
    "latest_snapshot_date",
    "latest_sale_date",
    "historical_vendors",
    "stores",
}
TEXT_TYPES = {"character", "character varying", "text"}


def ensure_audit_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE SCHEMA IF NOT EXISTS audit;

            CREATE TABLE IF NOT EXISTS audit.data_quality_profile_runs (
                run_id UUID PRIMARY KEY,
                profile_version TEXT NOT NULL,
                data_cutoff_date DATE NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ,
                profiled_tables INTEGER,
                profiled_columns INTEGER,
                status TEXT NOT NULL CHECK (
                    status IN ('RUNNING', 'COMPLETED', 'FAILED')
                ),
                error_message TEXT
            );

            CREATE TABLE IF NOT EXISTS audit.data_quality_column_profiles (
                run_id UUID NOT NULL
                    REFERENCES audit.data_quality_profile_runs(run_id),
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
                PRIMARY KEY (
                    run_id,
                    schema_name,
                    table_name,
                    source_name,
                    column_name
                )
            );
            """
        )
    conn.commit()


def get_columns(conn, schema_name, table_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema_name, table_name),
        )
        columns = cur.fetchall()
    if not columns:
        raise RuntimeError(f"Profile target does not exist: {schema_name}.{table_name}")
    return columns


def column_policy(schema_name, table_name, source_name, column_name):
    if column_name in PII_COLUMNS:
        return "PII_PROFILE_COUNTS_ONLY", "NONE_PII_EXCLUDED"
    if column_name in REQUIRED_COLUMNS:
        return "REQUIRED", "JOIN_OR_TIME_SERIES"
    if column_name in KPI_INPUT_COLUMNS:
        return "KPI_INPUT_REQUIRED", "KPI_CALCULATION"
    if column_name == "manufacturer" and source_name == "Metropac":
        return "EXPECTED_SOURCE_NULL", "MANUFACTURER_GROUPING"
    if column_name in CONDITIONAL_COLUMNS:
        return "CONDITIONAL", "DIMENSION_OR_SECONDARY_METRIC"
    if schema_name == "analytics" and table_name == "sku_phase1_kpis":
        return "CONDITIONAL_BY_OBSERVED_HISTORY", "PUBLISHED_KPI"
    return "OPTIONAL", "NONE_OR_INFORMATIONAL"


def trust_status(total_rows, missing_rows, null_policy):
    if total_rows == 0:
        return "NO_DATA"
    if null_policy == "PII_PROFILE_COUNTS_ONLY":
        return "NOT_EXPOSED"
    if missing_rows == 0:
        return "TRUSTED"
    if null_policy == "EXPECTED_SOURCE_NULL":
        return "KNOWN_SOURCE_LIMITATION"
    if null_policy == "REQUIRED":
        return "NOT_TRUSTED"
    if null_policy == "KPI_INPUT_REQUIRED":
        return "LIMITED"
    if null_policy.startswith("CONDITIONAL"):
        return "EXPECTED_CONDITIONAL"
    return "REVIEW"


def profile_table(conn, schema_name, table_name, source_column):
    columns = get_columns(conn, schema_name, table_name)
    aggregate_expressions = [sql.SQL("COUNT(*)")]

    for column_name, data_type in columns:
        identifier = sql.Identifier(column_name)
        aggregate_expressions.append(
            sql.SQL("COUNT(*) FILTER (WHERE {} IS NULL)").format(identifier)
        )
        if data_type in TEXT_TYPES:
            blank_condition = sql.SQL(
                "{} IS NOT NULL AND BTRIM({}::text) = ''"
            ).format(identifier, identifier)
            missing_condition = sql.SQL(
                "{} IS NULL OR BTRIM({}::text) = ''"
            ).format(identifier, identifier)
        else:
            blank_condition = sql.SQL("FALSE")
            missing_condition = sql.SQL("{} IS NULL").format(identifier)
        aggregate_expressions.append(
            sql.SQL("COUNT(*) FILTER (WHERE {})").format(blank_condition)
        )
        aggregate_expressions.append(
            sql.SQL("COUNT(*) FILTER (WHERE {})").format(missing_condition)
        )

    if source_column:
        source_expression = sql.SQL(
            "COALESCE({}::text, '<NULL_SOURCE>')"
        ).format(sql.Identifier(source_column))
        group_clause = sql.SQL(" GROUP BY {} ORDER BY {}").format(
            sql.Identifier(source_column),
            sql.Identifier(source_column),
        )
    else:
        source_expression = sql.Literal("ALL")
        group_clause = sql.SQL("")

    query = sql.SQL("SELECT {}, {} FROM {}.{}{}").format(
        source_expression,
        sql.SQL(", ").join(aggregate_expressions),
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        group_clause,
    )

    with conn.cursor() as cur:
        cur.execute(query)
        result_rows = cur.fetchall()

    profiles = []
    for result in result_rows:
        source_name = result[0]
        total_rows = result[1]
        offset = 2
        for column_name, data_type in columns:
            null_rows, blank_rows, missing_rows = result[offset : offset + 3]
            offset += 3
            null_policy, kpi_impact = column_policy(
                schema_name,
                table_name,
                source_name,
                column_name,
            )
            missing_pct = (
                round(100.0 * missing_rows / total_rows, 4) if total_rows else 0.0
            )
            profiles.append(
                {
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "source_name": source_name,
                    "column_name": column_name,
                    "data_type": data_type,
                    "total_rows": total_rows,
                    "null_rows": null_rows,
                    "blank_rows": blank_rows,
                    "missing_rows": missing_rows,
                    "missing_pct": missing_pct,
                    "null_policy": null_policy,
                    "kpi_impact": kpi_impact,
                    "trust_status": trust_status(
                        total_rows,
                        missing_rows,
                        null_policy,
                    ),
                }
            )
    return profiles


def persist_profiles(conn, run_id, profiles):
    values = [
        (
            run_id,
            row["schema_name"],
            row["table_name"],
            row["source_name"],
            row["column_name"],
            row["data_type"],
            row["total_rows"],
            row["null_rows"],
            row["blank_rows"],
            row["missing_rows"],
            row["missing_pct"],
            row["null_policy"],
            row["kpi_impact"],
            row["trust_status"],
        )
        for row in profiles
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO audit.data_quality_column_profiles (
                run_id,
                schema_name,
                table_name,
                source_name,
                column_name,
                data_type,
                total_rows,
                null_rows,
                blank_rows,
                missing_rows,
                missing_pct,
                null_policy,
                kpi_impact,
                trust_status
            ) VALUES %s
            """,
            values,
            page_size=1000,
        )


def aggregate_columns(profiles):
    grouped = defaultdict(list)
    for row in profiles:
        grouped[
            (row["schema_name"], row["table_name"], row["column_name"])
        ].append(row)

    output = []
    trust_priority = {
        "NOT_TRUSTED": 6,
        "LIMITED": 5,
        "REVIEW": 4,
        "KNOWN_SOURCE_LIMITATION": 3,
        "EXPECTED_CONDITIONAL": 2,
        "TRUSTED": 1,
        "NOT_EXPOSED": 0,
        "NO_DATA": 0,
    }
    for (schema_name, table_name, column_name), rows in grouped.items():
        total_rows = sum(row["total_rows"] for row in rows)
        missing_rows = sum(row["missing_rows"] for row in rows)
        output.append(
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column_name,
                "data_type": rows[0]["data_type"],
                "total_rows": total_rows,
                "missing_rows": missing_rows,
                "missing_pct": (
                    round(100.0 * missing_rows / total_rows, 4)
                    if total_rows
                    else 0.0
                ),
                "sources_profiled": len(rows),
                "sources_with_missing": sum(
                    row["missing_rows"] > 0 for row in rows
                ),
                "null_policy": rows[0]["null_policy"],
                "kpi_impact": rows[0]["kpi_impact"],
                "trust_status": max(
                    (row["trust_status"] for row in rows),
                    key=lambda status: trust_priority[status],
                ),
            }
        )
    return sorted(
        output,
        key=lambda row: (
            row["schema_name"],
            row["table_name"],
            row["column_name"],
        ),
    )


def write_csv(path, rows, fieldnames):
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def export_reports(run_id, profiles):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    profile_fields = [
        "schema_name",
        "table_name",
        "source_name",
        "column_name",
        "data_type",
        "total_rows",
        "null_rows",
        "blank_rows",
        "missing_rows",
        "missing_pct",
        "null_policy",
        "kpi_impact",
        "trust_status",
    ]
    missing_rows = [row for row in profiles if row["missing_rows"] > 0]
    write_csv(
        REPORTS_DIR / "data_quality_missing_values.csv",
        missing_rows,
        profile_fields,
    )

    column_profiles = aggregate_columns(profiles)
    write_csv(
        REPORTS_DIR / "data_quality_column_profile.csv",
        column_profiles,
        list(column_profiles[0]) if column_profiles else [],
    )

    status_counts = defaultdict(int)
    for row in profiles:
        status_counts[row["trust_status"]] += 1
    highest_missing = sorted(
        missing_rows,
        key=lambda row: (-row["missing_pct"], -row["missing_rows"]),
    )[:20]

    lines = [
        "# Data Quality Profile Summary",
        "",
        f"- Profile run ID: `{run_id}`",
        f"- Profile version: `{PROFILE_VERSION}`",
        f"- Data cutoff date: `{date.today().isoformat()}`",
        f"- Tables profiled: `{len(TARGETS)}`",
        f"- Source-column profiles: `{len(profiles)}`",
        f"- Profiles containing missing values: `{len(missing_rows)}`",
        "",
        "## Trust Status Counts",
        "",
    ]
    lines.extend(
        f"- {status}: {count}"
        for status, count in sorted(status_counts.items())
    )
    lines.extend(["", "## Highest Missing Rates", ""])
    lines.extend(
        (
            f"- `{row['schema_name']}.{row['table_name']}` / "
            f"`{row['source_name']}` / `{row['column_name']}`: "
            f"{row['missing_pct']:.4f}% "
            f"({row['trust_status']}; {row['null_policy']})"
        )
        for row in highest_missing
    )
    lines.extend(
        [
            "",
            "Counts profile PII columns without exporting any PII values.",
            "Known source limitations are not equivalent to unexpected data loss.",
            "",
        ]
    )
    (REPORTS_DIR / "data_quality_profile_summary.md").write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


def main():
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    conn = get_pg_conn()
    ensure_audit_tables(conn)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.data_quality_profile_runs (
                    run_id,
                    profile_version,
                    data_cutoff_date,
                    started_at,
                    status
                ) VALUES (%s, %s, %s, %s, 'RUNNING')
                """,
                (run_id, PROFILE_VERSION, date.today(), started_at),
            )
        conn.commit()

        profiles = []
        for schema_name, table_name, source_column in TARGETS:
            print(f"Profiling {schema_name}.{table_name}...")
            profiles.extend(
                profile_table(
                    conn,
                    schema_name,
                    table_name,
                    source_column,
                )
            )

        persist_profiles(conn, run_id, profiles)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE audit.data_quality_profile_runs
                SET completed_at = %s,
                    profiled_tables = %s,
                    profiled_columns = %s,
                    status = 'COMPLETED'
                WHERE run_id = %s
                """,
                (
                    datetime.now(timezone.utc),
                    len(TARGETS),
                    len(profiles),
                    run_id,
                ),
            )
        conn.commit()
        export_reports(run_id, profiles)
    except Exception as exc:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE audit.data_quality_profile_runs
                SET completed_at = %s,
                    status = 'FAILED',
                    error_message = %s
                WHERE run_id = %s
                """,
                (datetime.now(timezone.utc), str(exc), run_id),
            )
        conn.commit()
        raise
    finally:
        conn.close()

    print(f"Data-quality profile completed: {run_id}")
    print(f"Reports written to: {REPORTS_DIR}")


if __name__ == "__main__":
    main()
