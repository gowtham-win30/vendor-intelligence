"""Publish a few columns into the SQL Server database Parts1.

Parts1 lives on the same instance as the source Parts database. The source
login can read Parts and write Parts1, so sales and vendor changes are copied
server-side with INSERT ... SELECT (no client round-trip). The local E: files
are pushed from the client into local_offer. Each run fills _next tables and
renames them into place inside one transaction.

The remote link can reset during long operations, so the target connection
retries transient network errors by reconnecting. Every statement is atomic
under autocommit, and committed work survives a reconnect, so retries do not
duplicate rows.

Customer name, email, phone, and address are never selected.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date
from pathlib import Path

import pyodbc
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[2]
_PIPELINE = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env")
if str(_PIPELINE) not in sys.path:
    sys.path.insert(0, str(_PIPELINE))

from ingest_parts_to_postgres import sql_server_conn

from pipeline.parts1.extract_parts import (
    FE_SALES_GAPS,
    SALES_TABLES,
    VENDOR_SOURCES,
    fe_sales_gap_sql,
    neuco_qty_changes_sql,
    sales_extract_sql,
    sales_reject_sql,
    vendor_extract_sql,
)
from pipeline.parts1.parse_local import (
    ParseReject,
    iter_offer_rows,
    select_local_snapshots,
)

SCHEMA_PATH = Path(__file__).resolve().parent / "sql" / "schema.sql"
SOURCE_DB = os.getenv("SQL_SERVER_DB", "Parts")
TARGET_DB = os.getenv("SQL_SERVER_TARGET_DB", "Parts1")
TARGET_SCHEMA = "vendor_intelligence"
DEFAULT_ROOTS = {
    "Neuco": Path(os.getenv("NEUCO_FILES_DIR", r"E:\NeucoFiles")),
    "Metropac": Path(os.getenv("METROPAC_FILES_DIR", r"E:\Metropac")),
    "UED": Path(os.getenv("UED_FILES_DIR", r"E:\UED FIiles")),
}
LOCK_RESOURCE = "vendor_intelligence_mssql_load"
DATA_TABLES = ("sales_line", "vendor_change", "local_offer")

LOCAL_OFFER_COLUMNS = (
    "vendor_code, snapshot_date, manufacturer, part_code, qty_on_hand, price, price_kind"
)
LOCAL_OFFER_ROW = "(?, ?, ?, ?, ?, ?, ?)"
# Eight bound values per row. SQL Server allows 2,100 parameters per statement.
LOCAL_BATCH = 250

TRANSIENT_MARKERS = (
    "10054", "10053", "08S01", "08001",
    "General network", "Communication link failure",
    "ConnectionRead", "ConnectionWrite", "connection was recovered",
)


def qualified(table: str) -> str:
    return f"{TARGET_SCHEMA}.{table}"


def source_name(table: str) -> str:
    return f"{SOURCE_DB}.{table}"


def is_transient(exc: Exception) -> bool:
    return any(marker in str(exc) for marker in TRANSIENT_MARKERS)


class Target:
    """SQL Server connection for Parts1 that reconnects on transient drops."""

    def __init__(self, lock_resource: str = LOCK_RESOURCE) -> None:
        self.conn: pyodbc.Connection | None = None
        self.cursor: pyodbc.Cursor | None = None
        self.lock_base = lock_resource
        self.lock_resource = lock_resource
        self.lock_generation = 0
        self.locked = False
        self._open()

    def _open(self) -> None:
        self.conn = sql_server_conn()
        self.conn.timeout = 0
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        self.cursor.execute(f"USE {TARGET_DB}")
        self.cursor.execute(
            "DECLARE @status int; "
            "EXEC @status = sp_getapplock @Resource = ?, @LockMode = 'Exclusive', "
            "@LockOwner = 'Session', @LockTimeout = 10000; "
            "SELECT @status;",
            self.lock_resource,
        )
        row = self.cursor.fetchone()
        if row is None or int(row[0]) < 0:
            raise RuntimeError(
                f"Another Parts1 load holds the lock '{self.lock_resource}'. "
                "If a prior run was force-killed, its session may still hold it; "
                "clear it on the server (KILL the SPID or restart) or rerun with "
                "a different --lock-suffix."
            )
        self.locked = True

    def _reopen(self) -> None:
        self.locked = False
        try:
            self.conn.close()
        except Exception:
            pass
        time.sleep(3)
        # A dropped session can keep this applock until SQL Server drops it.
        # A new name lets this process continue; the retried work deletes and reinserts.
        self.lock_generation += 1
        self.lock_resource = f"{self.lock_base}_{self.lock_generation}"
        self._open()

    def execute(self, sql: str, *params, retries: int = 5, fetch: bool = False):
        return self.run(lambda cur: (cur.execute(sql, *params),
                                     cur.fetchone() if fetch else None)[1],
                        retries=retries)

    def run(self, work, retries: int = 5):
        """Run a callable that may issue several statements; retry it whole on a
        transient drop. The callable must be idempotent so a reconnect that
        loses a commit acknowledgement does not duplicate rows."""
        for attempt in range(retries):
            try:
                return work(self.cursor)
            except pyodbc.Error as exc:
                if is_transient(exc) and attempt < retries - 1:
                    print(f"transient error (attempt {attempt + 1}), reconnecting: {exc}")
                    self._reopen()
                    continue
                raise

    def run_transaction(self, work, retries: int = 20):
        """Commit work as one transaction. A dropped connection rolls it back."""
        for attempt in range(retries):
            try:
                self.conn.autocommit = False
                try:
                    work(self.cursor)
                    self.conn.commit()
                    return
                except Exception:
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass
                    raise
                finally:
                    try:
                        self.conn.autocommit = True
                    except Exception:
                        pass
            except pyodbc.Error as exc:
                if is_transient(exc) and attempt < retries - 1:
                    print(f"transient error (attempt {attempt + 1}), reconnecting: {exc}")
                    time.sleep(min(30, 5 * (attempt + 1)))
                    self._reopen()
                    continue
                raise

    def close(self) -> None:
        if self.locked and self.cursor is not None:
            try:
                self.cursor.execute(
                    "EXEC sp_releaseapplock @Resource = ?, @LockOwner = 'Session'",
                    self.lock_resource,
                )
            except Exception:
                pass
        for closable in (self.cursor, self.conn):
            try:
                if closable is not None:
                    closable.close()
            except Exception:
                pass


def sales_insert_sql(dest: str, store: str, table: str) -> str:
    sql = (
        f"DELETE FROM {dest} WHERE store_code = '{store}';\n"
        f"INSERT INTO {dest} (store_code, order_id, sku, sold_on, units, unit_price) "
        + sales_extract_sql(store, source_name(table))
    )
    if store == "PartsFE":
        for gap in FE_SALES_GAPS:
            sql += (
                f";\nINSERT INTO {dest} (store_code, order_id, sku, sold_on, units, unit_price) "
                + fe_sales_gap_sql(source_name(gap), source_name("dbo.FeSalesData"))
            )
    return sql


def sales_insert_queries() -> dict[str, str]:
    dest = qualified("sales_line_next")
    return {
        store: sales_insert_sql(dest, store, table)
        for store, table in SALES_TABLES.items()
    }


def vendor_insert_sql(dest: str) -> dict[str, str]:
    queries = {}
    for code, table, change_date, sku_expr, qty_before, qty_after, sku_column in VENDOR_SOURCES:
        sql = (
            f"DELETE FROM {dest} WHERE vendor_code = '{code}';\n"
            f"INSERT INTO {dest} (vendor_code, sku, change_date, qty_before, qty_after) "
            + vendor_extract_sql(code, source_name(table), change_date,
                                 sku_expr, qty_before, qty_after, sku_column)
        )
        if code == "Neuco":
            sql += (
                f";\nINSERT INTO {dest} (vendor_code, sku, change_date, qty_before, qty_after) "
                + neuco_qty_changes_sql().replace("FROM dbo.NeucoQtyChanges",
                                                  f"FROM {source_name('dbo.NeucoQtyChanges')}")
            )
        queries[code] = sql
    return queries


def vendor_insert_queries() -> dict[str, str]:
    return vendor_insert_sql(qualified("vendor_change_next"))


def run_schema(target: Target) -> None:
    target.execute(
        f"IF SCHEMA_ID('{TARGET_SCHEMA}') IS NULL EXEC('CREATE SCHEMA {TARGET_SCHEMA}')"
    )
    text = SCHEMA_PATH.read_text(encoding="utf-8")
    batch_lines: list[str] = []
    for line in text.splitlines():
        if line.strip().upper() == "GO":
            batch = "\n".join(batch_lines).strip()
            if batch:
                target.execute(batch)
            batch_lines = []
        else:
            batch_lines.append(line)
    tail = "\n".join(batch_lines).strip()
    if tail:
        target.execute(tail)


def offer_param(row: tuple) -> tuple:
    vendor_code, snapshot_iso, manufacturer, part_code, qty, price, price_kind = row
    return (
        vendor_code,
        snapshot_iso,
        manufacturer,
        part_code,
        qty or None,
        price or None,
        price_kind,
    )


STAGE_ROW = "(?, ?, ?, ?, ?, ?, ?, ?)"


def ensure_offer_stage(target: Target) -> None:
    """Empty side table. Live rows move only after a day is fully staged."""
    target.execute(
        f"IF OBJECT_ID('{TARGET_SCHEMA}.local_offer_stage', 'U') IS NOT NULL "
        f"DROP TABLE {TARGET_SCHEMA}.local_offer_stage"
    )
    target.execute(
        f"CREATE TABLE {TARGET_SCHEMA}.local_offer_stage ("
        "vendor_code VARCHAR(64) NOT NULL, "
        "snapshot_date DATE NOT NULL, "
        "manufacturer VARCHAR(255) NOT NULL, "
        "part_code VARCHAR(128) NOT NULL, "
        "qty_on_hand VARCHAR(32) NULL, "
        "price DECIMAL(18, 4) NULL, "
        "price_kind VARCHAR(32) NOT NULL, "
        "batch_no INT NOT NULL)"
    )


def stage_offer_slice(target: Target, vendor_code: str, snapshot_iso: str, offers: list[tuple]) -> None:
    """Replace one live day only after that day is committed in the side table.

    All batches for the day commit together. A dropped connection rolls the day
    back, and the retry loads it again. The live day is deleted only inside the
    swap, after the staged count matches.
    """
    stage = qualified("local_offer_stage")
    live = qualified("local_offer")
    params = [offer_param(row) for row in offers]
    expected = len(params)

    def load(cursor):
        cursor.execute(
            f"DELETE FROM {stage} WHERE vendor_code = ? AND snapshot_date = ?",
            vendor_code,
            snapshot_iso,
        )
        for batch_no, start in enumerate(range(0, expected, LOCAL_BATCH)):
            chunk = params[start:start + LOCAL_BATCH]
            sql = (
                f"INSERT INTO {stage} ({LOCAL_OFFER_COLUMNS}, batch_no) VALUES "
                + ", ".join(STAGE_ROW for _ in chunk)
            )
            values = [value for row in chunk for value in (*row, batch_no)]
            cursor.execute(sql, *values)

    target.run_transaction(load)

    def swap(cursor):
        cursor.execute(
            f"SELECT COUNT(*) FROM {stage} WHERE vendor_code = ? AND snapshot_date = ?",
            vendor_code,
            snapshot_iso,
        )
        staged = int(cursor.fetchone()[0])
        cursor.execute(
            f"SELECT COUNT(*) FROM {live} WHERE vendor_code = ? AND snapshot_date = ?",
            vendor_code,
            snapshot_iso,
        )
        stored = int(cursor.fetchone()[0])
        if staged == 0 and stored == expected:
            return
        if staged != expected:
            raise RuntimeError(
                f"{vendor_code} {snapshot_iso} staged {staged} rows, expected {expected}"
            )
        cursor.execute(
            f"DELETE FROM {live} WHERE vendor_code = ? AND snapshot_date = ?",
            vendor_code,
            snapshot_iso,
        )
        cursor.execute(
            f"INSERT INTO {live} ({LOCAL_OFFER_COLUMNS}) "
            f"SELECT {LOCAL_OFFER_COLUMNS} FROM {stage} "
            f"WHERE vendor_code = ? AND snapshot_date = ?",
            vendor_code,
            snapshot_iso,
        )
        cursor.execute(
            f"DELETE FROM {stage} WHERE vendor_code = ? AND snapshot_date = ?",
            vendor_code,
            snapshot_iso,
        )

    target.run_transaction(swap)


def load_one_file(target: Target, item, offers: list[tuple]) -> None:
    """Load one snapshot file. Each selected file is a unique vendor_code plus
    snapshot_date, so deleting that slice first makes a reconnect retry safe."""
    offer_table = qualified("local_offer_next")
    params = [offer_param(offer) for offer in offers]

    def work(cursor):
        cursor.execute(
            f"DELETE FROM {offer_table} WHERE vendor_code = ? AND snapshot_date = ?",
            item.vendor_code, item.snapshot_date.isoformat(),
        )
        for start in range(0, len(params), LOCAL_BATCH):
            chunk = params[start:start + LOCAL_BATCH]
            sql = (
                f"INSERT INTO {offer_table} ({LOCAL_OFFER_COLUMNS}) VALUES "
                + ", ".join(LOCAL_OFFER_ROW for _ in chunk)
            )
            cursor.execute(sql, *[value for row in chunk for value in row])

    target.run(work)


def load_local_offers(target: Target, selected) -> tuple[int, list[ParseReject]]:
    rejects: list[ParseReject] = []
    loaded = 0
    for item in selected:
        try:
            offers, counts = iter_offer_rows(item)
        except Exception as exc:
            rejects.append(
                ParseReject("READ_FAIL", item.vendor_code, item.path.name, 1,
                            f"{type(exc).__name__}: {exc}")
            )
            continue
        load_one_file(target, item, offers)
        loaded += len(offers)
        print(f"parsed {item.vendor_code} {item.path.name} rows={len(offers)} loaded={loaded:,}")
        for reason, count in sorted(counts.items()):
            if count:
                rejects.append(
                    ParseReject(reason, item.vendor_code, item.path.name, count, "row dropped")
                )
    return loaded, rejects


def sales_reject_counts(target: Target, table: str) -> dict[str, int]:
    row = target.execute(sales_reject_sql(source_name(table)), fetch=True)
    names = ("MISSING_ORDER_ID", "INVALID_UNITS", "FUTURE_SOLD_ON")
    return {name: int(value or 0) for name, value in zip(names, row)}


def insert_rejects(target: Target, run_id: int, rejects: list[ParseReject]) -> None:
    for reject in rejects:
        target.execute(
            f"INSERT INTO {qualified('load_reject')} (run_id, reason_code, vendor_code, "
            "file_name, rejected_count, detail) VALUES (?, ?, ?, ?, ?, ?)",
            run_id, reject.reason_code, reject.vendor_code, reject.file_name,
            reject.rejected_count, reject.detail,
        )


def preserve_local_history(target: Target, selected) -> None:
    """Keep Neuco and Metropac snapshot days that this run did not reload.

    The fresh load replaces the latest Neuco file, the latest Metropac file,
    and every UED day. Older local days stay in the table across a rebuild.
    """
    live = qualified("local_offer")
    exists = target.execute(f"SELECT OBJECT_ID('{live}', 'U')", fetch=True)
    if exists is None or exists[0] is None:
        return
    replaced = {
        (item.vendor_code, item.snapshot_date.isoformat())
        for item in selected
        if item.vendor_code in {"Neuco", "Metropac"}
    }
    sql = (
        f"INSERT INTO {qualified('local_offer_next')} "
        "(vendor_code, snapshot_date, manufacturer, part_code, qty_on_hand, price, price_kind) "
        f"SELECT vendor_code, snapshot_date, manufacturer, part_code, qty_on_hand, price, price_kind "
        f"FROM {live} WHERE vendor_code IN ('Neuco', 'Metropac')"
    )
    params: list[str] = []
    for vendor_code, snapshot in sorted(replaced):
        sql += " AND NOT (vendor_code = ? AND snapshot_date = ?)"
        params.extend((vendor_code, snapshot))
    target.execute(sql, *params)


def swap_sql() -> str:
    lines = ["SET XACT_ABORT ON;", "BEGIN TRANSACTION;"]
    for table in DATA_TABLES:
        live = qualified(table)
        lines.append(f"IF OBJECT_ID('{live}', 'U') IS NOT NULL DROP TABLE {live};")
        lines.append(f"EXEC sp_rename '{live}_next', '{table}';")
    lines.append("COMMIT TRANSACTION;")
    return "\n".join(lines)


def load(roots: dict[str, Path], today: date | None = None,
         lock_resource: str = LOCK_RESOURCE) -> dict[str, int]:
    today = today or date.today()

    missing_folders = [f"{name} ({path})" for name, path in roots.items() if not path.is_dir()]
    if missing_folders:
        raise RuntimeError(
            "Local vendor folders are not available: " + "; ".join(missing_folders)
        )

    target = Target(lock_resource)
    run_id = None
    try:
        run_schema(target)

        run_id = int(
            target.execute(
                f"INSERT INTO {qualified('load_run')} (started_at, status, note) "
                "OUTPUT INSERTED.run_id VALUES (SYSDATETIME(), 'RUNNING', ?)",
                "Server-side sales and vendor changes from Parts; latest Neuco/Metropac, dated UED.",
                fetch=True,
            )[0]
        )

        for store, query in sales_insert_queries().items():
            target.execute(query)
            print(f"sales {store}: {target.cursor.rowcount:,}")
        for code, query in vendor_insert_queries().items():
            target.execute(query)
            print(f"vendor {code}: {target.cursor.rowcount:,}")

        selected, rejects = select_local_snapshots(roots, today)
        print(f"local files selected: {len(selected)}")
        _, local_rejects = load_local_offers(target, selected)
        rejects.extend(local_rejects)
        preserve_local_history(target, selected)

        for store, table in SALES_TABLES.items():
            for reason, count in sales_reject_counts(target, table).items():
                if count:
                    rejects.append(ParseReject(reason, store, table, count, "row not published"))
        insert_rejects(target, run_id, rejects)

        counts = {}
        for table in DATA_TABLES:
            counts[table] = int(
                target.execute(f"SELECT COUNT(*) FROM {qualified(table)}_next", fetch=True)[0]
            )
        if any(value == 0 for value in counts.values()):
            raise RuntimeError(f"Refusing to swap, a table is empty: {counts}")

        target.execute(swap_sql())
        target.execute(
            f"UPDATE {qualified('load_run')} SET completed_at = SYSDATETIME(), "
            "status = 'COMPLETED', sales_rows = ?, vendor_change_rows = ?, "
            "local_offer_rows = ? WHERE run_id = ?",
            counts["sales_line"], counts["vendor_change"], counts["local_offer"], run_id,
        )

        print(f"run_id {run_id}")
        for table in DATA_TABLES:
            print(f"{table} {counts[table]:,}")
        return {"run_id": run_id, **counts}
    except Exception as exc:
        if run_id is not None:
            try:
                target.execute(
                    f"UPDATE {qualified('load_run')} SET completed_at = SYSDATETIME(), "
                    "status = 'FAILED', note = ? WHERE run_id = ?",
                    str(exc)[:400], run_id, retries=1,
                )
            except Exception:
                pass
        raise
    finally:
        target.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Copy Parts sales/qty and local vendor files into SQL Server Parts1."
    )
    parser.add_argument("--neuco", type=Path, default=DEFAULT_ROOTS["Neuco"])
    parser.add_argument("--metropac", type=Path, default=DEFAULT_ROOTS["Metropac"])
    parser.add_argument("--ued", type=Path, default=DEFAULT_ROOTS["UED"])
    parser.add_argument(
        "--lock-suffix", default="",
        help="Recovery only: append to the lock name if a prior force-killed run "
             "left the default lock held by an un-killable session.",
    )
    args = parser.parse_args()
    load(
        {"Neuco": args.neuco, "Metropac": args.metropac, "UED": args.ued},
        lock_resource=LOCK_RESOURCE + args.lock_suffix,
    )


if __name__ == "__main__":
    main()
