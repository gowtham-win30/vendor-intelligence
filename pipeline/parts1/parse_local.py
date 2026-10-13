"""Read local vendor files into a few offer columns.

Neuco and Metropac contribute the latest file only. UED contributes every file
whose name resolves to one calendar date. Source files are never modified.
"""
from __future__ import annotations

import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from itertools import combinations
from pathlib import Path

# Extra digits in a UED name are accepted only when one calendar date lands here.
UED_TRIM_START = date(2025, 12, 1)
UED_TRIM_END = date(2026, 9, 30)

OFFER_COLUMNS = (
    "vendor_code",
    "snapshot_date",
    "manufacturer",
    "part_code",
    "qty_on_hand",
    "price",
    "price_kind",
)


@dataclass(frozen=True)
class SelectedFile:
    path: Path
    vendor_code: str
    snapshot_date: date
    price_kind: str
    manufacturer_column: str
    part_column: str
    qty_column: str
    price_column: str


@dataclass(frozen=True)
class ParseReject:
    reason_code: str
    vendor_code: str
    file_name: str
    rejected_count: int
    detail: str


def normalize_header(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\ufeff", "").strip().lower()
    return re.sub(r"\s+", " ", text)


def _calendar_date(token: str) -> date | None:
    if len(token) != 6 or not token.isdigit():
        return None
    try:
        return datetime.strptime(token, "%m%d%y").date()
    except ValueError:
        return None


def ued_snapshot_date(file_name: str, today: date) -> tuple[str, date | None]:
    stem = Path(file_name).stem
    stem = re.sub(r"\s*\(\d+\)\s*$", "", stem)
    stem = re.sub(r"B$", "", stem, flags=re.I)
    stem = re.sub(r"IInventory", "Inventory", stem, flags=re.I)
    stem = re.sub(r"InventoryFile", "Inventory", stem, flags=re.I)
    match = re.search(r"Inventory(\d+)$", stem, flags=re.I)
    if not match:
        return "NO_DATE_TOKEN", None
    digits = match.group(1)
    if len(digits) < 6:
        return "DATE_TOO_SHORT", None
    if len(digits) == 6:
        parsed = _calendar_date(digits)
        if parsed is None:
            return "DATE_UNPARSEABLE", None
        if parsed > today:
            return "DATE_IN_FUTURE", None
        return "OK", parsed
    found: dict[date, str] = {}
    for removed in combinations(range(len(digits)), len(digits) - 6):
        token = "".join(ch for i, ch in enumerate(digits) if i not in set(removed))
        parsed = _calendar_date(token)
        if parsed is None or parsed < UED_TRIM_START or parsed > UED_TRIM_END or parsed > today:
            continue
        found[parsed] = token
    if len(found) == 1:
        return "OK_TRIMMED", next(iter(found))
    if not found:
        return "DATE_UNPARSEABLE", None
    return "AMBIGUOUS_DATE", None


def neuco_snapshot_date(file_name: str) -> date | None:
    match = re.search(r"\.daily\.(\d{8})", file_name, flags=re.I)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def utc_file_date(path: Path) -> date:
    modified = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
    return modified.date()


def _later(paths: list[Path]) -> Path:
    return max(paths, key=lambda path: (path.stat().st_mtime, path.name))


def select_local_snapshots(
    roots: dict[str, Path],
    today: date,
) -> tuple[list[SelectedFile], list[ParseReject]]:
    selected: list[SelectedFile] = []
    rejects: list[ParseReject] = []
    selected.extend(_select_neuco(roots.get("Neuco"), rejects))
    selected.extend(_select_metropac(roots.get("Metropac"), rejects))
    selected.extend(_select_ued(roots.get("UED"), today, rejects))
    return selected, rejects


def _missing_folder(vendor_code: str, root: Path | None, rejects: list[ParseReject]) -> bool:
    if root is not None and root.is_dir():
        return False
    rejects.append(
        ParseReject("FOLDER_MISSING", vendor_code, str(root or ""), 1, "local folder is not available")
    )
    return True


def _select_neuco(root: Path | None, rejects: list[ParseReject]) -> list[SelectedFile]:
    if _missing_folder("Neuco", root, rejects):
        return []
    dated: dict[date, list[Path]] = defaultdict(list)
    for path in root.glob("*.txt"):
        snapshot = neuco_snapshot_date(path.name)
        if snapshot is None:
            rejects.append(ParseReject("DATE_UNPARSEABLE", "Neuco", path.name, 1, "name has no daily.YYYYMMDD"))
            continue
        dated[snapshot].append(path)
    if not dated:
        rejects.append(ParseReject("NO_FILES", "Neuco", "", 0, "no dated Neuco files"))
        return []
    newest = max(dated)
    winner = _later(dated[newest])
    for path in dated[newest]:
        if path != winner:
            rejects.append(ParseReject("DUPLICATE_DATE", "Neuco", path.name, 1, f"kept {winner.name}"))
    return [_neuco_file(winner, newest)]


def _neuco_file(path: Path, snapshot: date) -> SelectedFile:
    return SelectedFile(
        path=path,
        vendor_code="Neuco",
        snapshot_date=snapshot,
        price_kind="SELL",
        manufacturer_column="manu code",
        part_column="item",
        qty_column="qty available",
        price_column="sell_price",
    )


def _select_metropac(root: Path | None, rejects: list[ParseReject]) -> list[SelectedFile]:
    if _missing_folder("Metropac", root, rejects):
        return []
    files = [path for path in root.glob("*.txt") if not path.name.startswith("~$")]
    if not files:
        rejects.append(ParseReject("NO_FILES", "Metropac", "", 0, "no Metropac files"))
        return []
    winner = _later(files)
    return [
        SelectedFile(
            path=winner,
            vendor_code="Metropac",
            snapshot_date=utc_file_date(winner),
            price_kind="NET",
            manufacturer_column="manufacturer",
            part_column="metropac part number",
            qty_column="onhand",
            price_column="net_price",
        )
    ]


def _select_ued(root: Path | None, today: date, rejects: list[ParseReject]) -> list[SelectedFile]:
    if _missing_folder("UED", root, rejects):
        return []
    dated: dict[date, list[Path]] = defaultdict(list)
    locks = 0
    for path in root.iterdir():
        if not path.is_file():
            continue
        if path.name.startswith("~$"):
            locks += 1
            continue
        if path.suffix.lower() not in {".csv", ".xlsx"}:
            rejects.append(ParseReject("UNSUPPORTED_FILE", "UED", path.name, 1, path.suffix))
            continue
        status, snapshot = ued_snapshot_date(path.name, today)
        if status in {"OK", "OK_TRIMMED"} and snapshot is not None:
            dated[snapshot].append(path)
            continue
        rejects.append(ParseReject(status, "UED", path.name, 1, "snapshot date was not stored"))
    if locks:
        rejects.append(ParseReject("SKIP_LOCK", "UED", "", locks, "Excel lock file, not data"))
    chosen: list[SelectedFile] = []
    for snapshot, paths in sorted(dated.items()):
        winner = _later(paths)
        for path in paths:
            if path != winner:
                rejects.append(
                    ParseReject("DUPLICATE_DATE", "UED", path.name, 1, f"kept {winner.name}")
                )
        chosen.append(
            SelectedFile(
                path=winner,
                vendor_code="UED",
                snapshot_date=snapshot,
                price_kind="DEALER_EXCHANGE",
                manufacturer_column="mfg",
                part_column="mfgitemcode",
                qty_column="qtyavail",
                price_column="dealerexchprice",
            )
        )
    return chosen


def parse_price(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    text = str(value).strip().replace(",", "")
    if text == "":
        return None
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise ValueError("price is not numeric") from exc


def format_price(price: Decimal) -> str:
    text = format(price, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def qty_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    return text or None


def _cell(row: list[object], index: int) -> object:
    if index >= len(row):
        return None
    return row[index]


def iter_offer_rows(selected: SelectedFile):
    header, rows, close = _open_table(selected.path)
    try:
        return _offers_from_rows(selected, header, rows)
    finally:
        close()


def _offers_from_rows(selected: SelectedFile, header: list[object], rows) -> tuple[list[tuple], dict[str, int]]:
    normalized = [normalize_header(cell) for cell in header]
    required = {
        "manufacturer": selected.manufacturer_column,
        "part": selected.part_column,
        "qty": selected.qty_column,
        "price": selected.price_column,
    }
    missing = [name for name in required.values() if name not in normalized]
    if missing:
        raise ValueError("missing columns: " + ", ".join(missing))
    indexes = {key: normalized.index(name) for key, name in required.items()}
    seen: set[tuple[str, str]] = set()
    counts: dict[str, int] = defaultdict(int)
    offers: list[tuple] = []
    for row in rows:
        manufacturer = str(_cell(row, indexes["manufacturer"]) or "").strip()
        part_code = str(_cell(row, indexes["part"]) or "").strip()
        try:
            price = parse_price(_cell(row, indexes["price"]))
        except ValueError:
            counts["PRICE_NOT_NUMERIC"] += 1
            continue
        qty = qty_text(_cell(row, indexes["qty"]))
        if not manufacturer or not part_code:
            if price is None and qty is None:
                continue
            counts["MISSING_PART_KEY"] += 1
            continue
        if price is not None and price < 0:
            counts["NEGATIVE_PRICE"] += 1
            continue
        key = (manufacturer, part_code)
        if key in seen:
            counts["DUPLICATE_PART"] += 1
            continue
        seen.add(key)
        offers.append(
            (
                selected.vendor_code,
                selected.snapshot_date.isoformat(),
                manufacturer,
                part_code,
                qty or "",
                "" if price is None else format_price(price),
                selected.price_kind,
            )
        )
    return offers, counts


def _open_table(path: Path):
    if path.suffix.lower() == ".xlsx":
        import openpyxl

        workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
        sheet = workbook.worksheets[0]
        iterator = sheet.iter_rows(values_only=True)
        try:
            header = list(next(iterator))
        except StopIteration:
            workbook.close()
            return [], iter(()), workbook.close
        return header, iterator, workbook.close
    text = _read_text(path)
    first = text.splitlines()[0] if text else ""
    delimiter = "\t" if "\t" in first else ","
    rows = list(csv.reader(text.splitlines(), delimiter=delimiter))
    if not rows:
        return [], iter(()), lambda: None
    return rows[0], iter(rows[1:]), lambda: None


def _read_text(path: Path) -> str:
    raw = path.read_bytes()
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def write_offers(selected_files: list[SelectedFile], destination: Path) -> list[ParseReject]:
    rejects: list[ParseReject] = []
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        for selected in selected_files:
            try:
                offers, counts = iter_offer_rows(selected)
            except Exception as exc:
                rejects.append(
                    ParseReject(
                        "READ_FAIL",
                        selected.vendor_code,
                        selected.path.name,
                        1,
                        f"{type(exc).__name__}: {exc}",
                    )
                )
                continue
            writer.writerows(offers)
            print(f"parsed {selected.vendor_code} {selected.path.name} rows={len(offers)}")
            for reason, count in sorted(counts.items()):
                if count:
                    rejects.append(
                        ParseReject(reason, selected.vendor_code, selected.path.name, count, "row dropped")
                    )
    return rejects
