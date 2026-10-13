"""Upload the local files that did not finish.

The SQL Server connection resets during a large insert. The previous loader
deleted a day before that insert finished, so the day was left partial and
every later Neuco file was never sent. This run writes each day into
local_offer_stage and replaces the live day only after the staged count matches.
Days already stored in full are left as they are.

Neuco files after the first day are stored as the rows whose price or quantity
changed. Copying every repeated catalog row is hundreds of millions of rows,
and this connection already drops on a single 10,000-row file.
"""
from __future__ import annotations

import pickle
import re
from collections import defaultdict
from datetime import date
from itertools import combinations
from pathlib import Path

import os

from pipeline.parts1.load import LOCK_RESOURCE, Target, ensure_offer_stage, stage_offer_slice
from pipeline.parts1.load_missing import changed_offers, metropac_days, neuco_days
from pipeline.parts1.parse_local import (
    SelectedFile,
    _calendar_date,
    _later,
    _neuco_file,
    iter_offer_rows,
    ued_snapshot_date,
    utc_file_date,
)

NEUCO_ROOT = Path(os.getenv("NEUCO_FILES_DIR", r"E:\NeucoFiles"))
METRO_ROOT = Path(os.getenv("METROPAC_FILES_DIR", r"E:\Metropac"))
UED_ROOT = Path(os.getenv("UED_FILES_DIR", r"E:\UED FIiles"))
STATE_PATH = Path(__file__).resolve().parents[1] / "state" / "neuco_chain.pkl"
NEUCO_PARTIAL = date(2024, 4, 8)
UED_TRIM_START = date(2025, 12, 1)
UED_TRIM_END = date(2026, 9, 30)


def as_date(value) -> date:
    if isinstance(value, date) and not hasattr(value, "hour"):
        return value
    text = str(value).strip().strip("'")[:10]
    return date.fromisoformat(text)


def snapshot_counts(target: Target, vendor_code: str) -> dict[date, int]:
    target.cursor.execute(
        "SELECT CONVERT(varchar(10), snapshot_date, 23), COUNT(*) "
        "FROM vendor_intelligence.local_offer "
        "WHERE vendor_code = ? "
        "GROUP BY CONVERT(varchar(10), snapshot_date, 23)",
        vendor_code,
    )
    return {as_date(row[0]): int(row[1]) for row in target.cursor.fetchall()}


def file_data_lines(path: Path) -> int:
    count = 0
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for count, _line in enumerate(handle, start=1):
            pass
    return max(count - 1, 0)


def load_metropac(target: Target) -> None:
    if not METRO_ROOT.is_dir():
        print(f"Metropac folder not available, skipping: {METRO_ROOT}")
        return
    stored = snapshot_counts(target, "Metropac")
    for snapshot, path in metropac_days(METRO_ROOT):
        lines = file_data_lines(path)
        have = stored.get(snapshot, 0)
        if have >= int(0.9 * lines) and have > 0:
            continue
        item = SelectedFile(
            path=path,
            vendor_code="Metropac",
            snapshot_date=snapshot,
            price_kind="NET",
            manufacturer_column="manufacturer",
            part_column="metropac part number",
            qty_column="onhand",
            price_column="net_price",
        )
        offers, _counts = iter_offer_rows(item)
        print(f"Metropac upload {snapshot} {path.name} file_rows={lines:,} db_rows={have:,} parsed={len(offers):,}")
        stage_offer_slice(target, "Metropac", snapshot.isoformat(), offers)
        print(f"Metropac stored {snapshot} rows={len(offers):,}")


def ued_candidates(file_name: str, today: date) -> set[date]:
    stem = Path(file_name).stem
    stem = re.sub(r"\s*\(\d+\)\s*$", "", stem)
    stem = re.sub(r"B$", "", stem, flags=re.I)
    match = re.search(r"Inventory(\d+)$", stem, flags=re.I)
    if match is None:
        return set()
    digits = match.group(1)
    if len(digits) <= 6:
        return set()
    found: set[date] = set()
    for removed in combinations(range(len(digits)), len(digits) - 6):
        token = "".join(ch for i, ch in enumerate(digits) if i not in set(removed))
        parsed = _calendar_date(token)
        if parsed is None or parsed < UED_TRIM_START or parsed > UED_TRIM_END or parsed > today:
            continue
        found.add(parsed)
    return found


def load_ued(target: Target) -> None:
    if not UED_ROOT.is_dir():
        print(f"UED folder not available, skipping: {UED_ROOT}")
        return
    stored = snapshot_counts(target, "UED")
    today = date.today()
    dated: dict[date, list[Path]] = defaultdict(list)
    for path in UED_ROOT.iterdir():
        if not path.is_file() or path.name.startswith("~$"):
            continue
        if path.suffix.lower() not in {".csv", ".xlsx"}:
            continue
        status, snapshot = ued_snapshot_date(path.name, today)
        if status in {"OK", "OK_TRIMMED"} and snapshot is not None:
            dated[snapshot].append(path)
            continue
        if status != "AMBIGUOUS_DATE":
            print(f"UED skipped {path.name} {status}")
            continue
        candidates = ued_candidates(path.name, today)
        modified = utc_file_date(path)
        if modified not in candidates:
            print(f"UED skipped {path.name} ambiguous candidates={sorted(candidates)} mtime={modified}")
            continue
        if modified in stored:
            print(f"UED {path.name} matches {modified}, and that day is already uploaded")
            continue
        print(f"UED upload ambiguous {path.name} as {modified}")
        item = SelectedFile(
            path=path,
            vendor_code="UED",
            snapshot_date=modified,
            price_kind="DEALER_EXCHANGE",
            manufacturer_column="mfg",
            part_column="mfgitemcode",
            qty_column="qtyavail",
            price_column="dealerexchprice",
        )
        offers, _counts = iter_offer_rows(item)
        stage_offer_slice(target, "UED", modified.isoformat(), offers)
        stored[modified] = len(offers)
        print(f"UED stored {modified} rows={len(offers):,}")
    for snapshot, paths in dated.items():
        if len(paths) < 2:
            continue
        winner = _later(paths)
        for path in paths:
            if path != winner:
                print(f"UED {path.name} is an earlier copy of {snapshot}; kept {winner.name}")


def save_chain(snapshot: date, current: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = STATE_PATH.with_suffix(".tmp")
    with temporary.open("wb") as handle:
        pickle.dump({"snapshot": snapshot.isoformat(), "current": current}, handle, protocol=pickle.HIGHEST_PROTOCOL)
    temporary.replace(STATE_PATH)


def load_chain() -> tuple[date | None, dict | None]:
    if not STATE_PATH.exists():
        return None, None
    with STATE_PATH.open("rb") as handle:
        payload = pickle.load(handle)
    return date.fromisoformat(payload["snapshot"]), payload["current"]


def neuco_needed(snapshot: date, latest: date, stored: dict[date, int]) -> bool:
    if snapshot == latest:
        return stored.get(snapshot, 0) < 100_000
    if snapshot < NEUCO_PARTIAL:
        return stored.get(snapshot, 0) == 0
    return True


def load_neuco(target: Target) -> None:
    if not NEUCO_ROOT.is_dir():
        print(f"Neuco folder not available, skipping: {NEUCO_ROOT}")
        return
    stored = snapshot_counts(target, "Neuco")
    days = neuco_days(NEUCO_ROOT)
    latest = days[-1][0]
    resume_on, previous = load_chain()
    print(f"Neuco files={len(days)} resume_after={resume_on} latest={latest} db_days={len(stored)}")
    for snapshot, path in days:
        if resume_on is not None and snapshot <= resume_on:
            continue
        item = _neuco_file(path, snapshot)
        try:
            offers, _counts = iter_offer_rows(item)
        except ValueError as exc:
            print(f"Neuco skipped {snapshot} {path.name}: {exc}")
            if previous is not None:
                save_chain(snapshot, previous)
            continue
        current = {(row[2], row[3]): (row[4], row[5]) for row in offers}
        if neuco_needed(snapshot, latest, stored):
            delta = changed_offers("Neuco", snapshot, item.price_kind, current, previous)
            print(
                f"Neuco upload {snapshot} {path.name} catalog={len(offers):,} "
                f"kept={len(delta):,}"
            )
            stage_offer_slice(target, "Neuco", snapshot.isoformat(), delta)
            print(f"Neuco stored {snapshot} rows={len(delta):,}")
        else:
            print(f"Neuco already stored {snapshot} catalog={len(offers):,}")
        previous = current
        save_chain(snapshot, current)
        resume_on = None


def main() -> None:
    target = Target(LOCK_RESOURCE + "_resume")
    try:
        ensure_offer_stage(target)
        load_metropac(target)
        load_ued(target)
        load_neuco(target)
        print("local upload finished")
    finally:
        target.close()


if __name__ == "__main__":
    main()
