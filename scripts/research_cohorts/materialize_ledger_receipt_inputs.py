"""Create receipt inputs from the checked-in artifact ledger.

The committed CSV is the current 9,479-artifact ledger.  Readback workflows
must not spend GitHub Actions API quota downloading older copies of it merely
to seed their receipt totals.  This helper preserves every ledger row and
materializes the prior-count shape consumed by ``finalize_artifact_cache_receipts``.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


COUNT_COLUMNS = (
    "source_cells",
    "cache_match_cells",
    "cache_missing_cells",
    "cache_conflict_cells",
    "unmatched_cache_cells",
    "blocked_schema_cells",
)


def _count(value: str | None) -> int:
    return int(value or 0)


def materialize(
    ledger_csv: Path,
    ledger_json: Path,
    prior_receipts: Path,
    *,
    exclude_artifact_ids: set[str] | None = None,
) -> dict[str, int]:
    """Write full-ledger and receipt-seed JSON files without changing the CSV."""
    with ledger_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise SystemExit("artifact ledger CSV is empty")
    if any(not row.get("artifact_id") for row in rows):
        raise SystemExit("artifact ledger contains a row without artifact_id")

    ledger_json.write_text(json.dumps({"rows": rows}, indent=2) + "\n", encoding="utf-8")
    excluded = exclude_artifact_ids or set()
    prior_rows = [
        {"artifact_id": row["artifact_id"], **{column: _count(row.get(column)) for column in COUNT_COLUMNS}}
        for row in rows
        if row["artifact_id"] not in excluded
    ]
    prior_receipts.write_text(json.dumps({"rows": prior_rows}, indent=2) + "\n", encoding="utf-8")
    return {"ledger_rows": len(rows), "prior_receipt_rows": len(prior_rows)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger-csv", type=Path, required=True)
    parser.add_argument("--ledger-json", type=Path, required=True)
    parser.add_argument("--prior-receipts", type=Path, required=True)
    parser.add_argument("--exclude-artifact-ids", default="")
    args = parser.parse_args()
    excluded = {value.strip() for value in args.exclude_artifact_ids.split(",") if value.strip()}
    print(json.dumps(
        materialize(
            args.ledger_csv, args.ledger_json, args.prior_receipts,
            exclude_artifact_ids=excluded,
        ),
        sort_keys=True,
    ))


if __name__ == "__main__":
    main()
