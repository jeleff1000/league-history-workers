"""Map every audited artifact file to a fixed canonical promotion contract.

The output is an evidence artifact, not a second ledger and not a cache write.
The durable CSV ledger remains the sole registry; this map tells its promotion
worker which exact contract each source file must satisfy.
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path

import duckdb

from artifact_promotion_specmap import bind_to_canonical_schema, classify_source_schema


OUTPUT_FIELDS = (
    "artifact_id", "artifact", "file", "source_grain", "source_key", "cache_key",
    "writable_target_columns", "blocked_source_columns", "may_insert_missing_player_rows",
    "terminal_reason", "quarantined_source_columns",
)


def _columns(value: str) -> set[str]:
    parsed = json.loads(value or "[]")
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise ValueError("checklist columns must be a JSON string array")
    return set(parsed)


def _joined(values: tuple[str, ...]) -> str:
    return "|".join(values)


def build_specmap_rows(
    checklist_rows: list[dict[str, str]], *, canonical_columns: set[str],
) -> list[dict[str, str]]:
    """Return one stable source-to-cache contract row per inspected file."""
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in checklist_rows:
        artifact_id = str(row.get("artifact_id", "") or "")
        file_name = str(row.get("file", "") or "")
        if not artifact_id or not file_name:
            raise ValueError("checklist row is missing artifact_id or file")
        identity = (artifact_id, file_name)
        if identity in seen:
            raise ValueError(f"duplicate artifact file identity: {identity}")
        seen.add(identity)
        contract = classify_source_schema(_columns(str(row.get("columns", "") or "[]")))
        bound = bind_to_canonical_schema(contract, canonical_columns)
        out.append({
            "artifact_id": artifact_id,
            "artifact": str(row.get("artifact", "") or ""),
            "file": file_name,
            "source_grain": contract.grain,
            "source_key": _joined(contract.source_key),
            "cache_key": _joined(contract.cache_key),
            "writable_target_columns": _joined(bound.writable_target_columns),
            "blocked_source_columns": _joined(bound.blocked_source_columns),
            "may_insert_missing_player_rows": str(contract.may_insert_missing_player_rows).lower(),
            "terminal_reason": contract.terminal_reason,
            "quarantined_source_columns": _joined(contract.quarantined_source_columns),
        })
    return sorted(out, key=lambda item: (int(item["artifact_id"]), item["file"]))


def _canonical_columns(path: Path) -> set[str]:
    con = duckdb.connect(str(path), read_only=True)
    try:
        return {row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()}
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--checklist", type=Path, required=True)
    parser.add_argument("--canonical-cache", type=Path, required=True)
    parser.add_argument("--out-csv", type=Path, required=True)
    parser.add_argument("--out-json", type=Path, required=True)
    args = parser.parse_args()

    with args.checklist.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    result_rows = build_specmap_rows(rows, canonical_columns=_canonical_columns(args.canonical_cache))
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.out_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(result_rows)
    summary = {
        "artifact_files": len(result_rows),
        "artifact_ids": len({row["artifact_id"] for row in result_rows}),
        "source_grains": dict(sorted(Counter(row["source_grain"] for row in result_rows).items())),
        "files_with_writable_cells": sum(bool(row["writable_target_columns"]) for row in result_rows),
        "files_with_schema_blocked_cells": sum(bool(row["blocked_source_columns"]) for row in result_rows),
        "cache_mutated": False,
        "new_lineage": False,
    }
    args.out_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
