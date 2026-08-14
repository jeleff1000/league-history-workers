"""Turn source rows covered by a spec-map contract into candidate cache cells.

This module is deliberately a staging-only boundary.  It cannot open or write
the canonical cache.  Its output has one source-backed candidate cell per row,
plus a receipt that explains every skipped source row.  A separate, guarded
comparison stage will decide whether each candidate reaches a canonical row.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

import duckdb

from normalize_artifact_promotion_candidates import normalize_source_row


CANDIDATE_COLUMNS = (
    "artifact_id", "artifact_file", "file_instance", "source_grain",
    "db_name", "year", "week", "NFL_player_id", "team_key", "manager",
    "source_column", "target_column", "source_value_text",
)


def parse_pipe(value: str) -> tuple[str, ...]:
    return tuple(part for part in (value or "").split("|") if part)


def parse_mapping(value: str) -> tuple[tuple[str, str], ...]:
    mappings: list[tuple[str, str]] = []
    for item in parse_pipe(value):
        if ":" not in item:
            raise ValueError(f"invalid source-to-target mapping: {item}")
        source, target = item.split(":", 1)
        if not source or not target:
            raise ValueError(f"invalid source-to-target mapping: {item}")
        mappings.append((source, target))
    return tuple(mappings)


def materialize_rows(
    spec: dict[str, str], rows: Iterable[dict[str, Any]],
) -> tuple[list[dict[str, Any]], Counter[str]]:
    """Normalize source rows under one fixed spec-map row.

    Missing identity keys never create a partial candidate.  The caller uses
    the receipt counter to retain a specific, auditable reason in the one
    existing artifact ledger.
    """
    source_key = parse_pipe(spec["source_key"])
    mapping = parse_mapping(spec["source_to_target_columns"])
    candidates: list[dict[str, Any]] = []
    receipt: Counter[str] = Counter()
    for row in rows:
        receipt["source_rows"] += 1
        try:
            normalized = normalize_source_row(
                artifact_id=spec["artifact_id"],
                source_grain=spec["source_grain"],
                source_key=source_key,
                source_to_target=mapping,
                row=row,
            )
        except ValueError as exc:
            receipt[f"blocked_{str(exc).replace(' ', '_')}"] += 1
            continue
        for cell in normalized:
            cell["artifact_file"] = spec["file"]
            cell["file_instance"] = spec["file_instance"]
            value = cell.pop("source_value")
            cell["source_value_text"] = str(value)
            candidates.append({column: cell.get(column) for column in CANDIDATE_COLUMNS})
        receipt["candidate_cells"] += len(normalized)
    return candidates, receipt


def _source_paths(root: Path, *, name: str, instance: int) -> Path:
    matches = sorted(path for path in root.rglob(name) if path.is_file())
    if instance < 1 or len(matches) < instance:
        raise FileNotFoundError(
            f"source file instance missing: name={name!r} instance={instance} matches={len(matches)}"
        )
    return matches[instance - 1]


def _read_parquet_rows(path: Path, required_columns: set[str]) -> list[dict[str, Any]]:
    con = duckdb.connect()
    try:
        columns = {row[0] for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()}
        missing = sorted(required_columns - columns)
        if missing:
            raise ValueError(f"source schema drift: missing columns {missing}")
        ordered = sorted(required_columns)
        quoted = ", ".join('"' + field.replace('"', '""') + '"' for field in ordered)
        result = con.execute(f"SELECT {quoted} FROM read_parquet(?)", [str(path)]).fetchall()
        return [dict(zip(ordered, values)) for values in result]
    finally:
        con.close()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--specmap", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--artifact-id", required=True)
    parser.add_argument("--out-parquet", type=Path, required=True)
    parser.add_argument("--out-receipt", type=Path, required=True)
    args = parser.parse_args()

    with args.specmap.open(newline="", encoding="utf-8") as handle:
        specs = [row for row in csv.DictReader(handle) if row["artifact_id"] == str(args.artifact_id)]
    if not specs:
        raise SystemExit(f"artifact {args.artifact_id} is not in the fixed spec map")

    all_cells: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    for spec in specs:
        if spec["source_grain"] not in {"player_week", "team_week", "team_week_manager", "league_year_settings"}:
            receipts.append({
                "artifact_id": spec["artifact_id"], "file": spec["file"], "file_instance": spec["file_instance"],
                "status": "not_data_bearing", "reason": spec["terminal_reason"], "candidate_cells": 0,
            })
            continue
        mapping = parse_mapping(spec["source_to_target_columns"])
        if not mapping:
            receipts.append({
                "artifact_id": spec["artifact_id"], "file": spec["file"], "file_instance": spec["file_instance"],
                "status": "no_writable_columns", "reason": "empty schema-bound mapping", "candidate_cells": 0,
            })
            continue
        try:
            path = _source_paths(args.source_root, name=spec["file"], instance=int(spec["file_instance"]))
            required = set(parse_pipe(spec["source_key"])) | {source for source, _ in mapping}
            rows = _read_parquet_rows(path, required)
            cells, counts = materialize_rows(spec, rows)
            all_cells.extend(cells)
            receipts.append({
                "artifact_id": spec["artifact_id"], "file": spec["file"], "file_instance": spec["file_instance"],
                "status": "candidate_cells_materialized", "source_path": str(path), "source_sha256": _sha256(path),
                **dict(counts),
            })
        except Exception as exc:
            receipts.append({
                "artifact_id": spec["artifact_id"], "file": spec["file"], "file_instance": spec["file_instance"],
                "status": "materialization_blocked", "reason": f"{type(exc).__name__}: {exc}", "candidate_cells": 0,
            })

    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE candidates AS SELECT * FROM (SELECT NULL::VARCHAR AS artifact_id, NULL::VARCHAR AS artifact_file, NULL::VARCHAR AS file_instance, NULL::VARCHAR AS source_grain, NULL::VARCHAR AS db_name, NULL::VARCHAR AS year, NULL::VARCHAR AS week, NULL::VARCHAR AS NFL_player_id, NULL::VARCHAR AS team_key, NULL::VARCHAR AS manager, NULL::VARCHAR AS source_column, NULL::VARCHAR AS target_column, NULL::VARCHAR AS source_value_text) WHERE FALSE")
        if all_cells:
            placeholders = ", ".join("?" for _ in CANDIDATE_COLUMNS)
            con.executemany(
                f"INSERT INTO candidates VALUES ({placeholders})",
                [tuple(cell[column] for column in CANDIDATE_COLUMNS) for cell in all_cells],
            )
        args.out_parquet.parent.mkdir(parents=True, exist_ok=True)
        escaped = str(args.out_parquet).replace("'", "''")
        con.execute(f"COPY candidates TO '{escaped}' (FORMAT PARQUET)")
    finally:
        con.close()
    args.out_receipt.parent.mkdir(parents=True, exist_ok=True)
    args.out_receipt.write_text(json.dumps({
        "artifact_id": str(args.artifact_id), "files": receipts,
        "candidate_cells": len(all_cells), "cache_mutated": False, "new_lineage": False,
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"artifact_id": str(args.artifact_id), "candidate_cells": len(all_cells), "files": len(receipts), "cache_mutated": False, "new_lineage": False}, sort_keys=True))


if __name__ == "__main__":
    main()
