"""Extract safe player-table signals from structured artifact payloads."""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator

SIGNALS = {"win", "loss", "tie", "team_points", "is_playoffs", "champion", "final_playoff_seed", "made_playoffs", "clutch_equity"}


def records(value: Any, parent: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
    context = dict(parent or {})
    if isinstance(value, dict):
        own = {str(k): v for k, v in value.items() if not isinstance(v, (dict, list))}
        merged = {**context, **own}
        if own:
            yield merged
        for child in value.values():
            if isinstance(child, (dict, list)):
                yield from records(child, merged)
    elif isinstance(value, list):
        for child in value:
            yield from records(child, context)


def load(path: Path) -> Iterator[dict[str, Any]]:
    try:
        if path.suffix.lower() == ".csv":
            with path.open(newline="", encoding="utf-8", errors="replace") as f:
                yield from records(list(csv.DictReader(f)))
        elif path.suffix.lower() == ".jsonl":
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                if line.strip():
                    try:
                        yield from records(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        elif path.suffix.lower() == ".json":
            try:
                yield from records(json.loads(path.read_text(encoding="utf-8", errors="replace")))
            except json.JSONDecodeError:
                return
    except OSError:
        return


def truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def norm(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).strip().lower().split())
    return text or None


def value_key(value: Any) -> str:
    if value is None or value == "":
        return "<NULL>"
    return str(value)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidates", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--report", type=Path, required=True)
    args = ap.parse_args()

    grouped: dict[tuple[str, int, int, str], list[dict[str, Any]]] = defaultdict(list)
    files_seen = 0
    for path in sorted(args.candidates.rglob("*")):
        if path.suffix.lower() not in {".json", ".jsonl", ".csv"}:
            continue
        for row in load(path):
            keys = {str(k).lower(): v for k, v in row.items()}
            db_name = keys.get("db_name")
            manager = keys.get("manager")
            try:
                year = int(keys.get("year"))
                week = int(keys.get("week"))
            except (TypeError, ValueError):
                continue
            manager_key = norm(manager)
            if not db_name or manager_key is None or not (1 <= week <= 30):
                continue
            source: dict[str, Any] = {
                "db_name": str(db_name), "year": year, "week": week,
                "manager": str(manager), "manager_key": manager_key,
                "platform": norm(keys.get("platform")),
                "source_file": str(path),
            }
            for field in SIGNALS:
                if field in keys and keys[field] not in (None, ""):
                    source[f"source_{field}"] = keys[field]
            # Championship credit requires an explicit championship-game
            # marker.  A season-level champion flag is not sufficient.
            if truthy(keys.get("is_championship")):
                source["source_is_championship"] = 1
                source["source_is_playoffs"] = 1
                source["source_champion"] = 1 if truthy(keys.get("champion")) else 0
            if any(k.startswith("source_") and k not in {"source_file", "source_is_championship"} for k in source):
                grouped[(str(db_name), year, week, manager_key)].append(source)
        files_seen += 1

    out_rows: list[dict[str, Any]] = []
    conflicts = 0
    duplicate_rows = 0
    field_counts: dict[str, int] = defaultdict(int)
    for key, rows in sorted(grouped.items()):
        duplicate_rows += max(0, len(rows) - 1)
        fields = sorted({k for row in rows for k in row if k.startswith("source_") and k not in {"source_file", "source_is_championship"}})
        bad = False
        result = {"db_name": key[0], "year": key[1], "week": key[2], "manager": key[3]}
        # Preserve source provenance in the sidecar.  This is not a canonical
        # player-table column; it is required for the file-by-file promotion
        # ledger and is ignored by the player materializer.
        result["source_files"] = "|".join(sorted({str(row.get("source_file")) for row in rows if row.get("source_file")}))
        for field in fields:
            vals = {value_key(row.get(field)) for row in rows}
            if len(vals) > 1:
                bad = True
                break
            result[field] = next((row.get(field) for row in rows if field in row), None)
            field_counts[field] += 1
        if bad:
            conflicts += 1
            continue
        out_rows.append(result)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    if out_rows:
        import pyarrow as pa
        import pyarrow.parquet as pq
        keys = ["db_name", "year", "week", "manager"]
        fields = sorted({k for row in out_rows for k in row if k.startswith("source_") or k == "source_files"})
        table = pa.Table.from_pylist([{k: row.get(k) for k in keys + fields} for row in out_rows])
        pq.write_table(table, args.out)
    else:
        import pyarrow as pa
        import pyarrow.parquet as pq
        pq.write_table(pa.table({"db_name": pa.array([], type=pa.string()), "year": pa.array([], type=pa.int32()), "week": pa.array([], type=pa.int32()), "manager": pa.array([], type=pa.string())}), args.out)
    report = {
        "files_seen": files_seen, "candidate_rows": sum(len(v) for v in grouped.values()),
        "delta_rows": len(out_rows), "duplicate_rows_collapsed": duplicate_rows,
        "conflicting_keys_excluded": conflicts, "field_rows": dict(sorted(field_counts.items())),
        "schema_changed": False, "new_lineage": False, "cache_mutated": False,
        "championship_requires_explicit_marker": True,
    }
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
