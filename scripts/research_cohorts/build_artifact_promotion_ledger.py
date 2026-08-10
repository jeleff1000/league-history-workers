"""Build a file-by-file evidence ledger for an audited promotion candidate.

This ledger never mutates the canonical cache.  It maps every source path in
the candidate deltas back to the frozen artifact manifest and records whether
the working-copy materializer proved the accepted cells on readback.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path

import duckdb


SOURCE_FIELD_MAP = {
    "team": {
        "source_win": "win", "source_loss": "loss", "source_tie": "tie",
        "source_team_points": "team_points", "source_playoffs": "is_playoffs",
        "source_champion": "champion", "source_final_playoff_seed": "final_playoff_seed",
        "source_made_playoffs": "made_playoffs",
    },
    "exact": {
        "source_win": "win", "source_loss": "loss", "source_tie": "tie",
        "source_team_points": "team_points", "source_playoffs": "is_playoffs",
        "source_has_po_signal": "has_po_signal", "source_champion": "champion",
        "source_final_playoff_seed": "final_playoff_seed", "source_made_playoffs": "made_playoffs",
        "source_clutch_equity": "clutch_equity",
    },
    "structured": {"source_is_playoffs": "is_playoffs", "source_champion": "champion"},
}


def source_id(path: str) -> str | None:
    match = re.search(r"/candidates/(\d+)/candidates/(\d+)__([^/|]+)$", path)
    if match:
        return match.group(2)
    match = re.search(r"candidates/(\d+)__([^/|]+)$", path)
    return match.group(1) if match else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file-ledger", type=Path, required=True)
    ap.add_argument("--team-delta", type=Path, required=True)
    ap.add_argument("--exact-delta", type=Path, required=True)
    ap.add_argument("--structured-delta", type=Path, required=True)
    ap.add_argument("--cache-comparison", type=Path, required=True)
    ap.add_argument("--materialization-report", type=Path, required=True)
    ap.add_argument("--out-json", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    args = ap.parse_args()

    file_ledger = json.loads(args.file_ledger.read_text(encoding="utf-8"))
    materialization = json.loads(args.materialization_report.read_text(encoding="utf-8"))
    cache_comparison = json.loads(args.cache_comparison.read_text(encoding="utf-8"))
    canonical_columns = set(cache_comparison.get("cache_player_columns", []))
    artifacts: dict[str, dict] = {}
    for row in file_ledger["files"]:
        aid = str(row["artifact_id"])
        entry = artifacts.setdefault(aid, {
            "artifact_id": int(aid), "artifact": row.get("artifact"),
            "workflow_run_id": row.get("workflow_run_id"),
            "dispositions": set(), "manifest_file_count": 0,
            "candidate_file_count": 0, "candidate_files": [],
        })
        entry["manifest_file_count"] += 1
        if row.get("candidate"):
            entry["candidate_file_count"] += 1
            entry["candidate_files"].append(row.get("file"))
        if row.get("disposition"):
            entry["dispositions"].add(row["disposition"])

    con = duckdb.connect()
    evidence: dict[str, dict] = defaultdict(lambda: {"rows": 0, "fields": set(), "files": set()})

    def add_view(kind: str, path: Path, column: str) -> None:
        if not path.exists() or path.stat().st_size == 0:
            return
        view = f"v_{kind}"
        escaped = str(path.resolve()).replace("'", "''")
        con.execute(f"CREATE OR REPLACE TEMP VIEW {view} AS SELECT * FROM read_parquet('{escaped}')")
        columns = {r[0] for r in con.execute(f"DESCRIBE {view}").fetchall()}
        if column not in columns:
            return
        field_expr = [f"COUNT(*) FILTER (WHERE {name} IS NOT NULL) AS \"{name}\"" for name in SOURCE_FIELD_MAP[kind] if name in columns]
        field_sql = ", ".join(field_expr) or "COUNT(*) AS no_fields"
        query = f"""
            SELECT source_path, COUNT(*) AS rows, {field_sql}
            FROM {view}, UNNEST(string_split(CAST({column} AS VARCHAR), '|')) AS u(source_path)
            WHERE source_path IS NOT NULL AND source_path <> ''
            GROUP BY source_path
        """
        for row in con.execute(query).fetchall():
            path_value, count = row[0], int(row[1])
            aid = source_id(str(path_value))
            if aid is None:
                continue
            e = evidence[aid]
            e["rows"] += count
            e["files"].add(str(path_value))
            for index, source_name in enumerate([name for name in SOURCE_FIELD_MAP[kind] if name in columns], start=2):
                if row[index]:
                    e["fields"].add(SOURCE_FIELD_MAP[kind][source_name])

    add_view("team", args.team_delta, "source_files")
    add_view("exact", args.exact_delta, "source_file")
    add_view("structured", args.structured_delta, "source_files")
    con.close()

    readback = materialization.get("readback_remaining_nulls", {})
    readback_clean = all(int(value) == 0 for value in readback.values())
    rows = []
    for aid, entry in sorted(artifacts.items(), key=lambda item: int(item[0])):
        ev = evidence.get(aid, {})
        has_evidence = bool(ev.get("rows"))
        missing_fields = sorted(set(ev.get("fields", set())) - canonical_columns)
        proof = "not_proven"
        if has_evidence and missing_fields:
            proof = "blocked_by_canonical_schema"
        elif has_evidence and readback_clean:
            proof = "working_copy_cell_readback_zero_remaining_nulls"
        rows.append({
            "artifact_id": entry["artifact_id"], "artifact": entry["artifact"],
            "workflow_run_id": entry["workflow_run_id"],
            "manifest_file_count": entry["manifest_file_count"],
            "candidate_file_count": entry["candidate_file_count"],
            "dispositions": sorted(entry["dispositions"]),
            "candidate_files": sorted(set(entry["candidate_files"])),
            "candidate_delta_rows": int(ev.get("rows", 0)),
            "candidate_fields": sorted(ev.get("fields", set())),
            "source_path_count": len(ev.get("files", set())),
            "source_refs_mapped": has_evidence,
            "canonical_fields_missing": missing_fields,
            "cache_readback_proof": proof,
            "canonical_cache_replaced": bool(materialization.get("canonical_cache_replaced", False)),
            "working_copy_only": bool(materialization.get("working_copy_only", False)),
        })

    report = {
        "manifest_artifacts": len(rows), "expected_manifest_artifacts": 9479,
        "all_manifest_artifacts_listed": len(rows) == 9479,
        "source_artifact_ids_mapped": len(evidence),
        "source_artifact_ids_unmapped": sorted(set(evidence) - set(artifacts)),
        "working_copy_readback_clean": readback_clean,
        "canonical_player_columns": sorted(canonical_columns),
        "source_fields_blocked_by_canonical_schema": sorted({field for row in rows for field in row["canonical_fields_missing"]}),
        "canonical_cache_replaced": bool(materialization.get("canonical_cache_replaced", False)),
        "new_lineage": bool(materialization.get("new_lineage", False)),
        "schema_unchanged": bool(materialization.get("schema_unchanged", False)),
        "ops_unchanged": bool(materialization.get("ops_unchanged", False)),
        "rows": rows,
    }
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    fields = ["artifact_id", "artifact", "workflow_run_id", "manifest_file_count", "candidate_file_count", "dispositions", "candidate_files", "candidate_delta_rows", "candidate_fields", "canonical_fields_missing", "source_path_count", "source_refs_mapped", "cache_readback_proof", "canonical_cache_replaced", "working_copy_only"]
    with args.out_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(row[key], separators=(",", ":")) if isinstance(row[key], (list, dict)) else row[key] for key in fields})
    print(json.dumps({key: report[key] for key in ("manifest_artifacts", "source_artifact_ids_mapped", "source_artifact_ids_unmapped", "working_copy_readback_clean", "canonical_cache_replaced", "new_lineage", "schema_unchanged", "ops_unchanged")}, sort_keys=True))


if __name__ == "__main__":
    main()
