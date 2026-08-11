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
SETTINGS_FIELD_MAP = {
    "source_scoring_pass_td": "scoring_pass_td",
    "source_playoff_teams": "playoff_teams",
    "source_roster_FLX": "roster_FLX",
    "source_roster_SUPER_FLEX": "roster_SUPER_FLEX",
    "source_roster_IDP": "roster_IDP",
    "source_sleeper_best_ball": "sleeper_best_ball",
}


def source_id(path: str) -> str | None:
    match = re.search(r"settings-candidate-shard-\d+-\d+/(\d+)/[^/|]+$", path)
    if match:
        return match.group(1)
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
    ap.add_argument("--settings-delta", type=Path, required=False)
    ap.add_argument("--settings-materialization-report", type=Path, required=False)
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
            "settings_source_reference_rows": 0, "settings_fields": set(), "settings_source_paths": set(),
        })
        entry["manifest_file_count"] += 1
        if row.get("candidate"):
            entry["candidate_file_count"] += 1
            entry["candidate_files"].append(row.get("file"))
        if row.get("disposition"):
            entry["dispositions"].add(row["disposition"])

    con = duckdb.connect()
    evidence: dict[str, dict] = defaultdict(lambda: {"rows": 0, "fields": set(), "files": set()})
    settings_delta_input_rows = 0

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
    if args.settings_delta and args.settings_delta.exists() and args.settings_delta.stat().st_size > 0:
        path = str(args.settings_delta.resolve()).replace("'", "''")
        con.execute(f"CREATE OR REPLACE TEMP VIEW settings_delta AS SELECT * FROM read_parquet('{path}')")
        settings_delta_input_rows = int(con.execute("SELECT COUNT(*) FROM settings_delta").fetchone()[0])
        cols = {r[0] for r in con.execute("DESCRIBE settings_delta").fetchall()}
        source_names = [name for name in SETTINGS_FIELD_MAP if name in cols]
        if "source_files" not in cols:
            raise SystemExit("settings delta missing source_files provenance")
        expressions = ", ".join(f'COUNT(*) FILTER (WHERE "{name}" IS NOT NULL)' for name in source_names) or "COUNT(*)"
        for source_path, count, *values in con.execute(f"""
            SELECT source_path, COUNT(*), {expressions}
            FROM settings_delta, UNNEST(string_split(CAST(source_files AS VARCHAR), '|')) AS u(source_path)
            WHERE source_path IS NOT NULL AND source_path <> '' GROUP BY source_path
        """).fetchall():
            aid = source_id(str(source_path))
            if aid is None:
                continue
            entry = artifacts.setdefault(aid, {
                "artifact_id": int(aid), "artifact": None, "workflow_run_id": None,
                "dispositions": set(), "manifest_file_count": 0, "candidate_file_count": 0,
                "candidate_files": [], "settings_source_reference_rows": 0, "settings_fields": set(),
                "settings_source_paths": set(),
            })
            entry["settings_source_reference_rows"] += int(count)
            entry["settings_source_paths"].add(str(source_path))
            for index, name in enumerate(source_names):
                if values[index]:
                    entry["settings_fields"].add(SETTINGS_FIELD_MAP[name])
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
        settings_has_evidence = int(entry.get("settings_source_reference_rows", 0)) > 0
        settings_proof = "not_supplied"
        if settings_has_evidence:
            settings_proof = "not_proven"
        rows.append({
            "artifact_id": entry["artifact_id"], "artifact": entry["artifact"],
            "workflow_run_id": entry["workflow_run_id"],
            "manifest_file_count": entry["manifest_file_count"],
            "candidate_file_count": entry["candidate_file_count"],
            "dispositions": sorted(entry["dispositions"]),
            "candidate_files": sorted(set(entry["candidate_files"])),
            "candidate_delta_rows": int(ev.get("rows", 0)),
            "candidate_fields": sorted(ev.get("fields", set())),
            "settings_source_reference_rows": int(entry.get("settings_source_reference_rows", 0)),
            "settings_fields": sorted(entry.get("settings_fields", set())),
            "settings_source_path_count": len(entry.get("settings_source_paths", set())),
            "source_path_count": len(ev.get("files", set())),
            "source_refs_mapped": has_evidence,
            "canonical_fields_missing": missing_fields,
            "cache_readback_proof": proof,
            "canonical_cache_replaced": bool(materialization.get("canonical_cache_replaced", False)),
            "working_copy_only": bool(materialization.get("working_copy_only", False)),
            "settings_cache_readback_proof": settings_proof,
        })

    if args.settings_materialization_report and args.settings_materialization_report.exists():
        settings_report = json.loads(args.settings_materialization_report.read_text(encoding="utf-8"))
        settings_clean = all(int(value) == 0 for value in settings_report.get("readback_remaining_nulls", {}).values())
        for row in rows:
            if row["settings_source_reference_rows"]:
                row["settings_cache_readback_proof"] = "working_copy_settings_cell_readback_zero_remaining_nulls" if settings_clean else "not_proven"

    source_ids_seen = set(evidence)
    source_ids_seen.update(
        aid for aid, entry in artifacts.items() if int(entry.get("settings_source_reference_rows", 0)) > 0
    )
    mapped_ids = {
        aid for aid, entry in artifacts.items()
        if evidence.get(aid, {}).get("rows") or int(entry.get("settings_source_reference_rows", 0)) > 0
    }
    settings_rows_with_proof = sum(
        int(row["settings_source_reference_rows"])
        for row in rows
        if row["settings_cache_readback_proof"] == "working_copy_settings_cell_readback_zero_remaining_nulls"
    )

    report = {
        "manifest_artifacts": len(rows), "expected_manifest_artifacts": 9479,
        "all_manifest_artifacts_listed": len(rows) == 9479,
        "source_artifact_ids_mapped": len(mapped_ids),
        "source_artifact_ids_unmapped": sorted(source_ids_seen - set(artifacts)),
        "working_copy_readback_clean": readback_clean,
        "canonical_player_columns": sorted(canonical_columns),
        "source_fields_blocked_by_canonical_schema": sorted({field for row in rows for field in row["canonical_fields_missing"]}),
        "canonical_cache_replaced": bool(materialization.get("canonical_cache_replaced", False)),
        "new_lineage": bool(materialization.get("new_lineage", False)),
        "schema_unchanged": bool(materialization.get("schema_unchanged", False)),
        "ops_unchanged": bool(materialization.get("ops_unchanged", False)),
        "rows": rows,
        "settings_delta_input_rows": settings_delta_input_rows,
        "settings_source_reference_rows": sum(int(row["settings_source_reference_rows"]) for row in rows),
        "settings_source_reference_rows_with_cache_readback_proof": settings_rows_with_proof,
        "settings_readback_proof_clean": all(row["settings_cache_readback_proof"] in {"not_supplied", "working_copy_settings_cell_readback_zero_remaining_nulls"} for row in rows),
    }
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    fields = ["artifact_id", "artifact", "workflow_run_id", "manifest_file_count", "candidate_file_count", "dispositions", "candidate_files", "candidate_delta_rows", "candidate_fields", "canonical_fields_missing", "source_path_count", "source_refs_mapped", "cache_readback_proof", "settings_source_reference_rows", "settings_fields", "settings_source_path_count", "settings_cache_readback_proof", "canonical_cache_replaced", "working_copy_only"]
    with args.out_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(row[key], separators=(",", ":")) if isinstance(row[key], (list, dict)) else row[key] for key in fields})
    print(json.dumps({key: report[key] for key in ("manifest_artifacts", "source_artifact_ids_mapped", "source_artifact_ids_unmapped", "working_copy_readback_clean", "canonical_cache_replaced", "new_lineage", "schema_unchanged", "ops_unchanged")}, sort_keys=True))


if __name__ == "__main__":
    main()
