"""Merge exact-scope artifact-audit reports without touching any cache."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--report", type=Path, nargs="+", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--checklist", type=Path, required=True)
    args = ap.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    ids = [int(item["artifact_id"]) for item in manifest["artifacts"]]
    if len(ids) != 9479 or len(set(ids)) != len(ids):
        raise SystemExit(f"exact manifest invalid: count={len(ids)} unique={len(set(ids))}")
    expected = set(ids)
    records = []
    for path in args.report:
        records.extend(json.loads(path.read_text(encoding="utf-8")).get("records", []))
    by_id = {}
    duplicate = set()
    for record in records:
        aid = int(record["artifact_id"])
        if aid in by_id:
            duplicate.add(aid)
        else:
            by_id[aid] = record
    missing = sorted(expected - set(by_id))
    out_of_scope = sorted(set(by_id) - expected)
    errors = sorted(
        (record for record in by_id.values() if record.get("status") != "inspected"),
        key=lambda record: int(record["artifact_id"]),
    )
    report = {
        "manifest_artifacts": len(expected),
        "inspected_records": sum(record.get("status") == "inspected" for record in by_id.values()),
        "inspection_errors": len(errors),
        "duplicate_artifact_ids": sorted(duplicate),
        "missing_artifact_ids": missing,
        "out_of_scope_artifact_ids": out_of_scope,
        "complete": len(by_id) == len(expected) and not duplicate and not missing and not out_of_scope and not errors,
        "records": sorted(by_id.values(), key=lambda record: int(record["artifact_id"])),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    fields = ["artifact_id", "artifact", "workflow_run_id", "created_at", "artifact_status", "file", "file_type", "kind", "rows", "column_count", "columns", "missing_canonical", "forbidden_columns"]
    args.checklist.parent.mkdir(parents=True, exist_ok=True)
    with args.checklist.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for record in report["records"]:
            for file_record in record.get("files") or [{}]:
                writer.writerow({
                    "artifact_id": record.get("artifact_id"), "artifact": record.get("artifact"),
                    "workflow_run_id": record.get("workflow_run_id"), "created_at": record.get("created_at"),
                    "artifact_status": record.get("status"), "file": file_record.get("file"),
                    "file_type": file_record.get("type"), "kind": file_record.get("kind"),
                    "rows": file_record.get("rows"), "column_count": file_record.get("column_count"),
                    "columns": json.dumps(file_record.get("columns", []), separators=(",", ":")),
                    "missing_canonical": json.dumps(file_record.get("missing_canonical", []), separators=(",", ":")),
                    "forbidden_columns": json.dumps(file_record.get("forbidden_columns", []), separators=(",", ":")),
                })
    print(json.dumps({key: report[key] for key in ("manifest_artifacts", "inspected_records", "inspection_errors", "complete")}, sort_keys=True))
    if not report["complete"]:
        raise SystemExit("exact artifact audit incomplete; no cache action is permitted")


if __name__ == "__main__":
    main()
