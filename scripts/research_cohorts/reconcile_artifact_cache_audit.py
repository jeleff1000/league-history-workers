"""Reconcile the complete artifact inventory with extracted candidates and cache audit."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


EXPECTED_ARTIFACTS = 9_479
EXPECTED_FILES = 24_982


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--extract-ledger", type=Path, nargs="+", required=True)
    ap.add_argument("--comparison", type=Path, required=True)
    ap.add_argument("--structured-audit", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    manifest = read_json(args.manifest)
    files = manifest.get("files", [])
    if int(manifest.get("artifact_count", 0)) != EXPECTED_ARTIFACTS:
        raise SystemExit("manifest artifact count is not 9,479")
    if int(manifest.get("file_count", 0)) != EXPECTED_FILES or len(files) != EXPECTED_FILES:
        raise SystemExit("manifest file count is not 24,982")

    def key(row: dict) -> tuple[int, str]:
        return int(row["artifact_id"]), str(row["file"])

    # The manifest is file-level, and some non-candidate aggregate artifacts
    # legitimately contain repeated basenames inside one artifact.  Candidate
    # files are required to have unique keys because they are the files that
    # must be extracted and compared.  Do not collapse non-candidate rows: the
    # final ledger must retain one disposition for every manifest file record.
    candidate_files = [row for row in files if row.get("candidate")]
    expected = {key(row): row for row in candidate_files}
    if len(expected) != len(candidate_files):
        raise SystemExit("manifest contains duplicate candidate artifact/file keys")
    extracted = []
    for path in args.extract_ledger:
        extracted.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    by_key = {}
    duplicate_keys = set()
    for row in extracted:
        k = key(row)
        if k in by_key:
            duplicate_keys.add(k)
        else:
            by_key[k] = row
    selected = set(expected)
    actual = set(by_key)
    missing_selected = sorted(selected - actual)
    out_of_scope = sorted(actual - set(expected))
    extraction_failures = sorted(k for k in selected if by_key.get(k, {}).get("status") not in {"candidate_extracted", "structured_evidence_extracted"})
    comparison = read_json(args.comparison)
    structured = read_json(args.structured_audit)
    exact_report_path = args.comparison.parent / "exact_player_delta_report.json"
    exact_report = read_json(exact_report_path) if exact_report_path.exists() else {}
    cache_invariants = {
        "cache_mutated": bool(comparison.get("cache_mutated")),
        "new_lineage": bool(comparison.get("new_lineage")),
        "canonical_schema_unchanged": bool(comparison.get("canonical_schema_unchanged")),
    }
    complete = not missing_selected and not duplicate_keys and not out_of_scope and not extraction_failures
    unresolved_structured_payloads = int(structured.get("promotable_structured_rows", 0)) + int(structured.get("settings_rows", 0))
    unresolved_exact_rows = int(exact_report.get("unresolved_unmatched_rows", 0))
    promotion_ready = complete and unresolved_structured_payloads == 0 and unresolved_exact_rows == 0 and not cache_invariants["cache_mutated"] and not cache_invariants["new_lineage"] and cache_invariants["canonical_schema_unchanged"]
    report = {
        "complete_artifact_audit_run": manifest.get("complete_artifact_audit_run"),
        "manifest_artifacts": EXPECTED_ARTIFACTS, "manifest_files": EXPECTED_FILES,
        "selected_candidate_files": len(selected), "extracted_candidate_files": len(actual & selected),
        "missing_candidate_files": len(missing_selected), "duplicate_extracted_keys": len(duplicate_keys),
        "out_of_scope_extracted_keys": len(out_of_scope), "extraction_failures": len(extraction_failures),
        "disposition_counts": dict(sorted(Counter(row.get("disposition", "unknown") for row in files).items())),
        "extraction_status_counts": dict(sorted(Counter(row.get("status", "missing") for row in by_key.values()).items())),
        "cache_invariants": cache_invariants, "complete_file_reconciliation": complete,
        "promotion_ready": promotion_ready,
        "exact_delta": {
            "matched_rows": exact_report.get("matched_rows", 0),
            "delta_rows": exact_report.get("delta_rows", 0),
            "insert_candidate_rows": exact_report.get("insert_candidate_rows", 0),
            "unresolved_unmatched_rows": unresolved_exact_rows,
            "insertion_schema_complete": exact_report.get("insertion_schema_complete", False),
        },
        "structured_audit": {k: structured.get(k) for k in ("files", "direct_signal_files", "settings_payload_files", "promotable_structured_rows", "settings_rows")},
        "unresolved_structured_payloads": unresolved_structured_payloads,
        "missing_candidate_keys": [list(k) for k in missing_selected[:100]],
        "duplicate_extracted_keys_sample": [list(k) for k in sorted(duplicate_keys)[:100]],
        "out_of_scope_extracted_keys_sample": [list(k) for k in sorted(out_of_scope)[:100]],
        "extraction_failure_keys_sample": [list(k) for k in extraction_failures[:100]],
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    if not complete:
        raise SystemExit("artifact/file reconciliation incomplete; promotion is forbidden")


if __name__ == "__main__":
    main()
