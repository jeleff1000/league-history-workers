"""Build one disposition/evidence row for every file in the frozen artifact manifest.

This is deliberately a ledger, not a summary.  A source file is not considered
complete merely because its parent artifact downloaded: candidate files must
have an extraction record and a comparison/apply disposition.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--extract-ledger", type=Path, nargs="+", required=True)
    ap.add_argument("--comparison", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    manifest = read_json(args.manifest)
    files = manifest.get("files", [])
    # Preserve every manifest file row.  Repeated basenames occur in several
    # non-candidate aggregate artifacts; only candidate rows participate in
    # extraction-key uniqueness checks.
    candidate_rows = [r for r in files if r.get("candidate")]
    expected = {(int(r["artifact_id"]), str(r["file"])): r for r in candidate_rows}
    if len(expected) != len(candidate_rows):
        raise SystemExit("manifest contains duplicate candidate artifact/file keys")
    evidence: dict[tuple[int, str], list[dict]] = {}
    candidate_keys = set(expected)
    for path in args.extract_ledger:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            key = (int(row["artifact_id"]), str(row["file"]))
            # Parent extraction ledgers include non-candidate files too.  Do
            # not let their repeated aggregate basenames contaminate the
            # candidate promotion ledger.
            if key not in candidate_keys:
                continue
            evidence.setdefault(key, []).append(row)
    status_priority = {
        "candidate_extracted": 3,
        "structured_evidence_extracted": 3,
        "archive_download_failed": 1,
        "candidate_file_missing_or_ambiguous": 0,
    }
    extracted = {
        key: max(rows, key=lambda row: status_priority.get(row.get("status"), 0))
        for key, rows in evidence.items()
    }
    duplicate_extracts = {key for key, rows in evidence.items() if len(rows) > 1}

    exact_sources = set()
    exact_list = args.comparison / "exact_source_files.txt"
    if exact_list.exists():
        exact_sources = {Path(x.strip()).name for x in exact_list.read_text().splitlines() if x.strip()}

    structured = {}
    structured_path = args.comparison / "structured_evidence_audit.json"
    if structured_path.exists():
        for row in read_json(structured_path).get("records", []):
            structured[Path(str(row.get("file", ""))).name] = row

    rows = []
    for expected_row in files:
        key = (int(expected_row["artifact_id"]), str(expected_row["file"]))
        artifact_id, file_name = key
        row = dict(expected_row)
        row["artifact_file_key"] = f"{artifact_id}:{file_name}"
        row["extraction_status"] = None
        row["comparison_status"] = None
        row["promotion_status"] = "not_applicable"
        row["promotion_reason"] = str(expected_row.get("reason", ""))

        if not expected_row.get("candidate"):
            rows.append(row)
            continue

        extraction = extracted.get(key)
        if extraction is None:
            row["promotion_status"] = "missing_extraction_evidence"
            row["promotion_reason"] = "Candidate file has no extraction ledger row."
            rows.append(row)
            continue

        row["extraction_status"] = extraction.get("status")
        if key in duplicate_extracts:
            row["promotion_status"] = "duplicate_extraction_evidence"
            row["promotion_reason"] = "More than one shard emitted this artifact/file key."
            rows.append(row)
            continue
        if extraction.get("status") not in {"candidate_extracted", "structured_evidence_extracted"}:
            row["promotion_status"] = "extraction_failed"
            row["promotion_reason"] = extraction.get("error") or extraction.get("status", "unknown extraction status")
            rows.append(row)
            continue

        disposition = expected_row.get("disposition")
        extracted_name = Path(str(extraction.get("extracted_file", file_name))).name
        if disposition == "direct_player_candidate":
            if extracted_name in exact_sources or str(extraction.get("extracted_file", "")) in exact_sources:
                row["comparison_status"] = "exact_player_compared"
                row["promotion_status"] = "comparison_complete_pending_apply_audit"
                row["promotion_reason"] = "Exact player source was included in the canonical-key comparison."
            else:
                row["promotion_status"] = "extracted_not_in_comparison"
                row["promotion_reason"] = "Exact player candidate was extracted but is absent from exact_source_files.txt."
        elif disposition == "team_week_signal_candidate":
            row["comparison_status"] = "team_signal_compared"
            row["promotion_status"] = "comparison_complete_pending_apply_audit"
            row["promotion_reason"] = "Team/week source was included in team-key fan-out audit."
        elif disposition == "player_signal_candidate":
            row["promotion_status"] = "extracted_signal_pending_join"
            row["promotion_reason"] = "Player signal was extracted; a player-grain join/apply report is required."
        elif disposition == "settings_candidate":
            row["promotion_status"] = "extracted_settings_pending_join"
            row["promotion_reason"] = "Settings evidence was extracted; league-year fan-out/apply report is required."
        elif disposition == "structured_evidence_candidate":
            s = structured.get(Path(file_name).name)
            row["comparison_status"] = "structured_audit_complete" if s is not None else "structured_audit_missing"
            row["promotion_status"] = "structured_evidence_audited" if s is not None else "structured_evidence_missing"
            row["promotion_reason"] = "Structured evidence requires field-specific normalization before promotion."
        else:
            row["promotion_status"] = "extracted_without_known_promotion_route"
            row["promotion_reason"] = "Candidate disposition has no registered promotion route."
        rows.append(row)

    summary = {
        "manifest_files": len(files),
        "extraction_rows": len(extracted),
        "duplicate_extraction_keys": len(duplicate_extracts),
        "status_counts": dict(sorted(Counter(r["promotion_status"] for r in rows).items())),
        "candidate_files": sum(bool(r.get("candidate")) for r in rows),
        "candidate_files_with_terminal_evidence": sum(
            bool(r.get("candidate")) and r["promotion_status"] not in {
                "missing_extraction_evidence", "duplicate_extraction_evidence", "extraction_failed",
                "extracted_not_in_comparison", "extracted_signal_pending_join", "extracted_settings_pending_join",
                "structured_evidence_missing", "extracted_without_known_promotion_route",
            } for r in rows
        ),
    }
    result = {"summary": summary, "files": rows}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
