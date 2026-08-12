"""Fail fast when the durable artifact ledger contradicts its evidence."""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path


CLOSED_STATUSES = {
    "cache_verified",
    "no_promotable_candidate_emitted",
    "not_data_bearing",
}
REQUIRED_FIELDS = (
    "artifact_id",
    "artifact",
    "workflow_run_id",
    "canonical_cache_key",
    "final_status",
    "final_reason",
    "next_action",
)


def _number(row: dict[str, str], field: str) -> int:
    return int(str(row.get(field, "") or "0"))


def _issue(artifact_id: str, issue: str) -> dict[str, str]:
    return {"artifact_id": str(artifact_id), "issue": issue}


def validate_rows(rows: list[dict[str, str]]) -> dict:
    """Validate every disposition against its receipt and quantified evidence."""
    issues: list[dict[str, str]] = []
    for row in rows:
        artifact_id = str(row.get("artifact_id", ""))
        for field in REQUIRED_FIELDS:
            if not str(row.get(field, "") or "").strip():
                issues.append(_issue(artifact_id, f"missing_{field}"))

        status = str(row.get("final_status", "") or "")
        reason = str(row.get("final_reason", "") or "")
        if status != "not_data_bearing":
            for field in ("receipt_run_id", "receipt_json_sha256"):
                if not str(row.get(field, "") or "").strip():
                    issues.append(_issue(artifact_id, f"missing_{field}"))

        gap_total = sum(
            _number(row, field)
            for field in (
                "cache_missing_cells",
                "cache_conflict_cells",
                "unmatched_cache_cells",
                "blocked_schema_cells",
            )
        )
        if status == "cache_verified" and gap_total:
            issues.append(_issue(artifact_id, "cache_verified_has_nonzero_gap"))
        if status == "not_data_bearing" and _number(row, "candidate_delta_rows"):
            issues.append(_issue(artifact_id, "not_data_bearing_has_candidate"))
        if status == "no_promotable_candidate_emitted":
            has_historical_probe_only = (
                "no valid NFL-player fill" in reason
                and "NFL_player_id null" in reason
            )
            if _number(row, "candidate_delta_rows") and not has_historical_probe_only:
                issues.append(_issue(artifact_id, "no_candidate_has_delta"))
        if status not in CLOSED_STATUSES:
            if gap_total == 0:
                issues.append(_issue(artifact_id, "open_row_has_no_quantified_gap"))
        if _number(row, "cache_conflict_cells") and "precedence" not in str(
            row.get("next_action", "") or ""
        ).lower():
            issues.append(_issue(artifact_id, "conflict_missing_precedence_action"))

        # A bridge claim is evidence, not a vague note.  If an artifact says
        # it has player-to-team evidence, retain enough receipt information to
        # reproduce that claim before it can ever be used for a fan-out.
        bridge_files = _number(row, "bridge_evidence_file_count")
        if bridge_files:
            if _number(row, "bridge_evidence_row_count") <= 0:
                issues.append(_issue(artifact_id, "bridge_evidence_has_no_rows"))
            if not str(row.get("bridge_evidence_strength", "") or "").strip():
                issues.append(_issue(artifact_id, "bridge_evidence_missing_strength"))
            if not str(row.get("bridge_evidence_receipt_run_id", "") or "").strip():
                issues.append(_issue(artifact_id, "bridge_evidence_missing_receipt"))

    return {
        "ok": not issues,
        "ledger_rows": len(rows),
        "status_counts": dict(sorted(Counter(row.get("final_status", "") for row in rows).items())),
        "issues": issues,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", type=Path, required=True)
    args = ap.parse_args()
    with args.ledger.open(newline="", encoding="utf-8-sig") as handle:
        result = validate_rows(list(csv.DictReader(handle)))
    print(json.dumps(result, indent=2, sort_keys=True))
    if not result["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
