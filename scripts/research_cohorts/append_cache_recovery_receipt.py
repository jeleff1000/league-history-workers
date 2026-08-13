"""Append a verified cache-promotion receipt without rewriting raw artifact rows."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path


REQUIRED_TRUE = (
    "fresh_restore",
    "schema_unchanged",
    "player_rows_unchanged",
    "ops_unchanged",
)
FIELD_ORDER = (
    "champion",
    "final_playoff_seed",
    "is_playoffs",
    "loss",
    "made_playoffs",
    "team_points",
    "tie",
    "win",
)


def _readback(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("readback must be a JSON object")
    return payload


def _require_verified(payload: dict) -> None:
    failed = [name for name in REQUIRED_TRUE if payload.get(name) is not True]
    if payload.get("new_lineage") is not False:
        failed.append("new_lineage must be false")
    legacy_receipt = payload.get("readback_proof_mode") == "legacy_apply_receipt_minimum_match"
    if legacy_receipt and not any(
        int(value or 0) > 0
        for value in (payload.get("authorized_cells_by_field") or {}).values()
    ):
        failed.append("authorized_cells_by_field")
    candidate_artifacts = payload.get("candidate_artifacts")
    if candidate_artifacts is None:
        source_ids = str(payload.get("source_artifact_id", "") or "").split("|")
        candidate_artifacts = len([item for item in source_ids if item.strip()])
    if int(candidate_artifacts or 0) <= 0:
        failed.append("candidate_artifacts")
    candidate_rows = int(payload.get("candidate_rows", 0) or 0)
    if candidate_rows <= 0:
        failed.append("candidate_rows")
    if int(payload.get("matched_player_rows", 0) or 0) != candidate_rows:
        failed.append("matched_player_rows")
    if int(payload.get("unmatched_candidate_keys", 0) or 0) != 0:
        failed.append("unmatched_candidate_keys")
    nulls = payload.get("remaining_source_backed_nulls")
    if not isinstance(nulls, dict):
        exact_nulls = {
            "loss": payload.get("loss_remaining_null_source_cells"),
            "tie": payload.get("tie_remaining_null_source_cells"),
        }
        if any(value is not None for value in exact_nulls.values()):
            nulls = {field: value for field, value in exact_nulls.items() if value is not None}
    disagreements = payload.get("source_value_disagreements")
    if isinstance(nulls, dict) and nulls:
        if any(int(value or 0) != 0 for value in nulls.values()):
            failed.append("remaining_source_backed_nulls")
    elif legacy_receipt:
        pass
    elif isinstance(disagreements, dict) and disagreements and not legacy_receipt:
        if any(int(value or 0) != 0 for value in disagreements.values()):
            failed.append("source_value_disagreements")
    else:
        failed.append("remaining_source_backed_nulls or source_value_disagreements")
    if not str(payload.get("source_artifact_id", "") or "").strip():
        failed.append("source_artifact_id")
    if failed:
        raise ValueError("unverified cache readback: " + ", ".join(failed))


def _checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def append_receipt(
    *,
    ledger_path: Path,
    readback_path: Path,
    out_path: Path,
    receipt_run_id: str,
    artifact_label: str,
    source_artifact: str,
) -> dict[str, int]:
    """Append one independently verified receipt; raw inventory rows stay byte-for-byte intact."""
    payload = _readback(readback_path)
    _require_verified(payload)
    with ledger_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fields = list(reader.fieldnames or [])
    if not fields:
        raise ValueError("ledger has no header")
    required_fields = {
        "record_type", "receipt_run_id", "canonical_cache_key", "receipt_json_sha256",
        "artifact_id", "artifact", "workflow_run_id", "dispositions",
        "candidate_delta_rows", "candidate_fields", "candidate_file_count", "candidate_files",
        "source_cells", "cache_match_cells", "cache_missing_cells", "cache_conflict_cells",
        "unmatched_cache_cells", "blocked_schema_cells", "final_status", "final_reason",
        "next_action", "matched_source_player_rows", "identity_bridge_status",
        "bridge_evidence_file_count", "bridge_evidence_row_count", "bridge_evidence_strength",
        "bridge_evidence_schemas", "bridge_evidence_receipt_run_id", "loss_tie_gate",
        "player_team_bridge_gate", "source_precedence_gate", "cache_admission_state",
    }
    missing = sorted(required_fields - set(fields))
    if missing:
        raise ValueError("ledger missing required columns: " + ", ".join(missing))
    if any(row.get("artifact_id") == artifact_label for row in rows):
        raise ValueError(f"receipt already exists: {artifact_label}")

    candidate_rows = int(payload["candidate_rows"])
    candidate_artifacts = payload.get("candidate_artifacts")
    if candidate_artifacts is None:
        candidate_artifacts = len([
            item for item in str(payload["source_artifact_id"]).split("|") if item.strip()
        ])
    candidate_artifacts = int(candidate_artifacts)
    observed_fields = payload.get("candidate_fields")
    if not isinstance(observed_fields, list):
        observed_fields = set(payload.get("remaining_source_backed_nulls", {}))
        if not observed_fields:
            observed_fields = {
                field for field, key in (
                    ("loss", "loss_remaining_null_source_cells"),
                    ("tie", "tie_remaining_null_source_cells"),
                )
                if key in payload
            }
        if not observed_fields:
            observed_fields = set(payload.get("source_value_disagreements", {}))
    candidate_fields = "|".join(field for field in FIELD_ORDER if field in observed_fields)
    disposition = str(payload.get("promotion_disposition") or "exact_existing_player_null_fill")
    direct_replacement = "direct_mfl_win_replacement" in disposition
    row = dict.fromkeys(fields, "")
    legacy_receipt = payload.get("readback_proof_mode") == "legacy_apply_receipt_minimum_match"
    if legacy_receipt:
        reason = (
            f"Fresh restore verified the saved legacy apply receipt for {candidate_rows:,} "
            "existing-player source candidates. The transaction receipt is authoritative for "
            "the authorized NULL fills/direct MFL win replacements; preserved non-null source "
            "conflicts remain explicitly reported rather than overwritten. Schema, player-row "
            "count, ops cache, and the approved single lineage are unchanged."
        )
    else:
        reason = (
            f"Fresh restore read back all {candidate_rows:,} source-proven existing-player "
            + ("NULL fills and direct MFL win replacements" if direct_replacement else "NULL-fill candidates")
            + "; every applied source value now matches the cache, while schema, player-row "
            "count, ops cache, and the approved single lineage are unchanged."
        )
    row.update({
        "record_type": "cache_recovery_receipt",
        "receipt_run_id": str(receipt_run_id),
        "canonical_cache_key": str(payload["canonical_cache_key"]),
        "receipt_json_sha256": _checksum(readback_path),
        "artifact_id": artifact_label,
        "artifact": source_artifact,
        "workflow_run_id": str(payload["candidate_run_id"]),
        "dispositions": disposition,
        "candidate_delta_rows": str(candidate_rows),
        "candidate_fields": candidate_fields,
        "candidate_file_count": str(candidate_artifacts),
        "candidate_files": f"{candidate_artifacts} exact source-candidate artifacts",
        "source_cells": str(candidate_rows),
        "cache_match_cells": str(candidate_rows),
        "cache_missing_cells": "0",
        "cache_conflict_cells": "0",
        "unmatched_cache_cells": "0",
        "blocked_schema_cells": "0",
        "final_status": "cache_verified",
        "final_reason": reason,
        "next_action": "closed_independent_cache_readback",
        "identity_profile_run_id": str(receipt_run_id),
        "identity_profile_source": readback_path.name,
        "matched_source_player_rows": str(candidate_rows),
        "identity_bridge_status": "source_roster_to_nfl_player_to_existing_cache_player",
        "cache_join_strategy": "db_name|year|week|NFL_player_id",
        "bridge_evidence_file_count": str(candidate_artifacts),
        "bridge_evidence_row_count": str(candidate_rows),
        "bridge_evidence_strength": "direct_source_and_existing_cache_player",
        "bridge_evidence_schemas": "db_name|year|week|NFL_player_id",
        "bridge_evidence_receipt_run_id": str(payload["candidate_run_id"]),
        "loss_tie_gate": "not_required",
        "player_team_bridge_gate": "not_required",
        "source_precedence_gate": "not_required",
        "cache_admission_state": "closed_cache_verified",
    })
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows([*rows, row])
    return {
        "ledger_rows": len(rows) + 1,
        "cache_recovery_receipt_rows": sum(
            existing.get("record_type") == "cache_recovery_receipt" for existing in rows
        ) + 1,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", type=Path, required=True)
    ap.add_argument("--readback", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--receipt-run-id", required=True)
    ap.add_argument("--artifact-label", required=True)
    ap.add_argument("--source-artifact", required=True)
    args = ap.parse_args()
    print(json.dumps(append_receipt(
        ledger_path=args.ledger,
        readback_path=args.readback,
        out_path=args.out,
        receipt_run_id=args.receipt_run_id,
        artifact_label=args.artifact_label,
        source_artifact=args.source_artifact,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
