"""Append post-inventory MFL artifact dispositions to the canonical ledger."""
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path


FIELD_BY_MANIFEST_COLUMN = {
    "win_cells": "win",
    "loss_cells": "loss",
    "tie_cells": "tie",
    "team_points_cells": "team_points",
    "playoff_cells": "is_playoffs",
}
SHARD_RE = re.compile(r"-shard-(\d+)-")


def _number(row: dict[str, str], name: str) -> int:
    try:
        return int(row.get(name, "") or "0")
    except ValueError as exc:
        raise ValueError(f"invalid {name}: {row.get(name)!r}") from exc


def _shard_index(row: dict[str, str]) -> str:
    index = row.get("artifact_index", "").strip()
    if index.isdigit():
        return index
    match = SHARD_RE.search(row.get("artifact", ""))
    if match:
        return match.group(1)
    raise ValueError("excluded no-candidate record requires a shard index")


def _artifact_row(
    manifest_row: dict[str, str],
    fields: list[str],
    *,
    canonical_cache_key: str,
    receipt_sha256: str,
) -> dict[str, str]:
    state = manifest_row["admission_state"]
    candidates = _number(manifest_row, "candidate_rows")
    source_cells = sum(_number(manifest_row, name) for name in FIELD_BY_MANIFEST_COLUMN)
    candidate_fields = "|".join(
        field
        for source_column, field in FIELD_BY_MANIFEST_COLUMN.items()
        if _number(manifest_row, source_column) > 0
    )
    artifact_id = manifest_row["artifact_id"].strip()
    if not artifact_id:
        if state != "explicitly_excluded_no_candidate":
            raise ValueError(f"{state} requires a GitHub artifact_id")
        index = _shard_index(manifest_row)
        # This is deliberately not a fabricated GitHub artifact ID. The source
        # run emitted no artifact, so the ledger uses an explicit logical
        # receipt key to preserve the missing-output fact exactly once.
        artifact_id = (
            f"no-github-artifact:run-{manifest_row['source_run_id']}:shard-{index}"
        )
    row = dict.fromkeys(fields, "")
    row.update({
        "record_type": "artifact_admission",
        "receipt_run_id": manifest_row["receipt_run_id"],
        "canonical_cache_key": canonical_cache_key,
        "receipt_json_sha256": receipt_sha256,
        "artifact_id": artifact_id,
        "artifact": manifest_row["artifact"],
        "workflow_run_id": manifest_row["source_run_id"],
        "candidate_delta_rows": str(candidates),
        "candidate_fields": candidate_fields,
        "candidate_file_count": "1",
        "candidate_files": manifest_row["artifact"],
        "source_cells": str(source_cells),
        "cache_missing_cells": "0",
        "cache_conflict_cells": "0",
        "unmatched_cache_cells": "0",
        "blocked_schema_cells": "0",
        "identity_profile_run_id": manifest_row["receipt_run_id"],
        "identity_profile_source": "research_matchup_mfl_batch_31663728938_admission_manifest.csv",
        "loss_tie_gate": "not_required",
        "player_team_bridge_gate": "not_required",
        "source_precedence_gate": "not_required",
    })
    if state == "included_in_verified_cache_batch":
        row.update({
            "dispositions": "batch_member_legacy_cache_readback",
            "cache_match_cells": str(candidates),
            "final_status": "cache_verified",
            "final_reason": (
                f"This artifact emitted {candidates:,} candidate player rows and is included in "
                f"the saved same-key apply evidence and independent receipt {manifest_row['receipt_run_id']}. "
                "The receipt is batch-level legacy readback: it proves authorized aggregate changes "
                "without claiming per-cell canonical prestate was retained."
            ),
            "next_action": "closed_batch_legacy_cache_readback",
            "matched_source_player_rows": str(candidates),
            "identity_bridge_status": "batch_exact_existing_player_bridge",
            "cache_join_strategy": "db_name|year|week|NFL_player_id",
            "bridge_evidence_file_count": "1",
            "bridge_evidence_row_count": str(candidates),
            "bridge_evidence_strength": "batch_source_and_existing_cache_player",
            "bridge_evidence_schemas": "db_name|year|week|NFL_player_id",
            "bridge_evidence_receipt_run_id": manifest_row["receipt_run_id"],
            "cache_admission_state": "closed_cache_verified",
        })
    elif state == "admitted_no_promotable_delta":
        row.update({
            "dispositions": "exact_cache_comparison_no_delta",
            "final_status": "no_promotable_candidate_emitted",
            "final_reason": manifest_row["reason"],
            "next_action": "closed_no_promotable_candidate_emitted",
            "cache_admission_state": "closed_no_promotable_candidate_emitted",
        })
    elif state == "source_diagnostic_only":
        row.update({
            "dispositions": "source_diagnostic_only",
            "final_status": "not_data_bearing",
            "final_reason": manifest_row["reason"],
            "next_action": "retain_diagnostic_only_no_team_signal_candidate",
            "cache_admission_state": "closed_not_data_bearing",
        })
    elif state == "explicitly_excluded_no_candidate":
        row.update({
            "dispositions": "no_recovery_output",
            "final_status": "not_data_bearing",
            "final_reason": manifest_row["reason"],
            "next_action": "rerun_only_this_shard_if_its_target_week_remains_open",
            "cache_admission_state": "closed_not_data_bearing",
        })
    else:
        raise ValueError(f"unsupported admission_state: {state!r}")
    return row


def append_admissions(
    ledger_path: Path,
    manifest_path: Path,
    out_path: Path,
    *,
    canonical_cache_key: str,
    receipt_sha256: str,
) -> dict[str, int]:
    with ledger_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fields = list(reader.fieldnames or [])
    if not fields:
        raise ValueError("ledger has no header")
    existing_ids = {row.get("artifact_id", "") for row in rows}
    with manifest_path.open(newline="", encoding="utf-8") as handle:
        manifest = list(csv.DictReader(handle))
    needed = {"artifact", "artifact_id", "admission_state", "reason", "candidate_rows", "receipt_run_id", "source_run_id", *FIELD_BY_MANIFEST_COLUMN}
    if not manifest or needed - set(manifest[0]):
        raise ValueError("manifest missing required columns")
    additions = []
    for manifest_row in manifest:
        artifact_id = manifest_row["artifact_id"].strip()
        if not artifact_id:
            index = _shard_index(manifest_row)
            artifact_id = f"no-github-artifact:run-{manifest_row['source_run_id']}:shard-{index}"
        if artifact_id in existing_ids:
            raise ValueError(f"artifact already exists in ledger: {artifact_id}")
        additions.append(_artifact_row(
            manifest_row,
            fields,
            canonical_cache_key=canonical_cache_key,
            receipt_sha256=receipt_sha256,
        ))
        existing_ids.add(artifact_id)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows([*rows, *additions])
    return {"ledger_rows": len(rows) + len(additions), "artifact_admission_rows": len(additions)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--canonical-cache-key", required=True)
    parser.add_argument("--receipt-sha256", required=True)
    args = parser.parse_args()
    print(append_admissions(
        args.ledger,
        args.manifest,
        args.out,
        canonical_cache_key=args.canonical_cache_key,
        receipt_sha256=args.receipt_sha256,
    ))


if __name__ == "__main__":
    main()
