"""Attach existing player-to-team bridge evidence to the durable artifact ledger.

This consumes a prior read-only artifact receipt and updates only the existing
CSV ledger.  It neither restores nor changes a research cache.
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


BRIDGE_COLUMNS = (
    "bridge_evidence_file_count",
    "bridge_evidence_row_count",
    "bridge_evidence_strength",
    "bridge_evidence_schemas",
    "bridge_evidence_receipt_run_id",
)
REQUIRED_PLAYER_WEEK = {"db_name", "year", "week", "NFL_player_id"}
DERIVED_PLAYER_FILES = {
    "promotable_delta.parquet",
}


def _is_independent_membership_source(record: dict) -> bool:
    """Reject outputs made by rejoining the canonical cache to team signals.

    A derived delta can repeat a player row's manager/team columns, but that
    is not independent source evidence of roster membership.  Letting it
    qualify would make the bridge circular.
    """
    name = str(record.get("file") or "").strip().lower()
    return name not in DERIVED_PLAYER_FILES and not name.startswith("outcome_targets_")


def _strength(columns: set[str]) -> tuple[str, str] | None:
    """Classify source fields without claiming that a cache bridge exists.

    A source file can establish a candidate player/team membership edge, but
    cache-side team identity must still be proven by a separate join receipt.
    """
    if not REQUIRED_PLAYER_WEEK <= columns:
        return None
    if "team_key" in columns:
        return "candidate_player_team_key", "player_team_key"
    if "team_name" in columns and "manager" in columns:
        return "candidate_player_manager_team_name", "player_manager_team_name"
    if "team_name" in columns:
        return "candidate_player_team_name", "player_team_name"
    if "manager" in columns:
        return "candidate_player_manager_only", "player_manager_only"
    return None


def enrich(*, ledger_path: Path, receipt_path: Path, receipt_run_id: str, out_path: Path) -> dict[str, int]:
    ledger_rows = list(csv.DictReader(ledger_path.open(newline="", encoding="utf-8-sig")))
    if not ledger_rows:
        raise SystemExit("ledger is empty")
    ledger_ids = {int(row["artifact_id"]) for row in ledger_rows}
    if len(ledger_ids) != len(ledger_rows):
        raise SystemExit("ledger has duplicate artifact IDs")

    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    evidence: dict[int, list[tuple[str, int, str]]] = defaultdict(list)
    unknown: set[int] = set()
    for record in receipt.get("files", []):
        if record.get("artifact_id") is None:
            continue
        if not _is_independent_membership_source(record):
            continue
        # A zero-row Parquet file proves only that a producer ran, not that it
        # retained a player/team membership edge usable by the bridge.
        row_count = int(record.get("rows") or 0)
        if row_count <= 0:
            continue
        artifact_id = int(record["artifact_id"])
        if artifact_id not in ledger_ids:
            unknown.add(artifact_id)
            continue
        classified = _strength(set(record.get("columns") or []))
        if classified is None:
            continue
        strength, schema = classified
        evidence[artifact_id].append((strength, row_count, schema))

    fields = list(ledger_rows[0].keys())
    for column in BRIDGE_COLUMNS:
        if column not in fields:
            fields.append(column)
    player_team_candidate = 0
    manager_only = 0
    for row in ledger_rows:
        artifact_evidence = evidence.get(int(row["artifact_id"]), [])
        for column in BRIDGE_COLUMNS:
            row[column] = ""
        if not artifact_evidence:
            continue
        strengths = {value[0] for value in artifact_evidence}
        row["bridge_evidence_file_count"] = str(len(artifact_evidence))
        row["bridge_evidence_row_count"] = str(sum(value[1] for value in artifact_evidence))
        team_strengths = {
            "candidate_player_team_key",
            "candidate_player_manager_team_name",
            "candidate_player_team_name",
        }
        row["bridge_evidence_strength"] = next(
            (strength for strength in (
                "candidate_player_team_key",
                "candidate_player_manager_team_name",
                "candidate_player_team_name",
            ) if strength in strengths),
            "candidate_player_manager_only",
        )
        row["bridge_evidence_schemas"] = "|".join(sorted({value[2] for value in artifact_evidence}))
        row["bridge_evidence_receipt_run_id"] = str(receipt_run_id)
        if row["bridge_evidence_strength"] in team_strengths:
            player_team_candidate += 1
        else:
            manager_only += 1

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(ledger_rows)
    return {
        "ledger_rows": len(ledger_rows),
        "bridge_artifacts": len(evidence),
        "player_team_candidate_artifacts": player_team_candidate,
        "manager_only_candidate_artifacts": manager_only,
        "unknown_receipt_artifacts": len(unknown),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", type=Path, required=True)
    ap.add_argument("--receipt", type=Path, required=True)
    ap.add_argument("--receipt-run-id", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    print(json.dumps(enrich(
        ledger_path=args.ledger,
        receipt_path=args.receipt,
        receipt_run_id=args.receipt_run_id,
        out_path=args.out,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
