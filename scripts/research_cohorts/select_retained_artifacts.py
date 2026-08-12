"""Select ledger-exact retained artifacts for a read-only cache receipt."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


TEAM_RECHECK_STATUSES = {
    "candidate_provenance_not_found",
    "partial_schema_blocked_unmatched",
    "source_only_team_signal_missing_player_team_bridge",
    # The readback receipt already proved this source has an actual canonical
    # NULL cell.  It must be eligible for candidate materialization; excluding
    # it here strands the evidence before the manager-week fan-out.
    "still_missing_cache_cells",
    "unmatched_cache_key",
    # These statuses were assigned while canonical loss/tie fields did not
    # exist.  Once those two sanctioned fields are present, their source facts
    # must be re-audited against the live cache rather than left permanently
    # stranded behind an obsolete schema gate.
    "partial_schema_blocked_direct_identity",
    "partial_schema_blocked_conflicts",
}
STRUCTURED_RECHECK_STATUSES = {
    "candidate_provenance_not_found",
    "no_promotable_candidate_emitted",
    "candidate_built_pending_canonical_promotion",
}
CANONICAL_SNAPSHOT_RECHECK_STATUSES = {
    "candidate_provenance_not_found",
}


def select(
    ledger_csv: Path,
    *,
    source_kind: str,
    prefix: str,
    requested_ids: set[int],
) -> list[dict[str, str]]:
    if source_kind in {"team", "championship_probe"}:
        eligible_statuses = TEAM_RECHECK_STATUSES
    elif source_kind == "structured_player":
        eligible_statuses = STRUCTURED_RECHECK_STATUSES
    elif source_kind == "canonical_player_snapshot":
        eligible_statuses = CANONICAL_SNAPSHOT_RECHECK_STATUSES
    else:
        raise SystemExit(f"unsupported source kind: {source_kind}")
    with ledger_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    selected = [
        row for row in rows
        if row.get("record_type") == "artifact_inventory"
        and row.get("final_status") in eligible_statuses
        and (int(row["artifact_id"]) in requested_ids if requested_ids else row["artifact"].startswith(prefix))
    ]
    if not selected:
        raise SystemExit(f"no unresolved retained artifacts for prefix {prefix!r}")
    return selected


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger-csv", type=Path, required=True)
    parser.add_argument(
        "--source-kind", required=True,
        choices=("team", "championship_probe", "structured_player", "canonical_player_snapshot"),
    )
    parser.add_argument("--prefix", default="")
    parser.add_argument("--artifact-ids", default="")
    parser.add_argument("--out-json", type=Path, required=True)
    parser.add_argument("--out-tsv", type=Path, required=True)
    args = parser.parse_args()
    requested_ids = {int(value) for value in args.artifact_ids.split(",") if value.strip()}
    rows = select(
        args.ledger_csv, source_kind=args.source_kind, prefix=args.prefix,
        requested_ids=requested_ids,
    )
    args.out_json.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    with args.out_tsv.open("w", encoding="utf-8", newline="") as handle:
        for row in rows:
            handle.write(f"{row['workflow_run_id']}\t{row['artifact']}\t{row['artifact_id']}\n")
    print(json.dumps({"selected_artifacts": len(rows), "prefix": args.prefix}, sort_keys=True))


if __name__ == "__main__":
    main()
