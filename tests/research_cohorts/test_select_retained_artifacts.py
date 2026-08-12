from __future__ import annotations

import csv
from pathlib import Path


def test_team_readback_reselects_earlier_team_key_only_receipts(tmp_path: Path) -> None:
    from scripts.research_cohorts.select_retained_artifacts import select

    path = tmp_path / "ledger.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["artifact_id", "artifact", "final_status"])
        writer.writeheader()
        writer.writerow({
            "artifact_id": "101", "artifact": "research-source-matchup-rescue-a",
            "final_status": "source_only_team_signal_missing_player_team_bridge",
        })
        writer.writerow({
            "artifact_id": "102", "artifact": "research-source-matchup-rescue-b",
            "final_status": "cache_verified",
        })

    rows = select(path, source_kind="team", prefix="research-source-matchup-rescue-", requested_ids={101})

    assert [row["artifact_id"] for row in rows] == ["101"]


def test_team_readback_selects_explicit_unmatched_cache_key_probe(tmp_path: Path) -> None:
    from scripts.research_cohorts.select_retained_artifacts import select

    path = tmp_path / "ledger.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["artifact_id", "artifact", "final_status"])
        writer.writeheader()
        writer.writerow({
            "artifact_id": "103", "artifact": "research-championship-identity-probe-a",
            "final_status": "unmatched_cache_key",
        })

    rows = select(path, source_kind="team", prefix="", requested_ids={103})

    assert [row["artifact_id"] for row in rows] == ["103"]


def test_championship_probe_readback_uses_the_same_exact_team_selection(tmp_path: Path) -> None:
    from scripts.research_cohorts.select_retained_artifacts import select

    path = tmp_path / "ledger.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["artifact_id", "artifact", "final_status"])
        writer.writeheader()
        writer.writerow({
            "artifact_id": "104", "artifact": "research-championship-identity-probe-a",
            "final_status": "unmatched_cache_key",
        })

    rows = select(path, source_kind="championship_probe", prefix="", requested_ids={104})

    assert [row["artifact_id"] for row in rows] == ["104"]


def test_snapshot_readback_selects_unreceipted_canonical_player_artifacts(tmp_path: Path) -> None:
    """The snapshot reader must select only the still-unreceipted snapshot artifact."""
    from scripts.research_cohorts.select_retained_artifacts import select

    path = tmp_path / "ledger.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["artifact_id", "artifact", "final_status"])
        writer.writeheader()
        writer.writerow({
            "artifact_id": "201", "artifact": "promotable-rescue-delta-31283057739-6",
            "final_status": "candidate_provenance_not_found",
        })
        writer.writerow({
            "artifact_id": "202", "artifact": "promotable-rescue-delta-31283057739-5",
            "final_status": "cache_verified",
        })

    rows = select(
        path, source_kind="canonical_player_snapshot",
        prefix="promotable-rescue-delta-31283057739-", requested_ids=set(),
    )

    assert [row["artifact_id"] for row in rows] == ["201"]
