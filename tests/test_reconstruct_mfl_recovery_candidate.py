from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path


def test_build_campaign_work_groups_all_entries_by_immutable_archive() -> None:
    from scripts.extraplatform_corpus.reconstruct_mfl_recovery_candidate import build_campaign_work

    entries = [
        {
            "source_type": "campaign_artifact",
            "season": 2004,
            "league_id": "10005",
            "db_name": "mfl_2004_10005",
            "run_id": 31896395998,
            "archive": r"D:\artifact_zips\31896395998\9250193768__batch.zip",
            "archive_sha256": "a" * 64,
            "payload_member": "batch_output/2004_10005/league.duckdb",
            "payload_sha256": "b" * 64,
            "receipt_member": "batch_output/2004_10005/receipt.json",
        },
        {
            "source_type": "campaign_artifact",
            "season": 2004,
            "league_id": "10007",
            "db_name": "mfl_2004_10007",
            "run_id": 31896395998,
            "archive": r"D:\artifact_zips\31896395998\9250193768__batch.zip",
            "archive_sha256": "a" * 64,
            "payload_member": "batch_output/2004_10007/league.duckdb",
            "payload_sha256": "c" * 64,
            "receipt_member": "batch_output/2004_10007/receipt.json",
        },
    ]

    groups = build_campaign_work(entries, remote_campaign_root="/recovery/artifacts/campaign")

    assert len(groups) == 1
    assert groups[0]["archive_path"] == "/recovery/artifacts/campaign/31896395998/9250193768__batch.zip"
    assert [item["db_name"] for item in groups[0]["entries"]] == ["mfl_2004_10005", "mfl_2004_10007"]


def test_build_campaign_work_rejects_duplicate_canonical_identity() -> None:
    from scripts.extraplatform_corpus.reconstruct_mfl_recovery_candidate import build_campaign_work

    entry = {
        "source_type": "campaign_artifact",
        "season": 2004,
        "league_id": "10005",
        "db_name": "mfl_2004_10005",
        "run_id": 1,
        "archive": r"D:\artifact_zips\1\a.zip",
        "archive_sha256": "a" * 64,
        "payload_member": "batch_output/2004_10005/league.duckdb",
        "payload_sha256": "b" * 64,
        "receipt_member": "batch_output/2004_10005/receipt.json",
    }

    try:
        build_campaign_work([entry, entry], remote_campaign_root="/recovery/artifacts/campaign")
    except ValueError as error:
        assert "duplicate canonical identity" in str(error)
    else:
        raise AssertionError("duplicate identity must fail closed")


def test_read_validated_payload_checks_hash_and_receipt_identity(tmp_path: Path) -> None:
    from scripts.extraplatform_corpus.reconstruct_mfl_recovery_candidate import read_validated_payload

    payload = b"minimal-payload"
    archive = tmp_path / "batch.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("batch_output/2004_10005/league.duckdb", payload)
        handle.writestr(
            "batch_output/2004_10005/receipt.json",
            json.dumps({"db_name": "mfl_2004_10005", "league_id": "10005", "season": 2004}),
        )
    entry = {
        "season": 2004,
        "league_id": "10005",
        "db_name": "mfl_2004_10005",
        "payload_member": "batch_output/2004_10005/league.duckdb",
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "receipt_member": "batch_output/2004_10005/receipt.json",
    }

    actual_payload, receipt = read_validated_payload(archive, entry)

    assert actual_payload == payload
    assert receipt["db_name"] == "mfl_2004_10005"


def test_read_validated_payload_rejects_bad_receipt_identity(tmp_path: Path) -> None:
    from scripts.extraplatform_corpus.reconstruct_mfl_recovery_candidate import read_validated_payload

    payload = b"minimal-payload"
    archive = tmp_path / "batch.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("batch_output/2004_10005/league.duckdb", payload)
        handle.writestr(
            "batch_output/2004_10005/receipt.json",
            json.dumps({"db_name": "mfl_2004_other", "league_id": "10005", "season": 2004}),
        )
    entry = {
        "season": 2004,
        "league_id": "10005",
        "db_name": "mfl_2004_10005",
        "payload_member": "batch_output/2004_10005/league.duckdb",
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "receipt_member": "batch_output/2004_10005/receipt.json",
    }

    try:
        read_validated_payload(archive, entry)
    except ValueError as error:
        assert "receipt identity mismatch" in str(error)
    else:
        raise AssertionError("bad receipt identity must fail closed")
