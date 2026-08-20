from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

import duckdb


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


def _canonical_db(path: Path, *, marker: str, mismatch: bool = False, db_name: str = "mfl_2004_10005") -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    for table in ("league_settings", "matchup", "player_fantasy"):
        if mismatch and table == "matchup":
            con.execute(f"CREATE TABLE public.{table} (db_name VARCHAR, year INTEGER, wrong_marker VARCHAR)")
            con.execute(f"INSERT INTO public.{table} VALUES (?, 2004, ?)", [db_name, marker])
        else:
            con.execute(f"CREATE TABLE public.{table} (db_name VARCHAR, year INTEGER, marker VARCHAR)")
            con.execute(f"INSERT INTO public.{table} VALUES (?, 2004, ?)", [db_name, marker])
    con.close()


def test_append_canonical_payload_creates_and_appends_exact_schema(tmp_path: Path) -> None:
    from scripts.extraplatform_corpus.reconstruct_mfl_recovery_candidate import append_canonical_payload

    first = tmp_path / "first.duckdb"
    second = tmp_path / "second.duckdb"
    candidate = tmp_path / "candidate.duckdb"
    _canonical_db(first, marker="one")
    _canonical_db(second, marker="two")

    schema = append_canonical_payload(candidate, first)
    append_canonical_payload(candidate, second, expected_schema=schema)

    con = duckdb.connect(str(candidate), read_only=True)
    for table in ("league_settings", "matchup", "player_fantasy"):
        assert con.execute(f"SELECT marker FROM public.{table} ORDER BY marker").fetchall() == [("one",), ("two",)]
    con.close()


def test_append_canonical_payload_rejects_mismatched_schema_before_candidate_creation(tmp_path: Path) -> None:
    from scripts.extraplatform_corpus.reconstruct_mfl_recovery_candidate import append_canonical_payload

    payload = tmp_path / "bad.duckdb"
    candidate = tmp_path / "candidate.duckdb"
    _canonical_db(payload, marker="bad", mismatch=True)

    try:
        append_canonical_payload(candidate, payload, expected_schema={
            "league_settings": [("db_name", "VARCHAR"), ("year", "INTEGER"), ("marker", "VARCHAR")],
            "matchup": [("db_name", "VARCHAR"), ("year", "INTEGER"), ("marker", "VARCHAR")],
            "player_fantasy": [("db_name", "VARCHAR"), ("year", "INTEGER"), ("marker", "VARCHAR")],
        })
    except ValueError as error:
        assert "canonical schema mismatch" in str(error)
    else:
        raise AssertionError("schema mismatch must fail closed")
    assert not candidate.exists()


def test_assemble_campaign_group_writes_validated_payloads_once(tmp_path: Path) -> None:
    from scripts.extraplatform_corpus.reconstruct_mfl_recovery_candidate import assemble_campaign_group

    payload_one = tmp_path / "one.duckdb"
    payload_two = tmp_path / "two.duckdb"
    _canonical_db(payload_one, marker="one", db_name="mfl_2004_10005")
    _canonical_db(payload_two, marker="two", db_name="mfl_2004_10007")
    archive = tmp_path / "batch.zip"
    first_bytes = payload_one.read_bytes()
    second_bytes = payload_two.read_bytes()
    with zipfile.ZipFile(archive, "w") as handle:
        for league_id, payload, marker in (("10005", first_bytes, "one"), ("10007", second_bytes, "two")):
            handle.writestr(f"batch_output/2004_{league_id}/league.duckdb", payload)
            handle.writestr(
                f"batch_output/2004_{league_id}/receipt.json",
                json.dumps({"db_name": f"mfl_2004_{league_id}", "league_id": league_id, "season": 2004}),
            )
    group = {
        "archive_path": str(archive),
        "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
        "entries": [
            {
                "season": 2004, "league_id": "10005", "db_name": "mfl_2004_10005",
                "payload_member": "batch_output/2004_10005/league.duckdb",
                "payload_sha256": hashlib.sha256(first_bytes).hexdigest(),
                "receipt_member": "batch_output/2004_10005/receipt.json",
            },
            {
                "season": 2004, "league_id": "10007", "db_name": "mfl_2004_10007",
                "payload_member": "batch_output/2004_10007/league.duckdb",
                "payload_sha256": hashlib.sha256(second_bytes).hexdigest(),
                "receipt_member": "batch_output/2004_10007/receipt.json",
            },
        ],
    }
    candidate = tmp_path / "candidate.duckdb"
    work = tmp_path / "work"

    records = assemble_campaign_group(group, candidate_path=candidate, work_dir=work)

    assert [record["db_name"] for record in records] == ["mfl_2004_10005", "mfl_2004_10007"]
    assert all(Path(record["payload_path"]).is_file() for record in records)
    con = duckdb.connect(str(candidate), read_only=True)
    assert con.execute("SELECT db_name, marker FROM public.player_fantasy ORDER BY db_name").fetchall() == [
        ("mfl_2004_10005", "one"),
        ("mfl_2004_10007", "two"),
    ]
    con.close()
