from __future__ import annotations

from pathlib import Path

import duckdb

import hashlib
import json
import zipfile

from audit_mfl_original_payloads import (
    PayloadAuditError,
    audit_landed_source_archives,
    inspect_receipt_payload,
    inspect_source_archive,
)


def _payload_database(path: Path) -> dict[str, object]:
    connection = duckdb.connect(str(path))
    connection.execute("CREATE SCHEMA public")
    connection.execute("CREATE TABLE public.league_settings(db_name VARCHAR, year INTEGER)")
    connection.execute("INSERT INTO public.league_settings VALUES ('mfl_2006_26487', 2006)")
    connection.execute("CREATE TABLE public.matchup(db_name VARCHAR, year INTEGER, week INTEGER)")
    connection.execute("INSERT INTO public.matchup VALUES ('mfl_2006_26487', 2006, 1)")
    connection.execute("CREATE TABLE public.player_fantasy(db_name VARCHAR, year INTEGER, week INTEGER, player VARCHAR)")
    connection.execute("INSERT INTO public.player_fantasy VALUES ('mfl_2006_26487', 2006, 1, 'Player')")
    receipt = {
        "schema_version": "mfl_register_receipt_v1",
        "season": 2006,
        "league_id": "26487",
        "db_name": "mfl_2006_26487",
        "row_counts": {"matchup": 1, "player_fantasy": 1},
        "schema_columns": {
            "matchup": ["db_name", "year", "week"],
            "player_fantasy": ["db_name", "year", "week", "player"],
        },
    }
    connection.close()
    return receipt


def test_inspect_receipt_payload_requires_the_receipt_identity_and_actual_full_tables(tmp_path: Path) -> None:
    database = tmp_path / "league.duckdb"
    receipt = _payload_database(database)

    result = inspect_receipt_payload(receipt, database)

    assert result["identity"] == (2006, "26487")
    assert result["row_counts"] == {"league_settings": 1, "matchup": 1, "player_fantasy": 1}


def test_inspect_receipt_payload_rejects_a_database_with_wrong_receipt_row_count(tmp_path: Path) -> None:
    database = tmp_path / "league.duckdb"
    receipt = _payload_database(database)
    receipt["row_counts"] = {"matchup": 2, "player_fantasy": 1}

    try:
        inspect_receipt_payload(receipt, database)
    except PayloadAuditError as exc:
        assert "row count" in str(exc)
    else:
        raise AssertionError("a mismatched full-schema payload was accepted")


def test_inspect_source_archive_records_valid_payloads_and_batch_failures(tmp_path: Path) -> None:
    database = tmp_path / "league.duckdb"
    receipt = _payload_database(database)
    archive = tmp_path / "source.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("batch_output/2006_26487/receipt.json", json.dumps(receipt))
        zipped.write(database, "batch_output/2006_26487/league.duckdb")
        zipped.writestr(
            "batch_output/batch_receipt.json",
            json.dumps({"planned": 2, "successes": 1, "failures": [{"season": 2006, "league_id": "x", "reason": "fetch failed"}]}),
        )

    report = inspect_source_archive(
        archive,
        provenance={"artifact_id": 7, "artifact_name": "mfl-register-batch-1", "run_id": 3},
        scratch_directory=tmp_path / "scratch",
    )

    assert report["valid_payloads"] == [{
        "artifact_id": 7,
        "artifact_name": "mfl-register-batch-1",
        "created_at": None,
        "identity": (2006, "26487"),
        "payload_sha256": hashlib.sha256(database.read_bytes()).hexdigest(),
        "run_id": 3,
    }]
    assert report["invalid_payloads"] == []
    assert report["batch_failures"] == [{"league_id": "x", "reason": "fetch failed", "season": 2006}]


def test_audit_landed_source_archives_preserves_artifact_provenance_for_every_payload(tmp_path: Path) -> None:
    database = tmp_path / "league.duckdb"
    receipt = _payload_database(database)
    raw_archive = tmp_path / "raw.zip"
    with zipfile.ZipFile(raw_archive, "w") as zipped:
        zipped.writestr("batch_output/2006_26487/receipt.json", json.dumps(receipt))
        zipped.write(database, "batch_output/2006_26487/league.duckdb")

    landing = tmp_path / "D" / "pc_artifacts" / "campaigns" / "456" / "100"
    (landing / "artifacts").mkdir(parents=True)
    (landing / "artifacts" / "7.zip").write_bytes(raw_archive.read_bytes())
    (landing / "lane-manifest.json").write_text(
        json.dumps(
            {
                "lane": 0,
                "artifacts": [
                    {
                        "artifact_id": 7,
                        "run_id": 3,
                        "name": "mfl-register-batch-1",
                        "created_at": "2026-08-15T00:00:00Z",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = audit_landed_source_archives(
        tmp_path / "D" / "pc_artifacts",
        run_id=456,
        scratch_directory=tmp_path / "scratch",
    )

    assert report["source_archive_count"] == 1
    assert report["valid_payload_count"] == 1
    assert report["valid_payloads"][0]["artifact_id"] == 7
    assert report["valid_payloads"][0]["identity"] == (2006, "26487")
