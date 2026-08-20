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


def _canonical_db(
    path: Path,
    *,
    marker: str,
    mismatch: bool = False,
    db_name: str = "mfl_2004_10005",
    schema: str = "public",
    include_league_key: bool = False,
    year: int = 2004,
    league_key: str = "10005",
) -> None:
    con = duckdb.connect(str(path))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for table in ("league_settings", "matchup", "player_fantasy"):
        table_name = f"{schema}.{table}"
        settings_suffix = ", league_key VARCHAR" if table == "league_settings" and include_league_key else ""
        if mismatch and table == "matchup":
            con.execute(f"CREATE TABLE {table_name} (db_name VARCHAR, year INTEGER, wrong_marker VARCHAR)")
            con.execute(f"INSERT INTO {table_name} VALUES (?, ?, ?)", [db_name, year, marker])
        else:
            con.execute(f"CREATE TABLE {table_name} (db_name VARCHAR, year INTEGER, marker VARCHAR{settings_suffix})")
            if table == "league_settings" and include_league_key:
                con.execute(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)", [db_name, year, marker, league_key])
            else:
                con.execute(f"INSERT INTO {table_name} VALUES (?, ?, ?)", [db_name, year, marker])
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


def test_validate_candidate_identities_uses_year_and_league_key(tmp_path: Path) -> None:
    from scripts.extraplatform_corpus.reconstruct_mfl_recovery_candidate import (
        validate_candidate_identities,
    )

    candidate = tmp_path / "candidate.duckdb"
    _canonical_db(candidate, marker="one", db_name="mfl_2004_10005")
    con = duckdb.connect(str(candidate))
    con.execute("ALTER TABLE public.league_settings ADD COLUMN league_key VARCHAR")
    con.execute("UPDATE public.league_settings SET league_key = '10005'")
    con.close()

    assert validate_candidate_identities(
        candidate,
        [{"season": 2004, "league_id": "10005", "db_name": "mfl_2004_10005"}],
    ) == {"league_settings": 1, "matchup": 1, "player_fantasy": 1}


def test_append_protected_source_filters_and_validates_exact_identities(tmp_path: Path) -> None:
    from scripts.extraplatform_corpus.reconstruct_mfl_recovery_candidate import (
        append_protected_source_chunk,
        canonical_schema,
        validate_candidate_identities,
    )

    candidate = tmp_path / "candidate.duckdb"
    source = tmp_path / "protected.duckdb"
    _canonical_db(candidate, marker="campaign", db_name="mfl_2004_10005", include_league_key=True)
    _canonical_db(
        source,
        marker="protected",
        db_name="mfl_2016_51031",
        schema="main",
        include_league_key=True,
        year=2016,
        league_key="51031",
    )
    expected = [
        {"season": 2004, "league_id": "10005", "db_name": "mfl_2004_10005"},
        {"season": 2016, "league_id": "51031"},
    ]

    appended = append_protected_source_chunk(
        candidate,
        source,
        [{"season": 2016, "league_id": "51031"}],
        expected_schema=canonical_schema(candidate),
    )

    assert appended == ["mfl_2016_51031"]
    assert validate_candidate_identities(candidate, expected) == {
        "league_settings": 2,
        "matchup": 2,
        "player_fantasy": 2,
    }


def test_run_lane_creates_one_validated_candidate_and_immutable_proof(tmp_path: Path) -> None:
    from scripts.extraplatform_corpus.reconstruct_mfl_recovery_candidate import run_lane

    payload = tmp_path / "campaign_payload.duckdb"
    protected_chunk = tmp_path / "protected_chunk.duckdb"
    _canonical_db(payload, marker="campaign", db_name="mfl_2004_10005", include_league_key=True)
    _canonical_db(
        protected_chunk,
        marker="protected",
        db_name="mfl_2016_51031",
        schema="main",
        include_league_key=True,
        year=2016,
        league_key="51031",
    )
    campaign_archive = tmp_path / "campaign.zip"
    campaign_bytes = payload.read_bytes()
    with zipfile.ZipFile(campaign_archive, "w") as handle:
        handle.writestr("batch_output/2004_10005/league.duckdb", campaign_bytes)
        handle.writestr(
            "batch_output/2004_10005/receipt.json",
            json.dumps({"db_name": "mfl_2004_10005", "league_id": "10005", "season": 2004}),
        )
    protected_archive = tmp_path / "protected.zip"
    with zipfile.ZipFile(protected_archive, "w") as handle:
        handle.writestr("chunk_state/mfl_register_chunk.duckdb", protected_chunk.read_bytes())
    campaign_entry = {
        "season": 2004,
        "league_id": "10005",
        "db_name": "mfl_2004_10005",
        "payload_member": "batch_output/2004_10005/league.duckdb",
        "payload_sha256": hashlib.sha256(campaign_bytes).hexdigest(),
        "receipt_member": "batch_output/2004_10005/receipt.json",
    }
    lane = {
        "lane": 1,
        "items": [
            {
                "kind": "campaign",
                "path": str(campaign_archive),
                "sha256": hashlib.sha256(campaign_archive.read_bytes()).hexdigest(),
                "entries": [campaign_entry],
            },
            {
                "kind": "protected",
                "path": str(protected_archive),
                "sha256": hashlib.sha256(protected_archive.read_bytes()).hexdigest(),
                "entries": [{"season": 2016, "league_id": "51031"}],
            },
        ],
    }
    candidate = tmp_path / "lane.duckdb"
    work_dir = tmp_path / "work"
    proof = tmp_path / "proof.json"

    report = run_lane(lane, candidate_path=candidate, work_dir=work_dir, proof_path=proof)

    assert report["ok"] is True
    assert report["expected_identity_count"] == 2
    assert report["table_rows"] == {"league_settings": 2, "matchup": 2, "player_fantasy": 2}
    assert json.loads(proof.read_text(encoding="utf-8"))["candidate_sha256"] == report["candidate_sha256"]


def test_run_all_lanes_writes_final_proof_only_after_every_lane_passes(tmp_path: Path) -> None:
    from scripts.extraplatform_corpus.run_mfl_reconstruction_lanes import run_all_lanes

    payload = tmp_path / "campaign_payload.duckdb"
    _canonical_db(payload, marker="campaign", db_name="mfl_2004_10005", include_league_key=True)
    campaign_bytes = payload.read_bytes()
    campaign_archive = tmp_path / "campaign.zip"
    with zipfile.ZipFile(campaign_archive, "w") as handle:
        handle.writestr("batch_output/2004_10005/league.duckdb", campaign_bytes)
        handle.writestr(
            "batch_output/2004_10005/receipt.json",
            json.dumps({"db_name": "mfl_2004_10005", "league_id": "10005", "season": 2004}),
        )
    plan = {
        "expected_identity_count": 1,
        "lanes": [{
            "lane": 0,
            "identity_count": 1,
            "items": [{
                "kind": "campaign",
                "path": str(campaign_archive),
                "sha256": hashlib.sha256(campaign_archive.read_bytes()).hexdigest(),
                "entries": [{
                    "season": 2004,
                    "league_id": "10005",
                    "db_name": "mfl_2004_10005",
                    "payload_member": "batch_output/2004_10005/league.duckdb",
                    "payload_sha256": hashlib.sha256(campaign_bytes).hexdigest(),
                    "receipt_member": "batch_output/2004_10005/receipt.json",
                }],
            }],
        }],
    }
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    run_root = tmp_path / "run"

    report = run_all_lanes(plan_path, run_root=run_root, workers=1)

    assert report["ok"] is True
    assert report["lane_count"] == 1
    assert report["identity_count"] == 1
    assert (run_root / "ALL_LANES_OK.json").is_file()


def test_finalize_lanes_requires_all_proven_candidates_and_writes_merge_index(tmp_path: Path) -> None:
    from scripts.extraplatform_corpus.finalize_mfl_reconstruction_lanes import finalize_lanes

    run_root = tmp_path / "run"
    candidates = run_root / "candidates"
    proof_dir = run_root / "proof"
    candidates.mkdir(parents=True)
    proof_dir.mkdir()
    first = candidates / "lane_00.duckdb"
    second = candidates / "lane_01.duckdb"
    _canonical_db(first, marker="one", db_name="mfl_2004_10005", include_league_key=True)
    _canonical_db(
        second,
        marker="two",
        db_name="mfl_2005_10006",
        include_league_key=True,
        year=2005,
        league_key="10006",
    )
    first_hash = hashlib.sha256(first.read_bytes()).hexdigest()
    second_hash = hashlib.sha256(second.read_bytes()).hexdigest()
    for ordinal, candidate, digest in ((0, first, first_hash), (1, second, second_hash)):
        (proof_dir / f"lane_{ordinal:02d}.json").write_text(
            json.dumps({"ok": True, "candidate_path": str(candidate), "candidate_sha256": digest}),
            encoding="utf-8",
        )
    (run_root / "ALL_LANES_OK.json").write_text(
        json.dumps({
            "ok": True,
            "identity_count": 2,
            "lane_count": 2,
            "lane_candidates": [
                {"lane": 0, "path": str(first), "sha256": first_hash},
                {"lane": 1, "path": str(second), "sha256": second_hash},
            ],
        }),
        encoding="utf-8",
    )
    lane_plan = tmp_path / "lane_plan.json"
    lane_plan.write_text(
        json.dumps({
            "expected_identity_count": 2,
            "lanes": [
                {"lane": 0, "identity_count": 1, "items": [{"entries": [{"season": 2004, "league_id": "10005", "db_name": "mfl_2004_10005"}]}]},
                {"lane": 1, "identity_count": 1, "items": [{"entries": [{"season": 2005, "league_id": "10006", "db_name": "mfl_2005_10006"}]}]},
            ],
        }),
        encoding="utf-8",
    )
    candidate = tmp_path / "mfl_reconstructed.duckdb"
    index = tmp_path / "mfl_register_all_runs.json"
    proof = tmp_path / "finalize_proof.json"

    report = finalize_lanes(run_root, lane_plan, candidate, index, proof)

    assert report["ok"] is True
    assert report["identity_count"] == 2
    assert json.loads(index.read_text(encoding="utf-8"))["accepted_by_year"] == {"2004": 1, "2005": 1}
    con = duckdb.connect(str(candidate), read_only=True)
    assert con.execute("SELECT db_name FROM public.league_settings ORDER BY db_name").fetchall() == [
        ("mfl_2004_10005",),
        ("mfl_2005_10006",),
    ]
    con.close()


def test_finalize_completed_lanes_writes_terminal_receipt(tmp_path: Path, monkeypatch) -> None:
    from scripts.extraplatform_corpus import finalize_completed_mfl_reconstruction as module

    run_root = tmp_path / "run"
    run_root.mkdir()
    (run_root / "ALL_LANES_OK.json").write_text(
        json.dumps({"ok": True, "identity_count": 39_368, "lane_count": 15}),
        encoding="utf-8",
    )
    candidate = tmp_path / "final" / "candidate.duckdb"
    index = tmp_path / "final" / "index.json"
    proof = tmp_path / "final" / "proof.json"
    calls: list[tuple[Path, Path, Path, Path, Path]] = []

    def fake_finalize(
        observed_run_root: Path,
        observed_plan: Path,
        observed_candidate: Path,
        observed_index: Path,
        observed_proof: Path,
    ) -> dict[str, object]:
        calls.append((observed_run_root, observed_plan, observed_candidate, observed_index, observed_proof))
        proof.parent.mkdir(parents=True, exist_ok=True)
        proof.write_text(json.dumps({"ok": True}), encoding="utf-8")
        return {"ok": True, "identity_count": 39_368, "candidate_sha256": "a" * 64}

    monkeypatch.setattr(module, "finalize_lanes", fake_finalize)
    report = module.finalize_completed_lanes(
        run_root=run_root,
        lane_plan_path=tmp_path / "plan.json",
        candidate_path=candidate,
        index_path=index,
        proof_path=proof,
    )

    assert calls == [(run_root, tmp_path / "plan.json", candidate, index, proof)]
    assert report["ok"] is True
    assert json.loads((run_root / "FINALIZATION_OK.json").read_text(encoding="utf-8"))["identity_count"] == 39_368


def test_finalize_completed_lanes_rejects_failed_or_incomplete_run(tmp_path: Path) -> None:
    from scripts.extraplatform_corpus.finalize_completed_mfl_reconstruction import finalize_completed_lanes

    run_root = tmp_path / "run"
    run_root.mkdir()

    try:
        finalize_completed_lanes(
            run_root=run_root,
            lane_plan_path=tmp_path / "plan.json",
            candidate_path=tmp_path / "candidate.duckdb",
            index_path=tmp_path / "index.json",
            proof_path=tmp_path / "proof.json",
        )
    except RuntimeError as error:
        assert "not complete" in str(error)
    else:
        raise AssertionError("incomplete lane run must not finalize")

    (run_root / "RUN_FAILED.json").write_text(json.dumps({"ok": False}), encoding="utf-8")
    try:
        finalize_completed_lanes(
            run_root=run_root,
            lane_plan_path=tmp_path / "plan.json",
            candidate_path=tmp_path / "candidate.duckdb",
            index_path=tmp_path / "index.json",
            proof_path=tmp_path / "proof.json",
        )
    except RuntimeError as error:
        assert "failed" in str(error)
    else:
        raise AssertionError("failed lane run must not finalize")


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
