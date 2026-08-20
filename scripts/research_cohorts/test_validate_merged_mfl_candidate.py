from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest


def _db(path: Path, *, identity: str | None, include_sleeper: bool = True) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute("CREATE TABLE public.league_settings (db_name VARCHAR, year INTEGER, league_key VARCHAR, platform VARCHAR)")
    con.execute("CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, marker VARCHAR)")
    con.execute("CREATE TABLE public.player_fantasy (db_name VARCHAR, year INTEGER, marker VARCHAR)")
    if identity is not None:
        con.execute("INSERT INTO public.league_settings VALUES ('smpl_mfl_2004_1', 2004, ?, 'mfl')", [identity])
        con.execute("INSERT INTO public.matchup VALUES ('smpl_mfl_2004_1', 2004, 'ok')")
        con.execute("INSERT INTO public.player_fantasy VALUES ('smpl_mfl_2004_1', 2004, 'ok')")
    if include_sleeper:
        con.execute("INSERT INTO public.league_settings VALUES ('smpl_sleeper_1', 2004, 's1', 'sleeper')")
        con.execute("INSERT INTO public.matchup VALUES ('smpl_sleeper_1', 2004, 'keep')")
        con.execute("INSERT INTO public.player_fantasy VALUES ('smpl_sleeper_1', 2004, 'keep')")
    con.close()


def _ops(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA nfl_historical")
    con.execute("CREATE TABLE nfl_historical.nfl_player_stats_all (year INTEGER, player VARCHAR)")
    con.execute("INSERT INTO nfl_historical.nfl_player_stats_all VALUES (2004, 'sample')")
    con.close()


def _index(path: Path) -> None:
    path.write_text(json.dumps({
        "league_count": 1,
        "accepted_by_year": {"2004": 1},
        "leagues": [{"db_name": "mfl_2004_001", "season": 2004, "league_id": "001"}],
    }), encoding="utf-8")


def test_validate_merged_candidate_writes_complete_fresh_proof_set(tmp_path: Path) -> None:
    from scripts.research_cohorts.validate_merged_mfl_candidate import validate_merged_candidate

    base = tmp_path / "base.duckdb"
    recovered = tmp_path / "recovered.duckdb"
    candidate = tmp_path / "candidate.duckdb"
    ops = tmp_path / "ops.duckdb"
    index = tmp_path / "index.json"
    recovery = tmp_path / "recovery.json"
    proof_dir = tmp_path / "proof"
    _db(base, identity="001")
    _db(recovered, identity="001")
    _db(candidate, identity="001")
    _ops(ops)
    _index(index)
    recovery.write_text(json.dumps({"ok": True, "identity_count": 1}), encoding="utf-8")

    report = validate_merged_candidate(
        base=base,
        recovered=recovered,
        recovered_index=index,
        candidate=candidate,
        ops=ops,
        recovery_proof=recovery,
        proof_dir=proof_dir,
    )

    assert report["ok"] is True
    assert report["expected_identity_count"] == 1
    assert (proof_dir / "merged_identity_registry.json").is_file()
    assert (proof_dir / "merge_proof.json").is_file()
    assert (proof_dir / "artifact_recovery_report.json").is_file()
    assert (proof_dir / "missing_data_report.json").is_file()
    assert (proof_dir / "conflict_report.json").is_file()
    assert (proof_dir / "schema_proof.json").is_file()
    assert (proof_dir / "checksum_manifest.json").is_file()


def test_validate_merged_candidate_writes_missing_report_then_fails(tmp_path: Path) -> None:
    from scripts.research_cohorts.validate_merged_mfl_candidate import validate_merged_candidate

    base = tmp_path / "base.duckdb"
    recovered = tmp_path / "recovered.duckdb"
    candidate = tmp_path / "candidate.duckdb"
    ops = tmp_path / "ops.duckdb"
    index = tmp_path / "index.json"
    recovery = tmp_path / "recovery.json"
    proof_dir = tmp_path / "proof"
    _db(base, identity=None)
    _db(recovered, identity="001")
    _db(candidate, identity=None)
    _ops(ops)
    _index(index)
    recovery.write_text(json.dumps({"ok": True, "identity_count": 1}), encoding="utf-8")

    with pytest.raises(RuntimeError, match="missing expected MFL identities"):
        validate_merged_candidate(
            base=base,
            recovered=recovered,
            recovered_index=index,
            candidate=candidate,
            ops=ops,
            recovery_proof=recovery,
            proof_dir=proof_dir,
        )
    missing = json.loads((proof_dir / "missing_data_report.json").read_text(encoding="utf-8"))
    assert missing["missing_identities"] == [{"season": 2004, "league_id": "1"}]


def test_validate_merged_candidate_rejects_lost_unaffected_base_rows(tmp_path: Path) -> None:
    from scripts.research_cohorts.validate_merged_mfl_candidate import validate_merged_candidate

    base = tmp_path / "base.duckdb"
    recovered = tmp_path / "recovered.duckdb"
    candidate = tmp_path / "candidate.duckdb"
    ops = tmp_path / "ops.duckdb"
    index = tmp_path / "index.json"
    recovery = tmp_path / "recovery.json"
    _db(base, identity="001")
    _db(recovered, identity="001")
    _db(candidate, identity="001", include_sleeper=False)
    _ops(ops)
    _index(index)
    recovery.write_text(json.dumps({"ok": True, "identity_count": 1}), encoding="utf-8")

    with pytest.raises(RuntimeError, match="unaffected base rows"):
        validate_merged_candidate(
            base=base,
            recovered=recovered,
            recovered_index=index,
            candidate=candidate,
            ops=ops,
            recovery_proof=recovery,
            proof_dir=tmp_path / "proof",
        )
