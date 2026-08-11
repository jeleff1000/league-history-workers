import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest


def _write_parquet(path: Path, rows: list[dict]) -> None:
    con = duckdb.connect()
    con.register("rows", pd.DataFrame(rows))
    con.execute("CREATE TABLE source AS SELECT * FROM rows")
    con.execute(f"COPY source TO '{path.as_posix()}' (FORMAT PARQUET)")
    con.close()


def _base_cache(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("""
        CREATE SCHEMA public;
        CREATE TABLE public.player_fantasy AS
        SELECT
          'league'::VARCHAR AS db_name, 2024::INTEGER AS year, 15::INTEGER AS week,
          'p1'::VARCHAR AS NFL_player_id, 1::INTEGER AS is_started,
          'mgr'::VARCHAR AS manager, 'team'::VARCHAR AS team_key,
          'T'::VARCHAR AS team_name, 'sleeper'::VARCHAR AS platform,
          1::INTEGER AS win, NULL::INTEGER AS champion, 100.0::DOUBLE AS team_points,
          0::INTEGER AS is_playoffs;
        CREATE TABLE public.league_settings AS
        SELECT 'league'::VARCHAR AS db_name, 2024::INTEGER AS year,
               '4pt'::VARCHAR AS scoring_pass_td;
    """)
    con.close()


def test_receipt_marks_cache_match_missing_and_non_candidate(tmp_path: Path) -> None:
    """Removing cache comparison must turn the verified candidate into non-verified."""
    from scripts.research_cohorts.finalize_artifact_cache_receipts import build_receipts

    base = tmp_path / "base.duckdb"
    _base_cache(base)
    ledger = tmp_path / "ledger.json"
    ledger.write_text(json.dumps({"rows": [
        {"artifact_id": 10, "artifact": "team", "candidate_delta_rows": 1,
         "candidate_fields": ["win", "champion"], "dispositions": ["team_week_signal_candidate"]},
        {"artifact_id": 11, "artifact": "settings", "candidate_delta_rows": 0,
         "candidate_fields": [], "settings_source_reference_rows": 1,
         "settings_fields": ["scoring_pass_td"], "dispositions": ["settings_candidate"]},
        {"artifact_id": 12, "artifact": "report", "candidate_delta_rows": 0,
         "candidate_fields": [], "dispositions": ["report_only"]},
        {"artifact_id": 13, "artifact": "empty-candidate", "candidate_delta_rows": 0,
         "candidate_fields": [], "dispositions": ["team_week_signal_candidate"]},
    ]}))
    team = tmp_path / "team.parquet"
    _write_parquet(team, [{
        "db_name": "league", "year": 2024, "week": 15, "NFL_player_id": "p1",
        "platform": "sleeper", "manager": "mgr", "team_key": "team", "team_name": "T",
        "source_win": 1, "source_champion": 1, "source_files": "candidates/10__team.parquet",
    }])
    settings = tmp_path / "settings.parquet"
    _write_parquet(settings, [{
        "db_name": "league", "year": 2024, "source_scoring_pass_td": "4pt",
        "source_files": "settings-candidate-shard-0-1/11/x.json",
    }])
    out = tmp_path / "receipts.json"

    result = build_receipts(
        ledger_path=ledger, base_path=base, team_delta=team, exact_delta=None,
        structured_delta=None, settings_delta=settings, out_path=out,
    )

    rows = {row["artifact_id"]: row for row in result["rows"]}
    assert rows[10]["final_status"] == "still_missing_cache_cells"
    assert rows[10]["cache_match_cells"] == 1
    assert rows[10]["cache_missing_cells"] == 1
    assert rows[11]["final_status"] == "cache_verified"
    assert rows[12]["final_status"] == "not_data_bearing"
    assert rows[13]["final_status"] == "no_promotable_candidate_emitted"


def test_receipt_fans_raw_team_signal_to_matching_player_rows(tmp_path: Path) -> None:
    """Removing team-key fanout would leave raw team evidence unaccounted for."""
    from scripts.research_cohorts.finalize_artifact_cache_receipts import build_receipts

    base = tmp_path / "base.duckdb"
    _base_cache(base)
    con = duckdb.connect(str(base))
    con.execute("UPDATE public.player_fantasy SET is_playoffs = 1")
    con.close()
    ledger = tmp_path / "ledger.json"
    ledger.write_text(json.dumps({"rows": [{
        "artifact_id": 42, "artifact": "raw-team", "candidate_delta_rows": 1,
        "candidate_fields": ["win", "team_points", "is_playoffs", "champion"],
        "dispositions": ["team_week_signal_candidate"],
    }]}))
    raw = tmp_path / "raw-team.parquet"
    _write_parquet(raw, [{
        "db_name": "league", "year": 2024, "week": 15, "team_key": "team",
        "manager": "mgr", "win": 1, "team_points": 100.0,
        "is_playoffs": 1, "champion": 1,
    }])
    manifest = tmp_path / "raw-team-manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 42, "path": str(raw)}]))
    out = tmp_path / "receipts.json"

    result = build_receipts(
        ledger_path=ledger, base_path=base, team_delta=None, exact_delta=None,
        structured_delta=None, settings_delta=None, team_signal_manifest=manifest,
        out_path=out,
    )

    row = result["rows"][0]
    assert row["final_status"] == "still_missing_cache_cells"
    assert row["source_cells"] == 4
    assert row["cache_match_cells"] == 3
    assert row["cache_missing_cells"] == 1


def test_receipt_preserves_prior_artifact_readbacks_when_overlaying_raw_signals(tmp_path: Path) -> None:
    """Removing prior-receipt seeding would regress already-verified artifacts."""
    from scripts.research_cohorts.finalize_artifact_cache_receipts import build_receipts

    base = tmp_path / "base.duckdb"
    _base_cache(base)
    ledger = tmp_path / "ledger.json"
    ledger.write_text(json.dumps({"rows": [
        {"artifact_id": 42, "artifact": "raw-team", "candidate_delta_rows": 1,
         "candidate_fields": ["win"], "dispositions": ["team_week_signal_candidate"]},
        {"artifact_id": 43, "artifact": "already-verified", "candidate_delta_rows": 1,
         "candidate_fields": ["win"], "dispositions": ["direct_player_candidate"]},
    ]}))
    prior = tmp_path / "prior.json"
    prior.write_text(json.dumps({"rows": [{
        "artifact_id": 43, "source_cells": 1, "cache_match_cells": 1,
        "cache_missing_cells": 0, "cache_conflict_cells": 0,
        "unmatched_cache_cells": 0, "blocked_schema_cells": 0,
    }]}))
    raw = tmp_path / "raw-team.parquet"
    _write_parquet(raw, [{
        "db_name": "league", "year": 2024, "week": 15, "team_key": "team", "win": 1,
    }])
    manifest = tmp_path / "raw-team-manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 42, "path": str(raw)}]))

    result = build_receipts(
        ledger_path=ledger, base_path=base, team_delta=None, exact_delta=None,
        structured_delta=None, settings_delta=None, team_signal_manifest=manifest,
        prior_receipts_path=prior, out_path=tmp_path / "receipts.json",
    )

    rows = {row["artifact_id"]: row for row in result["rows"]}
    assert rows[43]["final_status"] == "cache_verified"
    assert rows[43]["cache_match_cells"] == 1


def test_receipt_uses_manager_as_the_canonical_team_identity(tmp_path: Path) -> None:
    """Replacing manager fanout with either team-key value must make this unmatched."""
    from scripts.research_cohorts.finalize_artifact_cache_receipts import build_receipts

    base = tmp_path / "base.duckdb"
    _base_cache(base)
    con = duckdb.connect(str(base))
    con.execute("UPDATE public.player_fantasy SET platform='mfl', team_key='mfl_team_123_0001'")
    con.close()
    ledger = tmp_path / "ledger.json"
    ledger.write_text(json.dumps({"rows": [{
        "artifact_id": 55, "artifact": "mfl-team", "candidate_delta_rows": 1,
        "candidate_fields": ["win"], "dispositions": ["team_week_signal_candidate"],
    }]}))
    raw = tmp_path / "mfl-team.parquet"
    _write_parquet(raw, [{
        "db_name": "league", "year": 2024, "week": 15, "platform": "mfl",
        "team_key": "0001", "manager_guid": "mfl_team_other_0001", "manager": "mgr", "win": 1,
    }])
    manifest = tmp_path / "mfl-team-manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 55, "path": str(raw)}]))

    result = build_receipts(
        ledger_path=ledger, base_path=base, team_delta=None, exact_delta=None,
        structured_delta=None, settings_delta=None, team_signal_manifest=manifest,
        out_path=tmp_path / "receipts.json",
    )

    row = result["rows"][0]
    assert row["final_status"] == "cache_verified"
    assert row["cache_match_cells"] == 1


def test_receipt_reads_raw_player_outcome_evidence_on_its_exact_key(tmp_path: Path) -> None:
    """Raw MFL player evidence must not be treated as a manager-week fan-out."""
    from scripts.research_cohorts.finalize_artifact_cache_receipts import build_receipts

    base = tmp_path / "base.duckdb"
    _base_cache(base)
    con = duckdb.connect(str(base))
    con.execute("UPDATE public.player_fantasy SET is_playoffs = NULL")
    con.close()
    ledger = tmp_path / "ledger.json"
    ledger.write_text(json.dumps({"rows": [{
        "artifact_id": 56, "artifact": "mfl-player-outcome", "candidate_delta_rows": 1,
        "candidate_fields": ["win", "team_points", "is_playoffs"],
        "dispositions": ["direct_player_candidate"],
    }]}))
    raw = tmp_path / "mfl-player.parquet"
    _write_parquet(raw, [{
        "db_name": "league", "year": 2024, "week": 15,
        "NFL_player_id": "p1", "manager": "mgr",
        "source_win": 1, "source_team_points": 100.0,
        "source_is_playoffs": 1,
    }])
    manifest = tmp_path / "mfl-player-manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 56, "path": str(raw)}]))

    result = build_receipts(
        ledger_path=ledger, base_path=base, team_delta=None, exact_delta=None,
        structured_delta=None, settings_delta=None, structured_player_manifest=manifest,
        out_path=tmp_path / "receipts.json",
    )

    row = result["rows"][0]
    assert row["final_status"] == "still_missing_cache_cells"
    assert row["source_cells"] == 3
    assert row["cache_match_cells"] == 2
    assert row["cache_missing_cells"] == 1
