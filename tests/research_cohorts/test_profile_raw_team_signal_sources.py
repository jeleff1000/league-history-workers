import json
from pathlib import Path

import duckdb
import pandas as pd


def _parquet(path: Path, rows: list[dict]) -> None:
    con = duckdb.connect()
    con.register("rows", pd.DataFrame(rows))
    con.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_profile_batches_exact_team_key_checks_without_mutating_cache(tmp_path: Path) -> None:
    """Removing the single batched semi-join would make per-artifact scans regress."""
    from scripts.research_cohorts.profile_raw_team_signal_sources import profile

    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT 'league'::VARCHAR AS db_name, 2024::INTEGER AS year, 1::INTEGER AS week,
               'team-a'::VARCHAR AS team_key, 'p1'::VARCHAR AS NFL_player_id
        UNION ALL
        SELECT 'league', 2024, 1, 'team-a', 'p2'
    """)
    con.close()

    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    _parquet(first, [{"db_name": "league", "year": 2024, "week": 1, "team_key": "team-a", "win": 1}])
    _parquet(second, [
        {"db_name": "league", "year": 2024, "week": 1, "team_key": "team-b", "win": 1},
        {"db_name": "league", "year": 2024, "week": 1, "team_key": "team-b", "win": 1},
    ])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([
        {"artifact_id": 10, "path": str(first)},
        {"artifact_id": 11, "path": str(second)},
    ]))

    result = profile(base=base, manifest=manifest)

    by_id = {row["artifact_id"]: row for row in result["rows"]}
    assert result["read_only"] is True
    assert result["cache_mutated"] is False
    assert by_id[10] == {
        "artifact_id": 10,
        "source_team_rows": 1,
        "source_team_keys": 1,
        "league_week_overlap_keys": 1,
        "league_week_absent_keys": 0,
        "matched_source_team_rows": 1,
        "unmatched_source_team_rows": 0,
        "matched_source_team_keys": 1,
        "unmatched_source_team_keys": 0,
        "matched_by_mfl_manager_guid": 0,
        "matched_by_manager_team_name": 0,
        "matched_by_unique_manager": 0,
        "matched_source_player_rows": 2,
    }
    assert by_id[11]["source_team_rows"] == 2
    assert by_id[11]["source_team_keys"] == 1
    assert by_id[11]["matched_source_team_rows"] == 0
    assert by_id[11]["unmatched_source_team_rows"] == 2
    assert by_id[11]["unmatched_source_team_keys"] == 1

    check = duckdb.connect(str(base), read_only=True)
    assert check.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0] == 2
    check.close()


def test_profile_uses_mfl_manager_guid_as_the_canonical_team_week_identity(tmp_path: Path) -> None:
    """MFL raw ``team_key`` is only a slot; its manager_guid is the cache key."""
    from scripts.research_cohorts.profile_raw_team_signal_sources import profile

    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT 'smpl_mfl_2015_16002'::VARCHAR AS db_name,
               2003::INTEGER AS year, 1::INTEGER AS week,
               'mfl_team_78253_0001'::VARCHAR AS team_key,
               'p1'::VARCHAR AS NFL_player_id
        UNION ALL
        SELECT 'smpl_mfl_2015_16002', 2003, 1,
               'mfl_team_78253_0001', 'p2'
    """)
    con.close()

    raw = tmp_path / "mfl.parquet"
    _parquet(raw, [{
        "db_name": "smpl_mfl_2015_16002", "year": 2003, "week": 1,
        "platform": "mfl", "team_key": "0001",
        "manager_guid": "mfl_team_78253_0001", "win": 1,
    }])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 12, "path": str(raw)}]))

    result = profile(base=base, manifest=manifest)

    row = result["rows"][0]
    assert row["matched_source_team_rows"] == 1
    assert row["matched_source_team_keys"] == 1
    assert row["matched_source_player_rows"] == 2
    assert row["matched_by_mfl_manager_guid"] == 1


def test_profile_uses_unique_manager_and_team_name_when_mfl_keys_are_rewritten(tmp_path: Path) -> None:
    """An MFL source team can still map exactly when its canonical key was rewritten."""
    from scripts.research_cohorts.profile_raw_team_signal_sources import profile

    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT 'smpl_mfl_2015_16002'::VARCHAR AS db_name,
               2003::INTEGER AS year, 1::INTEGER AS week,
               'rewritten-cache-key'::VARCHAR AS team_key,
               'BLTN'::VARCHAR AS manager, 'BLTN'::VARCHAR AS team_name,
               'p1'::VARCHAR AS NFL_player_id
        UNION ALL
        SELECT 'smpl_mfl_2015_16002', 2003, 1,
               'rewritten-cache-key', 'BLTN', 'BLTN', 'p2'
    """)
    con.close()
    raw = tmp_path / "mfl.parquet"
    _parquet(raw, [{
        "db_name": "smpl_mfl_2015_16002", "year": 2003, "week": 1,
        "platform": "mfl", "team_key": "0001",
        "manager_guid": "mfl_team_78253_0001", "manager": "BLTN",
        "team_name": "BLTN", "win": 1,
    }])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 13, "path": str(raw)}]))

    row = profile(base=base, manifest=manifest)["rows"][0]

    assert row["matched_source_team_rows"] == 1
    assert row["matched_source_player_rows"] == 2
    assert row["matched_by_manager_team_name"] == 1


def test_profile_reports_source_league_weeks_present_before_identity_resolution(tmp_path: Path) -> None:
    """The receipt must distinguish an absent league-week from an unresolved team key."""
    from scripts.research_cohorts.profile_raw_team_signal_sources import profile

    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT 'league'::VARCHAR AS db_name, 2024::INTEGER AS year,
               1::INTEGER AS week, 'cache-team'::VARCHAR AS team_key,
               'p1'::VARCHAR AS NFL_player_id
    """)
    con.close()
    raw = tmp_path / "raw.parquet"
    _parquet(raw, [
        {"db_name": "league", "year": 2024, "week": 1, "team_key": "other"},
        {"db_name": "missing", "year": 2024, "week": 1, "team_key": "other"},
    ])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 14, "path": str(raw)}]))

    row = profile(base=base, manifest=manifest)["rows"][0]

    assert row["league_week_overlap_keys"] == 1
    assert row["league_week_absent_keys"] == 1


def test_profile_fans_unique_raw_manager_to_cache_players_when_team_name_is_absent(tmp_path: Path) -> None:
    """Manager-only is safe only when one source team owns that manager-week."""
    from scripts.research_cohorts.profile_raw_team_signal_sources import profile

    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT 'league'::VARCHAR AS db_name, 2024::INTEGER AS year,
               1::INTEGER AS week, NULL::VARCHAR AS team_key,
               'manager-a'::VARCHAR AS manager, NULL::VARCHAR AS team_name,
               'p1'::VARCHAR AS NFL_player_id
        UNION ALL
        SELECT 'league', 2024, 1, NULL, 'manager-a', NULL, 'p2'
    """)
    con.close()
    raw = tmp_path / "raw.parquet"
    _parquet(raw, [{
        "db_name": "league", "year": 2024, "week": 1,
        "team_key": "source-team", "manager": "manager-a",
        "team_name": "source team",
    }])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 15, "path": str(raw)}]))

    row = profile(base=base, manifest=manifest)["rows"][0]

    assert row["matched_source_team_keys"] == 1
    assert row["matched_source_player_rows"] == 2
    assert row["matched_by_unique_manager"] == 1


def test_profile_blocks_manager_only_fanout_for_multi_team_source_manager(tmp_path: Path) -> None:
    """A manager with two raw teams that week is not a safe player-team bridge."""
    from scripts.research_cohorts.profile_raw_team_signal_sources import profile

    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT 'league'::VARCHAR AS db_name, 2024::INTEGER AS year,
               1::INTEGER AS week, NULL::VARCHAR AS team_key,
               'manager-a'::VARCHAR AS manager, NULL::VARCHAR AS team_name,
               'p1'::VARCHAR AS NFL_player_id
    """)
    con.close()
    raw = tmp_path / "raw.parquet"
    _parquet(raw, [
        {"db_name": "league", "year": 2024, "week": 1,
         "team_key": "source-team-a", "manager": "manager-a"},
        {"db_name": "league", "year": 2024, "week": 1,
         "team_key": "source-team-b", "manager": "manager-a"},
    ])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 16, "path": str(raw)}]))

    row = profile(base=base, manifest=manifest)["rows"][0]

    assert row["matched_source_team_keys"] == 0
    assert row["matched_source_player_rows"] == 0
    assert row["matched_by_unique_manager"] == 0
