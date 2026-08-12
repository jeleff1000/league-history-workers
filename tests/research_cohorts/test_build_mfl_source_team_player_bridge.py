from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


def _parquet(path: Path, rows: list[dict]) -> None:
    con = duckdb.connect()
    con.register("rows", pd.DataFrame(rows))
    con.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_emits_a_source_team_player_bridge_without_requiring_the_cache_manager(tmp_path: Path) -> None:
    """Roster membership proves player-to-source-team before any cache fan-out."""
    from scripts.research_cohorts.build_mfl_source_team_player_bridge import build

    memberships = tmp_path / "memberships.parquet"
    _parquet(memberships, [
        {"db_name": "league", "year": 2014, "week": 12, "source_year": 2014,
         "source_franchise_id": "0001", "mfl_player_id": "10"},
        # A player on two source teams is ambiguous and must not be emitted.
        {"db_name": "league", "year": 2014, "week": 12, "source_year": 2014,
         "source_franchise_id": "0001", "mfl_player_id": "11"},
        {"db_name": "league", "year": 2014, "week": 12, "source_year": 2014,
         "source_franchise_id": "0002", "mfl_player_id": "12"},
        # There is no source-team signal for this franchise.
        {"db_name": "league", "year": 2014, "week": 12, "source_year": 2014,
         "source_franchise_id": "0003", "mfl_player_id": "13"},
    ])
    crosswalk = tmp_path / "crosswalk.parquet"
    _parquet(crosswalk, [
        {"source_year": 2014, "mfl_player_id": "10", "NFL_player_id": "p1"},
        {"source_year": 2014, "mfl_player_id": "11", "NFL_player_id": "p2"},
        {"source_year": 2014, "mfl_player_id": "12", "NFL_player_id": "p2"},
        {"source_year": 2014, "mfl_player_id": "13", "NFL_player_id": "p3"},
    ])
    signals = tmp_path / "signals.parquet"
    _parquet(signals, [
        {"db_name": "league", "year": 2014, "week": 12, "team_key": "0001",
         "manager": "Shuffling Crew", "team_name": "Shuffling Crew"},
        {"db_name": "league", "year": 2014, "week": 12, "team_key": "0002",
         "manager": "MilkdeezNutz", "team_name": "MilkdeezNutz"},
    ])

    out = tmp_path / "bridge.parquet"
    report = tmp_path / "report.json"
    result = build(memberships=memberships, crosswalk=crosswalk, team_signals=signals, out=out, report=report)

    assert result["emitted_bridge_rows"] == 1
    assert result["ambiguous_player_team_rows"] == 2
    assert result["source_team_unmatched_rows"] == 1
    con = duckdb.connect()
    rows = con.execute(
        "SELECT db_name, year, week, NFL_player_id, team_key, manager, team_name FROM read_parquet(?)",
        [str(out)],
    ).fetchall()
    con.close()
    assert rows == [("league", 2014, 12, "p1", "0001", "Shuffling Crew", "Shuffling Crew")]
