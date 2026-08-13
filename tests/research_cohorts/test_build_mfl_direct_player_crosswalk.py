from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


def _parquet(path: Path, rows: list[dict]) -> None:
    con = duckdb.connect()
    con.register("rows", pd.DataFrame(rows))
    con.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_direct_crosswalk_requires_one_name_position_team_year_recipient(tmp_path: Path) -> None:
    """MFL's source-only IDs can be resolved only by an exact historical identity."""
    from scripts.research_cohorts.build_mfl_direct_player_crosswalk import build

    directory = tmp_path / "directory.parquet"
    _parquet(directory, [
        {"source_year": 2003, "mfl_player_id": "1", "espn_id": None,
         "raw_name": "Andersen, Morten", "raw_position": "PK", "raw_team": "KCC"},
        {"source_year": 2003, "mfl_player_id": "2", "espn_id": None,
         "raw_name": "Smith, Alex", "raw_position": "QB", "raw_team": "SFO"},
        {"source_year": 2003, "mfl_player_id": "3", "espn_id": None,
         "raw_name": "League, Coach", "raw_position": "Coach", "raw_team": None},
    ])
    existing = tmp_path / "existing.parquet"
    _parquet(existing, [
        {"source_year": 2003, "mfl_player_id": "1", "NFL_player_id": None,
         "crosswalk_status": "missing_espn_id"},
        {"source_year": 2003, "mfl_player_id": "2", "NFL_player_id": None,
         "crosswalk_status": "missing_espn_id"},
        {"source_year": 2003, "mfl_player_id": "3", "NFL_player_id": None,
         "crosswalk_status": "missing_espn_id"},
    ])
    ops = tmp_path / "ops.duckdb"
    con = duckdb.connect(str(ops))
    con.execute("CREATE SCHEMA nfl_historical")
    con.execute("""
        CREATE TABLE nfl_historical.nfl_player_stats_all (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER, player VARCHAR,
          position VARCHAR, nfl_team_api VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO nfl_historical.nfl_player_stats_all VALUES
          ('morten', 2003, 1, 'Morten Andersen', 'K', 'KCC'),
          ('alex-one', 2003, 1, 'Alex Smith', 'QB', 'SFO'),
          ('alex-two', 2003, 2, 'Alex Smith', 'QB', 'SFO')
    """)
    con.close()

    out = tmp_path / "direct.parquet"
    report = tmp_path / "report.json"
    result = build(
        directory=directory, existing_crosswalk=existing, ops_cache=ops,
        out=out, report=report,
    )

    assert result["candidate_rows"] == 1
    assert result["status_counts"] == {
        "ambiguous_name_position_team_year": 1,
        "excluded_non_nfl_product": 1,
        "resolved_name_position_team_year": 1,
    }
    check = duckdb.connect()
    rows = check.execute(
        "SELECT source_year, mfl_player_id, NFL_player_id, crosswalk_status FROM read_parquet(?)",
        [str(out)],
    ).fetchall()
    check.close()
    assert rows == [(2003, "1", "morten", "resolved_name_position_team_year")]
