from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


def _parquet(path: Path, rows: list[dict]) -> None:
    con = duckdb.connect()
    con.register("rows", pd.DataFrame(rows))
    con.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_crosswalk_only_resolves_unique_espn_to_nfl_identity(tmp_path: Path) -> None:
    from scripts.research_cohorts.build_mfl_roster_player_crosswalk import build

    source = tmp_path / "mfl_directory.parquet"
    _parquet(source, [
        {"source_year": 2004, "mfl_player_id": "10", "espn_id": "100", "raw_name": "One"},
        {"source_year": 2004, "mfl_player_id": "11", "espn_id": "101", "raw_name": "Two"},
        {"source_year": 2004, "mfl_player_id": "12", "espn_id": None, "raw_name": "DST"},
        {"source_year": 2004, "mfl_player_id": "13", "espn_id": "102", "raw_name": "Ambiguous"},
    ])
    ops = tmp_path / "ops.duckdb"
    con = duckdb.connect(str(ops))
    con.execute("CREATE SCHEMA nfl_historical")
    con.execute("""
        CREATE TABLE nfl_historical.player_bio (
          espn_id VARCHAR, NFL_player_id VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO nfl_historical.player_bio VALUES
          ('100', 'nfl-1'), ('101', 'nfl-2'), ('102', 'nfl-3'), ('102', 'nfl-4')
    """)
    con.close()

    out = tmp_path / "crosswalk.parquet"
    report = tmp_path / "report.json"
    result = build(directory=source, ops_cache=ops, out=out, report=report)

    assert result["directory_keys"] == 4
    assert result["status_counts"] == {
        "ambiguous_espn_to_nfl": 1,
        "missing_espn_id": 1,
        "resolved_espn_to_nfl": 2,
    }
    check = duckdb.connect()
    rows = check.execute(
        "SELECT mfl_player_id, NFL_player_id, crosswalk_status FROM read_parquet(?) ORDER BY mfl_player_id",
        [str(out)],
    ).fetchall()
    check.close()
    assert rows == [
        ("10", "nfl-1", "resolved_espn_to_nfl"),
        ("11", "nfl-2", "resolved_espn_to_nfl"),
        ("12", None, "missing_espn_id"),
        ("13", None, "ambiguous_espn_to_nfl"),
    ]
