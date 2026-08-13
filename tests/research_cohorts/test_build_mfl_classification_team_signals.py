from pathlib import Path

import duckdb
import pandas as pd


def _parquet(path: Path, rows: list[dict]) -> None:
    con = duckdb.connect()
    con.register("rows", pd.DataFrame(rows))
    con.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_builds_one_team_week_signal_from_consistent_rostered_player_evidence(tmp_path: Path) -> None:
    from scripts.research_cohorts.build_mfl_classification_team_signals import build

    classifications = tmp_path / "classification.parquet"
    _parquet(classifications, [
        {"db_name": "league", "year": 2015, "week": 12, "NFL_player_id": "p1",
         "source_win": 1, "source_loss": 0, "source_tie": 0,
         "source_team_points": 123.4, "source_is_playoffs": True},
        {"db_name": "league", "year": 2015, "week": 12, "NFL_player_id": "p2",
         "source_win": 1, "source_loss": 0, "source_tie": 0,
         "source_team_points": 123.4, "source_is_playoffs": True},
        # A player without a recovered franchise cannot create a team signal.
        {"db_name": "league", "year": 2015, "week": 12, "NFL_player_id": "no-franchise",
         "source_win": 0, "source_loss": 1, "source_tie": 0,
         "source_team_points": 90.0, "source_is_playoffs": False},
    ])
    memberships = tmp_path / "memberships.parquet"
    _parquet(memberships, [
        {"db_name": "league", "year": 2015, "week": 12, "source_year": 2015,
         "source_franchise_id": "0001", "mfl_player_id": "101"},
        {"db_name": "league", "year": 2015, "week": 12, "source_year": 2015,
         "source_franchise_id": "0001", "mfl_player_id": "102"},
    ])
    crosswalk = tmp_path / "crosswalk.parquet"
    _parquet(crosswalk, [
        {"source_year": 2015, "mfl_player_id": "101", "NFL_player_id": "p1"},
        {"source_year": 2015, "mfl_player_id": "102", "NFL_player_id": "p2"},
    ])
    out = tmp_path / "signals.parquet"
    report = tmp_path / "report.json"

    result = build(
        classifications=[classifications], memberships=memberships,
        crosswalk=crosswalk, out=out, report=report,
    )

    assert result["resolved_classification_rows"] == 2
    assert result["unresolved_classification_rows"] == 1
    assert result["emitted_team_signals"] == 1
    con = duckdb.connect()
    rows = con.execute(
        "SELECT db_name, year, week, team_key, manager, team_name, win, loss, tie, team_points, is_playoffs "
        "FROM read_parquet(?)", [str(out)]
    ).fetchall()
    con.close()
    assert rows == [("league", 2015, 12, "0001", None, None, 1, 0, 0, 123.4, 1)]
