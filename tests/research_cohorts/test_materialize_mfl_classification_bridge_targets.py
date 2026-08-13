import json
from pathlib import Path

import duckdb
import pandas as pd


def _parquet(path: Path, rows: list[dict]) -> None:
    con = duckdb.connect()
    con.register("rows", pd.DataFrame(rows))
    con.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def _cache(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          manager VARCHAR, win INTEGER, loss INTEGER, tie INTEGER,
          team_points DOUBLE, is_playoffs INTEGER
        )
    """)
    con.execute("""
        INSERT INTO public.player_fantasy VALUES
          ('league', 2015, 12, 'p1', 'Team A', 1, NULL, 0, 100.0, 0),
          ('league', 2015, 12, 'p2', 'Team B', 0, 1, 0, 90.0, 0),
          ('league', 2015, 13, 'p3', 'Team C', 1, NULL, 0, 110.0, 1)
    """)
    con.close()


def test_materializes_only_source_signals_with_current_null_recipients(tmp_path: Path) -> None:
    from scripts.research_cohorts.materialize_mfl_classification_bridge_targets import materialize

    cache = tmp_path / "cache.duckdb"
    _cache(cache)
    source = tmp_path / "classification.parquet"
    _parquet(source, [
        {"db_name": "league", "year": 2015, "week": 12, "NFL_player_id": "p1",
         "manager": "Team A", "source_id": "77", "source_win": 1,
         "source_loss": 0, "source_tie": 0, "source_team_points": 100.0,
         "source_is_playoffs": False},
        # Same league-week: target manifest must be one deterministic group.
        {"db_name": "league", "year": 2015, "week": 12, "NFL_player_id": "p9",
         "manager": "Team Z", "source_id": "77", "source_win": 1,
         "source_loss": 0, "source_tie": 0, "source_team_points": 100.0,
         "source_is_playoffs": False},
        # All supported values already exist in the cache: not a target.
        {"db_name": "league", "year": 2015, "week": 12, "NFL_player_id": "p2",
         "manager": "Team B", "source_id": "88", "source_win": 0,
         "source_loss": 1, "source_tie": 0, "source_team_points": 90.0,
         "source_is_playoffs": False},
        # No source signal: not a target.
        {"db_name": "league", "year": 2015, "week": 13, "NFL_player_id": "p3",
         "manager": "Team C", "source_id": "99", "source_win": None,
         "source_loss": None, "source_tie": None, "source_team_points": None,
         "source_is_playoffs": None},
    ])
    out = tmp_path / "targets.json"
    report = tmp_path / "report.json"

    result = materialize(base=cache, sources=[source], out=out, report=report)

    assert result["cache_mutated"] is False
    assert result["new_lineage"] is False
    assert result["candidate_cache_rows"] == 1
    assert result["target_groups"] == 1
    assert json.loads(out.read_text()) == {"groups": [
        {"db_name": "league", "year": 2015, "week": 12, "source_id": "77"}
    ]}
