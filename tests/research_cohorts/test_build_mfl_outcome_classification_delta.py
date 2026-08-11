import json
from pathlib import Path

import duckdb
import pandas as pd


def _parquet(path: Path, rows: list[dict]) -> None:
    con = duckdb.connect()
    con.register("rows", pd.DataFrame(rows))
    con.execute("CREATE TABLE source AS SELECT * FROM rows")
    con.execute(f"COPY source TO '{path.as_posix()}' (FORMAT PARQUET)")
    con.close()


def test_mfl_outcome_delta_keeps_only_exact_null_cell_repairs(tmp_path: Path) -> None:
    from scripts.research_cohorts.build_mfl_outcome_classification_delta import build

    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("""
      CREATE SCHEMA public;
      CREATE TABLE public.player_fantasy AS
      SELECT 'l'::VARCHAR AS db_name, 2024::INTEGER AS "year", 1::INTEGER AS "week",
             'p1'::VARCHAR NFL_player_id, 'm'::VARCHAR manager,
             NULL::INTEGER win, 100.0::DOUBLE team_points, NULL::INTEGER is_playoffs
      UNION ALL
      SELECT 'l', 2024, 1, 'p2', 'm', 0, 99.0, 0;
    """)
    con.close()
    raw = tmp_path / "mfl.parquet"
    _parquet(raw, [
        {"db_name": "l", "year": 2024, "week": 1, "NFL_player_id": "p1", "manager": "m", "source_win": 1, "source_team_points": 100.0, "source_is_playoffs": 1, "source_loss": 0, "source_tie": 0},
        {"db_name": "l", "year": 2024, "week": 1, "NFL_player_id": "p1", "manager": "m", "source_win": 1, "source_team_points": 100.0, "source_is_playoffs": 1, "source_loss": 0, "source_tie": 0},
        {"db_name": "l", "year": 2024, "week": 1, "NFL_player_id": "p2", "manager": "m", "source_win": 1, "source_team_points": 101.0, "source_is_playoffs": 1, "source_loss": 0, "source_tie": 0},
    ])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 11, "path": str(raw)}]))
    out, report = tmp_path / "delta.parquet", tmp_path / "report.json"

    result = build(base=base, manifest=manifest, out=out, report=report)

    assert result["cache_mutated"] is False
    assert result["delta_rows"] == 1
    assert result["supported_fields"]["win"]["cache_null_candidates"] == 1
    assert result["supported_fields"]["team_points"]["cache_null_candidates"] == 0
    assert result["supported_fields"]["is_playoffs"]["cache_null_candidates"] == 1
    assert result["supported_fields"]["win"]["cache_conflicts"] == 1
    assert result["blocked_schema_fields"]["loss"]["source_non_null_keys"] == 2
    check = duckdb.connect()
    row = check.execute("SELECT NFL_player_id, source_win, source_team_points, source_is_playoffs FROM read_parquet(?)", [str(out)]).fetchone()
    assert row == ("p1", 1, 100.0, 1)
    check.close()
