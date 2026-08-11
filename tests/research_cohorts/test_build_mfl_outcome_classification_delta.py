import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest


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


def test_mfl_outcome_delta_ignores_unrelated_duplicate_canonical_keys(tmp_path: Path) -> None:
    """Only source keys must be unique; unrelated cache duplication is irrelevant."""
    from scripts.research_cohorts.build_mfl_outcome_classification_delta import build

    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("""
      CREATE SCHEMA public;
      CREATE TABLE public.player_fantasy AS
      SELECT 'target'::VARCHAR AS db_name, 2024::INTEGER AS "year", 1::INTEGER AS "week",
             'p1'::VARCHAR NFL_player_id, 'm'::VARCHAR manager,
             NULL::INTEGER win, 100.0::DOUBLE team_points, NULL::INTEGER is_playoffs
      UNION ALL
      SELECT 'unrelated', 2024, 1, 'p2', 'm', NULL, 99.0, NULL
      UNION ALL
      SELECT 'unrelated', 2024, 1, 'p2', 'm', NULL, 99.0, NULL;
    """)
    con.close()
    raw = tmp_path / "mfl.parquet"
    _parquet(raw, [{
        "db_name": "target", "year": 2024, "week": 1, "NFL_player_id": "p1", "manager": "m",
        "source_win": 1, "source_team_points": 100.0, "source_is_playoffs": 1,
        "source_loss": 0, "source_tie": 0,
    }])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 12, "path": str(raw)}]))

    result = build(
        base=base, manifest=manifest, out=tmp_path / "delta.parquet", report=tmp_path / "report.json"
    )

    assert result["canonical_duplicate_exact_keys"] == 0
    assert result["delta_rows"] == 1


def test_mfl_outcome_delta_writes_source_duplicate_profile_before_failing(tmp_path: Path) -> None:
    from scripts.research_cohorts.build_mfl_outcome_classification_delta import build

    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("""
      CREATE SCHEMA public;
      CREATE TABLE public.player_fantasy AS
      SELECT 'target'::VARCHAR AS db_name, 2024::INTEGER AS "year", 1::INTEGER AS "week",
             'p1'::VARCHAR NFL_player_id, 'm'::VARCHAR manager,
             NULL::INTEGER win, 100.0::DOUBLE team_points, NULL::INTEGER is_playoffs,
             'team-a'::VARCHAR team_key, 'A'::VARCHAR team_name, 'mfl'::VARCHAR platform
      UNION ALL
      SELECT 'target', 2024, 1, 'p1', 'm', NULL, 100.0, NULL, 'team-b', 'B', 'mfl';
    """)
    con.close()
    raw = tmp_path / "mfl.parquet"
    _parquet(raw, [{
        "db_name": "target", "year": 2024, "week": 1, "NFL_player_id": "p1", "manager": "m",
        "source_win": 1, "source_team_points": 100.0, "source_is_playoffs": 1,
        "source_loss": 0, "source_tie": 0,
    }])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 13, "path": str(raw)}]))
    report = tmp_path / "report.json"

    with pytest.raises(RuntimeError, match="duplicate exact MFL outcome keys"):
        build(base=base, manifest=manifest, out=tmp_path / "delta.parquet", report=report)

    profile = json.loads(report.read_text())
    assert profile["canonical_duplicate_exact_keys"] == 1
    assert profile["duplicate_source_key_profile"]["sample"][0]["team_key_variants"] == 2
