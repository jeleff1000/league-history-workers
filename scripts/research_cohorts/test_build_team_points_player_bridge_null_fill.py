from __future__ import annotations

import json
from pathlib import Path

import duckdb

from build_team_points_player_bridge_null_fill import build


def _base(path: Path) -> None:
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
          ('league', 2024, 1, 'p1', 'alpha', NULL, NULL, NULL, 80.0, NULL),
          ('league', 2024, 1, 'p1', 'bravo', NULL, NULL, NULL, 90.0, NULL)
    """)
    con.close()


def _source(path: Path) -> None:
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE source AS
        SELECT 'league'::VARCHAR AS db_name, 2024::INTEGER AS year,
               1::INTEGER AS week, 'p1'::VARCHAR AS NFL_player_id,
               NULL::VARCHAR AS manager, 1::INTEGER AS source_win,
               NULL::INTEGER AS source_loss, NULL::INTEGER AS source_tie,
               90.0::DOUBLE AS source_team_points,
               NULL::INTEGER AS source_is_playoffs
    """)
    con.execute("COPY source TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_team_points_uniquely_bridges_null_manager_to_existing_player_row(tmp_path: Path) -> None:
    base, source = tmp_path / "base.duckdb", tmp_path / "source.parquet"
    manifest, out, report = tmp_path / "manifest.json", tmp_path / "delta.parquet", tmp_path / "report.json"
    _base(base)
    _source(source)
    manifest.write_text(json.dumps([{"artifact_id": 1, "path": str(source)}]))

    result = build(base=base, manifest=manifest, out=out, report=report)

    assert result["cache_mutated"] is False
    assert result["new_lineage"] is False
    assert result["unique_team_points_bridges"] == 1
    assert result["null_fill_rows"] == 1
    check = duckdb.connect()
    row = check.execute("SELECT manager, source_win, source_team_points FROM read_parquet(?)", [str(out)]).fetchone()
    assert row == ("bravo", 1, None)
    check.close()
    base_check = duckdb.connect(str(base), read_only=True)
    assert base_check.execute("SELECT count(*) FROM public.player_fantasy WHERE win IS NULL").fetchone()[0] == 2
    base_check.close()


def test_tied_team_points_do_not_emit_a_candidate(tmp_path: Path) -> None:
    base, source = tmp_path / "base.duckdb", tmp_path / "source.parquet"
    manifest, out, report = tmp_path / "manifest.json", tmp_path / "delta.parquet", tmp_path / "report.json"
    _base(base)
    con = duckdb.connect(str(base))
    con.execute("UPDATE public.player_fantasy SET team_points=90.0")
    con.close()
    _source(source)
    manifest.write_text(json.dumps([{"artifact_id": 1, "path": str(source)}]))

    result = build(base=base, manifest=manifest, out=out, report=report)

    assert result["unique_team_points_bridges"] == 0
    assert result["ambiguous_team_points_bridges"] == 1
    assert result["null_fill_rows"] == 0
