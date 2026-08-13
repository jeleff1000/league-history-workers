from __future__ import annotations

import json
from pathlib import Path

import duckdb

from build_team_manager_signal_null_fill import build


def _base(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute("""
      CREATE TABLE public.player_fantasy (
        db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
        manager VARCHAR, win INTEGER, team_points DOUBLE, is_playoffs INTEGER,
        champion INTEGER, untouched VARCHAR
      )
    """)
    con.execute("""
      INSERT INTO public.player_fantasy VALUES
        ('league',2024,1,'p1','Alpha',NULL,NULL,NULL,NULL,'keep'),
        ('league',2024,1,'p2','Alpha',1,90.0,0,0,'keep'),
        ('league',2024,1,'p3','Bravo',NULL,NULL,NULL,NULL,'keep')
    """)
    con.close()


def _source(path: Path) -> None:
    con = duckdb.connect()
    con.execute("""
      CREATE TABLE source AS SELECT
        'league'::VARCHAR AS db_name, 2024::INTEGER AS year, 1::INTEGER AS week,
        ' alpha '::VARCHAR AS manager, 'team'::VARCHAR AS team_name,
        '1'::VARCHAR AS team_key, '0'::VARCHAR AS win, '100'::VARCHAR AS team_points,
        '1'::VARCHAR AS is_playoffs, '1'::VARCHAR AS champion
    """)
    con.execute("COPY source TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_manager_fanout_fills_only_null_cells_on_exact_team_week(tmp_path: Path) -> None:
    base, source = tmp_path / 'base.duckdb', tmp_path / 'source.parquet'
    manifest, out, report = tmp_path / 'manifest.json', tmp_path / 'delta.parquet', tmp_path / 'report.json'
    _base(base); _source(source)
    manifest.write_text(json.dumps([{'artifact_id': 1, 'path': str(source)}]))

    result = build(base=base, manifest=manifest, out=out, report=report)

    assert result['cache_mutated'] is False
    assert result['safe_source_team_weeks'] == 1
    assert result['matched_player_rows'] == 2
    assert result['delta_rows'] == 1
    assert result['comparison_cells_by_field']['win'] == {
        'source_non_null': 2, 'cache_equal': 0, 'cache_conflict': 1, 'cache_null': 1,
    }
    check = duckdb.connect()
    row = check.execute('SELECT NFL_player_id, source_win, source_team_points, source_is_playoffs, source_champion FROM read_parquet(?)', [str(out)]).fetchone()
    assert row == ('p1', 0, 100.0, 1, 1)
    check.close()


def test_duplicate_source_manager_week_is_excluded(tmp_path: Path) -> None:
    base, source = tmp_path / 'base.duckdb', tmp_path / 'source.parquet'
    manifest, out, report = tmp_path / 'manifest.json', tmp_path / 'delta.parquet', tmp_path / 'report.json'
    _base(base); _source(source)
    duplicate = source.with_name('duplicate.parquet')
    con = duckdb.connect()
    con.execute("CREATE TABLE source AS SELECT * FROM read_parquet(?)", [str(source)])
    con.execute("INSERT INTO source VALUES ('league',2024,1,'Alpha','other','2','1','101','1','1')")
    con.execute("COPY source TO ? (FORMAT PARQUET)", [str(duplicate)])
    con.close()
    manifest.write_text(json.dumps([{'artifact_id': 1, 'path': str(source)}, {'artifact_id': 2, 'path': str(duplicate)}]))

    result = build(base=base, manifest=manifest, out=out, report=report)

    assert result['conflicting_source_team_weeks'] == 1
    assert result['delta_rows'] == 0
