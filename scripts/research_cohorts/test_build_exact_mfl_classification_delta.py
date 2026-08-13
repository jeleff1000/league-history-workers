import json

import duckdb

from build_exact_mfl_classification_delta import build


def test_reports_unattributable_managerless_ambiguous_source(tmp_path):
    """A source outcome without a franchise/manager cannot choose duplicate cache rows."""
    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
            platform VARCHAR, win INTEGER, loss INTEGER, tie INTEGER,
            team_points DOUBLE, is_playoffs INTEGER, manager VARCHAR,
            team_key VARCHAR, team_name VARCHAR, mfl_player_id VARCHAR,
            fantasy_position VARCHAR, is_rostered INTEGER, is_started INTEGER,
            fantasy_points DOUBLE
        )
    """)
    con.execute("""
        INSERT INTO public.player_fantasy VALUES
        ('smpl_mfl_1', 2023, 15, '00-1', 'mfl', NULL, NULL, NULL, NULL, NULL,
         'manager-a', 'team-a', 'A', '1', 'RB', 1, 1, 10.0),
        ('smpl_mfl_1', 2023, 15, '00-1', 'mfl', NULL, NULL, NULL, NULL, NULL,
         NULL, NULL, NULL, NULL, NULL, 1, 1, 10.0)
    """)
    source = tmp_path / "source.parquet"
    con.execute("""
        COPY (
            SELECT 1::BIGINT AS target_ordinal, 'smpl_mfl_1'::VARCHAR AS db_name,
              2023::INTEGER AS year, 15::INTEGER AS week, '00-1'::VARCHAR AS NFL_player_id,
              NULL::VARCHAR AS manager, '1'::VARCHAR AS source_id,
              'confirmed_outcome'::VARCHAR AS status, 1::INTEGER AS source_win,
              0::INTEGER AS source_loss, 0::INTEGER AS source_tie,
              120.0::DOUBLE AS source_team_points, FALSE AS source_is_playoffs
        ) TO ? (FORMAT PARQUET)
    """, [str(source)])
    con.close()
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 7, "path": str(source)}]))

    report = build(
        base=base,
        manifest=manifest,
        out=tmp_path / "candidate.parquet",
        report=tmp_path / "report.json",
        ambiguous_out=tmp_path / "ambiguous.parquet",
    )

    assert report["candidate_rows"] == 0
    assert report["artifact_unattributable_state"] == [{
        "artifact_id": 7,
        "unattributable_ambiguous_keys": 1,
        "unattributable_source_cells": 5,
        "all_ambiguous_source_keys_lack_manager": True,
    }]
