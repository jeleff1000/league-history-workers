import json

import duckdb

from build_structured_player_signal_delta import build


def test_reports_ambiguous_null_cells_without_promoting_them(tmp_path):
    base = tmp_path / "base.duckdb"
    source = tmp_path / "source.parquet"
    manifest = tmp_path / "manifest.json"
    delta = tmp_path / "delta.parquet"
    rejected = tmp_path / "rejected.parquet"
    report = tmp_path / "report.json"

    con = duckdb.connect(str(base))
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
          ('league', 2024, 1, 'player', 'manager', NULL, NULL, NULL, NULL, NULL),
          ('league', 2024, 1, 'player', 'manager', NULL, NULL, NULL, NULL, NULL)
    """)
    con.execute("""
        COPY (
          SELECT 'league' AS db_name, 2024 AS year, 1 AS week,
                 'player' AS NFL_player_id, 'manager' AS manager,
                 1 AS source_win, 100.0 AS source_team_points,
                 1 AS source_is_playoffs
        ) TO ? (FORMAT PARQUET)
    """, [str(source)])
    con.close()
    manifest.write_text(json.dumps([{"artifact_id": 7, "path": str(source)}]))

    result = build(
        base=base,
        manifest=manifest,
        out=delta,
        rejected_out=rejected,
        report=report,
    )

    assert result["delta_rows"] == 0
    assert result["residual_null_cells_by_reason"] == {
        "ambiguous_canonical_identity": 3,
        "source_value_conflict": 0,
    }
    rows = duckdb.connect().execute(
        "SELECT target_field, rejection_reason, canonical_null_rows FROM read_parquet(?)",
        [str(rejected)],
    ).fetchall()
    assert sorted(rows) == [
        ("is_playoffs", "ambiguous_canonical_identity", 1),
        ("team_points", "ambiguous_canonical_identity", 1),
        ("win", "ambiguous_canonical_identity", 1),
    ]
    assert result["artifact_current_state"] == [
        {
            "artifact_id": 7,
            "ambiguous_null_cells": 3,
            "cache_conflict_cells": 0,
            "cache_equal_cells": 0,
            "safe_null_candidates": 0,
            "source_rows": 1,
        }
    ]


def test_emits_loss_and_tie_null_candidates_from_exact_player_source(tmp_path):
    base = tmp_path / "base.duckdb"
    source = tmp_path / "source.parquet"
    manifest = tmp_path / "manifest.json"
    delta = tmp_path / "delta.parquet"
    report = tmp_path / "report.json"

    con = duckdb.connect(str(base))
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
          ('league', 2024, 1, 'player', 'manager', 1, NULL, NULL, 90.0, 0)
    """)
    con.execute("""
        COPY (
          SELECT 'league' AS db_name, 2024 AS year, 1 AS week,
                 'player' AS NFL_player_id, 'manager' AS manager,
                 0 AS source_win, 1 AS source_loss, 0 AS source_tie,
                 99.0 AS source_team_points, 0 AS source_is_playoffs
        ) TO ? (FORMAT PARQUET)
    """, [str(source)])
    con.close()
    manifest.write_text(json.dumps([{"artifact_id": 9, "path": str(source)}]))

    result = build(base=base, manifest=manifest, out=delta, report=report)

    assert result["supported_fields"]["loss"] == {
        "source_non_null_keys": 1,
        "source_conflicting_keys": 0,
        "cache_equal": 0,
        "cache_null_candidates": 1,
        "cache_conflicts": 0,
    }
    assert result["supported_fields"]["tie"] == {
        "source_non_null_keys": 1,
        "source_conflicting_keys": 0,
        "cache_equal": 0,
        "cache_null_candidates": 1,
        "cache_conflicts": 0,
    }
    assert duckdb.connect().execute(
        "SELECT source_win, source_loss, source_tie, source_team_points, source_is_playoffs FROM read_parquet(?)",
        [str(delta)],
    ).fetchall() == [(None, 1, 0, None, None)]


def test_emits_only_exact_conflicting_cells_in_source_replace_mode(tmp_path):
    base = tmp_path / "base.duckdb"
    source = tmp_path / "source.parquet"
    manifest = tmp_path / "manifest.json"
    delta = tmp_path / "delta.parquet"
    report = tmp_path / "report.json"

    con = duckdb.connect(str(base))
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
          ('league', 2024, 1, 'player', 'manager', 1, 0, 0, 90.0, 0)
    """)
    con.execute("""
        COPY (
          SELECT 'league' AS db_name, 2024 AS year, 1 AS week,
                 'player' AS NFL_player_id, 'manager' AS manager,
                 0 AS source_win, 0 AS source_loss, 0 AS source_tie,
                 90.0 AS source_team_points, 0 AS source_is_playoffs
        ) TO ? (FORMAT PARQUET)
    """, [str(source)])
    con.close()
    manifest.write_text(json.dumps([{"artifact_id": 10, "path": str(source)}]))

    result = build(
        base=base, manifest=manifest, out=delta, report=report,
        mode="source-replace",
    )

    assert result["mode"] == "source-replace"
    assert result["delta_rows"] == 1
    assert duckdb.connect().execute(
        "SELECT source_win, source_loss, source_tie, source_team_points, "
        "source_is_playoffs, expected_win, expected_loss, expected_tie, "
        "expected_team_points, expected_is_playoffs FROM read_parquet(?)",
        [str(delta)],
    ).fetchall() == [(0, None, None, None, None, 1, None, None, None, None)]
