import json
from pathlib import Path

import duckdb
import pandas as pd


def _write_parquet(path: Path, rows: list[dict]) -> None:
    con = duckdb.connect()
    con.register("rows", pd.DataFrame(rows))
    con.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def _base(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT 'league'::VARCHAR AS db_name, 2013::INTEGER AS year, 9::INTEGER AS week,
               'nfl-1'::VARCHAR AS NFL_player_id, NULL::VARCHAR AS manager,
               'mfl-1'::VARCHAR AS mfl_player_id, 'mfl'::VARCHAR AS platform, 0::INTEGER AS win, NULL::INTEGER AS loss,
               NULL::INTEGER AS tie, NULL::DOUBLE AS team_points, NULL::INTEGER AS is_playoffs
    """)
    con.close()


def test_builds_exact_mfl_candidate_for_null_manager_cache_recipient(tmp_path: Path) -> None:
    from scripts.research_cohorts.build_exact_mfl_classification_delta import build

    base = tmp_path / "base.duckdb"
    _base(base)
    source = tmp_path / "classification.parquet"
    _write_parquet(source, [{
        "db_name": "league", "year": 2013, "week": 9, "NFL_player_id": "nfl-1",
        "manager": "source manager", "status": "confirmed_outcome",
        "source_win": 1, "source_loss": 0, "source_tie": 0,
        "source_team_points": 121.5, "source_is_playoffs": False,
    }])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 1, "path": str(source)}]))
    out = tmp_path / "delta.parquet"

    report = build(base=base, manifest=manifest, out=out, report=tmp_path / "report.json")

    assert report["read_only"] is True
    assert report["cache_mutated"] is False
    assert report["candidate_rows"] == 1
    assert report["unmatched_source_keys"] == 0
    assert report["source_conflicting_keys"] == 0
    assert report["artifact_current_state"] == [{
        "artifact_id": 1,
        "source_rows": 1,
        "source_cells": 5,
        "cache_equal_cells": 0,
        "safe_null_candidates": 4,
        "cache_conflict_cells": 1,
        "ambiguous_null_cells": 0,
    }]
    con = duckdb.connect()
    row = con.execute(
        "SELECT NFL_player_id, canonical_win, source_win, canonical_loss, source_loss, "
        "canonical_team_points, source_team_points FROM read_parquet(?)",
        [str(out)],
    ).fetchone()
    con.close()
    assert row == ("nfl-1", 0, 1, None, 0, None, 121.5)


def test_quarantines_conflicting_source_values_for_one_exact_player_key(tmp_path: Path) -> None:
    from scripts.research_cohorts.build_exact_mfl_classification_delta import build

    base = tmp_path / "base.duckdb"
    _base(base)
    source = tmp_path / "classification.parquet"
    rows = [{
        "db_name": "league", "year": 2013, "week": 9, "NFL_player_id": "nfl-1",
        "manager": "source manager", "status": "confirmed_outcome",
        "source_win": value, "source_loss": 1 - value, "source_tie": 0,
        "source_team_points": 121.5, "source_is_playoffs": False,
    } for value in (0, 1)]
    _write_parquet(source, rows)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 2, "path": str(source)}]))

    report = build(
        base=base, manifest=manifest, out=tmp_path / "delta.parquet",
        report=tmp_path / "report.json",
    )

    assert report["candidate_rows"] == 0
    assert report["source_conflicting_keys"] == 1


def test_emits_each_canonical_recipient_for_an_ambiguous_exact_player_key(tmp_path: Path) -> None:
    from scripts.research_cohorts.build_exact_mfl_classification_delta import build

    base = tmp_path / "base.duckdb"
    _base(base)
    con = duckdb.connect(str(base))
    con.execute("INSERT INTO public.player_fantasy SELECT * FROM public.player_fantasy")
    con.close()
    source = tmp_path / "classification.parquet"
    _write_parquet(source, [{
        "db_name": "league", "year": 2013, "week": 9, "NFL_player_id": "nfl-1",
        "manager": "source manager", "status": "confirmed_outcome",
        "source_win": 1, "source_loss": 0, "source_tie": 0,
        "source_team_points": 121.5, "source_is_playoffs": False,
    }])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 3, "path": str(source)}]))
    ambiguous = tmp_path / "ambiguous.parquet"

    report = build(
        base=base, manifest=manifest, out=tmp_path / "delta.parquet",
        report=tmp_path / "report.json", ambiguous_out=ambiguous,
    )

    assert report["candidate_rows"] == 0
    assert report["ambiguous_recipient_keys"] == 1
    assert report["ambiguous_recipient_profile"] == {
        "keys": 1,
        "recipient_rows": 2,
        "recipient_count_distribution": {"2": 1},
        "rows_without_canonical_manager": 2,
        "rows_without_canonical_mfl_player_id": 0,
        "rows_without_canonical_team_key": 2,
        "rows_without_canonical_team_name": 2,
        "rows_without_canonical_fantasy_position": 2,
    }
    con = duckdb.connect()
    rows = con.execute(
        "SELECT recipient_count, canonical_mfl_player_id, canonical_win, source_win FROM read_parquet(?) ORDER BY player_rowid",
        [str(ambiguous)],
    ).fetchall()
    con.close()
    assert rows == [(2, "mfl-1", 0, 1), (2, "mfl-1", 0, 1)]
