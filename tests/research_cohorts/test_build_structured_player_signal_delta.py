import json
from pathlib import Path

import duckdb
import pandas as pd


def _parquet(path: Path, rows: list[dict]) -> None:
    con = duckdb.connect()
    con.register("rows", pd.DataFrame(rows))
    con.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_structured_player_delta_keeps_only_unambiguous_null_cell_repairs(tmp_path: Path) -> None:
    from scripts.research_cohorts.build_structured_player_signal_delta import build

    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT 'l'::VARCHAR db_name, 2024::INTEGER AS "year", 1::INTEGER AS "week",
               'p1'::VARCHAR NFL_player_id, 'm'::VARCHAR manager,
               NULL::INTEGER win, NULL::DOUBLE team_points, NULL::INTEGER is_playoffs
        UNION ALL SELECT 'l', 2024, 1, 'p2', 'm', 0, 99.0, 0
        UNION ALL SELECT 'l', 2024, 1, 'p3', 'm', NULL, NULL, NULL
        UNION ALL SELECT 'l', 2024, 1, 'p4', 'm', NULL, NULL, NULL
        UNION ALL SELECT 'l', 2024, 1, 'p4', 'm', NULL, NULL, NULL
    """)
    con.close()

    source = tmp_path / "source.parquet"
    _parquet(source, [
        {"db_name": "l", "year": 2024, "week": 1, "NFL_player_id": "p1", "manager": "m", "source_win": 1, "source_team_points": 101.0, "source_is_playoffs": 1},
        {"db_name": "l", "year": 2024, "week": 1, "NFL_player_id": "p2", "manager": "m", "source_win": 1, "source_team_points": 101.0, "source_is_playoffs": 1},
        {"db_name": "l", "year": 2024, "week": 1, "NFL_player_id": "p3", "manager": "m", "source_win": 1, "source_team_points": 101.0, "source_is_playoffs": 1},
        {"db_name": "l", "year": 2024, "week": 1, "NFL_player_id": "p3", "manager": "m", "source_win": 1, "source_team_points": 101.0, "source_is_playoffs": 1},
        {"db_name": "l", "year": 2024, "week": 1, "NFL_player_id": "p4", "manager": "m", "source_win": 1, "source_team_points": 101.0, "source_is_playoffs": 1},
        {"db_name": "l", "year": 2024, "week": 1, "NFL_player_id": "p5", "manager": "m", "source_win": 0, "source_team_points": None, "source_is_playoffs": None},
        {"db_name": "l", "year": 2024, "week": 1, "NFL_player_id": "p5", "manager": "m", "source_win": 1, "source_team_points": None, "source_is_playoffs": None},
    ])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 7, "path": str(source)}]))
    out, report = tmp_path / "delta.parquet", tmp_path / "report.json"

    result = build(base=base, manifest=manifest, out=out, report=report)

    assert result["cache_mutated"] is False
    assert result["new_lineage"] is False
    assert result["delta_rows"] == 2
    assert result["ambiguous_canonical_player_keys"] == 1
    assert result["ambiguous_identity_nullness"]["fully_specified"] == 1
    assert result["source_conflicting_player_keys"] == 1
    assert result["supported_fields"]["win"]["cache_null_candidates"] == 2
    assert result["supported_fields"]["win"]["cache_conflicts"] == 1
    check = duckdb.connect()
    assert check.execute(
        "SELECT NFL_player_id, source_win, source_team_points, source_is_playoffs "
        "FROM read_parquet(?) ORDER BY NFL_player_id", [str(out)]
    ).fetchall() == [("p1", 1, 101.0, 1), ("p3", 1, 101.0, 1)]
    check.close()
