import json
from pathlib import Path

import duckdb
import pandas as pd


def _parquet(path: Path, rows: list[dict]) -> None:
    con = duckdb.connect()
    con.register("rows", pd.DataFrame(rows))
    con.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_profile_batches_exact_team_key_checks_without_mutating_cache(tmp_path: Path) -> None:
    """Removing the single batched semi-join would make per-artifact scans regress."""
    from scripts.research_cohorts.profile_raw_team_signal_sources import profile

    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT 'league'::VARCHAR AS db_name, 2024::INTEGER AS year, 1::INTEGER AS week,
               'team-a'::VARCHAR AS team_key, 'p1'::VARCHAR AS NFL_player_id
        UNION ALL
        SELECT 'league', 2024, 1, 'team-a', 'p2'
    """)
    con.close()

    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    _parquet(first, [{"db_name": "league", "year": 2024, "week": 1, "team_key": "team-a", "win": 1}])
    _parquet(second, [
        {"db_name": "league", "year": 2024, "week": 1, "team_key": "team-b", "win": 1},
        {"db_name": "league", "year": 2024, "week": 1, "team_key": "team-b", "win": 1},
    ])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([
        {"artifact_id": 10, "path": str(first)},
        {"artifact_id": 11, "path": str(second)},
    ]))

    result = profile(base=base, manifest=manifest)

    by_id = {row["artifact_id"]: row for row in result["rows"]}
    assert result["read_only"] is True
    assert result["cache_mutated"] is False
    assert by_id[10] == {
        "artifact_id": 10,
        "source_team_rows": 1,
        "source_team_keys": 1,
        "matched_source_team_rows": 1,
        "unmatched_source_team_rows": 0,
        "matched_source_team_keys": 1,
        "unmatched_source_team_keys": 0,
    }
    assert by_id[11]["source_team_rows"] == 2
    assert by_id[11]["source_team_keys"] == 1
    assert by_id[11]["matched_source_team_rows"] == 0
    assert by_id[11]["unmatched_source_team_rows"] == 2
    assert by_id[11]["unmatched_source_team_keys"] == 1

    check = duckdb.connect(str(base), read_only=True)
    assert check.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0] == 2
    check.close()
