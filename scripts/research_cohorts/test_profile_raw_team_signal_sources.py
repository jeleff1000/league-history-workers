import json

import duckdb

from profile_raw_team_signal_sources import profile


def test_manager_team_fanout_does_not_match_other_null_team_keys(tmp_path):
    """A source team may reach only its own manager/team's player rows."""
    base = tmp_path / "canonical.duckdb"
    con = duckdb.connect(str(base))
    con.execute("""
        CREATE SCHEMA public
    """)
    con.execute("""
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR, year INTEGER, week INTEGER, team_key VARCHAR,
            manager VARCHAR, team_name VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO public.player_fantasy VALUES
          ('league', 2024, 1, NULL, 'Alice', 'Apex'),
          ('league', 2024, 1, NULL, 'Alice', 'Apex'),
          ('league', 2024, 1, NULL, 'Bob', 'Bruins'),
          ('league', 2024, 1, NULL, 'Bob', 'Bruins')
    """)
    source = tmp_path / "source.parquet"
    con.execute("""
        COPY (
          SELECT 'league'::VARCHAR AS db_name, 2024::INTEGER AS year,
                 1::INTEGER AS week, 'sleeper'::VARCHAR AS platform,
                 'source-team-a'::VARCHAR AS team_key,
                 'Alice'::VARCHAR AS manager, 'Apex'::VARCHAR AS team_name
        ) TO ? (FORMAT PARQUET)
    """, [str(source)])
    con.close()

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"artifact_id": 1, "path": str(source)}]))

    result = profile(base=base, manifest=manifest)

    assert result["rows"] == [{
        "artifact_id": 1,
        "source_team_rows": 1,
        "source_team_keys": 1,
        "league_week_overlap_keys": 1,
        "league_week_absent_keys": 0,
        "matched_source_team_rows": 1,
        "unmatched_source_team_rows": 0,
        "matched_source_team_keys": 1,
        "unmatched_source_team_keys": 0,
        "matched_by_mfl_manager_guid": 0,
        "matched_by_manager_team_name": 0,
        "matched_by_unique_manager": 1,
        "matched_source_player_rows": 2,
    }]
