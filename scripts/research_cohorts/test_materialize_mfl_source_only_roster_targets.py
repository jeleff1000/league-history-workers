import json

import duckdb

from materialize_mfl_source_only_roster_targets import build


def test_targets_only_mfl_weeks_with_a_source_team_that_has_no_safe_player_recipient(tmp_path):
    """A roster pull is needed only when team signals cannot safely fan out."""
    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
            manager VARCHAR, team_key VARCHAR, team_name VARCHAR, platform VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO public.player_fantasy VALUES
        ('mfl-a', 2003, 1, 'p1', 'ALPHA', NULL, NULL, 'mfl'),
        ('mfl-a', 2003, 2, 'p2', 'DUP', NULL, NULL, 'mfl')
    """)
    source = tmp_path / "source.parquet"
    con.execute("""
        COPY (
          SELECT * FROM (VALUES
            ('mfl-a', 2003, 1, '100', '0001', 'Alpha', 'Alpha', 'mfl'),
            ('mfl-a', 2003, 1, '100', '0002', 'Bravo', 'Bravo', 'mfl'),
            ('mfl-a', 2003, 2, '100', '0001', 'Dup', 'One', 'mfl'),
            ('mfl-a', 2003, 2, '100', '0002', 'Dup', 'Two', 'mfl')
          ) AS t(db_name, year, week, league_id, team_key, manager, team_name, platform)
        ) TO ? (FORMAT PARQUET)
    """, [str(source)])
    con.close()

    output = tmp_path / "targets.json"
    report = build(base=base, source=source, out=output)

    assert report["source_team_weeks"] == 4
    assert report["safe_recipient_team_weeks"] == 1
    assert report["source_only_team_weeks"] == 3
    assert report["target_league_weeks"] == 2
    assert json.loads(output.read_text()) == {
        "groups": [
            {"db_name": "mfl-a", "year": 2003, "week": 1, "source_id": "100"},
            {"db_name": "mfl-a", "year": 2003, "week": 2, "source_id": "100"},
        ],
        "read_only": True,
        "source_only_team_weeks": 3,
    }
