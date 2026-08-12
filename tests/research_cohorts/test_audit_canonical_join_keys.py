import duckdb


def test_profiles_player_keys_and_team_fanout_without_treating_manager_as_unique() -> None:
    from scripts.research_cohorts.audit_canonical_join_keys import profile_relation

    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE player_fantasy (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          platform VARCHAR, manager VARCHAR, team_key VARCHAR, team_name VARCHAR,
          mfl_player_id VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO player_fantasy VALUES
          ('league', 2024, 1, 'p1', 'mfl', 'alice', 'franchise-a', 'A', 'mfl-1'),
          ('league', 2024, 1, 'p2', 'mfl', 'alice', 'franchise-a', 'A', 'mfl-2'),
          ('league', 2024, 1, 'p3', 'mfl', 'alice', 'franchise-b', 'B', NULL),
          ('league', 2024, 1, 'p4', 'mfl', 'bob', 'franchise-c', 'C', 'mfl-4')
    """)

    result = profile_relation(con, "player_fantasy")

    assert result["rows"] == 4
    assert result["player_keys"]["player_week_nfl_team_key"]["unique"] is True
    assert result["team_keys"]["team_week_team_key"]["team_groups"] == 3
    assert result["team_keys"]["team_week_team_key"]["max_player_fanout"] == 2
    assert result["manager_fanout"]["manager_week"]["manager_groups"] == 2
    assert result["manager_fanout"]["manager_week"]["max_player_fanout"] == 3
    assert result["manager_fanout"]["manager_team_name_week"]["manager_groups"] == 3
    assert result["player_keys"]["player_week_platform_native_id_manager"]["unique"] is True
    assert result["platform_id_coverage"] == [{
        "platform": "mfl",
        "rows": 4,
        "rows_with_nfl_player_id": 4,
        "rows_with_mfl_player_id": 3,
        "rows_with_mfl_and_nfl_player_id": 3,
    }]
