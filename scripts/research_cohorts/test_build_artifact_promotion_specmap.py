from build_artifact_promotion_specmap import build_specmap_rows


def test_builds_one_reproducible_contract_row_per_artifact_file():
    rows = build_specmap_rows([
        {
            "artifact_id": "101", "artifact": "player-source", "file": "players.parquet",
            "kind": "parquet", "columns": '["db_name","year","week","NFL_player_id","is_started","win"]',
        },
        {
            "artifact_id": "102", "artifact": "team-source", "file": "teams.parquet",
            "kind": "parquet", "columns": '["db_name","year","week","team_key","win","team_points"]',
        },
        {
            "artifact_id": "103", "artifact": "audit", "file": "summary.json",
            "kind": "json", "columns": '["mean_spread","sample_size"]',
        },
    ], canonical_columns={
        "db_name", "year", "week", "NFL_player_id", "is_started", "win", "team_key", "team_points",
    })

    assert rows == [
        {
            "artifact_id": "101", "artifact": "player-source", "file": "players.parquet",
            "source_grain": "player_week", "source_key": "db_name|year|week|NFL_player_id",
            "cache_key": "db_name|year|week|NFL_player_id", "writable_target_columns": "is_started|win",
            "blocked_source_columns": "", "may_insert_missing_player_rows": "true",
            "terminal_reason": "", "quarantined_source_columns": "",
        },
        {
            "artifact_id": "102", "artifact": "team-source", "file": "teams.parquet",
            "source_grain": "team_week", "source_key": "db_name|year|week|team_key",
            "cache_key": "db_name|year|week|team_key", "writable_target_columns": "win|team_points",
            "blocked_source_columns": "", "may_insert_missing_player_rows": "false",
            "terminal_reason": "", "quarantined_source_columns": "",
        },
        {
            "artifact_id": "103", "artifact": "audit", "file": "summary.json",
            "source_grain": "aggregate_only", "source_key": "", "cache_key": "",
            "writable_target_columns": "", "blocked_source_columns": "",
            "may_insert_missing_player_rows": "false",
            "terminal_reason": "no_supported_row_level_contract", "quarantined_source_columns": "",
        },
    ]
