from build_artifact_promotion_specmap import build_specmap_rows


def test_builds_one_reproducible_contract_row_per_artifact_file():
    rows = build_specmap_rows([
        {
            "artifact_id": "101", "artifact": "player-source", "file": "players.parquet", "file_instance": "1",
            "kind": "parquet", "columns": '["db_name","year","week","NFL_player_id","is_started","win"]',
        },
        {
            "artifact_id": "102", "artifact": "team-source", "file": "teams.parquet", "file_instance": "1",
            "kind": "parquet", "columns": '["db_name","year","week","team_key","win","team_points"]',
        },
        {
            "artifact_id": "103", "artifact": "audit", "file": "summary.json", "file_instance": "1",
            "kind": "json", "columns": '["mean_spread","sample_size"]',
        },
    ], canonical_columns={
        "db_name", "year", "week", "NFL_player_id", "is_started", "win", "team_key", "team_points",
    })

    assert rows == [
        {
            "artifact_id": "101", "artifact": "player-source", "file": "players.parquet", "file_instance": "1",
            "source_grain": "player_week", "source_key": "db_name|year|week|NFL_player_id",
            "cache_key": "db_name|year|week|NFL_player_id", "writable_target_columns": "is_started|win",
            "blocked_source_columns": "", "may_insert_missing_player_rows": "true",
            "terminal_reason": "", "quarantined_source_columns": "",
            "source_to_target_columns": "is_started:is_started|win:win",
        },
        {
            "artifact_id": "102", "artifact": "team-source", "file": "teams.parquet", "file_instance": "1",
            "source_grain": "team_week", "source_key": "db_name|year|week|team_key",
            "cache_key": "db_name|year|week|team_key", "writable_target_columns": "win|team_points",
            "blocked_source_columns": "", "may_insert_missing_player_rows": "false",
            "terminal_reason": "", "quarantined_source_columns": "",
            "source_to_target_columns": "win:win|team_points:team_points",
        },
        {
            "artifact_id": "103", "artifact": "audit", "file": "summary.json", "file_instance": "1",
            "source_grain": "aggregate_only", "source_key": "", "cache_key": "",
            "writable_target_columns": "", "blocked_source_columns": "",
            "may_insert_missing_player_rows": "false",
            "terminal_reason": "no_supported_row_level_contract", "quarantined_source_columns": "",
            "source_to_target_columns": "",
        },
    ]


def test_preserves_duplicate_base_filenames_as_distinct_source_file_instances():
    rows = build_specmap_rows([
        {
            "artifact_id": "101", "artifact": "multi-file", "file": "player.parquet",
            "kind": "parquet", "columns": '["db_name","year","week","NFL_player_id","win"]',
        },
        {
            "artifact_id": "101", "artifact": "multi-file", "file": "player.parquet",
            "kind": "parquet", "columns": '["db_name","year","week","NFL_player_id","win"]',
        },
    ], canonical_columns={"db_name", "year", "week", "NFL_player_id", "win"})

    assert [row["file_instance"] for row in rows] == ["1", "2"]
