from artifact_candidate_comparison_contract import admissible_cells


def test_championship_candidate_requires_direct_player_start_evidence():
    cells = [
        {
            "artifact_id": "1", "artifact_file": "players.parquet", "file_instance": "1",
            "source_grain": "player_week", "db_name": "league", "year": "2020", "week": "16",
            "NFL_player_id": "p1", "team_key": None, "manager": None,
            "source_column": "is_started", "target_column": "is_started", "source_value_text": "1",
        },
        {
            "artifact_id": "1", "artifact_file": "players.parquet", "file_instance": "1",
            "source_grain": "player_week", "db_name": "league", "year": "2020", "week": "16",
            "NFL_player_id": "p1", "team_key": None, "manager": None,
            "source_column": "champion", "target_column": "champion", "source_value_text": "1",
        },
    ]

    admitted, blocked = admissible_cells(cells)

    assert [row["target_column"] for row in admitted] == ["is_started", "champion"]
    assert blocked == []


def test_team_level_champion_is_quarantined_even_when_a_team_has_started_players():
    cells = [{
        "artifact_id": "2", "artifact_file": "team.parquet", "file_instance": "1",
        "source_grain": "team_week", "db_name": "league", "year": "2020", "week": "16",
        "NFL_player_id": None, "team_key": "t1", "manager": None,
        "source_column": "champion", "target_column": "champion", "source_value_text": "1",
    }]

    admitted, blocked = admissible_cells(cells)

    assert admitted == []
    assert blocked == [{"cell": cells[0], "reason": "team_signal_cannot_award_player_championship_credit"}]
