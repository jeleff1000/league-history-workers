from materialize_artifact_candidate_cells import materialize_rows


SPEC = {
    "artifact_id": "100", "file": "source.parquet", "file_instance": "1",
    "source_grain": "team_week_manager", "source_key": "db_name|year|week|manager",
    "source_to_target_columns": "win:win|is_playoffs:is_playoffs",
}


def test_materializer_emits_one_candidate_per_non_null_mapped_source_cell():
    cells, receipt = materialize_rows(SPEC, [{
        "db_name": "league", "year": 2020, "week": 14, "manager": "Team A",
        "win": 1, "is_playoffs": 1,
    }])

    assert receipt["source_rows"] == 1
    assert receipt["candidate_cells"] == 2
    assert [(cell["source_column"], cell["target_column"], cell["source_value_text"]) for cell in cells] == [
        ("win", "win", "1"), ("is_playoffs", "is_playoffs", "1"),
    ]
    assert all(cell["NFL_player_id"] is None for cell in cells)


def test_materializer_blocks_rows_without_the_contract_identity_instead_of_guessing():
    cells, receipt = materialize_rows(SPEC, [{
        "db_name": "league", "year": 2020, "week": 14, "manager": None,
        "win": 1, "is_playoffs": 1,
    }])

    assert cells == []
    assert receipt["blocked_missing_required_source_key:_manager"] == 1
